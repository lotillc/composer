/**
 * Temporal Activity Worker
 *
 * Registers and executes step business logic as Temporal activities.
 * Accepts workflow definitions declaratively and extracts steps from them.
 *
 * ## Activity Worker Responsibilities:
 * - Connect to Temporal Server
 * - Extract step definitions from provided workflow objects
 * - Create Temporal activities from steps
 * - Create wrapper activities for error handlers and FanOut operations
 * - Register activities with appropriate task queues (fast/standard/heavy)
 * - Execute business logic directly (steps handle their own dependencies)
 *
 * ## What Activities CAN Do (unlike workflows):
 * - Access Node.js APIs (fs, http, process, etc.)
 * - Make database queries
 * - Call external APIs
 * - Use non-deterministic functions
 * - Import and execute business logic
 *
 * ## Step Execution Model:
 * Steps are pure business logic that receive only their declared inputs (bag fields).
 * Steps obtain their own dependencies (e.g., IxDbConnection.getInstance()).
 * The workflow engine handles all observability automatically.
 *
 * @module activity-worker
 */

import { Context as ActivityContext, ApplicationFailure } from "@temporalio/activity";
import { VersioningBehavior } from "@temporalio/common";
import { NativeConnection, Worker } from "@temporalio/worker";
import type { StepContextProvider } from "../../context-provider";
import type { FanOutMetadata } from "../../dag-sync-fanout";
import type { AsyncStepRuntime, Step } from "../../dag-sync-step";
import type { Workflow } from "../../dag-sync-workflow";
import { defaultLogger } from "../../defaults";
import type { ComposerLogger } from "../../types";
import { denamespaceSyntheticSteps } from "../build-scripts/utils/common";
import { isFanOut } from "../build-scripts/utils/type-guards";
import { startTaskQueueMetrics, type TaskQueueMetricsHandle } from "../metrics/task-queue-metrics";
import { isComposerError } from "../utils/is-composer-error";
import { collectAllWorkflows } from "./generate-workflow-source";

interface ActiveActivitySummary {
  activityName: string;
  stepName: string;
  workflowId: string;
  runId: string;
  activityId: string;
  attempt: number;
  startedAt: string;
  runningForMs: number;
}

interface ActiveActivityTracker {
  start: (activity: Omit<ActiveActivitySummary, "runningForMs">) => string;
  finish: (key: string) => void;
  snapshot: () => ActiveActivitySummary[];
  count: () => number;
}

interface EcsTaskMetadata {
  cluster?: string;
  taskArn?: string;
  family?: string;
  revision?: string;
  availabilityZone?: string;
  desiredStatus?: string;
  knownStatus?: string;
  containers: Array<{
    name?: string;
    image?: string;
    imageId?: string;
    desiredStatus?: string;
    knownStatus?: string;
    exitCode?: number;
  }>;
}

function createActiveActivityTracker(): ActiveActivityTracker {
  const activeActivities = new Map<string, Omit<ActiveActivitySummary, "runningForMs">>();

  return {
    start: (activity) => {
      const key = `${activity.workflowId}/${activity.runId}/${activity.activityId}/${activity.attempt}`;
      activeActivities.set(key, activity);
      return key;
    },
    finish: (key) => {
      activeActivities.delete(key);
    },
    snapshot: () => {
      const now = Date.now();
      return [...activeActivities.values()].map((activity) => ({
        ...activity,
        runningForMs: now - Date.parse(activity.startedAt),
      }));
    },
    count: () => activeActivities.size,
  };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object";
}

function optionalString(record: Record<string, unknown>, key: string): string | undefined {
  const value = record[key];
  return typeof value === "string" ? value : undefined;
}

function optionalNumber(record: Record<string, unknown>, key: string): number | undefined {
  const value = record[key];
  return typeof value === "number" ? value : undefined;
}

function parseEcsTaskMetadata(raw: unknown): EcsTaskMetadata | undefined {
  if (!isRecord(raw)) return undefined;

  const rawContainers = raw.Containers;
  const containers = Array.isArray(rawContainers)
    ? rawContainers.filter(isRecord).map((container) => ({
        name: optionalString(container, "Name"),
        image: optionalString(container, "Image"),
        imageId: optionalString(container, "ImageID"),
        desiredStatus: optionalString(container, "DesiredStatus"),
        knownStatus: optionalString(container, "KnownStatus"),
        exitCode: optionalNumber(container, "ExitCode"),
      }))
    : [];

  return {
    cluster: optionalString(raw, "Cluster"),
    taskArn: optionalString(raw, "TaskARN"),
    family: optionalString(raw, "Family"),
    revision: optionalString(raw, "Revision"),
    availabilityZone: optionalString(raw, "AvailabilityZone"),
    desiredStatus: optionalString(raw, "DesiredStatus"),
    knownStatus: optionalString(raw, "KnownStatus"),
    containers,
  };
}

async function fetchEcsTaskMetadata(logger: ComposerLogger): Promise<EcsTaskMetadata | undefined> {
  const metadataUri = process.env.ECS_CONTAINER_METADATA_URI_V4;
  if (!metadataUri) return undefined;

  try {
    const response = await fetch(`${metadataUri.replace(/\/$/, "")}/task`, {
      signal: AbortSignal.timeout(1_000),
    });
    if (!response.ok) {
      logger.debug("Failed to fetch ECS task metadata", { status: response.status });
      return undefined;
    }
    return parseEcsTaskMetadata(await response.json());
  } catch (error) {
    logger.debug("Failed to fetch ECS task metadata", {
      error: error instanceof Error ? error.message : String(error),
    });
    return undefined;
  }
}

/**
 * Converts a structured error (with code + parentCodes) to a Temporal ApplicationFailure,
 * preserving the error code as the `type` field which survives serialization.
 *
 * The error code and parentCodes are also stored in the details for full reconstruction.
 * We don't set `cause` to avoid message duplication - all info is in type/details.
 * With `cause` set we get an exact duplicate of the error in the cause chain.
 *
 * Compatible with errors that carry a stable string code and a parentCodes array
 * (the {code, parentCodes} convention used by typical error-class factories).
 */
function toCodedApplicationFailure(
  error: Error & { code: string; parentCodes: readonly string[] },
): ApplicationFailure {
  return ApplicationFailure.create({
    // No `cause` - avoids message duplication. All info preserved in type/details.
    type: error.code, // This is preserved through Temporal serialization!
    message: error.message,
    details: [
      {
        code: error.code,
        parentCodes: error.parentCodes,
        data: (error as { data?: unknown }).data,
        // Include stack trace in details since we're not using cause
        stack: error.stack,
      },
    ],
    nonRetryable: false,
  });
}

/**
 * Configuration for the Activity Worker.
 * All fields are required - callers must provide complete configuration.
 *
 * @typeParam TContext - The context type provided to steps
 */
export interface ActivityWorkerConfig<TContext = unknown> {
  /**
   * Temporal Server address (e.g., "localhost:7233" for local dev)
   */
  serverAddress: string;

  /**
   * Temporal namespace to connect to
   */
  namespace: string;

  /**
   * Deployment series name for Worker Versioning (e.g., "orders-service-activities").
   * Derived from the service name via getDeploymentSeriesNames().
   */
  deploymentSeriesName: string;

  /**
   * Build identifier for Worker Versioning (typically git commit SHA).
   * When set, workers register with this buildId for version-aware routing.
   * When unset, Worker Versioning is disabled.
   */
  buildId?: string;

  /**
   * Task queues to listen on for activity tasks.
   * Activities are routed to queues based on their workerProfile.
   */
  taskQueues: string[];

  /**
   * Maximum number of concurrent activity executions per queue
   */
  maxConcurrentActivityTaskExecutions: number;

  /**
   * Context provider for step execution.
   * If provided, beforeStep/afterStep hooks will be called for each activity.
   */
  contextProvider?: StepContextProvider<TContext>;

  /**
   * Workflow definitions to register.
   * Steps are extracted from each workflow's .steps array.
   * Error handlers and FanOut wrappers are derived from the workflows.
   */
  workflows: Workflow<any, any, any>[];

  /**
   * Logger for worker lifecycle and activity execution messages.
   * If not provided, defaults to the console-based defaultLogger.
   */
  logger?: ComposerLogger;
}

/**
 * Creates activity functions from workflow definitions.
 *
 * For step-based activities, each activity wraps the step's run function:
 * 1. Gets workflowId from Temporal's activity context (for logging)
 * 2. Creates context via beforeStep hook (if provider exists)
 * 3. Executes the step with context and its declared input data
 * 4. Cleans up via afterStep hook (if provider exists)
 * 5. Returns the provided fields
 *
 * For workflows with error handlers, creates wrapper activities that delegate
 * to the workflow's errorHandler function.
 *
 * For FanOut steps, creates mapInput and aggregateResults wrapper activities.
 */
function createActivitiesFromWorkflows<TContext>(
  workflows: Workflow<any, any, any>[],
  contextProvider: StepContextProvider<TContext> | undefined,
  logger: ComposerLogger,
  activeActivityTracker: ActiveActivityTracker,
  metricsHandle?: TaskQueueMetricsHandle,
): Record<string, (...args: unknown[]) => Promise<unknown>> {
  const activities: Record<string, (...args: unknown[]) => Promise<unknown>> = {};

  const allWorkflows = collectAllWorkflows(workflows);
  const allSteps: Step<any, any, any>[] = [];
  for (const workflow of allWorkflows) {
    const flattenedSteps = denamespaceSyntheticSteps(workflow.steps);
    for (const step of flattenedSteps) {
      if (!isFanOut(step)) {
        allSteps.push(step);
      }
    }
  }

  for (const step of allSteps) {
    const activityName = step.name;
    const stepName = step.name;

    // Signature: (workflowInput, stepInput) to match workflow-factory.ts
    activities[activityName] = async (_workflowInput: unknown, stepInput: unknown) => {
      const input = stepInput as Record<string, unknown>;
      // Get workflowId from Temporal's activity execution context for logging
      const activityInfo = ActivityContext.current().info;
      const workflowId = activityInfo.workflowExecution?.workflowId ?? "unknown";
      const runId = activityInfo.workflowExecution?.runId ?? "unknown";
      const activityId = activityInfo.activityId ?? "unknown";
      const attempt = activityInfo.attempt ?? 0;

      let ctx: TContext | undefined;
      let stepError: Error | undefined;
      const activeActivityKey = activeActivityTracker.start({
        activityName,
        stepName,
        workflowId,
        runId,
        activityId,
        attempt,
        startedAt: new Date().toISOString(),
      });
      metricsHandle?.activityStarted();

      try {
        logger.info("Activity execution started", {
          activityName,
          stepName,
          workflowId,
          inputKeys: Object.keys(input),
        });

        if (contextProvider) {
          ctx = await contextProvider.beforeStep(stepName);
        }

        const runtime: AsyncStepRuntime = {
          heartbeat: (details?: unknown) => ActivityContext.current().heartbeat(details),
          getHeartbeatDetails: <T = unknown>() =>
            ActivityContext.current().info.heartbeatDetails as T | undefined,
        };
        // Object.assign preserves context identity and prototype chain when ctx is
        // a class instance, so afterStep receives the same object the step mutated.
        // Falls back to spread when ctx is undefined (no context provider).
        const stepCtx =
          ctx != null && typeof ctx === "object" ? Object.assign(ctx, runtime) : runtime;

        const result = await step.run(stepCtx, input);
        logger.info("Activity execution completed", {
          activityName,
          stepName,
          workflowId,
          resultKeys: Object.keys(result),
        });
        return result;
      } catch (error) {
        stepError = error instanceof Error ? error : new Error(String(error));
        logger.error("Activity execution failed", {
          activityName,
          stepName,
          workflowId,
          error: stepError.message,
          code: isComposerError(error) ? error.code : undefined,
        });
        // Convert structured errors to ApplicationFailure to preserve the error code through serialization
        if (isComposerError(error)) {
          throw toCodedApplicationFailure(error);
        }
        throw error;
      } finally {
        metricsHandle?.activityFinished();

        // Call afterStep hook for cleanup if provider exists.
        // Wrapped in try/catch to prevent cleanup errors (e.g., EM flush failures)
        // from swallowing the original step error. The step's business logic error
        // is more important to surface than infrastructure cleanup errors.
        if (contextProvider && ctx !== undefined) {
          try {
            await contextProvider.afterStep(ctx, stepError);
          } catch (cleanupError) {
            logger.error("afterStep cleanup failed", {
              activityName,
              stepName,
              workflowId,
              cleanupError:
                cleanupError instanceof Error ? cleanupError.message : String(cleanupError),
              originalStepError: stepError?.message,
            });
          }
        }
        activeActivityTracker.finish(activeActivityKey);
      }
    };
  }

  for (const workflow of allWorkflows) {
    if (workflow.errorHandler) {
      const errorHandlerName = `${workflow.name}__errorHandler`;
      const handler = workflow.errorHandler;
      logger.debug("Registering error handler activity", { activityName: errorHandlerName });

      activities[errorHandlerName] = async (
        _workflowInput: unknown,
        bag: unknown,
        errorInfo: unknown,
      ) => {
        const typedBag = bag as Record<string, unknown>;
        const typedErrorInfo = errorInfo as {
          batchNumber: number;
          workflowId: string;
          errors: Array<{ stepName: string; message: string; code?: string; type: string }>;
        };

        const error = {
          code: "WORKFLOW_BATCH_ERROR",
          batchNumber: typedErrorInfo.batchNumber,
          workflowId: typedErrorInfo.workflowId,
          errors: typedErrorInfo.errors,
          message: `Workflow batch ${typedErrorInfo.batchNumber} failed: ${typedErrorInfo.errors.length} step(s) failed`,
        };

        const result = await (handler as (...args: unknown[]) => Promise<unknown>)(
          undefined,
          typedBag,
          error,
        );

        if (result === undefined) {
          return { handled: true, bag: typedBag };
        }
        const err = result as Error & { code?: string };
        return {
          handled: false,
          error: { message: err.message, code: err.code },
          bag: typedBag,
        };
      };
    }

    for (const step of workflow.steps) {
      if (!isFanOut(step)) continue;

      const fanOut = (step as Step<any, any, any> & { __fanOut: FanOutMetadata }).__fanOut;
      const mapInputName = `${step.name}__mapInput`;
      const aggName = `${step.name}__aggregateResults`;

      logger.debug("Registering FanOut activities", {
        mapInputName,
        aggregateResultsName: aggName,
      });

      activities[mapInputName] = async (_workflowInput: unknown, bagSlice: unknown) => {
        return fanOut.mapInput(bagSlice as Record<string, unknown>);
      };

      activities[aggName] = async (_workflowInput: unknown, results: unknown) => {
        return fanOut.aggregateResults(results as Record<string, unknown>[]);
      };
    }
  }

  return activities;
}

/**
 * Creates Temporal Activity Workers with automatic connection management.
 *
 * Extracts steps from the provided workflow definitions, creates activity
 * functions, and registers them with Temporal workers for the specified
 * task queues.
 *
 * @param config - Worker configuration
 * @returns Object containing workers array and the created connection
 */
export async function createActivityWorkers<TContext = unknown>(
  config: ActivityWorkerConfig<TContext>,
): Promise<{
  workers: Worker[];
  connection: NativeConnection;
  metricsHandle?: TaskQueueMetricsHandle;
  activeActivityTracker: ActiveActivityTracker;
}> {
  const logger = config.logger ?? defaultLogger;

  const workflowCount = config.workflows.length;
  logger.info("Creating Activity Workers", {
    server: config.serverAddress,
    namespace: config.namespace,
    taskQueues: config.taskQueues,
    workflowCount,
    maxConcurrentActivityTaskExecutions: config.maxConcurrentActivityTaskExecutions,
    hasContextProvider: !!config.contextProvider,
  });

  if (config.taskQueues.length === 0) {
    throw new Error("No task queues configured - cannot create workers");
  }

  const connection = await NativeConnection.connect({
    address: config.serverAddress,
  });
  logger.info("Connected to Temporal Server", { address: config.serverAddress });

  const metricsHandle = startTaskQueueMetrics({
    connection,
    taskQueues: config.taskQueues,
    temporalNamespace: config.namespace,
    logger,
  });

  const activeActivityTracker = createActiveActivityTracker();
  const activities = createActivitiesFromWorkflows(
    config.workflows,
    config.contextProvider,
    logger,
    activeActivityTracker,
    metricsHandle,
  );
  const activityCount = Object.keys(activities).length;
  logger.info("Activities created from workflows", { activityCount, workflowCount });

  // Create workers for each task queue
  // Each queue gets all activities registered (Temporal handles routing)
  // TODO: Add OpenTelemetry interceptor for trace context propagation in activities
  // Once @temporalio/interceptors-opentelemetry is installed, add:
  // import { OpenTelemetryActivityInboundInterceptor } from "@temporalio/interceptors-opentelemetry/lib/worker";
  // Then add to Worker.create options: interceptors: { activity: [(ctx) => ({ inbound: new OpenTelemetryActivityInboundInterceptor(ctx) })] }
  const workers = await Promise.all(
    config.taskQueues.map(async (taskQueue) => {
      const worker = await Worker.create({
        connection,
        namespace: config.namespace,
        taskQueue,
        activities,
        maxConcurrentActivityTaskExecutions: config.maxConcurrentActivityTaskExecutions,
        // Enable Build ID versioning for safe rainbow deploys when a git hash is available.
        // PINNED ensures workflows started on a specific version continue using that version.
        ...(config.buildId
          ? {
              workerDeploymentOptions: {
                useWorkerVersioning: true,
                version: {
                  deploymentName: config.deploymentSeriesName,
                  buildId: config.buildId,
                },
                defaultVersioningBehavior: VersioningBehavior.PINNED,
              },
            }
          : {}),
      });

      logger.info("Worker created for task queue", { taskQueue, namespace: config.namespace });
      return worker;
    }),
  );

  logger.info("Activity Workers ready", {
    taskQueues: config.taskQueues,
    activityCount,
    workflowCount,
    workerCount: workers.length,
  });

  return { workers, connection, metricsHandle, activeActivityTracker };
}

/**
 * Starts Activity Workers and runs them until interrupted.
 *
 * Creates a Temporal connection, initializes workers with loaded step implementations,
 * and handles graceful shutdown on SIGINT/SIGTERM. The connection is automatically
 * managed and closed on shutdown.
 *
 * @param config - Worker configuration
 *
 * @example
 * ```typescript
 * // Run activity workers (blocks until interrupted)
 * await runActivityWorkers({
 *   serverAddress: "localhost:7233",
 *   namespace: "default",
 *   taskQueues: ["workflow-tasks"],
 *   maxConcurrentActivityTaskExecutions: 10,
 *   workflows: [myWorkflow],
 * });
 * ```
 */
export async function runActivityWorkers<TContext = unknown>(
  config: ActivityWorkerConfig<TContext>,
): Promise<void> {
  const logger = config.logger ?? defaultLogger;

  // Create workers (connection is created internally)
  const { workers, connection, metricsHandle, activeActivityTracker } =
    await createActivityWorkers(config);
  const runPromises = workers.map((worker) => worker.run());
  const ecsTaskMetadata = await fetchEcsTaskMetadata(logger);
  logger.info("Activity Workers runtime diagnostics initialized", {
    pid: process.pid,
    uptimeMs: Math.round(process.uptime() * 1000),
    taskQueues: config.taskQueues,
    deploymentSeriesName: config.deploymentSeriesName,
    buildId: config.buildId,
    ecsTaskMetadata,
  });

  // Setup graceful shutdown for all workers
  let isShuttingDown = false;
  const shutdown = async (signal: NodeJS.Signals) => {
    if (isShuttingDown) return; // Prevent duplicate shutdown
    isShuttingDown = true;

    const shutdownEcsTaskMetadata = (await fetchEcsTaskMetadata(logger)) ?? ecsTaskMetadata;
    logger.info("Activity Workers shutdown signal received", {
      signal,
      pid: process.pid,
      uptimeMs: Math.round(process.uptime() * 1000),
      workerCount: workers.length,
      taskQueues: config.taskQueues,
      deploymentSeriesName: config.deploymentSeriesName,
      buildId: config.buildId,
      activeActivityCount: activeActivityTracker.count(),
      activeActivities: activeActivityTracker.snapshot(),
      ecsTaskMetadata: shutdownEcsTaskMetadata,
    });

    logger.info("Shutting down Activity Workers", { workerCount: workers.length });
    for (const worker of workers) {
      try {
        worker.shutdown();
      } catch (error) {
        logger.debug("Activity Worker shutdown request ignored", {
          error: error instanceof Error ? error.message : String(error),
        });
      }
    }
    logger.info("Activity Worker shutdown requested", {
      workerCount: workers.length,
      activeActivityCount: activeActivityTracker.count(),
    });

    await Promise.allSettled(runPromises);
    logger.info("Activity Worker run loops settled", {
      activeActivityCount: activeActivityTracker.count(),
    });
    await metricsHandle?.stop();
    logger.info("Activity Worker metrics stopped");
    await connection.close();
    logger.info("Activity Workers shutdown complete");
    // TODO: process.exit(0) here makes this function non-composable when multiple
    // worker types run in the same process (e.g., startAllWorkers). Consider
    // returning cleanly and letting the caller control process exit. Look for:
    // the Promise.all in startAllWorkers resolving naturally after shutdown,
    // rather than racing on process.exit(0). Not urgent since production
    // always uses separate containers and local dev handles this via Temporal
    // server-side task retries.
    process.exit(0);
  };

  process.on("SIGINT", () => void shutdown("SIGINT"));
  process.on("SIGTERM", () => void shutdown("SIGTERM"));

  // Run all workers in parallel
  try {
    await Promise.all(runPromises);
  } catch (error) {
    if (isShuttingDown) {
      return;
    }
    logger.error("Activity Workers error", {
      error: error instanceof Error ? error.message : String(error),
    });
    throw error;
  }
}
