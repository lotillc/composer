/**
 * Temporal Workflow Worker
 *
 * Registers and executes workflow orchestration logic in a deterministic sandbox.
 * Accepts workflow definitions declaratively, generates workflow plans, writes a
 * temporary source file, and lets Temporal's internal Webpack bundle it for the
 * V8 sandbox.
 *
 * ## Workflow Worker Responsibilities:
 * - Connect to Temporal Server
 * - Generate WorkflowPlan objects from provided workflow definitions
 * - Write a temp CJS file exporting workflow functions via createWorkflowFunction
 * - Pass the temp file to Temporal's Webpack via workflowsPath
 * - Execute workflow coordination (batch sequencing, parallel execution)
 * - Schedule activities to appropriate task queues
 *
 * ## What Workflows CANNOT Do:
 * - Access Node.js APIs (fs, http, process, etc.)
 * - Use non-deterministic functions (Date.now(), Math.random(), crypto.randomBytes())
 * - Make external API calls or database queries
 * - Import business logic (all business logic runs in activities)
 *
 * @module workflow-worker
 */

import { dirname, resolve } from "node:path";
import { VersioningBehavior } from "@temporalio/common";
import { NativeConnection, Worker } from "@temporalio/worker";
import type { Workflow } from "../../dag-sync-workflow";
import { defaultLogger } from "../../defaults";
import type { ComposerLogger } from "../../types";
import { writeWorkflowSourceFile } from "./generate-workflow-source";

/**
 * Resolve the node_modules directory containing @temporalio/workflow.
 *
 * Temporal's workflow Worker uses webpack internally to bundle workflow code
 * into a deterministic sandbox. Webpack needs to resolve `@temporalio/workflow`
 * at bundle time, but pnpm's strict node_modules structure may not hoist it
 * to the consumer's top-level node_modules. Since @lotiai/composer lists
 * @temporalio/workflow as a direct dependency, require.resolve finds it from
 * here. We pass this directory to webpack via bundlerOptions so it can resolve
 * the package regardless of the consumer's package manager layout.
 */
function getTemporalWorkflowNodeModulesDir(): string {
  const temporalWorkflowPkg = require.resolve("@temporalio/workflow/package.json");
  // Go up from .../node_modules/@temporalio/workflow/package.json
  // to .../node_modules (scoped package = 2 levels above the package dir)
  return resolve(dirname(temporalWorkflowPkg), "..", "..");
}

/**
 * Configuration for the Workflow Worker.
 */
export interface WorkflowWorkerConfig {
  /**
   * Temporal Server address (e.g., "localhost:7233" for local dev)
   * @default "localhost:7233"
   */
  serverAddress?: string;

  /**
   * Temporal namespace to connect to
   * @default "default"
   */
  namespace?: string;

  /**
   * Task queue(s) to listen on for workflow tasks.
   * All workflows are registered to the same queue(s).
   * @default ["workflow-tasks"]
   */
  taskQueues?: string[];

  /**
   * Workflow definitions to register.
   * Plans are generated from these at startup.
   */
  workflows: Workflow<any, any, any>[];

  /**
   * Maximum number of concurrent workflow executions
   * @default 100
   */
  maxConcurrentWorkflowTaskExecutions?: number;

  /**
   * Deployment series name for Worker Versioning (e.g., "orders-service-workflows").
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
   * Logger for worker lifecycle messages.
   * If not provided, defaults to the console-based defaultLogger.
   */
  logger?: ComposerLogger;
}

/**
 * Default configuration for local development.
 */
const DEFAULT_CONFIG = {
  serverAddress: "localhost:7233",
  namespace: "default",
  taskQueues: ["workflow-tasks"],
  maxConcurrentWorkflowTaskExecutions: 100,
} as const;

/**
 * Creates Temporal Workflow Workers with automatic connection management.
 *
 * At startup:
 * 1. Generates WorkflowPlan objects from provided workflow definitions
 * 2. Writes a temp CJS file with createWorkflowFunction calls
 * 3. Passes the temp file to Temporal's Webpack via workflowsPath
 * 4. Creates worker instances for the specified task queues
 *
 * @param config - Worker configuration
 * @returns Object containing workers array and the created connection
 *
 * @example
 * ```typescript
 * const { workers, connection } = await createWorkflowWorkers({
 *   workflows: [myWorkflow],
 *   taskQueues: ["workflow-tasks"],
 * });
 *
 * // Run workers
 * await Promise.all(workers.map(w => w.run()));
 *
 * // Cleanup
 * await Promise.all(workers.map(w => w.shutdown()));
 * await connection.close();
 * ```
 */
export async function createWorkflowWorkers(
  config: WorkflowWorkerConfig,
): Promise<{ workers: Worker[]; connection: NativeConnection }> {
  const logger = config.logger ?? defaultLogger;
  const finalConfig = { ...DEFAULT_CONFIG, ...config };

  const workflowCount = config.workflows.length;
  logger.info("Creating Workflow Workers", {
    server: finalConfig.serverAddress,
    namespace: finalConfig.namespace,
    taskQueues: finalConfig.taskQueues,
    workflowCount,
  });

  const workflowsPath = await writeWorkflowSourceFile(config.workflows);

  logger.info("Workflow source file generated", { workflowsPath });

  const connection = await NativeConnection.connect({
    address: finalConfig.serverAddress,
  });

  // TODO: This webpackConfigHook may no longer be necessary. It was added when
  // the esbuild bundle lived in the consumer's dist/ and webpack resolved
  // @temporalio/workflow from there (where it's not a direct dep). Now that
  // workflow-factory.js is loaded via absolute path from @lotiai/composer (which
  // has @temporalio/workflow as a direct dep), webpack should resolve it
  // normally. Try removing the hook + getTemporalWorkflowNodeModulesDir and
  // verify the workflow worker still starts.
  const temporalNodeModulesDir = getTemporalWorkflowNodeModulesDir();

  // Create workers for each task queue
  // Note: Temporal requires one Worker per task queue, but they all load the same workflows
  const workers = await Promise.all(
    finalConfig.taskQueues.map(async (taskQueue) => {
      const worker = await Worker.create({
        connection,
        namespace: finalConfig.namespace,
        taskQueue,
        workflowsPath,
        maxConcurrentWorkflowTaskExecutions: finalConfig.maxConcurrentWorkflowTaskExecutions,
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
        bundlerOptions: {
          // Tell webpack where to find @temporalio/workflow. Without this, pnpm's
          // strict node_modules layout prevents webpack from resolving it when the
          // workflow source file lives in the consumer's dist/ directory.
          webpackConfigHook: (config) => {
            config.resolve = config.resolve ?? {};
            const modules = config.resolve.modules ?? [];
            if (!modules.includes(temporalNodeModulesDir)) {
              modules.push(temporalNodeModulesDir);
            }
            config.resolve.modules = modules;
            return config;
          },
        },
      });

      logger.info("Worker created for task queue", { taskQueue, namespace: finalConfig.namespace });
      return worker;
    }),
  );

  // Return all workers (caller must run them all)
  if (workers.length === 0) {
    throw new Error("No task queues configured - cannot create workers");
  }

  logger.info("Workflow Workers ready", {
    taskQueues: finalConfig.taskQueues,
    workerCount: workers.length,
    workflowCount,
  });

  return { workers, connection };
}

/**
 * Starts Workflow Workers and runs them until interrupted.
 *
 * Creates a Temporal connection, initializes workers from provided workflows, and
 * handles graceful shutdown on SIGINT/SIGTERM. The connection is automatically
 * managed and closed on shutdown.
 *
 * @param config - Worker configuration
 *
 * @example
 * ```typescript
 * // Run workflow workers (blocks until interrupted)
 * await runWorkflowWorkers({
 *   workflows: [myWorkflow],
 *   taskQueues: ["workflow-tasks"],
 * });
 * ```
 */
export async function runWorkflowWorkers(config: WorkflowWorkerConfig): Promise<void> {
  const logger = config.logger ?? defaultLogger;

  const { workers, connection } = await createWorkflowWorkers(config);

  // Setup graceful shutdown for all workers
  let isShuttingDown = false;
  const shutdown = async () => {
    if (isShuttingDown) return; // Prevent duplicate shutdown
    isShuttingDown = true;

    logger.info("Shutting down Workflow Workers", { workerCount: workers.length });
    await Promise.all(workers.map((worker) => worker.shutdown()));
    await connection.close();
    logger.info("Workflow Workers shutdown complete");
    // TODO: process.exit(0) here makes this function non-composable when multiple
    // worker types run in the same process (e.g., startAllWorkers). Consider
    // returning cleanly and letting the caller control process exit. Look for:
    // the Promise.all in startAllWorkers resolving naturally after shutdown,
    // rather than racing on process.exit(0). Not urgent since production
    // always uses separate containers and local dev handles this via Temporal
    // server-side task retries.
    process.exit(0);
  };

  process.on("SIGINT", () => void shutdown());
  process.on("SIGTERM", () => void shutdown());

  // Run all workers in parallel
  try {
    await Promise.all(workers.map((worker) => worker.run()));
  } catch (error) {
    logger.error("Workflow Workers error", {
      error: error instanceof Error ? error.message : String(error),
    });
    throw error;
  }
}
