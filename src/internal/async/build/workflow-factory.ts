/**
 * Async Workflow Factory - Creates Temporal workflow functions from pre-computed plans.
 *
 * This factory ONLY uses @temporalio/workflow APIs to ensure deterministic execution.
 * It is bundled by Temporal's workflow bundler and runs in an isolated sandbox.
 * It CANNOT import Node.js APIs or external dependencies (no fs, no network,
 * no logging libraries) -- only @temporalio/workflow primitives.
 *
 * ## Factory Pattern:
 * - Input: WorkflowPlan (build-time generated, contains batches + activity mappings)
 * - Output: Temporal workflow function (deterministic orchestration logic)
 * - Result: Worker registers these functions, Temporal invokes them at runtime
 *
 * All business logic runs in activities, not in the workflow itself.
 * The workflow only orchestrates: batch sequencing, parallel step execution, data flow.
 *
 * @module workflow-factory
 */

import * as wf from "@temporalio/workflow";
import type { DurationString } from "../../dag-sync-step";

/**
 * Input structure for our generated Temporal workflows.
 */
export interface WorkflowInput {
  /** Initial data for the workflow bag */
  initialData: Record<string, any>;

  /** Additional metadata */
  metadata?: Record<string, string>;

  /** Deployment environment for activities that need environment-aware behavior */
  environment?: string;
}

/**
 * Serializable error info for cause chains.
 * Includes parentCodes to support LotiError subclass matching via extractIfPresent.
 */
export interface SerializedErrorInfo {
  message: string;
  code?: string;
  /** Parent error codes for subclass matching (e.g., NOT_FOUND -> DB_NOT_FOUND) */
  parentCodes?: readonly string[];
  type?: string;
  cause?: SerializedErrorInfo;
}

/**
 * Information about a step failure, passed to error handler activities.
 */
export interface StepFailureInfo {
  /** Step name that failed */
  stepName: string;
  /** Error message (from outermost error wrapper) */
  message: string;
  /** Error code if available */
  code?: string;
  /** Parent error codes for subclass matching */
  parentCodes?: readonly string[];
  /** Error type/name */
  type: string;
  /** Nested cause chain (Temporal wraps errors in ActivityFailure -> ApplicationFailure) */
  cause?: SerializedErrorInfo;
}

/**
 * Information about a batch failure, passed to error handler activities.
 */
export interface BatchErrorInfo {
  /** Batch number (1-indexed) */
  batchNumber: number;
  /** Workflow ID */
  workflowId: string;
  /** All step failures in the batch */
  errors: StepFailureInfo[];
}

// ============================================================================
// Error Extraction Helpers
// ============================================================================
// These help extract LotiError info from Temporal's error wrappers.
// Temporal wraps activity errors: ActivityFailure -> ApplicationFailure -> original error
// We store LotiError code/parentCodes in ApplicationFailure.type and .details

/** Error with optional cause chain */
type ErrorWithCause = Error & { code?: string; cause?: Error };

/** ApplicationFailure-like structure with our LotiError details */
interface ApplicationFailureLike extends ErrorWithCause {
  type?: string;
  details?: Array<{ code?: string; parentCodes?: readonly string[]; data?: unknown }>;
}

/**
 * LotiError formats messages as "CODE: message" - extract code from message if not available elsewhere.
 */
function extractCodeFromMessage(message: string | undefined): string | undefined {
  if (!message) return undefined;
  const colonIndex = message.indexOf(": ");
  if (colonIndex > 0 && colonIndex < 50) {
    return message.substring(0, colonIndex);
  }
  return undefined;
}

/**
 * Extract LotiError metadata from ApplicationFailure.details[0].
 * We store { code, parentCodes, data } there when converting LotiErrors.
 */
function extractLotiDetails(
  err: ApplicationFailureLike | undefined,
): { code?: string; parentCodes?: readonly string[] } | undefined {
  if (!err?.details || !Array.isArray(err.details) || err.details.length === 0) {
    return undefined;
  }
  const detail = err.details[0];
  if (detail && typeof detail === "object" && "code" in detail) {
    return detail;
  }
  return undefined;
}

/**
 * Recursively build the cause chain from a Temporal error.
 * ApplicationFailure stores error code in `type` field.
 */
function buildCauseChain(
  err: ErrorWithCause | undefined,
  depth: number,
): SerializedErrorInfo | undefined {
  if (!err?.cause || depth >= 5) return undefined;
  const cause = err.cause as ApplicationFailureLike;
  const message = cause.message ?? String(cause);
  const lotiDetails = extractLotiDetails(cause);
  const code = cause.code ?? cause.type ?? lotiDetails?.code ?? extractCodeFromMessage(message);
  return {
    message,
    code,
    parentCodes: lotiDetails?.parentCodes,
    type: cause.name,
    cause: buildCauseChain(cause, depth + 1),
  };
}

/**
 * Build StepFailureInfo from a Temporal activity error.
 * Extracts LotiError code/parentCodes from the ApplicationFailure wrapper.
 */
function buildStepFailureInfo(stepName: string, rawError: unknown): StepFailureInfo {
  const error = rawError as ApplicationFailureLike;
  // ActivityFailure wraps our ApplicationFailure in cause
  const causeError = error?.cause as ApplicationFailureLike | undefined;
  const lotiDetails = extractLotiDetails(causeError) ?? extractLotiDetails(error);
  const code =
    error?.code ??
    causeError?.type ??
    lotiDetails?.code ??
    extractCodeFromMessage(causeError?.message ?? error?.message);

  return {
    stepName,
    message: causeError?.message ?? error?.message ?? String(rawError),
    code,
    parentCodes: lotiDetails?.parentCodes,
    type: error?.name ?? "Error",
    cause: buildCauseChain(error, 0),
  };
}

/**
 * A FanOut entry in a batch. Orchestrates dynamic child workflow parallelism:
 * mapInput activity -> lane-based child workflow execution -> aggregateResults activity.
 */
export interface FanOutBatchEntry {
  /** FanOut step name */
  name: string;

  /** Versioned Temporal workflow name for the child workflow */
  childWorkflowName: string;

  /** Activity that calls the user's mapInput function */
  mapInputActivityName: string;

  /** Activity that calls the user's aggregateResults function */
  aggregateResultsActivityName: string;

  /** Fields this FanOut needs from the bag (passed to mapInput) */
  needs: string[];

  /** Fields this FanOut provides to the bag (returned by aggregateResults) */
  provides: string[];

  /** Maximum concurrent child workflows (null = unbounded, from Infinity via JSON round-trip) */
  concurrency: number | null;

  /** Task queue for mapInput/aggregateResults activities */
  taskQueue: string;

  /** Per-step activity configuration (timeouts, retries) */
  activityConfig?: StepActivityConfig;
}

/**
 * Per-step activity configuration, threaded from step definitions through the plan.
 * All fields are optional; the workflow factory falls back to defaults when absent.
 */
export interface StepActivityConfig {
  startToCloseTimeout?: DurationString;
  heartbeatTimeout?: DurationString;
  retry?: {
    maximumAttempts?: number;
    backoffCoefficient?: number;
    initialInterval?: DurationString;
    maximumInterval?: DurationString;
  };
}

/**
 * Represents a batch of steps that can execute in parallel.
 */
export interface StepBatch {
  /** Steps in this batch (all can execute in parallel) */
  steps: Array<{
    /** Step name (used for activity lookup) */
    name: string;

    /** Activity name (includes hash for versioning) */
    activityName: string;

    /** Fields this step needs from the bag */
    needs: string[];

    /** Fields this step provides to the bag */
    provides: string[];

    /** Task queue to execute on (based on worker profile) */
    taskQueue: string;

    /** Per-step activity configuration (timeouts, retries) */
    activityConfig?: StepActivityConfig;
  }>;

  /** FanOut entries in this batch (execute in parallel alongside steps) */
  fanOuts?: FanOutBatchEntry[];
}

/**
 * Workflow execution plan - defines the order and parallelism of step execution.
 */
export interface WorkflowPlan {
  /** Name of the workflow */
  name: string;

  /** Batches of steps (each batch executes sequentially, steps within batch run in parallel) */
  batches: StepBatch[];

  /**
   * Optional error handler activity name.
   * If provided, this activity will be called when a batch fails.
   * The activity receives (workflowInput, bag, errorInfo) where errorInfo
   * contains details about the batch failure.
   */
  errorHandlerActivityName?: string;

  /**
   * Task queue for the error handler activity.
   * Defaults to 'workflow-tasks' if not specified.
   */
  errorHandlerTaskQueue?: string;

  /**
   * Checkpoints for early return in async workflows.
   * Each checkpoint fires after its designated batch completes.
   */
  checkpoints?: Array<{
    /** Unique checkpoint name (used by client to await) */
    name: string;
    /** 0-indexed batch number that triggers this checkpoint */
    afterBatch: number;
    /** Timeout in milliseconds for awaiting this checkpoint */
    timeout: number;
  }>;
}

/**
 * Result type for Temporal workflow execution.
 * Mirrors the sync runtime's WorkflowResult type.
 */
export interface TemporalWorkflowResult {
  /** The workflow bag containing all step outputs and initial data */
  bag: Record<string, any>;
  /** Error if workflow failed, undefined if successful */
  error?: {
    /** Error message */
    message: string;
    /** Error code if available */
    code?: string;
    /** Error type/name */
    type: string;
    /** Batch number where the error occurred */
    batchNumber?: number;
    /** Individual step/child failures when this is a batch or fanOut error */
    errors?: StepFailureInfo[];
  };
}

/**
 * Arguments for the awaitCheckpoint update.
 */
export interface AwaitCheckpointArgs {
  /** Name of the checkpoint to wait for */
  checkpointName: string;
}

/**
 * Temporal Update definition for awaiting checkpoints.
 * Clients call this to wait for a checkpoint and receive the partial bag state.
 *
 * The update handler blocks until the requested checkpoint is reached,
 * then returns a snapshot of the bag at that point.
 *
 * Import this for type-safe executeUpdate calls from client code.
 */
export const awaitCheckpointUpdate = wf.defineUpdate<
  Record<string, unknown>, // Return type: bag state snapshot
  [AwaitCheckpointArgs] // Args: checkpoint name
>("awaitCheckpoint");

/**
 * Factory function that creates a Temporal workflow function from a pre-computed plan.
 *
 * This factory creates deterministic orchestration logic using only Temporal workflow APIs.
 * All actual business logic runs in activities - the workflow only coordinates execution.
 *
 * The workflow returns { bag, error? } instead of throwing, matching the sync runtime.
 *
 * @param plan - The workflow execution plan (generated at build time)
 * @returns A Temporal workflow function ready for worker registration
 */
export function createWorkflowFunction(plan: WorkflowPlan) {
  return async function temporalWorkflow(input: WorkflowInput): Promise<TemporalWorkflowResult> {
    const workflowId = wf.workflowInfo().workflowId;

    wf.log.info("Starting workflow execution", {
      workflowId: wf.workflowInfo().workflowId,
      workflowName: plan.name,
      initialDataKeys: Object.keys(input.initialData),
    });

    // Initialize bag with initial data
    const bag: Record<string, any> = { ...input.initialData };

    // Track which checkpoints have been reached
    const reachedCheckpoints = new Set<string>();

    // Set up checkpoint update handler if workflow has checkpoints defined
    // The handler blocks until the requested checkpoint is reached, then returns the bag state
    if (plan.checkpoints && plan.checkpoints.length > 0) {
      wf.setHandler(awaitCheckpointUpdate, async ({ checkpointName }) => {
        // Validate the checkpoint exists in this workflow's plan
        const checkpoint = plan.checkpoints?.find((c) => c.name === checkpointName);
        if (!checkpoint) {
          const validNames = plan.checkpoints?.map((c) => c.name).join(", ") ?? "none";
          throw new Error(
            `Unknown checkpoint "${checkpointName}" for workflow "${plan.name}". ` +
              `Valid checkpoints: [${validNames}]`,
          );
        }

        wf.log.debug("Awaiting checkpoint", {
          workflowId,
          checkpointName,
          alreadyReached: reachedCheckpoints.has(checkpointName),
        });

        // Wait until this checkpoint is reached (or already has been)
        await wf.condition(() => reachedCheckpoints.has(checkpointName));

        wf.log.debug("Checkpoint reached, returning bag snapshot", {
          workflowId,
          checkpointName,
          bagKeys: Object.keys(bag),
        });

        // Return a snapshot of the bag at this checkpoint
        return { ...bag };
      });
    }

    // Execute batches sequentially
    for (let batchIndex = 0; batchIndex < plan.batches.length; batchIndex++) {
      const batch = plan.batches[batchIndex]!;
      const batchNumber = batchIndex + 1;

      wf.log.debug(`Executing batch ${batchNumber}/${plan.batches.length}`, {
        batchSteps: batch.steps.map((s) => s.name),
      });

      // Create activity proxies once per unique (taskQueue + activityConfig) combination.
      // proxyActivities applies timeout/retry config to all activities on a proxy,
      // so steps with different configs need separate proxies.
      const activityProxiesByKey = new Map<string, any>();
      const getProxyKey = (taskQueue: string, config?: StepActivityConfig): string => {
        if (!config) return taskQueue;
        return `${taskQueue}|${JSON.stringify(config)}`;
      };
      const ensureActivityProxy = (taskQueue: string, config?: StepActivityConfig) => {
        const key = getProxyKey(taskQueue, config);
        if (!activityProxiesByKey.has(key)) {
          const activities = wf.proxyActivities<{
            [key: string]: (...args: any[]) => Promise<any>;
          }>({
            taskQueue,
            startToCloseTimeout: config?.startToCloseTimeout ?? "5 minutes",
            heartbeatTimeout: config?.heartbeatTimeout,
            retry: {
              maximumAttempts: config?.retry?.maximumAttempts ?? 3,
              backoffCoefficient: config?.retry?.backoffCoefficient ?? 2,
              initialInterval: config?.retry?.initialInterval ?? "1s",
              maximumInterval: config?.retry?.maximumInterval ?? "60s",
            },
          });
          activityProxiesByKey.set(key, activities);
        }
      };
      for (const step of batch.steps) {
        ensureActivityProxy(step.taskQueue, step.activityConfig);
      }
      for (const fanOut of batch.fanOuts ?? []) {
        ensureActivityProxy(fanOut.taskQueue, fanOut.activityConfig);
      }

      // Build all promises for this batch (steps + fanOuts execute in parallel)
      type BatchResult = { stepName: string; output: Record<string, any> };
      const allPromises: Array<Promise<BatchResult>> = [];
      const allNames: string[] = [];

      // Step promises
      for (const step of batch.steps) {
        allNames.push(step.name);
        allPromises.push(
          (async (): Promise<BatchResult> => {
            const stepInput: Record<string, any> = {};
            for (const neededField of step.needs) {
              stepInput[neededField] = bag[neededField];
            }

            wf.log.debug(`Executing step activity`, {
              stepName: step.name,
              activityName: step.activityName,
              taskQueue: step.taskQueue,
              needs: step.needs,
              provides: step.provides,
            });

            const proxyKey = getProxyKey(step.taskQueue, step.activityConfig);
            const activities = activityProxiesByKey.get(proxyKey)!;
            const activityFn = activities[step.activityName];
            if (!activityFn) {
              throw new Error(`Activity not found: ${step.activityName}`);
            }

            const stepOutput = await activityFn(input, stepInput);

            wf.log.debug(`Step completed`, {
              stepName: step.name,
              outputKeys: Object.keys(stepOutput),
            });

            return { stepName: step.name, output: stepOutput };
          })(),
        );
      }

      // FanOut promises
      for (const fanOut of batch.fanOuts ?? []) {
        allNames.push(fanOut.name);
        allPromises.push(
          (async (): Promise<BatchResult> => {
            const output = await executeFanOutInWorkflow(
              fanOut,
              bag,
              input,
              workflowId,
              activityProxiesByKey,
              getProxyKey,
            );
            return { stepName: fanOut.name, output };
          })(),
        );
      }

      // Execute all in parallel using allSettled
      const settledResults = await Promise.allSettled(allPromises);

      // Separate fulfilled and rejected results
      const fulfilled: Array<BatchResult> = [];
      const rejected: Array<{ stepName: string; error: unknown }> = [];

      for (let i = 0; i < settledResults.length; i++) {
        const result = settledResults[i]!;
        if (result.status === "fulfilled") {
          fulfilled.push(result.value);
        } else {
          rejected.push({ stepName: allNames[i]!, error: result.reason });
        }
      }

      // First, merge successful step outputs into the bag
      // This ensures the bag has all available data even if some steps failed
      for (const { output } of fulfilled) {
        Object.assign(bag, output);
      }

      // Mark any checkpoints that trigger after this batch completes
      // This happens regardless of whether some steps in the batch failed,
      // allowing clients to receive partial results if they requested a checkpoint
      for (const checkpoint of plan.checkpoints ?? []) {
        if (checkpoint.afterBatch === batchIndex) {
          wf.log.debug("Marking checkpoint as reached", {
            workflowId,
            checkpointName: checkpoint.name,
            batchIndex,
            batchNumber,
          });
          reachedCheckpoints.add(checkpoint.name);
        }
      }

      // If any steps failed, handle the batch error
      if (rejected.length > 0) {
        const failedStepNames = rejected.map((r) => r.stepName).join(", ");

        wf.log.error("Batch execution failed", {
          workflowId,
          batchNumber,
          failedSteps: failedStepNames,
          successfulSteps: fulfilled.map((f) => f.stepName),
          errorCount: rejected.length,
        });

        // Build error info for the error handler
        const batchErrorInfo: BatchErrorInfo = {
          batchNumber,
          workflowId,
          errors: rejected.map((r) => buildStepFailureInfo(r.stepName, r.error)),
        };

        // Log the batch error for observability
        // Structured logging backends (OTEL, etc.) will serialize this correctly
        // Terminal may show [Object] for nested objects - that's a display limitation
        wf.log.warn("Workflow batch failed", {
          workflowId: wf.workflowInfo().workflowId,
          batchNumber,
          failedSteps: failedStepNames,
          errors: batchErrorInfo.errors,
        });

        // Call error handler activity if configured
        // The error handler activity returns Error | undefined:
        // - undefined = error was handled, no error to propagate
        // - Error = error to propagate (possibly transformed)
        let resultError: TemporalWorkflowResult["error"] = {
          message: `Batch ${batchNumber} failed: ${rejected.length} step(s) failed [${failedStepNames}]`,
          code: "WORKFLOW_BATCH_ERROR",
          type: "WorkflowBatchError",
          batchNumber,
          errors: batchErrorInfo.errors,
        };

        if (plan.errorHandlerActivityName) {
          // Default to standard-tasks (same queue as other activities)
          const errorHandlerQueue = plan.errorHandlerTaskQueue ?? "standard-tasks";
          const errorHandlerActivities = wf.proxyActivities<{
            [key: string]: (
              workflowInput: WorkflowInput,
              bag: Record<string, any>,
              errorInfo: BatchErrorInfo,
            ) => Promise<{
              handled: boolean;
              error?: { message: string; code?: string };
              bag?: Record<string, any>;
            }>;
          }>({
            taskQueue: errorHandlerQueue,
            startToCloseTimeout: "1 minute",
            retry: {
              maximumAttempts: 1, // Don't retry error handler
            },
          });

          const errorHandlerFn = errorHandlerActivities[plan.errorHandlerActivityName];
          if (errorHandlerFn) {
            try {
              wf.log.info("Invoking error handler activity", {
                workflowId,
                activityName: plan.errorHandlerActivityName,
                batchNumber,
              });

              const handlerResult = await errorHandlerFn(input, bag, batchErrorInfo);

              if (handlerResult.bag) {
                Object.assign(bag, handlerResult.bag);
              }

              wf.log.info("Error handler activity completed", {
                workflowId,
                activityName: plan.errorHandlerActivityName,
                handled: handlerResult.handled,
              });

              if (handlerResult.handled) {
                // Error was fully handled - no error to propagate
                resultError = undefined;
              } else if (handlerResult.error) {
                // Error was transformed
                resultError = {
                  message: handlerResult.error.message,
                  code: handlerResult.error.code,
                  type: "TransformedError",
                  batchNumber,
                };
              }
              // If neither, keep the original batch error
            } catch (handlerError) {
              wf.log.error("Error handler activity failed", {
                workflowId,
                activityName: plan.errorHandlerActivityName,
                error: handlerError instanceof Error ? handlerError.message : String(handlerError),
              });
              // Handler crashed - wrap both errors
              resultError = {
                message: `Error handler failed: ${handlerError instanceof Error ? handlerError.message : String(handlerError)} (original: ${failedStepNames})`,
                code: "WORKFLOW_ERROR_HANDLER_FAILURE",
                type: "WorkflowErrorHandlerFailure",
                batchNumber,
                errors: batchErrorInfo.errors,
              };
            }
          }
        }

        if (resultError) {
          // Unhandled error: throw ApplicationFailure to mark workflow as Failed.
          // Include bag in details so callers can extract partial workflow state.
          throw wf.ApplicationFailure.create({
            message: resultError.message,
            type: resultError.type ?? "WorkflowError",
            nonRetryable: true,
            details: [{ bag, error: resultError }],
          });
        }
        // Error was handled - continue processing remaining batches
      }
    }

    wf.log.info("Workflow execution completed successfully", {
      workflowId,
      outputKeys: Object.keys(bag),
    });

    return { bag, error: undefined };
  };
}

/**
 * Executes a FanOut entry within a Temporal workflow using child workflows.
 *
 * 1. Calls mapInput activity to produce child workflow inputs from the bag
 * 2. Spawns min(concurrency, N) async lanes that start child workflows
 *    via wf.executeChild with deterministic IDs ({parentId}/{fanOutName}/{index})
 * 3. Collects all results (allSettled semantics via try/catch per child)
 * 4. If all succeed, calls aggregateResults activity to merge results
 * 5. If any fail, throws an error describing which children failed
 */
async function executeFanOutInWorkflow(
  fanOut: FanOutBatchEntry,
  bag: Record<string, any>,
  input: WorkflowInput,
  parentWorkflowId: string,
  activityProxiesByKey: Map<string, any>,
  getProxyKeyFn: (taskQueue: string, config?: StepActivityConfig) => string,
): Promise<Record<string, any>> {
  const proxyKey = getProxyKeyFn(fanOut.taskQueue, fanOut.activityConfig);
  const activities = activityProxiesByKey.get(proxyKey)!;

  // Build bag slice with only the fields mapInput needs
  const bagSlice: Record<string, any> = {};
  for (const field of fanOut.needs) {
    bagSlice[field] = bag[field];
  }

  wf.log.info("FanOut: calling mapInput", {
    fanOutName: fanOut.name,
    childWorkflow: fanOut.childWorkflowName,
    needs: fanOut.needs,
  });

  // Call mapInput activity to produce child workflow inputs
  const mapInputFn = activities[fanOut.mapInputActivityName];
  if (!mapInputFn) {
    throw new Error(`Activity not found: ${fanOut.mapInputActivityName}`);
  }
  const childInputs: Record<string, any>[] = await mapInputFn(input, bagSlice);

  // Handle empty input array
  if (childInputs.length === 0) {
    wf.log.info("FanOut: empty input array, calling aggregateResults with []", {
      fanOutName: fanOut.name,
    });
    const aggFn = activities[fanOut.aggregateResultsActivityName];
    if (!aggFn) {
      throw new Error(`Activity not found: ${fanOut.aggregateResultsActivityName}`);
    }
    return await aggFn(input, []);
  }

  wf.log.info("FanOut: starting child workflows", {
    fanOutName: fanOut.name,
    childCount: childInputs.length,
    concurrency: fanOut.concurrency,
    childWorkflow: fanOut.childWorkflowName,
  });

  // Lane-based child workflow execution
  // concurrency may be null after JSON round-trip (Infinity -> null via JSON.stringify)
  const effectiveConcurrency = fanOut.concurrency ?? childInputs.length;
  const laneCount = Math.min(effectiveConcurrency, childInputs.length);
  const childResults: ({ bag: Record<string, any> } | { error: unknown })[] = new Array(
    childInputs.length,
  );
  let nextIndex = 0;

  const lane = async () => {
    while (true) {
      const index = nextIndex++;
      if (index >= childInputs.length) break;

      const childWorkflowId = `${parentWorkflowId}/${fanOut.name}/${index}`;
      try {
        const childResult = (await wf.executeChild(fanOut.childWorkflowName, {
          workflowId: childWorkflowId,
          args: [{ initialData: childInputs[index]! } satisfies WorkflowInput],
        })) as TemporalWorkflowResult;

        childResults[index] = { bag: childResult.bag };
      } catch (err) {
        // Child workflows throw ApplicationFailure for unhandled errors.
        // Extract the structured error from ChildWorkflowFailure -> ApplicationFailure.
        let errorDetail: unknown = err;
        if (err instanceof wf.ChildWorkflowFailure && err.cause instanceof wf.ApplicationFailure) {
          const detail = err.cause.details?.[0] as
            | { error?: TemporalWorkflowResult["error"] }
            | undefined;
          if (detail?.error) {
            errorDetail = detail.error;
          }
        }
        childResults[index] = { error: errorDetail };
      }
    }
  };

  await Promise.all(Array.from({ length: laneCount }, () => lane()));

  // Separate successes from failures
  const successBags: Record<string, any>[] = [];
  const failures: Array<{ index: number; error: unknown }> = [];

  for (let i = 0; i < childResults.length; i++) {
    const result = childResults[i]!;
    if ("error" in result) {
      failures.push({ index: i, error: result.error });
    } else {
      successBags.push(result.bag);
    }
  }

  if (failures.length > 0) {
    const failureDetails = failures
      .map((f) => {
        const err = f.error as Record<string, unknown> | Error | undefined;
        const msg =
          err instanceof Error
            ? err.message
            : typeof err === "object" && err !== null && typeof err.message === "string"
              ? err.message
              : String(err);
        return `child ${f.index}: ${msg}`;
      })
      .join("; ");

    wf.log.error("FanOut: child workflow failures", {
      fanOutName: fanOut.name,
      failedCount: failures.length,
      totalCount: childInputs.length,
      failures: failureDetails,
    });

    throw new Error(
      `FanOut "${fanOut.name}": ${failures.length} of ${childInputs.length} child workflow(s) failed [${failureDetails}]`,
    );
  }

  wf.log.info("FanOut: all children completed, calling aggregateResults", {
    fanOutName: fanOut.name,
    successCount: successBags.length,
  });

  // Call aggregateResults activity
  const aggFn = activities[fanOut.aggregateResultsActivityName];
  if (!aggFn) {
    throw new Error(`Activity not found: ${fanOut.aggregateResultsActivityName}`);
  }
  return await aggFn(input, successBags);
}
