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
import type { DurationString } from "../../dag-sync-step.js";
// Type-only, so nothing from the workflow module reaches the sandbox bundle.
import type {
  StageInitialVersioningBehavior,
  WorkflowVersioningBehavior,
} from "../../dag-sync-workflow.js";
import { durationToMs } from "../../duration.js";
import { safeErrorName } from "../../error-for-log.js";
import {
  SIGNAL_WAIT_TIMEOUT_CODE,
  STEP_NOT_READY_CODE,
  STEP_POLL_TIMEOUT_CODE,
} from "../../workflow-codes.js";

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

  /**
   * State carried from the previous run of a staged workflow. Set only on runs started by
   * continue-as-new at a stage boundary; never passed to activities.
   */
  continuation?: WorkflowContinuation;
}

/** Format version written into every continuation by this build. */
export const CONTINUATION_VERSION = 1;

/**
 * State a staged workflow carries across continue-as-new.
 *
 * This is a wire format between builds: the build that ends a run writes it and whichever
 * build is current reads it, possibly an older one after a rollback. Change it additively
 * only -- never remove, rename or repurpose a field -- and read every field tolerantly, as
 * a continuation written by another build may lack fields this one knows or carry fields it
 * does not. Bump `version` only for a change an older reader must be able to detect.
 */
export interface WorkflowContinuation {
  /** Format version of the build that wrote this continuation. Absent means 1. */
  version?: number;
  /** Name of the stage boundary the previous run ended at. */
  stage: string;
  /**
   * Bag fields carried forward. Initial data travels in `initialData`; a field here that
   * shares a name with initial data is a value the workflow changed, and wins.
   */
  bag?: Record<string, unknown>;
  /** Steps that already ran in earlier runs; never run again even if the plan moved them. */
  completedSteps?: string[];
  /** Signals received but not yet consumed by a wait, oldest first, by signal name. */
  signals?: Record<string, unknown[]>;
  /**
   * Signals already received with a dedupe key, oldest first. Each entry is the JSON
   * encoding of `[signalName, dedupeKey]`, so no name or key can collide with another pair.
   */
  seenSignalKeys?: string[];
}

/**
 * Serializable error info for cause chains.
 * Includes parentCodes to support LotiError subclass matching via extractIfPresent.
 */
export interface SerializedErrorInfo {
  code?: string;
  /** Parent error codes for subclass matching (e.g., NOT_FOUND -> DB_NOT_FOUND) */
  parentCodes?: readonly string[];
  type?: string;
  cause?: SerializedErrorInfo;
}

/**
 * Information about a step failure, passed to error handler activities.
 *
 * Carries no error message. This value is an activity argument and a member of the
 * `ApplicationFailure` details, so both copies land in Temporal event history at rest,
 * where only a `payloadCodec` could reach them. Codes and types classify a failure
 * without quoting whatever the driver put in the message.
 */
export interface StepFailureInfo {
  /** Step name that failed */
  stepName: string;
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
 *
 * A code is a documented application classifier here, so it may be preserved when it has the
 * identifier shape the error-matching contract guarantees. Unlike a name or message, arbitrary
 * prose cannot become one through this path. Default trace attributes have a stricter policy.
 */
function extractCodeFromMessage(message: string | undefined): string | undefined {
  if (!message) return undefined;
  const colonIndex = message.indexOf(": ");
  if (colonIndex <= 0) return undefined;
  const candidate = message.substring(0, colonIndex);
  return workflowErrorCode(candidate);
}

/** Code carried by the error a FanOut throws when any of its children fail. */
export const FANOUT_CHILD_FAILURE_CODE = "FANOUT_CHILD_FAILURE";

// ============================================================================
// Workflow-side polling
// ============================================================================

/** Fraction of each wait randomized, so a shared dependency is not polled in lockstep. */
const POLL_JITTER = 0.2;

type PollConfig = NonNullable<StepActivityConfig["poll"]>;


/**
 * True when a step signalled "not ready" rather than failing.
 *
 * Temporal wraps it as ActivityFailure -> ApplicationFailure with `type` set to the
 * error's code, so this reuses the same chain walk the failure classifier uses.
 */
function isStepNotReady(error: unknown): boolean {
  return codeOf(error) === STEP_NOT_READY_CODE;
}

/** The `reason` a StepNotReadyError carried, if it survived serialization. */
function notReadyReason(error: unknown): string | undefined {
  const failure = error as ApplicationFailureLike | undefined;
  // Temporal wraps the ApplicationFailure in an ActivityFailure, so look at both levels.
  for (const candidate of [failure, failure?.cause as ApplicationFailureLike | undefined]) {
    const data = (candidate?.details?.[0] as { data?: { reason?: unknown } } | undefined)?.data;
    if (typeof data?.reason === "string") return data.reason;
  }
  return undefined;
}

/**
 * Runs `invoke` until it stops signalling "not ready", sleeping on a Temporal timer
 * between attempts.
 *
 * Each attempt is an ordinary activity invocation that ends, so the worker releases its
 * slot for the whole wait. Elapsed time accumulates the intervals actually slept rather
 * than reading a clock, and the jitter draws from Temporal's seeded PRNG, so replay is
 * deterministic.
 */
function validatePollConfig(stepName: string, poll: PollConfig): void {
  const coefficient = poll.backoffCoefficient ?? 2;
  if (!Number.isFinite(coefficient) || coefficient < 1) {
    throw new Error(
      `Step "${stepName}": asyncPoll.backoffCoefficient must be a finite number >= 1, got ${coefficient}`,
    );
  }
  for (const field of ["initialInterval", "maximumInterval", "timeout"] as const) {
    const value = durationToMs(poll[field]);
    if (!Number.isFinite(value) || value <= 0) {
      throw new Error(`Step "${stepName}": asyncPoll.${field} must be a positive duration`);
    }
  }
}

async function runWithPolling<T>(
  stepName: string,
  poll: PollConfig,
  invoke: () => Promise<T>,
): Promise<T> {
  validatePollConfig(stepName, poll);

  const maximumIntervalMs = durationToMs(poll.maximumInterval);
  const coefficient = poll.backoffCoefficient ?? 2;
  // Date.now() is the workflow clock inside the sandbox: monotonic across replay and
  // identical on every replay, so a deadline built from it stays deterministic while
  // counting the time activities spend running, not just the time we spend sleeping.
  const startedAtMs = Date.now();
  const deadlineMs = startedAtMs + durationToMs(poll.timeout);

  let intervalMs = durationToMs(poll.initialInterval);
  let attempts = 0;

  for (;;) {
    try {
      return await invoke();
    } catch (error) {
      if (!isStepNotReady(error)) throw error;
      attempts++;

      const jitterFactor = 1 + (Math.random() * 2 - 1) * POLL_JITTER;
      const cappedMs = Math.min(intervalMs, maximumIntervalMs);
      const delayMs = Math.max(1, Math.round(cappedMs * jitterFactor));
      const elapsedMs = Date.now() - startedAtMs;

      // Checked before sleeping, so `timeout` is a real ceiling rather than one the
      // final wait is allowed to overshoot.
      if (Date.now() + delayMs > deadlineMs) {
        throw wf.ApplicationFailure.create({
          type: STEP_POLL_TIMEOUT_CODE,
          message: `Step "${stepName}" still not ready after ${attempts} polls over ${elapsedMs}ms`,
          details: [{ code: STEP_POLL_TIMEOUT_CODE, data: { stepName, attempts, elapsedMs } }],
          nonRetryable: true,
        });
      }

      wf.log.debug("Step not ready; waiting before re-invoking", {
        stepName,
        attempts,
        delayMs,
        elapsedMs,
        reason: notReadyReason(error),
      });

      await wf.sleep(delayMs);
      intervalMs = Math.min(intervalMs * coefficient, maximumIntervalMs);
    }
  }
}

// ============================================================================
// Durable signal waits
// ============================================================================

/**
 * Unconsumed payloads kept per signal name when no wait sets `maxPendingSignals`. The
 * inbox is carried across every continue-as-new, so a queue of duplicate or unwanted
 * signals would otherwise grow each run's input without limit.
 */
export const DEFAULT_MAX_PENDING_SIGNALS = 100;

/**
 * Dedupe keys remembered across runs. A key older than this many later keys can be
 * accepted again, so the window must outlast any plausible retry of the same delivery.
 */
const MAX_SEEN_SIGNAL_KEYS = 1000;

/** Delivery metadata `signalAsyncWorkflow` sends as the signal's second argument. */
export interface SignalDeliveryMeta {
  /** Caller-chosen key; a second signal with the same name and key is dropped. */
  dedupeKey?: string;
}

/**
 * Received, unconsumed signals, carried across continue-as-new.
 *
 * Accepts only the signal names some wait in this workflow declares (any stage, not just
 * the current one), so a stray or hostile sender cannot grow the carried state. Queues a
 * previous run carried in are kept even when this build no longer declares them, so a
 * rollback to a build that still waits on them loses nothing.
 */
interface SignalInbox {
  queues: Map<string, unknown[]>;
  /** Maximum pending payloads per declared signal name. */
  caps: Map<string, number>;
  seenKeys: string[];
  seenKeySet: Set<string>;
}

function createSignalInbox(plan: WorkflowPlan, continuation?: WorkflowContinuation): SignalInbox {
  const caps = new Map<string, number>();
  for (const batch of plan.batches) {
    for (const wait of batch.signalWaits ?? []) {
      const cap = wait.maxPendingSignals ?? DEFAULT_MAX_PENDING_SIGNALS;
      caps.set(wait.signalName, Math.max(cap, caps.get(wait.signalName) ?? 0));
    }
  }
  const seenKeys = [...(continuation?.seenSignalKeys ?? [])];
  // A build may lower a signal's cap while runs carry longer queues; the newest payloads
  // within the current cap are kept. Undeclared queues are carried as they are.
  return {
    queues: new Map(
      Object.entries(continuation?.signals ?? {}).map(([name, queue]) => {
        const cap = caps.get(name);
        return [name, cap === undefined ? [...queue] : queue.slice(-cap)];
      }),
    ),
    caps,
    seenKeys,
    seenKeySet: new Set(seenKeys),
  };
}

function receiveSignal(
  inbox: SignalInbox,
  signalName: string,
  payload: unknown,
  meta: SignalDeliveryMeta | undefined,
): void {
  const cap = inbox.caps.get(signalName);
  if (cap === undefined) {
    wf.log.warn("Dropped a signal no wait in this workflow declares", { signalName });
    return;
  }

  const dedupeKey =
    typeof meta?.dedupeKey === "string" ? JSON.stringify([signalName, meta.dedupeKey]) : undefined;
  if (dedupeKey !== undefined) {
    if (inbox.seenKeySet.has(dedupeKey)) {
      wf.log.info("Dropped a duplicate signal", { signalName, dedupeKey: meta!.dedupeKey });
      return;
    }
    inbox.seenKeys.push(dedupeKey);
    inbox.seenKeySet.add(dedupeKey);
    if (inbox.seenKeys.length > MAX_SEEN_SIGNAL_KEYS) {
      inbox.seenKeySet.delete(inbox.seenKeys.shift()!);
    }
  }

  const queue = inbox.queues.get(signalName) ?? [];
  queue.push(payload);
  if (queue.length > cap) {
    queue.splice(0, queue.length - cap);
    wf.log.warn("Signal inbox full; dropped the oldest pending payload", {
      signalName,
      limit: cap,
    });
  }
  inbox.queues.set(signalName, queue);
}

/**
 * Waits for the next payload of `wait.signalName` and returns the node's output.
 *
 * The wait is a workflow condition, optionally bounded by a durable timer, so it costs no
 * worker slot however long it lasts. Two waits on the same signal can wake together; the
 * loop lets the one that finds the queue already drained go back to waiting.
 */
/** Ends a wait its batch no longer needs: a sibling failed, so the batch is finishing. */
class SignalWaitReleased extends Error {}

async function runSignalWait(
  wait: SignalWaitBatchEntry,
  inbox: SignalInbox,
  released: () => boolean,
  deadlines: Map<string, number>,
): Promise<Record<string, unknown>> {
  const hasPending = () => (inbox.queues.get(wait.signalName)?.length ?? 0) > 0;
  const wake = () => hasPending() || released();
  const timeoutMs = wait.timeout === undefined ? undefined : durationToMs(wait.timeout);
  if (timeoutMs !== undefined && (!Number.isFinite(timeoutMs) || timeoutMs <= 0)) {
    // `waitForSignal` rejects this where the wait is defined; a hand-built plan can still
    // carry it. Fail the node, not the workflow task, so it reaches onError rather than
    // retrying forever.
    throw wf.ApplicationFailure.create({
      type: "COMPOSER_INVALID_SIGNAL_WAIT",
      message: `Signal wait "${wait.name}": timeout must be a positive duration, got ${wait.timeout}`,
      details: [{ code: "COMPOSER_INVALID_SIGNAL_WAIT", data: { stepName: wait.name } }],
      nonRetryable: true,
    });
  }
  const deadlineMs =
    timeoutMs === undefined ? undefined : (deadlines.get(wait.name) ?? Date.now() + timeoutMs);
  if (deadlineMs !== undefined) deadlines.set(wait.name, deadlineMs);

  for (;;) {
    if (hasPending()) {
      return { [wait.provides]: inbox.queues.get(wait.signalName)!.shift() };
    }
    if (released()) throw new SignalWaitReleased();
    if (deadlineMs === undefined) {
      await wf.condition(wake);
      continue;
    }
    const remainingMs = deadlineMs - Date.now();
    if (remainingMs > 0 && (await wf.condition(wake, remainingMs))) continue;

    if (wait.hasTimeoutValue) {
      wf.log.info("Signal wait timed out; using its timeout value", {
        stepName: wait.name,
        signalName: wait.signalName,
      });
      return { [wait.provides]: wait.timeoutValue };
    }
    throw wf.ApplicationFailure.create({
      type: SIGNAL_WAIT_TIMEOUT_CODE,
      message: `Signal wait "${wait.name}" got no "${wait.signalName}" signal within ${wait.timeout}`,
      details: [
        {
          code: SIGNAL_WAIT_TIMEOUT_CODE,
          data: { stepName: wait.name, signalName: wait.signalName, timeout: wait.timeout },
        },
      ],
      nonRetryable: true,
    });
  }
}

/**
 * Settles a batch. Like `Promise.allSettled`, except that a wait does not hold the batch
 * open once it has failed: when any entry rejects, the batch still lets its steps finish,
 * as it always has, then releases the waits still blocked. Without this, a failure beside
 * a wait with no timeout would never be handled, and the run would hang.
 *
 * Released waits are returned as `undefined`: they neither succeeded nor failed.
 */
async function settleBatch<T>(
  promises: Promise<T>[],
  isWait: boolean[],
  releaseWaits: () => void,
): Promise<Array<PromiseSettledResult<T> | undefined>> {
  let markFailed!: () => void;
  const failed = new Promise<void>((resolve) => {
    markFailed = resolve;
  });
  const settled = promises.map((promise) =>
    promise.then(
      (value): PromiseSettledResult<T> | undefined => ({ status: "fulfilled", value }),
      (reason: unknown): PromiseSettledResult<T> | undefined => {
        if (reason instanceof SignalWaitReleased) return undefined;
        markFailed();
        return { status: "rejected", reason };
      },
    ),
  );
  const all = Promise.all(settled);
  const stepsDone = Promise.all(settled.filter((_, i) => !isWait[i]));

  await Promise.race([all, failed.then(() => stepsDone)]);
  // A no-op when everything has settled; otherwise wakes the waits so they end.
  releaseWaits();
  return all;
}

const WORKFLOW_ERROR_CODE = /^[A-Z0-9_]{2,50}$/;

function workflowErrorCode(value: unknown): string | undefined {
  return typeof value === "string" && WORKFLOW_ERROR_CODE.test(value) ? value : undefined;
}

/** A thrown value's classifier code, walking Temporal's wrapper chain when necessary. */
function codeOf(value: unknown, depth = 0): string | undefined {
  if (value === null || typeof value !== "object" || depth >= 5) return undefined;
  const error = value as { code?: unknown; type?: unknown; cause?: unknown };
  return (
    workflowErrorCode(error.code) ??
    codeOf(error.cause, depth + 1) ??
    workflowErrorCode(error.type)
  );
}

function firstSafeCode(...values: unknown[]): string | undefined {
  for (const value of values) {
    const code = workflowErrorCode(value);
    if (code !== undefined) return code;
  }
  return undefined;
}

function safeParentCodes(values: readonly string[] | undefined): readonly string[] | undefined {
  if (!Array.isArray(values)) return undefined;
  const codes = values?.flatMap((value) => {
    const code = workflowErrorCode(value);
    return code === undefined ? [] : [code];
  });
  return codes && codes.length > 0 ? codes : undefined;
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
  const lotiDetails = extractLotiDetails(cause);
  // The message is read only to recover a `CODE: message` prefix; it is not carried forward.
  const code = firstSafeCode(
    cause.code,
    cause.type,
    lotiDetails?.code,
    extractCodeFromMessage(cause.message),
  );
  return {
    code,
    parentCodes: safeParentCodes(lotiDetails?.parentCodes),
    type: safeErrorName(cause.name),
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
  const code = firstSafeCode(
    error?.code,
    causeError?.type,
    lotiDetails?.code,
    extractCodeFromMessage(causeError?.message ?? error?.message),
  );

  return {
    stepName,
    code,
    parentCodes: safeParentCodes(lotiDetails?.parentCodes),
    type: safeErrorName(error?.name),
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
    /**
     * Error codes that must not be retried, matched against
     * `ApplicationFailure.type`. Mirrors `StepRetryPolicy.nonRetryableErrorTypes`.
     */
    nonRetryableErrorTypes?: string[];
  };
  /** Workflow-side poll policy. Mirrors `StepPollPolicy`. */
  poll?: {
    initialInterval: DurationString;
    maximumInterval: DurationString;
    timeout: DurationString;
    backoffCoefficient?: number;
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

  /** Signal waits in this batch (wait in parallel alongside steps) */
  signalWaits?: SignalWaitBatchEntry[];
}

/**
 * A durable wait in a batch: blocks until the named signal arrives, then provides its
 * payload as a single bag field.
 */
export interface SignalWaitBatchEntry {
  /** Wait node name */
  name: string;
  /** Signal to consume */
  signalName: string;
  /** The one bag field the payload is written to */
  provides: string;
  /** Bound on the wait; unset means no limit */
  timeout?: DurationString;
  /** Field value on timeout, when `hasTimeoutValue` */
  timeoutValue?: unknown;
  /** Without a timeout value, a timeout fails the node */
  hasTimeoutValue: boolean;
  /** Pending payloads of this signal kept for later waits; defaults to 100 */
  maxPendingSignals?: number;
}

/**
 * A stage boundary in the plan: after `afterBatch`, the run continues as new.
 */
export interface StageBoundaryPlanEntry {
  /** Stage name the next run resumes at */
  name: string;
  /** 0-indexed batch the boundary follows */
  afterBatch: number;
  /** Versioning behavior of the next run's first task; absent on unversioned workers */
  initialVersioningBehavior?: StageInitialVersioningBehavior;
  /** Earlier names of this stage; a continuation carrying one resumes here */
  aliases?: string[];
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

  /** Stage boundaries, in batch order, at which the run continues as new. */
  stageBoundaries?: StageBoundaryPlanEntry[];

  /**
   * Worker Versioning behavior declared by the workflow, applied with
   * `setWorkflowOptions`. Unset leaves the worker default in force.
   */
  versioningBehavior?: WorkflowVersioningBehavior;
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
  const temporalWorkflow = async function temporalWorkflow(
    input: WorkflowInput,
  ): Promise<TemporalWorkflowResult> {
    const workflowId = wf.workflowInfo().workflowId;
    const continuation = input.continuation;

    wf.log.info("Starting workflow execution", {
      workflowId: wf.workflowInfo().workflowId,
      workflowName: plan.name,
      initialDataKeys: Object.keys(input.initialData),
      ...(continuation && { resumingStage: continuation.stage }),
    });

    // Activities get the input without the continuation: the carried bag is already split
    // into each step's own inputs, and repeating it would bloat every activity payload.
    const { continuation: _continuation, ...activityInput } = input;

    if (continuation && (continuation.version ?? 1) > CONTINUATION_VERSION) {
      // Fields are only ever added, so reading what this build knows is still sound.
      wf.log.warn("Resuming from a continuation written by a newer build", {
        version: continuation.version,
        supported: CONTINUATION_VERSION,
      });
    }

    // Initialize bag with initial data, plus whatever an earlier run carried forward
    const bag: Record<string, any> = { ...input.initialData, ...continuation?.bag };

    // Track which checkpoints have been reached
    const reachedCheckpoints = new Set<string>();
    // Checkpoints an earlier run of this workflow reached; their snapshots are gone.
    const checkpointsPassedEarlier = new Set<string>();

    // A resumed run starts after the stage boundary its predecessor ended at. The stage is
    // found by name, which is stable across builds; batch indices are not.
    let firstBatchIndex = 0;
    const completedSteps = new Set<string>(continuation?.completedSteps ?? []);
    if (continuation) {
      const stage = plan.stageBoundaries?.find(
        (b) => b.name === continuation.stage || (b.aliases?.includes(continuation.stage) ?? false),
      );
      if (!stage) {
        // Thrown as a plain Error, this fails the workflow task rather than the workflow,
        // so Temporal retries it until a build that knows the stage picks it up.
        const known = plan.stageBoundaries?.map((b) => b.name).join(", ") ?? "none";
        wf.log.error("Cannot resume at unknown stage", {
          workflowName: plan.name,
          stage: continuation.stage,
          knownStages: known,
        });
        throw new Error(
          `Workflow "${plan.name}" cannot resume at unknown stage "${continuation.stage}". ` +
            `Known stages: [${known}]`,
        );
      }
      firstBatchIndex = stage.afterBatch + 1;
      // Checkpoints all precede the first boundary, so a resumed run has passed them. Its bag
      // is the stage's, not the checkpoint's -- it may hold later outputs -- so a late
      // awaitCheckpoint is refused at once rather than blocking forever or answering with
      // the wrong snapshot.
      for (const checkpoint of plan.checkpoints ?? []) {
        if (checkpoint.afterBatch <= stage.afterBatch) checkpointsPassedEarlier.add(checkpoint.name);
      }
    }

    // Staged or waiting workflows take signals into an inbox that outlives the run. The
    // handler catches every name so an undeclared one is dropped visibly, not left buffered
    // in the SDK to vanish at continue-as-new.
    const usesSignals =
      (plan.stageBoundaries?.length ?? 0) > 0 ||
      plan.batches.some((batch) => (batch.signalWaits?.length ?? 0) > 0);
    const inbox = createSignalInbox(plan, continuation);

    // Shown by Temporal UI and describe, so an operator can see where a long-lived run is
    // without decoding its input. A stage is named by the boundary that begins it.
    const waitingOn = new Set<string>();
    const showDetails = () => {
      if (!usesSignals) return;
      const stage = continuation?.stage ?? "(first stage)";
      const waits = [...waitingOn].map((name) => `"${name}"`).join(", ");
      wf.setCurrentDetails(`Stage: ${stage}${waits ? `\nWaiting for signal: ${waits}` : ""}`);
    };
    showDetails();

    if (usesSignals) {
      wf.setDefaultSignalHandler((signalName, ...args) => {
        receiveSignal(inbox, signalName, args[0], args[1] as SignalDeliveryMeta | undefined);
      });
    }

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

        if (checkpointsPassedEarlier.has(checkpointName)) {
          throw wf.ApplicationFailure.create({
            type: "COMPOSER_CHECKPOINT_PASSED",
            message:
              `Checkpoint "${checkpointName}" of workflow "${plan.name}" was reached by an ` +
              `earlier run, which has since continued as new; its snapshot is no longer available`,
            nonRetryable: true,
          });
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

    // A wait keeps the deadline it started with when a handled failure makes it wait again.
    const waitDeadlines = new Map<string, number>();

    // Execute batches sequentially
    for (let batchIndex = firstBatchIndex; batchIndex < plan.batches.length; batchIndex++) {
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
          // A polling step's "not ready" signal must not be retried by Temporal, or the
          // step would burn its own attempts before control returns to the workflow loop.
          // Its asyncRetry budget then still covers genuine transient failures.
          const nonRetryableErrorTypes = config?.poll
            ? [...(config.retry?.nonRetryableErrorTypes ?? []), STEP_NOT_READY_CODE]
            : config?.retry?.nonRetryableErrorTypes;
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
              // Omitted entirely when unset or empty, so steps that don't use it
              // get the same retry policy they got before this field existed.
              ...(nonRetryableErrorTypes &&
                nonRetryableErrorTypes.length > 0 && { nonRetryableErrorTypes }),
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

      // A wait released by a handled failure has not run. It is waited on again, against its
      // original deadline, before the batch is done, so nothing downstream reads its field
      // unset. Each later round re-runs only those waits.
      let rerunWaits: Set<string> | undefined;
      for (;;) {
        // Build all promises for this batch (steps + fanOuts execute in parallel)
        type BatchResult = { stepName: string; output: Record<string, any> };
        const allPromises: Array<Promise<BatchResult>> = [];
        const allNames: string[] = [];

        // Step promises
        for (const step of batch.steps) {
          // An earlier run already ran it; a plan change may have moved it past the boundary.
          if (rerunWaits || completedSteps.has(step.name)) continue;
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

              const poll = step.activityConfig?.poll;
              const stepOutput = poll
                ? await runWithPolling(step.name, poll, () => activityFn(activityInput, stepInput))
                : await activityFn(activityInput, stepInput);

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
          if (rerunWaits || completedSteps.has(fanOut.name)) continue;
          allNames.push(fanOut.name);
          allPromises.push(
            (async (): Promise<BatchResult> => {
              const output = await executeFanOutInWorkflow(
                fanOut,
                bag,
                activityInput,
                workflowId,
                activityProxiesByKey,
                getProxyKey,
              );
              return { stepName: fanOut.name, output };
            })(),
          );
        }

        // Signal wait promises
        let waitsReleased = false;
        const isWait = allPromises.map(() => false);
        for (const wait of batch.signalWaits ?? []) {
          if (completedSteps.has(wait.name) || (rerunWaits && !rerunWaits.has(wait.name))) continue;
          allNames.push(wait.name);
          isWait.push(true);
          allPromises.push(
            (async (): Promise<BatchResult> => {
              waitingOn.add(wait.signalName);
              showDetails();
              try {
                const output = await runSignalWait(
                  wait,
                  inbox,
                  () => waitsReleased,
                  waitDeadlines,
                );
                return { stepName: wait.name, output };
              } finally {
                waitingOn.delete(wait.signalName);
                showDetails();
              }
            })(),
          );
        }

        // Execute all in parallel; a failure releases any waits still blocked
        const settledResults = await settleBatch(allPromises, isWait, () => {
          waitsReleased = true;
        });

        // Separate fulfilled and rejected results; a released wait is neither
        const fulfilled: Array<BatchResult> = [];
        const rejected: Array<{ stepName: string; error: unknown }> = [];
        const released: string[] = [];

        for (let i = 0; i < settledResults.length; i++) {
          const result = settledResults[i];
          if (result === undefined) {
            released.push(allNames[i]!);
          } else if (result.status === "fulfilled") {
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

        // Everything that ran, failed or not, is done: a resumed run must not repeat it. A
        // released wait consumed nothing, so it is not done.
        for (const name of allNames) {
          if (!released.includes(name)) completedSteps.add(name);
        }
        if (released.length > 0) {
          wf.log.warn("Released signal waits because their batch failed", {
            workflowId,
            batchNumber,
            releasedWaits: released,
          });
        }

        // Mark any checkpoints that trigger after this batch completes
        // This happens regardless of whether some steps in the batch failed,
        // allowing clients to receive partial results if they requested a checkpoint.
        // While a released wait is still to run again the batch is not complete, so marking
        // waits for that round -- or for the workflow failing without it.
        const markBatchCheckpoints = () => {
          for (const checkpoint of plan.checkpoints ?? []) {
            if (checkpoint.afterBatch !== batchIndex || reachedCheckpoints.has(checkpoint.name)) {
              continue;
            }
            wf.log.debug("Marking checkpoint as reached", {
              workflowId,
              checkpointName: checkpoint.name,
              batchIndex,
              batchNumber,
            });
            reachedCheckpoints.add(checkpoint.name);
          }
        };
        if (released.length === 0) markBatchCheckpoints();

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

                const handlerResult = await errorHandlerFn(activityInput, bag, batchErrorInfo);

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
                    code: workflowErrorCode(handlerResult.error.code),
                    type: "TransformedError",
                    batchNumber,
                  };
                }
                // If neither, keep the original batch error
              } catch (handlerError) {
                // Name and code only: `wf.log` crosses the sandbox boundary by structured
                // clone, so an Error cannot reach the logger's serializer from here, and a
                // flattened message is the raw driver text with nothing left to redact it.
                wf.log.error("Error handler activity failed", {
                  workflowId,
                  activityName: plan.errorHandlerActivityName,
                  errorName: handlerError instanceof Error ? safeErrorName(handlerError.name) : typeof handlerError,
                  errorCode: codeOf(handlerError),
                });
                // Handler crashed - wrap both errors
                resultError = {
                  message: `Error handler failed while handling batch ${batchNumber} [${failedStepNames}]`,
                  code: "WORKFLOW_ERROR_HANDLER_FAILURE",
                  type: "WorkflowErrorHandlerFailure",
                  batchNumber,
                  errors: batchErrorInfo.errors,
                };
              }
            }
          }

          if (resultError) {
            // Failing without the released waits, so the batch is as complete as it gets.
            markBatchCheckpoints();
            // Unhandled error: throw ApplicationFailure to mark workflow as Failed.
            // The bag is a details entry of its own, not a key on the error entry: it is every
            // value the workflow touched, it goes to Temporal event history at rest, and a
            // scrubbing failure converter has to be able to drop it without rebuilding the
            // error alongside it. Callers extracting partial state read details[1].bag.
            throw wf.ApplicationFailure.create({
              message: resultError.message,
              type: resultError.type ?? "WorkflowError",
              nonRetryable: true,
              details: [{ error: resultError }, { bag }],
            });
          }
          // Error was handled - continue processing remaining batches
        }
        if (released.length === 0) break;
        rerunWaits = new Set(released);
        wf.log.info("Waiting again on signal waits released by a handled failure", {
          workflowId,
          batchNumber,
          waits: released,
        });
      }

      const boundary = plan.stageBoundaries?.find((b) => b.afterBatch === batchIndex);
      if (boundary) {
        await continueAtStage(boundary, input, bag, completedSteps, inbox);
      }
    }

    wf.log.info("Workflow execution completed successfully", {
      workflowId,
      outputKeys: Object.keys(bag),
    });

    return { bag, error: undefined };
  };

  // Declared on the function itself so Temporal reads it before the first task completes.
  if (plan.versioningBehavior) {
    wf.setWorkflowOptions({ versioningBehavior: plan.versioningBehavior }, temporalWorkflow);
  }

  return temporalWorkflow;
}

/**
 * Ends the run at a stage boundary and continues as new, carrying the bag, the steps that
 * already ran, and any unconsumed signals into the next run.
 *
 * Waits for in-flight signal and update handlers first, so none is cut off mid-run.
 */
async function continueAtStage(
  boundary: StageBoundaryPlanEntry,
  input: WorkflowInput,
  bag: Record<string, any>,
  completedSteps: Set<string>,
  inbox: SignalInbox,
): Promise<never> {
  await wf.condition(wf.allHandlersFinished);

  // Initial data travels in `initialData`; the continuation holds produced fields, plus any
  // initial field the workflow changed (an error handler can), which would otherwise revert.
  const carried: Record<string, unknown> = {};
  for (const [field, value] of Object.entries(bag)) {
    if (field in input.initialData) {
      if (value !== input.initialData[field]) carried[field] = value;
      continue;
    }
    carried[field] = value;
  }

  const signals: Record<string, unknown[]> = {};
  for (const [name, queue] of inbox.queues) {
    if (queue.length > 0) signals[name] = [...queue];
  }

  wf.log.info("Stage complete; continuing as new", {
    workflowId: wf.workflowInfo().workflowId,
    stage: boundary.name,
    carriedKeys: Object.keys(carried),
    initialVersioningBehavior: boundary.initialVersioningBehavior,
  });

  const { continuation: _previous, ...rest } = input;
  const next: WorkflowInput = {
    ...rest,
    continuation: {
      version: CONTINUATION_VERSION,
      stage: boundary.name,
      bag: carried,
      completedSteps: [...completedSteps],
      signals,
      ...(inbox.seenKeys.length > 0 && { seenSignalKeys: [...inbox.seenKeys] }),
    },
  };
  return wf.makeContinueAsNewFunc<(input: WorkflowInput) => Promise<TemporalWorkflowResult>>(
    boundary.initialVersioningBehavior
      ? { initialVersioningBehavior: boundary.initialVersioningBehavior }
      : {},
  )(next);
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
    // Identify each failed child by index and code, never by message. This string is both
    // logged and inlined into the thrown Error, which becomes a step failure and so reaches
    // Temporal event history -- a child's raw message would be at rest in two places.
    const failureDetails = failures
      .map((f) => {
        const code = codeOf(f.error);
        return code === undefined ? `child ${f.index}` : `child ${f.index}: ${code}`;
      })
      .join("; ");

    wf.log.error("FanOut: child workflow failures", {
      fanOutName: fanOut.name,
      failedCount: failures.length,
      totalCount: childInputs.length,
      failures: failureDetails,
    });

    throw Object.assign(
      new Error(
        `FanOut "${fanOut.name}": ${failures.length} of ${childInputs.length} child workflow(s) failed [${failureDetails}]`,
      ),
      // The message is dropped before this reaches event history, so the code is what is
      // left to classify on. Without it, a code would be guessed from the message prefix.
      { code: FANOUT_CHILD_FAILURE_CODE },
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
