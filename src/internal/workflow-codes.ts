/**
 * Classifier codes for the workflow-side poll loop, signal waits, checkpoints and stage
 * boundaries.
 *
 * Kept in a leaf module with no imports: the workflow factory runs in Temporal's
 * deterministic sandbox and cannot reach the Node-side error classes, while step
 * code cannot import the workflow factory. Both sides share these constants.
 *
 * @module workflow-codes
 */

/** Thrown by a step to mean "not done yet"; the workflow sleeps and re-invokes it. */
export const STEP_NOT_READY_CODE = "COMPOSER_STEP_NOT_READY";

/** Thrown by the workflow when a step's `asyncPoll.timeout` is exhausted. */
export const STEP_POLL_TIMEOUT_CODE = "COMPOSER_STEP_POLL_TIMEOUT";

/** Thrown by the workflow when a `waitForSignal` timeout elapses with no signal and no fallback. */
export const SIGNAL_WAIT_TIMEOUT_CODE = "COMPOSER_SIGNAL_WAIT_TIMEOUT";

/**
 * Thrown by the workflow when the state a staged workflow carries into its next run is too
 * large to continue as new. Temporal caps one payload at 2 MB by default, and a command over
 * it fails its workflow task on every retry, so the workflow fails clearly instead.
 */
export const CONTINUATION_TOO_LARGE_CODE = "COMPOSER_CONTINUATION_TOO_LARGE";

/** A signal wait the workflow cannot run: a non-positive timeout, or a wait not alone. */
export const INVALID_SIGNAL_WAIT_CODE = "COMPOSER_INVALID_SIGNAL_WAIT";

/** `awaitCheckpoint` named a checkpoint the running build's workflow does not declare. */
export const UNKNOWN_CHECKPOINT_CODE = "COMPOSER_UNKNOWN_CHECKPOINT";

/** `awaitCheckpoint` named a checkpoint an earlier run reached before continuing as new. */
export const CHECKPOINT_PASSED_CODE = "COMPOSER_CHECKPOINT_PASSED";

/** `awaitCheckpoint` gave up waiting after the checkpoint's timeout. */
export const CHECKPOINT_TIMEOUT_CODE = "COMPOSER_CHECKPOINT_TIMEOUT";
