/**
 * Classifier codes for the workflow-side poll loop.
 *
 * Kept in a leaf module with no imports: the workflow factory runs in Temporal's
 * deterministic sandbox and cannot reach the Node-side error classes, while step
 * code cannot import the workflow factory. Both sides share these constants.
 *
 * @module poll-codes
 */

/** Thrown by a step to mean "not done yet"; the workflow sleeps and re-invokes it. */
export const STEP_NOT_READY_CODE = "COMPOSER_STEP_NOT_READY";

/** Thrown by the workflow when a step's `asyncPoll.timeout` is exhausted. */
export const STEP_POLL_TIMEOUT_CODE = "COMPOSER_STEP_POLL_TIMEOUT";
