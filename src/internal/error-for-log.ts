/**
 * Makes an unknown rejected value safe to hand to an injected logger.
 *
 * Error serializers can classify a real Error. A JSON-style logger, however, would emit every
 * enumerable field on an arbitrary rejected object, potentially exposing secrets it contains.
 */
export function errorForLog(error: unknown): Error {
  return error instanceof Error ? error : new Error("A non-Error value was thrown");
}

const SAFE_ERROR_CODES = new Set([
  "WORKFLOW_STEP_ERROR",
  "WORKFLOW_BATCH_ERROR",
  "WORKFLOW_ERROR_HANDLER_FAILURE",
  "FANOUT_CHILD_FAILURE",
]);

const SAFE_ERROR_NAMES = new Set([
  "Error",
  "EvalError",
  "RangeError",
  "ReferenceError",
  "SyntaxError",
  "TypeError",
  "URIError",
  "AggregateError",
  "WorkflowStepError",
  "WorkflowBatchError",
  "WorkflowErrorHandlerFailure",
]);

/**
 * A code on an arbitrary Error is mutable user text. Only Composer's own codes and a numeric
 * SQLSTATE are stable, non-prose identifiers that may be written without a renderer.
 */
export function safeErrorCode(value: unknown): string | undefined {
  return typeof value === "string" && (/^\d{5}$/.test(value) || SAFE_ERROR_CODES.has(value))
    ? value
    : undefined;
}

/** Error names are also mutable; retain only built-in and Composer wrapper types. */
export function safeErrorName(value: unknown): string {
  return typeof value === "string" && SAFE_ERROR_NAMES.has(value) ? value : "Error";
}
