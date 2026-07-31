/**
 * Duck-type check for structured errors carrying a stable string code.
 *
 * Used to detect errors from any error factory following the {code} or
 * {code, parentCodes} convention, so the code can be preserved across the
 * Temporal activity boundary as `ApplicationFailure.type` -- where instanceof
 * would not work.
 *
 * `parentCodes` is optional. It enriches subclass matching downstream when
 * present, but an error carrying only `code` is still a coded error, and
 * requiring the array would drop its code on the way to Temporal. That code is
 * exactly what `StepRetryPolicy.nonRetryableErrorTypes` is matched against, so
 * requiring `parentCodes` would silently defeat non-retryable steps for plain
 * coded errors.
 */

export interface ComposerErrorInstance extends Error {
  readonly code: string;
  readonly parentCodes?: readonly string[];
}

export function isComposerError(error: unknown): error is ComposerErrorInstance {
  return error instanceof Error && typeof (error as { code?: unknown }).code === "string";
}

/**
 * Reads an error's parent codes, tolerating absent or malformed values.
 * Returns an empty array when `parentCodes` is missing or not an array.
 */
export function parentCodesOf(error: ComposerErrorInstance): readonly string[] {
  return Array.isArray(error.parentCodes) ? error.parentCodes : [];
}
