/**
 * Duck-type check for structured errors with a code and parent code chain.
 *
 * Used to detect errors carrying a stable string code (from any error factory
 * that follows the {code, parentCodes} convention) so they can be converted to
 * a Temporal ApplicationFailure with the code preserved across the activity
 * boundary, where instanceof would not work.
 */

export interface ComposerErrorInstance extends Error {
  readonly code: string;
  readonly parentCodes: readonly string[];
}

export function isComposerError(error: unknown): error is ComposerErrorInstance {
  return (
    error instanceof Error &&
    typeof (error as { code?: unknown }).code === "string" &&
    Array.isArray((error as { parentCodes?: unknown }).parentCodes)
  );
}
