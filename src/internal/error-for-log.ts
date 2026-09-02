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
  "CONTENT_REJECTED",
  "VALIDATION_FAILED",
  "POLICY_ERROR",
]);

// PostgreSQL 17's complete errcodes.txt catalog. A five-character shape alone is not enough:
// `Error.code` is user-controlled, and arbitrary values must not become trace attributes.
const SAFE_SQLSTATE_CODES = new Set(
  `00000 01000 01003 01004 01006 01007 01008 0100C 01P01 02000 02001 03000 08000 08001 08003 08004 08006 08007 08P01 09000 0A000 0B000 0F000 0F001 0L000 0LP01 0P000 0Z000 0Z002 20000 21000 22000 22001 22002 22003 22004 22005 22007 22008 22009 2200B 2200C 2200D 2200F 2200G 2200H 2200L 2200M 2200N 2200S 2200T 22010 22011 22012 22013 22014 22015 22016 22018 22019 2201B 2201E 2201F 2201G 2201W 2201X 22021 22022 22023 22024 22025 22026 22027 2202E 2202G 2202H 22030 22031 22032 22033 22034 22035 22036 22037 22038 22039 2203A 2203B 2203C 2203D 2203E 2203F 2203G 22P01 22P02 22P03 22P04 22P05 22P06 23000 23001 23502 23503 23505 23514 23P01 24000 25000 25001 25002 25003 25004 25005 25006 25007 25008 25P01 25P02 25P03 25P04 26000 27000 28000 28P01 2B000 2BP01 2D000 2F000 2F002 2F003 2F004 2F005 34000 38000 38001 38002 38003 38004 39000 39001 39004 39P01 39P02 39P03 3B000 3B001 3D000 3F000 40000 40001 40002 40003 40P01 42000 42501 42601 42602 42611 42622 42701 42702 42703 42704 42710 42712 42723 42725 42803 42804 42809 42830 42846 42883 428C9 42939 42P01 42P02 42P03 42P04 42P05 42P06 42P07 42P08 42P09 42P10 42P11 42P12 42P13 42P14 42P15 42P16 42P17 42P18 42P19 42P20 42P21 42P22 44000 53000 53100 53200 53300 53400 54000 54001 54011 54023 55000 55006 55P02 55P03 55P04 57000 57014 57P01 57P02 57P03 57P04 57P05 58000 58030 58P01 58P02 F0000 F0001 HV000 HV001 HV002 HV004 HV005 HV006 HV007 HV008 HV009 HV00A HV00B HV00C HV00D HV00J HV00K HV00L HV00M HV00N HV00P HV00Q HV00R HV010 HV014 HV021 HV024 HV090 HV091 P0000 P0001 P0002 P0003 P0004 XX000 XX001 XX002`.split(
    " ",
  ),
);

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
 * cataloged PostgreSQL SQLSTATE are stable, non-prose identifiers that may be written without
 * a renderer.
 */
export function safeErrorCode(value: unknown): string | undefined {
  return typeof value === "string" &&
    (SAFE_SQLSTATE_CODES.has(value) || SAFE_ERROR_CODES.has(value))
    ? value
    : undefined;
}

/** Error names are also mutable; retain only built-in and Composer wrapper types. */
export function safeErrorName(value: unknown): string {
  return typeof value === "string" && SAFE_ERROR_NAMES.has(value) ? value : "Error";
}
