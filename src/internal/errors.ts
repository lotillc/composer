// Hard-coded constant for controlling debug logging
// TODO: Make this configurable via feature flags in the future
export const enableDebugLogging = true;

/**
 * Enhanced error class for workflow step failures
 *
 * Provides rich context about where and why a step failed, including:
 * - Error code for matching via `matchesError()`
 * - Workflow identification for correlation
 * - Step-specific context (name, batch number, workflow path)
 * - Original error preservation with stack trace
 * - Optional bag state for debugging (controlled by enableDebugLogging)
 */
export class WorkflowStepError<Bag = Record<string, any>> extends Error {
  static readonly code = "WORKFLOW_STEP_ERROR" as const;

  /** Error code - either from originalError.code or the default WORKFLOW_STEP_ERROR */
  public readonly code: string;
  public readonly workflowId: string;
  public readonly stepName: string;
  public readonly batchNumber: number;
  public readonly originalError: Error;
  public readonly bagState: Bag;
  public readonly workflowPath?: string[];

  constructor(options: {
    workflowId: string;
    stepName: string;
    batchNumber: number;
    originalError: Error;
    bagState: Bag;
    workflowPath?: string[];
  }) {
    // Create enhanced error message with context
    let contextInfo = `Workflow "${options.workflowId}" failed at step "${options.stepName}" (batch ${options.batchNumber})`;

    // Include workflow path if this is from a composed workflow
    if (options.workflowPath && options.workflowPath.length > 0) {
      contextInfo += ` [subworkflow path: ${options.workflowPath.join(".")}]`;
    }

    const originalMessage = options.originalError.message;

    // Use standard error chaining to preserve both stack traces
    super(`${contextInfo}: ${originalMessage}`, { cause: options.originalError });

    // Set error name
    this.name = "WorkflowStepError";

    // Extract code from original error if present, otherwise use default
    const originalCode =
      "code" in options.originalError && typeof options.originalError.code === "string"
        ? options.originalError.code
        : undefined;
    this.code = originalCode ?? WorkflowStepError.code;

    // Store all context information
    this.workflowId = options.workflowId;
    this.stepName = options.stepName;
    this.batchNumber = options.batchNumber;
    this.originalError = options.originalError;
    this.bagState = options.bagState;
    this.workflowPath = options.workflowPath;
  }

  /**
   * Get a structured representation of the error for logging
   */
  toLogContext(): Record<string, any> {
    const context: Record<string, any> = {
      code: this.code,
      workflowId: this.workflowId,
      stepName: this.stepName,
      batchNumber: this.batchNumber,
      originalError: {
        name: this.originalError.name,
        message: this.originalError.message,
        stack: this.originalError.stack,
        code: "code" in this.originalError ? this.originalError.code : undefined,
      },
    };

    if (this.workflowPath && this.workflowPath.length > 0) {
      context.workflowPath = this.workflowPath;
      context.subworkflowName = this.workflowPath[this.workflowPath.length - 1];
    }

    if (this.bagState) {
      context.bagState = this.bagState;
    }

    return context;
  }
}

/**
 * Error class for batch failures in workflows using Promise.allSettled.
 *
 * When a batch contains multiple parallel steps and some fail, this error
 * aggregates all the failures while preserving the bag state that includes
 * outputs from successful steps in the same batch.
 *
 * Use `matchesError()` to check if any step in the batch threw a specific error code.
 */
export class WorkflowBatchError<Bag = Record<string, any>> extends Error {
  static readonly code = "WORKFLOW_BATCH_ERROR" as const;
  public readonly code = WorkflowBatchError.code;

  /** All step failures from the batch */
  public readonly errors: WorkflowStepError<Bag>[];

  /** Bag state including outputs from successful steps in this batch */
  public readonly bagState: Bag;

  /** The batch number where the failures occurred */
  public readonly batchNumber: number;

  /** Workflow ID for correlation */
  public readonly workflowId: string;

  constructor(options: {
    errors: WorkflowStepError<Bag>[];
    bagState: Bag;
    batchNumber: number;
    workflowId: string;
  }) {
    const failedSteps = options.errors.map((e) => e.stepName).join(", ");
    const message = `Workflow "${options.workflowId}" batch ${options.batchNumber} failed: ${options.errors.length} step(s) failed [${failedSteps}]`;

    super(message);
    this.name = "WorkflowBatchError";
    this.errors = options.errors;
    this.bagState = options.bagState;
    this.batchNumber = options.batchNumber;
    this.workflowId = options.workflowId;
  }

  /**
   * Get a structured representation of the error for logging
   */
  toLogContext(): Record<string, any> {
    return {
      code: this.code,
      workflowId: this.workflowId,
      batchNumber: this.batchNumber,
      failedStepCount: this.errors.length,
      failedSteps: this.errors.map((e) => ({
        stepName: e.stepName,
        code: e.code,
        message: e.originalError.message,
      })),
      bagState: this.bagState,
    };
  }
}

/**
 * Error class for when a workflow's error handler itself throws an error.
 *
 * This preserves both the original workflow error that triggered the handler
 * and the error that occurred within the handler, allowing callers to
 * understand the full failure chain.
 */
export class WorkflowErrorHandlerFailure extends Error {
  static readonly code = "WORKFLOW_ERROR_HANDLER_FAILURE" as const;
  public readonly code = WorkflowErrorHandlerFailure.code;

  /** The original workflow error that triggered the error handler */
  public readonly originalError: WorkflowStepError | WorkflowBatchError;

  /** The error thrown by the error handler itself */
  public readonly handlerError: Error;

  /** Workflow ID for correlation */
  public readonly workflowId: string;

  constructor(options: {
    originalError: WorkflowStepError | WorkflowBatchError;
    handlerError: Error;
    workflowId: string;
  }) {
    const message = `Workflow "${options.workflowId}" error handler failed: ${options.handlerError.message} (original error: ${options.originalError.message})`;

    super(message, { cause: options.handlerError });
    this.name = "WorkflowErrorHandlerFailure";
    this.originalError = options.originalError;
    this.handlerError = options.handlerError;
    this.workflowId = options.workflowId;
  }

  /**
   * Get a structured representation of the error for logging
   */
  toLogContext(): Record<string, any> {
    return {
      code: this.code,
      workflowId: this.workflowId,
      originalError: this.originalError.toLogContext(),
      handlerError: {
        name: this.handlerError.name,
        message: this.handlerError.message,
        stack: this.handlerError.stack,
        code: "code" in this.handlerError ? this.handlerError.code : undefined,
      },
    };
  }
}

/**
 * Target for {@link matchesError} — either a string error code or an error class
 * with a static `code` property (e.g. {@link WorkflowStepError}).
 */
export type ErrorMatchTarget = string | { readonly code: string };

/**
 * Returns true if `error` (or any error wrapped inside it) has a code matching
 * `target`. Walks `error.errors[]` (batch), `error.originalError` (step) and
 * `error.cause` (standard chaining) so that codes survive being wrapped by
 * {@link WorkflowStepError} and {@link WorkflowBatchError}, including across
 * Temporal serialization where `instanceof` does not work. Errors carrying a
 * `parentCodes` array (from a hierarchical error factory) match if `target` is
 * the error's own code or any of its parent codes.
 */
export function matchesError(error: unknown, target: ErrorMatchTarget): boolean {
  const targetCode = typeof target === "string" ? target : target.code;
  for (const candidate of extractErrorCandidates(error)) {
    if (candidate.code === targetCode) return true;
    if (Array.isArray(candidate.parentCodes) && candidate.parentCodes.includes(targetCode)) {
      return true;
    }
  }
  return false;
}

interface CodedErrorLike {
  code?: string;
  parentCodes?: readonly string[];
  [key: string]: unknown;
}

function extractErrorCandidates(error: unknown): CodedErrorLike[] {
  if (error == null) return [];
  const err = error as Record<string, unknown>;
  const candidates: CodedErrorLike[] = [];

  if (typeof err.code === "string") {
    candidates.push(err as CodedErrorLike);
  }
  if (Array.isArray(err.errors)) {
    for (const inner of err.errors) {
      candidates.push(...extractErrorCandidates(inner));
    }
  }
  if (err.originalError != null) {
    candidates.push(...extractErrorCandidates(err.originalError));
  }
  if (err.cause != null) {
    candidates.push(...extractErrorCandidates(err.cause));
  }

  return candidates;
}
