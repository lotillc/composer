// NOTE: This observability is only used for sync workflows.
// For async workflows, we will use Temporal's observability features.
// https://docs.temporal.io/docs/concepts/workflows/observability

import {
  type Attributes,
  context as otelContext,
  type Span,
  SpanStatusCode,
  trace,
} from "@opentelemetry/api";
import type { Step } from "./dag-sync-step";
import type { Workflow } from "./dag-sync-workflow";
import { createDefaultMetrics } from "./defaults";
import { enableDebugLogging } from "./errors";
import type { ComposerLogger, Counter, Histogram, MetricsCollector, UUIDV7 } from "./types";

// Module-level observability singletons for sync workflow execution.
// Tracing and metrics use @opentelemetry/api directly -- if the user has an OTel SDK
// configured, these automatically collect data. If not, they are safe no-ops.
const workflowTracer = trace.getTracer("composer-workflow");
const workflowMetrics: MetricsCollector = createDefaultMetrics("composer-workflow");

// Result types for end functions
export type ObservabilityResult =
  | { success: true; outputFields?: string[]; batchCount?: number }
  | { success: false; error: Error };

// Handle types returned by start functions
export interface WorkflowObservabilityHandle {
  workflowId: UUIDV7;
  workflowSpan: Span;
  workflowContext: ReturnType<typeof otelContext.active>;
  workflowStartTime: number;
  workflowName: string;
  logger: ComposerLogger;
  // Pre-initialized metrics for reuse
  executionsCounter: Counter;
  durationHistogram: Histogram;
  stepExecutionsCounter: Counter;
  stepDurationHistogram: Histogram;
  batchSizeHistogram: Histogram;
}

export interface BatchObservabilityHandle {
  batchSpan: Span;
  batchContext: ReturnType<typeof otelContext.active>;
  workflowName: string;
  batchNumber: number;
  batchSize: number;
  // Cache of active SubWorkflow spans within this batch
  subworkflowSpans: Map<string, SubWorkflowObservabilityHandle>;
}

export interface SubWorkflowObservabilityHandle {
  subworkflowSpan: Span;
  subworkflowContext: ReturnType<typeof otelContext.active>;
  subworkflowName: string;
  workflowPath: string[];
  stepCount: number;
}

export interface StepObservabilityHandle {
  stepSpan: Span;
  stepContext: ReturnType<typeof otelContext.active>;
  stepStartTime: number;
  workflowName: string;
  stepName: string;
  stepDurationHistogram: Histogram;
  stepExecutionsCounter: Counter;
}

// Workflow observability functions
export function startWorkflowObservability(
  workflowId: UUIDV7,
  workflow: Workflow<any, any, any>,
  initialData: any,
  logger: ComposerLogger,
): WorkflowObservabilityHandle {
  // Create workflow-level span
  const workflowSpan = workflowTracer.startSpan(`workflow.${workflow.name}`, {
    attributes: {
      "workflow.name": workflow.name,
      "workflow.id": workflowId,
      "workflow.steps.count": workflow.steps.length,
      "workflow.initial.fields": initialData ? Object.keys(initialData).join(",") : "",
    },
  });

  // Create context with this span as active for child spans
  const workflowContext = trace.setSpan(otelContext.active(), workflowSpan);

  // Initialize metrics
  const executionsCounter = workflowMetrics.counter(
    "workflow_executions_total",
    "Total number of workflow executions",
  );
  const durationHistogram = workflowMetrics.histogram(
    "workflow_duration_seconds",
    "Duration of workflow executions in seconds",
  );
  const stepExecutionsCounter = workflowMetrics.counter(
    "step_executions_total",
    "Total number of step executions",
  );
  const stepDurationHistogram = workflowMetrics.histogram(
    "step_duration_seconds",
    "Duration of step executions in seconds",
  );
  const batchSizeHistogram = workflowMetrics.histogram(
    "batch_size",
    "Number of steps executed in parallel batches",
  );

  // Record workflow start time (counter incremented on completion only)
  const workflowStartTime = Date.now();

  return {
    workflowId,
    workflowSpan,
    workflowContext,
    workflowStartTime,
    workflowName: workflow.name,
    logger,
    executionsCounter,
    durationHistogram,
    stepExecutionsCounter,
    stepDurationHistogram,
    batchSizeHistogram,
  };
}

// Execution context for tracking workflow progress
export interface ExecutionContext {
  stepName?: string;
  stepNumber?: number;
  totalSteps: number;
  batchNumber: number;
  stepStartTime?: number;
}

export function endWorkflowObservability(
  handle: WorkflowObservabilityHandle,
  result: ObservabilityResult,
  executionContext: ExecutionContext,
  bagState?: any,
): void {
  const workflowDuration = (Date.now() - handle.workflowStartTime) / 1000; // Convert to seconds

  if (result.success) {
    // Record workflow success metrics
    handle.durationHistogram.record(workflowDuration, {
      "workflow.name": handle.workflowName,
      "workflow.status": "success",
    });
    handle.executionsCounter.add(1, {
      "workflow.name": handle.workflowName,
      "workflow.status": "success",
    });

    // Log workflow success
    handle.logger.info("Workflow completed successfully", {
      workflowName: handle.workflowName,
      workflowId: handle.workflowId,
      duration: workflowDuration,
      batchCount: result.batchCount || 0,
      outputFields: result.outputFields,
    });

    // Add workflow success attributes
    handle.workflowSpan.setAttributes({
      "workflow.status": "success",
      "workflow.batches.count": result.batchCount || 0,
      "workflow.output.fields": result.outputFields?.join(",") || "",
    });

    // Set span status to OK
    handle.workflowSpan.setStatus({ code: SpanStatusCode.OK });
  } else {
    // Record workflow error metrics
    handle.durationHistogram.record(workflowDuration, {
      "workflow.name": handle.workflowName,
      "workflow.status": "error",
    });
    handle.executionsCounter.add(1, {
      "workflow.name": handle.workflowName,
      "workflow.status": "error",
    });

    // Log workflow error with enhanced context including execution state
    const logContext: Record<string, unknown> = {
      workflowName: handle.workflowName,
      workflowId: handle.workflowId,
      duration: workflowDuration,
      error: {
        name: result.error.name,
        message: result.error.message,
        stack: result.error.stack,
      },
      failureContext: {
        stepName: executionContext.stepName,
        stepNumber: executionContext.stepNumber,
        totalSteps: executionContext.totalSteps,
        batchNumber: executionContext.batchNumber,
        ...(executionContext.stepStartTime
          ? { stepDuration: (Date.now() - executionContext.stepStartTime) / 1000 }
          : {}),
      },
    };

    // Include bag state only if debug logging is enabled
    if (enableDebugLogging && bagState) {
      logContext.bagState = bagState;
    }

    handle.logger.error("Workflow execution failed", logContext);

    // Add workflow error attributes
    handle.workflowSpan.setAttributes({
      "workflow.status": "error",
      "workflow.error.message": result.error.message,
    });
    handle.workflowSpan.recordException(result.error);

    // Set span status to ERROR
    handle.workflowSpan.setStatus({
      code: SpanStatusCode.ERROR,
      message: result.error.message,
    });
  }

  handle.workflowSpan.end();
}

// Batch observability functions
export function startBatchObservability(
  workflowHandle: WorkflowObservabilityHandle,
  batchNumber: number,
  stepNames: string[],
): BatchObservabilityHandle {
  // Create batch-level span as a child of the workflow span
  const batchSpan = workflowTracer.startSpan(
    `workflow.${workflowHandle.workflowName}.batch.${batchNumber}`,
    {
      attributes: {
        "workflow.name": workflowHandle.workflowName,
        "workflow.id": workflowHandle.workflowId,
        "workflow.batch.number": batchNumber,
        "workflow.batch.steps.count": stepNames.length,
        "workflow.batch.steps.names": stepNames.join(","),
      },
    },
    workflowHandle.workflowContext, // Use workflow context as parent
  );

  // Create context with this span as active for child spans (steps)
  const batchContext = trace.setSpan(workflowHandle.workflowContext, batchSpan);

  // Record batch size metric
  workflowHandle.batchSizeHistogram.record(stepNames.length, {
    "workflow.name": workflowHandle.workflowName,
    "workflow.batch.number": batchNumber,
  });

  return {
    batchSpan,
    batchContext,
    workflowName: workflowHandle.workflowName,
    batchNumber,
    batchSize: stepNames.length,
    subworkflowSpans: new Map(),
  };
}

export function endBatchObservability(
  handle: BatchObservabilityHandle,
  result: ObservabilityResult,
): void {
  if (result.success) {
    // Add batch success attributes
    handle.batchSpan.setAttributes({
      "workflow.batch.status": "success",
      "workflow.batch.output.fields": result.outputFields?.join(",") || "",
    });

    // Set span status to OK
    handle.batchSpan.setStatus({ code: SpanStatusCode.OK });
  } else {
    // Add batch error attributes
    handle.batchSpan.setAttributes({
      "workflow.batch.status": "error",
      "workflow.batch.error.message": result.error.message,
    });
    handle.batchSpan.recordException(result.error);

    // Set span status to ERROR
    handle.batchSpan.setStatus({
      code: SpanStatusCode.ERROR,
      message: result.error.message,
    });
  }

  // End all SubWorkflow spans before ending the batch span
  for (const subworkflowHandle of handle.subworkflowSpans.values()) {
    endSubWorkflowObservability(subworkflowHandle);
  }

  handle.batchSpan.end();
}

// SubWorkflow observability functions
/**
 * Gets or creates a SubWorkflow span for a given workflow path.
 * This creates a virtual span boundary for composed workflows even though execution is flat.
 */
function getOrCreateSubWorkflowSpan(
  workflowId: UUIDV7,
  batchHandle: BatchObservabilityHandle,
  workflowPath: string[],
): SubWorkflowObservabilityHandle {
  const pathKey = workflowPath.join(".");

  // Check if we already have a span for this subworkflow path
  const existing = batchHandle.subworkflowSpans.get(pathKey);
  if (existing) {
    existing.stepCount++;
    return existing;
  }

  // Create new SubWorkflow span as child of batch
  const subworkflowName = workflowPath[workflowPath.length - 1] || "unknown";
  const subworkflowSpan = workflowTracer.startSpan(
    `workflow.${batchHandle.workflowName}.subworkflow.${subworkflowName}`,
    {
      attributes: {
        "workflow.name": batchHandle.workflowName,
        "workflow.id": workflowId,
        "workflow.batch.number": batchHandle.batchNumber,
        "subworkflow.name": subworkflowName,
        "subworkflow.path": workflowPath.join("."),
      },
    },
    batchHandle.batchContext, // Child of batch
  );

  // Create context with SubWorkflow span as active
  const subworkflowContext = trace.setSpan(batchHandle.batchContext, subworkflowSpan);

  const handle: SubWorkflowObservabilityHandle = {
    subworkflowSpan,
    subworkflowContext,
    subworkflowName,
    workflowPath,
    stepCount: 1,
  };

  // Cache for reuse by other steps from same subworkflow
  batchHandle.subworkflowSpans.set(pathKey, handle);

  return handle;
}

function endSubWorkflowObservability(handle: SubWorkflowObservabilityHandle): void {
  // Add SubWorkflow completion attributes
  handle.subworkflowSpan.setAttributes({
    "subworkflow.steps.count": handle.stepCount,
    "subworkflow.status": "completed",
  });

  // Set span status to OK (errors are tracked at step level)
  handle.subworkflowSpan.setStatus({ code: SpanStatusCode.OK });
  handle.subworkflowSpan.end();
}

// Step observability functions
export function startStepObservability(
  workflowHandle: WorkflowObservabilityHandle,
  batchHandle: BatchObservabilityHandle,
  step: Step<any, any, any>,
): StepObservabilityHandle {
  // Determine parent context: SubWorkflow span if this step is from a composed workflow,
  // otherwise the batch span directly
  let parentContext = batchHandle.batchContext;
  const attributes: Attributes = {
    "workflow.name": workflowHandle.workflowName,
    "workflow.id": workflowHandle.workflowId,
    "workflow.batch.number": batchHandle.batchNumber,
    "batch.size": batchHandle.batchSize,
    "step.name": step.name,
    "step.needs": step.needs.join(","),
    "step.provides": step.provides.join(","),
  };

  // If step has a workflow path, it's from a composed workflow
  if (step.workflowPath && step.workflowPath.length > 0) {
    const subworkflowHandle = getOrCreateSubWorkflowSpan(
      workflowHandle.workflowId,
      batchHandle,
      step.workflowPath,
    );
    parentContext = subworkflowHandle.subworkflowContext;

    // Add subworkflow metadata to step attributes
    attributes["subworkflow.name"] = subworkflowHandle.subworkflowName;
    attributes["subworkflow.path"] = step.workflowPath.join(".");
  }

  // Create step-level span as a child of the appropriate parent (SubWorkflow or Batch)
  const stepSpan = workflowTracer.startSpan(
    `workflow.${workflowHandle.workflowName}.step.${step.name}`,
    { attributes },
    parentContext,
  );

  // Create context with step span active - used to wrap step execution
  const stepContext = trace.setSpan(parentContext, stepSpan);

  // Record step start time (counter incremented on completion only)
  const stepStartTime = Date.now();

  return {
    stepSpan,
    stepContext,
    stepStartTime,
    workflowName: workflowHandle.workflowName,
    stepName: step.name,
    stepDurationHistogram: workflowHandle.stepDurationHistogram,
    stepExecutionsCounter: workflowHandle.stepExecutionsCounter,
  };
}

export function endStepObservability(
  handle: StepObservabilityHandle,
  result: ObservabilityResult,
): void {
  const stepDuration = (Date.now() - handle.stepStartTime) / 1000; // Convert to seconds

  if (result.success) {
    // Record step success metrics
    handle.stepDurationHistogram.record(stepDuration, {
      "workflow.name": handle.workflowName,
      "step.name": handle.stepName,
      "step.status": "success",
    });
    handle.stepExecutionsCounter.add(1, {
      "workflow.name": handle.workflowName,
      "step.name": handle.stepName,
      "step.status": "success",
    });

    // Add step success attributes
    handle.stepSpan.setAttributes({
      "step.status": "success",
      "step.output.fields": result.outputFields?.join(",") || "",
    });

    // Set span status to OK
    handle.stepSpan.setStatus({ code: SpanStatusCode.OK });
  } else {
    // Record step error metrics
    handle.stepDurationHistogram.record(stepDuration, {
      "workflow.name": handle.workflowName,
      "step.name": handle.stepName,
      "step.status": "error",
    });
    handle.stepExecutionsCounter.add(1, {
      "workflow.name": handle.workflowName,
      "step.name": handle.stepName,
      "step.status": "error",
    });

    // Add step error attributes
    handle.stepSpan.setAttributes({
      "step.status": "error",
      "step.error.message": result.error.message,
    });
    handle.stepSpan.recordException(result.error);

    // Set span status to ERROR
    handle.stepSpan.setStatus({
      code: SpanStatusCode.ERROR,
      message: result.error.message,
    });
  }

  handle.stepSpan.end();
}
