import { beforeEach, describe, expect, it, vi } from "vitest";

import { createWorkflow, type Step, type Workflow } from "../internal";
import {
  type BatchObservabilityHandle,
  type ExecutionContext,
  endBatchObservability,
  endStepObservability,
  endWorkflowObservability,
  type StepObservabilityHandle,
  startBatchObservability,
  startStepObservability,
  startWorkflowObservability,
  type WorkflowObservabilityHandle,
} from "../internal/observability";
import type { UUIDV7 } from "../internal/types";
import { createMockSpan, mockLogger, mockMetrics, mockTracer } from "./observability-mocks";
import { createTestStep, type TestBag } from "./test-utils";

type WorkflowFailureLog = {
  failureContext: {
    stepDuration?: number;
  };
  bagState?: unknown;
};

const workflowId = (id: string) => id as UUIDV7;

function firstWorkflowFailureLog(): WorkflowFailureLog {
  const metadata = vi.mocked(mockLogger.error).mock.calls[0]?.[1];
  expect(metadata).toBeDefined();
  return metadata as unknown as WorkflowFailureLog;
}

// Mock @opentelemetry/api so trace.getTracer() returns our mockTracer
vi.mock("@opentelemetry/api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@opentelemetry/api")>();
  const mocks =
    await vi.importActual<typeof import("./observability-mocks")>("./observability-mocks");
  const mockTrace = Object.create(actual.trace);
  mockTrace.getTracer = () => mocks.mockTracer;
  return { ...actual, trace: mockTrace };
});

// Mock defaults so createDefaultMetrics() returns our mockMetrics
vi.mock("../internal/defaults", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../internal/defaults")>();
  const mocks =
    await vi.importActual<typeof import("./observability-mocks")>("./observability-mocks");
  return { ...actual, createDefaultMetrics: () => mocks.mockMetrics };
});

describe("Observability Functions", () => {
  let mockWorkflow: Workflow<TestBag>;
  let mockStep: Step<TestBag, readonly ["input"], readonly ["processed"]>;

  beforeEach(() => {
    // Create a simple mock step for testing
    mockStep = {
      name: "testStep",
      needs: ["input"] as const,
      provides: ["processed"] as const,
      run: async (_context: unknown, bag: Pick<TestBag, "input">) => ({
        processed: bag.input.toUpperCase(),
      }),
    };
    // Create a simple mock workflow object for testing
    mockWorkflow = {
      name: "test-workflow",
      steps: [mockStep],
    };
  });

  describe("Workflow Observability", () => {
    describe("startWorkflowObservability", () => {
      it("should create workflow span with correct attributes", () => {
        const handle = startWorkflowObservability(
          workflowId("test-workflow-id"),
          mockWorkflow,
          {
            input: "test",
          },
          mockLogger,
        );

        expect(mockTracer.startSpan).toHaveBeenCalledWith("workflow.test-workflow", {
          attributes: {
            "workflow.name": "test-workflow",
            "workflow.id": "test-workflow-id",
            "workflow.steps.count": 1,
            "workflow.initial.fields": "input",
          },
        });

        expect(handle).toMatchObject({
          workflowName: "test-workflow",
          workflowStartTime: expect.any(Number),
          workflowSpan: expect.any(Object),
          executionsCounter: expect.any(Object),
          durationHistogram: expect.any(Object),
          stepExecutionsCounter: expect.any(Object),
          stepDurationHistogram: expect.any(Object),
          batchSizeHistogram: expect.any(Object),
        });
      });

      it("should initialize all required metrics", () => {
        startWorkflowObservability(workflowId("test-wf-id"), mockWorkflow, {}, mockLogger);

        expect(mockMetrics.counter).toHaveBeenCalledWith(
          "workflow_executions_total",
          "Total number of workflow executions",
        );
        expect(mockMetrics.histogram).toHaveBeenCalledWith(
          "workflow_duration_seconds",
          "Duration of workflow executions in seconds",
        );
        expect(mockMetrics.counter).toHaveBeenCalledWith(
          "step_executions_total",
          "Total number of step executions",
        );
        expect(mockMetrics.histogram).toHaveBeenCalledWith(
          "step_duration_seconds",
          "Duration of step executions in seconds",
        );
        expect(mockMetrics.histogram).toHaveBeenCalledWith(
          "batch_size",
          "Number of steps executed in parallel batches",
        );
      });

      it("should handle empty initial data", () => {
        startWorkflowObservability(workflowId("test-wf-id"), mockWorkflow, {}, mockLogger);

        expect(mockTracer.startSpan).toHaveBeenCalledWith("workflow.test-workflow", {
          attributes: {
            "workflow.name": "test-workflow",
            "workflow.id": "test-wf-id",
            "workflow.steps.count": 1,
            "workflow.initial.fields": "",
          },
        });
      });
    });

    describe("endWorkflowObservability", () => {
      let handle: WorkflowObservabilityHandle;
      let executionContext: ExecutionContext;

      beforeEach(() => {
        // Reset span mock to return fresh mocks
        mockTracer.startSpan.mockImplementation(() => createMockSpan());

        handle = startWorkflowObservability(
          workflowId("test-wf-id"),
          mockWorkflow,
          {
            input: "test",
          },
          mockLogger,
        );
        executionContext = {
          stepName: "testStep",
          stepNumber: 1,
          totalSteps: 2,
          batchNumber: 1,
          stepStartTime: Date.now() - 1000,
        };
      });

      it("should record success metrics correctly", () => {
        endWorkflowObservability(
          handle,
          {
            success: true,
            batchCount: 2,
            outputFields: ["processed", "result"],
          },
          executionContext,
        );

        expect(handle.durationHistogram.record).toHaveBeenCalledWith(expect.any(Number), {
          "workflow.name": "test-workflow",
          "workflow.status": "success",
        });
        expect(handle.executionsCounter.add).toHaveBeenCalledWith(1, {
          "workflow.name": "test-workflow",
          "workflow.status": "success",
        });
      });

      it("should record error metrics correctly", () => {
        const testError = new Error("Test error");

        endWorkflowObservability(
          handle,
          {
            success: false,
            error: testError,
          },
          executionContext,
        );

        expect(handle.durationHistogram.record).toHaveBeenCalledWith(expect.any(Number), {
          "workflow.name": "test-workflow",
          "workflow.status": "error",
        });
        expect(handle.executionsCounter.add).toHaveBeenCalledWith(1, {
          "workflow.name": "test-workflow",
          "workflow.status": "error",
        });
      });

      it("should log success with correct context", () => {
        endWorkflowObservability(
          handle,
          {
            success: true,
            batchCount: 2,
            outputFields: ["processed"],
          },
          executionContext,
        );

        expect(mockLogger.info).toHaveBeenCalledWith("Workflow completed successfully", {
          workflowName: "test-workflow",
          workflowId: "test-wf-id",
          duration: expect.any(Number),
          batchCount: 2,
          outputFields: ["processed"],
        });
      });

      it("should log error with enhanced failure context", () => {
        const testError = new Error("Test error");

        endWorkflowObservability(
          handle,
          {
            success: false,
            error: testError,
          },
          executionContext,
        );

        expect(mockLogger.error).toHaveBeenCalledWith(
          "Workflow execution failed",
          expect.objectContaining({
            workflowName: "test-workflow",
            workflowId: "test-wf-id",
            duration: expect.any(Number),
            error: {
              name: "Error",
              message: "Test error",
              stack: expect.any(String),
            },
            failureContext: expect.objectContaining({
              stepName: "testStep",
              stepNumber: 1,
              totalSteps: 2,
              batchNumber: 1,
            }),
          }),
        );
      });

      it("should include step duration when stepStartTime is available", () => {
        const testError = new Error("Test error");

        endWorkflowObservability(
          handle,
          {
            success: false,
            error: testError,
          },
          executionContext,
        );

        const logCall = firstWorkflowFailureLog();
        expect(logCall.failureContext.stepDuration).toBeCloseTo(1, 0); // ~1 second
      });

      it("should exclude step duration when stepStartTime is not available", () => {
        const executionContextWithoutStartTime: ExecutionContext = {
          stepName: undefined,
          stepNumber: undefined,
          totalSteps: 2,
          batchNumber: 0,
          stepStartTime: undefined,
        };

        const testError = new Error("Validation error");

        endWorkflowObservability(
          handle,
          {
            success: false,
            error: testError,
          },
          executionContextWithoutStartTime,
        );

        const logCall = firstWorkflowFailureLog();
        expect(logCall.failureContext.stepDuration).toBeUndefined();
      });

      it("should include bag state when debug logging enabled", () => {
        const testError = new Error("Test error");
        const bagState = { input: "test", processed: "TEST" };

        endWorkflowObservability(
          handle,
          {
            success: false,
            error: testError,
          },
          executionContext,
          bagState,
        );

        const logCall = firstWorkflowFailureLog();
        expect(logCall.bagState).toEqual(bagState);
      });

      it("should set span attributes correctly for success", () => {
        endWorkflowObservability(
          handle,
          {
            success: true,
            batchCount: 2,
            outputFields: ["processed", "result"],
          },
          executionContext,
        );

        expect(handle.workflowSpan.setAttributes).toHaveBeenCalledWith({
          "workflow.status": "success",
          "workflow.batches.count": 2,
          "workflow.output.fields": "processed,result",
        });
        expect(handle.workflowSpan.end).toHaveBeenCalled();
      });

      it("should set span attributes correctly for error", () => {
        const testError = new Error("Test error");

        endWorkflowObservability(
          handle,
          {
            success: false,
            error: testError,
          },
          executionContext,
        );

        expect(handle.workflowSpan.setAttributes).toHaveBeenCalledWith({
          "workflow.status": "error",
          "workflow.error.message": "Test error",
        });
        expect(handle.workflowSpan.recordException).toHaveBeenCalledWith(testError);
        expect(handle.workflowSpan.end).toHaveBeenCalled();
      });
    });
  });

  describe("Batch Observability", () => {
    let workflowHandle: WorkflowObservabilityHandle;

    beforeEach(() => {
      mockTracer.startSpan.mockImplementation(() => createMockSpan());
      workflowHandle = startWorkflowObservability(
        workflowId("test-wf-id"),
        mockWorkflow,
        {},
        mockLogger,
      );
    });

    describe("startBatchObservability", () => {
      it("should create batch span with correct attributes", () => {
        const stepNames = ["step1", "step2"];
        const handle = startBatchObservability(workflowHandle, 2, stepNames);

        expect(mockTracer.startSpan).toHaveBeenCalledWith(
          "workflow.test-workflow.batch.2",
          {
            attributes: {
              "workflow.name": "test-workflow",
              "workflow.id": "test-wf-id",
              "workflow.batch.number": 2,
              "workflow.batch.steps.names": "step1,step2",
              "workflow.batch.steps.count": 2,
            },
          },
          workflowHandle.workflowContext,
        );

        expect(handle).toMatchObject({
          workflowName: "test-workflow",
          batchNumber: 2,
          batchSpan: expect.any(Object),
        });
      });

      it("should record batch size metric", () => {
        const stepNames = ["step1", "step2", "step3"];
        startBatchObservability(workflowHandle, 1, stepNames);

        expect(workflowHandle.batchSizeHistogram.record).toHaveBeenCalledWith(3, {
          "workflow.name": "test-workflow",
          "workflow.batch.number": 1,
        });
      });
    });

    describe("endBatchObservability", () => {
      let handle: BatchObservabilityHandle;

      beforeEach(() => {
        handle = startBatchObservability(workflowHandle, 1, ["step1"]);
      });

      it("should set span attributes correctly for success", () => {
        endBatchObservability(handle, {
          success: true,
          outputFields: ["processed", "result"],
        });

        expect(handle.batchSpan.setAttributes).toHaveBeenCalledWith({
          "workflow.batch.status": "success",
          "workflow.batch.output.fields": "processed,result",
        });
        expect(handle.batchSpan.end).toHaveBeenCalled();
      });

      it("should set span attributes correctly for error", () => {
        const testError = new Error("Batch error");

        endBatchObservability(handle, {
          success: false,
          error: testError,
        });

        expect(handle.batchSpan.setAttributes).toHaveBeenCalledWith({
          "workflow.batch.status": "error",
          "workflow.batch.error.message": "Batch error",
        });
        expect(handle.batchSpan.recordException).toHaveBeenCalledWith(testError);
        expect(handle.batchSpan.end).toHaveBeenCalled();
      });
    });
  });

  describe("Step Observability", () => {
    let workflowHandle: WorkflowObservabilityHandle;
    let batchHandle: BatchObservabilityHandle;

    beforeEach(() => {
      mockTracer.startSpan.mockImplementation(() => createMockSpan());
      workflowHandle = startWorkflowObservability(
        workflowId("test-wf-id"),
        mockWorkflow,
        {},
        mockLogger,
      );
      batchHandle = startBatchObservability(workflowHandle, 2, ["testStep"]);
    });

    describe("startStepObservability", () => {
      it("should create step span with correct attributes", () => {
        const handle = startStepObservability(workflowHandle, batchHandle, mockStep);

        expect(mockTracer.startSpan).toHaveBeenCalledWith(
          "workflow.test-workflow.step.testStep",
          {
            attributes: {
              "workflow.name": "test-workflow",
              "workflow.id": "test-wf-id",
              "workflow.batch.number": 2,
              "batch.size": 1,
              "step.name": "testStep",
              "step.needs": "input",
              "step.provides": "processed",
            },
          },
          batchHandle.batchContext,
        );

        expect(handle).toMatchObject({
          workflowName: "test-workflow",
          stepName: "testStep",
          stepStartTime: expect.any(Number),
          stepSpan: expect.any(Object),
          stepDurationHistogram: expect.any(Object),
          stepExecutionsCounter: expect.any(Object),
        });
      });

      it("should handle steps with multiple needs and provides", () => {
        const complexStep = createTestStep(
          "complexStep",
          ["input", "processed"],
          ["result", "count"],
          (bag) => ({
            result: `${bag.input}-${bag.processed}`,
            count: 42,
          }),
        );

        // Create a new batch handle for batch 1
        const batch1Handle = startBatchObservability(workflowHandle, 1, ["complexStep"]);
        startStepObservability(workflowHandle, batch1Handle, complexStep);

        expect(mockTracer.startSpan).toHaveBeenCalledWith(
          "workflow.test-workflow.step.complexStep",
          {
            attributes: {
              "workflow.name": "test-workflow",
              "workflow.id": "test-wf-id",
              "workflow.batch.number": 1,
              "batch.size": 1,
              "step.name": "complexStep",
              "step.needs": "input,processed",
              "step.provides": "result,count",
            },
          },
          batch1Handle.batchContext,
        );
      });
    });

    describe("endStepObservability", () => {
      let handle: StepObservabilityHandle;

      beforeEach(() => {
        handle = startStepObservability(workflowHandle, batchHandle, mockStep);
      });

      it("should record success metrics correctly", () => {
        endStepObservability(handle, {
          success: true,
          outputFields: ["processed"],
        });

        expect(handle.stepDurationHistogram.record).toHaveBeenCalledWith(expect.any(Number), {
          "workflow.name": "test-workflow",
          "step.name": "testStep",
          "step.status": "success",
        });
        expect(handle.stepExecutionsCounter.add).toHaveBeenCalledWith(1, {
          "workflow.name": "test-workflow",
          "step.name": "testStep",
          "step.status": "success",
        });
      });

      it("should record error metrics correctly", () => {
        const testError = new Error("Step error");

        endStepObservability(handle, {
          success: false,
          error: testError,
        });

        expect(handle.stepDurationHistogram.record).toHaveBeenCalledWith(expect.any(Number), {
          "workflow.name": "test-workflow",
          "step.name": "testStep",
          "step.status": "error",
        });
        expect(handle.stepExecutionsCounter.add).toHaveBeenCalledWith(1, {
          "workflow.name": "test-workflow",
          "step.name": "testStep",
          "step.status": "error",
        });
      });

      it("should set span attributes correctly for success", () => {
        endStepObservability(handle, {
          success: true,
          outputFields: ["processed", "result"],
        });

        expect(handle.stepSpan.setAttributes).toHaveBeenCalledWith({
          "step.status": "success",
          "step.output.fields": "processed,result",
        });
        expect(handle.stepSpan.end).toHaveBeenCalled();
      });

      it("should set span attributes correctly for error", () => {
        const testError = new Error("Step error");

        endStepObservability(handle, {
          success: false,
          error: testError,
        });

        expect(handle.stepSpan.setAttributes).toHaveBeenCalledWith({
          "step.status": "error",
          "step.error.message": "Step error",
        });
        expect(handle.stepSpan.recordException).toHaveBeenCalledWith(testError);
        expect(handle.stepSpan.end).toHaveBeenCalled();
      });

      it("should calculate duration accurately", () => {
        endStepObservability(handle, {
          success: true,
          outputFields: ["processed"],
        });

        const recordCall = vi.mocked(handle.stepDurationHistogram.record).mock.calls[0];
        expect(recordCall).toBeDefined();
        const duration = recordCall![0];

        expect(duration).toBeGreaterThanOrEqual(0);
        expect(duration).toBeLessThan(1); // Should be less than 1 second for this test
      });
    });
  });

  describe("Handle Object Integrity", () => {
    beforeEach(() => {
      mockTracer.startSpan.mockImplementation(() => createMockSpan());
    });

    it("should maintain handle object references correctly", () => {
      const workflowHandle = startWorkflowObservability(
        workflowId("test-wf-id"),
        mockWorkflow,
        {},
        mockLogger,
      );
      const batchHandle = startBatchObservability(workflowHandle, 1, ["step1"]);
      const stepHandle = startStepObservability(workflowHandle, batchHandle, mockStep);

      // Verify handles maintain correct references
      expect(batchHandle.workflowName).toBe(workflowHandle.workflowName);
      expect(stepHandle.workflowName).toBe(workflowHandle.workflowName);
      expect(stepHandle.stepDurationHistogram).toBe(workflowHandle.stepDurationHistogram);
      expect(stepHandle.stepExecutionsCounter).toBe(workflowHandle.stepExecutionsCounter);
    });

    it("should not interfere between multiple workflow handles", () => {
      const workflow1 = createWorkflow<TestBag>("workflow-1").build([]);
      const workflow2 = createWorkflow<TestBag>("workflow-2").build([]);

      const handle1 = startWorkflowObservability(workflowId("wf-id-1"), workflow1, {}, mockLogger);
      const handle2 = startWorkflowObservability(workflowId("wf-id-2"), workflow2, {}, mockLogger);

      expect(handle1.workflowName).toBe("workflow-1");
      expect(handle2.workflowName).toBe("workflow-2");
      expect(handle1.workflowSpan).not.toBe(handle2.workflowSpan);
    });
  });
});
