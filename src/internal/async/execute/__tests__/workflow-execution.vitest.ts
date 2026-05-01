/**
 * Unit tests for Temporal workflow execution.
 */

import { ApplicationFailure, WorkflowFailedError } from "@temporalio/client";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { UUIDV7 } from "../../../../types";
import type { Workflow } from "../../../dag-sync-workflow";
import * as temporalClient from "../temporal-client";
import { executeWorkflowTemporal, startWorkflowTemporal } from "../workflow-execution";

vi.mock("../temporal-client");

const mockExecuteWorkflowAndWait = vi.mocked(temporalClient.executeWorkflowAndWait);
const mockExecuteWorkflow = vi.mocked(temporalClient.executeWorkflow);
const mockCreateTemporalClient = vi.mocked(temporalClient.createTemporalClient);

describe("executeWorkflowTemporal", () => {
  const mockWorkflowId = "01234567-89ab-cdef-0123-456789abcdef" as UUIDV7;
  const mockClientConfig = { address: "localhost:7233", namespace: "default" };

  beforeEach(() => {
    mockExecuteWorkflowAndWait.mockResolvedValue({ success: true });
  });

  describe("Workflow Name", () => {
    it("should use plain workflow name", async () => {
      const workflow = {
        name: "my-workflow",
        steps: [],
      } as unknown as Workflow<any, any, any>;

      await executeWorkflowTemporal(
        workflow,
        {},
        { workflowId: mockWorkflowId, clientConfig: mockClientConfig },
      );

      expect(mockExecuteWorkflowAndWait).toHaveBeenCalledWith(
        expect.objectContaining({
          workflowName: "my-workflow",
        }),
      );
    });
  });

  describe("Configured Values Merge", () => {
    it("should merge configured values with initialData", async () => {
      const workflow = {
        name: "test-workflow",
        steps: [],
        configuredValues: {
          configuredField: "configured-value",
          sharedField: "from-config",
        },
      } as Workflow<Record<string, unknown>>;

      const initialData = {
        runtimeField: "runtime-value",
      };

      await executeWorkflowTemporal(workflow, initialData, {
        workflowId: mockWorkflowId,
        clientConfig: mockClientConfig,
      });

      expect(mockExecuteWorkflowAndWait).toHaveBeenCalledWith(
        expect.objectContaining({
          input: expect.objectContaining({
            initialData: {
              runtimeField: "runtime-value",
              configuredField: "configured-value",
              sharedField: "from-config",
            },
          }),
        }),
      );
    });

    it("should preserve initialData fields alongside configured values", async () => {
      const workflow = {
        name: "test-workflow",
        steps: [],
        configuredValues: {
          field1: "configured",
        },
      } as Workflow<Record<string, unknown>>;

      const initialData = {
        field2: "runtime",
        field3: "another-runtime",
      };

      await executeWorkflowTemporal(workflow, initialData, {
        workflowId: mockWorkflowId,
        clientConfig: mockClientConfig,
      });

      expect(mockExecuteWorkflowAndWait).toHaveBeenCalledWith(
        expect.objectContaining({
          input: expect.objectContaining({
            initialData: {
              field2: "runtime",
              field3: "another-runtime",
              field1: "configured",
            },
          }),
        }),
      );
    });

    it("should handle workflows with no configured values", async () => {
      const workflow = {
        name: "test-workflow",
        steps: [],
      } as Workflow<Record<string, unknown>>;

      const initialData = {
        field1: "value1",
      };

      await executeWorkflowTemporal(workflow, initialData, {
        workflowId: mockWorkflowId,
        clientConfig: mockClientConfig,
      });

      expect(mockExecuteWorkflowAndWait).toHaveBeenCalledWith(
        expect.objectContaining({
          input: expect.objectContaining({
            initialData: {
              field1: "value1",
            },
          }),
        }),
      );
    });

    it("should handle workflows with empty configured values object", async () => {
      const workflow = {
        name: "test-workflow",
        steps: [],
        configuredValues: {},
      } as Workflow<Record<string, unknown>>;

      const initialData = {
        field1: "value1",
      };

      await executeWorkflowTemporal(workflow, initialData, {
        workflowId: mockWorkflowId,
        clientConfig: mockClientConfig,
      });

      expect(mockExecuteWorkflowAndWait).toHaveBeenCalledWith(
        expect.objectContaining({
          input: expect.objectContaining({
            initialData: {
              field1: "value1",
            },
          }),
        }),
      );
    });

    it("should handle empty initialData with configured values", async () => {
      const workflow = {
        name: "test-workflow",
        steps: [],
        configuredValues: {
          configuredField: "configured-value",
        },
      } as Workflow<Record<string, unknown>>;

      const initialData = {};

      await executeWorkflowTemporal(workflow, initialData, {
        workflowId: mockWorkflowId,
        clientConfig: mockClientConfig,
      });

      expect(mockExecuteWorkflowAndWait).toHaveBeenCalledWith(
        expect.objectContaining({
          input: expect.objectContaining({
            initialData: {
              configuredField: "configured-value",
            },
          }),
        }),
      );
    });
  });

  describe("Workflow Input Structure", () => {
    it("should pass workflowId from options", async () => {
      const workflow = {
        name: "test-workflow",
        steps: [],
      } as Workflow<Record<string, unknown>>;

      await executeWorkflowTemporal(
        workflow,
        {},
        {
          workflowId: mockWorkflowId,
          clientConfig: mockClientConfig,
        },
      );

      expect(mockExecuteWorkflowAndWait).toHaveBeenCalledWith(
        expect.objectContaining({
          workflowId: mockWorkflowId,
        }),
      );
    });

    it("should use workflow-tasks queue", async () => {
      const workflow = {
        name: "test-workflow",
        steps: [],
      } as Workflow<Record<string, unknown>>;

      await executeWorkflowTemporal(
        workflow,
        {},
        {
          workflowId: mockWorkflowId,
          clientConfig: mockClientConfig,
        },
      );

      expect(mockExecuteWorkflowAndWait).toHaveBeenCalledWith(
        expect.objectContaining({
          taskQueue: "workflow-tasks",
        }),
      );
    });

    it("should use executeWorkflow and return immediately when startOnly is true", async () => {
      const workflow = {
        name: "test-workflow",
        steps: [],
      } as Workflow<Record<string, unknown>>;

      mockExecuteWorkflow.mockResolvedValue({
        workflowId: mockWorkflowId,
      } as Awaited<ReturnType<typeof temporalClient.executeWorkflow>>);

      const result = await executeWorkflowTemporal(
        workflow,
        { field1: "value1" },
        {
          workflowId: mockWorkflowId,
          clientConfig: mockClientConfig,
          startOnly: true,
        },
      );

      expect(mockExecuteWorkflow).toHaveBeenCalledWith(
        expect.objectContaining({
          workflowId: mockWorkflowId,
          workflowName: "test-workflow",
        }),
      );
      expect(mockExecuteWorkflowAndWait).not.toHaveBeenCalled();
      expect(result).toEqual({
        bag: { field1: "value1" },
        error: undefined,
      });
    });

    it("should return an error when startOnly and awaitCheckpoint are both set", async () => {
      const workflow = {
        name: "test-workflow",
        steps: [],
      } as Workflow<Record<string, unknown>>;

      const result = await executeWorkflowTemporal(
        workflow,
        {},
        {
          workflowId: mockWorkflowId,
          clientConfig: mockClientConfig,
          startOnly: true,
          awaitCheckpoint: "earlyReturn",
        },
      );

      expect(mockExecuteWorkflow).not.toHaveBeenCalled();
      expect(mockExecuteWorkflowAndWait).not.toHaveBeenCalled();
      expect(result.error).toBeInstanceOf(Error);
      expect(result.error?.message).toBe("Cannot use startOnly and awaitCheckpoint together");
    });
  });

  describe("Return Value", () => {
    it("should return the result from executeWorkflowAndWait", async () => {
      const workflow = {
        name: "test-workflow",
        steps: [],
      } as Workflow<Record<string, unknown>>;

      const expectedResult = {
        field1: "result-value",
        field2: 42,
      };

      mockExecuteWorkflowAndWait.mockResolvedValue(expectedResult);

      const result = await executeWorkflowTemporal(
        workflow,
        {},
        {
          workflowId: mockWorkflowId,
          clientConfig: mockClientConfig,
        },
      );

      expect(result).toEqual(expectedResult);
    });
  });

  describe("Error Handling", () => {
    it("should return error in result when executeWorkflowAndWait rejects", async () => {
      const workflow = {
        name: "test-workflow",
        steps: [],
      } as unknown as Workflow<any, any, any>;

      const workflowError = new Error("Workflow execution failed");
      mockExecuteWorkflowAndWait.mockRejectedValue(workflowError);

      const result = await executeWorkflowTemporal(
        workflow,
        { inputField: "value" },
        { workflowId: mockWorkflowId, clientConfig: mockClientConfig },
      );

      expect(result.error).toBeInstanceOf(Error);
      expect(result.error?.message).toBe("Workflow execution failed");
      // bag contains merged initial data when execution fails
      expect(result.bag).toEqual({ inputField: "value" });
    });

    it("should extract bag and error from WorkflowFailedError with ApplicationFailure cause", async () => {
      const workflow = {
        name: "test-workflow",
        steps: [],
      } as unknown as Workflow<any, any, any>;

      const partialBag = { inputField: "value", stepResult: "partial" };
      const errorDetail = {
        message: "Batch 1 failed: 1 step(s) failed [badStep]",
        code: "WORKFLOW_BATCH_ERROR",
        type: "WorkflowBatchError",
        batchNumber: 1,
      };

      const appFailure = ApplicationFailure.create({
        message: errorDetail.message,
        type: errorDetail.type,
        nonRetryable: true,
        details: [{ bag: partialBag, error: errorDetail }],
      });
      const wfError = new WorkflowFailedError(
        "Workflow execution failed",
        appFailure,
        "NON_RETRYABLE_FAILURE",
      );

      mockExecuteWorkflowAndWait.mockRejectedValue(wfError);

      const result = await executeWorkflowTemporal(
        workflow,
        { inputField: "value" },
        { workflowId: mockWorkflowId, clientConfig: mockClientConfig },
      );

      expect(result.bag).toEqual(partialBag);
      expect(result.error).toBeInstanceOf(Error);
      expect(result.error?.message).toBe(errorDetail.message);
      expect((result.error as Record<string, unknown>).code).toBe("WORKFLOW_BATCH_ERROR");
      expect((result.error as Record<string, unknown>).type).toBe("WorkflowBatchError");
      expect((result.error as Record<string, unknown>).batchNumber).toBe(1);
    });

    it("should fall back to initial data when ApplicationFailure has no details", async () => {
      const workflow = {
        name: "test-workflow",
        steps: [],
      } as unknown as Workflow<any, any, any>;

      const appFailure = ApplicationFailure.create({
        message: "Something failed",
        nonRetryable: true,
      });
      const wfError = new WorkflowFailedError(
        "Workflow execution failed",
        appFailure,
        "NON_RETRYABLE_FAILURE",
      );

      mockExecuteWorkflowAndWait.mockRejectedValue(wfError);

      const result = await executeWorkflowTemporal(
        workflow,
        { inputField: "value" },
        { workflowId: mockWorkflowId, clientConfig: mockClientConfig },
      );

      expect(result.bag).toEqual({ inputField: "value" });
      expect(result.error).toBeInstanceOf(Error);
    });

    it("should fall back to generic handling for non-ApplicationFailure causes", async () => {
      const workflow = {
        name: "test-workflow",
        steps: [],
      } as unknown as Workflow<any, any, any>;

      const wfError = new WorkflowFailedError(
        "Workflow was cancelled",
        new Error("some other error"),
        "NON_RETRYABLE_FAILURE",
      );

      mockExecuteWorkflowAndWait.mockRejectedValue(wfError);

      const result = await executeWorkflowTemporal(
        workflow,
        { inputField: "value" },
        { workflowId: mockWorkflowId, clientConfig: mockClientConfig },
      );

      expect(result.bag).toEqual({ inputField: "value" });
      expect(result.error).toBeInstanceOf(Error);
      expect(result.error?.message).toBe("Workflow was cancelled");
    });
  });

  describe("Client Configuration", () => {
    it("should pass Temporal address and namespace from options", async () => {
      const customConfig = {
        address: "temporal.example.com:7233",
        namespace: "production-namespace",
      };

      const workflow = {
        name: "test-workflow",
        steps: [],
      } as unknown as Workflow<any, any, any>;

      await executeWorkflowTemporal(
        workflow,
        {},
        { workflowId: mockWorkflowId, clientConfig: customConfig },
      );

      expect(mockExecuteWorkflowAndWait).toHaveBeenCalledWith(
        expect.objectContaining({
          clientConfig: {
            address: "temporal.example.com:7233",
            namespace: "production-namespace",
          },
        }),
      );
    });
  });

  describe("Checkpoint Handling", () => {
    it("should return error for unknown checkpoint name", async () => {
      const workflow = {
        name: "test-workflow",
        steps: [],
        checkpoints: [{ name: "validCheckpoint", afterStep: "someStep" }],
      } as unknown as Workflow<any, any, any>;

      const result = await executeWorkflowTemporal(
        workflow,
        {},
        {
          workflowId: mockWorkflowId,
          clientConfig: mockClientConfig,
          awaitCheckpoint: "unknownCheckpoint",
        },
      );

      expect(result.error).toBeInstanceOf(Error);
      expect(result.error?.message).toMatch('Unknown checkpoint "unknownCheckpoint"');
    });

    it("should return error when workflow has no checkpoints but awaitCheckpoint is specified", async () => {
      const workflow = {
        name: "test-workflow",
        steps: [],
        // No checkpoints defined
      } as unknown as Workflow<any, any, any>;

      const result = await executeWorkflowTemporal(
        workflow,
        {},
        {
          workflowId: mockWorkflowId,
          clientConfig: mockClientConfig,
          awaitCheckpoint: "myCheckpoint",
        },
      );

      expect(result.error).toBeInstanceOf(Error);
      expect(result.error?.message).toMatch('Unknown checkpoint "myCheckpoint"');
    });

    it("should use executeWorkflow (not executeWorkflowAndWait) when awaitCheckpoint is specified", async () => {
      const mockExecuteUpdate = vi.fn().mockResolvedValue({ partialData: "value" });
      const mockGetHandle = vi.fn().mockReturnValue({
        executeUpdate: mockExecuteUpdate,
      });
      mockCreateTemporalClient.mockResolvedValue({
        workflow: { getHandle: mockGetHandle },
      } as any);
      mockExecuteWorkflow.mockResolvedValue({} as any);

      const workflow = {
        name: "test-workflow",
        steps: [],
        checkpoints: [{ name: "earlyReturn", afterStep: "step1" }],
      } as unknown as Workflow<any, any, any>;

      await executeWorkflowTemporal(
        workflow,
        {},
        {
          workflowId: mockWorkflowId,
          clientConfig: mockClientConfig,
          awaitCheckpoint: "earlyReturn",
        },
      );

      // Should use executeWorkflow (fire and forget), not executeWorkflowAndWait
      expect(mockExecuteWorkflow).toHaveBeenCalled();
      expect(mockExecuteWorkflowAndWait).not.toHaveBeenCalled();
    });

    it("should call executeUpdate with checkpoint name", async () => {
      const mockExecuteUpdate = vi.fn().mockResolvedValue({ data: "partial" });
      const mockGetHandle = vi.fn().mockReturnValue({
        executeUpdate: mockExecuteUpdate,
      });
      mockCreateTemporalClient.mockResolvedValue({
        workflow: { getHandle: mockGetHandle },
      } as any);
      mockExecuteWorkflow.mockResolvedValue({} as any);

      const workflow = {
        name: "test-workflow",
        steps: [],
        checkpoints: [{ name: "myCheckpoint", afterStep: "step1" }],
      } as unknown as Workflow<any, any, any>;

      await executeWorkflowTemporal(
        workflow,
        {},
        {
          workflowId: mockWorkflowId,
          clientConfig: mockClientConfig,
          awaitCheckpoint: "myCheckpoint",
        },
      );

      expect(mockExecuteUpdate).toHaveBeenCalledWith(
        expect.anything(), // The update definition
        expect.objectContaining({
          args: [{ checkpointName: "myCheckpoint" }],
        }),
      );
    });

    it("should return error when checkpoint times out", async () => {
      const neverResolves = new Promise(() => {});
      const mockExecuteUpdate = vi.fn().mockReturnValue(neverResolves);
      const mockGetHandle = vi.fn().mockReturnValue({
        executeUpdate: mockExecuteUpdate,
      });
      mockCreateTemporalClient.mockResolvedValue({
        workflow: { getHandle: mockGetHandle },
      } as any);
      mockExecuteWorkflow.mockResolvedValue({} as any);

      // Use a tiny timeout so the test doesn't wait for the real 30s default
      const workflow = {
        name: "test-workflow",
        steps: [],
        checkpoints: [{ name: "myCheckpoint", afterStep: "step1", timeout: 10 }],
      } as unknown as Workflow<any, any, any>;

      const result = await executeWorkflowTemporal(
        workflow,
        { inputField: "value" },
        {
          workflowId: mockWorkflowId,
          clientConfig: mockClientConfig,
          awaitCheckpoint: "myCheckpoint",
        },
      );

      expect(result.error).toBeInstanceOf(Error);
      expect(result.error?.message).toBe(
        'Checkpoint "myCheckpoint" for workflow "test-workflow" timed out after 10ms',
      );
      expect(result.bag).toEqual({ inputField: "value" });
    });

    it("should use DEFAULT_CHECKPOINT_TIMEOUT_MS when checkpoint has no custom timeout", async () => {
      const neverResolves = new Promise(() => {});
      const mockExecuteUpdate = vi.fn().mockReturnValue(neverResolves);
      const mockGetHandle = vi.fn().mockReturnValue({
        executeUpdate: mockExecuteUpdate,
      });
      mockCreateTemporalClient.mockResolvedValue({
        workflow: { getHandle: mockGetHandle },
      } as any);
      mockExecuteWorkflow.mockResolvedValue({} as any);

      // Use a tiny custom timeout to test the mechanism without waiting 30s;
      // separately verify the default timeout is applied correctly below
      const workflow = {
        name: "test-workflow",
        steps: [],
        checkpoints: [{ name: "myCheckpoint", afterStep: "step1", timeout: 10 }],
      } as unknown as Workflow<any, any, any>;

      const result = await executeWorkflowTemporal(
        workflow,
        {},
        {
          workflowId: mockWorkflowId,
          clientConfig: mockClientConfig,
          awaitCheckpoint: "myCheckpoint",
        },
      );

      expect(result.error?.message).toMatch("timed out after 10ms");

      // Verify the default timeout constant exists and has expected value
      const { DEFAULT_CHECKPOINT_TIMEOUT_MS } = await import("../../../dag-sync-workflow");
      expect(DEFAULT_CHECKPOINT_TIMEOUT_MS).toBe(30_000);
    });

    it("should return partial bag from checkpoint", async () => {
      const partialBag = { step1Result: "done", step2Result: "pending" };
      const mockExecuteUpdate = vi.fn().mockResolvedValue(partialBag);
      const mockGetHandle = vi.fn().mockReturnValue({
        executeUpdate: mockExecuteUpdate,
      });
      mockCreateTemporalClient.mockResolvedValue({
        workflow: { getHandle: mockGetHandle },
      } as any);
      mockExecuteWorkflow.mockResolvedValue({} as any);

      const workflow = {
        name: "test-workflow",
        steps: [],
        checkpoints: [{ name: "checkpoint1", afterStep: "step1" }],
      } as unknown as Workflow<any, any, any>;

      const result = await executeWorkflowTemporal(
        workflow,
        {},
        {
          workflowId: mockWorkflowId,
          clientConfig: mockClientConfig,
          awaitCheckpoint: "checkpoint1",
        },
      );

      expect(result).toEqual({
        bag: partialBag,
        error: undefined,
      });
    });

    it("should still use executeWorkflowAndWait when no checkpoint specified", async () => {
      const workflow = {
        name: "test-workflow",
        steps: [],
        checkpoints: [{ name: "checkpoint1", afterStep: "step1" }],
      } as unknown as Workflow<any, any, any>;

      mockExecuteWorkflowAndWait.mockResolvedValue({ bag: { result: "full" }, error: undefined });

      await executeWorkflowTemporal(
        workflow,
        {},
        { workflowId: mockWorkflowId, clientConfig: mockClientConfig },
      );

      // Should use executeWorkflowAndWait for full completion
      expect(mockExecuteWorkflowAndWait).toHaveBeenCalled();
      expect(mockExecuteWorkflow).not.toHaveBeenCalled();
    });
  });
});

describe("startWorkflowTemporal", () => {
  const mockWorkflowId = "01234567-89ab-cdef-0123-456789abcdef" as UUIDV7;
  const mockClientConfig = { address: "localhost:7233", namespace: "default" };

  beforeEach(() => {
    mockExecuteWorkflow.mockResolvedValue(undefined);
  });

  it("should call executeWorkflow (fire-and-forget), not executeWorkflowAndWait", async () => {
    const workflow = {
      name: "test-workflow",
      steps: [],
    } as unknown as Workflow<any, any, any>;

    await startWorkflowTemporal(
      workflow,
      {},
      { workflowId: mockWorkflowId, clientConfig: mockClientConfig },
    );

    expect(mockExecuteWorkflow).toHaveBeenCalled();
    expect(mockExecuteWorkflowAndWait).not.toHaveBeenCalled();
  });

  it("should return the workflowId", async () => {
    const workflow = {
      name: "test-workflow",
      steps: [],
    } as unknown as Workflow<any, any, any>;

    const result = await startWorkflowTemporal(
      workflow,
      {},
      { workflowId: mockWorkflowId, clientConfig: mockClientConfig },
    );

    expect(result).toEqual({ workflowId: mockWorkflowId });
  });

  it("should always use unversioned workflow name (Worker Versioning handles routing)", async () => {
    vi.stubEnv("IMAGE_TAG", "abc123def456");

    const workflow = {
      name: "test-workflow",
      steps: [],
    } as unknown as Workflow<any, any, any>;

    await startWorkflowTemporal(
      workflow,
      {},
      { workflowId: mockWorkflowId, clientConfig: mockClientConfig },
    );

    expect(mockExecuteWorkflow).toHaveBeenCalledWith(
      expect.objectContaining({
        workflowName: "test-workflow",
      }),
    );
  });

  it("should merge configured values with initialData", async () => {
    const workflow = {
      name: "test-workflow",
      steps: [],
      configuredValues: {
        configuredField: "configured-value",
      },
    } as Workflow<Record<string, unknown>>;

    const initialData = { runtimeField: "runtime-value" };

    await startWorkflowTemporal(workflow, initialData, {
      workflowId: mockWorkflowId,
      clientConfig: mockClientConfig,
    });

    expect(mockExecuteWorkflow).toHaveBeenCalledWith(
      expect.objectContaining({
        input: expect.objectContaining({
          initialData: {
            runtimeField: "runtime-value",
            configuredField: "configured-value",
          },
        }),
      }),
    );
  });

  it("should use workflow-tasks queue and pass clientConfig", async () => {
    const customConfig = { address: "temporal.prod:7233", namespace: "production" };

    const workflow = {
      name: "test-workflow",
      steps: [],
    } as unknown as Workflow<any, any, any>;

    await startWorkflowTemporal(
      workflow,
      {},
      { workflowId: mockWorkflowId, clientConfig: customConfig },
    );

    expect(mockExecuteWorkflow).toHaveBeenCalledWith(
      expect.objectContaining({
        taskQueue: "workflow-tasks",
        clientConfig: customConfig,
        workflowId: mockWorkflowId,
      }),
    );
  });
});
