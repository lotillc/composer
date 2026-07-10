import { beforeEach, describe, expect, it, vi } from "vitest";
import { noOpContextProvider } from "../../__tests__/test-utils";
import * as temporalClient from "../async/execute/temporal-client";
import { createComposer } from "../create-composer";

vi.mock("../async/execute/temporal-client");

const mockDescribeWorkflow = vi.mocked(temporalClient.describeWorkflow);

const composer = createComposer({
  contextProvider: noOpContextProvider,
  temporal: { serverAddress: "localhost:7233", namespace: "test", serviceName: "test-service" },
});

describe("createComposer liveness API", () => {
  beforeEach(() => {
    mockDescribeWorkflow.mockReset();
  });

  describe("describeWorkflow", () => {
    it("delegates to the internal describeWorkflow with the composer's Temporal config", async () => {
      mockDescribeWorkflow.mockResolvedValue({ status: "RUNNING" });

      const result = await composer.describeWorkflow("wf-1");

      expect(result).toEqual({ status: "RUNNING" });
      expect(mockDescribeWorkflow).toHaveBeenCalledWith("wf-1", {
        address: "localhost:7233",
        namespace: "test",
      });
    });

    it("returns null when the workflow does not exist", async () => {
      mockDescribeWorkflow.mockResolvedValue(null);

      expect(await composer.describeWorkflow("wf-missing")).toBeNull();
    });
  });

  describe("isWorkflowRunning", () => {
    it("returns true when the workflow status is RUNNING", async () => {
      mockDescribeWorkflow.mockResolvedValue({ status: "RUNNING" });

      expect(await composer.isWorkflowRunning("wf-1")).toBe(true);
    });

    it("returns false for a non-running status", async () => {
      mockDescribeWorkflow.mockResolvedValue({ status: "COMPLETED" });

      expect(await composer.isWorkflowRunning("wf-1")).toBe(false);
    });

    it("returns false when the workflow does not exist", async () => {
      mockDescribeWorkflow.mockResolvedValue(null);

      expect(await composer.isWorkflowRunning("wf-missing")).toBe(false);
    });
  });
});
