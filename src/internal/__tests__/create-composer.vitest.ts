import type { DataConverter } from "@temporalio/common";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { noOpContextProvider } from "../../__tests__/test-utils";
import * as temporalClient from "../async/execute/temporal-client";
import * as activityWorker from "../async/register/activity-worker";
import * as scheduleSync from "../async/schedule/sync-schedules";
import * as workflowWorker from "../async/register/workflow-worker";
import { createComposer } from "../create-composer";
import type { Workflow } from "../dag-sync-workflow";

vi.mock("../async/execute/temporal-client");
vi.mock("../async/register/activity-worker");
vi.mock("../async/schedule/sync-schedules");
vi.mock("../async/register/workflow-worker");

const mockDescribeWorkflow = vi.mocked(temporalClient.describeWorkflow);
const mockRunActivityWorkers = vi.mocked(activityWorker.runActivityWorkers);
const mockSyncSchedules = vi.mocked(scheduleSync.syncSchedules);
const mockRunWorkflowWorkers = vi.mocked(workflowWorker.runWorkflowWorkers);

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

// The scrubbing seams are configured once on the composer, so every worker it starts and
// every client it opens gets them -- a per-call-site knob is one a caller can forget.
describe("createComposer scrubbing seams", () => {
  const dataConverter: DataConverter = {
    failureConverterPath: "/srv/scrubbing-failure-converter.js",
  };
  const activityInterceptor = vi.fn();
  const workflows = [] as Workflow<Record<string, unknown>, never, never>[];

  const seamComposer = () =>
    createComposer({
      contextProvider: noOpContextProvider,
      temporal: {
        serverAddress: "localhost:7233",
        namespace: "test",
        serviceName: "test-service",
        dataConverter,
        interceptors: { activity: [activityInterceptor] },
      },
    });

  beforeEach(() => {
    mockRunActivityWorkers.mockReset().mockResolvedValue(undefined);
    mockRunWorkflowWorkers.mockReset().mockResolvedValue(undefined);
    mockDescribeWorkflow.mockReset().mockResolvedValue(null);
  });

  it("forwards both seams to the activity workers", async () => {
    await seamComposer().runActivityWorkers({
      taskQueues: ["standard-tasks"],
      maxConcurrentActivityTaskExecutions: 10,
      workflows,
    });

    expect(mockRunActivityWorkers).toHaveBeenCalledWith(
      expect.objectContaining({
        dataConverter,
        interceptors: { activity: [activityInterceptor] },
      }),
    );
  });

  it("forwards both seams to the workflow workers", async () => {
    await seamComposer().runWorkflowWorkers({
      taskQueues: ["workflow-tasks"],
      maxConcurrentWorkflowTaskExecutions: 100,
      workflows,
    });

    expect(mockRunWorkflowWorkers).toHaveBeenCalledWith(
      expect.objectContaining({
        dataConverter,
        interceptors: { activity: [activityInterceptor] },
      }),
    );
  });

  it("forwards the converter to the client syncSchedules opens", async () => {
    mockSyncSchedules.mockResolvedValue({ created: [], updated: [], deleted: [], errors: [] });

    await seamComposer().syncSchedules([]);

    expect(mockSyncSchedules).toHaveBeenCalledWith(
      expect.objectContaining({
        temporalConfig: expect.objectContaining({ dataConverter }),
      }),
    );
  });

  it("forwards the converter to the client it uses to read workflow status", async () => {
    await seamComposer().describeWorkflow("wf-1");

    expect(mockDescribeWorkflow).toHaveBeenCalledWith(
      "wf-1",
      expect.objectContaining({ dataConverter }),
    );
  });
});
