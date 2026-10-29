import type { DataConverter } from "@temporalio/common";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { noOpContextProvider } from "../../__tests__/test-utils.js";
import * as temporalClient from "../async/execute/temporal-client.js";
import * as activityWorker from "../async/register/activity-worker.js";
import * as scheduleSync from "../async/schedule/sync-schedules.js";
import * as workflowWorker from "../async/register/workflow-worker.js";
import * as replayHistories from "../async/register/replay-histories.js";
import { createComposer } from "../create-composer.js";
import { defineWorkflowSignal } from "../dag-sync-signal.js";
import type { Workflow } from "../dag-sync-workflow.js";

vi.mock("../async/execute/temporal-client.js");
vi.mock("../async/register/activity-worker.js");
vi.mock("../async/schedule/sync-schedules.js");
vi.mock("../async/register/workflow-worker.js");
vi.mock("../async/register/replay-histories.js");

const mockDescribeWorkflow = vi.mocked(temporalClient.describeWorkflow);
const mockRunActivityWorkers = vi.mocked(activityWorker.runActivityWorkers);
const mockSyncSchedules = vi.mocked(scheduleSync.syncSchedules);
const mockRunWorkflowWorkers = vi.mocked(workflowWorker.runWorkflowWorkers);
const mockExecuteWorkflow = vi.mocked(temporalClient.executeWorkflow);

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

describe("worker deployment identity", () => {
  const workflows: Workflow<any, any, any>[] = [];
  const versioned = createComposer({
    contextProvider: noOpContextProvider,
    temporal: {
      serverAddress: "localhost:7233",
      namespace: "test",
      serviceName: "test-service",
      buildId: "preview-abc1234",
    },
  });

  beforeEach(() => {
    mockRunActivityWorkers.mockReset().mockResolvedValue(undefined);
    mockRunWorkflowWorkers.mockReset().mockResolvedValue(undefined);
    mockExecuteWorkflow.mockReset().mockResolvedValue(
      undefined as unknown as Awaited<ReturnType<typeof temporalClient.executeWorkflow>>,
    );
  });

  // Both worker types must land in one Worker Deployment, otherwise the activity
  // task queues are not members of the workflow's pinned version and Temporal
  // routes every step to whatever is Current -- the exact skew this guards.
  it("registers activity and workflow workers under the same deployment name", async () => {
    await versioned.runActivityWorkers({
      taskQueues: ["standard-tasks"],
      maxConcurrentActivityTaskExecutions: 10,
      workflows,
    });
    await versioned.runWorkflowWorkers({
      taskQueues: ["workflow-tasks"],
      maxConcurrentWorkflowTaskExecutions: 100,
      workflows,
    });

    const activityName = mockRunActivityWorkers.mock.calls[0]?.[0]?.deploymentSeriesName;
    const workflowName = mockRunWorkflowWorkers.mock.calls[0]?.[0]?.deploymentSeriesName;

    expect(activityName).toBe("test-service-workers");
    expect(workflowName).toBe(activityName);
  });

  it("pins async starts to the deployment the workers register under", async () => {
    await versioned.runWorkflowWorkers({
      taskQueues: ["workflow-tasks"],
      maxConcurrentWorkflowTaskExecutions: 100,
      workflows,
    });
    await versioned.runAsyncWorkflow(
      { name: "test-workflow", steps: [] } as unknown as Workflow<any, any, any>,
      {},
      { workflowId: "01234567-89ab-cdef-0123-456789abcdef", startOnly: true },
    );

    expect(mockExecuteWorkflow).toHaveBeenCalledWith(
      expect.objectContaining({
        versioningOverride: {
          pinnedTo: {
            buildId: "preview-abc1234",
            deploymentName: mockRunWorkflowWorkers.mock.calls[0]?.[0]?.deploymentSeriesName,
          },
        },
      }),
    );
  });
});

// A start-time pin outranks the workflow's own versioning declaration and survives every
// continue-as-new on the same task queue (Temporal server, mutable_state_impl.go), so an
// AUTO_UPGRADE workflow started pinned could never leave the build that started it.
describe("per-workflow start-time pinning", () => {
  const versioned = createComposer({
    contextProvider: noOpContextProvider,
    temporal: {
      serverAddress: "localhost:7233",
      namespace: "test",
      serviceName: "test-service",
      buildId: "build-1",
    },
  });
  const pinned = {
    pinnedTo: { buildId: "build-1", deploymentName: "test-service-workers" },
  };
  const defaultWorkflow = { name: "default-wf", steps: [] } as unknown as Workflow<any, any, any>;
  const pinnedWorkflow = {
    name: "pinned-wf",
    steps: [],
    versioningBehavior: "PINNED",
  } as unknown as Workflow<any, any, any>;
  const upgradingWorkflow = {
    name: "upgrading-wf",
    steps: [],
    versioningBehavior: "AUTO_UPGRADE",
    checkpoints: [{ name: "early", afterStep: "s" }],
  } as unknown as Workflow<any, any, any>;
  const mockExecuteWorkflowAndWait = vi.mocked(temporalClient.executeWorkflowAndWait);
  const mockStartWorkflowWithUpdate = vi.mocked(temporalClient.startWorkflowWithUpdate);

  function overrideOfLastStart(): unknown {
    const calls = [
      ...mockExecuteWorkflow.mock.calls.map((c) => c[0]),
      ...mockExecuteWorkflowAndWait.mock.calls.map((c) => c[0]),
      ...mockStartWorkflowWithUpdate.mock.calls.map((c) => c[0]),
    ];
    expect(calls).toHaveLength(1);
    return calls[0]?.versioningOverride;
  }

  beforeEach(() => {
    mockExecuteWorkflow.mockReset().mockResolvedValue(
      undefined as unknown as Awaited<ReturnType<typeof temporalClient.executeWorkflow>>,
    );
    mockExecuteWorkflowAndWait.mockReset().mockResolvedValue({ bag: {} });
    mockStartWorkflowWithUpdate.mockReset().mockResolvedValue({});
  });

  // A pin would be inherited by every continued run and override each boundary's routing.
  const stagedPinnedWorkflow = {
    name: "staged-pinned-wf",
    steps: [],
    versioningBehavior: "PINNED",
    stageBoundaries: [{ name: "later", afterStep: "s" }],
  } as unknown as Workflow<any, any, any>;

  // A fan-out child inherits its parent's start-time pin, which would outrank its own stages.
  const parentOfStagedChild = {
    name: "parent-of-staged",
    steps: [
      {
        name: "fan",
        needs: [],
        provides: [],
        run: async () => ({}),
        __fanOut: { childWorkflow: stagedPinnedWorkflow, concurrency: 1 },
      },
    ],
  } as unknown as Workflow<any, any, any>;

  it.each([
    ["no declaration", defaultWorkflow, pinned],
    ["PINNED", pinnedWorkflow, pinned],
    ["AUTO_UPGRADE", upgradingWorkflow, undefined],
    ["PINNED with stage boundaries", stagedPinnedWorkflow, undefined],
    ["a fan-out to a staged child", parentOfStagedChild, undefined],
  ])("startAsyncWorkflow with %s", async (_label, workflow, expected) => {
    await versioned.startAsyncWorkflow(workflow, {});
    expect(overrideOfLastStart()).toEqual(expected);
  });

  it.each([
    ["no declaration", defaultWorkflow, pinned],
    ["AUTO_UPGRADE", upgradingWorkflow, undefined],
    ["PINNED with stage boundaries", stagedPinnedWorkflow, undefined],
  ])("runAsyncWorkflow (wait for result) with %s", async (_label, workflow, expected) => {
    await versioned.runAsyncWorkflow(workflow, {});
    expect(overrideOfLastStart()).toEqual(expected);
  });

  it.each([
    ["no declaration", defaultWorkflow, pinned],
    ["AUTO_UPGRADE", upgradingWorkflow, undefined],
  ])("runAsyncWorkflow (startOnly) with %s", async (_label, workflow, expected) => {
    await versioned.runAsyncWorkflow(workflow, {}, { startOnly: true });
    expect(overrideOfLastStart()).toEqual(expected);
  });

  it("runAsyncWorkflow (awaitCheckpoint) starts an AUTO_UPGRADE workflow unpinned", async () => {
    await versioned.runAsyncWorkflow(upgradingWorkflow, {}, {
      awaitCheckpoint: "early",
    } as never);
    expect(overrideOfLastStart()).toBeUndefined();
  });

  it("starts unpinned when no buildId is configured, whatever the workflow declares", async () => {
    await composer.startAsyncWorkflow(pinnedWorkflow, {});
    expect(overrideOfLastStart()).toBeUndefined();
  });

  it.each([
    ["startAsyncWorkflow", () => versioned.startAsyncWorkflow(defaultWorkflow, {}, { ifRunning: "useExisting" })],
    ["runAsyncWorkflow", () => versioned.runAsyncWorkflow(defaultWorkflow, {}, { startOnly: true, ifRunning: "useExisting" })],
  ])("%s with ifRunning useExisting starts with USE_EXISTING", async (_label, start) => {
    await start();
    expect(mockExecuteWorkflow).toHaveBeenCalledWith(
      expect.objectContaining({ workflowIdConflictPolicy: "USE_EXISTING" }),
    );
  });

  it("leaves Temporal's fail-on-conflict default in place otherwise", async () => {
    await versioned.startAsyncWorkflow(defaultWorkflow, {}, { workflowId: "request-42" });
    expect(mockExecuteWorkflow.mock.calls[0]?.[0].workflowIdConflictPolicy).toBeUndefined();
  });

  it("startAsyncWorkflow uses a caller-supplied workflowId", async () => {
    const { workflowId } = await versioned.startAsyncWorkflow(defaultWorkflow, {}, {
      workflowId: "request-42",
    });
    expect(workflowId).toBe("request-42");
    expect(mockExecuteWorkflow).toHaveBeenCalledWith(
      expect.objectContaining({ workflowId: "request-42" }),
    );
  });
});

describe("signalAsyncWorkflow", () => {
  it("sends the signal by name with the composer's client config", async () => {
    const mockSignalWorkflow = vi.mocked(temporalClient.signalWorkflow);
    mockSignalWorkflow.mockReset().mockResolvedValue(undefined);
    const approval = defineWorkflowSignal<{ approved: boolean }>("approval");

    await composer.signalAsyncWorkflow("request-42", approval, { approved: true });

    expect(mockSignalWorkflow).toHaveBeenCalledWith(
      "request-42",
      "approval",
      { approved: true },
      { address: "localhost:7233", namespace: "test", dataConverter: undefined },
      undefined,
    );
  });

  it("sends a dedupe key as delivery metadata", async () => {
    const mockSignalWorkflow = vi.mocked(temporalClient.signalWorkflow);
    mockSignalWorkflow.mockReset().mockResolvedValue(undefined);
    const approval = defineWorkflowSignal<{ approved: boolean }>("approval");

    await composer.signalAsyncWorkflow("request-42", approval, { approved: true }, {
      dedupeKey: "evt-1",
    });

    expect(mockSignalWorkflow.mock.calls[0]?.[4]).toEqual({ dedupeKey: "evt-1" });
  });

  it("rejects a dedupe key over 256 JSON bytes before sending, counting escapes", async () => {
    const mockSignalWorkflow = vi.mocked(temporalClient.signalWorkflow);
    mockSignalWorkflow.mockReset().mockResolvedValue(undefined);
    const approval = defineWorkflowSignal<{ approved: boolean }>("approval");

    await expect(
      composer.signalAsyncWorkflow("request-42", approval, { approved: true }, {
        dedupeKey: "\u0001".repeat(50), // 50 characters, 302 bytes escaped
      }),
    ).rejects.toThrow("dedupeKey must be at most 256 bytes as JSON");
    expect(mockSignalWorkflow).not.toHaveBeenCalled();
  });

  it("validates the payload with the signal's parse before sending", async () => {
    const mockSignalWorkflow = vi.mocked(temporalClient.signalWorkflow);
    mockSignalWorkflow.mockReset().mockResolvedValue(undefined);
    const approval = defineWorkflowSignal("approval", {
      parse: (value: unknown) => {
        const approved = (value as { approved?: unknown })?.approved;
        if (typeof approved !== "boolean") throw new Error("approved must be a boolean");
        return { approved };
      },
    });

    await composer.signalAsyncWorkflow("request-42", approval, { approved: true });
    expect(mockSignalWorkflow.mock.calls[0]?.[2]).toEqual({ approved: true });

    await expect(
      composer.signalAsyncWorkflow("request-42", approval, { approved: "yes" } as never),
    ).rejects.toThrow("approved must be a boolean");
    expect(mockSignalWorkflow).toHaveBeenCalledTimes(1);
  });

  it("rejects a payload that does not match the signal's type", () => {
    const approval = defineWorkflowSignal<{ approved: boolean }>("approval");
    // @ts-expect-error -- payload must match the signal's declared type
    void composer.signalAsyncWorkflow("request-42", approval, { approved: "yes" });
  });
});

// The budget decides whether a boundary continues as new or fails, so the replay check must
// use the same one the workers do.
describe("continuation budget", () => {
  const budgeted = createComposer({
    contextProvider: noOpContextProvider,
    temporal: {
      serverAddress: "localhost:7233",
      namespace: "test",
      serviceName: "test-service",
      continuationBudgetBytes: 900_000,
    },
  });

  it("passes the budget to the workflow workers and to the replay check alike", async () => {
    vi.mocked(workflowWorker.runWorkflowWorkers).mockReset().mockResolvedValue(undefined);
    vi.mocked(replayHistories.replayWorkflowHistories)
      .mockReset()
      .mockResolvedValue({ replayed: 0, failures: [] });

    await budgeted.runWorkflowWorkers({
      taskQueues: ["workflow-tasks"],
      maxConcurrentWorkflowTaskExecutions: 1,
      workflows: [],
    });
    await budgeted.replayWorkflowHistories({ workflows: [], histories: [] });

    expect(vi.mocked(workflowWorker.runWorkflowWorkers).mock.calls[0]?.[0]).toMatchObject({
      continuationBudgetBytes: 900_000,
    });
    expect(vi.mocked(replayHistories.replayWorkflowHistories).mock.calls[0]?.[0]).toMatchObject({
      continuationBudgetBytes: 900_000,
    });
  });
});
