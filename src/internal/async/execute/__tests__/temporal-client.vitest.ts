import { WorkflowNotFoundError } from "@temporalio/client";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { UUIDV7 } from "../../../types.js";

const { mockConnect, mockGetHandle, mockStart, mockExecuteUpdateWithStart, clientOptions } =
  vi.hoisted(() => ({
    mockConnect: vi.fn(),
    mockGetHandle: vi.fn(),
    mockStart: vi.fn(),
    mockExecuteUpdateWithStart: vi.fn(),
    clientOptions: [] as Array<Record<string, unknown>>,
  }));

vi.mock("@temporalio/client", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@temporalio/client")>();
  return {
    ...actual, // keep the real WorkflowNotFoundError so `instanceof` works
    Connection: {
      connect: mockConnect,
    },
    Client: class {
      workflow = {
        getHandle: mockGetHandle,
        start: mockStart,
        executeUpdateWithStart: mockExecuteUpdateWithStart,
      };
      constructor(options: Record<string, unknown>) {
        clientOptions.push(options);
      }
    },
  };
});

const startTime = new Date("2026-11-06T10:00:00.000Z");
const closeTime = new Date("2026-11-06T12:30:00.000Z");

// What the SDK's `handle.describe()` resolves, in the fields this module reads.
function describedAs(status: string, extra: { closeTime?: Date } = {}) {
  return {
    status: { name: status },
    runId: "run-1",
    startTime,
    historyLength: 12,
    executionTime: startTime,
    ...extra,
  };
}

function handleReturning(describeResult: ReturnType<typeof describedAs>) {
  return { describe: vi.fn().mockResolvedValue(describeResult) };
}

describe("describeWorkflow", () => {
  beforeEach(() => {
    mockConnect.mockResolvedValue({});
    mockGetHandle.mockReset();
  });

  it("returns the current run's status, id, start time and history length while it runs", async () => {
    mockGetHandle.mockReturnValue(handleReturning(describedAs("RUNNING")));

    const { describeWorkflow } = await import("../temporal-client.js");
    const result = await describeWorkflow("wf-running" as UUIDV7, {
      address: "status-test:7233",
      namespace: "default",
    });

    expect(result).toEqual({ status: "RUNNING", runId: "run-1", startTime, historyLength: 12 });
    expect(result).not.toHaveProperty("closeTime");
    expect(mockGetHandle).toHaveBeenCalledWith("wf-running");
  });

  it("adds the close time once the run has closed", async () => {
    mockGetHandle.mockReturnValue(handleReturning(describedAs("TERMINATED", { closeTime })));

    const { describeWorkflow } = await import("../temporal-client.js");
    const result = await describeWorkflow("wf-closed" as UUIDV7, {
      address: "closed-test:7233",
      namespace: "default",
    });

    expect(result).toEqual({
      status: "TERMINATED",
      runId: "run-1",
      startTime,
      closeTime,
      historyLength: 12,
    });
  });

  it("returns null when the workflow does not exist", async () => {
    mockGetHandle.mockReturnValue({
      describe: vi.fn().mockRejectedValue(new WorkflowNotFoundError("not found", "wf-missing", undefined)),
    });

    const { describeWorkflow } = await import("../temporal-client.js");
    const result = await describeWorkflow("wf-missing" as UUIDV7, {
      address: "missing-test:7233",
      namespace: "default",
    });

    expect(result).toBeNull();
  });

  it("rethrows errors that are not WorkflowNotFoundError", async () => {
    mockGetHandle.mockReturnValue({
      describe: vi.fn().mockRejectedValue(new Error("connection refused")),
    });

    const { describeWorkflow } = await import("../temporal-client.js");
    await expect(
      describeWorkflow("wf-error" as UUIDV7, {
        address: "error-test:7233",
        namespace: "default",
      }),
    ).rejects.toThrow("connection refused");
  });

  it("reuses the cached client across calls with the same config", async () => {
    mockGetHandle.mockReturnValue(handleReturning(describedAs("COMPLETED")));

    const { describeWorkflow } = await import("../temporal-client.js");
    const config = { address: "cache-test:7233", namespace: "default" };
    await describeWorkflow("wf-a" as UUIDV7, config);
    await describeWorkflow("wf-b" as UUIDV7, config);

    expect(mockConnect).toHaveBeenCalledTimes(1);
  });
});

// A failure converter installed only on the workers leaves the client unable to decode what
// they wrote, so the same converter has to reach both -- and connections are cached, so the
// cache key has to tell two converters apart without leaking a connection per call.
describe("createTemporalClient dataConverter", () => {
  beforeEach(() => {
    mockConnect.mockResolvedValue({});
    clientOptions.length = 0;
  });

  it("passes the dataConverter to the Client", async () => {
    const dataConverter = { failureConverterPath: "/srv/scrubbing-failure-converter.js" };

    const { createTemporalClient } = await import("../temporal-client.js");
    await createTemporalClient({
      address: "converter-test:7233",
      namespace: "default",
      dataConverter,
    });

    expect(clientOptions.at(-1)?.dataConverter).toBe(dataConverter);
  });

  it("omits the key entirely when no converter is configured", async () => {
    const { createTemporalClient } = await import("../temporal-client.js");
    await createTemporalClient({ address: "no-converter:7233", namespace: "default" });

    expect(clientOptions.at(-1)).not.toHaveProperty("dataConverter");
  });

  it("reuses one connection for an equivalent converter rebuilt per call", async () => {
    const { createTemporalClient } = await import("../temporal-client.js");
    const config = () => ({
      address: "reuse-test:7233",
      namespace: "default",
      dataConverter: { failureConverterPath: "/srv/failure-converter.js" },
    });

    const first = await createTemporalClient(config());
    const second = await createTemporalClient(config());

    expect(second).toBe(first);
    expect(clientOptions).toHaveLength(1);
  });

  it("does not hand one converter's client to a caller asking for another", async () => {
    const { createTemporalClient } = await import("../temporal-client.js");
    const base = { address: "distinct-test:7233", namespace: "default" };

    const scrubbing = await createTemporalClient({
      ...base,
      dataConverter: { failureConverterPath: "/srv/scrubbing.js" },
    });
    const plain = await createTemporalClient(base);

    expect(plain).not.toBe(scrubbing);
    expect(clientOptions).toHaveLength(2);
  });

  it("tells two codec sets apart even though codecs are not serializable", async () => {
    const { createTemporalClient } = await import("../temporal-client.js");
    const base = { address: "codec-test:7233", namespace: "default" };
    const codecA = { encode: async (p: never[]) => p, decode: async (p: never[]) => p };
    const codecB = { encode: async (p: never[]) => p, decode: async (p: never[]) => p };

    const withA = await createTemporalClient({ ...base, dataConverter: { payloadCodecs: [codecA] } });
    const withB = await createTemporalClient({ ...base, dataConverter: { payloadCodecs: [codecB] } });
    const withAAgain = await createTemporalClient({
      ...base,
      dataConverter: { payloadCodecs: [codecA] },
    });

    expect(withB).not.toBe(withA);
    expect(withAAgain).toBe(withA);
    expect(clientOptions).toHaveLength(2);
  });
});

describe("executeWorkflow conflict policy", () => {
  const clientConfig = { address: "start-test:7233", namespace: "default" };
  const base = {
    workflowName: "wf",
    workflowId: "request-42" as UUIDV7,
    input: { initialData: {} },
    clientConfig,
  };

  beforeEach(() => {
    mockConnect.mockResolvedValue({});
    mockStart.mockReset().mockResolvedValue({});
  });

  it("passes USE_EXISTING through to Temporal", async () => {
    const { executeWorkflow } = await import("../temporal-client.js");
    await executeWorkflow({ ...base, workflowIdConflictPolicy: "USE_EXISTING" });
    expect(mockStart.mock.calls[0]?.[1]).toMatchObject({ workflowIdConflictPolicy: "USE_EXISTING" });
  });

  it("omits the key so Temporal's default applies", async () => {
    const { executeWorkflow } = await import("../temporal-client.js");
    await executeWorkflow(base);
    expect(mockStart.mock.calls[0]?.[1]).not.toHaveProperty("workflowIdConflictPolicy");
  });
});

describe("executeWorkflow execution timeout", () => {
  const clientConfig = { address: "timeout-test:7233", namespace: "default" };
  const base = {
    workflowName: "wf",
    workflowId: "request-43" as UUIDV7,
    input: { initialData: {} },
    clientConfig,
  };

  beforeEach(() => {
    mockConnect.mockResolvedValue({});
    mockStart.mockReset().mockResolvedValue({});
  });

  it("passes the limit to Temporal as workflowExecutionTimeout", async () => {
    const { executeWorkflow } = await import("../temporal-client.js");
    await executeWorkflow({ ...base, workflowExecutionTimeoutMs: 86_400_000 });
    expect(mockStart.mock.calls[0]?.[1]).toMatchObject({ workflowExecutionTimeout: 86_400_000 });
  });

  it("omits the key when there is no limit, so Temporal applies none", async () => {
    const { executeWorkflow } = await import("../temporal-client.js");
    await executeWorkflow(base);
    expect(mockStart.mock.calls[0]?.[1]).not.toHaveProperty("workflowExecutionTimeout");
  });

  // The checkpoint path starts through Update-With-Start, so the limit has to ride on its
  // start operation too.
  it("passes the limit on an update-with-start as well", async () => {
    mockExecuteUpdateWithStart.mockReset().mockResolvedValue({});
    const { startWorkflowWithUpdate } = await import("../temporal-client.js");
    await startWorkflowWithUpdate({
      ...base,
      workflowExecutionTimeoutMs: 86_400_000,
      update: { name: "await-checkpoint" } as never,
      updateArgs: [],
    });
    const startOperation = mockExecuteUpdateWithStart.mock.calls[0]?.[1]?.startWorkflowOperation;
    expect(startOperation?.options).toMatchObject({ workflowExecutionTimeout: 86_400_000 });
  });
});

describe("terminateWorkflow", () => {
  const clientConfig = { address: "terminate-test:7233", namespace: "default" };
  const terminate = vi.fn();

  beforeEach(() => {
    mockConnect.mockResolvedValue({});
    terminate.mockReset().mockResolvedValue({});
    mockGetHandle.mockReset().mockReturnValue({ terminate });
  });

  it("terminates the workflow's current run with the reason and reports it", async () => {
    const { terminateWorkflow } = await import("../temporal-client.js");
    const outcome = await terminateWorkflow(
      "wf-stuck" as UUIDV7,
      { reason: "replaced by run 2" },
      clientConfig,
    );
    expect(outcome).toBe("terminated");
    expect(mockGetHandle).toHaveBeenCalledWith("wf-stuck", undefined);
    expect(terminate).toHaveBeenCalledWith("replaced by run 2");
  });

  it("terminates only the run named, so a run that has moved on is left alone", async () => {
    const { terminateWorkflow } = await import("../temporal-client.js");
    await terminateWorkflow("wf-stuck" as UUIDV7, { runId: "run-1" }, clientConfig);
    expect(mockGetHandle).toHaveBeenCalledWith("wf-stuck", "run-1");
    expect(terminate).toHaveBeenCalledWith(undefined);
  });

  it("reports notRunning when Temporal has nothing running under the id", async () => {
    terminate.mockRejectedValue(new WorkflowNotFoundError("not found", "wf-gone", undefined));
    const { terminateWorkflow } = await import("../temporal-client.js");
    expect(await terminateWorkflow("wf-gone" as UUIDV7, {}, clientConfig)).toBe("notRunning");
  });

  it("rethrows errors that are not WorkflowNotFoundError", async () => {
    terminate.mockRejectedValue(new Error("connection refused"));
    const { terminateWorkflow } = await import("../temporal-client.js");
    await expect(terminateWorkflow("wf-err" as UUIDV7, {}, clientConfig)).rejects.toThrow(
      "connection refused",
    );
  });
});

describe("signalWorkflow", () => {
  const clientConfig = { address: "signal-test:7233", namespace: "default" };
  const signal = vi.fn();

  beforeEach(() => {
    mockConnect.mockResolvedValue({});
    signal.mockReset().mockResolvedValue(undefined);
    mockGetHandle.mockReset().mockReturnValue({ signal });
  });

  it("sends the payload alone when there is no metadata", async () => {
    const { signalWorkflow } = await import("../temporal-client.js");
    await signalWorkflow("wf-1" as UUIDV7, "approval", { ok: true }, clientConfig);
    expect(signal).toHaveBeenCalledWith("approval", { ok: true });
  });

  it("sends metadata as the second signal argument", async () => {
    const { signalWorkflow } = await import("../temporal-client.js");
    await signalWorkflow("wf-1" as UUIDV7, "approval", { ok: true }, clientConfig, {
      dedupeKey: "evt-1",
    });
    expect(signal).toHaveBeenCalledWith("approval", { ok: true }, { dedupeKey: "evt-1" });
  });
});
