import { WorkflowNotFoundError } from "@temporalio/client";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { UUIDV7 } from "../../../types";

const { mockConnect, mockGetHandle, clientOptions } = vi.hoisted(() => ({
  mockConnect: vi.fn(),
  mockGetHandle: vi.fn(),
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
      workflow = { getHandle: mockGetHandle };
      constructor(options: Record<string, unknown>) {
        clientOptions.push(options);
      }
    },
  };
});

function handleReturning(describeResult: { status: { name: string } }) {
  return { describe: vi.fn().mockResolvedValue(describeResult) };
}

describe("describeWorkflow", () => {
  beforeEach(() => {
    mockConnect.mockResolvedValue({});
    mockGetHandle.mockReset();
  });

  it("returns the workflow status when the workflow exists", async () => {
    mockGetHandle.mockReturnValue(handleReturning({ status: { name: "RUNNING" } }));

    const { describeWorkflow } = await import("../temporal-client");
    const result = await describeWorkflow("wf-running" as UUIDV7, {
      address: "status-test:7233",
      namespace: "default",
    });

    expect(result).toEqual({ status: "RUNNING" });
    expect(mockGetHandle).toHaveBeenCalledWith("wf-running");
  });

  it("returns null when the workflow does not exist", async () => {
    mockGetHandle.mockReturnValue({
      describe: vi.fn().mockRejectedValue(new WorkflowNotFoundError("not found", "wf-missing", undefined)),
    });

    const { describeWorkflow } = await import("../temporal-client");
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

    const { describeWorkflow } = await import("../temporal-client");
    await expect(
      describeWorkflow("wf-error" as UUIDV7, {
        address: "error-test:7233",
        namespace: "default",
      }),
    ).rejects.toThrow("connection refused");
  });

  it("reuses the cached client across calls with the same config", async () => {
    mockGetHandle.mockReturnValue(handleReturning({ status: { name: "COMPLETED" } }));

    const { describeWorkflow } = await import("../temporal-client");
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

    const { createTemporalClient } = await import("../temporal-client");
    await createTemporalClient({
      address: "converter-test:7233",
      namespace: "default",
      dataConverter,
    });

    expect(clientOptions.at(-1)?.dataConverter).toBe(dataConverter);
  });

  it("omits the key entirely when no converter is configured", async () => {
    const { createTemporalClient } = await import("../temporal-client");
    await createTemporalClient({ address: "no-converter:7233", namespace: "default" });

    expect(clientOptions.at(-1)).not.toHaveProperty("dataConverter");
  });

  it("reuses one connection for an equivalent converter rebuilt per call", async () => {
    const { createTemporalClient } = await import("../temporal-client");
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
    const { createTemporalClient } = await import("../temporal-client");
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
    const { createTemporalClient } = await import("../temporal-client");
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
