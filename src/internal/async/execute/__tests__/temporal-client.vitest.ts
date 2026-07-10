import { WorkflowNotFoundError } from "@temporalio/client";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { UUIDV7 } from "../../../types";

const { mockConnect, mockGetHandle } = vi.hoisted(() => ({
  mockConnect: vi.fn(),
  mockGetHandle: vi.fn(),
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
