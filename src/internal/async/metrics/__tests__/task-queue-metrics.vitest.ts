import type { CloudWatchClient } from "@aws-sdk/client-cloudwatch";
import type { NativeConnection } from "@temporalio/worker";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { startTaskQueueMetrics } from "../task-queue-metrics";

type PutMetricDataCommandLike = {
  input?: {
    MetricData?: Array<{
      MetricName?: string;
      Timestamp?: Date;
      Value?: number;
    }>;
  };
};

describe("startTaskQueueMetrics", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-01-01T00:00:00.250Z"));
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("publishes on aligned interval boundaries without drifting after publish work completes", async () => {
    const send = vi.fn<[PutMetricDataCommandLike], Promise<void>>().mockResolvedValue(undefined);
    const describeTaskQueue = vi
      .fn<[unknown], Promise<{ stats: { approximateBacklogCount: number } }>>()
      .mockImplementation(
        () =>
          new Promise((resolve) => {
            setTimeout(() => resolve({ stats: { approximateBacklogCount: 3 } }), 200);
          }),
      );

    const handle = startTaskQueueMetrics({
      connection: {
        workflowService: { describeTaskQueue },
      } as unknown as NativeConnection,
      taskQueues: ["standard-tasks"],
      temporalNamespace: "interchange",
      environmentName: "preview",
      logger: {
        info: vi.fn(),
        warn: vi.fn(),
      } as unknown as Parameters<typeof startTaskQueueMetrics>[0]["logger"],
      pollIntervalMs: 1_000,
      cloudWatchClient: { send } as unknown as CloudWatchClient,
    });

    await vi.advanceTimersByTimeAsync(749);
    expect(describeTaskQueue).not.toHaveBeenCalled();
    expect(send).not.toHaveBeenCalled();

    await vi.advanceTimersByTimeAsync(1);
    expect(describeTaskQueue).toHaveBeenCalledTimes(1);
    expect(send).not.toHaveBeenCalled();

    await vi.advanceTimersByTimeAsync(200);
    expect(send).toHaveBeenCalledTimes(1);
    expect(send.mock.calls[0]?.[0].input?.MetricData?.[0]?.Timestamp?.toISOString()).toBe(
      "2026-01-01T00:00:01.000Z",
    );

    await vi.advanceTimersByTimeAsync(799);
    expect(describeTaskQueue).toHaveBeenCalledTimes(1);

    await vi.advanceTimersByTimeAsync(1);
    expect(describeTaskQueue).toHaveBeenCalledTimes(2);

    await vi.advanceTimersByTimeAsync(200);
    expect(send).toHaveBeenCalledTimes(2);
    expect(send.mock.calls[1]?.[0].input?.MetricData?.[0]?.Timestamp?.toISOString()).toBe(
      "2026-01-01T00:00:02.000Z",
    );

    handle.stop();
  });
});
