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
type TaskProtectionFetch = Parameters<typeof startTaskQueueMetrics>[0]["taskProtectionFetch"];
type SendMetricData = (command: PutMetricDataCommandLike) => Promise<void>;
type DescribeTaskQueue = (
  request: unknown,
) => Promise<{ stats: { approximateBacklogCount: number } }>;

const makeLogger = () =>
  ({
    info: vi.fn(),
    warn: vi.fn(),
  }) as unknown as Parameters<typeof startTaskQueueMetrics>[0]["logger"];

describe("startTaskQueueMetrics", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-01-01T00:00:00.250Z"));
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("publishes on aligned interval boundaries without drifting after publish work completes", async () => {
    const send = vi.fn<SendMetricData>().mockResolvedValue(undefined);
    const describeTaskQueue = vi
      .fn<DescribeTaskQueue>()
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
      temporalNamespace: "test-namespace",
      environmentName: "preview",
      logger: makeLogger(),
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

    await handle.stop();
  });

  it("protects the local ECS task while activities are running", async () => {
    const send = vi.fn<SendMetricData>().mockResolvedValue(undefined);
    const describeTaskQueue = vi
      .fn<DescribeTaskQueue>()
      .mockResolvedValue({ stats: { approximateBacklogCount: 0 } });
    const taskProtectionFetch = vi.fn<NonNullable<TaskProtectionFetch>>().mockResolvedValue({
      ok: true,
      status: 200,
      text: async () => "",
    });

    const handle = startTaskQueueMetrics({
      connection: {
        workflowService: { describeTaskQueue },
      } as unknown as NativeConnection,
      taskQueues: ["heavy-tasks"],
      temporalNamespace: "test-namespace",
      environmentName: "prod",
      logger: makeLogger(),
      pollIntervalMs: 60_000,
      cloudWatchClient: { send } as unknown as CloudWatchClient,
      taskProtectionAgentUri: "http://169.254.170.2",
      taskProtectionFetch,
    });

    handle.activityStarted();
    handle.activityStarted();
    await Promise.resolve();
    expect(taskProtectionFetch).toHaveBeenCalledTimes(1);
    expect(taskProtectionFetch).toHaveBeenCalledWith(
      "http://169.254.170.2/task-protection/v1/state",
      expect.objectContaining({
        method: "PUT",
        body: JSON.stringify({ ProtectionEnabled: true, ExpiresInMinutes: 360 }),
      }),
    );

    handle.activityFinished();
    expect(taskProtectionFetch).toHaveBeenCalledTimes(1);

    handle.activityFinished();
    await handle.stop();
    expect(taskProtectionFetch).toHaveBeenCalledTimes(2);
    expect(taskProtectionFetch).toHaveBeenLastCalledWith(
      "http://169.254.170.2/task-protection/v1/state",
      expect.objectContaining({
        method: "PUT",
        body: JSON.stringify({ ProtectionEnabled: false }),
      }),
    );
  });

  it("releases task protection when stopped while activities are still running", async () => {
    const send = vi.fn<SendMetricData>().mockResolvedValue(undefined);
    const describeTaskQueue = vi
      .fn<DescribeTaskQueue>()
      .mockResolvedValue({ stats: { approximateBacklogCount: 0 } });
    const taskProtectionFetch = vi.fn<NonNullable<TaskProtectionFetch>>().mockResolvedValue({
      ok: true,
      status: 200,
      text: async () => "",
    });

    const handle = startTaskQueueMetrics({
      connection: {
        workflowService: { describeTaskQueue },
      } as unknown as NativeConnection,
      taskQueues: ["heavy-tasks"],
      temporalNamespace: "test-namespace",
      environmentName: "prod",
      logger: makeLogger(),
      pollIntervalMs: 60_000,
      cloudWatchClient: { send } as unknown as CloudWatchClient,
      taskProtectionAgentUri: "http://169.254.170.2",
      taskProtectionFetch,
      taskProtectionRenewIntervalMs: 1_000,
    });

    handle.activityStarted();

    await handle.stop();
    expect(taskProtectionFetch).toHaveBeenCalledTimes(2);
    expect(taskProtectionFetch).toHaveBeenLastCalledWith(
      "http://169.254.170.2/task-protection/v1/state",
      expect.objectContaining({
        method: "PUT",
        body: JSON.stringify({ ProtectionEnabled: false }),
      }),
    );

    await vi.advanceTimersByTimeAsync(1_000);
    expect(taskProtectionFetch).toHaveBeenCalledTimes(2);
  });

  it("renews task protection while activities continue running", async () => {
    const send = vi.fn<SendMetricData>().mockResolvedValue(undefined);
    const describeTaskQueue = vi
      .fn<DescribeTaskQueue>()
      .mockResolvedValue({ stats: { approximateBacklogCount: 0 } });
    const taskProtectionFetch = vi.fn<NonNullable<TaskProtectionFetch>>().mockResolvedValue({
      ok: true,
      status: 200,
      text: async () => "",
    });

    const handle = startTaskQueueMetrics({
      connection: {
        workflowService: { describeTaskQueue },
      } as unknown as NativeConnection,
      taskQueues: ["heavy-tasks"],
      temporalNamespace: "test-namespace",
      environmentName: "prod",
      logger: makeLogger(),
      pollIntervalMs: 60_000,
      cloudWatchClient: { send } as unknown as CloudWatchClient,
      taskProtectionAgentUri: "http://169.254.170.2/",
      taskProtectionFetch,
      taskProtectionRenewIntervalMs: 1_000,
    });

    handle.activityStarted();
    await Promise.resolve();
    expect(taskProtectionFetch).toHaveBeenCalledTimes(1);

    await vi.advanceTimersByTimeAsync(1_000);
    expect(taskProtectionFetch).toHaveBeenCalledTimes(2);

    handle.activityFinished();
    await handle.stop();
    expect(taskProtectionFetch).toHaveBeenCalledTimes(3);

    await vi.advanceTimersByTimeAsync(1_000);
    expect(taskProtectionFetch).toHaveBeenCalledTimes(3);
  });
});
