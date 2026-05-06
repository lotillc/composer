/**
 * Tests for Temporal Activity Worker
 *
 * Tests the declarative workflow registration approach where workflows
 * are passed directly and steps are extracted from them.
 */

import type { MockedFunction } from "vitest";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { createActivityWorkers, runActivityWorkers } from "../activity-worker";

type AsyncVoidFn = () => Promise<void>;
type MockConnection = { close: MockedFunction<AsyncVoidFn> };
type MockWorkerInstance = {
  run: MockedFunction<AsyncVoidFn>;
  shutdown: MockedFunction<() => void>;
};
type MockMetricsHandle = {
  stop: MockedFunction<AsyncVoidFn>;
  activityStarted: MockedFunction<() => void>;
  activityFinished: MockedFunction<() => void>;
};
type ConnectionOptions = { address: string };
type WorkerCreateOptions = {
  connection: MockConnection;
  namespace: string;
  taskQueue: string;
  activities: Record<string, unknown>;
  maxConcurrentActivityTaskExecutions: number;
};
type MockActivityContext = {
  info: {
    workflowExecution: {
      workflowId: string;
      runId: string;
    };
  };
};
type MockStepContext = { em: Record<string, unknown> };

const mockActivityCurrent = vi.hoisted(() => vi.fn<[], MockActivityContext>());
const mockConnect = vi.hoisted(() => vi.fn<[ConnectionOptions], Promise<MockConnection>>());
const mockWorkerCreate = vi.hoisted(() =>
  vi.fn<[WorkerCreateOptions], Promise<MockWorkerInstance>>(),
);
const mockStartTaskQueueMetrics = vi.hoisted(() =>
  vi.fn<
    [
      {
        connection: MockConnection;
        taskQueues: string[];
        temporalNamespace: string;
        logger: unknown;
      },
    ],
    MockMetricsHandle
  >(),
);

vi.mock("@temporalio/worker", () => ({
  NativeConnection: { connect: mockConnect },
  Worker: { create: mockWorkerCreate },
}));
vi.mock("@temporalio/activity", () => ({
  Context: { current: mockActivityCurrent },
}));
vi.mock("@temporalio/common", () => ({
  VersioningBehavior: { PINNED: 2 },
}));
vi.mock("../../metrics/task-queue-metrics", () => ({
  startTaskQueueMetrics: mockStartTaskQueueMetrics,
}));

const createMockContextProvider = () => ({
  beforeStep: vi.fn<[string], Promise<MockStepContext>>().mockResolvedValue({ em: {} }),
  afterStep: vi
    .fn<[MockStepContext, Error | undefined], Promise<void>>()
    .mockResolvedValue(undefined),
});

const mockStepRun = vi.fn().mockResolvedValue({ output: "test-result" });
const anotherStepRun = vi.fn().mockResolvedValue({ output2: "another-result" });
const mockErrorHandler = vi.fn().mockResolvedValue(undefined);

const mockWorkflows = [
  {
    name: "test-workflow",
    steps: [
      {
        name: "testStep",
        needs: ["input"] as const,
        provides: ["output"] as const,
        run: mockStepRun,
      },
      {
        name: "anotherStep",
        needs: ["input2"] as const,
        provides: ["output2"] as const,
        run: anotherStepRun,
      },
    ],
    errorHandler: mockErrorHandler,
  },
];

const createTestConfig = (
  overrides: Partial<Parameters<typeof createActivityWorkers>[0]> = {},
) => ({
  serverAddress: "localhost:7233",
  namespace: "default",
  taskQueues: ["fast-tasks", "standard-tasks", "heavy-tasks"],
  maxConcurrentActivityTaskExecutions: 100,
  workflows: mockWorkflows,
  contextProvider: createMockContextProvider(),
  ...overrides,
});

describe("Activity Worker", () => {
  let mockConnection: MockConnection;
  let mockWorkerInstance: MockWorkerInstance;
  let mockMetricsHandle: MockMetricsHandle;

  beforeEach(() => {
    vi.stubEnv("ENVIRONMENT_NAME", "local");

    vi.spyOn(console, "log").mockImplementation(() => undefined);
    vi.spyOn(console, "info").mockImplementation(() => undefined);
    vi.spyOn(console, "warn").mockImplementation(() => undefined);
    vi.spyOn(console, "error").mockImplementation(() => undefined);
    vi.spyOn(console, "debug").mockImplementation(() => undefined);

    mockStepRun.mockReset().mockResolvedValue({ output: "test-result" });

    mockActivityCurrent.mockReturnValue({
      info: {
        workflowExecution: {
          workflowId: "test-workflow-id",
          runId: "test-run-id",
        },
      },
    });

    mockConnection = {
      close: vi.fn<[], Promise<void>>().mockResolvedValue(undefined),
    };
    mockConnect.mockResolvedValue(mockConnection);

    mockWorkerInstance = {
      run: vi.fn<[], Promise<void>>().mockResolvedValue(undefined),
      shutdown: vi.fn<[], void>(),
    };
    mockWorkerCreate.mockResolvedValue(mockWorkerInstance);

    mockMetricsHandle = {
      stop: vi.fn<[], Promise<void>>().mockResolvedValue(undefined),
      activityStarted: vi.fn<[], void>(),
      activityFinished: vi.fn<[], void>(),
    };
    mockStartTaskQueueMetrics.mockReturnValue(mockMetricsHandle);

    vi.spyOn(process, "exit").mockImplementation(() => undefined as never);
  });

  afterEach(() => {
    process.removeAllListeners("SIGINT");
    process.removeAllListeners("SIGTERM");
    vi.restoreAllMocks();
  });

  describe("createActivityWorkers", () => {
    it("should create workers from provided workflows", async () => {
      const { workers, connection } = await createActivityWorkers(createTestConfig());

      expect(workers).toHaveLength(3);
      expect(connection).toBe(mockConnection);
    });

    it("should register step activities extracted from workflows", async () => {
      await createActivityWorkers(createTestConfig());

      const createCall = mockWorkerCreate.mock.calls[0]?.[0];
      expect(createCall).toBeDefined();
      const activityNames = Object.keys(createCall!.activities);

      expect(activityNames).toContain("testStep");
      expect(activityNames).toContain("anotherStep");
    });

    it("should register error handler activities from workflows", async () => {
      await createActivityWorkers(createTestConfig());

      const createCall = mockWorkerCreate.mock.calls[0]?.[0];
      expect(createCall).toBeDefined();
      const activityNames = Object.keys(createCall!.activities);

      expect(activityNames).toContain("test-workflow__errorHandler");
    });

    it("should create workers for all specified task queues", async () => {
      const taskQueues = ["queue-1", "queue-2", "queue-3"];
      await createActivityWorkers(createTestConfig({ taskQueues }));

      expect(mockWorkerCreate).toHaveBeenCalledTimes(3);
      for (const taskQueue of taskQueues) {
        expect(mockWorkerCreate).toHaveBeenCalledWith(expect.objectContaining({ taskQueue }));
      }
    });

    it("should throw error if no task queues configured", async () => {
      await expect(createActivityWorkers(createTestConfig({ taskQueues: [] }))).rejects.toThrow(
        "No task queues configured",
      );
    });

    it("should start task queue metrics for the worker namespace and queues", async () => {
      const taskQueues = ["queue-1", "queue-2"];
      await createActivityWorkers(createTestConfig({ namespace: "test-namespace", taskQueues }));

      expect(mockStartTaskQueueMetrics).toHaveBeenCalledWith(
        expect.objectContaining({
          connection: mockConnection,
          taskQueues,
          temporalNamespace: "test-namespace",
        }),
      );
    });

    describe("Activity Execution", () => {
      it("should create activity function that calls step.run with correct arguments", async () => {
        const mockContextProvider = createMockContextProvider();
        await createActivityWorkers(createTestConfig({ contextProvider: mockContextProvider }));

        const createCall = mockWorkerCreate.mock.calls[0]?.[0];
        expect(createCall).toBeDefined();
        const activityFn = createCall!.activities.testStep;

        const result = await (activityFn as (a: unknown, b: unknown) => Promise<unknown>)(
          { correlationId: "123" },
          { input: "test-value" },
        );

        expect(mockContextProvider.beforeStep).toHaveBeenCalledWith("testStep");
        expect(mockContextProvider.afterStep).toHaveBeenCalled();
        expect(mockMetricsHandle.activityStarted).toHaveBeenCalledTimes(1);
        expect(mockMetricsHandle.activityFinished).toHaveBeenCalledTimes(1);
        expect(result).toEqual({ output: "test-result" });
      });

      it("should work without context provider", async () => {
        await createActivityWorkers(createTestConfig({ contextProvider: undefined }));

        const createCall = mockWorkerCreate.mock.calls[0]?.[0];
        expect(createCall).toBeDefined();
        const activityFn = createCall!.activities.testStep;

        const result = await (activityFn as (a: unknown, b: unknown) => Promise<unknown>)(
          {},
          { input: "test-value" },
        );

        expect(result).toEqual({ output: "test-result" });
      });

      it("should call afterStep even when step throws error", async () => {
        const mockContextProvider = createMockContextProvider();
        const stepError = new Error("Step failed");
        mockStepRun.mockRejectedValueOnce(stepError);

        await createActivityWorkers(createTestConfig({ contextProvider: mockContextProvider }));

        const createCall = mockWorkerCreate.mock.calls[0]?.[0];
        expect(createCall).toBeDefined();
        const activityFn = createCall!.activities.testStep;

        await expect(
          (activityFn as (a: unknown, b: unknown) => Promise<unknown>)({}, { input: "test" }),
        ).rejects.toThrow("Step failed");

        expect(mockContextProvider.afterStep).toHaveBeenCalledWith(expect.anything(), stepError);
        expect(mockMetricsHandle.activityStarted).toHaveBeenCalledTimes(1);
        expect(mockMetricsHandle.activityFinished).toHaveBeenCalledTimes(1);
      });

      it("should finish metrics tracking when beforeStep throws", async () => {
        const setupError = new Error("Context setup failed");
        const mockContextProvider = createMockContextProvider();
        mockContextProvider.beforeStep.mockRejectedValueOnce(setupError);

        await createActivityWorkers(createTestConfig({ contextProvider: mockContextProvider }));

        const createCall = mockWorkerCreate.mock.calls[0]?.[0];
        expect(createCall).toBeDefined();
        const activityFn = createCall!.activities.testStep;

        await expect(
          (activityFn as (a: unknown, b: unknown) => Promise<unknown>)({}, { input: "test" }),
        ).rejects.toThrow("Context setup failed");

        expect(mockContextProvider.afterStep).not.toHaveBeenCalled();
        expect(mockMetricsHandle.activityStarted).toHaveBeenCalledTimes(1);
        expect(mockMetricsHandle.activityFinished).toHaveBeenCalledTimes(1);
      });

      it("should propagate step error even when afterStep throws cleanup error", async () => {
        const mockContextProvider = createMockContextProvider();
        const stepError = new Error("Step business logic failed");
        mockStepRun.mockRejectedValueOnce(stepError);
        mockContextProvider.afterStep.mockRejectedValueOnce(new Error("Cleanup failed"));

        await createActivityWorkers(createTestConfig({ contextProvider: mockContextProvider }));

        const createCall = mockWorkerCreate.mock.calls[0]?.[0];
        expect(createCall).toBeDefined();
        const activityFn = createCall!.activities.testStep;

        await expect(
          (activityFn as (a: unknown, b: unknown) => Promise<unknown>)({}, { input: "test" }),
        ).rejects.toThrow("Step business logic failed");
      });
    });
  });

  describe("runActivityWorkers", () => {
    it("should create and run workers", async () => {
      const runPromise = runActivityWorkers(createTestConfig());
      await new Promise((resolve) => setImmediate(resolve));
      expect(mockWorkerCreate).toHaveBeenCalledTimes(3);
      expect(mockWorkerInstance.run).toHaveBeenCalled();
      await runPromise;
    });

    it("should throw error if worker run fails", async () => {
      mockWorkerInstance.run.mockRejectedValue(new Error("Worker failed"));
      await expect(runActivityWorkers(createTestConfig())).rejects.toThrow("Worker failed");
    });

    it("should wait for workers to stop before closing the connection on SIGTERM", async () => {
      let resolveRun: (() => void) | undefined;
      mockWorkerInstance.run.mockReturnValue(
        new Promise<void>((resolve) => {
          resolveRun = resolve;
        }),
      );

      void runActivityWorkers(createTestConfig());
      await new Promise((resolve) => setImmediate(resolve));

      process.emit("SIGTERM");
      await new Promise((resolve) => setImmediate(resolve));

      expect(mockWorkerInstance.shutdown).toHaveBeenCalledTimes(3);
      expect(mockConnection.close).not.toHaveBeenCalled();
      expect(mockMetricsHandle.stop).not.toHaveBeenCalled();

      resolveRun?.();
      await new Promise((resolve) => setImmediate(resolve));

      expect(mockMetricsHandle.stop).toHaveBeenCalledTimes(1);
      expect(mockConnection.close).toHaveBeenCalledTimes(1);
      expect(process.exit).toHaveBeenCalledWith(0);
    });
  });
});
