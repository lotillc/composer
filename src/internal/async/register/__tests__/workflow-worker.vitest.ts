/**
 * Tests for Temporal Workflow Worker
 *
 * Tests the declarative workflow registration approach where workflows
 * are passed directly and plans are generated from them.
 */

import type { MockedFunction } from "vitest";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  createWorkflowWorkers,
  runWorkflowWorkers,
  type WorkflowWorkerConfig,
} from "../workflow-worker";

type AsyncVoidFn = () => Promise<void>;
type MockConnection = { close: MockedFunction<AsyncVoidFn> };
type MockWorkerInstance = {
  run: MockedFunction<AsyncVoidFn>;
  shutdown: MockedFunction<AsyncVoidFn>;
};
type ConnectionOptions = { address: string };
type WorkerCreateOptions = {
  connection: MockConnection;
  namespace: string;
  taskQueue: string;
  workflowsPath: string;
  maxConcurrentWorkflowTaskExecutions: number;
};

const mockConnect = vi.hoisted(() => vi.fn<(options: ConnectionOptions) => Promise<MockConnection>>());
const mockWorkerCreate = vi.hoisted(() =>
  vi.fn<(options: WorkerCreateOptions) => Promise<MockWorkerInstance>>(),
);
const mockWriteWorkflowSourceFile = vi.hoisted(() => vi.fn());

vi.mock("@temporalio/worker", () => ({
  NativeConnection: { connect: mockConnect },
  Worker: { create: mockWorkerCreate },
}));
vi.mock("@temporalio/common", () => ({
  VersioningBehavior: { PINNED: 2 },
}));

vi.mock("../generate-workflow-source", () => ({
  writeWorkflowSourceFile: mockWriteWorkflowSourceFile,
}));

const mockWorkflows = [
  { name: "test-workflow", steps: [] },
] as unknown as WorkflowWorkerConfig["workflows"];

describe("Workflow Workers", () => {
  let mockConnection: MockConnection;
  let mockWorkerInstance: MockWorkerInstance;

  const createTestConfig = (
    overrides: Partial<WorkflowWorkerConfig> = {},
  ): WorkflowWorkerConfig => ({
    workflows: mockWorkflows,
    deploymentSeriesName: "test-workflows",
    ...overrides,
  });

  beforeEach(() => {
    vi.stubEnv("ENVIRONMENT_NAME", "local");

    vi.spyOn(console, "log").mockImplementation(() => undefined);
    vi.spyOn(console, "info").mockImplementation(() => undefined);
    vi.spyOn(console, "warn").mockImplementation(() => undefined);
    vi.spyOn(console, "error").mockImplementation(() => undefined);
    vi.spyOn(console, "debug").mockImplementation(() => undefined);

    mockWriteWorkflowSourceFile.mockResolvedValue("/tmp/__workflow-source.js");

    mockConnection = {
      close: vi.fn<AsyncVoidFn>().mockResolvedValue(undefined),
    };
    mockConnect.mockResolvedValue(mockConnection);

    mockWorkerInstance = {
      run: vi.fn<AsyncVoidFn>().mockResolvedValue(undefined),
      shutdown: vi.fn<AsyncVoidFn>().mockResolvedValue(undefined),
    };
    mockWorkerCreate.mockResolvedValue(mockWorkerInstance);
  });

  describe("createWorkflowWorkers", () => {
    it("should create workers from provided workflows", async () => {
      const { workers, connection } = await createWorkflowWorkers(createTestConfig());

      expect(connection).toBe(mockConnection);
      expect(workers).toEqual([mockWorkerInstance]);
    });

    it("should write workflow source file with provided workflows", async () => {
      await createWorkflowWorkers(createTestConfig());

      expect(mockWriteWorkflowSourceFile).toHaveBeenCalledWith(mockWorkflows);

      expect(mockWorkerCreate).toHaveBeenCalledWith(
        expect.objectContaining({
          workflowsPath: "/tmp/__workflow-source.js",
        }),
      );
    });

    it("should use default configuration", async () => {
      await createWorkflowWorkers(createTestConfig());

      expect(mockConnect).toHaveBeenCalledWith({ address: "localhost:7233" });
      expect(mockWorkerCreate).toHaveBeenCalledWith(
        expect.objectContaining({
          namespace: "default",
          taskQueue: "workflow-tasks",
          maxConcurrentWorkflowTaskExecutions: 100,
        }),
      );
    });

    it("should use specified namespace", async () => {
      await createWorkflowWorkers(createTestConfig({ namespace: "production" }));

      expect(mockWorkerCreate).toHaveBeenCalledWith(
        expect.objectContaining({ namespace: "production" }),
      );
    });

    it("should create workers for all task queues", async () => {
      const { workers } = await createWorkflowWorkers(
        createTestConfig({ taskQueues: ["queue-1", "queue-2", "queue-3"] }),
      );

      expect(workers).toHaveLength(3);
      expect(mockWorkerCreate).toHaveBeenCalledTimes(3);
    });

    it("should throw error if no task queues configured", async () => {
      await expect(createWorkflowWorkers(createTestConfig({ taskQueues: [] }))).rejects.toThrow(
        "No task queues configured",
      );
    });

    it("should set maxConcurrentWorkflowTaskExecutions from config", async () => {
      await createWorkflowWorkers(createTestConfig({ maxConcurrentWorkflowTaskExecutions: 200 }));

      expect(mockWorkerCreate).toHaveBeenCalledWith(
        expect.objectContaining({ maxConcurrentWorkflowTaskExecutions: 200 }),
      );
    });

    it("should handle worker creation errors", async () => {
      mockWorkerCreate.mockRejectedValue(new Error("Worker creation failed"));

      await expect(createWorkflowWorkers(createTestConfig())).rejects.toThrow(
        "Worker creation failed",
      );
    });
  });

  describe("runWorkflowWorkers", () => {
    let mockExit: ReturnType<typeof vi.spyOn>;

    beforeEach(() => {
      mockExit = vi.spyOn(process, "exit").mockImplementation(() => undefined as never);
    });

    afterEach(() => {
      mockExit.mockRestore();
    });

    it("should create and run all workers", async () => {
      const runPromise = runWorkflowWorkers(createTestConfig());
      await new Promise((resolve) => setImmediate(resolve));

      expect(mockWorkerInstance.run).toHaveBeenCalled();
      await expect(runPromise).resolves.not.toThrow();
    });

    it("should handle SIGINT gracefully", async () => {
      const signalHandlers: Record<string, () => void | Promise<void>> = {};
      const onSpy = vi.spyOn(process, "on").mockImplementation((signal, handler) => {
        signalHandlers[String(signal)] = handler as () => void | Promise<void>;
        return process;
      });

      runWorkflowWorkers(createTestConfig());
      await new Promise((resolve) => setImmediate(resolve));

      if (signalHandlers.SIGINT) {
        await signalHandlers.SIGINT();
      }
      await new Promise((resolve) => setImmediate(resolve));

      expect(mockWorkerInstance.shutdown).toHaveBeenCalled();
      expect(mockConnection.close).toHaveBeenCalled();
      expect(mockExit).toHaveBeenCalledWith(0);

      onSpy.mockRestore();
    });

    it("should throw error if worker run fails", async () => {
      mockWorkerInstance.run.mockRejectedValue(new Error("Worker failed"));

      await expect(runWorkflowWorkers(createTestConfig())).rejects.toThrow("Worker failed");
    });
  });
});
