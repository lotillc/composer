/**
 * Tests for startActivityWorker() framework function
 */

import { beforeEach, describe, expect, it, vi } from "vitest";
import type { Composer } from "../../../context-provider";
import type { Workflow } from "../../../dag-sync-workflow";
import { startActivityWorker } from "../start-activity-worker";

// Mock ensureNamespaceExists
vi.mock("../../utils/ensure-namespace", () => ({
  ensureNamespaceExists: vi.fn().mockResolvedValue(undefined),
}));

// Import after mock setup
const { ensureNamespaceExists } = await import("../../utils/ensure-namespace");
const mockEnsureNamespaceExists = vi.mocked(ensureNamespaceExists);

function createMockComposer(overrides?: Partial<Composer<unknown>>): Composer<unknown> {
  return {
    contextProvider: {
      beforeStep: vi.fn(),
      afterStep: vi.fn(),
    },
    logger: {
      info: vi.fn(),
      warn: vi.fn(),
      error: vi.fn(),
      debug: vi.fn(),
    },
    temporal: {
      serverAddress: "localhost:7233",
      namespace: "default",
      serviceName: "test-service",
    },
    runSyncWorkflow: vi.fn() as Composer<unknown>["runSyncWorkflow"],
    runAsyncWorkflow: vi.fn() as Composer<unknown>["runAsyncWorkflow"],
    startAsyncWorkflow: vi.fn() as Composer<unknown>["startAsyncWorkflow"],
    runActivityWorkers: vi.fn().mockResolvedValue(undefined),
    runWorkflowWorkers: vi.fn().mockResolvedValue(undefined),
    syncSchedules: vi.fn() as Composer<unknown>["syncSchedules"],
    ...overrides,
  } as unknown as Composer<unknown>;
}

const mockWorkflows: Workflow<any, any, any>[] = [{ name: "test-workflow", steps: [] }];

describe("startActivityWorker", () => {
  let mockExit: ReturnType<typeof vi.spyOn>;

  beforeEach(() => {
    mockExit = vi.spyOn(process, "exit").mockImplementation(() => undefined as never);
    mockEnsureNamespaceExists.mockReset().mockResolvedValue(undefined);
  });

  describe("Normal Operation", () => {
    it("should call ensureNamespaceExists and runActivityWorkers", async () => {
      const composer = createMockComposer();

      await startActivityWorker(composer, {
        taskQueues: ["standard-tasks"],
        maxConcurrentActivityTaskExecutions: 15,
        workflows: mockWorkflows,
      });

      expect(mockEnsureNamespaceExists).toHaveBeenCalledWith("localhost:7233", "default");
      expect(composer.runActivityWorkers).toHaveBeenCalledWith({
        taskQueues: ["standard-tasks"],
        maxConcurrentActivityTaskExecutions: 15,
        workflows: mockWorkflows,
      });
    });

    it("should use temporal config from composer", async () => {
      const composer = createMockComposer({
        temporal: {
          serverAddress: "prod.temporal.io:7233",
          namespace: "production",
          serviceName: "prod-service",
        },
      });

      await startActivityWorker(composer, {
        taskQueues: ["fast-tasks"],
        maxConcurrentActivityTaskExecutions: 50,
        workflows: mockWorkflows,
      });

      expect(mockEnsureNamespaceExists).toHaveBeenCalledWith("prod.temporal.io:7233", "production");
      expect(composer.runActivityWorkers).toHaveBeenCalledWith({
        taskQueues: ["fast-tasks"],
        maxConcurrentActivityTaskExecutions: 50,
        workflows: mockWorkflows,
      });
    });

    it("should log configuration on startup", async () => {
      const composer = createMockComposer();

      await startActivityWorker(composer, {
        taskQueues: ["standard-tasks"],
        maxConcurrentActivityTaskExecutions: 15,
        workflows: mockWorkflows,
      });

      expect(composer.logger.info).toHaveBeenCalledWith(
        "Starting Activity Workers",
        expect.objectContaining({
          serverAddress: "localhost:7233",
          namespace: "default",
          taskQueues: ["standard-tasks"],
          maxConcurrentActivityTaskExecutions: 15,
        }),
      );
    });

    it("should pass multiple task queues", async () => {
      const composer = createMockComposer();

      await startActivityWorker(composer, {
        taskQueues: ["fast-tasks", "standard-tasks", "heavy-tasks"],
        maxConcurrentActivityTaskExecutions: 10,
        workflows: mockWorkflows,
      });

      expect(composer.runActivityWorkers).toHaveBeenCalledWith({
        taskQueues: ["fast-tasks", "standard-tasks", "heavy-tasks"],
        maxConcurrentActivityTaskExecutions: 10,
        workflows: mockWorkflows,
      });
    });
  });

  describe("Namespace Creation", () => {
    it("should ensure namespace by default", async () => {
      const composer = createMockComposer();

      await startActivityWorker(composer, {
        taskQueues: ["standard-tasks"],
        maxConcurrentActivityTaskExecutions: 15,
        workflows: mockWorkflows,
      });

      expect(mockEnsureNamespaceExists).toHaveBeenCalledTimes(1);
    });

    it("should skip namespace creation when ensureNamespace is false", async () => {
      const composer = createMockComposer();

      await startActivityWorker(composer, {
        taskQueues: ["standard-tasks"],
        maxConcurrentActivityTaskExecutions: 15,
        workflows: mockWorkflows,
        ensureNamespace: false,
      });

      expect(mockEnsureNamespaceExists).not.toHaveBeenCalled();
    });

    it("should ensure namespace when explicitly set to true", async () => {
      const composer = createMockComposer();

      await startActivityWorker(composer, {
        taskQueues: ["standard-tasks"],
        maxConcurrentActivityTaskExecutions: 15,
        workflows: mockWorkflows,
        ensureNamespace: true,
      });

      expect(mockEnsureNamespaceExists).toHaveBeenCalledTimes(1);
    });
  });

  describe("Error Handling", () => {
    it("should exit with code 1 if runActivityWorkers fails", async () => {
      const composer = createMockComposer({
        runActivityWorkers: vi.fn().mockRejectedValue(new Error("Worker failed to start")),
      });

      await startActivityWorker(composer, {
        taskQueues: ["standard-tasks"],
        maxConcurrentActivityTaskExecutions: 15,
        workflows: mockWorkflows,
      });

      expect(mockExit).toHaveBeenCalledWith(1);
      expect(composer.logger.error).toHaveBeenCalledWith(
        "Failed to start Activity Workers",
        expect.objectContaining({ error: "Worker failed to start" }),
      );
    });

    it("should exit with code 1 if ensureNamespaceExists fails", async () => {
      const composer = createMockComposer();
      mockEnsureNamespaceExists.mockRejectedValue(new Error("Cannot connect to Temporal"));

      await startActivityWorker(composer, {
        taskQueues: ["standard-tasks"],
        maxConcurrentActivityTaskExecutions: 15,
        workflows: mockWorkflows,
      });

      expect(mockExit).toHaveBeenCalledWith(1);
      expect(composer.logger.error).toHaveBeenCalledWith(
        "Failed to start Activity Workers",
        expect.objectContaining({ error: "Cannot connect to Temporal" }),
      );
    });

    it("should not exit on successful startup", async () => {
      const composer = createMockComposer();

      await startActivityWorker(composer, {
        taskQueues: ["standard-tasks"],
        maxConcurrentActivityTaskExecutions: 15,
        workflows: mockWorkflows,
      });

      expect(mockExit).not.toHaveBeenCalled();
    });
  });
});
