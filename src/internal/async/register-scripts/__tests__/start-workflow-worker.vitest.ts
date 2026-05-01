/**
 * Tests for startWorkflowWorker() framework function
 */

import { beforeEach, describe, expect, it, vi } from "vitest";
import type { Composer } from "../../../context-provider";
import type { Workflow } from "../../../dag-sync-workflow";
import { startWorkflowWorker } from "../start-workflow-worker";

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
    },
    runSyncWorkflow: vi.fn() as Composer<unknown>["runSyncWorkflow"],
    runAsyncWorkflow: vi.fn() as Composer<unknown>["runAsyncWorkflow"],
    runActivityWorkers: vi.fn().mockResolvedValue(undefined),
    runWorkflowWorkers: vi.fn().mockResolvedValue(undefined),
    ...overrides,
  };
}

const mockWorkflows: Workflow<any, any, any>[] = [{ name: "test-workflow", steps: [] }];

describe("startWorkflowWorker", () => {
  let mockExit: ReturnType<typeof vi.spyOn>;

  beforeEach(() => {
    mockExit = vi.spyOn(process, "exit").mockImplementation(() => undefined as never);
    mockEnsureNamespaceExists.mockReset().mockResolvedValue(undefined);
  });

  describe("Normal Operation", () => {
    it("should call ensureNamespaceExists and runWorkflowWorkers", async () => {
      const composer = createMockComposer();

      await startWorkflowWorker(composer, {
        taskQueues: ["workflow-tasks"],
        maxConcurrentWorkflowTaskExecutions: 100,
        workflows: mockWorkflows,
      });

      expect(mockEnsureNamespaceExists).toHaveBeenCalledWith("localhost:7233", "default");
      expect(composer.runWorkflowWorkers).toHaveBeenCalledWith({
        taskQueues: ["workflow-tasks"],
        maxConcurrentWorkflowTaskExecutions: 100,
        workflows: mockWorkflows,
      });
    });

    it("should use temporal config from composer", async () => {
      const composer = createMockComposer({
        temporal: {
          serverAddress: "prod.temporal.io:7233",
          namespace: "production",
        },
      });

      await startWorkflowWorker(composer, {
        taskQueues: ["workflow-tasks"],
        maxConcurrentWorkflowTaskExecutions: 200,
        workflows: mockWorkflows,
      });

      expect(mockEnsureNamespaceExists).toHaveBeenCalledWith("prod.temporal.io:7233", "production");
      expect(composer.runWorkflowWorkers).toHaveBeenCalledWith({
        taskQueues: ["workflow-tasks"],
        maxConcurrentWorkflowTaskExecutions: 200,
        workflows: mockWorkflows,
      });
    });

    it("should log configuration on startup", async () => {
      const composer = createMockComposer();

      await startWorkflowWorker(composer, {
        taskQueues: ["workflow-tasks"],
        maxConcurrentWorkflowTaskExecutions: 100,
        workflows: mockWorkflows,
      });

      expect(composer.logger.info).toHaveBeenCalledWith(
        "Starting Workflow Workers",
        expect.objectContaining({
          serverAddress: "localhost:7233",
          namespace: "default",
          taskQueues: ["workflow-tasks"],
          maxConcurrentWorkflowTaskExecutions: 100,
        }),
      );
    });

    it("should pass multiple task queues", async () => {
      const composer = createMockComposer();

      await startWorkflowWorker(composer, {
        taskQueues: ["workflow-tasks", "priority-workflow-tasks"],
        maxConcurrentWorkflowTaskExecutions: 50,
        workflows: mockWorkflows,
      });

      expect(composer.runWorkflowWorkers).toHaveBeenCalledWith({
        taskQueues: ["workflow-tasks", "priority-workflow-tasks"],
        maxConcurrentWorkflowTaskExecutions: 50,
        workflows: mockWorkflows,
      });
    });
  });

  describe("Namespace Creation", () => {
    it("should ensure namespace by default", async () => {
      const composer = createMockComposer();

      await startWorkflowWorker(composer, {
        taskQueues: ["workflow-tasks"],
        maxConcurrentWorkflowTaskExecutions: 100,
        workflows: mockWorkflows,
      });

      expect(mockEnsureNamespaceExists).toHaveBeenCalledTimes(1);
    });

    it("should skip namespace creation when ensureNamespace is false", async () => {
      const composer = createMockComposer();

      await startWorkflowWorker(composer, {
        taskQueues: ["workflow-tasks"],
        maxConcurrentWorkflowTaskExecutions: 100,
        workflows: mockWorkflows,
        ensureNamespace: false,
      });

      expect(mockEnsureNamespaceExists).not.toHaveBeenCalled();
    });

    it("should ensure namespace when explicitly set to true", async () => {
      const composer = createMockComposer();

      await startWorkflowWorker(composer, {
        taskQueues: ["workflow-tasks"],
        maxConcurrentWorkflowTaskExecutions: 100,
        workflows: mockWorkflows,
        ensureNamespace: true,
      });

      expect(mockEnsureNamespaceExists).toHaveBeenCalledTimes(1);
    });
  });

  describe("Error Handling", () => {
    it("should exit with code 1 if runWorkflowWorkers fails", async () => {
      const composer = createMockComposer({
        runWorkflowWorkers: vi.fn().mockRejectedValue(new Error("Worker failed to start")),
      });

      await startWorkflowWorker(composer, {
        taskQueues: ["workflow-tasks"],
        maxConcurrentWorkflowTaskExecutions: 100,
        workflows: mockWorkflows,
      });

      expect(mockExit).toHaveBeenCalledWith(1);
      expect(composer.logger.error).toHaveBeenCalledWith(
        "Failed to start Workflow Workers",
        expect.objectContaining({ error: "Worker failed to start" }),
      );
    });

    it("should exit with code 1 if ensureNamespaceExists fails", async () => {
      const composer = createMockComposer();
      mockEnsureNamespaceExists.mockRejectedValue(new Error("Cannot connect to Temporal"));

      await startWorkflowWorker(composer, {
        taskQueues: ["workflow-tasks"],
        maxConcurrentWorkflowTaskExecutions: 100,
        workflows: mockWorkflows,
      });

      expect(mockExit).toHaveBeenCalledWith(1);
      expect(composer.logger.error).toHaveBeenCalledWith(
        "Failed to start Workflow Workers",
        expect.objectContaining({ error: "Cannot connect to Temporal" }),
      );
    });

    it("should not exit on successful startup", async () => {
      const composer = createMockComposer();

      await startWorkflowWorker(composer, {
        taskQueues: ["workflow-tasks"],
        maxConcurrentWorkflowTaskExecutions: 100,
        workflows: mockWorkflows,
      });

      expect(mockExit).not.toHaveBeenCalled();
    });
  });
});
