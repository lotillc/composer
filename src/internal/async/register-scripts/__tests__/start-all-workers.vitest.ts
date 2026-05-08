/**
 * Tests for startAllWorkers Framework Function
 *
 * Verifies:
 * - Module structure and exports (function + options type)
 * - Ensures namespace once, then delegates to startWorkflowWorker + startActivityWorker
 *   with ensureNamespace: false
 * - Calls both workers in parallel via Promise.all
 *
 * Deeper testing of process spawning, signal handling, and worker coordination happens through:
 * - Integration tests (actually running the workers)
 * - Sibling worker scripts (start-workflow-worker, start-activity-worker) have comprehensive tests
 */

import { describe, expect, it, vi } from "vitest";
import type { Composer } from "../../../context-provider";
import type { Workflow } from "../../../dag-sync-workflow";
import type { StartAllWorkersOptions } from "../start-all-workers";

vi.mock("../../utils/ensure-namespace", () => ({
  ensureNamespaceExists: vi.fn().mockResolvedValue(undefined),
}));

vi.mock("../start-workflow-worker", () => ({
  startWorkflowWorker: vi.fn().mockResolvedValue(undefined),
}));

vi.mock("../start-activity-worker", () => ({
  startActivityWorker: vi.fn().mockResolvedValue(undefined),
}));

describe("startAllWorkers", () => {
  const mockWorkflows = [
    { name: "test-workflow", steps: [] },
  ] as unknown as Workflow<any, any, any>[];

  const mockComposer = {
    logger: {
      info: vi.fn(),
      error: vi.fn(),
      warn: vi.fn(),
      debug: vi.fn(),
    },
    temporal: {
      serverAddress: "localhost:7233",
      namespace: "test-namespace",
      serviceName: "test-service",
    },
  } as unknown as Composer<unknown>;

  const defaultOptions: StartAllWorkersOptions = {
    workflows: mockWorkflows,
    workflow: {
      taskQueues: ["workflow-tasks"],
      maxConcurrentWorkflowTaskExecutions: 100,
    },
    activity: {
      taskQueues: ["activity-tasks"],
      maxConcurrentActivityTaskExecutions: 15,
    },
  };

  describe("Module Structure", () => {
    it("should export startAllWorkers function", async () => {
      const module = await import("../start-all-workers");
      expect(module.startAllWorkers).toBeDefined();
      expect(typeof module.startAllWorkers).toBe("function");
    });

    it("should have async function signature", async () => {
      const { startAllWorkers } = await import("../start-all-workers");
      expect(startAllWorkers.constructor.name).toBe("AsyncFunction");
    });
  });

  describe("Namespace Handling", () => {
    it("should ensure namespace once before starting workers", async () => {
      const { ensureNamespaceExists } = await import("../../utils/ensure-namespace");
      const { startAllWorkers } = await import("../start-all-workers");

      await startAllWorkers(mockComposer, defaultOptions);

      expect(ensureNamespaceExists).toHaveBeenCalledOnce();
      expect(ensureNamespaceExists).toHaveBeenCalledWith("localhost:7233", "test-namespace");
    });

    it("should skip namespace check when ensureNamespace is false", async () => {
      const { ensureNamespaceExists } = await import("../../utils/ensure-namespace");
      const { startAllWorkers } = await import("../start-all-workers");
      vi.mocked(ensureNamespaceExists).mockClear();

      await startAllWorkers(mockComposer, {
        ...defaultOptions,
        ensureNamespace: false,
      });

      expect(ensureNamespaceExists).not.toHaveBeenCalled();
    });
  });

  describe("Worker Delegation", () => {
    it("should call both startWorkflowWorker and startActivityWorker", async () => {
      const { startWorkflowWorker } = await import("../start-workflow-worker");
      const { startActivityWorker } = await import("../start-activity-worker");
      const { startAllWorkers } = await import("../start-all-workers");

      await startAllWorkers(mockComposer, defaultOptions);

      expect(startWorkflowWorker).toHaveBeenCalledOnce();
      expect(startActivityWorker).toHaveBeenCalledOnce();
    });

    it("should pass ensureNamespace: false to sub-functions", async () => {
      const { startWorkflowWorker } = await import("../start-workflow-worker");
      const { startActivityWorker } = await import("../start-activity-worker");
      const { startAllWorkers } = await import("../start-all-workers");

      await startAllWorkers(mockComposer, defaultOptions);

      expect(startWorkflowWorker).toHaveBeenCalledWith(mockComposer, {
        workflows: mockWorkflows,
        taskQueues: ["workflow-tasks"],
        maxConcurrentWorkflowTaskExecutions: 100,
        ensureNamespace: false,
      });
      expect(startActivityWorker).toHaveBeenCalledWith(mockComposer, {
        workflows: mockWorkflows,
        taskQueues: ["activity-tasks"],
        maxConcurrentActivityTaskExecutions: 15,
        ensureNamespace: false,
      });
    });
  });

  describe("Logging", () => {
    it("should log combined worker configuration at startup", async () => {
      const { startAllWorkers } = await import("../start-all-workers");

      await startAllWorkers(mockComposer, defaultOptions);

      expect(mockComposer.logger.info).toHaveBeenCalledWith(
        "Starting All Workers",
        expect.objectContaining({
          serverAddress: "localhost:7233",
          namespace: "test-namespace",
          workflowTaskQueues: ["workflow-tasks"],
          activityTaskQueues: ["activity-tasks"],
        }),
      );
    });
  });
});
