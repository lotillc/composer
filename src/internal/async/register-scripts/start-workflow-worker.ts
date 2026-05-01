/**
 * Start Workflow Worker
 *
 * Generic framework function that starts Temporal Workflow Workers from a
 * configured Composer instance. The Composer provides the Temporal connection
 * config (server address, namespace) and logger.
 *
 * The caller is responsible for determining task queues and concurrency settings
 * (e.g., from environment variables or hardcoded defaults).
 *
 * ## Usage:
 *
 * ```typescript
 * import { startWorkflowWorker } from "@lotiai/composer";
 * import { composer } from "./my-app-composer";
 *
 * await startWorkflowWorker(composer, {
 *   taskQueues: ["workflow-tasks"],
 *   maxConcurrentWorkflowTaskExecutions: 100,
 * });
 * ```
 *
 * @module start-workflow-worker
 */

import type { Composer } from "../../context-provider";
import type { Workflow } from "../../dag-sync-workflow";
import { ensureNamespaceExists } from "../utils/ensure-namespace";

/**
 * Options for starting a workflow worker.
 */
export interface StartWorkflowWorkerOptions {
  /**
   * Task queues to listen on for workflow tasks.
   */
  taskQueues: string[];

  /**
   * Maximum number of concurrent workflow task executions per queue.
   */
  maxConcurrentWorkflowTaskExecutions: number;

  /**
   * Workflow definitions to register. Plans are generated from these.
   */
  workflows: Workflow<any, any, any>[];

  /**
   * Whether to ensure the Temporal namespace exists before starting workers.
   * Useful for local development where the namespace may not exist yet.
   *
   * @default true
   */
  ensureNamespace?: boolean;
}

/**
 * Starts Temporal Workflow Workers using a configured Composer instance.
 *
 * This function:
 * 1. Optionally ensures the Temporal namespace exists (idempotent)
 * 2. Logs the worker configuration
 * 3. Delegates to `composer.runWorkflowWorkers()` which handles worker lifecycle
 *    (connection, bundle loading, shutdown signals)
 * 4. Exits with code 1 on failure
 *
 * The Composer instance provides:
 * - Temporal server address and namespace (from its temporal config)
 * - Logger (for structured log output)
 *
 * @param composer - A fully configured Composer instance (must have temporal config)
 * @param options - Worker startup options (task queues, concurrency)
 */
export async function startWorkflowWorker<TContext>(
  composer: Composer<TContext>,
  options: StartWorkflowWorkerOptions,
): Promise<void> {
  const logger = composer.logger;
  const { serverAddress, namespace } = composer.temporal;

  logger.info("Starting Workflow Workers", {
    serverAddress,
    namespace,
    taskQueues: options.taskQueues,
    maxConcurrentWorkflowTaskExecutions: options.maxConcurrentWorkflowTaskExecutions,
    ensureNamespace: options.ensureNamespace ?? true,
  });

  try {
    // Ensure namespace exists before starting workers (idempotent)
    if (options.ensureNamespace !== false) {
      await ensureNamespaceExists(serverAddress, namespace);
    }

    await composer.runWorkflowWorkers({
      taskQueues: options.taskQueues,
      maxConcurrentWorkflowTaskExecutions: options.maxConcurrentWorkflowTaskExecutions,
      workflows: options.workflows,
    });
  } catch (error) {
    logger.error("Failed to start Workflow Workers", {
      error: error instanceof Error ? error.message : String(error),
    });
    process.exit(1);
  }
}
