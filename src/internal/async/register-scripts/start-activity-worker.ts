/**
 * Start Activity Worker
 *
 * Generic framework function that starts Temporal Activity Workers from a
 * configured Composer instance. The Composer provides the Temporal connection
 * config (server address, namespace), context provider, and logger.
 *
 * The caller is responsible for determining task queues and concurrency settings
 * (e.g., from environment variables, worker profiles, or hardcoded defaults).
 *
 * ## Usage:
 *
 * ```typescript
 * import { startActivityWorker } from "@lotiai/composer";
 * import { composer } from "./my-app-composer";
 *
 * await startActivityWorker(composer, {
 *   taskQueues: ["standard-tasks"],
 *   maxConcurrentActivityTaskExecutions: 15,
 * });
 * ```
 *
 * @module start-activity-worker
 */

import type { Composer } from "../../context-provider";
import type { Workflow } from "../../dag-sync-workflow";
import { ensureNamespaceExists } from "../utils/ensure-namespace";

/**
 * Options for starting an activity worker.
 */
export interface StartActivityWorkerOptions {
  /**
   * Task queues to listen on for activity tasks.
   */
  taskQueues: string[];

  /**
   * Maximum number of concurrent activity executions per queue.
   */
  maxConcurrentActivityTaskExecutions: number;

  /**
   * Workflow definitions to register. Steps are extracted from these.
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
 * Starts Temporal Activity Workers using a configured Composer instance.
 *
 * This function:
 * 1. Optionally ensures the Temporal namespace exists (idempotent)
 * 2. Logs the worker configuration
 * 3. Delegates to `composer.runActivityWorkers()` which handles worker lifecycle
 *    (connection, bundle loading, shutdown signals)
 * 4. Exits with code 1 on failure
 *
 * The Composer instance provides:
 * - Temporal server address and namespace (from its temporal config)
 * - Context provider (for step execution lifecycle)
 * - Logger (for structured log output)
 *
 * @param composer - A fully configured Composer instance (must have temporal config)
 * @param options - Worker startup options (task queues, concurrency)
 */
export async function startActivityWorker<TContext>(
  composer: Composer<TContext>,
  options: StartActivityWorkerOptions,
): Promise<void> {
  const logger = composer.logger;
  const { serverAddress, namespace } = composer.temporal;

  logger.info("Starting Activity Workers", {
    serverAddress,
    namespace,
    taskQueues: options.taskQueues,
    maxConcurrentActivityTaskExecutions: options.maxConcurrentActivityTaskExecutions,
    ensureNamespace: options.ensureNamespace ?? true,
  });

  try {
    // Ensure namespace exists before starting workers (idempotent)
    if (options.ensureNamespace !== false) {
      await ensureNamespaceExists(serverAddress, namespace);
    }

    await composer.runActivityWorkers({
      taskQueues: options.taskQueues,
      maxConcurrentActivityTaskExecutions: options.maxConcurrentActivityTaskExecutions,
      workflows: options.workflows,
    });
  } catch (error) {
    logger.error("Failed to start Activity Workers", {
      error: error instanceof Error ? error.message : String(error),
    });
    process.exit(1);
  }
}
