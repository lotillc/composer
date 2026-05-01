/**
 * Start All Workers
 *
 * Generic framework function that starts both Temporal Workflow Workers and
 * Activity Workers in parallel from a configured Composer instance. This is a
 * convenience function for local development when you want to run the entire
 * worker system in a single process.
 *
 * ## Usage:
 *
 * ```typescript
 * import { startAllWorkers } from "@lotiai/composer";
 * import { composer } from "./my-app-composer";
 * import { myWorkflow } from "./workflows/my-workflow";
 *
 * await startAllWorkers(composer, {
 *   workflows: [myWorkflow],
 *   workflow: {
 *     taskQueues: ["workflow-tasks"],
 *     maxConcurrentWorkflowTaskExecutions: 100,
 *   },
 *   activity: {
 *     taskQueues: ["standard-tasks"],
 *     maxConcurrentActivityTaskExecutions: 15,
 *   },
 * });
 * ```
 *
 * ## Note:
 *
 * In production, you would typically run these workers in separate containers/processes
 * to enable independent scaling and deployment. This function is primarily for
 * local development convenience.
 *
 * @module start-all-workers
 */

import type { Composer } from "../../context-provider";
import type { Workflow } from "../../dag-sync-workflow";
import { ensureNamespaceExists } from "../utils/ensure-namespace";
import { type StartActivityWorkerOptions, startActivityWorker } from "./start-activity-worker";
import { type StartWorkflowWorkerOptions, startWorkflowWorker } from "./start-workflow-worker";

/**
 * Options for starting all workers (workflow + activity) in a single process.
 */
export interface StartAllWorkersOptions {
  /**
   * Workflow definitions shared by both workers.
   * Activity worker extracts steps; workflow worker generates plans.
   */
  workflows: Workflow<any, any, any>[];

  /**
   * Options for the workflow worker (task queues, concurrency).
   * The `ensureNamespace` and `workflows` options are controlled at the top level.
   */
  workflow: Omit<StartWorkflowWorkerOptions, "ensureNamespace" | "workflows">;

  /**
   * Options for the activity worker (task queues, concurrency).
   * The `ensureNamespace` and `workflows` options are controlled at the top level.
   */
  activity: Omit<StartActivityWorkerOptions, "ensureNamespace" | "workflows">;

  /**
   * Whether to ensure the Temporal namespace exists before starting workers.
   * When true, the namespace check is performed once before starting both workers.
   *
   * @default true
   */
  ensureNamespace?: boolean;
}

/**
 * Starts both Temporal Workflow Workers and Activity Workers in parallel
 * using a configured Composer instance.
 *
 * This function:
 * 1. Optionally ensures the Temporal namespace exists (once, not per worker)
 * 2. Logs the combined worker configuration
 * 3. Delegates to `startWorkflowWorker()` and `startActivityWorker()` via `Promise.all`
 *
 * @param composer - A fully configured Composer instance (must have temporal config)
 * @param options - Combined worker startup options
 */
export async function startAllWorkers<TContext>(
  composer: Composer<TContext>,
  options: StartAllWorkersOptions,
): Promise<void> {
  const logger = composer.logger;
  const { serverAddress, namespace } = composer.temporal;

  logger.info("Starting All Workers", {
    serverAddress,
    namespace,
    workflowTaskQueues: options.workflow.taskQueues,
    activityTaskQueues: options.activity.taskQueues,
    maxConcurrentWorkflowTaskExecutions: options.workflow.maxConcurrentWorkflowTaskExecutions,
    maxConcurrentActivityTaskExecutions: options.activity.maxConcurrentActivityTaskExecutions,
    ensureNamespace: options.ensureNamespace ?? true,
  });

  // Ensure namespace once before starting both workers
  if (options.ensureNamespace !== false) {
    await ensureNamespaceExists(serverAddress, namespace);
  }

  await Promise.all([
    startWorkflowWorker(composer, {
      ...options.workflow,
      workflows: options.workflows,
      ensureNamespace: false,
    }),
    startActivityWorker(composer, {
      ...options.activity,
      workflows: options.workflows,
      ensureNamespace: false,
    }),
  ]);
}
