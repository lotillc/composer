/**
 * Composer Factory
 *
 * Creates a configured Composer instance with methods for running workflows
 * synchronously or asynchronously, and for starting activity workers.
 *
 * The composer provides a unified interface that uses the same context provider
 * for all execution modes, ensuring consistent context management.
 */

import { v7 as uuidv7 } from "uuid";
import { getDeploymentSeriesNames } from "../temporal-naming";
import { executeWorkflowTemporal, startWorkflowTemporal } from "./async/execute/workflow-execution";
import { runActivityWorkers as runActivityWorkersInternal } from "./async/register/activity-worker";
import { runWorkflowWorkers as runWorkflowWorkersInternal } from "./async/register/workflow-worker";
import type { ScheduleDefinition } from "./async/schedule/define-schedule";
import { syncSchedules as syncSchedulesInternal } from "./async/schedule/sync-schedules";
import type {
  AsyncWorkflowOptions,
  Composer,
  ComposerConfig,
  StepContextProvider,
  SyncComposer,
  TemporalConfig,
} from "./context-provider";
import type { Step } from "./dag-sync-step";
import { runSyncWorkflow, type Workflow, type WorkflowResult } from "./dag-sync-workflow";
import { defaultLogger } from "./defaults";
import type { UUIDV7 } from "./types";

/**
 * Creates a Composer instance with the given configuration.
 *
 * The composer provides methods for:
 * - `runSyncWorkflow`: Execute workflows synchronously (in-process)
 * - `runAsyncWorkflow`: Execute workflows via Temporal
 * - `runActivityWorkers`: Start Temporal activity workers
 *
 * All methods use the same context provider, ensuring consistent context management.
 *
 * @param config - Configuration including the context provider
 * @returns A configured Composer instance
 *
 * @example
 * ```typescript
 * const composer = createComposer({
 *   contextProvider: {
 *     beforeStep: async () => ({
 *       em: await IxDbConnection.getInstance().getForkedEntityManager(),
 *     }),
 *     afterStep: async (ctx, error) => {
 *       if (!error) await ctx.em.flush();
 *       ctx.em.clear();
 *     },
 *   },
 * });
 *
 * // Run sync workflow
 * const result = await composer.runSyncWorkflow(myWorkflow, initialData);
 *
 * // Run async workflow
 * const result = await composer.runAsyncWorkflow(myWorkflow, initialData);
 *
 * // Start activity workers (in worker script)
 * await composer.runActivityWorkers({
 *   serverAddress: "localhost:7233",
 *   namespace: "default",
 *   taskQueues: ["standard-tasks"],
 *   maxConcurrentActivityTaskExecutions: 10,
 * });
 * ```
 */
// Overload: with temporal config -> full Composer (sync + async)
export function createComposer<TContext>(
  config: ComposerConfig<TContext> & { temporal: TemporalConfig },
): Composer<TContext>;

// Overload: without temporal config -> sync-only Composer
export function createComposer<TContext>(config: ComposerConfig<TContext>): SyncComposer<TContext>;

// Implementation
export function createComposer<TContext>(
  config: ComposerConfig<TContext>,
): SyncComposer<TContext> | Composer<TContext> {
  const { contextProvider, deepFreeze, temporal } = config;

  // Resolve effective logger (use provided or default) before defining closures
  // so all closures capture the resolved logger consistently.
  const resolvedLogger = config.logger ?? defaultLogger;

  // Create a type-erased version for internal use
  const internalContextProvider: StepContextProvider<unknown> = {
    beforeStep: contextProvider.beforeStep,
    afterStep: contextProvider.afterStep as (ctx: unknown, error?: Error) => Promise<void>,
  };

  // Implementation functions with overloads for proper type enforcement
  // Overload for workflows WITHOUT required initial fields
  function runSyncWorkflowImpl<
    Bag extends Record<string, any>,
    Config extends Partial<Bag>,
    Steps extends readonly Step<Bag, any, any, any, any>[],
  >(
    workflow: Workflow<Bag, never, Config, Steps>,
    initialData?: Partial<Bag>,
  ): Promise<WorkflowResult<Bag>>;

  // Overload for workflows WITH required initial fields
  function runSyncWorkflowImpl<
    Bag extends Record<string, any>,
    RequiredInitial extends keyof Bag,
    Config extends Partial<Bag>,
    Steps extends readonly Step<Bag, any, any, any, any>[],
  >(
    workflow: Workflow<Bag, RequiredInitial, Config, Steps>,
    initialData: Partial<Bag> & Pick<Bag, RequiredInitial>,
  ): Promise<WorkflowResult<Bag>>;

  // Implementation
  function runSyncWorkflowImpl<
    Bag extends Record<string, any>,
    RequiredInitial extends keyof Bag,
    Config extends Partial<Bag>,
    Steps extends readonly Step<Bag, any, any, any, any>[],
  >(
    workflow: Workflow<Bag, RequiredInitial, Config, Steps>,
    initialData?: Partial<Bag> & Pick<Bag, RequiredInitial>,
  ): Promise<WorkflowResult<Bag>> {
    return runSyncWorkflow(
      workflow as Workflow<Bag, RequiredInitial, Config, any>,
      initialData as Partial<Bag> & Pick<Bag, RequiredInitial>,
      internalContextProvider,
      resolvedLogger,
      deepFreeze,
    ) as Promise<WorkflowResult<Bag>>;
  }

  // Base result (sync-only)
  const syncComposer: SyncComposer<TContext> = {
    contextProvider,
    logger: resolvedLogger,
    runSyncWorkflow: runSyncWorkflowImpl,
  };

  // If no temporal config, return sync-only composer
  if (!temporal) {
    return syncComposer;
  }

  // Capture after guard so TypeScript narrows the type for closures
  const temporalConfig = temporal;

  // Construct versioningOverride for workflow starts when buildId is set.
  // This pins new workflows to the same deployment version as the workers,
  // eliminating the TOCTOU gap that would require a separate setCurrentDeployment call.
  const seriesNames = getDeploymentSeriesNames(temporalConfig.serviceName);
  const workflowVersioningOverride = temporalConfig.buildId
    ? {
        pinnedTo: {
          buildId: temporalConfig.buildId,
          deploymentName: seriesNames.workflows,
        },
      }
    : undefined;

  // Build async methods when temporal is available
  // Overload for workflows WITHOUT required initial fields
  function runAsyncWorkflowImpl<
    Bag extends Record<string, any>,
    Config extends Partial<Bag>,
    Steps extends readonly Step<Bag, any, any, any, any>[],
  >(
    workflow: Workflow<Bag, never, Config, Steps>,
    initialData?: Partial<Bag>,
    options?: AsyncWorkflowOptions,
  ): Promise<WorkflowResult<Bag>>;

  // Overload for workflows WITH required initial fields
  function runAsyncWorkflowImpl<
    Bag extends Record<string, any>,
    RequiredInitial extends keyof Bag,
    Config extends Partial<Bag>,
    Steps extends readonly Step<Bag, any, any, any, any>[],
  >(
    workflow: Workflow<Bag, RequiredInitial, Config, Steps>,
    initialData: Partial<Bag> & Pick<Bag, RequiredInitial>,
    options?: AsyncWorkflowOptions,
  ): Promise<WorkflowResult<Bag>>;

  // Implementation
  function runAsyncWorkflowImpl<
    Bag extends Record<string, any>,
    RequiredInitial extends keyof Bag,
    Config extends Partial<Bag>,
    Steps extends readonly Step<Bag, any, any, any, any>[],
  >(
    workflow: Workflow<Bag, RequiredInitial, Config, Steps>,
    initialData?: Partial<Bag> & Pick<Bag, RequiredInitial>,
    options?: AsyncWorkflowOptions,
  ): Promise<WorkflowResult<Bag>> {
    const workflowId = options?.workflowId ? (options.workflowId as UUIDV7) : (uuidv7() as UUIDV7);
    return executeWorkflowTemporal(
      workflow as Workflow<Bag, RequiredInitial, Config, any>,
      initialData as Partial<Bag>,
      {
        workflowId,
        clientConfig: {
          address: temporalConfig.serverAddress,
          namespace: temporalConfig.namespace,
        },
        versioningOverride: workflowVersioningOverride,
        awaitCheckpoint: options?.awaitCheckpoint,
        startOnly: options?.startOnly,
      },
    );
  }

  // Fire-and-forget: start workflow without waiting for completion
  // Overload for workflows WITHOUT required initial fields
  function startAsyncWorkflowImpl<
    Bag extends Record<string, any>,
    Config extends Partial<Bag>,
    Steps extends readonly Step<Bag, any, any, any, any>[],
  >(
    workflow: Workflow<Bag, never, Config, Steps>,
    initialData?: Partial<Bag>,
  ): Promise<{ workflowId: string }>;

  // Overload for workflows WITH required initial fields
  function startAsyncWorkflowImpl<
    Bag extends Record<string, any>,
    RequiredInitial extends keyof Bag,
    Config extends Partial<Bag>,
    Steps extends readonly Step<Bag, any, any, any, any>[],
  >(
    workflow: Workflow<Bag, RequiredInitial, Config, Steps>,
    initialData: Partial<Bag> & Pick<Bag, RequiredInitial>,
  ): Promise<{ workflowId: string }>;

  // Implementation
  function startAsyncWorkflowImpl<
    Bag extends Record<string, any>,
    RequiredInitial extends keyof Bag,
    Config extends Partial<Bag>,
    Steps extends readonly Step<Bag, any, any, any, any>[],
  >(
    workflow: Workflow<Bag, RequiredInitial, Config, Steps>,
    initialData?: Partial<Bag> & Pick<Bag, RequiredInitial>,
  ): Promise<{ workflowId: string }> {
    const workflowId = uuidv7() as UUIDV7;
    return startWorkflowTemporal(
      workflow as Workflow<Bag, RequiredInitial, Config, any>,
      initialData as Partial<Bag>,
      {
        workflowId,
        clientConfig: {
          address: temporalConfig.serverAddress,
          namespace: temporalConfig.namespace,
        },
        versioningOverride: workflowVersioningOverride,
      },
    );
  }

  return {
    ...syncComposer,
    temporal: temporalConfig,
    runAsyncWorkflow: runAsyncWorkflowImpl,
    startAsyncWorkflow: startAsyncWorkflowImpl,

    async runActivityWorkers(config: {
      taskQueues: string[];
      maxConcurrentActivityTaskExecutions: number;
      workflows: Workflow<any, any, any>[];
    }): Promise<void> {
      return runActivityWorkersInternal({
        serverAddress: temporalConfig.serverAddress,
        namespace: temporalConfig.namespace,
        deploymentSeriesName: seriesNames.activities,
        buildId: temporalConfig.buildId,
        ...config,
        contextProvider: internalContextProvider,
        logger: resolvedLogger,
      });
    },

    async runWorkflowWorkers(config: {
      taskQueues: string[];
      maxConcurrentWorkflowTaskExecutions: number;
      workflows: Workflow<any, any, any>[];
    }): Promise<void> {
      return runWorkflowWorkersInternal({
        serverAddress: temporalConfig.serverAddress,
        namespace: temporalConfig.namespace,
        deploymentSeriesName: seriesNames.workflows,
        buildId: temporalConfig.buildId,
        ...config,
        logger: resolvedLogger,
      });
    },

    async syncSchedules(schedules: ScheduleDefinition[], options?: { dryRun?: boolean }) {
      return syncSchedulesInternal({
        temporalConfig: {
          address: temporalConfig.serverAddress,
          namespace: temporalConfig.namespace,
        },
        schedules,
        dryRun: options?.dryRun,
        logger: resolvedLogger,
      });
    },
  };
}
