/**
 * Context Provider Types for Composer
 *
 * This module defines the generic context provider pattern that allows
 * steps to receive context (e.g., database connections) via lifecycle hooks.
 *
 * The context provider is configured once via createComposer() and used
 * for both sync and async workflow execution.
 */

import type { ScheduleDefinition } from "./async/schedule/define-schedule";
import type { SyncSchedulesResult } from "./async/schedule/sync-schedules";
import type {
  ExtractWorkflowBag,
  ExtractWorkflowCheckpointNames,
  ExtractWorkflowConfig,
  ExtractWorkflowRequiredInitial,
  ExtractWorkflowSteps,
  InferWorkflowResult,
  SafeConfiguredKeys,
  Workflow,
  WorkflowResult,
} from "./dag-sync-workflow";
import type { ComposerLogger } from "./types";

/**
 * Step Context Provider - manages context lifecycle for steps.
 *
 * Implement this interface to provide context (e.g., database connections,
 * loggers) to steps. The beforeStep/afterStep hooks ensure proper lifecycle
 * management (creation and cleanup).
 *
 * @typeParam TContext - The context type that steps will receive
 *
 * @example
 * ```typescript
 * const contextProvider: StepContextProvider<{ em: SqlEntityManager }> = {
 *   beforeStep: async (stepName) => ({
 *     em: await IxDbConnection.getInstance().getForkedEntityManager(),
 *   }),
 *   afterStep: async (ctx, error) => {
 *     if (!error) await ctx.em.flush();
 *     ctx.em.clear();
 *   },
 * };
 * ```
 */
export interface StepContextProvider<TContext> {
  /**
   * Called before each step executes.
   * Returns the context that will be passed to the step's run function.
   *
   * @param stepName - The name of the step about to execute
   * @returns The context for this step execution
   */
  beforeStep: (stepName: string) => Promise<TContext>;

  /**
   * Called after each step completes (success or failure).
   * Use this to clean up resources (e.g., clear entity manager).
   *
   * @param ctx - The context that was passed to the step
   * @param error - The error if the step failed, undefined if successful
   */
  afterStep: (ctx: TContext, error?: Error) => Promise<void>;
}

/**
 * Configuration for creating a Composer instance.
 *
 * @typeParam TContext - The context type that steps will receive
 *
 * @example
 * ```typescript
 * const composer = createComposer({
 *   contextProvider: myContextProvider,
 *   logger: myPinoLogger,     // optional, defaults to console
 *   deepFreeze: true,         // optional, defaults to false
 * });
 * ```
 */
export interface ComposerConfig<TContext> {
  /**
   * The context provider that manages step context lifecycle.
   */
  contextProvider: StepContextProvider<TContext>;

  /**
   * Custom logger for framework diagnostics.
   *
   * If not provided, defaults to a console-based logger.
   * The logger is used for internal framework messages (step cleanup failures,
   * error handler issues, workflow success/failure summaries).
   *
   * Any object with `info`, `warn`, `error`, and `debug` methods works.
   * Compatible with console, pino, winston, and most logging libraries.
   */
  logger?: ComposerLogger;

  /**
   * Enable deep-freezing of step outputs for immutability protection.
   *
   * When enabled, all step outputs are deeply frozen before being merged into
   * the shared bag. This prevents mutation bugs (parallel batch mutations,
   * downstream mutations, post-return mutations) at the cost of a small
   * performance overhead (~0.5-10ms per output depending on object size).
   *
   * Recommended for development and testing. In production, enable unless
   * profiling shows it's a bottleneck.
   *
   * @default false
   */
  deepFreeze?: boolean;

  /**
   * Temporal server configuration for async workflow execution.
   *
   * When provided, `createComposer` returns a full `Composer` with async methods.
   * When omitted, it returns a `SyncComposer` with only `runSyncWorkflow`.
   */
  temporal?: TemporalConfig;
}

/**
 * Temporal server connection configuration.
 */
export interface TemporalConfig {
  /** Temporal server address (e.g. "localhost:7233") */
  serverAddress: string;
  /** Temporal namespace (e.g. "default") */
  namespace: string;
  /**
   * Service name used as the base for Worker Versioning deployment series
   * (e.g., "orders-service"). Activity and workflow workers derive their
   * series names as "{serviceName}-activities" and "{serviceName}-workflows".
   */
  serviceName: string;
  /**
   * Build identifier for Worker Versioning (typically git commit SHA).
   * When set, enables versioning for both worker registration and workflow starts.
   * When unset (e.g., local dev), Worker Versioning is disabled.
   */
  buildId?: string;
}

/**
 * Options for async workflow execution.
 *
 * @typeParam CheckpointNames - Union of valid checkpoint names for the workflow (defaults to `never`)
 */
export interface AsyncWorkflowOptions<CheckpointNames extends string = never> {
  /**
   * Optional caller-supplied workflow ID.
   *
   * If omitted, Composer generates a UUIDv7 automatically.
   */
  workflowId?: string;

  /**
   * Checkpoint name to await for early return.
   *
   * If specified, returns partial bag state when the checkpoint is reached
   * instead of waiting for full workflow completion. The workflow continues
   * executing in the background after the checkpoint.
   *
   * Useful for fast HTTP responses while long-running work continues.
   *
   * @example
   * ```typescript
   * // Workflow with checkpoints
   * const wf = createWorkflow<Bag>("my-workflow")
   *   .build([step1, step2, step3])
   *   .checkpoint("earlyReturn", { afterStep: step1 });
   *
   * // Get partial result after step1 completes
   * const result = await composer.runAsyncWorkflow(wf, data, {
   *   awaitCheckpoint: "earlyReturn",
   * });
   * ```
   */
  // Valid checkpoint names are determined solely from the workflow definition.
  // NoInfer ensures TypeScript doesn't infer CheckpointNames from this property's value;
  // instead, awaitCheckpoint must match a checkpoint actually defined on the workflow.
  //
  // When CheckpointNames is `never` (no checkpoints defined), we use a descriptive
  // error string type instead of `never` to produce a helpful compile error message.
  awaitCheckpoint?: [CheckpointNames] extends [never]
    ? "ERROR: Cannot use awaitCheckpoint - this workflow has no checkpoints defined"
    : NoInfer<CheckpointNames>;

  /**
   * Start the Temporal workflow and return immediately after Temporal accepts it.
   *
   * This is intended for long-running fire-and-forget workflows where the caller
   * only needs to confirm handoff to Temporal, not wait for business completion.
   */
  startOnly?: boolean;
}

/**
 * Sync-only Composer instance.
 *
 * Returned by `createComposer()` when no `temporal` config is provided.
 * Supports only synchronous (in-process) workflow execution.
 *
 * Workflows never throw - errors are returned in `result.error`.
 * Check `result.error` to handle failures, use `result.bag` for success data.
 *
 * @typeParam TContext - The context type that steps will receive
 */
export interface SyncComposer<TContext> {
  /**
   * Run a workflow synchronously (in-process).
   *
   * Best for: tests, local development, simple use cases.
   *
   * @param workflow - The workflow to execute
   * @param initialData - Initial data for the workflow bag
   * @returns WorkflowResult with bag and optional error
   *
   * @example
   * ```typescript
   * const result = await composer.runSyncWorkflow(myWorkflow, data);
   * if (result.error) {
   *   // Handle error
   * }
   * // Use result.bag
   * ```
   */
  // Overload for workflows WITHOUT required initial fields
  runSyncWorkflow<W extends Workflow<any, never, any, any, any>>(
    workflow: W,
    initialData?: Partial<ExtractWorkflowBag<W>>,
  ): Promise<
    WorkflowResult<
      InferWorkflowResult<
        ExtractWorkflowBag<W>,
        ExtractWorkflowSteps<W>,
        never,
        SafeConfiguredKeys<ExtractWorkflowConfig<W>, ExtractWorkflowBag<W>>
      > &
        Partial<ExtractWorkflowBag<W>>
    >
  >;

  // Overload for workflows WITH required initial fields
  runSyncWorkflow<W extends Workflow<any, any, any, any, any>>(
    workflow: W,
    initialData: Partial<ExtractWorkflowBag<W>> &
      Pick<ExtractWorkflowBag<W>, ExtractWorkflowRequiredInitial<W>>,
  ): Promise<
    WorkflowResult<
      InferWorkflowResult<
        ExtractWorkflowBag<W>,
        ExtractWorkflowSteps<W>,
        ExtractWorkflowRequiredInitial<W>,
        SafeConfiguredKeys<ExtractWorkflowConfig<W>, ExtractWorkflowBag<W>>
      >
    >
  >;

  /**
   * Access the underlying context provider.
   * Useful for advanced use cases or when you need to share the provider.
   */
  contextProvider: StepContextProvider<TContext>;

  /**
   * The logger used for framework diagnostics.
   * This is the resolved logger (either the one passed to createComposer or the default).
   */
  readonly logger: ComposerLogger;
}

/**
 * Full Composer instance with sync and async workflow support.
 *
 * Returned by `createComposer()` when `temporal` config is provided.
 * Supports both synchronous and Temporal-based async workflow execution.
 *
 * @typeParam TContext - The context type that steps will receive
 */
export interface Composer<TContext> extends SyncComposer<TContext> {
  /**
   * Run a workflow asynchronously via Temporal.
   *
   * Best for: production, long-running workflows, reliability.
   *
   * @param workflow - The workflow to execute
   * @param initialData - Initial data for the workflow bag
   * @param options - Optional execution options (e.g., awaitCheckpoint for early return)
   * @returns WorkflowResult with bag and optional error
   *
   * @example
   * ```typescript
   * // Full workflow completion
   * const result = await composer.runAsyncWorkflow(myWorkflow, data);
   *
   * // Early return at checkpoint (workflow continues in background)
   * const partialResult = await composer.runAsyncWorkflow(myWorkflow, data, {
   *   awaitCheckpoint: "created",
   * });
   * ```
   */
  // Overload for workflows WITHOUT required initial fields
  runAsyncWorkflow<W extends Workflow<any, never, any, any, any>>(
    workflow: W,
    initialData?: Partial<ExtractWorkflowBag<W>>,
    options?: AsyncWorkflowOptions<ExtractWorkflowCheckpointNames<W>>,
  ): Promise<
    WorkflowResult<
      InferWorkflowResult<
        ExtractWorkflowBag<W>,
        ExtractWorkflowSteps<W>,
        never,
        SafeConfiguredKeys<ExtractWorkflowConfig<W>, ExtractWorkflowBag<W>>
      > &
        Partial<ExtractWorkflowBag<W>>
    >
  >;

  // Overload for workflows WITH required initial fields
  runAsyncWorkflow<W extends Workflow<any, any, any, any, any>>(
    workflow: W,
    initialData: Partial<ExtractWorkflowBag<W>> &
      Pick<ExtractWorkflowBag<W>, ExtractWorkflowRequiredInitial<W>>,
    options?: AsyncWorkflowOptions<ExtractWorkflowCheckpointNames<W>>,
  ): Promise<
    WorkflowResult<
      InferWorkflowResult<
        ExtractWorkflowBag<W>,
        ExtractWorkflowSteps<W>,
        ExtractWorkflowRequiredInitial<W>,
        SafeConfiguredKeys<ExtractWorkflowConfig<W>, ExtractWorkflowBag<W>>
      >
    >
  >;

  /**
   * Start an async workflow without waiting for completion (fire-and-forget).
   *
   * Returns only the workflowId. The workflow executes in the background
   * on Temporal workers. Use this for 202 Accepted HTTP responses.
   *
   * @example
   * ```typescript
   * const { workflowId } = await composer.startAsyncWorkflow(myWorkflow, data);
   * reply.code(202).send({ workflowId });
   * ```
   */
  // Overload for workflows WITHOUT required initial fields
  startAsyncWorkflow<W extends Workflow<any, never, any, any, any>>(
    workflow: W,
    initialData?: Partial<ExtractWorkflowBag<W>>,
  ): Promise<{ workflowId: string }>;

  // Overload for workflows WITH required initial fields
  startAsyncWorkflow<W extends Workflow<any, any, any, any, any>>(
    workflow: W,
    initialData: Partial<ExtractWorkflowBag<W>> &
      Pick<ExtractWorkflowBag<W>, ExtractWorkflowRequiredInitial<W>>,
  ): Promise<{ workflowId: string }>;

  /**
   * The Temporal connection configuration.
   * Available on full Composer instances (created with temporal config).
   */
  readonly temporal: TemporalConfig;

  /**
   * Start activity workers for Temporal execution.
   *
   * This starts long-running worker processes that execute step business logic.
   * The Temporal server address, namespace, context provider, and logger are
   * automatically injected from the Composer's configuration.
   *
   * Call this in your worker startup scripts.
   *
   * @param config - Activity worker configuration (queues, concurrency)
   */
  runActivityWorkers: (config: {
    taskQueues: string[];
    maxConcurrentActivityTaskExecutions: number;
    workflows: Workflow<any, any, any>[];
  }) => Promise<void>;

  /**
   * Start workflow workers for Temporal execution.
   *
   * This starts long-running worker processes that generate workflow plans and
   * execute workflow orchestration logic (batch sequencing, parallel execution).
   * The Temporal server address, namespace, and logger are automatically injected
   * from the Composer's configuration.
   *
   * Call this in your worker startup scripts.
   *
   * @param config - Workflow worker configuration (queues, concurrency, workflows)
   */
  runWorkflowWorkers: (config: {
    taskQueues: string[];
    maxConcurrentWorkflowTaskExecutions: number;
    workflows: Workflow<any, any, any>[];
  }) => Promise<void>;

  /**
   * Declaratively sync schedule definitions to the Temporal server.
   *
   * Creates new schedules, updates existing ones, and deletes composer-managed
   * schedules that are no longer in the definitions. Non-composer schedules
   * (without the `managedBy: "composer"` memo) are never touched.
   *
   * @param schedules - Schedule definitions (source of truth)
   * @param options - Optional sync options
   * @returns Summary of what was created, updated, and deleted
   */
  syncSchedules: (
    schedules: ScheduleDefinition[],
    options?: { dryRun?: boolean },
  ) => Promise<SyncSchedulesResult>;
}
