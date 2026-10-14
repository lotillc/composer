// Composer factory and types

export {
  clearBuildConfigCache,
  defineBuildConfig,
  getBuildConfigFilePath,
  hasBuildConfigFile,
  type LoadBuildConfigOptions,
  type LoadedBuildConfig,
  loadBuildConfig,
} from "./build-config/index.js";
export type {
  ComposerBuildConfig,
  ComposerBuildConfigInput,
  EnvExclusiveOptIn,
} from "./build-config/schema.js";
export {
  type AsyncStepRuntime,
  type AsyncWorkflowOptions,
  type Composer,
  type ComposerConfig,
  type ComposerWorkerInterceptors,
  createComposer,
  createWorkflow,
  type DurationString,
  defineSchedule,
  type ErrorHandler,
  type FanOut,
  fanOut,
  type InferWorkflowResultFromWorkflow,
  isFanOutStep,
  type ScheduleDefinition,
  type ScheduleDefinitionOptions,
  ScheduleOverlapPolicy,
  type ScheduleSpec,
  type Step,
  type StepContextProvider,
  type StepPollPolicy,
  type StepRetryPolicy,
  type SyncComposer,
  type SyncSchedulesResult,
  scheduleDefinitionSchema,
  scheduleOverlapPolicySchema,
  step,
  syncSchedules,
  type TemporalConfig,
  use,
  type Workflow,
  type WorkflowBuilder,
  type WorkflowResult,
} from "./internal/index.js";
export {
  isScheduleDefinition,
  isWorkflow,
} from "./internal/async/build-scripts/utils/type-guards.js";
export {
  type ActivityWorkerRuntimeConfig,
  type BuiltInWorkerProfile,
  type CombinedWorkerConfig,
  type CombinedWorkerConfigOptions,
  DEFAULT_WORKER_PROFILE,
  getAllTaskQueues,
  getEffectiveProfileConfig,
  getEffectiveResources,
  getTaskQueueForProfile,
  findUndeclaredWorkerProfiles,
  getWorkerProfiles,
  isValidWorkerProfile,
  isWorkerProfileName,
  type LoadAndResolveActivityWorkerRuntimeConfigOptions,
  loadAndResolveActivityWorkerRuntimeConfig,
  loadAndResolveCombinedWorkerConfigForLocalDev,
  type ResolveActivityWorkerRuntimeConfigOptions,
  MAX_WORKER_PROFILE_NAME_LENGTH,
  type ResolvedProfilesForEnvironment,
  resolveActivityWorkerRuntimeConfig,
  resolveProfilesForEnvironment,
  WORKER_PROFILES,
  type WorkerProfile,
  type UndeclaredWorkerProfileStep,
  WORKER_PROFILE_NAME_PATTERN,
  type WorkerProfileConfig,
} from "./internal/async/config/worker-profiles.js";
export {
  type StartActivityWorkerOptions,
  startActivityWorker,
} from "./internal/async/register-scripts/start-activity-worker.js";
export {
  type StartAllWorkersOptions,
  startAllWorkers,
} from "./internal/async/register-scripts/start-all-workers.js";
export {
  type StartWorkflowWorkerOptions,
  startWorkflowWorker,
} from "./internal/async/register-scripts/start-workflow-worker.js";
export {
  runScheduleSync,
  type SyncScheduleScriptOptions,
} from "./internal/async/register-scripts/sync-schedules.js";
export {
  type RunSyncSchedulesCliOptions,
  runSyncSchedulesCli,
} from "./internal/async/register-scripts/sync-schedules-cli.js";
export {
  type SyncSchedulesViaLambdaOptions,
  syncSchedulesViaLambda,
} from "./internal/async/register-scripts/sync-schedules-via-lambda.js";
export { findPackageRoot } from "./internal/async/utils/find-package-root.js";
export {
  type ErrorMatchTarget,
  matchesError,
  StepNotReadyError,
  StepPollTimeoutError,
  WorkflowBatchError,
  WorkflowErrorHandlerFailure,
  WorkflowStepError,
} from "./internal/errors.js";
export { STEP_NOT_READY_CODE, STEP_POLL_TIMEOUT_CODE } from "./internal/poll-codes.js";
export type { ComposerLogger, TraceErrorMessage } from "./internal/types.js";
export type { WorkflowExecutionStatusName } from "@temporalio/client";
// Re-exported so a consumer can type a converter or an interceptor without taking a direct
// dependency on the Temporal packages, which pnpm would not otherwise resolve for them.
export type { DataConverter, FailureConverter, PayloadCodec } from "@temporalio/common";
export type {
  ActivityExecuteInput,
  ActivityInboundCallsInterceptor,
  ActivityInterceptors,
  ActivityInterceptorsFactory,
} from "@temporalio/worker";
// Workflow-side `wf.log` lines are routed by Temporal, not by the injected ComposerLogger.
// Re-exported so a consumer can install their own logger on the Runtime without a direct
// dependency on @temporalio/worker; Composer never installs one itself. See the README.
export { Runtime, type RuntimeOptions } from "@temporalio/worker";

/**
 * Registry of worker profile names.
 *
 * Augment it to add a consumer-defined profile; each key becomes a valid
 * `WorkerProfile`, so a step pinned to an unregistered name fails typecheck.
 * Declare the profile in `composer.build-config.ts`'s `workerProfiles` as
 * well, so a worker is deployed for it. The value type is unused.
 *
 * Declared here rather than beside `WorkerProfile` because module
 * augmentation merges only into the module that declares an interface.
 *
 * @example
 * ```typescript
 * declare module "@lotiai/composer" {
 *   interface WorkerProfileRegistry {
 *     "request-signing": true;
 *   }
 * }
 * // Keeps the file a module: a .d.ts without one would shadow the package.
 * export {};
 * ```
 */
export interface WorkerProfileRegistry {
  standard: true;
  heavy: true;
}
