// Composer factory and types

export {
  clearBuildConfigCache,
  defineBuildConfig,
  getBuildConfigFilePath,
  hasBuildConfigFile,
  type LoadBuildConfigOptions,
  type LoadedBuildConfig,
  loadBuildConfig,
} from "./build-config/index";
export type {
  ComposerBuildConfig,
  ComposerBuildConfigInput,
  EnvExclusiveOptIn,
} from "./build-config/schema";
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
} from "./internal";
export {
  isScheduleDefinition,
  isWorkflow,
} from "./internal/async/build-scripts/utils/type-guards";
export {
  type ActivityWorkerRuntimeConfig,
  type CombinedWorkerConfig,
  type CombinedWorkerConfigOptions,
  DEFAULT_WORKER_PROFILE,
  getAllTaskQueues,
  getEffectiveProfileConfig,
  getEffectiveResources,
  getTaskQueueForProfile,
  isValidWorkerProfile,
  type LoadAndResolveActivityWorkerRuntimeConfigOptions,
  loadAndResolveActivityWorkerRuntimeConfig,
  loadAndResolveCombinedWorkerConfigForLocalDev,
  type ResolveActivityWorkerRuntimeConfigOptions,
  type ResolvedProfilesForEnvironment,
  resolveActivityWorkerRuntimeConfig,
  resolveProfilesForEnvironment,
  WORKER_PROFILES,
  type WorkerProfile,
  type WorkerProfileConfig,
} from "./internal/async/config/worker-profiles";
export {
  type StartActivityWorkerOptions,
  startActivityWorker,
} from "./internal/async/register-scripts/start-activity-worker";
export {
  type StartAllWorkersOptions,
  startAllWorkers,
} from "./internal/async/register-scripts/start-all-workers";
export {
  type StartWorkflowWorkerOptions,
  startWorkflowWorker,
} from "./internal/async/register-scripts/start-workflow-worker";
export {
  runScheduleSync,
  type SyncScheduleScriptOptions,
} from "./internal/async/register-scripts/sync-schedules";
export {
  type RunSyncSchedulesCliOptions,
  runSyncSchedulesCli,
} from "./internal/async/register-scripts/sync-schedules-cli";
export {
  type SyncSchedulesViaLambdaOptions,
  syncSchedulesViaLambda,
} from "./internal/async/register-scripts/sync-schedules-via-lambda";
export { findPackageRoot } from "./internal/async/utils/find-package-root";
export {
  type ErrorMatchTarget,
  matchesError,
  WorkflowBatchError,
  WorkflowErrorHandlerFailure,
  WorkflowStepError,
} from "./internal/errors";
export type { ComposerLogger, TraceErrorMessage } from "./internal/types";
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
