/**
 * Schedule Definition
 *
 * Provides the `defineSchedule()` function for declaring Temporal schedule
 * definitions that reference composer workflows. Schedule definitions are
 * declarative -- they describe what should be scheduled, not how to create
 * the schedule on the Temporal server (that's handled at sync time by
 * `composer.syncSchedules()` / `runScheduleSync()`).
 *
 * Consumers export schedule definitions from a barrel (typically
 * `src/schedules/index.ts`) and pass them explicitly to a sync entrypoint --
 * the same "pass it in" pattern used for workflows and steps.
 *
 * @module define-schedule
 */

import type { ScheduleSpec } from "@temporalio/client";
import { ScheduleOverlapPolicy } from "@temporalio/client";
import type {
  ExtractWorkflowBag,
  ExtractWorkflowConfig,
  ExtractWorkflowRequiredInitial,
  Workflow,
} from "../../dag-sync-workflow";

export type { ScheduleSpec } from "@temporalio/client";
export { ScheduleOverlapPolicy } from "@temporalio/client";

/**
 * Extracts the required initial data type for a workflow, combining
 * RequiredInitial fields with ConfiguredValues keys.
 *
 * - If the workflow has RequiredInitial fields, those must be provided.
 * - ConfiguredValues are already baked into the workflow and don't need
 *   to be provided in initialData.
 */
type ScheduleInitialData<W extends Workflow<any, any, any, any, any>> = [
  ExtractWorkflowRequiredInitial<W>,
] extends [never]
  ? Partial<ExtractWorkflowBag<W>> | undefined
  : Partial<ExtractWorkflowBag<W>> &
      Pick<
        ExtractWorkflowBag<W>,
        Exclude<ExtractWorkflowRequiredInitial<W>, keyof ExtractWorkflowConfig<W>>
      >;

/**
 * Options for defining a schedule.
 *
 * @typeParam W - The workflow type this schedule is for
 */
export interface ScheduleDefinitionOptions<W extends Workflow<any, any, any, any, any>> {
  /**
   * Unique, stable identifier for this schedule on the Temporal server.
   *
   * This ID persists across deployments -- it's how `syncSchedules()` knows
   * which schedule to update. Choose a descriptive, kebab-case name.
   *
   * @example "daily-report", "weekly-cleanup", "hourly-health-check"
   */
  scheduleId: string;

  /**
   * The workflow to execute on this schedule.
   *
   * Pass the workflow definition object (the result of `createWorkflow(...).build(...)`.
   */
  workflow: W;

  /**
   * Static initial data for scheduled runs.
   *
   * Type-safe: must satisfy the workflow's RequiredInitial constraint.
   * For workflows with no required initial fields, this is optional.
   */
  initialData: ScheduleInitialData<W>;

  /**
   * When the schedule should fire.
   *
   * Uses Temporal's `ScheduleSpec` type directly. Supports:
   * - `calendars`: Calendar-based times (like cron but more readable)
   * - `intervals`: Repeating intervals (e.g., every 1 hour)
   * - `cronExpressions`: Legacy cron strings
   * - `timezone`: IANA timezone (default: UTC)
   * - `jitter`: Random jitter added to each trigger time
   * - `startAt` / `endAt`: Time bounds
   * - `skip`: Calendar specs for times to exclude
   *
   * @example
   * // Every day at 2am UTC
   * { calendars: [{ hour: 2 }] }
   *
   * @example
   * // Every Monday at 8:30am Eastern
   * { calendars: [{ hour: 8, minute: 30, dayOfWeek: "MONDAY" }], timezone: "US/Eastern" }
   *
   * @example
   * // Every hour
   * { intervals: [{ every: "1h" }] }
   */
  spec: ScheduleSpec;

  /**
   * What happens when a new run would start while a previous is still running.
   *
   * @default ScheduleOverlapPolicy.SKIP
   */
  overlap?: ScheduleOverlapPolicy;

  /**
   * Maximum time period for catching up missed runs.
   *
   * If the Temporal server was down or the schedule was paused, this controls
   * how far back it will try to execute missed runs on recovery.
   *
   * Accepts ms-formatted strings (e.g., "1d", "1h") or milliseconds as a number.
   *
   * @default "1 minute" (Temporal's default)
   * @example "1 day", "1h", "0" (no catchup)
   */
  catchupWindow?: number | string;

  /**
   * Whether to create the schedule in a paused state.
   *
   * @default false
   */
  paused?: boolean;

  /**
   * Human-readable note about this schedule.
   * Visible in the Temporal UI.
   */
  note?: string;

  /**
   * Task queue for the scheduled workflow.
   * @default "workflow-tasks"
   */
  taskQueue?: string;

  /**
   * Environments in which this schedule should be active.
   *
   * Must be a non-empty list of environment names (e.g. `"prod"`, `"preview"`).
   * At sync time, the schedule is only reconciled when the current environment
   * (`ENVIRONMENT_NAME`) matches one of these values. This keeps
   * production-only schedules from running in preview / local environments.
   *
   * @example ["prod"]
   * @example ["preview", "prod"]
   */
  environments: readonly string[];
}

/**
 * A schedule definition ready for syncing to the Temporal server.
 *
 * This is the runtime representation produced by `defineSchedule()`.
 * It carries the workflow name (extracted from the workflow definition)
 * and all schedule configuration.
 */
export interface ScheduleDefinition {
  /** Marker for type guard identification */
  readonly __scheduleDefinition: true;

  /** Unique schedule ID on the Temporal server */
  readonly scheduleId: string;

  /** Workflow name (unversioned -- versioning is applied at sync time) */
  readonly workflowName: string;

  /** Static initial data for scheduled runs */
  readonly initialData: Record<string, unknown>;

  /** Configured values from the workflow (merged at sync time, overriding initialData) */
  readonly configuredValues: Record<string, unknown>;

  /** When the schedule should fire */
  readonly spec: ScheduleSpec;

  /** Overlap policy */
  readonly overlap: ScheduleOverlapPolicy;

  /** Catchup window for missed runs */
  readonly catchupWindow?: number | string;

  /** Whether to create in paused state */
  readonly paused: boolean;

  /** Human-readable note */
  readonly note?: string;

  /** Task queue */
  readonly taskQueue: string;

  /** Environments in which this schedule should be active (non-empty). */
  readonly environments: readonly string[];
}

/**
 * Defines a Temporal schedule for a composer workflow.
 *
 * Creates a typed, declarative schedule definition that consumers export from
 * a `schedules/` barrel and pass explicitly to a sync entrypoint (e.g.
 * `runScheduleSync()`), mirroring how workflows are passed to
 * `startWorkflowWorker()`.
 *
 * @example
 * ```typescript
 * import { defineSchedule, ScheduleOverlapPolicy } from "@lotiai/composer";
 * import { dailyReportWorkflow } from "../workflows/daily-report-workflow";
 *
 * export const dailyReport = defineSchedule({
 *   scheduleId: "daily-report",
 *   workflow: dailyReportWorkflow,
 *   initialData: { reportType: "daily" },
 *   environments: ["prod"],
 *   spec: {
 *     calendars: [{ hour: 8, minute: 0 }],
 *     timezone: "US/Eastern",
 *   },
 *   overlap: ScheduleOverlapPolicy.SKIP,
 * });
 * ```
 */
export function defineSchedule<W extends Workflow<any, any, any, any, any>>(
  options: ScheduleDefinitionOptions<W>,
): ScheduleDefinition {
  if (!options.scheduleId || typeof options.scheduleId !== "string") {
    throw new Error("scheduleId is required and must be a non-empty string");
  }

  if (!options.workflow || typeof options.workflow.name !== "string") {
    throw new Error("workflow is required and must be a valid workflow definition");
  }

  if (!options.spec) {
    throw new Error("spec is required -- define when the schedule should fire");
  }

  if (
    !Array.isArray(options.environments) ||
    options.environments.length === 0 ||
    !options.environments.every((env) => typeof env === "string" && env.length > 0)
  ) {
    throw new Error(
      `environments is required for schedule "${options.scheduleId}" and must be a non-empty array of environment names (e.g. ["prod"])`,
    );
  }

  return {
    __scheduleDefinition: true,
    scheduleId: options.scheduleId,
    workflowName: options.workflow.name,
    initialData: (options.initialData ?? {}) as Record<string, unknown>,
    configuredValues: (options.workflow.configuredValues ?? {}) as Record<string, unknown>,
    spec: options.spec,
    overlap: options.overlap ?? ScheduleOverlapPolicy.SKIP,
    catchupWindow: options.catchupWindow,
    paused: options.paused ?? false,
    note: options.note,
    taskQueue: options.taskQueue ?? "workflow-tasks",
    environments: [...options.environments],
  };
}
