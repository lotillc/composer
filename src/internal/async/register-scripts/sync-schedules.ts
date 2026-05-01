/**
 * Run Schedule Sync
 *
 * Generic framework function that syncs a list of `ScheduleDefinition`s to the
 * Temporal server configured on a Composer instance. Mirrors the shape of
 * `startActivityWorker` / `startWorkflowWorker`: the caller supplies the
 * schedules explicitly (no directory scanning), and this wrapper handles the
 * cross-cutting concerns -- current-environment filtering, `DRY_RUN` env var,
 * structured logging, and non-zero exit on sync errors.
 *
 * ## Usage
 *
 * ```typescript
 * import { runScheduleSync } from "@lotiai/composer";
 * import { composer } from "./my-app-composer";
 * import * as schedules from "./schedules";
 *
 * await runScheduleSync(composer, {
 *   schedules: Object.values(schedules).filter(isScheduleDefinition),
 * });
 * ```
 *
 * @module sync-schedules
 */
import type { Composer } from "../../context-provider";
import type { ScheduleDefinition } from "../schedule/define-schedule";

/**
 * Options for `runScheduleSync`.
 */
export interface SyncScheduleScriptOptions {
  /**
   * Schedule definitions to reconcile against the Temporal server.
   * The caller is responsible for discovering these (typically by filtering a
   * schedules barrel export with `isScheduleDefinition`).
   */
  schedules: ScheduleDefinition[];

  /**
   * Name of the current environment (e.g. "prod"). Used to filter schedules
   * that declare an `environments` allowlist.
   *
   * @default process.env.ENVIRONMENT_NAME
   */
  currentEnvironment?: string | null;

  /**
   * If true, log what would happen without making changes on the server.
   *
   * @default process.env.COMPOSER_SCHEDULE_SYNC_DRY_RUN === "true"
   */
  dryRun?: boolean;
}

/**
 * Reconciles composer-managed Temporal schedules with the supplied list using
 * the Composer's configured Temporal connection.
 *
 * This function:
 * 1. Resolves the current environment (options or `ENVIRONMENT_NAME`) and
 *    dry-run flag (options or `DRY_RUN`).
 * 2. Filters schedules by their `environments` allowlist.
 * 3. Logs the resolved configuration and any skipped schedules.
 * 4. Delegates to `composer.syncSchedules()` for the actual reconciliation.
 * 5. Logs a create/update/delete summary.
 * 6. Exits with code 1 if any schedule failed to sync.
 *
 * @param composer - Composer instance providing the Temporal connection + logger
 * @param options - Schedules, current environment, and dry-run flag
 */
export async function runScheduleSync<TContext>(
  composer: Composer<TContext>,
  options: SyncScheduleScriptOptions,
): Promise<void> {
  const logger = composer.logger;
  const { serverAddress, namespace } = composer.temporal;

  const currentEnvironment = options.currentEnvironment ?? process.env.ENVIRONMENT_NAME ?? null;
  const dryRun = options.dryRun ?? process.env.COMPOSER_SCHEDULE_SYNC_DRY_RUN === "true";

  if (currentEnvironment === null && options.schedules.length > 0) {
    logger.error("Cannot sync schedules: no current environment was provided", {
      hint: "Set ENVIRONMENT_NAME or pass options.currentEnvironment to runScheduleSync",
      scheduleCount: options.schedules.length,
    });
    process.exit(1);
  }

  const activeSchedules: ScheduleDefinition[] = [];
  const skippedSchedules: Array<{ scheduleId: string; environments: readonly string[] }> = [];

  for (const schedule of options.schedules) {
    if (currentEnvironment !== null && schedule.environments.includes(currentEnvironment)) {
      activeSchedules.push(schedule);
    } else {
      skippedSchedules.push({
        scheduleId: schedule.scheduleId,
        environments: schedule.environments,
      });
    }
  }

  logger.info("Syncing Temporal schedules", {
    serverAddress,
    namespace,
    currentEnvironment,
    dryRun,
    active: activeSchedules.length,
    skipped: skippedSchedules.length,
  });

  for (const { scheduleId, environments } of skippedSchedules) {
    logger.debug("Skipping schedule (not active in current environment)", {
      scheduleId,
      environments,
      currentEnvironment,
    });
  }

  try {
    const result = await composer.syncSchedules(activeSchedules, { dryRun });

    logger.info("Schedule sync complete", {
      dryRun,
      created: result.created.length,
      updated: result.updated.length,
      deleted: result.deleted.length,
      errors: result.errors.length,
    });

    if (result.errors.length > 0) {
      for (const { scheduleId, error } of result.errors) {
        logger.error("Schedule failed to sync", { scheduleId, error });
      }
      process.exit(1);
    }
  } catch (error) {
    logger.error("Failed to sync Temporal schedules", {
      error: error instanceof Error ? error.message : String(error),
    });
    process.exit(1);
  }
}
