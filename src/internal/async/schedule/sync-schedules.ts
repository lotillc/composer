/**
 * Schedule Synchronization
 *
 * Declaratively reconciles Temporal schedules with schedule definitions.
 * The set of definitions passed in is the source of truth -- schedules
 * are created, updated, or deleted to match.
 *
 * Ownership is tracked via a `managedBy: "composer"` memo on each schedule.
 * Schedules without this memo (manually created or from other systems) are
 * never touched.
 *
 * @module sync-schedules
 */

import type {
  ScheduleDescription,
  ScheduleOptions,
  ScheduleSummary,
  ScheduleUpdateOptions,
} from "@temporalio/client";
import { Client, Connection, ScheduleAlreadyRunning } from "@temporalio/client";
import { defaultLogger } from "../../defaults";
import type { ComposerLogger } from "../../types";
import { MANAGED_BY_MEMO_KEY, MANAGED_BY_MEMO_VALUE } from "./constants";
import type { ScheduleDefinition } from "./define-schedule";

/**
 * Configuration for connecting to Temporal for schedule sync.
 */
export interface TemporalScheduleConfig {
  /** Temporal server address (e.g., "localhost:7233") */
  address: string;
  /** Temporal namespace (e.g., "default") */
  namespace: string;
}

/**
 * Configuration for syncing schedules.
 */
export interface SyncSchedulesConfig {
  /** Temporal connection config */
  temporalConfig: TemporalScheduleConfig;

  /** Schedule definitions to sync (source of truth) */
  schedules: ScheduleDefinition[];

  /**
   * If true, log what would happen without making changes.
   * @default false
   */
  dryRun?: boolean;

  /** Logger for sync progress */
  logger?: ComposerLogger;
}

/**
 * Result of a schedule sync operation.
 */
export interface SyncSchedulesResult {
  /** Schedules that were created */
  created: string[];
  /** Schedules that were updated */
  updated: string[];
  /** Schedules that were deleted (no longer in definitions) */
  deleted: string[];
  /** Schedules that failed to sync */
  errors: Array<{ scheduleId: string; error: string }>;
}

/**
 * Checks if a schedule summary was created by composer (has the managed-by memo).
 */
function isComposerManaged(summary: ScheduleSummary): boolean {
  return summary.memo?.[MANAGED_BY_MEMO_KEY] === MANAGED_BY_MEMO_VALUE;
}

/**
 * Builds the Temporal ScheduleOptions from a ScheduleDefinition.
 */
function buildScheduleOptions(definition: ScheduleDefinition): ScheduleOptions {
  const mergedInitialData = {
    ...definition.initialData,
    ...definition.configuredValues,
  };

  return {
    scheduleId: definition.scheduleId,
    spec: definition.spec,
    action: {
      type: "startWorkflow" as const,
      workflowType: definition.workflowName,
      taskQueue: definition.taskQueue,
      args: [{ initialData: mergedInitialData }],
    },
    policies: {
      overlap: definition.overlap,
      ...(definition.catchupWindow != null ? { catchupWindow: definition.catchupWindow } : {}),
    } as ScheduleOptions["policies"],
    state: {
      paused: definition.paused,
      note: definition.note,
    },
    memo: {
      [MANAGED_BY_MEMO_KEY]: MANAGED_BY_MEMO_VALUE,
    },
  };
}

/**
 * Declaratively synchronizes schedule definitions to the Temporal server.
 *
 * This function:
 * 1. Lists all existing schedules with the `managedBy: "composer"` memo
 * 2. Creates schedules that exist in definitions but not on the server
 * 3. Updates schedules that exist in both (applies latest spec, versioned workflow name, policies)
 * 4. Deletes composer-managed schedules that are no longer in the definitions
 *
 * Non-composer schedules are never touched.
 *
 * @returns Summary of what was created, updated, and deleted
 */
export async function syncSchedules(config: SyncSchedulesConfig): Promise<SyncSchedulesResult> {
  const { temporalConfig, schedules, dryRun = false } = config;
  const logger = config.logger ?? defaultLogger;

  const result: SyncSchedulesResult = {
    created: [],
    updated: [],
    deleted: [],
    errors: [],
  };

  const connection = await Connection.connect({ address: temporalConfig.address });
  try {
    const client = new Client({ connection, namespace: temporalConfig.namespace });

    // Collect all composer-managed schedules currently on the server
    const existingManagedIds = new Set<string>();
    for await (const summary of client.schedule.list()) {
      if (isComposerManaged(summary)) {
        existingManagedIds.add(summary.scheduleId);
      }
    }

    // Build lookup of declared schedule IDs
    const declaredIds = new Set(schedules.map((s) => s.scheduleId));

    // Validate no duplicate schedule IDs in definitions
    if (declaredIds.size !== schedules.length) {
      const seen = new Set<string>();
      for (const s of schedules) {
        if (seen.has(s.scheduleId)) {
          throw new Error(`Duplicate scheduleId "${s.scheduleId}" found in schedule definitions`);
        }
        seen.add(s.scheduleId);
      }
    }

    // Create or update each declared schedule
    for (const definition of schedules) {
      const { scheduleId } = definition;
      const exists = existingManagedIds.has(scheduleId);

      try {
        if (exists) {
          if (dryRun) {
            logger.info("[dry-run] Would update schedule", { scheduleId });
          } else {
            await updateSchedule(client, definition, logger);
          }
          result.updated.push(scheduleId);
        } else {
          if (dryRun) {
            logger.info("[dry-run] Would create schedule", { scheduleId });
          } else {
            await createSchedule(client, definition, logger);
          }
          result.created.push(scheduleId);
        }
      } catch (err) {
        const errorMessage = err instanceof Error ? err.message : String(err);
        logger.error("Failed to sync schedule", { scheduleId, error: errorMessage });
        result.errors.push({ scheduleId, error: errorMessage });
      }
    }

    // Delete composer-managed schedules that are no longer declared
    for (const existingId of existingManagedIds) {
      if (!declaredIds.has(existingId)) {
        try {
          if (dryRun) {
            logger.info("[dry-run] Would delete schedule", { scheduleId: existingId });
          } else {
            const handle = client.schedule.getHandle(existingId);
            await handle.delete();
            logger.info("Deleted schedule", { scheduleId: existingId });
          }
          result.deleted.push(existingId);
        } catch (err) {
          const errorMessage = err instanceof Error ? err.message : String(err);
          logger.error("Failed to delete schedule", {
            scheduleId: existingId,
            error: errorMessage,
          });
          result.errors.push({ scheduleId: existingId, error: errorMessage });
        }
      }
    }

    // Summary
    logger.info("Schedule sync complete", {
      dryRun,
      created: result.created.length,
      updated: result.updated.length,
      deleted: result.deleted.length,
      errors: result.errors.length,
    });

    return result;
  } finally {
    await connection.close();
  }
}

/**
 * Creates a new schedule on the Temporal server.
 *
 * If the schedule already exists (race condition), falls back to update.
 */
async function createSchedule(
  client: Client,
  definition: ScheduleDefinition,
  logger: ComposerLogger,
): Promise<void> {
  const options = buildScheduleOptions(definition);

  try {
    await client.schedule.create(options);
    logger.info("Created schedule", {
      scheduleId: definition.scheduleId,
      workflowType: (options.action as { workflowType: string }).workflowType,
    });
  } catch (err) {
    if (err instanceof ScheduleAlreadyRunning) {
      const handle = client.schedule.getHandle(definition.scheduleId);
      const desc = await handle.describe();
      if (desc.memo?.[MANAGED_BY_MEMO_KEY] !== MANAGED_BY_MEMO_VALUE) {
        throw new Error(
          `Schedule "${definition.scheduleId}" already exists but is not managed by composer. ` +
            `Refusing to overwrite. Delete or rename the existing schedule to resolve this conflict.`,
        );
      }
      logger.info("Schedule already exists (race condition), updating instead", {
        scheduleId: definition.scheduleId,
      });
      await updateSchedule(client, definition, logger);
    } else {
      throw err;
    }
  }
}

/**
 * Updates an existing schedule on the Temporal server.
 */
async function updateSchedule(
  client: Client,
  definition: ScheduleDefinition,
  logger: ComposerLogger,
): Promise<void> {
  const handle = client.schedule.getHandle(definition.scheduleId);
  const mergedInitialData = {
    ...definition.initialData,
    ...definition.configuredValues,
  };

  await handle.update(
    (_previous: ScheduleDescription): ScheduleUpdateOptions => ({
      spec: definition.spec,
      action: {
        type: "startWorkflow" as const,
        workflowType: definition.workflowName,
        taskQueue: definition.taskQueue,
        args: [{ initialData: mergedInitialData }],
      },
      policies: {
        overlap: definition.overlap,
        ...(definition.catchupWindow != null ? { catchupWindow: definition.catchupWindow } : {}),
      } as ScheduleUpdateOptions["policies"],
      state: {
        paused: definition.paused,
        note: definition.note,
      },
    }),
  );

  logger.info("Updated schedule", {
    scheduleId: definition.scheduleId,
    workflowType: definition.workflowName,
  });
}
