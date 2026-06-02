/**
 * Sync Schedules via Lambda
 *
 * Framework helper that reconciles composer schedules by invoking a
 * `schedule-sync` Lambda rather than connecting to the Temporal server
 * directly. The Lambda receives `{ namespace, schedules, dryRun }` as JSON,
 * calls the reconciler (`syncSchedules`), and returns a diff summary.
 *
 * This is the intended deploy-time path: CI assumes an AWS role with
 * `lambda:InvokeFunction` permission and runs this helper from a
 * composer-instance's `sync-schedules` script.
 *
 * Supports two modes:
 * - `"invoke"` (default): invokes the Lambda synchronously, logs tail logs
 *   from the response, and exits `1` on any failure path (FunctionError,
 *   per-schedule errors, or SDK errors).
 * - `"emit"`: prints the JSON payload to stdout and exits `0` without
 *   invoking. Useful for capturing the payload as a CI artifact or for
 *   manually passing it through an approval gate.
 *
 * ## Usage
 *
 * ```typescript
 * import { isScheduleDefinition, syncSchedulesViaLambda } from "@lotiai/composer";
 * import { composer } from "./my-app-composer";
 * import * as schedules from "./schedules";
 *
 * await syncSchedulesViaLambda(composer, {
 *   schedules: Object.values(schedules).filter(isScheduleDefinition),
 *   lambdaFunctionName: "my-schedule-sync-function",
 * });
 * ```
 *
 * @module sync-schedules-via-lambda
 */
import { InvokeCommand, type InvokeCommandOutput, LambdaClient } from "@aws-sdk/client-lambda";
import type { Composer } from "../../context-provider";
import type { ScheduleDefinition } from "../schedule/define-schedule";
import type { SyncSchedulesResult } from "../schedule/sync-schedules";

const DEFAULT_REGION = "us-east-1";

/**
 * Payload shape sent to the schedule-sync Lambda.
 */
interface ScheduleSyncLambdaPayload {
  namespace: string;
  schedules: ScheduleDefinition[];
  dryRun: boolean;
}

/**
 * Options for `syncSchedulesViaLambda`.
 */
export interface SyncSchedulesViaLambdaOptions {
  /**
   * Schedule definitions to reconcile. Typically the values of a
   * composer-instance's `schedules/` barrel filtered by `isScheduleDefinition`.
   */
  schedules: ScheduleDefinition[];

  /**
   * Name of the schedule-sync Lambda to invoke (e.g.
   * `"my-schedule-sync-function"`).
   *
   * Ignored in `"emit"` mode but still required for type-safety.
   */
  lambdaFunctionName: string;

  /**
   * Current environment used to filter schedules by their `environments`
   * allowlist.
   *
   * @default process.env.ENVIRONMENT_NAME
   */
  currentEnvironment?: string | null;

  /**
   * If true, the Lambda runs the reconciliation in dry-run mode (logs what
   * would change, touches nothing on the server).
   *
   * @default process.env.COMPOSER_SCHEDULE_SYNC_DRY_RUN === "true"
   */
  dryRun?: boolean;

  /**
   * `"invoke"` calls the Lambda synchronously; `"emit"` prints the payload
   * to stdout and exits without invoking.
   *
   * @default "invoke"
   */
  mode?: "invoke" | "emit";

  /**
   * AWS region for the Lambda client.
   *
   * @default "us-east-1"
   */
  region?: string;
}

/**
 * Syncs composer schedules by invoking a schedule-sync Lambda (or emitting
 * the payload to stdout).
 *
 * @param composer - Composer instance providing the namespace + logger
 * @param options  - Schedules, Lambda function name, and mode/region/env flags
 */
export async function syncSchedulesViaLambda<TContext>(
  composer: Composer<TContext>,
  options: SyncSchedulesViaLambdaOptions,
): Promise<void> {
  const logger = composer.logger;
  const { namespace } = composer.temporal;

  const currentEnvironment = options.currentEnvironment ?? process.env.ENVIRONMENT_NAME ?? null;
  const dryRun = options.dryRun ?? process.env.COMPOSER_SCHEDULE_SYNC_DRY_RUN === "true";
  const mode = options.mode ?? "invoke";
  const region = options.region ?? DEFAULT_REGION;

  if (currentEnvironment === null && options.schedules.length > 0) {
    logger.error("Cannot sync schedules: no current environment was provided", {
      hint: "Set ENVIRONMENT_NAME or pass options.currentEnvironment to syncSchedulesViaLambda",
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

  const payload: ScheduleSyncLambdaPayload = {
    namespace,
    schedules: activeSchedules,
    dryRun,
  };

  logger.info("Preparing schedule sync payload", {
    namespace,
    currentEnvironment,
    dryRun,
    mode,
    active: activeSchedules.length,
    skipped: skippedSchedules.length,
    ...(mode === "invoke" ? { lambdaFunctionName: options.lambdaFunctionName } : {}),
  });

  for (const { scheduleId, environments } of skippedSchedules) {
    logger.debug("Skipping schedule (not active in current environment)", {
      scheduleId,
      environments,
      currentEnvironment,
    });
  }

  if (mode === "emit") {
    process.stdout.write(`${JSON.stringify(payload, null, 2)}\n`);
    return;
  }

  const client = new LambdaClient({ region });

  let response: InvokeCommandOutput;
  try {
    response = await client.send(
      new InvokeCommand({
        FunctionName: options.lambdaFunctionName,
        Payload: new TextEncoder().encode(JSON.stringify(payload)),
        LogType: "Tail",
      }),
    );
  } catch (error) {
    logger.error("Failed to invoke schedule-sync Lambda", {
      lambdaFunctionName: options.lambdaFunctionName,
      error: error instanceof Error ? error.message : String(error),
    });
    process.exit(1);
  }

  if (response.LogResult) {
    const logs = Buffer.from(response.LogResult, "base64").toString("utf-8");
    logger.info("Schedule-sync Lambda tail logs", { logs });
  }

  const responsePayload: unknown = response.Payload
    ? JSON.parse(new TextDecoder().decode(response.Payload))
    : null;

  if (response.FunctionError) {
    logger.error("Schedule-sync Lambda returned FunctionError", {
      functionError: response.FunctionError,
      payload: responsePayload,
    });
    process.exit(1);
  }

  const result = responsePayload as (SyncSchedulesResult & Record<string, unknown>) | null;

  logger.info("Schedule sync complete", {
    namespace,
    dryRun,
    created: result?.created?.length ?? 0,
    updated: result?.updated?.length ?? 0,
    deleted: result?.deleted?.length ?? 0,
    errors: result?.errors?.length ?? 0,
  });

  if (result?.errors && result.errors.length > 0) {
    for (const { scheduleId, error } of result.errors) {
      logger.error("Schedule failed to sync", { scheduleId, error });
    }
    process.exit(1);
  }
}
