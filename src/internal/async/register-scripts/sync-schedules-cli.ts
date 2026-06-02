/**
 * Run Sync Schedules CLI
 *
 * Framework helper that implements the complete command-line entrypoint for
 * a composer-instance's `sync-schedules` script. Each composer instance
 * needs exactly one thin wrapper that:
 *
 * ```typescript
 * import { isScheduleDefinition, runSyncSchedulesCli } from "@lotiai/composer";
 * import { getComposer } from "../app-composer";
 * import * as scheduleExports from "../schedules";
 *
 * void runSyncSchedulesCli(getComposer(), {
 *   schedules: Object.values(scheduleExports).filter(isScheduleDefinition),
 * });
 * ```
 *
 * Everything else -- argv parsing, current-environment resolution, default
 * Lambda function name, direct-vs-Lambda dispatch, and process exit on
 * errors -- is owned here so flag names and behavior stay consistent across
 * composer instances.
 *
 * ## Flags
 *
 * - `--lambda <name>`   Override the schedule-sync Lambda function name.
 *                       Defaults to `loti-ic-<ENVIRONMENT_NAME>-temporalschedulesync`.
 * - `--mode invoke|emit` How to reconcile via the Lambda path. `invoke`
 *                        (default) calls the Lambda; `emit` prints the JSON
 *                        payload to stdout and exits 0.
 * - `--dry-run`          Reconcile in dry-run mode on the server
 *                        (no changes). Defaults to
 *                        `COMPOSER_SCHEDULE_SYNC_DRY_RUN === "true"`.
 * - `--direct`           Connect to Temporal directly from this process
 *                        using the composer's configured `serverAddress`
 *                        (local dev / escape hatch). Incompatible with
 *                        `--mode`.
 * - `--region <region>`  AWS region for the Lambda client (default
 *                        `us-east-1`).
 *
 * @module sync-schedules-cli
 */
import yargs from "yargs";
import { hideBin } from "yargs/helpers";
import type { Composer } from "../../context-provider";
import type { ScheduleDefinition } from "../schedule/define-schedule";
import { runScheduleSync } from "./sync-schedules";
import { syncSchedulesViaLambda } from "./sync-schedules-via-lambda";

/**
 * Builds the default schedule-sync Lambda function name for the current
 * environment: an infra resource prefix + environment + resource slug with
 * hyphens stripped. Must match the name the schedule-sync Lambda is deployed
 * under; override with `--lambda` if your deployment uses a different name.
 */
function defaultLambdaFunctionName(environment: string): string {
  return `loti-ic-${environment}-temporalschedulesync`;
}

/**
 * Options for `runSyncSchedulesCli`.
 */
export interface RunSyncSchedulesCliOptions {
  /**
   * Schedule definitions to reconcile. Typically the values of a
   * composer-instance's `schedules/` barrel filtered by
   * `isScheduleDefinition`.
   */
  schedules: ScheduleDefinition[];

  /**
   * argv to parse. Defaults to `process.argv`; primarily useful for
   * testing.
   */
  argv?: string[];
}

/**
 * Parses CLI flags and dispatches to either `syncSchedulesViaLambda`
 * (default) or `runScheduleSync` (with `--direct`). Intended to be the
 * entire body of a composer-instance's `sync-schedules` script.
 *
 * @param composer - Composer instance providing the Temporal connection + logger
 * @param options - Schedules to reconcile (and optional argv override)
 */
export async function runSyncSchedulesCli<TContext>(
  composer: Composer<TContext>,
  options: RunSyncSchedulesCliOptions,
): Promise<void> {
  const argv = await yargs(options.argv ?? hideBin(process.argv))
    .scriptName("sync-schedules")
    .usage("$0 [--mode invoke|emit] [--dry-run] [--direct]")
    .option("lambda", {
      type: "string",
      describe:
        "Name of the schedule-sync Lambda function (defaults to loti-ic-<env>-temporalschedulesync).",
    })
    .option("mode", {
      type: "string",
      choices: ["invoke", "emit"] as const,
      default: "invoke" as const,
      describe:
        'How to reconcile: "invoke" calls the Lambda, "emit" prints the JSON payload to stdout and exits.',
    })
    .option("dry-run", {
      type: "boolean",
      describe:
        "Reconcile in dry-run mode (log the diff, change nothing). Defaults to the COMPOSER_SCHEDULE_SYNC_DRY_RUN env var.",
    })
    .option("direct", {
      type: "boolean",
      default: false,
      describe:
        "Connect to Temporal directly from this process instead of invoking the Lambda (local dev).",
    })
    .option("region", {
      type: "string",
      describe: "AWS region for the Lambda client (defaults to us-east-1).",
    })
    .strict()
    .help()
    .parseAsync();

  const currentEnvironment = process.env.ENVIRONMENT_NAME ?? null;

  if (argv.direct) {
    if (argv.mode !== "invoke") {
      throw new Error("--mode is only valid when invoking the Lambda; remove --direct or --mode");
    }
    await runScheduleSync(composer, {
      schedules: options.schedules,
      currentEnvironment,
      dryRun: argv.dryRun,
    });
    return;
  }

  if (currentEnvironment === null) {
    throw new Error(
      "ENVIRONMENT_NAME must be set to derive the default Lambda function name (or pass --lambda explicitly).",
    );
  }

  const lambdaFunctionName = argv.lambda ?? defaultLambdaFunctionName(currentEnvironment);

  await syncSchedulesViaLambda(composer, {
    schedules: options.schedules,
    lambdaFunctionName,
    currentEnvironment,
    dryRun: argv.dryRun,
    mode: argv.mode,
    region: argv.region,
  });
}
