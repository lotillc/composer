/**
 * Composer CLI
 *
 * Command-line interface for the Composer framework.
 * Reads configuration from composer.build-config.ts and provides commands for
 * managing development workflows.
 *
 * Schedule synchronization is handled by the schedule-sync Lambda at deploy
 * time and by the syncSchedulesViaLambda helper in @lotiai/composer, not via a
 * CLI command. See packages/composer/src/internal/async/register-scripts/
 * sync-schedules-via-lambda.ts and infrastructure/temporal/src/
 * schedule-sync-handler.ts.
 *
 * Usage:
 *   npx @lotiai/composer dev           # Start development watch mode
 *   npx @lotiai/composer dev:up        # Start Temporal dev server
 *   npx @lotiai/composer dev:down      # Stop Temporal dev server
 *   npx @lotiai/composer profiles      # Resolve worker profile deployment topology
 */

import yargs from "yargs";
import { hideBin } from "yargs/helpers";
import { devCommand } from "./commands/dev";
import { devDownCommand, devUpCommand } from "./commands/dev-temporal";
import { profilesCommand } from "./commands/profiles";

void yargs(hideBin(process.argv))
  .scriptName("composer")
  .usage("$0 <command> [options]")
  .command(devCommand)
  .command(devDownCommand)
  .command(devUpCommand)
  .command(profilesCommand)
  .demandCommand(1, "You must specify a command")
  .strict()
  .help()
  .alias("h", "help")
  .version()
  .alias("v", "version")
  .parse();
