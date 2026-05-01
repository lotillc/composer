/**
 * Dev:Up and Dev:Down Commands
 *
 * Start and stop the Temporal development server using the docker-compose file
 * bundled with the @lotiai/composer package. This allows consumers to run
 * `npx @lotiai/composer dev:up` without needing to manage compose files themselves.
 */

import { execFileSync } from "node:child_process";
import { resolve } from "node:path";
import type { CommandModule } from "yargs";

/**
 * Resolves the path to the docker-compose.temporal.yml file shipped with
 * the @lotiai/composer package. Uses __dirname so the path is correct whether
 * the CLI is run from node_modules or from the source repo.
 */
function getComposeFilePath(): string {
  // __dirname at runtime is dist/cli/commands/; the compose file is at the package root
  return resolve(__dirname, "..", "..", "..", "docker-compose.temporal.yml");
}

/**
 * Runs `docker compose` with the given arguments, forwarding stdio to the terminal.
 */
function dockerCompose(args: string[]): void {
  const composeFile = getComposeFilePath();
  execFileSync("docker", ["compose", "-f", composeFile, ...args], {
    stdio: "inherit",
  });
}

interface DevTemporalOptions {
  detach: boolean;
}

export const devUpCommand: CommandModule<object, DevTemporalOptions> = {
  command: "dev:up",
  describe: "Start the Temporal development server (PostgreSQL, Temporal, Temporal UI)",
  builder: (yargs) =>
    yargs.option("detach", {
      alias: "d",
      type: "boolean",
      default: true,
      description: "Run containers in the background",
    }),
  handler: (argv) => {
    try {
      const args = ["up"];
      if (argv.detach) {
        args.push("-d");
      }
      dockerCompose(args);
    } catch (error) {
      console.error("Failed to start Temporal:", error instanceof Error ? error.message : error);
      process.exit(1);
    }
  },
};

export const devDownCommand: CommandModule<object, object> = {
  command: "dev:down",
  describe: "Stop the Temporal development server",
  builder: (yargs) => yargs,
  handler: () => {
    try {
      dockerCompose(["down"]);
    } catch (error) {
      console.error("Failed to stop Temporal:", error instanceof Error ? error.message : error);
      process.exit(1);
    }
  },
};
