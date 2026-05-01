/**
 * Profiles Command
 *
 * Resolves effective worker profile deployment topology for an environment,
 * including dedicated profiles and fallback queue routing.
 */

import type { CommandModule } from "yargs";
import { loadBuildConfig } from "../../build-config/index";
import {
  resolveProfilesForEnvironment,
  WORKER_PROFILES,
  type WorkerProfile,
} from "../../internal/async/config/worker-profiles";

interface ProfilesOptions {
  config?: string;
  env: string;
  json: boolean;
}

export const profilesCommand: CommandModule<object, ProfilesOptions> = {
  command: "profiles",
  describe: "Resolve worker profile deployment and fallback queues for an environment",
  builder: (yargs) =>
    yargs
      .option("config", {
        alias: "c",
        type: "string",
        description: "Path to composer.build-config.ts (auto-detected if not specified)",
      })
      .option("env", {
        type: "string",
        demandOption: true,
        description: "Environment to resolve (for example: local, preview, prod)",
      })
      .option("json", {
        type: "boolean",
        default: false,
        description: "Output machine-readable JSON",
      }),
  handler: async (argv) => {
    const { config: configPath, env, json } = argv;

    try {
      const { config } = await loadBuildConfig(configPath ? { configPath } : undefined);
      const resolved = resolveProfilesForEnvironment(env, config);

      if (json) {
        console.log(JSON.stringify(resolved, null, 2));
        return;
      }

      console.log(`Environment: ${env}`);
      console.log("\nDedicated profiles:");

      for (const profile of Object.keys(WORKER_PROFILES) as WorkerProfile[]) {
        const dedicated = resolved.dedicatedProfiles[profile];
        if (!dedicated) {
          continue;
        }

        console.log(
          `  - ${profile} (${dedicated.taskQueue}) cpu=${dedicated.resources.cpu} memory=${dedicated.resources.memory} maxConcurrentActivities=${dedicated.resources.maxConcurrentActivities} initial=${dedicated.resources.initialCount} max=${dedicated.resources.maxCount}`,
        );
      }

      console.log("\nFallback queues:");
      let hasFallbacks = false;
      for (const profile of Object.keys(WORKER_PROFILES) as WorkerProfile[]) {
        const queues = resolved.fallbackQueues[profile];
        if (!queues || queues.length === 0) {
          continue;
        }
        hasFallbacks = true;
        console.log(`  - ${profile}: ${queues.join(", ")}`);
      }

      if (!hasFallbacks) {
        console.log("  - none");
      }
    } catch (error) {
      console.error("Failed to resolve profiles:", error instanceof Error ? error.message : error);
      process.exit(1);
    }
  },
};
