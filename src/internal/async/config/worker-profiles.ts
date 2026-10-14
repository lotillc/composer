/**
 * Worker Profile Definitions for Temporal Activity Workers
 *
 * This module defines worker profiles that map step performance characteristics
 * to appropriate infrastructure resources.
 *
 * ## Profiles
 *
 * - **standard**: General business logic (API calls, DB queries, content acquisition).
 *   0.5 vCPU / 2GB per container, 15 concurrent activities, scales 2-10.
 * - **heavy**: CPU-intensive or long-running work (zone ingestion, large data processing).
 *   2 vCPU / 4GB per container, 5 concurrent activities, starts at 3 containers and scales 1-3 by default.
 *
 * Each profile maps to a dedicated Temporal task queue and ECS service.
 *
 * ## Consumer-defined profiles
 *
 * A consumer can declare further profiles, e.g. to run one step on a worker
 * with its own IAM role. Register the name with the type system, then declare
 * it in `composer.build-config.ts`:
 * ```typescript
 * declare module "@lotiai/composer" {
 *   interface WorkerProfileRegistry {
 *     "liveness-erase": true;
 *   }
 * }
 *
 * // composer.build-config.ts
 * workerProfiles: {
 *   "liveness-erase": { minCount: 1, initialCount: 1, maxCount: 1 }
 * }
 * ```
 * A declared profile routes to the task queue `<name>-tasks` and starts from
 * the `standard` resources; its entry overrides only what it sets.
 *
 * ## Configuration
 *
 * Defaults are defined here. Users can override via `composer.build-config.ts`:
 * ```typescript
 * workerProfiles: {
 *   standard: { cpu: 1024, memory: 4096 }  // Override just what you need
 * }
 * ```
 */

import type {
  ComposerBuildConfig,
  WorkerProfileOverrides,
  WorkerProfileResources,
} from "../../../build-config/index.js";
import { loadBuildConfig } from "../../../build-config/loader.js";
// Declared in the package entry point: module augmentation only merges into
// the module that declares an interface, not one that re-exports it.
import type { WorkerProfileRegistry } from "../../../index.js";
import { isWorkerProfileName } from "./worker-profile-name.js";

export {
  isWorkerProfileName,
  MAX_WORKER_PROFILE_NAME_LENGTH,
  WORKER_PROFILE_NAME_PATTERN,
} from "./worker-profile-name.js";

/**
 * The profiles Composer ships with.
 */
export type BuiltInWorkerProfile = "standard" | "heavy";

/**
 * Worker profile type: the built-in profiles plus any registered through
 * `WorkerProfileRegistry`.
 *
 * Steps declare their profile via the `workerProfile` field. Steps without an
 * explicit profile default to "standard".
 */
export type WorkerProfile = Extract<keyof WorkerProfileRegistry, string>;

/**
 * Configuration for a worker profile, defining its resource characteristics
 * and operational parameters.
 */
export interface WorkerProfileConfig {
  /** Human-readable name */
  name: string;

  /** Temporal task queue name for this profile */
  taskQueue: string;

  /** Description of when to use this profile */
  description: string;

  /** Resource configuration (CPU, memory, scaling) */
  resources: WorkerProfileResources;
}

/**
 * Default worker profile configuration defining resource characteristics and task queues.
 *
 * These defaults are referenced by:
 * - Step definitions (via `workerProfile` field - currently all use "standard")
 * - Activity workers (to register on the "standard-tasks" queue)
 * - Infrastructure as Code (to create appropriately sized ECS tasks)
 *
 * Users can override these defaults via `composer.build-config.ts`.
 */
export const WORKER_PROFILES: Record<BuiltInWorkerProfile, WorkerProfileConfig> = {
  standard: {
    name: "Standard",
    taskQueue: "standard-tasks",
    description:
      "General business logic: API calls, database queries, content acquisition, moderate computation",
    resources: {
      cpu: 512, // 0.5 vCPU (1024 = 1 vCPU)
      memory: 2048, // 2 GB in MB
      maxConcurrentActivities: 15,
      minCount: 2,
      maxConcurrentWorkflows: 100,
      initialCount: 2,
      maxCount: 10,
    },
  },
  heavy: {
    name: "Heavy",
    taskQueue: "heavy-tasks",
    description:
      "CPU-intensive or long-running work: zone file ingestion (gunzip + metaphone), large data processing",
    resources: {
      cpu: 2048, // 2 vCPU
      memory: 4096, // 4 GB
      maxConcurrentActivities: 5,
      minCount: 1,
      maxConcurrentWorkflows: 100,
      initialCount: 3,
      maxCount: 3,
    },
  },
};

/**
 * Default worker profile for steps that don't explicitly declare one.
 *
 * Using "standard" as the default provides reasonable performance for
 * typical business logic without over-provisioning resources.
 */
export const DEFAULT_WORKER_PROFILE: WorkerProfile = "standard";

function isBuiltInWorkerProfile(profile: string): profile is BuiltInWorkerProfile {
  return Object.hasOwn(WORKER_PROFILES, profile);
}

/**
 * Gets the task queue name for a given worker profile: the built-in profile's
 * queue, or `<name>-tasks` for a consumer-defined one.
 *
 * The queue is derived from the name alone, so a workflow worker can route a
 * step without loading the build config.
 *
 * @param profile - The worker profile
 * @returns Temporal task queue name
 */
export function getTaskQueueForProfile(profile: WorkerProfile): string {
  if (isBuiltInWorkerProfile(profile)) {
    return WORKER_PROFILES[profile].taskQueue;
  }
  if (!isWorkerProfileName(profile)) {
    throw new Error(`Invalid worker profile name: "${profile}".`);
  }
  return `${profile}-tasks`;
}

/**
 * Lists every profile available under a build config: the built-ins, then
 * each consumer-defined profile declared in `workerProfiles`, in declaration
 * order.
 */
export function getWorkerProfiles(config?: ComposerBuildConfig): WorkerProfile[] {
  const profiles: string[] = Object.keys(WORKER_PROFILES);
  for (const name of Object.keys(config?.workerProfiles ?? {})) {
    if (!profiles.includes(name)) {
      profiles.push(name);
    }
  }
  return profiles as WorkerProfile[];
}

/**
 * Validates that a profile string is a valid WorkerProfile.
 *
 * Without a config only the built-in profiles are valid. With one, profiles
 * declared in its `workerProfiles` are valid too.
 *
 * @param profile - The profile string to validate
 * @param config - Optional build config declaring consumer-defined profiles
 * @returns true if valid, false otherwise
 */
export function isValidWorkerProfile(
  profile: string,
  config?: ComposerBuildConfig,
): profile is WorkerProfile {
  return (getWorkerProfiles(config) as string[]).includes(profile);
}

/**
 * Gets all task queue names (useful for workers that listen to multiple queues).
 *
 * @param config - Optional build config; its consumer-defined profiles' queues
 *   are included
 * @returns Array of all task queue names
 */
export function getAllTaskQueues(config?: ComposerBuildConfig): string[] {
  return getWorkerProfiles(config).map(getTaskQueueForProfile);
}

/**
 * A step pinned to a worker profile its build config does not declare.
 */
export interface UndeclaredWorkerProfileStep {
  workflow: string;
  step: string;
  workerProfile: string;
}

/**
 * Finds steps pinned to a profile the build config does not declare.
 *
 * Routing derives a step's task queue from its profile name, so such a step
 * would wait on a queue no worker polls. Typecheck catches names missing from
 * `WorkerProfileRegistry`; call this (e.g. in a test) to catch a registered
 * name missing from `composer.build-config.ts`.
 *
 * Fan-out child workflows are checked too.
 *
 * @returns The offending steps; empty when every profile is declared
 */
export function findUndeclaredWorkerProfiles(
  workflows: ReadonlyArray<{ name: string; steps: ReadonlyArray<unknown> }>,
  config?: ComposerBuildConfig,
): UndeclaredWorkerProfileStep[] {
  const undeclared: UndeclaredWorkerProfileStep[] = [];
  const seen = new Set<unknown>();
  const visit = (workflow: { name: string; steps: ReadonlyArray<unknown> }) => {
    if (seen.has(workflow)) {
      return;
    }
    seen.add(workflow);
    for (const rawStep of workflow.steps) {
      const step = rawStep as {
        name?: string;
        workerProfile?: string;
        __fanOut?: { childWorkflow?: { name: string; steps: ReadonlyArray<unknown> } };
      };
      if (step.workerProfile !== undefined && !isValidWorkerProfile(step.workerProfile, config)) {
        undeclared.push({
          workflow: workflow.name,
          step: String(step.name),
          workerProfile: step.workerProfile,
        });
      }
      if (step.__fanOut?.childWorkflow) {
        visit(step.__fanOut.childWorkflow);
      }
    }
  };
  workflows.forEach(visit);
  return undeclared;
}

/**
 * Gets the effective resources for a worker profile, merging config overrides with defaults.
 *
 * @param profile - The worker profile name
 * @param configOverrides - Optional overrides from composer.build-config.ts
 * @returns The merged resource configuration
 *
 * @example
 * ```typescript
 * // With no overrides, returns defaults
 * getEffectiveResources("standard");
 * // { cpu: 512, memory: 2048, maxConcurrentActivities: 15, minCount: 2, initialCount: 2, maxCount: 10 }
 *
 * // With overrides, merges them
 * getEffectiveResources("standard", { cpu: 1024 });
 * // { cpu: 1024, memory: 2048, maxConcurrentActivities: 15, minCount: 2, initialCount: 2, maxCount: 10 }
 * ```
 */
export function getEffectiveResources(
  profile: WorkerProfile,
  configOverrides?: WorkerProfileOverrides,
): WorkerProfileResources {
  // A consumer-defined profile starts from the standard resources.
  const baseProfile = isBuiltInWorkerProfile(profile) ? profile : "standard";
  const defaults = WORKER_PROFILES[baseProfile].resources;
  if (!configOverrides) {
    return defaults;
  }

  // Only merge resource fields; configOverrides may include non-resource keys
  // such as envExclusiveOptIn used for deployment topology decisions.
  return {
    cpu: configOverrides.cpu ?? defaults.cpu,
    memory: configOverrides.memory ?? defaults.memory,
    maxConcurrentActivities:
      configOverrides.maxConcurrentActivities ?? defaults.maxConcurrentActivities,
    minCount: configOverrides.minCount ?? defaults.minCount,
    maxConcurrentWorkflows:
      configOverrides.maxConcurrentWorkflows ?? defaults.maxConcurrentWorkflows,
    initialCount: configOverrides.initialCount ?? defaults.initialCount,
    maxCount: configOverrides.maxCount ?? defaults.maxCount,
  };
}

/**
 * Gets the effective worker profile config, merging config overrides with defaults.
 *
 * @param profile - The worker profile name
 * @param config - Optional composer config containing workerProfiles overrides;
 *   required for a consumer-defined profile, which must be declared in it
 * @returns The full profile config with merged resources
 */
export function getEffectiveProfileConfig(
  profile: WorkerProfile,
  config?: ComposerBuildConfig,
): WorkerProfileConfig {
  if (!isValidWorkerProfile(profile, config)) {
    throw new Error(
      `Worker profile "${profile}" is not declared in the build config's workerProfiles.`,
    );
  }
  const baseConfig: Omit<WorkerProfileConfig, "resources"> = isBuiltInWorkerProfile(profile)
    ? WORKER_PROFILES[profile]
    : {
        name: profile,
        taskQueue: getTaskQueueForProfile(profile),
        description: "Consumer-defined worker profile",
      };
  const overrides = config?.workerProfiles?.[profile];
  return {
    ...baseConfig,
    resources: getEffectiveResources(profile, overrides),
  };
}

export interface ResolvedProfilesForEnvironment {
  dedicatedProfiles: Partial<Record<WorkerProfile, WorkerProfileConfig>>;
  fallbackQueues: Partial<Record<WorkerProfile, string[]>>;
}

/**
 * Resolves which worker profiles are deployed for an environment and which task
 * queues should be absorbed by fallback profiles.
 *
 * Profiles with no envExclusiveOptIn are always dedicated.
 * Profiles with envExclusiveOptIn deploy only when environment is listed.
 */
export function resolveProfilesForEnvironment(
  environment: string,
  config?: ComposerBuildConfig,
): ResolvedProfilesForEnvironment {
  const dedicatedProfiles: Partial<Record<WorkerProfile, WorkerProfileConfig>> = {};
  const fallbackQueues: Partial<Record<WorkerProfile, string[]>> = {};
  const fallbackSources: Partial<Record<WorkerProfile, WorkerProfile[]>> = {};

  for (const profile of getWorkerProfiles(config)) {
    const overrides = config?.workerProfiles?.[profile];
    const envExclusiveOptIn = overrides?.envExclusiveOptIn;
    const isDedicated = !envExclusiveOptIn || envExclusiveOptIn.environments.includes(environment);

    if (isDedicated) {
      dedicatedProfiles[profile] = getEffectiveProfileConfig(profile, config);
      continue;
    }

    const fallbackProfile = envExclusiveOptIn.fallbackProfile;
    if (!isValidWorkerProfile(fallbackProfile, config)) {
      throw new Error(
        `Invalid envExclusiveOptIn.fallbackProfile "${fallbackProfile}" for profile "${profile}".`,
      );
    }

    const taskQueue = getTaskQueueForProfile(profile);
    const existingQueues = fallbackQueues[fallbackProfile] ?? [];
    if (!existingQueues.includes(taskQueue)) {
      existingQueues.push(taskQueue);
    }
    fallbackQueues[fallbackProfile] = existingQueues;

    const sources = fallbackSources[fallbackProfile] ?? [];
    sources.push(profile);
    fallbackSources[fallbackProfile] = sources;
  }

  for (const fallbackProfile of Object.keys(fallbackQueues) as WorkerProfile[]) {
    if (!dedicatedProfiles[fallbackProfile]) {
      const sourceProfiles = fallbackSources[fallbackProfile]?.join(", ") ?? "unknown";
      throw new Error(
        `Profiles [${sourceProfiles}] fall back to "${fallbackProfile}" in environment "${environment}", but "${fallbackProfile}" is not deployed as a dedicated profile.`,
      );
    }
  }

  return {
    dedicatedProfiles,
    fallbackQueues,
  };
}

export interface ResolveActivityWorkerRuntimeConfigOptions {
  environment: string;
  workerProfile: string;
  config: ComposerBuildConfig;
}

export interface ActivityWorkerRuntimeConfig {
  workerProfile: WorkerProfile;
  taskQueues: string[];
  maxConcurrentActivityTaskExecutions: number;
}

/**
 * Resolves runtime configuration for a single activity worker process.
 *
 * Validates the given profile, resolves the environment topology, and returns
 * the task queues this worker should listen on plus the profile-derived
 * max concurrent activities.
 */
export function resolveActivityWorkerRuntimeConfig(
  options: ResolveActivityWorkerRuntimeConfigOptions,
): ActivityWorkerRuntimeConfig {
  const { environment, workerProfile: rawProfile, config } = options;

  if (!isValidWorkerProfile(rawProfile, config)) {
    throw new Error(`Invalid worker profile: "${rawProfile}".`);
  }
  const workerProfile: WorkerProfile = rawProfile;

  // Dedicated profiles are profiles that are deployed in the environment. They run activities
  // from their own task queues. Fallback profiles are profiles that are not deployed in the environment.
  // Activities that would normally go to that profile (i.e. heavy) are run on the fallback queue.
  // i.e. In non-prod environments, heavy activities are run on the standard resource activity worker.
  const { dedicatedProfiles, fallbackQueues } = resolveProfilesForEnvironment(environment, config);
  const profileConfig = dedicatedProfiles[workerProfile];
  if (!profileConfig) {
    throw new Error(
      `Worker profile "${workerProfile}" is not configured as dedicated in environment "${environment}".`,
    );
  }

  const primaryQueue = getTaskQueueForProfile(workerProfile);
  const absorbedQueues = fallbackQueues[workerProfile] ?? [];
  const taskQueues = [primaryQueue, ...absorbedQueues];
  const maxConcurrentActivityTaskExecutions = profileConfig.resources.maxConcurrentActivities;

  return {
    workerProfile,
    taskQueues,
    maxConcurrentActivityTaskExecutions,
  };
}

export interface LoadAndResolveActivityWorkerRuntimeConfigOptions
  extends Omit<ResolveActivityWorkerRuntimeConfigOptions, "config"> {
  /** Directory containing the composer.build-config.ts file. Defaults to process.cwd(). */
  buildConfigDir?: string;
}

/**
 * Convenience wrapper that loads build config from disk and then resolves
 * activity worker runtime configuration.
 */
export async function loadAndResolveActivityWorkerRuntimeConfig(
  options: LoadAndResolveActivityWorkerRuntimeConfigOptions,
): Promise<ActivityWorkerRuntimeConfig> {
  const { environment, workerProfile, buildConfigDir } = options;
  const { config } = await loadBuildConfig(
    buildConfigDir ? { searchDir: buildConfigDir } : undefined,
  );
  return resolveActivityWorkerRuntimeConfig({
    environment,
    workerProfile,
    config,
  });
}

export interface CombinedWorkerConfigOptions {
  environment: string;
  /** Directory containing the composer.build-config.ts file. Defaults to process.cwd(). */
  buildConfigDir?: string;
}

export interface CombinedWorkerConfig {
  taskQueues: string[];
  maxConcurrentActivityTaskExecutions: number;
  maxConcurrentWorkflowTaskExecutions: number;
}

/**
 * Loads build config and resolves combined worker configuration
 * across all dedicated profiles for the given environment.
 *
 * Designed for local development where a single process handles all
 * task queues. Collects every dedicated profile's primary queue
 * plus any absorbed fallback queues, sums maxConcurrentActivities,
 * and takes the max of maxConcurrentWorkflows across profiles
 * (since there is only one workflow worker regardless of activity profiles).
 */
export async function loadAndResolveCombinedWorkerConfigForLocalDev(
  options: CombinedWorkerConfigOptions,
): Promise<CombinedWorkerConfig> {
  const { environment, buildConfigDir } = options;
  const { config } = await loadBuildConfig(
    buildConfigDir ? { searchDir: buildConfigDir } : undefined,
  );
  const { dedicatedProfiles, fallbackQueues } = resolveProfilesForEnvironment(environment, config);

  const taskQueues: string[] = [];
  let maxConcurrentActivityTaskExecutions = 0;
  let maxConcurrentWorkflowTaskExecutions = 0;

  for (const profile of Object.keys(dedicatedProfiles) as WorkerProfile[]) {
    const profileConfig = dedicatedProfiles[profile]!;
    taskQueues.push(getTaskQueueForProfile(profile), ...(fallbackQueues[profile] ?? []));
    maxConcurrentActivityTaskExecutions += profileConfig.resources.maxConcurrentActivities;
    maxConcurrentWorkflowTaskExecutions = Math.max(
      maxConcurrentWorkflowTaskExecutions,
      profileConfig.resources.maxConcurrentWorkflows,
    );
  }

  return { taskQueues, maxConcurrentActivityTaskExecutions, maxConcurrentWorkflowTaskExecutions };
}
