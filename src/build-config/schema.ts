/**
 * Composer Build Configuration Schema
 *
 * Defines the build configuration schema for the @lotiai/composer package.
 * This allows external teams to configure their own steps, workflows,
 * and worker profiles.
 */

import { z } from "zod";

/**
 * Worker profile resource configuration (required version for defaults).
 * This is the single source of truth for worker profile fields.
 */
export const WorkerProfileResourcesSchema = z.strictObject({
  /** CPU units (1024 = 1 vCPU) */
  cpu: z.number().int().positive(),
  /** Memory in MB */
  memory: z.number().int().positive(),
  /** Maximum concurrent activities per worker */
  maxConcurrentActivities: z.number().int().positive(),
  /** Minimum number of worker instances (auto-scaling floor) */
  minCount: z.number().int().nonnegative(),
  /** Maximum concurrent workflow task executions per worker */
  maxConcurrentWorkflows: z.number().int().positive(),
  /** Initial number of worker instances before autoscaling takes over */
  initialCount: z.number().int().nonnegative(),
  /** Maximum number of worker instances for auto-scaling */
  maxCount: z.number().int().positive(),
});

/**
 * Worker profile resource configuration (partial version for config overrides).
 * All fields are optional - only specify what you want to override.
 */
const EnvExclusiveOptInSchema = z.strictObject({
  /**
   * Environments where this profile should be deployed as a dedicated worker.
   * If the current env is not listed, the profile's queue is absorbed by fallbackProfile.
   */
  environments: z.array(z.string().min(1)).min(1),
  /**
   * Profile that should consume this profile's task queue when not deployed.
   */
  fallbackProfile: z.string().min(1),
});

/**
 * Worker profile override configuration.
 * Resource fields are optional and support environment-specific deployment via envExclusiveOptIn.
 */
const WorkerProfileOverridesSchema = z.strictObject({
  ...WorkerProfileResourcesSchema.partial().shape,
  envExclusiveOptIn: EnvExclusiveOptInSchema.optional(),
});

/**
 * Development configuration for the `composer dev` CLI command.
 */
const DevConfigSchema = z.strictObject({
  /**
   * Path to the compiled start-all-workers script to spawn for local dev,
   * relative to cwd (the package directory containing composer.build-config.ts).
   *
   * Can be overridden via the `--worker-script` CLI argument.
   *
   * @example "dist/scripts/start-all-workers.js"
   */
  startAllWorkersJsScript: z.string().min(1).optional(),

  /**
   * Glob patterns for files to watch in dev mode.
   * Paths are relative to the config file location (or absolute).
   *
   * @default ["src\/**\/*.ts"]
   * @example ["src\/**\/*.ts"]
   * @example ["../../packages\/**\/src\/**\/*.ts"] (workspace: watch sibling packages)
   */
  watchPatterns: z.array(z.string().min(1)).min(1).optional(),
});

/**
 * Complete composer build configuration schema.
 */
export const ComposerBuildConfigSchema = z.strictObject({
  /**
   * Optional worker profile overrides.
   * Values are merged with defaults from worker-profiles.ts.
   * Only specify fields you want to override.
   * @example { standard: { cpu: 1024, memory: 4096 } }
   */
  workerProfiles: z.record(z.string(), WorkerProfileOverridesSchema).optional().default({}),

  /**
   * Development configuration for the `composer dev` CLI command.
   */
  dev: DevConfigSchema.optional().default({}),
});

/**
 * Inferred TypeScript type from the Zod schema.
 */
export type ComposerBuildConfig = z.infer<typeof ComposerBuildConfigSchema>;

/**
 * Input type for defineBuildConfig (allows partial output config).
 */
export type ComposerBuildConfigInput = z.input<typeof ComposerBuildConfigSchema>;

/**
 * Development configuration type.
 */
export type DevConfig = z.infer<typeof DevConfigSchema>;

/**
 * Worker profile resources type (all fields required - for defaults).
 */
export type WorkerProfileResources = z.infer<typeof WorkerProfileResourcesSchema>;

/**
 * Worker profile env opt-in type.
 */
export type EnvExclusiveOptIn = z.infer<typeof EnvExclusiveOptInSchema>;

/**
 * Worker profile overrides type (all fields optional - for config).
 */
export type WorkerProfileOverrides = z.infer<typeof WorkerProfileOverridesSchema>;
