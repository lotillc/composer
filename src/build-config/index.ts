/**
 * Composer Build Configuration Module
 *
 * This module provides build configuration utilities for the @lotiai/composer package.
 * External teams can use this to configure their own composer setup.
 *
 * @example
 * ```typescript
 * // composer.build-config.ts
 * import { defineBuildConfig } from "@lotiai/composer/build-config";
 *
 * export default defineBuildConfig({
 *   dev: {
 *     startAllWorkersJsScript: "dist/scripts/start-all-workers.js",
 *     watchPatterns: ["src/**\/*.ts"],
 *   },
 * });
 * ```
 */

import type { ComposerBuildConfig, ComposerBuildConfigInput } from "./schema.js";
import { ComposerBuildConfigSchema } from "./schema.js";

// Loader exports
export {
  clearBuildConfigCache,
  getBuildConfigFilePath,
  hasBuildConfigFile,
  type LoadBuildConfigOptions,
  type LoadedBuildConfig,
  loadBuildConfig,
} from "./loader.js";
// Schema exports
export {
  type ComposerBuildConfig,
  type ComposerBuildConfigInput,
  ComposerBuildConfigSchema,
  type EnvExclusiveOptIn,
  type WorkerProfileOverrides,
  type WorkerProfileResources,
  WorkerProfileResourcesSchema,
} from "./schema.js";

/**
 * Helper function for creating a type-safe composer build configuration.
 *
 * This function provides TypeScript IntelliSense and validation hints
 * when creating a composer.build-config.ts file.
 *
 * @param config - The composer build configuration
 * @returns The same configuration (identity function for type inference)
 *
 * @example
 * ```typescript
 * // composer.build-config.ts
 * import { defineBuildConfig } from "@lotiai/composer/build-config";
 *
 * export default defineBuildConfig({
 *   workerProfiles: {
 *     standard: {
 *       cpu: 1024,
 *       memory: 4096,
 *       maxConcurrentActivities: 20,
 *     },
 *   },
 *   dev: {
 *     startAllWorkersJsScript: "dist/scripts/start-all-workers.js",
 *   },
 * });
 * ```
 */
export function defineBuildConfig(config: ComposerBuildConfigInput): ComposerBuildConfig {
  return ComposerBuildConfigSchema.parse(config);
}
