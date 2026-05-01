/**
 * Composer Build Configuration Loader
 *
 * Loads and validates composer build configuration from a config file.
 * Supports TypeScript (.ts), JavaScript (.js), and JSON (.json) config files.
 *
 * TypeScript files are loaded using jiti, which provides transparent
 * TypeScript support without requiring pre-compilation.
 */

import * as fs from "node:fs";
import * as path from "node:path";
import { createJiti } from "jiti";
import {
  type ComposerBuildConfig,
  type ComposerBuildConfigInput,
  ComposerBuildConfigSchema,
} from "./schema";

/**
 * Default config file names to search for, in order of preference.
 */
const BUILD_CONFIG_FILE_NAMES = [
  "composer.build-config.ts",
  "composer.build-config.js",
  "composer.build-config.json",
  // Compiled fallback: src/build-config.ts -> dist/build-config.js
  // In Docker images, root-level config files are pruned but dist/ survives.
  "dist/build-config.js",
];

/**
 * Result of loading a build config file.
 */
export interface LoadedBuildConfig {
  /** The validated and resolved configuration */
  config: ComposerBuildConfig;
  /** Absolute path to the config file */
  configPath: string;
  /** Directory containing the config file (used for resolving relative paths) */
  configDir: string;
}

/**
 * Options for loading build configuration.
 */
export interface LoadBuildConfigOptions {
  /**
   * Explicit path to config file.
   * If not provided, searches for config files in searchDir.
   */
  configPath?: string;

  /**
   * Directory to search for config file.
   * @default process.cwd()
   */
  searchDir?: string;
}

/**
 * Finds a build config file by searching for known config file names.
 *
 * @param searchDir - Directory to search in
 * @returns Absolute path to config file, or undefined if not found
 */
function findBuildConfigFile(searchDir: string): string | undefined {
  for (const fileName of BUILD_CONFIG_FILE_NAMES) {
    const filePath = path.join(searchDir, fileName);
    if (fs.existsSync(filePath)) {
      return filePath;
    }
  }
  return undefined;
}

/**
 * Extracts the config object from a loaded module, supporting default export,
 * named 'config' export, and direct module.exports.
 *
 * Handles jiti's double-wrapping of CJS default exports where
 * `module.default` may itself contain a nested `default` key.
 */
function extractConfigExport(configPath: string, module: unknown): ComposerBuildConfigInput {
  let config =
    (module as { default?: ComposerBuildConfigInput }).default ??
    (module as { config?: ComposerBuildConfigInput }).config ??
    module;

  // jiti may double-wrap CJS default exports: { default: { default: actualConfig } }
  if (config && typeof config === "object" && "default" in config) {
    config = (config as { default: ComposerBuildConfigInput }).default;
  }

  if (!config || typeof config !== "object") {
    throw new Error(
      `Config file ${configPath} must export a config object (default export, named 'config', or module.exports)`,
    );
  }
  return config as ComposerBuildConfigInput;
}

/**
 * Loads a build config file and returns its contents.
 * Uses jiti for TypeScript/JavaScript files to support .ts configs without compilation.
 *
 * @param configPath - Absolute path to the config file
 * @returns The config object (unvalidated)
 */
async function loadBuildConfigFile(configPath: string): Promise<ComposerBuildConfigInput> {
  const ext = path.extname(configPath).toLowerCase();

  if (ext === ".json") {
    const content = fs.readFileSync(configPath, "utf-8");
    return JSON.parse(content) as ComposerBuildConfigInput;
  }

  if (ext === ".ts" || ext === ".js") {
    // Use jiti to load TypeScript/JavaScript config files
    // jiti provides transparent TypeScript support without pre-compilation
    const jiti = createJiti(__filename, {
      fsCache: false,
      moduleCache: false,
    });

    const module = await jiti.import(configPath);
    return extractConfigExport(configPath, module);
  }

  throw new Error(`Unsupported config file extension: ${ext}`);
}

/**
 * Resolves relative paths in the config to absolute paths.
 *
 * @param config - The parsed config
 * @param configDir - Directory containing the config file
 * @returns Config with resolved paths
 */
function resolveBuildConfigPaths(
  config: ComposerBuildConfig,
  configDir: string,
): ComposerBuildConfig {
  const watchPatterns = (config.dev.watchPatterns ?? ["src/**/*.ts"]).map((p) =>
    path.isAbsolute(p) ? p : path.join(configDir, p),
  );

  return {
    ...config,
    dev: {
      ...config.dev,
      watchPatterns,
    },
  };
}

/**
 * Cached config result. The config is loaded once per process and reused.
 * Only a single config is supported per process; passing different options
 * to subsequent calls is treated as a programming error and will throw.
 */
let cachedResult: LoadedBuildConfig | undefined;

/**
 * Clears the cached build config, forcing the next `loadBuildConfig()` call to reload from disk.
 * Primarily useful for testing.
 */
export function clearBuildConfigCache(): void {
  cachedResult = undefined;
}

/**
 * Loads, validates, and resolves a composer build configuration.
 *
 * Results are cached per-process: the first call reads from disk and validates,
 * subsequent calls return the cached result immediately. This is safe because
 * config files do not change during a single CLI or build invocation.
 *
 * @param options - Options for loading the config
 * @returns The loaded and validated configuration
 * @throws Error if config file is not found or validation fails
 *
 * @example
 * ```typescript
 * // Load from current directory
 * const { config, configDir } = await loadBuildConfig();
 *
 * // Load from specific path
 * const { config } = await loadBuildConfig({ configPath: "./my-config.ts" });
 *
 * // Load from specific directory
 * const { config } = await loadBuildConfig({ searchDir: "/path/to/project" });
 * ```
 */
export async function loadBuildConfig(
  options: LoadBuildConfigOptions = {},
): Promise<LoadedBuildConfig> {
  if (cachedResult) {
    return cachedResult;
  }

  const searchDir = options.searchDir ?? process.cwd();

  // Find or use explicit config path
  let configPath: string;
  if (options.configPath) {
    configPath = path.isAbsolute(options.configPath)
      ? options.configPath
      : path.resolve(searchDir, options.configPath);

    if (!fs.existsSync(configPath)) {
      throw new Error(`Config file not found: ${configPath}`);
    }
  } else {
    const foundPath = findBuildConfigFile(searchDir);
    if (!foundPath) {
      throw new Error(
        `No config file found. Expected one of: ${BUILD_CONFIG_FILE_NAMES.join(", ")} in ${searchDir}`,
      );
    }
    configPath = foundPath;
  }

  const configDir = path.dirname(configPath);

  // Load and parse the config file
  const rawConfig = await loadBuildConfigFile(configPath);

  // Validate with Zod
  const parseResult = ComposerBuildConfigSchema.safeParse(rawConfig);
  if (!parseResult.success) {
    const issues = parseResult.error.issues
      .map((issue) => `  - ${issue.path.join(".")}: ${issue.message}`)
      .join("\n");
    throw new Error(`Invalid composer build config at ${configPath}:\n${issues}`);
  }

  // Resolve relative paths
  const resolvedConfig = resolveBuildConfigPaths(parseResult.data, configDir);

  const result: LoadedBuildConfig = {
    config: resolvedConfig,
    configPath,
    configDir,
  };

  cachedResult = result;
  return result;
}

/**
 * Synchronously checks if a build config file exists in the given directory.
 *
 * @param searchDir - Directory to search in
 * @returns true if a build config file exists
 */
export function hasBuildConfigFile(searchDir: string = process.cwd()): boolean {
  return findBuildConfigFile(searchDir) !== undefined;
}

/**
 * Gets the path to the build config file without loading it.
 *
 * @param searchDir - Directory to search in
 * @returns Absolute path to config file, or undefined if not found
 */
export function getBuildConfigFilePath(searchDir: string = process.cwd()): string | undefined {
  return findBuildConfigFile(searchDir);
}
