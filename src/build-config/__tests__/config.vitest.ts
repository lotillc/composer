import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import {
  ComposerBuildConfigSchema,
  clearBuildConfigCache,
  defineBuildConfig,
  hasBuildConfigFile,
  loadBuildConfig,
} from "../index";

describe("ComposerBuildConfigSchema", () => {
  it("validates a minimal config (all fields optional with defaults)", () => {
    const config = {};

    const result = ComposerBuildConfigSchema.safeParse(config);
    expect(result.success).toBe(true);
    if (result.success) {
      expect(result.data.workerProfiles).toEqual({}); // default
    }
  });

  it("applies default values for optional fields", () => {
    const config = {};

    const result = ComposerBuildConfigSchema.safeParse(config);
    expect(result.success).toBe(true);
    if (result.success) {
      expect(result.data.workerProfiles).toEqual({});
    }
  });

  it("validates worker profile overrides", () => {
    const config = {
      workerProfiles: {
        standard: {
          cpu: 1024,
          memory: 4096,
          maxConcurrentActivities: 20,
        },
      },
    };

    const result = ComposerBuildConfigSchema.safeParse(config);
    expect(result.success).toBe(true);
    if (result.success) {
      expect(result.data.workerProfiles.standard?.cpu).toBe(1024);
      expect(result.data.workerProfiles.standard?.memory).toBe(4096);
    }
  });

  it("rejects deprecated desiredCount", () => {
    const config = {
      workerProfiles: {
        standard: {
          desiredCount: 3,
        },
      },
    };

    const result = ComposerBuildConfigSchema.safeParse(config);
    expect(result.success).toBe(false);
  });

  it("rejects unknown top-level properties (strict object)", () => {
    const config = {
      output: { dir: "dist" }, // not a valid property
    };

    const result = ComposerBuildConfigSchema.safeParse(config);
    expect(result.success).toBe(false);
  });
});

describe("defineBuildConfig", () => {
  it("parses input and returns validated config with defaults", () => {
    const input = {
      dev: {
        startAllWorkersJsScript: "dist/scripts/start-all-workers.js",
      },
    };

    const result = defineBuildConfig(input);
    expect(result).toEqual({
      workerProfiles: {},
      dev: {
        startAllWorkersJsScript: "dist/scripts/start-all-workers.js",
      },
    });
  });
});

describe("loadBuildConfig", () => {
  let tempDir: string;

  beforeEach(() => {
    clearBuildConfigCache();
    tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "composer-config-test-"));
  });

  afterEach(() => {
    fs.rmSync(tempDir, { recursive: true, force: true });
  });

  it("loads and validates a JSON config file", async () => {
    const configContent = {
      workerProfiles: {
        standard: { cpu: 1024 },
      },
    };

    const configPath = path.join(tempDir, "composer.build-config.json");
    fs.writeFileSync(configPath, JSON.stringify(configContent));

    const { config, configDir } = await loadBuildConfig({ searchDir: tempDir });

    expect(config.workerProfiles.standard?.cpu).toBe(1024);
    expect(configDir).toBe(tempDir);
  });

  it("loads config from dist/build-config.js when root-level files are absent", async () => {
    // Simulates Docker images where only dist/ survives the prune step
    const distDir = path.join(tempDir, "dist");
    fs.mkdirSync(distDir);
    // Mimic tsc CommonJS output for `export default defineBuildConfig({})`
    fs.writeFileSync(
      path.join(distDir, "build-config.js"),
      `"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.default = { workerProfiles: {}, dev: {} };`,
    );

    const { config, configDir } = await loadBuildConfig({ searchDir: tempDir });
    expect(config).toBeDefined();
    expect(config.workerProfiles).toEqual({});
    expect(configDir).toBe(distDir);
  });

  it("throws error when no config file found", async () => {
    await expect(loadBuildConfig({ searchDir: tempDir })).rejects.toThrow(/No config file found/);
  });

  it("throws error for invalid config", async () => {
    const invalidConfig = { unknownField: true };
    const configPath = path.join(tempDir, "composer.build-config.json");
    fs.writeFileSync(configPath, JSON.stringify(invalidConfig));

    await expect(loadBuildConfig({ searchDir: tempDir })).rejects.toThrow(
      /Invalid composer build config/,
    );
  });

  it("resolves dev.watchPatterns to absolute paths", async () => {
    const configContent = {
      dev: {
        watchPatterns: ["src/**/*.ts"],
      },
    };

    const configPath = path.join(tempDir, "composer.build-config.json");
    fs.writeFileSync(configPath, JSON.stringify(configContent));

    const { config } = await loadBuildConfig({ searchDir: tempDir });

    expect(config.dev.watchPatterns).toHaveLength(1);
    expect(path.isAbsolute(config.dev.watchPatterns![0]!)).toBe(true);
  });
});

describe("hasBuildConfigFile", () => {
  let tempDir: string;

  beforeEach(() => {
    tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "composer-config-test-"));
  });

  afterEach(() => {
    fs.rmSync(tempDir, { recursive: true, force: true });
  });

  it("returns false when no config file exists", () => {
    expect(hasBuildConfigFile(tempDir)).toBe(false);
  });

  it("returns true when composer.build-config.json exists", () => {
    fs.writeFileSync(path.join(tempDir, "composer.build-config.json"), "{}");
    expect(hasBuildConfigFile(tempDir)).toBe(true);
  });

  it("returns true when composer.build-config.ts exists", () => {
    fs.writeFileSync(path.join(tempDir, "composer.build-config.ts"), "export default {};");
    expect(hasBuildConfigFile(tempDir)).toBe(true);
  });

  it("returns true when composer.build-config.js exists", () => {
    fs.writeFileSync(path.join(tempDir, "composer.build-config.js"), "module.exports = {};");
    expect(hasBuildConfigFile(tempDir)).toBe(true);
  });

  it("returns true when dist/build-config.js exists", () => {
    const distDir = path.join(tempDir, "dist");
    fs.mkdirSync(distDir);
    fs.writeFileSync(path.join(distDir, "build-config.js"), "module.exports = {};");
    expect(hasBuildConfigFile(tempDir)).toBe(true);
  });
});
