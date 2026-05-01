/**
 * Dev Command
 *
 * Development watch mode for Composer async workflows.
 * Watches for TypeScript file changes, then:
 * 1. Compiles TypeScript
 * 2. Restarts Temporal workers (workers load definitions at startup)
 *
 * Works in both standalone projects and monorepos. The watch root is
 * auto-detected by walking up from cwd to find `pnpm-lock.yaml`, and
 * watch patterns are configured via `dev.watchPatterns` in the build config.
 */

import { type ChildProcess, spawn } from "node:child_process";
import { resolve } from "node:path";
import type { CommandModule } from "yargs";
import { loadBuildConfig } from "../../build-config/index";
import type { ComposerBuildConfig } from "../../build-config/schema";

interface DevOptions {
  config?: string;
  verbose: boolean;
  workerScript?: string;
}

export const devCommand: CommandModule<object, DevOptions> = {
  command: "dev",
  describe: "Start development watch mode (compile, restart workers on changes)",
  builder: (yargs) =>
    yargs
      .option("config", {
        alias: "c",
        type: "string",
        description: "Path to composer.build-config.ts (auto-detected if not specified)",
      })
      .option("verbose", {
        type: "boolean",
        default: false,
        description: "Enable verbose output",
      })
      .option("worker-script", {
        type: "string",
        description:
          "Path to the compiled start-all-workers .js script (relative to cwd). Overrides dev.startAllWorkersJsScript in config.",
      }),
  handler: async (argv) => {
    const { config: configPath, verbose, workerScript: workerScriptArg } = argv;

    if (verbose) {
      console.log("Loading configuration...");
    }

    const { config } = await loadBuildConfig(configPath ? { configPath } : undefined);

    // Resolve worker script: CLI arg > config > error
    const workerScriptRelative = workerScriptArg ?? config.dev?.startAllWorkersJsScript;

    if (!workerScriptRelative) {
      console.error(
        "Error: No worker script specified. Provide --worker-script or set dev.startAllWorkersJsScript in composer.build-config.ts.",
      );
      process.exit(1);
    }

    if (verbose) {
      console.log(`  workerScript: ${workerScriptRelative}`);
    }

    await runDevMode(config, workerScriptRelative, verbose);
  },
};

// ─── Dev Mode Implementation ───────────────────────────────────────────────────
// Ported from packages/composer/scripts/watch-and-run.ts with config-driven
// worker script resolution.

// Track state
let workerProcess: ChildProcess | null = null;
let debounceTimeout: NodeJS.Timeout | null = null;
let isRebuilding = false;
let isShuttingDown = false; // For double Ctrl+C force kill pattern
let watcher: import("chokidar").FSWatcher | null = null; // Module-level so shutdown() can close it

const DEBOUNCE_MS = 300; // Wait for multi-file saves to complete

/**
 * Spawn a process and return its exit code.
 */
async function spawnWithExitCode(
  command: string,
  args: string[],
  description: string,
): Promise<number> {
  return new Promise((resolve) => {
    const startTime = Date.now();
    const proc = spawn(command, args, {
      stdio: "inherit",
      cwd: process.cwd(),
    });

    proc.on("close", (code) => {
      const duration = ((Date.now() - startTime) / 1000).toFixed(1);
      if (code === 0) {
        console.log(`[checkmark] ${description} (${duration}s)\n`);
      } else {
        console.error(`[x] ${description} failed with code ${code}\n`);
      }
      resolve(code ?? 1);
    });

    proc.on("error", (err) => {
      console.error(`[x] ${description} error:`, err.message);
      resolve(1);
    });
  });
}

/**
 * Compile TypeScript via `tsc --build`.
 *
 * Resolves `tsc` from the `typescript` package bundled as a direct dependency
 * of @lotiai/composer, so it works regardless of whether the consumer has
 * `typescript` installed or `tsc` on PATH.
 *
 * This follows project references in tsconfig.json, so in a monorepo it
 * compiles the current package and all its transitive dependencies.
 */
async function compile(): Promise<boolean> {
  const tscPath = require.resolve("typescript/bin/tsc");
  console.log("Compiling TypeScript (tsc --build)...");
  const exitCode = await spawnWithExitCode(process.execPath, [tscPath, "--build"], "Compilation");
  return exitCode === 0;
}

/**
 * Stop the currently running workers
 */
async function stopWorkers(): Promise<void> {
  if (!workerProcess) return;

  return new Promise((resolve) => {
    console.log("Stopping workers...");

    // TODO: stdio stream destruction before SIGTERM may be unnecessary -- the
    // 'exit' handler + SIGKILL fallback should ensure cleanup. Try removing
    // the stream destruction and verify that stopWorkers() still resolves
    // promptly (look for: the promise hanging because stdout/stderr keep
    // the event loop alive after the child is killed).
    if (workerProcess!.stdout) {
      workerProcess!.stdout.removeAllListeners();
      workerProcess!.stdout.destroy();
    }
    if (workerProcess!.stderr) {
      workerProcess!.stderr.removeAllListeners();
      workerProcess!.stderr.destroy();
    }

    // Try graceful shutdown first
    workerProcess!.kill("SIGTERM");

    const timeout = setTimeout(() => {
      // Force kill if still running after 5s
      if (workerProcess && !workerProcess.killed) {
        console.log("Force killing workers...");
        workerProcess.kill("SIGKILL");
      }
    }, 5000);

    workerProcess!.on("exit", () => {
      clearTimeout(timeout);
      workerProcess = null;
      console.log("[checkmark] Workers stopped\n");
      resolve();
    });
  });
}

/**
 * Start the Temporal workers
 */
async function startWorkers(workerScriptPath: string): Promise<void> {
  console.log("Starting workers...");

  // Track worker readiness
  // We wait for Temporal SDK's "Worker state changed" logs with state: 'RUNNING'
  // This ensures workflow bundles are compiled and workers are actually polling
  let workflowWorkerReady = false;
  let activityWorkerReady = false;
  let outputBuffer = ""; // Buffer to accumulate multi-line JSON logs from Temporal

  const readyPromise = new Promise<void>((resolve, reject) => {
    const timeout = setTimeout(() => {
      reject(new Error("Workers failed to start within 30 seconds"));
    }, 30000);

    const checkReady = () => {
      if (workflowWorkerReady && activityWorkerReady) {
        clearTimeout(timeout);
        resolve();
      }
    };

    workerProcess = spawn("node", [workerScriptPath], {
      stdio: ["inherit", "pipe", "pipe"],
      env: {
        ...process.env,
        FORCE_COLOR: "1", // Preserve colors when piping output
      },
    });

    // Parse stdout/stderr to detect when workers are actually ready
    // We check both streams because Temporal logs can go to either
    const parseWorkerOutput = (output: string) => {
      // Add to buffer and keep last 5000 chars to avoid unbounded growth
      outputBuffer += output;
      if (outputBuffer.length > 5000) {
        outputBuffer = outputBuffer.slice(-5000);
      }

      // Detect readiness from composer's own log messages, which are emitted
      // regardless of the task queue names configured by the consumer.
      if (outputBuffer.includes("Workflow Workers ready")) {
        if (!workflowWorkerReady) {
          console.log("   -> Workflow Worker now RUNNING");
          workflowWorkerReady = true;
          checkReady();
        }
      }

      if (outputBuffer.includes("Activity Workers ready")) {
        if (!activityWorkerReady) {
          console.log("   -> Activity Worker now RUNNING");
          activityWorkerReady = true;
          checkReady();
        }
      }
    };

    workerProcess.stdout?.on("data", (data) => {
      const output = data.toString();
      parseWorkerOutput(output);
      process.stdout.write(output);
    });

    workerProcess.stderr?.on("data", (data) => {
      const output = data.toString();
      parseWorkerOutput(output);
      process.stderr.write(output);
    });

    workerProcess.on("error", (err) => {
      clearTimeout(timeout);
      reject(err);
    });

    workerProcess.on("exit", (code, signal) => {
      clearTimeout(timeout);
      if (signal !== "SIGTERM" && signal !== "SIGKILL") {
        console.error(`Workers exited unexpectedly (code: ${code}, signal: ${signal})`);
      }
      workerProcess = null;
    });
  });

  try {
    await readyPromise;
    console.log("\nTemporal Ready\n");
  } catch (error) {
    console.error("Worker startup failed:", error);
    throw error;
  }
}

/**
 * Full rebuild: compile -> restart
 */
async function rebuild(
  workerScriptPath: string,
  _verbose: boolean,
  trigger?: string,
): Promise<void> {
  if (isRebuilding) {
    console.log("Skipping rebuild (already in progress)...\n");
    return;
  }

  isRebuilding = true;

  const startTime = Date.now();
  console.log("\n-------------------------------------------");
  if (trigger) {
    console.log(`Change detected: ${trigger}`);
  }
  console.log("Rebuilding...\n");

  try {
    const compileSuccess = await compile();
    if (!compileSuccess) {
      console.error("Compilation failed. Fix errors and save to retry.");
      console.log("Still watching for changes...\n");
      return;
    }

    await stopWorkers();
    await startWorkers(workerScriptPath);

    const totalTime = ((Date.now() - startTime) / 1000).toFixed(1);
    console.log(`Total rebuild time: ${totalTime}s`);
    console.log("Watching for changes...\n");
  } catch (error) {
    console.error("Rebuild error:", error);
    console.log("Still watching for changes...\n");
  } finally {
    isRebuilding = false;
  }
}

/**
 * Graceful shutdown
 */
async function shutdown(): Promise<void> {
  // Force kill on second Ctrl+C
  if (isShuttingDown) {
    console.log("\n\nForce killing all processes...");

    // Force kill worker process immediately
    if (workerProcess && !workerProcess.killed) {
      workerProcess.kill("SIGKILL");
    }

    // TODO: setImmediate wrapping process.exit may be unnecessary -- process.exit()
    // is synchronous and setImmediate does not reliably flush stdout. Try
    // replacing with direct process.exit() and verify shutdown still works
    // cleanly (check for truncated log output on exit).
    setImmediate(() => process.exit(1));
    return; // Don't proceed with graceful shutdown
  }

  isShuttingDown = true;

  console.log("\n\nShutting down watch mode... (Press Ctrl+C again to force quit)");

  // Set a timeout to force exit if graceful shutdown hangs
  // This ensures we always exit even if watcher or workers don't close cleanly
  const forceExitTimeout = setTimeout(() => {
    console.log("\nGraceful shutdown timed out, forcing exit...");
    process.exit(1);
  }, 10000); // 10 second timeout

  try {
    // Clear any pending debounce
    if (debounceTimeout) {
      clearTimeout(debounceTimeout);
    }

    // Close file watcher
    if (watcher) {
      // Remove all listeners before closing to prevent them keeping event loop alive
      watcher.removeAllListeners();
      await watcher.close();
    }

    // Stop workers gracefully
    await stopWorkers();

    clearTimeout(forceExitTimeout);
    console.log("Shutdown complete");
    // TODO: setImmediate wrapping process.exit may be unnecessary -- process.exit()
    // is synchronous and setImmediate does not reliably flush stdout. Try
    // replacing with direct process.exit() and verify shutdown still works
    // cleanly (check for truncated log output on exit).
    setImmediate(() => process.exit(0));
  } catch (error) {
    clearTimeout(forceExitTimeout);
    console.error("Shutdown error:", error);
    // TODO: setImmediate wrapping process.exit may be unnecessary -- process.exit()
    // is synchronous and setImmediate does not reliably flush stdout. Try
    // replacing with direct process.exit() and verify shutdown still works
    // cleanly (check for truncated log output on exit).
    setImmediate(() => process.exit(1));
  }
}

/**
 * Main dev mode entry point
 */
async function runDevMode(
  config: ComposerBuildConfig,
  workerScriptRelative: string,
  verbose: boolean,
): Promise<void> {
  // Dynamic import to keep chokidar as an optional devDependency
  const chokidar = await import("chokidar");

  const workerScriptPath = resolve(process.cwd(), workerScriptRelative);

  // watchPatterns are already resolved to absolute paths by the config loader
  const watchPatterns = config.dev.watchPatterns!;

  console.log("Starting Composer Development Mode...\n");
  console.log(`Watching: ${watchPatterns.join(", ")}`);
  console.log("Excludes: dist/, node_modules/, *.gen.{ts,json}");
  console.log("\nRuntime discovery mode: workers import compiled .js files directly");
  console.log("");

  // Handle file change with debouncing
  const handleFileChange = (path: string): void => {
    if (debounceTimeout) {
      clearTimeout(debounceTimeout);
    }

    // Debounce: wait for editor to finish saving multiple files
    debounceTimeout = setTimeout(() => {
      const relativePath = path.replace(`${process.cwd()}/`, "");
      rebuild(workerScriptPath, verbose, relativePath);
    }, DEBOUNCE_MS);
  };

  // Initial build - be forgiving for dev mode, allow starting with errors
  console.log("Initial setup...\n");
  const compileSuccess = await compile();
  if (!compileSuccess) {
    console.error("Initial compilation failed.");
    console.log("   Fix errors and save to retry.\n");
    console.log("Starting watch mode anyway...\n");
  }

  // Only start workers if compilation succeeded
  if (compileSuccess) {
    await startWorkers(workerScriptPath);
    console.log("Watching for changes...\n");
  } else {
    console.log("Initial setup incomplete. Workers not started.");
    console.log("   Fix errors and save to complete setup.");
    console.log("Watching for changes...\n");
  }

  // Setup file watcher (patterns are already absolute from config loader)
  watcher = chokidar.watch(watchPatterns, {
    persistent: true,
    ignoreInitial: true,
    ignored: [
      "**/node_modules/**",
      "**/dist/**",
      "**/*.gen.ts",
      "**/*.gen.json",
      "**/coverage/**",
      "**/test-results/**",
    ],
  });

  watcher.on("change", handleFileChange);
  watcher.on("add", handleFileChange);

  watcher.on("error", (error) => {
    console.error("Watcher error:", error);
  });

  // Setup shutdown handlers
  // Wrap shutdown() in arrow function with void to properly handle async without waiting
  process.on("SIGINT", () => void shutdown());
  process.on("SIGTERM", () => void shutdown());
}
