#!/usr/bin/env node
/**
 * Verifies that Temporal's webpack bundler can still build a workflow bundle
 * from the packaged tarball.
 *
 * This is the one failure mode that neither `tsc` nor vitest can see. The
 * workflow entrypoint is generated as a CommonJS *string* at runtime into
 * os.tmpdir(), and it `require()`s a module from this package -- which is an
 * ES module. Whether that boundary works is decided inside webpack.
 *
 * It must run against an installed tarball rather than the source checkout.
 * Temporal sets `resolve.extensionAlias: { ".js": [".ts", ".js"] }`, so from a
 * checkout the factory resolves to workflow-factory.ts, which does not match
 * webpack's `/\.js$/` default rule and is therefore classified
 * javascript/auto. Installed, it resolves to dist/*.js under a
 * `"type": "module"` package and is classified javascript/esm. Only the second
 * case is what ships.
 *
 * No Temporal server required -- bundleWorkflowCode is offline.
 *
 * Usage: node scripts/verify-workflow-bundle.mjs <path-to-installed-project>
 */
import { createRequire } from "node:module";
import { dirname, resolve } from "node:path";

const projectDir = process.argv[2];
if (!projectDir) {
  console.error("usage: node scripts/verify-workflow-bundle.mjs <installed-project-dir>");
  process.exit(2);
}

const req = createRequire(`${projectDir}/package.json`);
const { createWorkflow, step } = await import(req.resolve("@lotiai/composer"));
const { bundleWorkflowCode } = await import(req.resolve("@temporalio/worker"));

// Not in the exports map on purpose: this module is internal, but it is what
// generates the bundle entrypoint at runtime, so it is what must be exercised.
const composerRoot = resolve(dirname(req.resolve("@lotiai/composer")), "..");
const { writeWorkflowSourceFile } = await import(
  resolve(composerRoot, "dist/internal/async/register/generate-workflow-source.js")
);

const first = step()({
  name: "first",
  needs: [],
  provides: ["value"],
  run: async () => ({ value: "hello" }),
});
const second = step()({
  name: "second",
  needs: ["value"],
  provides: ["shouted"],
  run: async (_ctx, bag) => ({ shouted: String(bag.value).toUpperCase() }),
});

// A hyphenated name matters: it is not a valid JS identifier, which is why the
// generated source uses exports["..."] rather than a named export.
const workflow = createWorkflow("esm-bundle-smoke-workflow").build([first, second]);

const workflowsPath = await writeWorkflowSourceFile([workflow]);
console.log(`generated entrypoint: ${workflowsPath}`);

// Mirror the bundlerOptions that workflow-worker.ts passes. The entrypoint
// lives in os.tmpdir(), which has no node_modules above it, so webpack cannot
// find @temporalio/workflow by walking up from there.
const temporalNodeModulesDir = resolve(
  dirname(req.resolve("@temporalio/workflow/package.json")),
  "..",
  "..",
);

const { code } = await bundleWorkflowCode({
  workflowsPath,
  webpackConfigHook: (config) => {
    config.resolve = config.resolve ?? {};
    const modules = config.resolve.modules ?? [];
    if (!modules.includes(temporalNodeModulesDir)) {
      modules.push(temporalNodeModulesDir);
    }
    config.resolve.modules = modules;
    return config;
  },
});

if (!code || code.length < 100_000) {
  throw new Error(`workflow bundle looks truncated: ${code?.length ?? 0} bytes`);
}
if (!code.includes("esm-bundle-smoke-workflow")) {
  throw new Error("workflow bundle does not contain the registered workflow name");
}
console.log(`workflow bundle OK - ${code.length} bytes`);
