/**
 * Shared dynamic module import utility.
 *
 * Handles the fragile import logic needed for both compiled JS (dist/) and
 * TypeScript (src/) environments, including require.cache invalidation and
 * ESM cache-busting. Extracted so that all definition collectors share a
 * single implementation and don't drift.
 *
 * @module import-module
 */

import { resolve } from "node:path";

/**
 * Dynamically imports a TypeScript/JavaScript module with proper cache handling.
 *
 * - In compiled JS environments: rewrites `src/` to `dist/` and `.ts` to `.js`,
 *   then clears `require.cache` before importing.
 * - In ESM/tsx environments: appends a cache-busting query parameter.
 */
export async function importModuleFromFile(filePath: string): Promise<Record<string, unknown>> {
  let absolutePath = resolve(filePath);

  const isCompiledJs = __filename.endsWith(".js");
  const isNodeEnv = typeof require !== "undefined" && typeof require.cache !== "undefined";
  const shouldUseRequireCache = isCompiledJs || isNodeEnv;

  if (isCompiledJs) {
    const lastSrcIdx = absolutePath.lastIndexOf("/src/");
    if (lastSrcIdx !== -1) {
      absolutePath = `${absolutePath.substring(0, lastSrcIdx)}/dist/${absolutePath.substring(lastSrcIdx + 5)}`;
    }
    absolutePath = absolutePath.replace(/\.ts$/, ".js");
  }

  if (shouldUseRequireCache) {
    const resolvedPath = require.resolve(absolutePath);
    delete require.cache[resolvedPath];
    for (const key of Object.keys(require.cache)) {
      if (key === absolutePath || key === resolvedPath) {
        delete require.cache[key];
      }
    }
    return await import(absolutePath);
  }

  const cacheBuster = `?t=${Date.now()}`;
  return await import(`${absolutePath}${cacheBuster}`);
}
