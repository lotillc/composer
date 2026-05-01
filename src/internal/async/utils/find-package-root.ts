import { existsSync } from "node:fs";
import { join, resolve } from "node:path";

/**
 * Find the package root by searching upward for package.json.
 *
 * @param startDir - Directory to start searching from (typically __dirname)
 * @returns Absolute path to the package root directory
 * @throws Error if package.json cannot be found
 */
export function findPackageRoot(startDir: string): string {
  let currentDir = resolve(startDir);

  while (true) {
    const packageJsonPath = join(currentDir, "package.json");
    if (existsSync(packageJsonPath)) {
      return currentDir;
    }

    const parentDir = resolve(currentDir, "..");
    if (parentDir === currentDir) {
      break;
    }
    currentDir = parentDir;
  }

  throw new Error(`Could not find package.json searching up from ${startDir}`);
}
