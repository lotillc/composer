import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import ts from "typescript";
import { afterAll, describe, expect, it } from "vitest";

// Typechecks a consumer's standalone registry file, the shape the README
// documents, against the package source. The in-package test augments from a
// file that already imports, so it cannot catch a snippet that only works
// inside a module.

const srcDir = fileURLToPath(new URL("../../../../", import.meta.url));
const workDirs: string[] = [];

afterAll(() => {
  for (const dir of workDirs) {
    rmSync(dir, { recursive: true, force: true });
  }
});

const STEPS = `
import { step } from "@lotiai/composer";
import { defineBuildConfig } from "@lotiai/composer/build-config";

type Bag = { id: string; signed: boolean };

export const signRequest = step<Bag>()({
  name: "signRequest",
  needs: ["id"],
  provides: ["signed"],
  workerProfile: "request-signing",
  run: async () => ({ signed: true }),
});

export const config = defineBuildConfig({
  workerProfiles: { "request-signing": { minCount: 1, initialCount: 1, maxCount: 1 } },
});
`;

function typecheckConsumer(registryFile: string, registryName: string): string[] {
  const dir = mkdtempSync(join(tmpdir(), "composer-registry-"));
  workDirs.push(dir);
  const registryPath = join(dir, registryName);
  const stepsPath = join(dir, "steps.ts");
  writeFileSync(registryPath, registryFile);
  writeFileSync(stepsPath, STEPS);

  const program = ts.createProgram([registryPath, stepsPath], {
    module: ts.ModuleKind.NodeNext,
    moduleResolution: ts.ModuleResolutionKind.NodeNext,
    target: ts.ScriptTarget.ES2022,
    strict: true,
    noEmit: true,
    skipLibCheck: true,
    types: ["node"],
    typeRoots: [join(srcDir, "../node_modules/@types")],
    baseUrl: dir,
    paths: {
      "@lotiai/composer": [join(srcDir, "index.ts")],
      "@lotiai/composer/build-config": [join(srcDir, "build-config/index.ts")],
    },
  });
  return ts
    .getPreEmitDiagnostics(program)
    .filter((d) => d.file?.fileName === registryPath || d.file?.fileName === stepsPath)
    .map((d) => ts.flattenDiagnosticMessageText(d.messageText, "\n"));
}

const REGISTRY = `
declare module "@lotiai/composer" {
  interface WorkerProfileRegistry {
    "request-signing": true;
  }
}
`;

describe("standalone WorkerProfileRegistry augmentation", () => {
  for (const registryName of ["worker-profiles.ts", "worker-profiles.d.ts"]) {
    it(`augments the package from ${registryName} with export {}`, { timeout: 60_000 }, () => {
      expect(typecheckConsumer(`${REGISTRY}\nexport {};\n`, registryName)).toEqual([]);
    });
  }

  it("shadows the package from a .d.ts with no import or export", { timeout: 60_000 }, () => {
    // Pins why the documented snippet carries `export {}`.
    expect(typecheckConsumer(REGISTRY, "worker-profiles.d.ts")).toContainEqual(
      expect.stringMatching(/has no exported member 'step'/),
    );
  });
});
