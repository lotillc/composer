import { readFile } from "node:fs/promises";
import { describe, expect, it } from "vitest";
import { createWorkflow, fanOut, step } from "../../../../index.js";
import type { Workflow } from "../../../dag-sync-workflow.js";
import { generateWorkflowPlan, writeWorkflowSourceFile } from "../generate-workflow-source.js";

interface ChildBag {
  input: string;
  output: string;
}

interface ParentBag {
  items: string[];
  childResults: string[];
}

const childStep = step<ChildBag>()({
  name: "childStep",
  needs: ["input"] as const,
  provides: ["output"] as const,
  run: async () => ({ output: "done" }),
});

const childWorkflow = createWorkflow<ChildBag>("test-child-workflow")
  .requires("input")
  .build([childStep]);

const parentFanOut = fanOut<ParentBag>()({
  name: "parentFanOut",
  needs: ["items"] as const,
  childWorkflow,
  mapInput: (bag) => bag.items.map((item) => ({ input: item })),
  provides: ["childResults"] as const,
  aggregateResults: (results) => ({
    childResults: results.map((r) => r.output),
  }),
  concurrency: 5,
});

const parentWorkflow = createWorkflow<ParentBag>("test-parent-workflow")
  .requires("items")
  .build([parentFanOut]);

/**
 * Evaluates generated workflow source with a stub `require`, returning its
 * exports. The stubbed `createWorkflowFunction` returns the plan it is given.
 */
function evaluateWorkflowSource(code: string): Record<string, { name: string }> {
  const exports: Record<string, { name: string }> = {};
  const stubRequire = () => ({ createWorkflowFunction: (plan: { name: string }) => plan });
  new Function("exports", "require", code)(exports, stubRequire);
  return exports;
}

describe("writeWorkflowSourceFile", () => {
  const upgrading = createWorkflow<ChildBag>("test-upgrading-workflow")
    .requires("input")
    .build([childStep])
    .versioning("AUTO_UPGRADE");

  it("emits a workflow's versioning behavior for a versioned worker", async () => {
    const code = await readFile(
      await writeWorkflowSourceFile([upgrading], { workerVersioning: true }),
      "utf-8",
    );
    expect(code).toContain('"versioningBehavior":"AUTO_UPGRADE"');
  });

  // Temporal's core SDK rejects every completion that carries a versioning behavior from
  // an unversioned worker, which would leave the workflow retrying its first task forever.
  it("drops a workflow's versioning behavior for an unversioned worker", async () => {
    const code = await readFile(await writeWorkflowSourceFile([upgrading]), "utf-8");
    expect(code).not.toContain("versioningBehavior");
  });

  it("drops stage boundaries' next-run behavior for an unversioned worker", async () => {
    interface StagedBag {
      input: string;
      first: string;
      second: string;
    }
    const first = step<StagedBag>()({
      name: "first",
      needs: ["input"],
      provides: ["first"],
      run: async () => ({ first: "1" }),
    });
    const second = step<StagedBag>()({
      name: "second",
      needs: ["first"],
      provides: ["second"],
      run: async () => ({ second: "2" }),
    });
    const staged = createWorkflow<StagedBag>("test-staged-workflow")
      .requires("input")
      .build([first, second])
      .continueAsNew("after-first", { afterStep: first });
    const versioned = await readFile(
      await writeWorkflowSourceFile([staged], { workerVersioning: true }),
      "utf-8",
    );
    expect(versioned).toContain('"initialVersioningBehavior":"AUTO_UPGRADE"');
    const unversioned = await readFile(await writeWorkflowSourceFile([staged]), "utf-8");
    expect(unversioned).toContain('"stageBoundaries":[{"name":"after-first","afterBatch":0}]');
  });

  it("should include child workflows referenced by fanOut steps", async () => {
    const outputPath = await writeWorkflowSourceFile([parentWorkflow]);
    const code = await readFile(outputPath, "utf-8");

    const exported = evaluateWorkflowSource(code);
    expect(Object.keys(exported).sort()).toEqual(["test-child-workflow", "test-parent-workflow"]);
  });

  it("should not duplicate workflows when child is also explicitly provided", async () => {
    const outputPath = await writeWorkflowSourceFile([parentWorkflow, childWorkflow]);
    const code = await readFile(outputPath, "utf-8");

    const parentMatches = code.match(/"test-parent-workflow", \{ value:/g);
    const childMatches = code.match(/"test-child-workflow", \{ value:/g);
    expect(parentMatches).toHaveLength(1);
    expect(childMatches).toHaveLength(1);
  });

  it.each([
    "has spaces",
    'has "double" quotes',
    "has 'single' quotes",
    "1-leading-digit",
    "__proto__",
  ])("emits parseable source for a workflow named %j", async (name) => {
    // createWorkflow() rejects these names; the generator must stay safe regardless.
    const workflow = { ...childWorkflow, name } as Workflow<any, any, any, any, any>;

    const outputPath = await writeWorkflowSourceFile([workflow]);
    const code = await readFile(outputPath, "utf-8");

    const exported = evaluateWorkflowSource(code);
    expect(Object.hasOwn(exported, name)).toBe(true);
    expect(exported[name]?.name).toBe(name);
    expect(Object.getPrototypeOf(exported)).toBe(Object.prototype);
  });

  it("should serialize Infinity concurrency as null (not omit it)", async () => {
    const unboundedFanOut = fanOut<ParentBag>()({
      name: "unboundedFanOut",
      needs: ["items"] as const,
      childWorkflow,
      mapInput: (bag) => bag.items.map((item) => ({ input: item })),
      provides: ["childResults"] as const,
      aggregateResults: (results) => ({
        childResults: results.map((r) => r.output),
      }),
    });

    const unboundedWorkflow = createWorkflow<ParentBag>("test-unbounded-workflow")
      .requires("items")
      .build([unboundedFanOut]);

    const outputPath = await writeWorkflowSourceFile([unboundedWorkflow]);
    const code = await readFile(outputPath, "utf-8");

    expect(code).toContain('"concurrency":null');
    expect(code).not.toContain("Infinity");
  });

  it("should throw on workflow name collision from distinct objects", async () => {
    const conflicting = createWorkflow<ChildBag>("test-child-workflow")
      .requires("input")
      .build([childStep]);

    await expect(writeWorkflowSourceFile([parentWorkflow, conflicting])).rejects.toThrow(
      'Workflow name collision: "test-child-workflow"',
    );
  });
});

describe("generateWorkflowPlan activity config", () => {
  it("should carry asyncRetry.nonRetryableErrorTypes into the step's activity config", () => {
    const rejectableStep = step<ChildBag>()({
      name: "rejectableStep",
      needs: ["input"] as const,
      provides: ["output"] as const,
      asyncRetry: {
        maximumAttempts: 2,
        nonRetryableErrorTypes: ["CONTENT_REJECTED", "VALIDATION_FAILED"],
      },
      run: async () => ({ output: "done" }),
    });

    const workflow = createWorkflow<ChildBag>("test-non-retryable-workflow")
      .requires("input")
      .build([rejectableStep]);

    const plan = generateWorkflowPlan(workflow);

    expect(plan.batches[0]?.steps[0]?.activityConfig?.retry).toEqual({
      maximumAttempts: 2,
      nonRetryableErrorTypes: ["CONTENT_REJECTED", "VALIDATION_FAILED"],
    });
  });

  it("should survive JSON serialization into the generated workflow source", async () => {
    const rejectableStep = step<ChildBag>()({
      name: "rejectableStep",
      needs: ["input"] as const,
      provides: ["output"] as const,
      asyncRetry: { nonRetryableErrorTypes: ["CONTENT_REJECTED"] },
      run: async () => ({ output: "done" }),
    });

    const workflow = createWorkflow<ChildBag>("test-non-retryable-source-workflow")
      .requires("input")
      .build([rejectableStep]);

    const outputPath = await writeWorkflowSourceFile([workflow]);
    const code = await readFile(outputPath, "utf-8");

    expect(code).toContain('"nonRetryableErrorTypes":["CONTENT_REJECTED"]');
  });

  it("should leave activityConfig undefined for steps with no async overrides", () => {
    const plan = generateWorkflowPlan(childWorkflow);

    expect(plan.batches[0]?.steps[0]?.activityConfig).toBeUndefined();
  });
});
