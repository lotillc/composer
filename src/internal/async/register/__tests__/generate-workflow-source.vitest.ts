import { readFile } from "node:fs/promises";
import { describe, expect, it } from "vitest";
import { createWorkflow, fanOut, step } from "../../../../index";
import { writeWorkflowSourceFile } from "../generate-workflow-source";

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

describe("writeWorkflowSourceFile", () => {
  it("should include child workflows referenced by fanOut steps", async () => {
    const outputPath = await writeWorkflowSourceFile([parentWorkflow]);
    const code = await readFile(outputPath, "utf-8");

    expect(code).toContain('exports["test-parent-workflow"]');
    expect(code).toContain('exports["test-child-workflow"]');
  });

  it("should not duplicate workflows when child is also explicitly provided", async () => {
    const outputPath = await writeWorkflowSourceFile([parentWorkflow, childWorkflow]);
    const code = await readFile(outputPath, "utf-8");

    const parentMatches = code.match(/exports\["test-parent-workflow"\]/g);
    const childMatches = code.match(/exports\["test-child-workflow"\]/g);
    expect(parentMatches).toHaveLength(1);
    expect(childMatches).toHaveLength(1);
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
