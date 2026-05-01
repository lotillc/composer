/**
 * Unit tests for common.ts utility functions
 */

import { describe, expect, it } from "vitest";
import { denamespaceSyntheticSteps } from "../common";

type TestStep = {
  name: string;
  needs: string[];
  provides: string[];
  workflowPath?: string[];
};

// Type for the dynamically added properties from denamespaceSyntheticSteps
interface FlattenedStep {
  name: string;
  needs: string[];
  provides: string[];
  _syntheticName?: string;
  _workflowPath?: string[];
}

describe("denamespaceSyntheticSteps", () => {
  it("returns steps unchanged when no workflowPath", () => {
    const steps: TestStep[] = [
      { name: "step1", needs: ["a"], provides: ["b"] },
      { name: "step2", needs: ["b"], provides: ["c"] },
    ];

    const result = denamespaceSyntheticSteps(steps) as FlattenedStep[];

    expect(result).toHaveLength(2);
    expect(result[0]?.name).toBe("step1");
    expect(result[1]?.name).toBe("step2");
    expect(result[0]?._syntheticName).toBeUndefined();
    expect(result[1]?._syntheticName).toBeUndefined();
  });

  it("extracts real step name from namespaced name with single-level workflowPath", () => {
    const steps: TestStep[] = [
      {
        name: "childWorkflow.actualStep",
        needs: ["input"],
        provides: ["output"],
        workflowPath: ["childWorkflow"],
      },
    ];

    const result = denamespaceSyntheticSteps(steps) as FlattenedStep[];

    expect(result).toHaveLength(1);
    expect(result[0]?.name).toBe("actualStep");
    expect(result[0]?._syntheticName).toBe("childWorkflow.actualStep");
    expect(result[0]?._workflowPath).toEqual(["childWorkflow"]);
  });

  it("extracts real step name from deeply nested workflowPath", () => {
    const steps: TestStep[] = [
      {
        name: "parent.child.grandchild.deepStep",
        needs: ["a"],
        provides: ["b"],
        workflowPath: ["parent", "child", "grandchild"],
      },
    ];

    const result = denamespaceSyntheticSteps(steps) as FlattenedStep[];

    expect(result).toHaveLength(1);
    expect(result[0]?.name).toBe("deepStep");
    expect(result[0]?._syntheticName).toBe("parent.child.grandchild.deepStep");
  });

  it("handles mixed steps (with and without workflowPath)", () => {
    const steps: TestStep[] = [
      { name: "regularStep", needs: [], provides: ["a"] },
      {
        name: "composed.nestedStep",
        needs: ["a"],
        provides: ["b"],
        workflowPath: ["composed"],
      },
      { name: "anotherRegular", needs: ["b"], provides: ["c"] },
    ];

    const result = denamespaceSyntheticSteps(steps) as FlattenedStep[];

    expect(result).toHaveLength(3);
    expect(result[0]?.name).toBe("regularStep");
    expect(result[0]?._syntheticName).toBeUndefined();
    expect(result[1]?.name).toBe("nestedStep");
    expect(result[1]?._syntheticName).toBe("composed.nestedStep");
    expect(result[2]?.name).toBe("anotherRegular");
    expect(result[2]?._syntheticName).toBeUndefined();
  });

  it("preserves needs and provides arrays", () => {
    const steps: TestStep[] = [
      {
        name: "workflow.step",
        needs: ["input1", "input2"],
        provides: ["output1", "output2"],
        workflowPath: ["workflow"],
      },
    ];

    const result = denamespaceSyntheticSteps(steps) as FlattenedStep[];

    expect(result[0]?.needs).toEqual(["input1", "input2"]);
    expect(result[0]?.provides).toEqual(["output1", "output2"]);
  });

  it("handles empty workflowPath array", () => {
    const steps: TestStep[] = [
      {
        name: "stepWithEmptyPath",
        needs: [],
        provides: ["output"],
        workflowPath: [],
      },
    ];

    const result = denamespaceSyntheticSteps(steps) as FlattenedStep[];

    expect(result[0]?.name).toBe("stepWithEmptyPath");
    expect(result[0]?._syntheticName).toBeUndefined();
  });

  it("handles empty steps array", () => {
    const result = denamespaceSyntheticSteps([]);

    expect(result).toEqual([]);
  });
});
