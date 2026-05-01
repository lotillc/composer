/**
 * Tests for Type Guards
 */

import { describe, expect, it } from "vitest";

import { isFanOut, isStep, isWorkflow } from "../type-guards";

describe("type-guards", () => {
  describe("isStep", () => {
    it("should return true for valid step with all required fields", () => {
      const validStep = {
        name: "myStep",
        needs: [],
        provides: ["result"],
        run: async () => ({ result: "test" }),
      };

      expect(isStep(validStep)).toBe(true);
    });

    it("should return true for step with dependencies", () => {
      const stepWithDeps = {
        name: "processData",
        needs: ["input", "config"],
        provides: ["output"],
        run: async () => ({ output: "processed" }),
      };

      expect(isStep(stepWithDeps)).toBe(true);
    });

    it("should return true for step with workerProfile", () => {
      const stepWithProfile = {
        name: "standardStep",
        needs: [],
        provides: ["result"],
        run: async () => ({ result: "test" }),
        workerProfile: "standard",
      };

      expect(isStep(stepWithProfile)).toBe(true);
    });

    it("should return false for object missing name", () => {
      const missingName = {
        needs: [],
        provides: ["result"],
        run: async () => ({ result: "test" }),
      };

      expect(isStep(missingName)).toBe(false);
    });

    it("should return false for object with non-string name", () => {
      const invalidName = {
        name: 123,
        needs: [],
        provides: ["result"],
        run: async () => ({ result: "test" }),
      };

      expect(isStep(invalidName)).toBe(false);
    });

    it("should return false for object missing needs", () => {
      const missingNeeds = {
        name: "myStep",
        provides: ["result"],
        run: async () => ({ result: "test" }),
      };

      expect(isStep(missingNeeds)).toBe(false);
    });

    it("should return false for object with non-array needs", () => {
      const invalidNeeds = {
        name: "myStep",
        needs: "not-an-array",
        provides: ["result"],
        run: async () => ({ result: "test" }),
      };

      expect(isStep(invalidNeeds)).toBe(false);
    });

    it("should return false for object missing provides", () => {
      const missingProvides = {
        name: "myStep",
        needs: [],
        run: async () => ({ result: "test" }),
      };

      expect(isStep(missingProvides)).toBe(false);
    });

    it("should return false for object with non-array provides", () => {
      const invalidProvides = {
        name: "myStep",
        needs: [],
        provides: "not-an-array",
        run: async () => ({ result: "test" }),
      };

      expect(isStep(invalidProvides)).toBe(false);
    });

    it("should return false for object missing run", () => {
      const missingRun = {
        name: "myStep",
        needs: [],
        provides: ["result"],
      };

      expect(isStep(missingRun)).toBe(false);
    });

    it("should return false for object with non-function run", () => {
      const invalidRun = {
        name: "myStep",
        needs: [],
        provides: ["result"],
        run: "not-a-function",
      };

      expect(isStep(invalidRun)).toBe(false);
    });

    it("should return false for null", () => {
      expect(isStep(null)).toBeFalsy();
    });

    it("should return false for undefined", () => {
      expect(isStep(undefined)).toBeFalsy();
    });

    it("should return false for primitive values", () => {
      expect(isStep("string")).toBe(false);
      expect(isStep(123)).toBe(false);
      expect(isStep(true)).toBe(false);
    });

    it("should return false for arrays", () => {
      expect(isStep([])).toBe(false);
      expect(isStep([1, 2, 3])).toBe(false);
    });

    it("should return false for functions", () => {
      expect(isStep(() => {})).toBe(false);
    });

    it("should return false for empty objects", () => {
      expect(isStep({})).toBe(false);
    });
  });

  describe("isWorkflow", () => {
    it("should return true for valid workflow with all fields", () => {
      const validWorkflow = {
        name: "myWorkflow",
        steps: [],
        requiredInitial: ["input"],
      };

      expect(isWorkflow(validWorkflow)).toBe(true);
    });

    it("should return true for workflow with empty requiredInitial", () => {
      const workflowEmptyRequired = {
        name: "myWorkflow",
        steps: [],
        requiredInitial: [],
      };

      expect(isWorkflow(workflowEmptyRequired)).toBe(true);
    });

    it("should return true for workflow without requiredInitial field", () => {
      const workflowNoRequired = {
        name: "independentWorkflow",
        steps: [],
      };

      expect(isWorkflow(workflowNoRequired)).toBe(true);
    });

    it("should return true for workflow with requiredInitial undefined", () => {
      const workflowUndefinedRequired = {
        name: "myWorkflow",
        steps: [],
        requiredInitial: undefined,
      };

      expect(isWorkflow(workflowUndefinedRequired)).toBe(true);
    });

    it("should return true for workflow with steps array", () => {
      const workflowWithSteps = {
        name: "myWorkflow",
        steps: [{ name: "step1" }, { name: "step2" }],
        requiredInitial: [],
      };

      expect(isWorkflow(workflowWithSteps)).toBe(true);
    });

    it("should return false for object missing name", () => {
      const missingName = {
        steps: [],
        requiredInitial: [],
      };

      expect(isWorkflow(missingName)).toBe(false);
    });

    it("should return false for object with non-string name", () => {
      const invalidName = {
        name: 123,
        steps: [],
        requiredInitial: [],
      };

      expect(isWorkflow(invalidName)).toBe(false);
    });

    it("should return false for object missing steps", () => {
      const missingSteps = {
        name: "myWorkflow",
        requiredInitial: [],
      };

      expect(isWorkflow(missingSteps)).toBe(false);
    });

    it("should return false for object with non-array steps", () => {
      const invalidSteps = {
        name: "myWorkflow",
        steps: "not-an-array",
        requiredInitial: [],
      };

      expect(isWorkflow(invalidSteps)).toBe(false);
    });

    it("should return false for object with non-array requiredInitial (when present)", () => {
      const invalidRequired = {
        name: "myWorkflow",
        steps: [],
        requiredInitial: "not-an-array",
      };

      expect(isWorkflow(invalidRequired)).toBe(false);
    });

    it("should return false for null", () => {
      expect(isWorkflow(null)).toBeFalsy();
    });

    it("should return false for undefined", () => {
      expect(isWorkflow(undefined)).toBeFalsy();
    });

    it("should return false for primitive values", () => {
      expect(isWorkflow("string")).toBe(false);
      expect(isWorkflow(123)).toBe(false);
      expect(isWorkflow(true)).toBe(false);
    });

    it("should return false for arrays", () => {
      expect(isWorkflow([])).toBe(false);
      expect(isWorkflow([1, 2, 3])).toBe(false);
    });

    it("should return false for functions", () => {
      expect(isWorkflow(() => {})).toBe(false);
    });

    it("should return false for empty objects", () => {
      expect(isWorkflow({})).toBe(false);
    });
  });

  describe("isFanOut", () => {
    it("should return true for a FanOut object (step with __fanOut metadata)", () => {
      const fanOut = {
        name: "myFanOut",
        needs: ["items"],
        provides: ["results"],
        run: async () => ({}),
        __fanOut: {
          childWorkflow: { name: "child", steps: [], requiredInitial: ["item"] },
          mapInput: () => [],
          aggregateResults: () => ({}),
          concurrency: 5,
        },
      };

      expect(isFanOut(fanOut)).toBe(true);
    });

    it("should return false for a regular step without __fanOut", () => {
      const step = {
        name: "regularStep",
        needs: [],
        provides: ["output"],
        run: async () => ({ output: "test" }),
      };

      expect(isFanOut(step)).toBe(false);
    });

    it("should return false when __fanOut is null", () => {
      const stepWithNullFanOut = {
        name: "step",
        needs: [],
        provides: [],
        run: async () => ({}),
        __fanOut: null,
      };

      expect(isFanOut(stepWithNullFanOut)).toBe(false);
    });

    it("should return false for non-step objects with __fanOut", () => {
      const notAStep = {
        __fanOut: {
          childWorkflow: {},
          mapInput: () => [],
          aggregateResults: () => ({}),
          concurrency: 1,
        },
      };

      expect(isFanOut(notAStep)).toBe(false);
    });

    it("should return false for primitives and nulls", () => {
      expect(isFanOut(null)).toBeFalsy();
      expect(isFanOut(undefined)).toBeFalsy();
      expect(isFanOut("string")).toBe(false);
      expect(isFanOut(42)).toBe(false);
    });
  });

  describe("isStep vs isWorkflow distinction", () => {
    it("should distinguish between steps and workflows", () => {
      const step = {
        name: "myStep",
        needs: [],
        provides: ["result"],
        run: async () => ({ result: "test" }),
      };

      const workflow = {
        name: "myWorkflow",
        steps: [],
        requiredInitial: [],
      };

      expect(isStep(step)).toBe(true);
      expect(isWorkflow(step)).toBe(false);

      expect(isStep(workflow)).toBe(false);
      expect(isWorkflow(workflow)).toBe(true);
    });

    it("should not mistake objects with overlapping fields", () => {
      const ambiguous = {
        name: "ambiguous",
        steps: [],
        needs: [],
        provides: [],
        run: () => {},
      };

      // This is actually a valid step (has name, needs, provides, run)
      expect(isStep(ambiguous)).toBe(true);
      // And also a valid workflow (has name, steps, no requiredInitial requirement)
      expect(isWorkflow(ambiguous)).toBe(true);
    });
  });
});
