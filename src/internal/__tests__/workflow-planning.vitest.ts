/**
 * Tests for Workflow Planning Logic
 *
 * This test suite covers the DAG execution planning algorithm that performs
 * topological sorting and batch formation for parallel execution.
 */

import { describe, expect, it } from "vitest";

import type { Step } from "../dag-sync-step";
import { planWorkflowBatches, validateWorkflowPlan } from "../workflow-planning";

// Test bag type for our test cases
interface TestBag {
  userId: string;
  userData: { name: string };
  accountId: string;
  accountData: { balance: number };
  profileData: { preferences: Record<string, unknown> };
  orderData: { items: string[] };
  result: string;
  validationResult: boolean;
  x: number;
  y: number;
  z: number;
  a: number;
  b: number;
  c: number;
  d: number;
}

// Helper function to create mock steps for testing
function createMockStep<
  Needs extends readonly (keyof TestBag)[],
  Provides extends readonly (keyof TestBag)[],
>(name: string, needs: Needs, provides: Provides): Step<TestBag, Needs, Provides> {
  return {
    name,
    needs,
    provides,
    run: async (_ctx, _bag) => {
      return Object.fromEntries(
        provides.map((key) => [key, `mock-${String(key)}`]),
      ) as Pick<TestBag, Provides[number]>;
    },
  };
}

describe("workflow-planning", () => {
  describe("planWorkflowBatches", () => {
    describe("Simple workflows", () => {
      it("should handle empty workflow", () => {
        const plan = planWorkflowBatches<TestBag>([], new Set());

        expect(plan.batches).toHaveLength(0);
        expect(plan.producers.size).toBe(0);
        expect(plan.providedFields.size).toBe(0);
      });

      it("should handle single step with no dependencies", () => {
        const step = createMockStep("step1", [], ["result"]);
        const plan = planWorkflowBatches([step], new Set());

        expect(plan.batches).toHaveLength(1);
        expect(plan.batches[0]).toHaveLength(1);
        expect(plan.batches[0]?.[0]).toBe(step);
        expect(plan.producers.get("result")).toBe("step1");
        expect(plan.providedFields.has("result")).toBe(true);
      });

      it("should handle single step with initial field dependency", () => {
        const step = createMockStep("step1", ["userId"], ["result"]);
        const plan = planWorkflowBatches([step], new Set(["userId"]));

        expect(plan.batches).toHaveLength(1);
        expect(plan.batches[0]).toHaveLength(1);
        expect(plan.batches[0]?.[0]).toBe(step);
        expect(plan.providedFields.has("userId")).toBe(true);
        expect(plan.providedFields.has("result")).toBe(true);
      });

      it("should handle linear workflow with sequential steps", () => {
        const step1 = createMockStep("fetchUser", ["userId"], ["userData"]);
        const step2 = createMockStep("fetchAccount", ["userData"], ["accountData"]);
        const step3 = createMockStep("buildResult", ["accountData"], ["result"]);

        const plan = planWorkflowBatches([step1, step2, step3], new Set(["userId"]));

        // Should have 3 batches (linear execution)
        expect(plan.batches).toHaveLength(3);
        expect(plan.batches[0]).toEqual([step1]);
        expect(plan.batches[1]).toEqual([step2]);
        expect(plan.batches[2]).toEqual([step3]);
      });
    });

    describe("Parallel execution", () => {
      it("should group independent steps into same batch", () => {
        const step1 = createMockStep("step1", [], ["x"]);
        const step2 = createMockStep("step2", [], ["y"]);
        const step3 = createMockStep("step3", [], ["z"]);

        const plan = planWorkflowBatches([step1, step2, step3], new Set());

        // All steps are independent, should run in parallel
        expect(plan.batches).toHaveLength(1);
        expect(plan.batches[0]).toHaveLength(3);
        expect(plan.batches[0]).toContain(step1);
        expect(plan.batches[0]).toContain(step2);
        expect(plan.batches[0]).toContain(step3);
      });

      it("should create parallel batches when steps share same dependency", () => {
        const step1 = createMockStep("fetchUser", ["userId"], ["userData"]);
        const step2 = createMockStep("fetchAccount", ["userData"], ["accountData"]);
        const step3 = createMockStep("fetchProfile", ["userData"], ["profileData"]);
        const step4 = createMockStep("buildResult", ["accountData", "profileData"], ["result"]);

        const plan = planWorkflowBatches([step1, step2, step3, step4], new Set(["userId"]));

        // Should have 3 batches:
        // Batch 0: step1 (needs userId)
        // Batch 1: step2 and step3 (both need userData - parallel)
        // Batch 2: step4 (needs both accountData and profileData)
        expect(plan.batches).toHaveLength(3);
        expect(plan.batches[0]).toEqual([step1]);
        expect(plan.batches[1]).toHaveLength(2);
        expect(plan.batches[1]).toContain(step2);
        expect(plan.batches[1]).toContain(step3);
        expect(plan.batches[2]).toEqual([step4]);
      });

      it("should handle diamond dependency pattern", () => {
        // Diamond: stepA -> (stepB, stepC) -> stepD
        const stepA = createMockStep("stepA", [], ["x"]);
        const stepB = createMockStep("stepB", ["x"], ["y"]);
        const stepC = createMockStep("stepC", ["x"], ["z"]);
        const stepD = createMockStep("stepD", ["y", "z"], ["result"]);

        const plan = planWorkflowBatches([stepA, stepB, stepC, stepD], new Set());

        expect(plan.batches).toHaveLength(3);
        expect(plan.batches[0]).toEqual([stepA]);
        expect(plan.batches[1]).toHaveLength(2);
        expect(plan.batches[1]).toContain(stepB);
        expect(plan.batches[1]).toContain(stepC);
        expect(plan.batches[2]).toEqual([stepD]);
      });
    });

    describe("Complex dependency patterns", () => {
      it("should handle multiple initial fields", () => {
        const step1 = createMockStep("step1", ["userId", "accountId"], ["result"]);

        const plan = planWorkflowBatches([step1], new Set(["userId", "accountId"]));

        expect(plan.batches).toHaveLength(1);
        expect(plan.batches[0]).toEqual([step1]);
        expect(plan.providedFields.has("userId")).toBe(true);
        expect(plan.providedFields.has("accountId")).toBe(true);
        expect(plan.providedFields.has("result")).toBe(true);
      });

      it("should handle step with multiple dependencies from different producers", () => {
        const step1 = createMockStep("step1", [], ["x"]);
        const step2 = createMockStep("step2", [], ["y"]);
        const step3 = createMockStep("step3", ["x", "y"], ["result"]);

        const plan = planWorkflowBatches([step1, step2, step3], new Set());

        // step1 and step2 can run in parallel, step3 waits for both
        expect(plan.batches).toHaveLength(2);
        expect(plan.batches[0]).toHaveLength(2);
        expect(plan.batches[0]).toContain(step1);
        expect(plan.batches[0]).toContain(step2);
        expect(plan.batches[1]).toEqual([step3]);
      });

      it("should handle step providing multiple fields", () => {
        const step1 = createMockStep("multiProvider", [], ["x", "y", "z"]);
        const step2 = createMockStep("consumer1", ["x"], ["a"]);
        const step3 = createMockStep("consumer2", ["y"], ["b"]);
        const step4 = createMockStep("consumer3", ["z"], ["c"]);

        const plan = planWorkflowBatches([step1, step2, step3, step4], new Set());

        // step1 runs first, then step2/3/4 can run in parallel
        expect(plan.batches).toHaveLength(2);
        expect(plan.batches[0]).toEqual([step1]);
        expect(plan.batches[1]).toHaveLength(3);
        expect(plan.batches[1]).toContain(step2);
        expect(plan.batches[1]).toContain(step3);
        expect(plan.batches[1]).toContain(step4);
      });

      it("should handle transitive dependencies correctly", () => {
        // Chain: step1 -> step2 -> step3 -> step4
        const step1 = createMockStep("step1", [], ["a"]);
        const step2 = createMockStep("step2", ["a"], ["b"]);
        const step3 = createMockStep("step3", ["b"], ["c"]);
        const step4 = createMockStep("step4", ["c"], ["d"]);

        const plan = planWorkflowBatches([step1, step2, step3, step4], new Set());

        expect(plan.batches).toHaveLength(4);
        expect(plan.batches[0]).toEqual([step1]);
        expect(plan.batches[1]).toEqual([step2]);
        expect(plan.batches[2]).toEqual([step3]);
        expect(plan.batches[3]).toEqual([step4]);
      });

      it("should handle tree-like dependency structure", () => {
        // Tree structure:
        //        root
        //       /  |  \
        //      b1  b2  b3
        //       \  |  /
        //        final
        const root = createMockStep("root", [], ["x"]);
        const branch1 = createMockStep("branch1", ["x"], ["a"]);
        const branch2 = createMockStep("branch2", ["x"], ["b"]);
        const branch3 = createMockStep("branch3", ["x"], ["c"]);
        const final = createMockStep("final", ["a", "b", "c"], ["result"]);

        const plan = planWorkflowBatches([root, branch1, branch2, branch3, final], new Set());

        expect(plan.batches).toHaveLength(3);
        expect(plan.batches[0]).toEqual([root]);
        expect(plan.batches[1]).toHaveLength(3);
        expect(plan.batches[1]).toContain(branch1);
        expect(plan.batches[1]).toContain(branch2);
        expect(plan.batches[1]).toContain(branch3);
        expect(plan.batches[2]).toEqual([final]);
      });
    });

    describe("Producer tracking", () => {
      it("should track which step produces each field", () => {
        const step1 = createMockStep("fetchUser", [], ["userData"]);
        const step2 = createMockStep("fetchAccount", [], ["accountData"]);

        const plan = planWorkflowBatches([step1, step2], new Set());

        expect(plan.producers.get("userData")).toBe("fetchUser");
        expect(plan.producers.get("accountData")).toBe("fetchAccount");
      });

      it("should track multiple fields from same producer", () => {
        const step = createMockStep("multiStep", [], ["x", "y", "z"]);

        const plan = planWorkflowBatches([step], new Set());

        expect(plan.producers.get("x")).toBe("multiStep");
        expect(plan.producers.get("y")).toBe("multiStep");
        expect(plan.producers.get("z")).toBe("multiStep");
      });

      it("should include all provided fields in providedFields set", () => {
        const step1 = createMockStep("step1", [], ["x"]);
        const step2 = createMockStep("step2", ["x"], ["y"]);

        const plan = planWorkflowBatches([step1, step2], new Set(["userId"]));

        expect(plan.providedFields.has("userId")).toBe(true);
        expect(plan.providedFields.has("x")).toBe(true);
        expect(plan.providedFields.has("y")).toBe(true);
      });
    });

    describe("Error handling: Duplicate producers", () => {
      it("should throw error when two steps produce same field", () => {
        const step1 = createMockStep("step1", [], ["result"]);
        const step2 = createMockStep("step2", [], ["result"]);

        expect(() => planWorkflowBatches([step1, step2], new Set())).toThrow(
          /Duplicate producer for field "result"/,
        );
        expect(() => planWorkflowBatches([step1, step2], new Set())).toThrow(/step1.*step2/);
      });

      it("should detect duplicate even when step provides multiple fields", () => {
        const step1 = createMockStep("step1", [], ["x", "y"]);
        const step2 = createMockStep("step2", [], ["y", "z"]);

        expect(() => planWorkflowBatches([step1, step2], new Set())).toThrow(
          /Duplicate producer for field "y"/,
        );
      });
    });

    describe("Error handling: Initial field overwrites", () => {
      it("should throw error when step tries to overwrite initial field", () => {
        const step = createMockStep("badStep", [], ["userId"]);

        expect(() => planWorkflowBatches([step], new Set(["userId"]))).toThrow(
          /cannot overwrite initial field "userId"/,
        );
        expect(() => planWorkflowBatches([step], new Set(["userId"]))).toThrow(/badStep/);
      });

      it("should prevent overwriting any initial field", () => {
        const step1 = createMockStep("step1", [], ["x"]);
        const step2 = createMockStep("step2", [], ["userId"]);

        expect(() => planWorkflowBatches([step1, step2], new Set(["userId"]))).toThrow(
          /cannot overwrite initial field/,
        );
      });

      it("should allow step to use initial field without producing it", () => {
        const step = createMockStep("step1", ["userId"], ["result"]);

        expect(() => planWorkflowBatches([step], new Set(["userId"]))).not.toThrow();
      });
    });

    describe("Error handling: Missing dependencies", () => {
      it("should throw error when step needs field with no producer", () => {
        const step = createMockStep("needsX", ["x"], ["result"]);

        expect(() => planWorkflowBatches([step], new Set())).toThrow(
          /requires field "x" but no previous step provides it/,
        );
      });

      it("should list available fields in missing dependency error", () => {
        const step1 = createMockStep("step1", [], ["a"]);
        const step2 = createMockStep("step2", ["b"], ["result"]);

        expect(() => planWorkflowBatches([step1, step2], new Set())).toThrow(/Available fields/);
        expect(() => planWorkflowBatches([step1, step2], new Set())).toThrow(/\ba\b/);
      });

      it("should suggest including field in initial data", () => {
        const step = createMockStep("needsUserId", ["userId"], ["result"]);

        expect(() => planWorkflowBatches([step], new Set())).toThrow(/initial data/);
      });

      it("should succeed when all dependencies are satisfied", () => {
        const step1 = createMockStep("producer", [], ["x"]);
        const step2 = createMockStep("consumer", ["x"], ["result"]);

        expect(() => planWorkflowBatches([step1, step2], new Set())).not.toThrow();
      });
    });

    describe("Error handling: Circular dependencies", () => {
      it("should throw error for simple circular dependency", () => {
        const step1 = createMockStep("step1", ["b"], ["a"]);
        const step2 = createMockStep("step2", ["a"], ["b"]);

        expect(() => planWorkflowBatches([step1, step2], new Set())).toThrow(
          /Unable to resolve dependencies/,
        );
        expect(() => planWorkflowBatches([step1, step2], new Set())).toThrow(/circular/i);
      });

      it("should detect self-referential step as circular dependency", () => {
        const step = createMockStep("selfRef", ["x"], ["x"]);

        expect(() => planWorkflowBatches([step], new Set())).toThrow(
          /Unable to resolve dependencies/,
        );
        expect(() => planWorkflowBatches([step], new Set())).toThrow(/selfRef/);
      });

      it("should detect circular dependency in longer chain", () => {
        // A -> B -> C -> A (circular)
        const stepA = createMockStep("stepA", ["c"], ["a"]);
        const stepB = createMockStep("stepB", ["a"], ["b"]);
        const stepC = createMockStep("stepC", ["b"], ["c"]);

        expect(() => planWorkflowBatches([stepA, stepB, stepC], new Set())).toThrow(
          /Unable to resolve dependencies/,
        );
      });
    });

    describe("Step ordering and batch independence", () => {
      it("should produce same batches regardless of step order in array", () => {
        // Note: Order of steps within a batch is unspecified and doesn't matter
        // because they execute in parallel. This test verifies batch structure
        // consistency, not within-batch ordering.
        const stepA = createMockStep("stepA", [], ["x"]);
        const stepB = createMockStep("stepB", ["x"], ["y"]);
        const stepC = createMockStep("stepC", ["x"], ["z"]);

        const plan1 = planWorkflowBatches([stepA, stepB, stepC], new Set());
        const plan2 = planWorkflowBatches([stepC, stepA, stepB], new Set());
        const plan3 = planWorkflowBatches([stepB, stepC, stepA], new Set());

        // All should have same structure
        expect(plan1.batches).toHaveLength(2);
        expect(plan2.batches).toHaveLength(2);
        expect(plan3.batches).toHaveLength(2);

        // First batch should always be stepA
        expect(plan1.batches[0]).toEqual([stepA]);
        expect(plan2.batches[0]).toEqual([stepA]);
        expect(plan3.batches[0]).toEqual([stepA]);

        // Second batch should contain stepB and stepC (order may vary within batch)
        expect(plan1.batches[1]).toHaveLength(2);
        expect(plan2.batches[1]).toHaveLength(2);
        expect(plan3.batches[1]).toHaveLength(2);
      });
    });

    describe("Edge cases", () => {
      it("should handle workflow with only initial fields and no steps", () => {
        const plan = planWorkflowBatches<TestBag>([], new Set(["userId", "accountId"]));

        expect(plan.batches).toHaveLength(0);
        expect(plan.providedFields.has("userId")).toBe(true);
        expect(plan.providedFields.has("accountId")).toBe(true);
      });

      it("should handle step that needs nothing and provides nothing", () => {
        const step = createMockStep("noop", [], []);

        const plan = planWorkflowBatches([step], new Set());

        expect(plan.batches).toHaveLength(1);
        expect(plan.batches[0]).toEqual([step]);
      });

      it("should handle step with needs but no provides (side-effect step)", () => {
        const producer = createMockStep("producer", [], ["x"]);
        const sideEffect = createMockStep("logger", ["x"], []);

        const plan = planWorkflowBatches([producer, sideEffect], new Set());

        expect(plan.batches).toHaveLength(2);
        expect(plan.batches[0]).toEqual([producer]);
        expect(plan.batches[1]).toEqual([sideEffect]);
        expect(plan.producers.get("x")).toBe("producer");
        expect(plan.providedFields.has("x")).toBe(true);
      });

      it("should handle step with many dependencies", () => {
        const provider1 = createMockStep("p1", [], ["a"]);
        const provider2 = createMockStep("p2", [], ["b"]);
        const provider3 = createMockStep("p3", [], ["c"]);
        const consumer = createMockStep("consumer", ["a", "b", "c"], ["result"]);

        const plan = planWorkflowBatches([provider1, provider2, provider3, consumer], new Set());

        expect(plan.batches).toHaveLength(2);
        expect(plan.batches[0]).toHaveLength(3);
        expect(plan.batches[1]).toEqual([consumer]);
      });

      it("should handle workflow with all steps depending on initial fields", () => {
        const step1 = createMockStep("step1", ["userId"], ["userData"]);
        const step2 = createMockStep("step2", ["userId"], ["accountData"]);
        const step3 = createMockStep("step3", ["userId"], ["profileData"]);

        const plan = planWorkflowBatches([step1, step2, step3], new Set(["userId"]));

        // All steps can run in parallel since they only depend on initial field
        expect(plan.batches).toHaveLength(1);
        expect(plan.batches[0]).toHaveLength(3);
      });
    });

    describe("Real-world workflow patterns", () => {
      it("should handle typical ETL pattern: extract -> transform -> load", () => {
        const extract = createMockStep("extract", ["userId"], ["userData"]);
        const transform = createMockStep("transform", ["userData"], ["accountData"]);
        const load = createMockStep("load", ["accountData"], ["result"]);

        const plan = planWorkflowBatches([extract, transform, load], new Set(["userId"]));

        expect(plan.batches).toHaveLength(3);
        expect(plan.batches.map((batch) => batch.map((s) => s.name))).toEqual([
          ["extract"],
          ["transform"],
          ["load"],
        ]);
      });

      it("should handle validation + processing pattern", () => {
        const validate = createMockStep("validate", ["userId"], ["validationResult"]);
        const fetchData = createMockStep("fetchData", ["userId"], ["userData"]);
        const process = createMockStep("process", ["validationResult", "userData"], ["result"]);

        const plan = planWorkflowBatches([validate, fetchData, process], new Set(["userId"]));

        // validate and fetchData can run in parallel
        expect(plan.batches).toHaveLength(2);
        expect(plan.batches[0]).toHaveLength(2);
        expect(plan.batches[0]).toContain(validate);
        expect(plan.batches[0]).toContain(fetchData);
        expect(plan.batches[1]).toEqual([process]);
      });

      it("should handle fan-out then fan-in pattern", () => {
        // Common pattern: fetch one thing, then fetch many things in parallel, then combine
        const fetchUser = createMockStep("fetchUser", ["userId"], ["userData"]);
        const fetchOrders = createMockStep("fetchOrders", ["userData"], ["orderData"]);
        const fetchAccount = createMockStep("fetchAccount", ["userData"], ["accountData"]);
        const fetchProfile = createMockStep("fetchProfile", ["userData"], ["profileData"]);
        const combine = createMockStep(
          "combine",
          ["orderData", "accountData", "profileData"],
          ["result"],
        );

        const plan = planWorkflowBatches(
          [fetchUser, fetchOrders, fetchAccount, fetchProfile, combine],
          new Set(["userId"]),
        );

        expect(plan.batches).toHaveLength(3);
        expect(plan.batches[0]).toEqual([fetchUser]);
        expect(plan.batches[1]).toHaveLength(3); // Three parallel fetches
        expect(plan.batches[2]).toEqual([combine]);
      });
    });
  });

  describe("validateWorkflowPlan", () => {
    it("should return success with plan for valid workflow", () => {
      const step = createMockStep("step1", [], ["result"]);

      const result = validateWorkflowPlan([step], new Set());

      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.plan).toBeDefined();
        expect(result.plan.batches).toHaveLength(1);
      }
    });

    it("should return error for workflow with circular dependencies", () => {
      const step1 = createMockStep("step1", ["b"], ["a"]);
      const step2 = createMockStep("step2", ["a"], ["b"]);

      const result = validateWorkflowPlan([step1, step2], new Set());

      expect(result.success).toBe(false);
      if (!result.success) {
        expect(result.error).toContain("Unable to resolve dependencies");
      }
    });

    it("should return error for workflow with missing dependencies", () => {
      const step = createMockStep("step1", ["x"], ["result"]);

      const result = validateWorkflowPlan([step], new Set());

      expect(result.success).toBe(false);
      if (!result.success) {
        expect(result.error).toContain("requires field");
        expect(result.error).toContain("x");
      }
    });

    it("should return error for workflow with duplicate producers", () => {
      const step1 = createMockStep("step1", [], ["x"]);
      const step2 = createMockStep("step2", [], ["x"]);

      const result = validateWorkflowPlan([step1, step2], new Set());

      expect(result.success).toBe(false);
      if (!result.success) {
        expect(result.error).toContain("Duplicate producer");
        expect(result.error).toContain("x");
      }
    });

    it("should return error for workflow trying to overwrite initial field", () => {
      const step = createMockStep("step1", [], ["userId"]);

      const result = validateWorkflowPlan([step], new Set(["userId"]));

      expect(result.success).toBe(false);
      if (!result.success) {
        expect(result.error).toContain("cannot overwrite initial field");
        expect(result.error).toContain("userId");
      }
    });

    it("should return error message as string even for non-Error exceptions", () => {
      // This tests the error handling when something other than Error is thrown
      // Though in practice, our implementation always throws Error objects

      const step = createMockStep("step1", ["userId"] as const, ["result"]);

      const result = validateWorkflowPlan([step], new Set());

      expect(result.success).toBe(false);
      if (!result.success) {
        expect(typeof result.error).toBe("string");
        expect(result.error.length).toBeGreaterThan(0);
      }
    });

    it("should handle empty workflow", () => {
      const result = validateWorkflowPlan<TestBag>([], new Set());

      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.plan.batches).toHaveLength(0);
      }
    });

    it("should validate complex valid workflow", () => {
      const step1 = createMockStep("step1", ["userId"], ["userData"]);
      const step2 = createMockStep("step2", ["userData"], ["accountData"]);
      const step3 = createMockStep("step3", ["userData"], ["profileData"]);
      const step4 = createMockStep("step4", ["accountData", "profileData"], ["result"]);

      const result = validateWorkflowPlan([step1, step2, step3, step4], new Set(["userId"]));

      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.plan.batches).toHaveLength(3);
        expect(result.plan.batches[0]).toEqual([step1]);
        expect(result.plan.batches[1]).toHaveLength(2);
        expect(result.plan.batches[2]).toEqual([step4]);
      }
    });
  });

  describe("Integration with step metadata", () => {
    it("should preserve step metadata through planning", () => {
      const stepWithMetadata = {
        name: "standardStep",
        needs: [],
        provides: ["result"],
        run: async () => ({ result: "test" }),
        workerProfile: "standard",
        workflowPath: ["parent", "child"],
        _originalRun: async () => ({ result: "original" }),
      } as Step<TestBag, readonly [], readonly ["result"]>;

      const plan = planWorkflowBatches([stepWithMetadata], new Set());

      const plannedStep = plan.batches[0]?.[0];
      expect(plannedStep).toBe(stepWithMetadata);
      expect(plannedStep?.workerProfile).toBe("standard");
      expect(plannedStep?.workflowPath).toEqual(["parent", "child"]);
      expect(plannedStep?._originalRun).toBeDefined();
    });

    it("should handle steps without optional metadata", () => {
      const step = createMockStep("simpleStep", [], ["result"]);

      const plan = planWorkflowBatches([step], new Set());

      const plannedStep = plan.batches[0]?.[0];
      expect(plannedStep).toBe(step);
      expect(plannedStep?.workerProfile).toBeUndefined();
      expect(plannedStep?.workflowPath).toBeUndefined();
    });

    it("should preserve nested workflow paths through planning", () => {
      const nestedStep = {
        name: "parent.child.step",
        needs: [],
        provides: ["result"],
        run: async () => ({ result: "test" }),
        workflowPath: ["parent", "child", "grandchild"],
      } as Step<TestBag, readonly [], readonly ["result"]>;

      const plan = planWorkflowBatches([nestedStep], new Set());

      const plannedStep = plan.batches[0]?.[0];
      expect(plannedStep).toBe(nestedStep);
      expect(plannedStep?.workflowPath).toEqual(["parent", "child", "grandchild"]);
    });
  });

  describe("Type safety verification", () => {
    it("should maintain type information through planning", () => {
      type SpecificBag = {
        input: string;
        output: number;
      };

      const typedStep = {
        name: "typedStep",
        needs: ["input"],
        provides: ["output"],
        run: async (_ctx: unknown, bag: Pick<SpecificBag, "input">) => {
          return { output: bag.input.length };
        },
      } as Step<SpecificBag, readonly ["input"], readonly ["output"]>;

      const plan = planWorkflowBatches([typedStep], new Set<keyof SpecificBag>(["input"]));

      expect(plan.batches).toHaveLength(1);
      expect(plan.providedFields.has("input")).toBe(true);
      expect(plan.providedFields.has("output")).toBe(true);
    });
  });
});
