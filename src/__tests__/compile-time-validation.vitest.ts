/**
 * Compile-time Validation Tests
 *
 * These tests verify that our DAG workflow engine catches dependency errors
 * at compile time using TypeScript's type system.
 *
 * We use @ts-expect-error to assert that specific code should fail compilation.
 * If the code compiles successfully, the test will fail.
 */

import { describe, expect, it } from "vitest";

import { createWorkflow, step } from "../internal";
import { createTestStep, type TestBag, testAsyncComposer, testComposer } from "./test-utils";

// Self-contained test steps for compile-time validation testing
const testStepA = createTestStep("testStepA", [], ["input"], () => ({ input: "test input" }));

const testStepB = createTestStep("testStepB", ["input"], ["processed"], (bag) => ({
  processed: bag.input.toUpperCase(),
}));

const testStepC = createTestStep("testStepC", ["processed"], ["result"], (bag) => ({
  result: `Result: ${bag.processed}`,
}));

const testStepD = createTestStep("testStepD", ["input"], ["count"], (bag) => ({
  count: bag.input.length,
}));

const configuredAndInputStep = createTestStep(
  "configuredAndInputStep",
  ["configured", "input"],
  ["result"],
  (bag) => ({
    result: `configured=${bag.configured}; input=${bag.input}`,
  }),
);

describe("Compile-time Validation", () => {
  describe("Missing Dependencies", () => {
    it("should catch missing dependencies in single-step workflows", () => {
      // This should fail: testStepB needs "input" but no step provides it
      // @ts-expect-error - Expected error: "A step needs input but it is not available". If this test shows "Unused '@ts-expect-error' directive", it means compile time checking is broken.
      const invalidWorkflow1 = createWorkflow<TestBag>("test-workflow").build([testStepB]);

      // This should fail: testStepC needs "processed" but no step provides it
      // @ts-expect-error - Expected error: "A step needs processed but it is not available". If this test shows "Unused '@ts-expect-error' directive", it means compile time checking is broken.
      const invalidWorkflow2 = createWorkflow<TestBag>("test-workflow").build([testStepC]);

      void invalidWorkflow1;
      void invalidWorkflow2;
    });

    it("should catch missing dependencies in multi-step workflows", () => {
      // @ts-expect-error - testStepC needs "processed" but testStepA only provides "input"
      const invalidMultiStepWorkflow = createWorkflow<TestBag>("test-workflow").build([
        testStepA, // provides "input"
        testStepC, // needs "processed" (not provided by testStepA)
      ]);

      void invalidMultiStepWorkflow;
    });
  });

  describe("Valid Workflows Should Compile", () => {
    it("should allow valid workflows to compile without errors", () => {
      // Valid workflow with all dependencies satisfied
      const validWorkflow1 = createWorkflow<TestBag>("test-workflow").requires("input").build([
        testStepB, // needs "input", provides "processed"
        testStepC, // needs "processed", provides "result"
        testStepD, // needs "input", provides "count"
      ]);

      // Valid workflow with no dependencies
      const validWorkflow2 = createWorkflow<TestBag>("test-workflow").build([testStepA]);

      // Valid workflow with initial fields properly specified
      const validWorkflow3 = createWorkflow<TestBag>("test-workflow")
        .requires("input")
        .build([testStepB, testStepC, testStepD]);

      expect(validWorkflow1).toBeDefined();
      expect(validWorkflow2).toBeDefined();
      expect(validWorkflow3).toBeDefined();
    });
  });

  describe("Configure and Requires Overlap Prevention", () => {
    it("should prevent requiring a field that is already configured", () => {
      // This should fail: cannot require a field that's already configured
      const invalidWorkflow = createWorkflow<TestBag>("test-workflow")
        .configure({ input: "configured-value" })
        // @ts-expect-error - Type constraint prevents requiring configured fields
        .requires("input") // ← Error: "input" is already configured
        .build([]);

      void invalidWorkflow;
    });

    it("should prevent configuring a field that is already required", () => {
      // This should fail: cannot configure a field that's already required
      const invalidWorkflow = createWorkflow<TestBag>("test-workflow")
        .requires("input")
        // @ts-expect-error - Type constraint prevents configuring required fields
        .configure({ input: "configured-value" })
        .build([]);

      void invalidWorkflow;
    });

    it("should allow requiring fields that are NOT configured", () => {
      // This should work: requiring different fields than configured
      // Configure "processed" but require "input" (no overlap)
      const validWorkflow = createWorkflow<TestBag>("test-workflow")
        .configure({ count: 42 })
        .requires("input") // ← OK: "input" is not configured
        .build([testStepB]); // testStepB provides "processed", doesn't touch "count"

      expect(validWorkflow).toBeDefined();
    });

    it("should allow configuring fields that are NOT required", () => {
      // This should work: requiring different fields than configured
      // Require "input" but configure "count" (no overlap)
      const validWorkflow = createWorkflow<TestBag>("test-workflow")
        .requires("input")
        .configure({ count: 42 })
        .build([testStepB]);

      expect(validWorkflow).toBeDefined();
      expect(validWorkflow.requiredInitial).toEqual(["input"]);
      expect(validWorkflow.configuredValues).toEqual({ count: 42 });
    });

    it("should allow configuring fields without requiring any", () => {
      // This should work: configure without requires
      const validWorkflow = createWorkflow<TestBag>("test-workflow")
        .configure({ input: "default-input" })
        .build([testStepB, testStepC]);

      expect(validWorkflow).toBeDefined();
    });
  });

  describe("Configure and Requires Order Independence", () => {
    it("should validate dependencies when requires comes before configure", () => {
      const validWorkflow = createWorkflow<TestBag>("test-workflow")
        .requires("input")
        .configure({ configured: "configured-value" })
        .build([configuredAndInputStep]);

      expect(validWorkflow).toBeDefined();
      expect(validWorkflow.requiredInitial).toEqual(["input"]);
      expect(validWorkflow.configuredValues).toEqual({ configured: "configured-value" });
    });

    it("should still catch missing dependencies when requires comes before configure", () => {
      const invalidWorkflow = createWorkflow<TestBag>("test-workflow")
        .requires("input")
        .configure({ count: 42 })
        // @ts-expect-error - configuredAndInputStep also needs "configured"
        .build([configuredAndInputStep]);

      void invalidWorkflow;
    });
  });

  describe("Precise Return Type Inference", () => {
    it("should only allow access to fields actually provided by the workflow", async () => {
      const partialWorkflow = createWorkflow<TestBag>("test-workflow")
        .requires("input")
        .build([testStepB]); // provides "processed"

      const { bag } = await testComposer.runSyncWorkflow(partialWorkflow, { input: "test" });

      // These should work - fields actually provided
      expect(bag.input).toBe("test");
      expect(bag.processed).toBe("TEST");

      // These should cause TypeScript errors - fields not provided by this workflow
      // @ts-expect-error - Property 'doubled' does not exist on type
      bag.doubled;

      // @ts-expect-error - Property 'count' does not exist on type
      bag.count;

      // @ts-expect-error - Property 'result' does not exist on type
      bag.result;

      // @ts-expect-error - Property 'error' does not exist on type
      bag.error;
    });

    it("should provide correct types for different workflow configurations", async () => {
      const workflow = createWorkflow<TestBag>("test-workflow").requires("input").build([
        testStepB, // provides "processed"
        testStepC, // provides "result"
      ]);

      const { bag } = await testComposer.runSyncWorkflow(workflow, { input: "test" });

      // These should work
      expect(bag.input).toBe("test");
      expect(bag.processed).toBe("TEST");
      expect(bag.result).toBe("Result: TEST");

      // These should cause TypeScript errors
      // @ts-expect-error - Property 'count' does not exist on type
      bag.count;

      // @ts-expect-error - Property 'doubled' does not exist on type
      bag.doubled;
    });

    it("should handle empty workflows correctly", async () => {
      const emptyWorkflow = createWorkflow<TestBag>("test-workflow").requires("input").build([]);

      const { bag } = await testComposer.runSyncWorkflow(emptyWorkflow, { input: "test" });

      // Only initial field should be accessible
      expect(bag.input).toBe("test");

      // All other fields should cause TypeScript errors
      // @ts-expect-error - Property 'processed' does not exist on type
      bag.processed;

      // @ts-expect-error - Property 'doubled' does not exist on type
      bag.doubled;

      // @ts-expect-error - Property 'count' does not exist on type
      bag.count;

      // @ts-expect-error - Property 'result' does not exist on type
      bag.result;
    });

    it("should infer required, configured, and provided fields when requires comes before configure", async () => {
      const workflow = createWorkflow<TestBag>("test-workflow")
        .requires("input")
        .configure({ configured: "configured-value" })
        .build([testStepB]); // provides "processed"

      const { bag } = await testComposer.runSyncWorkflow(workflow, { input: "test" });

      // These should work - required, configured, and provided fields are all known
      expect(bag.input).toBe("test");
      expect(bag.configured).toBe("configured-value");
      expect(bag.processed).toBe("TEST");

      // These should cause TypeScript errors - fields not provided by this workflow
      // @ts-expect-error - Property 'count' does not exist on type
      bag.count;

      // @ts-expect-error - Property 'result' does not exist on type
      bag.result;
    });
  });

  describe("Composer Required Initial Fields Validation", () => {
    // Workflow with required fields
    const workflowWithRequires = createWorkflow<TestBag>("requires-test")
      .requires("input", "count")
      .build([testStepB, testStepC]);

    // Workflow without required fields
    const workflowWithoutRequires = createWorkflow<TestBag>("no-requires-test").build([testStepA]);

    describe("runSyncWorkflow", () => {
      it("should require all required fields to be provided", () => {
        // These tests verify compile-time behavior only.
        // The closures prevent runtime execution while @ts-expect-error validates compile-time errors.
        const missingCount = () =>
          // @ts-expect-error - Missing required field 'count'
          testComposer.runSyncWorkflow(workflowWithRequires, { input: "test" });
        const missingInput = () =>
          // @ts-expect-error - Missing required field 'input'
          testComposer.runSyncWorkflow(workflowWithRequires, { count: 5 });
        const missingBoth = () =>
          // @ts-expect-error - Missing both required fields
          testComposer.runSyncWorkflow(workflowWithRequires, {});
        void missingCount;
        void missingInput;
        void missingBoth;
      });

      it("should compile when all required fields are provided", () => {
        // This closure should compile without errors - all required fields provided
        const validCall = () =>
          testComposer.runSyncWorkflow(workflowWithRequires, { input: "test", count: 5 });
        expect(validCall).toBeInstanceOf(Function);
      });

      it("should allow optional initialData for workflows without required fields", () => {
        const noInitialData = () => testComposer.runSyncWorkflow(workflowWithoutRequires);
        const emptyInitialData = () => testComposer.runSyncWorkflow(workflowWithoutRequires, {});
        expect(noInitialData).toBeInstanceOf(Function);
        expect(emptyInitialData).toBeInstanceOf(Function);
      });
    });

    describe("runAsyncWorkflow", () => {
      it("should require all required fields to be provided", () => {
        const missingCount = () =>
          // @ts-expect-error - Missing required field 'count'
          testAsyncComposer.runAsyncWorkflow(workflowWithRequires, { input: "test" });
        const missingInput = () =>
          // @ts-expect-error - Missing required field 'input'
          testAsyncComposer.runAsyncWorkflow(workflowWithRequires, { count: 5 });
        const missingBoth = () =>
          // @ts-expect-error - Missing both required fields
          testAsyncComposer.runAsyncWorkflow(workflowWithRequires, {});
        void missingCount;
        void missingInput;
        void missingBoth;
      });

      it("should compile when all required fields are provided", () => {
        const validCall = () =>
          testAsyncComposer.runAsyncWorkflow(workflowWithRequires, { input: "test", count: 5 });
        expect(validCall).toBeInstanceOf(Function);
      });

      it("should allow optional initialData for workflows without required fields", () => {
        const noInitialData = () => testAsyncComposer.runAsyncWorkflow(workflowWithoutRequires);
        const emptyInitialData = () =>
          testAsyncComposer.runAsyncWorkflow(workflowWithoutRequires, {});
        expect(noInitialData).toBeInstanceOf(Function);
        expect(emptyInitialData).toBeInstanceOf(Function);
      });
    });
  });
});

describe("Compile-time Serializability Validation", () => {
  /**
   * These tests verify that the AssertSerializable type catches non-serializable
   * values at compile time. The Bag type includes Date, Function, and nested Date
   * fields specifically to test these constraints.
   *
   * When a step tries to provide a non-serializable field, TypeScript should error with:
   * "SERIALIZATION_ERROR: Property 'X' contains non-serializable type..."
   *
   * IMPORTANT: We use step() directly instead of createTestStep() because
   * createTestStep uses `as any` which bypasses type checking.
   */

  describe("Direct non-serializable types", () => {
    it("should catch Date fields at compile time", () => {
      const stepWithDate = step<TestBag, unknown>()({
        name: "dateStep",
        needs: ["input"],
        provides: ["timestamp"],
        // @ts-expect-error - SERIALIZATION_ERROR: Property 'timestamp' contains non-serializable type
        run: async () => ({ timestamp: new Date() }),
      });

      void stepWithDate;
    });

    it("should catch Function fields at compile time", () => {
      const stepWithFunction = step<TestBag, unknown>()({
        name: "funcStep",
        needs: ["input"],
        provides: ["callback"],
        // @ts-expect-error - SERIALIZATION_ERROR: Property 'callback' contains non-serializable type
        run: async () => ({ callback: () => console.log("hello") }),
      });

      void stepWithFunction;
    });
  });

  describe("Nested non-serializable types", () => {
    it("should catch nested Date fields at compile time", () => {
      const stepWithNestedDate = step<TestBag, unknown>()({
        name: "nestedDateStep",
        needs: ["input"],
        provides: ["nested"],
        // @ts-expect-error - SERIALIZATION_ERROR: Property 'nested' contains non-serializable type
        run: async () => ({ nested: { date: new Date() } }),
      });

      void stepWithNestedDate;
    });
  });

  describe("Valid serializable types should compile", () => {
    it("should allow primitive types", () => {
      // These should all compile without errors
      const stringStep = createTestStep("stringStep", [], ["input"], () => ({ input: "hello" }));
      const numberStep = createTestStep("numberStep", [], ["count"], () => ({ count: 42 }));
      const booleanStep = createTestStep("booleanStep", ["input"], ["result"], () => ({
        result: "true",
      }));

      expect(stringStep).toBeDefined();
      expect(numberStep).toBeDefined();
      expect(booleanStep).toBeDefined();
    });

    it("should allow nested objects with serializable values", () => {
      // Plain objects with primitives should compile
      const validNestedStep = createTestStep("validNestedStep", ["input"], ["result"], (bag) => ({
        result: JSON.stringify({ data: bag.input }),
      }));

      expect(validNestedStep).toBeDefined();
    });
  });
});

describe("Checkpoint API Compile-time Validation", () => {
  /**
   * These tests verify that the checkpoint API types work correctly at compile time.
   *
   * The `afterStep` parameter accepts a Step reference (not a string) which provides
   * compile-time validation that the step is part of the workflow.
   */

  describe("Valid checkpoint usage should compile", () => {
    it("should allow single checkpoint on workflow", () => {
      const workflowWithCheckpoint = createWorkflow<TestBag>("checkpoint-test")
        .requires("input")
        .build([testStepB, testStepC])
        .checkpoint("afterProcessing", { afterStep: testStepB });

      expect(workflowWithCheckpoint).toBeDefined();
    });

    it("should allow multiple checkpoints chained", () => {
      const workflowWithMultipleCheckpoints = createWorkflow<TestBag>("multi-checkpoint-test")
        .requires("input")
        .build([testStepB, testStepC])
        .checkpoint("first", { afterStep: testStepB })
        .checkpoint("second", { afterStep: testStepC, timeout: 60000 });

      expect(workflowWithMultipleCheckpoints).toBeDefined();
    });

    it("should allow checkpoint followed by onError", () => {
      const workflowWithCheckpointAndError = createWorkflow<TestBag>("checkpoint-error-test")
        .requires("input")
        .build([testStepB, testStepC])
        .checkpoint("afterProcessing", { afterStep: testStepB })
        .onError((_ctx, _bag, error) => error);

      expect(workflowWithCheckpointAndError).toBeDefined();
    });

    it("should preserve workflow type after checkpoint", async () => {
      const workflow = createWorkflow<TestBag>("type-preserve-test")
        .requires("input")
        .build([testStepB])
        .checkpoint("done", { afterStep: testStepB });

      // Verify the workflow can be run and returns correct types
      const { bag } = await testComposer.runSyncWorkflow(workflow, { input: "test" });

      expect(bag.input).toBe("test");
      expect(bag.processed).toBe("TEST");

      // These should still cause TypeScript errors - checkpoint doesn't change result type
      // @ts-expect-error - Property 'count' does not exist on type
      bag.count;
    });
  });

  describe("Invalid checkpoint usage should fail at compile time", () => {
    it("should reject step not in workflow", () => {
      // Create a step that is NOT in the workflow
      const orphanStep = createTestStep("orphanStep", [], ["input"], () => ({ input: "orphan" }));

      // With literal name types, TypeScript now distinguishes steps by their name.
      // orphanStep has type Step<..., "orphanStep"> which is not in the workflow's
      // Steps type (which only contains "testStepB" | "testStepC").
      //
      // Use a closure to verify compile-time error without runtime execution.
      const invalidCheckpoint = () =>
        createWorkflow<TestBag>("invalid-checkpoint-test")
          .requires("input")
          .build([testStepB, testStepC])
          // @ts-expect-error - orphanStep ("orphanStep") is not in workflow steps ("testStepB" | "testStepC")
          .checkpoint("invalid", { afterStep: orphanStep });

      void invalidCheckpoint;
      void orphanStep;
    });

    it("should reject duplicate checkpoint names", () => {
      // The CheckpointNames type parameter tracks checkpoint names as they're chained.
      // When you try to use a name that's already been used, TypeScript produces an error.
      //
      // Use a closure to verify compile-time error without runtime execution.
      const duplicateCheckpoint = () =>
        createWorkflow<TestBag>("duplicate-checkpoint-test")
          .requires("input")
          .build([testStepB, testStepC])
          .checkpoint("myCheckpoint", { afterStep: testStepB })
          // @ts-expect-error - Duplicate checkpoint name 'myCheckpoint'
          .checkpoint("myCheckpoint", { afterStep: testStepC });

      void duplicateCheckpoint;
    });

    it("should reject invalid awaitCheckpoint names in runAsyncWorkflow", () => {
      // Create a workflow with a checkpoint
      const workflowWithCheckpoint = createWorkflow<TestBag>("async-checkpoint-test")
        .requires("input")
        .build([testStepB, testStepC])
        .checkpoint("validCheckpoint", { afterStep: testStepB });

      // The awaitCheckpoint option is type-checked against the workflow's checkpoint names.
      // Only "validCheckpoint" should be allowed.
      const invalidAwaitCheckpoint = () =>
        // @ts-expect-error - "invalidCheckpoint" is not a valid checkpoint name
        testAsyncComposer.runAsyncWorkflow(
          workflowWithCheckpoint,
          { input: "test" },
          {
            awaitCheckpoint: "invalidCheckpoint",
          },
        );

      void invalidAwaitCheckpoint;
    });

    it("should reject awaitCheckpoint when workflow has no checkpoints", () => {
      // Create a workflow WITHOUT checkpoints
      const workflowWithoutCheckpoint = createWorkflow<TestBag>("no-checkpoint-test")
        .requires("input")
        .build([testStepB, testStepC]);

      // When a workflow has no checkpoints, awaitCheckpoint should not accept any value.
      // Wrapped in a closure to prevent runtime execution while verifying compile-time error.
      const invalidAwaitNoCheckpoints = () =>
        // @ts-expect-error - awaitCheckpoint is not valid when workflow has no checkpoints
        testAsyncComposer.runAsyncWorkflow(
          workflowWithoutCheckpoint,
          { input: "test" },
          {
            awaitCheckpoint: "zzz",
          },
        );

      void invalidAwaitNoCheckpoints;
    });

    it("should allow valid awaitCheckpoint names", () => {
      // Create a workflow with multiple checkpoints
      const workflowWithCheckpoints = createWorkflow<TestBag>("multi-checkpoint-test")
        .requires("input")
        .build([testStepB, testStepC])
        .checkpoint("checkpoint1", { afterStep: testStepB })
        .checkpoint("checkpoint2", { afterStep: testStepC });

      // These should compile without errors - valid checkpoint names
      const validAwait1 = () =>
        testAsyncComposer.runAsyncWorkflow(
          workflowWithCheckpoints,
          { input: "test" },
          {
            awaitCheckpoint: "checkpoint1",
          },
        );

      const validAwait2 = () =>
        testAsyncComposer.runAsyncWorkflow(
          workflowWithCheckpoints,
          { input: "test" },
          {
            awaitCheckpoint: "checkpoint2",
          },
        );

      // No options is also valid
      const noAwait = () =>
        testAsyncComposer.runAsyncWorkflow(workflowWithCheckpoints, { input: "test" });

      void validAwait1;
      void validAwait2;
      void noAwait;
    });

    it("should allow workflowId and startOnly options", () => {
      const workflowWithoutCheckpoint = createWorkflow<TestBag>("start-only-test")
        .requires("input")
        .build([testStepB, testStepC]);

      const validStartOnly = () =>
        testAsyncComposer.runAsyncWorkflow(
          workflowWithoutCheckpoint,
          { input: "test" },
          {
            workflowId: "0195e18c-cd5e-7d6c-9207-92314fef0d4c",
            startOnly: true,
          },
        );

      void validStartOnly;
    });
  });
});

describe("SyncComposer vs Composer Compile-time Validation", () => {
  /**
   * These tests verify that createComposer returns different types based on
   * whether temporal config is provided:
   *
   * - WITHOUT temporal: returns SyncComposer (only runSyncWorkflow)
   * - WITH temporal: returns full Composer (runSyncWorkflow + runAsyncWorkflow + runActivityWorkers)
   *
   * This prevents users from accidentally calling async methods without configuring Temporal.
   */

  const simpleWorkflow = createWorkflow<TestBag>("sync-async-test").build([testStepA]);

  describe("SyncComposer (no temporal config)", () => {
    it("should have runSyncWorkflow", () => {
      const validCall = () => testComposer.runSyncWorkflow(simpleWorkflow);
      expect(validCall).toBeInstanceOf(Function);
    });

    it("should NOT have runAsyncWorkflow", () => {
      // @ts-expect-error - Property 'runAsyncWorkflow' does not exist on type 'SyncComposer'
      const invalidCall = () => testComposer.runAsyncWorkflow(simpleWorkflow);
      void invalidCall;
    });

    it("should NOT have runActivityWorkers", () => {
      // @ts-expect-error - Property 'runActivityWorkers' does not exist on type 'SyncComposer'
      const invalidCall = () => testComposer.runActivityWorkers({});
      void invalidCall;
    });

    it("should NOT have runWorkflowWorkers", () => {
      // @ts-expect-error - Property 'runWorkflowWorkers' does not exist on type 'SyncComposer'
      const invalidCall = () => testComposer.runWorkflowWorkers({});
      void invalidCall;
    });

    it("should have contextProvider", () => {
      expect(testComposer.contextProvider).toBeDefined();
    });

    it("should have logger", () => {
      expect(testComposer.logger).toBeDefined();
    });

    it("should NOT have temporal", () => {
      // @ts-expect-error - Property 'temporal' does not exist on type 'SyncComposer'
      const invalidAccess = () => testComposer.temporal;
      void invalidAccess;
    });
  });

  describe("Composer (with temporal config)", () => {
    it("should have runSyncWorkflow", () => {
      const validCall = () => testAsyncComposer.runSyncWorkflow(simpleWorkflow);
      expect(validCall).toBeInstanceOf(Function);
    });

    it("should have runAsyncWorkflow", () => {
      const validCall = () => testAsyncComposer.runAsyncWorkflow(simpleWorkflow);
      expect(validCall).toBeInstanceOf(Function);
    });

    it("should have runActivityWorkers", () => {
      expect(testAsyncComposer.runActivityWorkers).toBeInstanceOf(Function);
    });

    it("should have runWorkflowWorkers", () => {
      expect(testAsyncComposer.runWorkflowWorkers).toBeInstanceOf(Function);
    });

    it("should have contextProvider", () => {
      expect(testAsyncComposer.contextProvider).toBeDefined();
    });

    it("should have logger", () => {
      expect(testAsyncComposer.logger).toBeDefined();
    });

    it("should have temporal config", () => {
      expect(testAsyncComposer.temporal).toBeDefined();
      expect(testAsyncComposer.temporal.serverAddress).toBe("localhost:7233");
      expect(testAsyncComposer.temporal.namespace).toBe("test");
    });
  });
});

/**
 * Note: These tests verify compile-time behavior using @ts-expect-error.
 *
 * How it works:
 * - @ts-expect-error tells TypeScript "the next line should have a compile error"
 * - If the line compiles successfully, TypeScript will report an error
 * - If the line fails to compile (as expected), the test passes
 *
 * This ensures our type system is working correctly to catch errors at compile time.
 */
