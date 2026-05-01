import { describe, expect, it, vi } from "vitest";

import { createWorkflow, use } from "../internal";
import { step } from "../internal/dag-sync-step";
import { WorkflowBatchError } from "../internal/errors";
import { mockTracer } from "./observability-mocks";
import { type TestBag as BaseTestBag, testComposer } from "./test-utils";

// Mock @opentelemetry/api so trace.getTracer() returns our mockTracer
vi.mock("@opentelemetry/api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@opentelemetry/api")>();
  const mocks =
    await vi.importActual<typeof import("./observability-mocks")>("./observability-mocks");
  const mockTrace = Object.create(actual.trace);
  mockTrace.getTracer = () => mocks.mockTracer;
  return { ...actual, trace: mockTrace };
});

// Mock defaults so createDefaultMetrics() returns our mockMetrics
vi.mock("../internal/defaults", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../internal/defaults")>();
  const mocks =
    await vi.importActual<typeof import("./observability-mocks")>("./observability-mocks");
  return { ...actual, createDefaultMetrics: () => mocks.mockMetrics };
});

// Extended TestBag for composition tests with additional fields
type CompositionTestBag = Omit<BaseTestBag, "result"> & {
  result?: string | number; // Allow both string and number for result
  x?: number;
  y?: number;
  z?: number;
  a?: string | number;
  b?: string | number;
  data?: string | number | Record<string, unknown>; // Allow string, number, or object
  other?: string;
  childData?: string;
  parentData?: string;
  level1?: string;
  level2?: string;
  level3?: string;
  level4?: number;
  level5?: number;
  l1?: number;
  l2?: number;
  l3?: number;
  l4?: number;
  l5?: number;
  processedData?: string;
  final?: string;
  config?: string;
  userId?: string;
  greeting?: string;
  output?: string;
};

// Helper function to create test steps for composition tests
const createTestStep = <
  Needs extends readonly (keyof CompositionTestBag)[],
  Provides extends readonly (keyof CompositionTestBag)[],
>(
  name: string,
  needs: Needs,
  provides: Provides,
  run:
    | ((bag: Pick<CompositionTestBag, Needs[number]>) => Pick<CompositionTestBag, Provides[number]>)
    | ((
        bag: Pick<CompositionTestBag, Needs[number]>,
      ) => Promise<Pick<CompositionTestBag, Provides[number]>>),
) =>
  step<CompositionTestBag, unknown>()({
    name,
    needs,
    provides,
    run: async (_context, bag): Promise<any> => {
      return Promise.resolve(run(bag));
    },
  });

describe("Workflow Composition", () => {
  describe("Basic Composition Mechanics", () => {
    it("should compose a simple child workflow into parent", async () => {
      // Create a child workflow with 2 steps
      const childStepA = createTestStep("childStepA", [], ["x"], () => ({ x: 10 }));
      const childStepB = createTestStep("childStepB", ["x"], ["y"], (bag) => ({ y: bag.x! * 2 }));
      const childWorkflow = createWorkflow<CompositionTestBag>("child").build([
        childStepA,
        childStepB,
      ]);

      // Create parent workflow that includes child and adds another step
      const parentStep = createTestStep("parentStep", ["y"], ["result"], (bag) => ({
        result: `Final: ${bag.y}`,
      }));
      const parentWorkflow = createWorkflow<CompositionTestBag>("parent").build([
        use(childWorkflow),
        parentStep,
      ]);

      const { bag } = await testComposer.runSyncWorkflow(parentWorkflow);

      expect(bag).toEqual({
        x: 10,
        y: 20,
        result: "Final: 20",
      });
    });

    it("should compose multiple child workflows", async () => {
      // Child 1 provides "a"
      const child1Step = createTestStep("child1Step", [], ["a"], () => ({ a: "hello" }));
      const child1 = createWorkflow<CompositionTestBag>("child1").build([child1Step]);

      // Child 2 provides "b"
      const child2Step = createTestStep("child2Step", [], ["b"], () => ({ b: "world" }));
      const child2 = createWorkflow<CompositionTestBag>("child2").build([child2Step]);

      // Parent combines outputs
      const combineStep = createTestStep("combine", ["a", "b"], ["result"], (bag) => ({
        result: `${bag.a} ${bag.b}`,
      }));
      const parentWorkflow = createWorkflow<CompositionTestBag>("parent").build([
        use(child1),
        use(child2),
        combineStep,
      ]);

      const { bag } = await testComposer.runSyncWorkflow(parentWorkflow);

      expect(bag).toEqual({
        a: "hello",
        b: "world",
        result: "hello world",
      });
    });

    it("should handle nested composition (3 levels)", async () => {
      // Grandchild workflow
      const grandchildStep = createTestStep("grandchildStep", [], ["level3"], () => ({
        level3: "deep",
      }));
      const grandchildWorkflow = createWorkflow<CompositionTestBag>("grandchild").build([
        grandchildStep,
      ]);

      // Child workflow includes grandchild
      const childStep = createTestStep("childStep", ["level3"], ["level2"], (bag) => ({
        level2: `${bag.level3}-middle`,
      }));
      const childWorkflow = createWorkflow<CompositionTestBag>("child").build([
        use(grandchildWorkflow),
        childStep,
      ]);

      // Parent includes child
      const parentStep = createTestStep("parentStep", ["level2"], ["level1"], (bag) => ({
        level1: `${bag.level2}-top`,
      }));
      const parentWorkflow = createWorkflow<CompositionTestBag>("parent").build([
        use(childWorkflow),
        parentStep,
      ]);

      const { bag } = await testComposer.runSyncWorkflow(parentWorkflow);

      expect(bag).toEqual({
        level3: "deep",
        level2: "deep-middle",
        level1: "deep-middle-top",
      });
    });

    it("should namespace child workflow steps", async () => {
      // Child has a step named "process"
      const childProcess = createTestStep("process", [], ["childData"], () => ({
        childData: "from-child",
      }));
      const childWorkflow = createWorkflow<CompositionTestBag>("child").build([childProcess]);

      // Parent also has a step named "process"
      const parentProcess = createTestStep("process", [], ["parentData"], () => ({
        parentData: "from-parent",
      }));

      const parentWorkflow = createWorkflow<CompositionTestBag>("parent").build([
        parentProcess,
        use(childWorkflow),
      ]);

      const { bag } = await testComposer.runSyncWorkflow(parentWorkflow);

      // Both should execute without conflict
      expect(bag).toEqual({
        parentData: "from-parent",
        childData: "from-child",
      });
    });

    it("should compose child workflow outputs with parent steps", async () => {
      const childStep = createTestStep("childStep", [], ["data"], () => ({ data: "test" }));
      const childWorkflow = createWorkflow<CompositionTestBag>("child").build([childStep]);

      const parentStep = createTestStep("parentStep", [], ["other"], () => ({ other: "value" }));
      const parentWorkflow = createWorkflow<CompositionTestBag>("parent").build([
        parentStep,
        use(childWorkflow),
      ]);

      const { bag, error } = await testComposer.runSyncWorkflow(parentWorkflow);

      expect(error).toBeUndefined();
      expect(bag).toEqual({
        data: "test",
        other: "value",
      });
    });
  });

  describe("Dependency Resolution", () => {
    it("should resolve dependencies when child consumes parent step output", async () => {
      // Parent provides data
      const parentProvides = createTestStep("parentProvides", [], ["data"], () => ({ data: 100 }));

      // Child needs data from parent
      const childNeeds = createTestStep("childNeeds", ["data"], ["result"], (bag) => ({
        result: (bag.data as number) * 2,
      }));
      const childWorkflow = createWorkflow<CompositionTestBag>("child")
        .requires("data")
        .build([childNeeds]);

      const parentWorkflow = createWorkflow<CompositionTestBag>("parent").build([
        parentProvides,
        use(childWorkflow),
      ]);

      const { bag } = await testComposer.runSyncWorkflow(parentWorkflow);

      expect(bag).toEqual({
        data: 100,
        result: 200,
      });
    });

    it("should resolve dependencies when parent consumes child workflow output", async () => {
      // Child provides data
      const childProvides = createTestStep("childProvides", [], ["processedData"], () => ({
        processedData: "child-output",
      }));
      const childWorkflow = createWorkflow<CompositionTestBag>("child").build([childProvides]);

      // Parent consumes child's output
      const parentNeeds = createTestStep("parentNeeds", ["processedData"], ["final"], (bag) => ({
        final: `Parent got: ${bag.processedData}`,
      }));

      const parentWorkflow = createWorkflow<CompositionTestBag>("parent").build([
        use(childWorkflow),
        parentNeeds,
      ]);

      const { bag } = await testComposer.runSyncWorkflow(parentWorkflow);

      expect(bag).toEqual({
        processedData: "child-output",
        final: "Parent got: child-output",
      });
    });

    it("should execute composed workflows in parallel when possible", async () => {
      vi.useFakeTimers();
      try {
        const executionOrder: string[] = [];

        // Parent provides common dependency
        const parentProvides = createTestStep("parentProvides", [], ["x"], () => ({ x: 5 }));

        // Child 1 needs x, provides y
        const child1Step = createTestStep("child1Step", ["x"], ["y"], async (bag) => {
          executionOrder.push("child1-start");
          await new Promise((resolve) => setTimeout(resolve, 10));
          executionOrder.push("child1-end");
          return { y: bag.x! + 1 };
        });
        const child1 = createWorkflow<CompositionTestBag>("child1")
          .requires("x")
          .build([child1Step]);

        // Child 2 needs x, provides z (can run in parallel with child1)
        const child2Step = createTestStep("child2Step", ["x"], ["z"], async (bag) => {
          executionOrder.push("child2-start");
          await new Promise((resolve) => setTimeout(resolve, 10));
          executionOrder.push("child2-end");
          return { z: bag.x! + 2 };
        });
        const child2 = createWorkflow<CompositionTestBag>("child2")
          .requires("x")
          .build([child2Step]);

        // Parent step needs both y and z
        const parentCombines = createTestStep("parentCombines", ["y", "z"], ["result"], (bag) => {
          executionOrder.push("final");
          return { result: bag.y! + bag.z! };
        });

        const parentWorkflow = createWorkflow<CompositionTestBag>("parent").build([
          parentProvides,
          use(child1),
          use(child2),
          parentCombines,
        ]);

        const workflowPromise = testComposer.runSyncWorkflow(parentWorkflow);
        await vi.runAllTimersAsync();
        await workflowPromise;

        // Verify parallel execution across workflow boundaries:
        // Expected flow: [child1-start, child2-start, child1-end, child2-end, final]
        // Both steps start before either ends (proves concurrent execution)
        expect(executionOrder).toHaveLength(5);
        expect(executionOrder[0]).toMatch(/^child[12]-start$/);
        expect(executionOrder[1]).toMatch(/^child[12]-start$/);
        expect(executionOrder[2]).toMatch(/^child[12]-end$/);
        expect(executionOrder[3]).toMatch(/^child[12]-end$/);
        expect(executionOrder[4]).toBe("final");

        // Verify both child workflows participated
        expect(executionOrder).toContain("child1-start");
        expect(executionOrder).toContain("child2-start");
      } finally {
        vi.useRealTimers();
      }
    });

    it("should satisfy child initial requirements from parent step", async () => {
      // Child workflow requires "config" to be provided initially
      const childStep = createTestStep("childStep", ["config"], ["output"], (bag) => ({
        output: `Processed with config: ${bag.config}`,
      }));
      const childWorkflow = createWorkflow<CompositionTestBag>("child")
        .requires("config")
        .build([childStep]);

      // Parent provides config via a step
      const parentProvidesConfig = createTestStep("provideConfig", [], ["config"], () => ({
        config: "my-config",
      }));

      const parentWorkflow = createWorkflow<CompositionTestBag>("parent").build([
        parentProvidesConfig,
        use(childWorkflow),
      ]);

      const { bag } = await testComposer.runSyncWorkflow(parentWorkflow);

      expect(bag).toEqual({
        config: "my-config",
        output: "Processed with config: my-config",
      });
    });

    it("should satisfy child initial requirements from parent initial data", async () => {
      // Child requires userId
      const childStep = createTestStep("childStep", ["userId"], ["greeting"], (bag) => ({
        greeting: `Hello, user ${bag.userId}`,
      }));
      const childWorkflow = createWorkflow<CompositionTestBag>("child")
        .requires("userId")
        .build([childStep]);

      // Parent also requires userId and passes it through
      const parentWorkflow = createWorkflow<CompositionTestBag>("parent")
        .requires("userId")
        .build([use(childWorkflow)]);

      const { bag } = await testComposer.runSyncWorkflow(parentWorkflow, { userId: "123" });

      expect(bag).toEqual({
        userId: "123",
        greeting: "Hello, user 123",
      });
    });
  });

  describe("Observability Integration", () => {
    it("should create SubWorkflow spans for composed workflows", async () => {
      const childStep = createTestStep("childStep", [], ["data"], () => ({ data: "test" }));
      const childWorkflow = createWorkflow<CompositionTestBag>("child").build([childStep]);

      const parentWorkflow = createWorkflow<CompositionTestBag>("parent").build([
        use(childWorkflow),
      ]);

      await testComposer.runSyncWorkflow(parentWorkflow);

      // Check that a SubWorkflow span was created
      const startSpanCalls = vi.mocked(mockTracer.startSpan).mock.calls;
      const subworkflowSpan = startSpanCalls.find((call) =>
        String(call[0]).includes("subworkflow.child"),
      );
      expect(subworkflowSpan).toBeDefined();
      if (subworkflowSpan) {
        expect(subworkflowSpan[1].attributes["subworkflow.name"]).toBe("child");
        expect(subworkflowSpan[1].attributes["subworkflow.path"]).toBe("parent.child");
      }
    });

    it("should create separate SubWorkflow spans for multiple composed workflows in same batch", async () => {
      // Two child workflows that can run in parallel
      const child1Step = createTestStep("child1Step", [], ["a"], () => ({ a: 1 }));
      const child1 = createWorkflow<CompositionTestBag>("child1").build([child1Step]);

      const child2Step = createTestStep("child2Step", [], ["b"], () => ({ b: 2 }));
      const child2 = createWorkflow<CompositionTestBag>("child2").build([child2Step]);

      const parentWorkflow = createWorkflow<CompositionTestBag>("parent").build([
        use(child1),
        use(child2),
      ]);

      await testComposer.runSyncWorkflow(parentWorkflow);

      // Should have 2 separate SubWorkflow spans
      const startSpanCalls = vi.mocked(mockTracer.startSpan).mock.calls;
      const child1Span = startSpanCalls.find((call) =>
        String(call[0]).includes("subworkflow.child1"),
      );
      const child2Span = startSpanCalls.find((call) =>
        String(call[0]).includes("subworkflow.child2"),
      );

      expect(child1Span).toBeDefined();
      expect(child2Span).toBeDefined();
      if (child1Span && child2Span) {
        expect(child1Span[1].attributes["subworkflow.name"]).toBe("child1");
        expect(child2Span[1].attributes["subworkflow.name"]).toBe("child2");
      }
    });

    it("should add subworkflow metadata to step span attributes", async () => {
      const childStep = createTestStep("childStep", [], ["data"], () => ({ data: "test" }));
      const childWorkflow = createWorkflow<CompositionTestBag>("child").build([childStep]);

      const parentWorkflow = createWorkflow<CompositionTestBag>("parent").build([
        use(childWorkflow),
      ]);

      await testComposer.runSyncWorkflow(parentWorkflow);

      // Find the step span
      const startSpanCalls = vi.mocked(mockTracer.startSpan).mock.calls;
      const stepSpan = startSpanCalls.find((call) =>
        String(call[0]).includes("step.child.childStep"),
      );

      expect(stepSpan).toBeDefined();
      if (stepSpan) {
        expect(stepSpan[1].attributes["subworkflow.name"]).toBe("child");
        expect(stepSpan[1].attributes["subworkflow.path"]).toBe("parent.child");
      }
    });
  });

  describe("Error Handling", () => {
    it("should include workflow path in WorkflowStepError", async () => {
      const failingStep = createTestStep("failingStep", [], ["data"], () => {
        throw new Error("Step failed!");
      });
      const childWorkflow = createWorkflow<CompositionTestBag>("child").build([failingStep]);

      const parentWorkflow = createWorkflow<CompositionTestBag>("parent").build([
        use(childWorkflow),
      ]);

      const { error } = await testComposer.runSyncWorkflow(parentWorkflow);

      // With Promise.allSettled, errors are wrapped in WorkflowBatchError
      expect(error).toBeInstanceOf(WorkflowBatchError);
      const batchError = error as WorkflowBatchError;
      const stepError = batchError.errors[0];
      expect(stepError!.name).toBe("WorkflowStepError");
      expect(stepError!.workflowPath).toEqual(["parent", "child"]);
      expect(stepError!.message).toContain("subworkflow path: parent.child");
    });

    it("should include full path in nested composition errors", async () => {
      const failingStep = createTestStep("failingStep", [], ["data"], () => {
        throw new Error("Deep error!");
      });
      const grandchildWorkflow = createWorkflow<CompositionTestBag>("grandchild").build([
        failingStep,
      ]);

      const childWorkflow = createWorkflow<CompositionTestBag>("child").build([
        use(grandchildWorkflow),
      ]);

      const parentWorkflow = createWorkflow<CompositionTestBag>("parent").build([
        use(childWorkflow),
      ]);

      const { error } = await testComposer.runSyncWorkflow(parentWorkflow);

      // With Promise.allSettled, errors are wrapped in WorkflowBatchError
      expect(error).toBeInstanceOf(WorkflowBatchError);
      const batchError = error as WorkflowBatchError;
      const stepError = batchError.errors[0];
      expect(stepError!.workflowPath).toEqual(["parent", "child", "grandchild"]);
      expect(stepError!.message).toContain("subworkflow path: parent.child.grandchild");
    });

    it("should include workflow path in error log context", async () => {
      const failingStep = createTestStep("failingStep", [], ["data"], () => {
        throw new Error("Test error");
      });
      const childWorkflow = createWorkflow<CompositionTestBag>("child").build([failingStep]);

      const parentWorkflow = createWorkflow<CompositionTestBag>("parent").build([
        use(childWorkflow),
      ]);

      const { error } = await testComposer.runSyncWorkflow(parentWorkflow);

      // With Promise.allSettled, errors are wrapped in WorkflowBatchError
      expect(error).toBeInstanceOf(WorkflowBatchError);
      const batchError = error as WorkflowBatchError;
      const stepError = batchError.errors[0];
      const logContext = stepError!.toLogContext();
      expect(logContext.workflowPath).toEqual(["parent", "child"]);
      expect(logContext.subworkflowName).toBe("child");
    });
  });

  describe("Compile-Time Validation", () => {
    it("should require parent to satisfy child initial requirements", () => {
      // Child workflow requires "userId"
      const childStep = createTestStep("childStep", ["userId"], ["greeting"], (bag) => ({
        greeting: `Hello, user ${bag.userId}`,
      }));
      const childWorkflow = createWorkflow<CompositionTestBag>("child")
        .requires("userId")
        .build([childStep]);

      // Parent workflow without initial data - should fail compile-time validation
      const parentWorkflowInvalid = createWorkflow<CompositionTestBag>("parent")
        // @ts-expect-error - Parent doesn't provide required initial field "userId" that child needs
        .build([use(childWorkflow)]);
      void parentWorkflowInvalid; // Intentionally unused

      // Parent workflow with correct initial data - should compile
      const parentWorkflowValid = createWorkflow<CompositionTestBag>("parent")
        .requires("userId")
        .build([use(childWorkflow)]);

      // Verify the valid one actually works at runtime
      expect(parentWorkflowValid).toBeDefined();
    });

    it("should require parent initial fields to include all child requirements", () => {
      // Child workflow requires both "userId" and "config"
      const childStep = createTestStep("childStep", ["userId", "config"], ["output"], (bag) => ({
        output: `User ${bag.userId} with config ${bag.config}`,
      }));
      const childWorkflow = createWorkflow<CompositionTestBag>("child")
        .requires("userId", "config")
        .build([childStep]);

      // Parent only provides "userId" - should fail
      const parentWorkflowPartial = createWorkflow<CompositionTestBag>("parent")
        .requires("userId")
        // @ts-expect-error - Parent provides "userId" but child also needs "config"
        .build([use(childWorkflow)]);
      void parentWorkflowPartial; // Intentionally unused

      // Parent provides both - should compile
      const parentWorkflowComplete = createWorkflow<CompositionTestBag>("parent")
        .requires("userId", "config")
        .build([use(childWorkflow)]);

      expect(parentWorkflowComplete).toBeDefined();
    });

    it("should allow parent step to satisfy child initial requirements", () => {
      // Child workflow requires "config"
      const childStep = createTestStep("childStep", ["config"], ["output"], (bag) => ({
        output: `Config: ${bag.config}`,
      }));
      const childWorkflow = createWorkflow<CompositionTestBag>("child")
        .requires("config")
        .build([childStep]);

      // Parent provides "config" via a step - should compile
      const provideConfig = createTestStep("provideConfig", [], ["config"], () => ({
        config: "my-config",
      }));
      const parentWorkflow = createWorkflow<CompositionTestBag>("parent").build([
        provideConfig,
        use(childWorkflow),
      ]);

      expect(parentWorkflow).toBeDefined();
    });

    it("should validate multiple composed workflows in sequence", () => {
      // First child requires "a", provides "b"
      const child1Step = createTestStep("child1Step", ["a"], ["b"], (bag) => ({
        b: `${bag.a}-transformed`,
      }));
      const child1 = createWorkflow<CompositionTestBag>("child1").requires("a").build([child1Step]);

      // Second child requires "b", provides "result"
      const child2Step = createTestStep("child2Step", ["b"], ["result"], (bag) => ({
        result: `Final: ${bag.b}`,
      }));
      const child2 = createWorkflow<CompositionTestBag>("child2").requires("b").build([child2Step]);

      // Parent doesn't provide "a" - should fail
      const parentWorkflowInvalid = createWorkflow<CompositionTestBag>("parent")
        // @ts-expect-error - First child needs "a" which is not provided
        .build([use(child1), use(child2)]);
      void parentWorkflowInvalid; // Intentionally unused

      // Parent provides "a" via initial data - should compile (child1 provides "b" for child2)
      const parentWorkflowValid = createWorkflow<CompositionTestBag>("parent")
        .requires("a")
        .build([use(child1), use(child2)]);

      expect(parentWorkflowValid).toBeDefined();
    });

    it("should validate nested composition requirements", () => {
      // Grandchild requires "x"
      const grandchildStep = createTestStep("grandchildStep", ["x"], ["y"], (bag) => ({
        y: bag.x! * 2,
      }));
      const grandchild = createWorkflow<CompositionTestBag>("grandchild")
        .requires("x")
        .build([grandchildStep]);

      // Child composes grandchild but doesn't provide "x" - should fail at child level
      const childInvalid = createWorkflow<CompositionTestBag>("child")
        // @ts-expect-error - Grandchild needs "x" which child doesn't provide
        .build([use(grandchild)]);
      void childInvalid; // Intentionally unused

      // Child provides "x" via initial data - should compile
      const childValid = createWorkflow<CompositionTestBag>("child")
        .requires("x")
        .build([use(grandchild)]);

      // Parent must provide "x" for the child - should fail
      const parentInvalid = createWorkflow<CompositionTestBag>("parent")
        // @ts-expect-error - Child needs "x" which parent doesn't provide
        .build([use(childValid)]);
      void parentInvalid; // Intentionally unused

      // Parent provides "x" - should compile
      const parentValid = createWorkflow<CompositionTestBag>("parent")
        .requires("x")
        .build([use(childValid)]);

      expect(parentValid).toBeDefined();
    });

    it("should validate parallel composed workflows with different requirements", () => {
      // Child 1 requires "userId"
      const child1Step = createTestStep("child1Step", ["userId"], ["greeting"], (bag) => ({
        greeting: `Hello ${bag.userId}`,
      }));
      const child1 = createWorkflow<CompositionTestBag>("child1")
        .requires("userId")
        .build([child1Step]);

      // Child 2 requires "config"
      const child2Step = createTestStep("child2Step", ["config"], ["output"], (bag) => ({
        output: `Config: ${bag.config}`,
      }));
      const child2 = createWorkflow<CompositionTestBag>("child2")
        .requires("config")
        .build([child2Step]);

      // Parent only provides "userId" - should fail because child2 needs "config"
      const parentWorkflowPartial = createWorkflow<CompositionTestBag>("parent")
        .requires("userId")
        // @ts-expect-error - Child2 needs "config" which is not provided
        .build([use(child1), use(child2)]);
      void parentWorkflowPartial; // Intentionally unused

      // Parent provides both - should compile
      const parentWorkflowComplete = createWorkflow<CompositionTestBag>("parent")
        .requires("userId", "config")
        .build([use(child1), use(child2)]);

      expect(parentWorkflowComplete).toBeDefined();
    });
  });

  describe("Edge Cases", () => {
    it("should handle empty child workflow", async () => {
      const emptyChild = createWorkflow<CompositionTestBag>("emptyChild").build([]);

      const parentStep = createTestStep("parentStep", [], ["data"], () => ({ data: "test" }));
      const parentWorkflow = createWorkflow<CompositionTestBag>("parent").build([
        use(emptyChild),
        parentStep,
      ]);

      const { bag } = await testComposer.runSyncWorkflow(parentWorkflow);

      expect(bag).toEqual({
        data: "test",
      });
    });

    it("should handle deeply nested composition (5 levels)", async () => {
      // Level 5 (deepest)
      const level5Step = createTestStep("level5Step", [], ["l5"], () => ({ l5: 5 }));
      const level5 = createWorkflow<CompositionTestBag>("level5").build([level5Step]);

      // Level 4
      const level4Step = createTestStep("level4Step", ["l5"], ["l4"], (bag) => ({
        l4: bag.l5! + 4,
      }));
      const level4 = createWorkflow<CompositionTestBag>("level4").build([use(level5), level4Step]);

      // Level 3
      const level3Step = createTestStep("level3Step", ["l4"], ["l3"], (bag) => ({
        l3: bag.l4! + 3,
      }));
      const level3 = createWorkflow<CompositionTestBag>("level3").build([use(level4), level3Step]);

      // Level 2
      const level2Step = createTestStep("level2Step", ["l3"], ["l2"], (bag) => ({
        l2: bag.l3! + 2,
      }));
      const level2 = createWorkflow<CompositionTestBag>("level2").build([use(level3), level2Step]);

      // Level 1 (top)
      const level1Step = createTestStep("level1Step", ["l2"], ["l1"], (bag) => ({
        l1: bag.l2! + 1,
      }));
      const level1 = createWorkflow<CompositionTestBag>("level1").build([use(level2), level1Step]);

      const { bag } = await testComposer.runSyncWorkflow(level1);

      expect(bag).toEqual({
        l5: 5,
        l4: 9, // 5 + 4
        l3: 12, // 9 + 3
        l2: 14, // 12 + 2
        l1: 15, // 14 + 1
      });
    });

    it("should detect duplicate field production across workflows", async () => {
      // Child provides "result"
      const childStep = createTestStep("childStep", [], ["result"], () => ({ result: "child" }));
      const childWorkflow = createWorkflow<CompositionTestBag>("child").build([childStep]);

      // Parent also provides "result"
      const parentStep = createTestStep("parentStep", [], ["result"], () => ({ result: "parent" }));
      const parentWorkflow = createWorkflow<CompositionTestBag>("parent").build([
        use(childWorkflow),
        parentStep,
      ]);

      // Should return error about duplicate producer
      const { error } = await testComposer.runSyncWorkflow(parentWorkflow);
      expect(error).toBeDefined();
      expect(error?.message).toMatch(/Duplicate producer/i);
    });

    it("should prevent child from overwriting parent initial data", async () => {
      // Child tries to provide "config" which is parent's initial field
      const childStep = createTestStep("childStep", [], ["config"], () => ({
        config: "child-config",
      }));
      const childWorkflow = createWorkflow<CompositionTestBag>("child").build([childStep]);

      const parentWorkflow = createWorkflow<CompositionTestBag>("parent")
        .requires("config")
        // @ts-expect-error - intentionally testing runtime error handling for child overwriting initial field
        .build([use(childWorkflow)]);

      // Should return error about overwriting initial field
      const { error } = await testComposer.runSyncWorkflow(parentWorkflow, {
        config: "initial-config",
      });
      expect(error).toBeDefined();
      expect(error?.message).toMatch(/cannot overwrite initial field/i);
    });
  });
});
