import { describe, expect, it, vi } from "vitest";

import { createWorkflow, fanOut, step, use, type Workflow } from "../internal";
import { WorkflowBatchError } from "../internal/errors";
import { createTestStep, type TestBag, testComposer } from "./test-utils";

describe("dag-sync", () => {
  describe("step creation", () => {
    it("should create a step with correct metadata", () => {
      const testStep = createTestStep("testStep", ["input"], ["processed"], (bag) => ({
        processed: bag.input.toUpperCase(),
      }));

      expect(testStep.name).toBe("testStep");
      expect(testStep.needs).toEqual(["input"]);
      expect(testStep.provides).toEqual(["processed"]);
    });

    it("should execute step logic correctly", async () => {
      const testStep = createTestStep("upperCase", ["input"], ["processed"], (bag) => ({
        processed: bag.input.toUpperCase(),
      }));

      const result = await testStep.run(undefined, { input: "hello" });
      expect(result).toEqual({ processed: "HELLO" });
    });
  });

  describe("workflow creation and execution", () => {
    // Test steps for workflow tests
    const stepA = createTestStep("stepA", [], ["input"], () => ({ input: "hello" }));
    const stepB = createTestStep("stepB", ["input"], ["processed"], (bag) => ({
      processed: bag.input.toUpperCase(),
    }));
    const stepC = createTestStep("stepC", ["processed"], ["result"], (bag) => ({
      result: `Result: ${bag.processed}`,
    }));

    it("should create and execute a simple sequential workflow", async () => {
      const workflow = createWorkflow<TestBag>("test-workflow").build([stepA, stepB, stepC]);
      const { bag, error } = await testComposer.runSyncWorkflow(workflow);

      expect(error).toBeUndefined();
      expect(bag).toEqual({
        input: "hello",
        processed: "HELLO",
        result: "Result: HELLO",
      });
    });

    it("should handle workflows with initial fields", async () => {
      const workflow = createWorkflow<TestBag>("test-workflow")
        .requires("input")
        .build([stepB, stepC]);

      const { bag, error } = await testComposer.runSyncWorkflow(workflow, { input: "world" });

      expect(error).toBeUndefined();
      expect(bag).toEqual({
        input: "world",
        processed: "WORLD",
        result: "Result: WORLD",
      });
    });

    it("should execute steps in parallel when possible", async () => {
      vi.useFakeTimers();
      try {
        const executionOrder: string[] = [];

        // Steps that can run in parallel (both need "input")
        // Each step includes async work to prove true concurrency
        const parallelStep1 = createTestStep("parallel1", ["input"], ["processed"], async (bag) => {
          executionOrder.push("parallel1-start");
          await new Promise((resolve) => setTimeout(resolve, 10));
          executionOrder.push("parallel1-end");
          return { processed: bag.input.toUpperCase() };
        });

        const parallelStep2 = createTestStep("parallel2", ["input"], ["doubled"], async (bag) => {
          executionOrder.push("parallel2-start");
          await new Promise((resolve) => setTimeout(resolve, 10));
          executionOrder.push("parallel2-end");
          return { doubled: bag.input + bag.input };
        });

        const finalStep = createTestStep("final", ["processed", "doubled"], ["result"], (bag) => {
          executionOrder.push("final");
          return { result: `${bag.processed}-${bag.doubled}` };
        });

        const workflow = createWorkflow<TestBag>("test-workflow")
          .requires("input")
          .build([parallelStep1, parallelStep2, finalStep]);

        const workflowPromise = testComposer.runSyncWorkflow(workflow, { input: "test" });
        await vi.runAllTimersAsync();
        const { bag, error } = await workflowPromise;

        expect(error).toBeUndefined();
        expect(bag).toEqual({
          input: "test",
          processed: "TEST",
          doubled: "testtest",
          result: "TEST-testtest",
        });

        // Verify parallel execution: both steps must start before either ends
        // This is the definitive test - if both starts happen before both ends,
        // the steps MUST be running concurrently (using Promise.all or equivalent)
        // The order of start/end between parallel1 and parallel2 is non-deterministic
        expect(executionOrder).toHaveLength(5);
        expect(executionOrder[0]).toMatch(/^parallel[12]-start$/);
        expect(executionOrder[1]).toMatch(/^parallel[12]-start$/);
        expect(executionOrder[2]).toMatch(/^parallel[12]-end$/);
        expect(executionOrder[3]).toMatch(/^parallel[12]-end$/);
        expect(executionOrder[4]).toBe("final");

        // Verify both parallel steps participated (regardless of order)
        expect(executionOrder).toContain("parallel1-start");
        expect(executionOrder).toContain("parallel2-start");
      } finally {
        vi.useRealTimers();
      }
    });
  });

  describe("error handling", () => {
    const errorStep = createTestStep("errorStep", ["input"], ["error"], () => {
      throw new Error("Test error");
    });

    it("should propagate step execution errors", async () => {
      const workflow = createWorkflow<TestBag>("test-workflow")
        .requires("input")
        .build([errorStep]);

      // With Promise.allSettled, errors are wrapped in WorkflowBatchError
      const { error } = await testComposer.runSyncWorkflow(workflow, { input: "test" });
      expect(error).toBeInstanceOf(WorkflowBatchError);
    });

    it("should handle missing initial fields at runtime", async () => {
      const stepNeedingInput = createTestStep("needsInput", ["input"], ["processed"], (bag) => ({
        processed: bag.input,
      }));
      const workflow = createWorkflow<TestBag>("test-workflow")
        .requires("input")
        .build([stepNeedingInput]);

      // Create a workflow object directly to bypass compile-time validation
      const invalidWorkflow = { steps: workflow.steps, requiredInitial: ["input"] };

      const { error } = await testComposer.runSyncWorkflow(
        invalidWorkflow as unknown as Workflow<TestBag>,
        {},
      );
      expect(error).toBeDefined();
      expect(error?.message).toMatch(/requires field "input"/);
    });

    it("should detect duplicate producers", async () => {
      const step1 = createTestStep("step1", [], ["input"], () => ({ input: "first" }));
      const step2 = createTestStep("step2", [], ["input"], () => ({ input: "second" }));

      // Create workflow object directly to bypass compile-time validation
      const invalidWorkflow = { steps: [step1, step2] };

      const { error } = await testComposer.runSyncWorkflow(
        invalidWorkflow as unknown as Workflow<TestBag>,
        {},
      );
      expect(error).toBeDefined();
      expect(error?.message).toMatch(/Duplicate producer for field "input"/);
    });

    it("should detect missing dependencies", async () => {
      const stepNeedingMissing = createTestStep(
        "needsMissing",
        ["processed"],
        ["result"],
        (bag) => ({ result: bag.processed }),
      );

      // Create workflow object directly to bypass compile-time validation
      const invalidWorkflow = { steps: [stepNeedingMissing] };

      const { error } = await testComposer.runSyncWorkflow(
        invalidWorkflow as unknown as Workflow<TestBag>,
        {},
      );
      expect(error).toBeDefined();
      expect(error?.message).toMatch(/requires field "processed" but no previous step provides it/);
    });

    it("should reject extra keys returned by a step", async () => {
      const badStep = step<TestBag>()({
        name: "badStep",
        needs: ["input"] as const,
        provides: ["processed"] as const,
        run: async () => ({ processed: "OK", sneaky: 1 }) as unknown as Pick<TestBag, "processed">,
      });

      const workflow = createWorkflow<TestBag>("test-workflow").requires("input").build([badStep]);

      const { error } = await testComposer.runSyncWorkflow(workflow, { input: "x" });
      // With Promise.allSettled, errors are wrapped in WorkflowBatchError
      expect(error).toBeInstanceOf(WorkflowBatchError);
      const batchError = error as WorkflowBatchError;
      expect(batchError.errors[0]!.message).toMatch(/unexpected property "sneaky"/i);
    });

    it("should reject when a step omits a declared provide", async () => {
      const badStep = step<TestBag>()({
        name: "badStep",
        needs: ["input"] as const,
        provides: ["processed"] as const,
        run: async () => ({}) as unknown as Pick<TestBag, "processed">,
      });

      const workflow = createWorkflow<TestBag>("test-workflow").requires("input").build([badStep]);

      const { error } = await testComposer.runSyncWorkflow(workflow, { input: "x" });
      expect(error).toBeInstanceOf(WorkflowBatchError);
      const batchError = error as WorkflowBatchError;
      expect(batchError.errors[0]!.message).toMatch(
        /failed to return required property "processed"/i,
      );
    });

    it("should reject when a step returns a Date object (non-serializable)", async () => {
      const badStep = step<TestBag>()({
        name: "badStep",
        needs: ["input"] as const,
        provides: ["processed"] as const,
        run: async () => ({ processed: new Date() }) as unknown as Pick<TestBag, "processed">,
      });

      const workflow = createWorkflow<TestBag>("test-workflow").requires("input").build([badStep]);

      const { error } = await testComposer.runSyncWorkflow(workflow, { input: "x" });
      expect(error).toBeInstanceOf(WorkflowBatchError);
      const batchError = error as WorkflowBatchError;
      expect(batchError.errors[0]!.message).toMatch(/non-serializable value[\s\S]*Type: Date/i);
    });

    it("should reject when a step returns a function (non-serializable)", async () => {
      const badStep = step<TestBag>()({
        name: "badStep",
        needs: ["input"] as const,
        provides: ["processed"] as const,
        run: async () => ({ processed: () => "hello" }) as unknown as Pick<TestBag, "processed">,
      });

      const workflow = createWorkflow<TestBag>("test-workflow").requires("input").build([badStep]);

      const { error } = await testComposer.runSyncWorkflow(workflow, { input: "x" });
      expect(error).toBeInstanceOf(WorkflowBatchError);
      const batchError = error as WorkflowBatchError;
      expect(batchError.errors[0]!.message).toMatch(/non-serializable value[\s\S]*Type: function/i);
    });

    it("should reject when a step returns nested Date objects", async () => {
      const badStep = step<TestBag>()({
        name: "badStep",
        needs: ["input"] as const,
        provides: ["processed"] as const,
        run: async () =>
          ({
            processed: { nested: { date: new Date() } },
          }) as unknown as Pick<TestBag, "processed">,
      });

      const workflow = createWorkflow<TestBag>("test-workflow").requires("input").build([badStep]);

      const { error } = await testComposer.runSyncWorkflow(workflow, { input: "x" });
      expect(error).toBeInstanceOf(WorkflowBatchError);
      const batchError = error as WorkflowBatchError;
      expect(batchError.errors[0]!.message).toMatch(
        /non-serializable value.*root\.processed\.nested\.date/i,
      );
    });

    it("should allow ISO date strings (serializable)", async () => {
      const goodStep = step<TestBag>()({
        name: "goodStep",
        needs: ["input"] as const,
        provides: ["processed"] as const,
        run: async () => ({ processed: new Date().toISOString() }),
      });

      const workflow = createWorkflow<TestBag>("test-workflow").requires("input").build([goodStep]);

      const { bag, error } = await testComposer.runSyncWorkflow(workflow, { input: "x" });
      expect(error).toBeUndefined();
      expect(typeof bag.processed).toBe("string");
      expect(bag.processed).toMatch(/^\d{4}-\d{2}-\d{2}T/);
    });

    it("should reject duplicate step names at build time", () => {
      const step1 = createTestStep("duplicate", [], ["input"], () => ({ input: "first" }));
      const step2 = createTestStep("duplicate", [], ["processed"], () => ({ processed: "second" }));

      expect(() => {
        createWorkflow<TestBag>("test-workflow").build([step1, step2]);
      }).toThrow(/Duplicate step name "duplicate" found at positions 0 and 1/);
    });

    it("should reject duplicate step names at runtime", async () => {
      const step1 = createTestStep("duplicate", [], ["input"], () => ({ input: "first" }));
      const step2 = createTestStep("duplicate", [], ["processed"], () => ({ processed: "second" }));

      // Create workflow object directly to bypass build-time validation
      const invalidWorkflow = { steps: [step1, step2] };

      const { error } = await testComposer.runSyncWorkflow(
        invalidWorkflow as unknown as Workflow<TestBag>,
        {},
      );
      expect(error).toBeDefined();
      expect(error?.message).toMatch(/Duplicate step name "duplicate"/);
    });

    it("should reject duplicate entries in needs array at build time", () => {
      const badStep = createTestStep("badStep", ["input", "input"], ["processed"], (bag) => ({
        processed: bag.input,
      }));

      expect(() => {
        createWorkflow<TestBag>("test-workflow").requires("input").build([badStep]);
      }).toThrow(/lists "input" twice in "needs"/);
    });

    it("should reject duplicate entries in provides array at build time", () => {
      const badStep = createTestStep("badStep", [], ["processed", "processed"], () => ({
        processed: "value",
      }));

      expect(() => {
        createWorkflow<TestBag>("test-workflow").build([badStep]);
      }).toThrow(/lists "processed" twice in "provides"/);
    });

    it("should reject duplicate entries in needs array at runtime", async () => {
      const badStep = createTestStep("badStep", ["input", "input"], ["processed"], (bag) => ({
        processed: bag.input,
      }));

      // Create workflow object directly to bypass build-time validation
      const invalidWorkflow = { steps: [badStep] };

      const { error } = await testComposer.runSyncWorkflow(
        invalidWorkflow as unknown as Workflow<TestBag>,
        { input: "test" },
      );
      expect(error).toBeDefined();
      expect(error?.message).toMatch(/lists "input" twice in "needs"/);
    });

    it("should reject duplicate entries in provides array at runtime", async () => {
      const badStep = createTestStep("badStep", [], ["processed", "processed"], () => ({
        processed: "value",
      }));

      // Create workflow object directly to bypass build-time validation
      const invalidWorkflow = { steps: [badStep] };

      const { error } = await testComposer.runSyncWorkflow(
        invalidWorkflow as unknown as Workflow<TestBag>,
        {},
      );
      expect(error).toBeDefined();
      expect(error?.message).toMatch(/lists "processed" twice in "provides"/);
    });
  });

  describe("immutability protection", () => {
    it("should prevent mutation of step outputs in non-production", async () => {
      vi.stubEnv("ENVIRONMENT_NAME", "local");

      const step1 = createTestStep("step1", [], ["processed"], () => ({
        processed: "initial",
      }));

      const step2 = createTestStep("step2", ["processed"], ["result"], (bag) => {
        // Verify the input is frozen
        expect(Object.isFrozen(bag)).toBe(true);
        expect(Object.isFrozen(bag.processed)).toBe(true);

        // Attempt mutation - should throw when frozen
        expect(() => {
          bag.processed = "mutated";
        }).toThrow(/read only property/);

        return { result: "done" };
      });

      const workflow = createWorkflow<TestBag>("test-workflow").build([step1, step2]);

      // This should complete without error even though step2 tried to mutate
      await testComposer.runSyncWorkflow(workflow, {});
    });

    it("should prevent mutation of nested objects and arrays", async () => {
      vi.stubEnv("ENVIRONMENT_NAME", "local");

      // Create a step that produces a complex nested structure
      type ComplexBag = {
        data: {
          items: Array<{ id: number; name: string }>;
          config: { timeout: number };
        };
        result?: string;
      };

      const step1 = step<ComplexBag>()({
        name: "producer",
        needs: [] as const,
        provides: ["data"] as const,
        run: async () => ({
          data: {
            items: [{ id: 1, name: "Alice" }],
            config: { timeout: 1000 },
          },
        }),
      });

      const step2 = step<ComplexBag, unknown>()({
        name: "consumer",
        needs: ["data"] as const,
        provides: ["result"] as const,
        run: async (_ctx, bag) => {
          // Verify the nested structure is deeply frozen
          expect(Object.isFrozen(bag.data)).toBe(true);
          expect(Object.isFrozen(bag.data.items)).toBe(true);
          expect(Object.isFrozen(bag.data.items[0])).toBe(true);
          expect(Object.isFrozen(bag.data.config)).toBe(true);

          // Attempt various mutations - all should throw when frozen
          expect(() => {
            bag.data.items.push({ id: 2, name: "Bob" });
          }).toThrow(/not extensible/);

          expect(() => {
            bag.data.items[0]!.name = "Modified";
          }).toThrow(/read only property/);

          expect(() => {
            bag.data.config.timeout = 2000;
          }).toThrow(/read only property/);

          return { result: "done" };
        },
      });

      const workflow = createWorkflow<ComplexBag>("test-workflow").build([step1, step2]);

      await testComposer.runSyncWorkflow(workflow, {});
    });
  });

  describe("performance characteristics", () => {
    it("should execute workflows with many parallel steps", async () => {
      // Create a workflow with 10 parallel steps to verify the engine
      // handles batches with many steps without errors or degradation
      const parallelSteps = Array.from({ length: 10 }, (_, i) =>
        createTestStep(
          `parallel${i}`,
          ["input"],
          [`result${i}` as keyof TestBag],
          (bag) =>
            ({
              [`result${i}`]: `${bag.input}-${i}`,
            }) as unknown as Pick<TestBag, keyof TestBag>,
        ),
      );

      const workflow = createWorkflow<TestBag>("test-workflow")
        .requires("input")
        .build(parallelSteps);

      const { bag, error } = await testComposer.runSyncWorkflow(workflow, { input: "test" });

      expect(error).toBeUndefined();
      // Verify initial input is preserved
      expect(bag.input).toBe("test");

      // Verify all 10 parallel steps executed and produced correct outputs
      for (let i = 0; i < 10; i++) {
        expect((bag as unknown as Record<string, string>)[`result${i}`]).toBe(`test-${i}`);
      }
    });

    it("should execute independent steps in parallel", async () => {
      const independentSteps = [
        createTestStep("step1", [], ["input"], () => ({ input: "value1" })),
        createTestStep("step2", [], ["processed"], () => ({ processed: "value2" })),
        createTestStep("step3", [], ["result"], () => ({ result: "value3" })),
      ];

      const workflow = createWorkflow<TestBag>("test-workflow").build(independentSteps);
      const { bag, error } = await testComposer.runSyncWorkflow(workflow, {});

      expect(error).toBeUndefined();
      expect(bag).toEqual({
        input: "value1",
        processed: "value2",
        result: "value3",
      });
    });
  });

  describe("cycle detection", () => {
    it("should catch cycles as dependency validation errors", async () => {
      // Create steps that form a cycle: stepX needs result, stepY needs input
      const stepX = createTestStep("stepX", ["result"], ["input"], (bag) => ({
        input: bag.result,
      }));
      const stepY = createTestStep("stepY", ["input"], ["result"], (bag) => ({
        result: bag.input,
      }));

      // This workflow has a cycle - now properly detected as such
      const cyclicWorkflow = { steps: [stepX, stepY] };

      const { error } = await testComposer.runSyncWorkflow(
        cyclicWorkflow as unknown as Workflow<TestBag>,
        {},
      );
      expect(error).toBeDefined();
      expect(error?.message).toMatch(
        /Unable to resolve dependencies for steps:.*circular dependency/,
      );
    });

    it("should catch cycles regardless of step order", async () => {
      // Same cycle, different order - still caught as circular dependency
      const stepY = createTestStep("stepY", ["input"], ["result"], (bag) => ({
        result: bag.input,
      }));
      const stepX = createTestStep("stepX", ["result"], ["input"], (bag) => ({
        input: bag.result,
      }));

      const cyclicWorkflow = { steps: [stepY, stepX] };

      const { error } = await testComposer.runSyncWorkflow(
        cyclicWorkflow as unknown as Workflow<TestBag>,
        {},
      );
      expect(error).toBeDefined();
      expect(error?.message).toMatch(
        /Unable to resolve dependencies for steps:.*circular dependency/,
      );
    });
  });

  describe("step input isolation", () => {
    it("should provide only needed fields to step.run()", async () => {
      let receivedInput: Record<string, unknown> = {};

      const stepA = createTestStep("stepA", [], ["input", "processed"], () => ({
        input: "test",
        processed: "extra",
      }));

      const stepB = createTestStep("stepB", ["input"], ["result"], (bag) => {
        receivedInput = bag;
        return { result: "done" };
      });

      const workflow = { steps: [stepA, stepB] };
      await testComposer.runSyncWorkflow(workflow as unknown as Workflow<TestBag>, {});

      // stepB should only receive "input", not "processed"
      expect(receivedInput).toEqual({ input: "test" });
      expect(receivedInput).not.toHaveProperty("processed");
    });

    it("should provide multiple needed fields correctly", async () => {
      let receivedInput: Record<string, unknown> = {};

      const stepA = createTestStep("stepA", [], ["input", "processed", "count"], () => ({
        input: "test",
        processed: "TEST",
        count: 4,
      }));

      const stepB = createTestStep("stepB", ["input", "count"], ["result"], (bag) => {
        receivedInput = bag;
        return { result: `${bag.input}-${bag.count}` };
      });

      const workflow = { steps: [stepA, stepB] };
      await testComposer.runSyncWorkflow(workflow as unknown as Workflow<TestBag>, {});

      // stepB should receive "input" and "count", but not "processed"
      expect(receivedInput).toEqual({ input: "test", count: 4 });
      expect(receivedInput).not.toHaveProperty("processed");
    });
  });

  describe("initial field handling", () => {
    it("should use initial fields for step dependencies", async () => {
      // stepB depends on "input" which is provided in initial
      const stepB = createTestStep("stepB", ["input"], ["result"], (bag) => ({
        result: `got: ${bag.input}`,
      }));

      const workflow = { steps: [stepB] };
      const { bag, error } = await testComposer.runSyncWorkflow(
        workflow as unknown as Workflow<TestBag>,
        { input: "from-initial" },
      );

      expect(error).toBeUndefined();
      // stepB should receive the initial value
      expect((bag as Record<string, unknown>).result).toBe("got: from-initial");
      expect(bag.input).toBe("from-initial");
    });

    it("should prevent steps from overwriting initial fields", async () => {
      // stepA tries to provide "input" which also exists in initial - should be rejected
      const stepA = createTestStep("stepA", [], ["input"], () => ({ input: "from-step" }));

      const workflow = { steps: [stepA] };

      // Should throw error when step tries to overwrite initial field
      const { error } = await testComposer.runSyncWorkflow(
        workflow as unknown as Workflow<TestBag>,
        { input: "from-initial" },
      );
      expect(error).toBeDefined();
      expect(error?.message).toMatch(
        /Step "stepA" cannot overwrite initial field "input". Initial data is protected from modification./,
      );
    });

    it("should handle mixed initial and step dependencies", async () => {
      // stepB needs both "input" (from initial) and "processed" (from stepA)
      const stepA = createTestStep("stepA", [], ["processed"], () => ({
        processed: "step-processed",
      }));
      const stepB = createTestStep("stepB", ["input", "processed"], ["result"], (bag) => ({
        result: `${bag.input}+${bag.processed}`,
      }));

      const workflow = { steps: [stepA, stepB] };
      const { bag, error } = await testComposer.runSyncWorkflow(
        workflow as unknown as Workflow<TestBag>,
        { input: "initial-input" },
      );

      expect(error).toBeUndefined();
      // stepB gets "input" from initial and "processed" from stepA
      expect((bag as Record<string, unknown>).result).toBe("initial-input+step-processed");
    });
  });

  describe("configuration handling", () => {
    it("should use configured fields for step dependencies", async () => {
      // Depends on "input" which is provided in initial
      const step = createTestStep("step", ["input"], ["result"], (bag) => ({
        result: `got: ${bag.input}`,
      }));

      const workflow = createWorkflow<TestBag>("test-workflow")
        .configure({ input: "from-initial" })
        .build([step]);
      const { bag, error } = await testComposer.runSyncWorkflow(workflow);

      expect(error).toBeUndefined();
      // step should receive the initial value
      expect((bag as Record<string, unknown>).result).toBe("got: from-initial");
      expect(bag.input).toBe("from-initial");
    });

    it("should handle mixed configuration and step dependencies", async () => {
      // stepB needs both "input" (from configuration) and "processed" (from stepA)
      const stepA = createTestStep("stepA", [], ["processed"], () => ({
        processed: "step-processed",
      }));
      const stepB = createTestStep("stepB", ["input", "processed"], ["result"], (bag) => ({
        result: `${bag.input}+${bag.processed}`,
      }));

      const workflow = createWorkflow<TestBag>("test-workflow")
        .configure({ input: "configured-input" })
        .build([stepA, stepB]);
      const { bag } = await testComposer.runSyncWorkflow(workflow as unknown as Workflow<TestBag>);

      // stepB gets "input" from configuration and "processed" from stepA
      expect((bag as Record<string, unknown>).result).toBe("configured-input+step-processed");
    });

    it("should handle mixed configuration and initial fields", async () => {
      // step needs both "configured" (from configuration) and "input" (from initial)
      const step = createTestStep("step", ["configured", "input"], ["result"], (bag) => ({
        result: `got: configured=${bag.configured}, input=${bag.input}`,
      }));

      const workflow = createWorkflow<TestBag>("test-workflow")
        .configure({ configured: "configured-input" })
        .requires("input")
        .build([step]);
      const { bag } = await testComposer.runSyncWorkflow(workflow, { input: "initial-input" });

      // step gets "input" from configuration and "processed" from stepA
      expect((bag as Record<string, unknown>).result).toBe(
        "got: configured=configured-input, input=initial-input",
      );
    });

    it("should handle mixed initial fields and configuration when requires comes first", async () => {
      // step needs both "configured" (from configuration) and "input" (from initial)
      const step = createTestStep("step", ["configured", "input"], ["result"], (bag) => ({
        result: `got: configured=${bag.configured}, input=${bag.input}`,
      }));

      const workflow = createWorkflow<TestBag>("test-workflow")
        .requires("input")
        .configure({ configured: "configured-input" })
        .build([step]);
      const { bag } = await testComposer.runSyncWorkflow(workflow, { input: "initial-input" });

      expect((bag as Record<string, unknown>).result).toBe(
        "got: configured=configured-input, input=initial-input",
      );
    });

    it("should prioritize configured values over initial data", async () => {
      const step = createTestStep("step", ["input"], ["result"], (bag) => ({
        result: `got: ${bag.input}`,
      }));

      const workflow = createWorkflow<TestBag>("test-workflow")
        .configure({ input: "configured-input" })
        .build([step]);
      const { bag } = await testComposer.runSyncWorkflow(workflow as unknown as Workflow<TestBag>, {
        input: "initial-input",
      });

      // step gets "input" from configuration
      expect((bag as Record<string, unknown>).result).toBe("got: configured-input");
    });

    describe("with sub-workflows", () => {
      // TODO: add test to allow sub-workflows to configure their own values when this is supported.

      it("should allow sub-workflows to consume values configured top level", async () => {
        const step = createTestStep("step", ["input"], ["result"], (bag) => ({
          result: `got: ${bag.input}`,
        }));

        const subWorkflow = createWorkflow<TestBag>("test-sub-workflow")
          .requires("input")
          .build([step]);
        const workflow = createWorkflow<TestBag>("test-workflow")
          .configure({ input: "configured-input" })
          .build([use(subWorkflow)]);
        const { bag } = await testComposer.runSyncWorkflow(
          workflow as unknown as Workflow<TestBag>,
        );

        // step gets "input" from top level configuration
        expect((bag as Record<string, unknown>).result).toBe("got: configured-input");
      });
    });
  });

  describe("error propagation in parallel batches", () => {
    it("should handle errors in parallel batch execution", async () => {
      const goodStep = createTestStep("good", [], ["input"], () => ({ input: "ok" }));
      const badStep = createTestStep("bad", [], ["processed"], () => {
        throw new Error("Step failed");
      });

      // Both steps can run in parallel (no dependencies)
      const workflow = { steps: [goodStep, badStep] };

      // With Promise.allSettled, errors are wrapped in WorkflowBatchError
      const { error } = await testComposer.runSyncWorkflow(
        workflow as unknown as Workflow<TestBag>,
        {},
      );
      expect(error).toBeInstanceOf(WorkflowBatchError);
    });

    it("should not execute dependent steps when parallel step fails", async () => {
      let dependentStepExecuted = false;

      const goodStep = createTestStep("good", [], ["input"], () => ({ input: "ok" }));
      const badStep = createTestStep("bad", [], ["processed"], () => {
        throw new Error("Parallel step failed");
      });
      const dependentStep = createTestStep("dependent", ["input"], ["result"], (bag) => {
        dependentStepExecuted = true;
        return { result: bag.input };
      });

      // goodStep and badStep run in parallel, dependentStep waits for goodStep
      const workflow = { steps: [goodStep, badStep, dependentStep] };

      // With Promise.allSettled, errors are wrapped in WorkflowBatchError
      const { error } = await testComposer.runSyncWorkflow(
        workflow as unknown as Workflow<TestBag>,
        {},
      );
      expect(error).toBeInstanceOf(WorkflowBatchError);

      // dependentStep should not have executed due to the error in the parallel batch
      expect(dependentStepExecuted).toBe(false);
    });

    it("should collect all errors when multiple steps fail in parallel", async () => {
      const badStep1 = createTestStep("bad1", [], ["input"], () => {
        throw new Error("First error");
      });
      const badStep2 = createTestStep("bad2", [], ["processed"], () => {
        throw new Error("Second error");
      });

      const workflow = { steps: [badStep1, badStep2] };

      // With Promise.allSettled, all errors are collected in WorkflowBatchError
      const { error } = await testComposer.runSyncWorkflow(
        workflow as unknown as Workflow<TestBag>,
        {},
      );
      expect(error).toBeInstanceOf(WorkflowBatchError);
      const batchError = error as WorkflowBatchError;
      // Both errors should be collected
      expect(batchError.errors).toHaveLength(2);
      expect(batchError.errors[0]!.message).toContain("error");
      expect(batchError.errors[1]!.message).toContain("error");
    });
  });

  describe("step-to-step overwriting behavior", () => {
    it("should prevent steps from providing the same field even with dependencies", async () => {
      // stepA provides "input", both stepB and stepC provide "result"
      const stepA = createTestStep("stepA", [], ["input"], () => ({ input: "base" }));
      const stepB = createTestStep("stepB", ["input"], ["result"], (bag) => ({
        result: `B-${bag.input}`,
      }));
      const stepC = createTestStep("stepC", ["input"], ["result"], (bag) => ({
        result: `C-${bag.input}`,
      }));

      const workflow = { steps: [stepA, stepB, stepC] };

      const { error } = await testComposer.runSyncWorkflow(
        workflow as unknown as Workflow<TestBag>,
        {},
      );
      expect(error).toBeDefined();
      expect(error?.message).toMatch(/Duplicate producer for field "result": stepB and stepC/);
    });

    it("should allow steps to provide different fields", async () => {
      // Each step provides a unique field - should work fine
      const stepA = createTestStep("stepA", [], ["input"], () => ({ input: "base" }));
      const stepB = createTestStep("stepB", ["input"], ["processed"], (bag) => ({
        processed: `processed-${bag.input}`,
      }));
      const stepC = createTestStep("stepC", ["processed"], ["result"], (bag) => ({
        result: `final-${bag.processed}`,
      }));

      const workflow = { steps: [stepA, stepB, stepC] };
      const { bag, error } = await testComposer.runSyncWorkflow(
        workflow as unknown as Workflow<TestBag>,
        {},
      );

      expect(error).toBeUndefined();
      expect(bag).toEqual({
        input: "base",
        processed: "processed-base",
        result: "final-processed-base",
      });
    });

    it("should preserve step outputs for dependent steps", async () => {
      // Step outputs are protected from being overwritten by other steps via duplicate producer checks
      const stepA = createTestStep("stepA", [], ["processed"], () => ({
        processed: "original-value",
      }));

      // This would fail if we tried to add it:
      // const stepB = createTestStep("stepB", [], ["processed"], () => ({ processed: "overwritten-value" }));

      const stepC = createTestStep("stepC", ["processed"], ["result"], (bag) => ({
        result: `used: ${bag.processed}`,
      }));

      const workflow = { steps: [stepA, stepC] };
      const { bag, error } = await testComposer.runSyncWorkflow(
        workflow as unknown as Workflow<TestBag>,
        {},
      );

      expect(error).toBeUndefined();
      // stepA's output is preserved and used by stepC
      expect((bag as Record<string, unknown>).result).toBe("used: original-value");
      expect((bag as Record<string, unknown>).processed).toBe("original-value");
    });
  });

  describe("edge cases", () => {
    it("should handle workflows with only initial data", async () => {
      const workflow = createWorkflow<TestBag>("test-workflow")
        .requires("input", "processed")
        .build([]);

      const { bag, error } = await testComposer.runSyncWorkflow(workflow, {
        input: "test",
        processed: "processed",
      });

      expect(error).toBeUndefined();
      expect(bag).toEqual({
        input: "test",
        processed: "processed",
      });
    });

    it("should handle steps that return promises", async () => {
      vi.useFakeTimers();
      try {
        const asyncStep = step<TestBag, unknown>()({
          name: "asyncStep",
          needs: ["input"],
          provides: ["processed"],
          run: async (_context, bag) => {
            await new Promise((resolve) => setTimeout(resolve, 10));
            return { processed: bag.input.toUpperCase() };
          },
        });

        const workflow = createWorkflow<TestBag>("test-workflow")
          .requires("input")
          .build([asyncStep]);

        const workflowPromise = testComposer.runSyncWorkflow(workflow, { input: "async" });
        await vi.runAllTimersAsync();
        const { bag, error } = await workflowPromise;
        expect(error).toBeUndefined();
        expect(bag.processed).toBe("ASYNC");
      } finally {
        vi.useRealTimers();
      }
    });
  });

  describe("type safety", () => {
    it("should provide correct TypeScript types for step inputs and outputs", () => {
      // This test verifies that TypeScript compilation enforces correct types
      // The primary test is that this code compiles without type errors

      const typedStep = step<TestBag, unknown>()({
        name: "typedStep",
        needs: ["input"],
        provides: ["processed"],
        run: (_context, bag) => {
          // TypeScript enforces that bag has 'input' property
          expect(typeof bag.input).toBe("string");
          // TypeScript enforces that we return an object with 'processed' property
          return { processed: bag.input.toUpperCase() };
        },
      });

      expect(typedStep.needs).toEqual(["input"]);
      expect(typedStep.provides).toEqual(["processed"]);
    });
  });

  describe("boundary conditions and edge cases", () => {
    it("should handle single-step workflows", async () => {
      const singleStep = createTestStep("onlyStep", ["input"], ["processed"], (bag) => ({
        processed: bag.input.toUpperCase(),
      }));

      const workflow = createWorkflow<TestBag>("single-step-workflow")
        .requires("input")
        .build([singleStep]);

      const { bag, error } = await testComposer.runSyncWorkflow(workflow, { input: "test" });

      expect(error).toBeUndefined();
      expect(bag).toEqual({
        input: "test",
        processed: "TEST",
      });
    });

    it("should handle workflows with no initial data", async () => {
      const independentStep = createTestStep("independent", [], ["result"], () => ({
        result: "generated",
      }));

      const workflow = createWorkflow<TestBag>("no-initial-workflow").build([independentStep]);
      const { bag, error } = await testComposer.runSyncWorkflow(workflow, {});

      expect(error).toBeUndefined();
      expect(bag).toEqual({
        result: "generated",
      });
    });

    it("should handle workflows with large initial data", async () => {
      type LargeBag = {
        input: string;
        processed: string;
        largeArray: string[];
        largeObject: Record<string, string>;
      };

      const largeData = {
        input: "test",
        largeArray: Array(100).fill("data"), // Smaller for faster tests
        largeObject: Object.fromEntries(
          Array(10)
            .fill(0)
            .map((_, i) => [`key${i}`, `value${i}`]),
        ),
      };

      const step1 = step<LargeBag>()({
        name: "step1",
        needs: ["input"] as const,
        provides: ["processed"] as const,
        run: (_context, bag) => ({
          processed: bag.input.toUpperCase(),
        }),
      });

      const workflow = createWorkflow<LargeBag>("large-data-workflow")
        .requires("input")
        .build([step1]);

      const { bag, error } = await testComposer.runSyncWorkflow(workflow, largeData);

      expect(error).toBeUndefined();
      expect(bag.processed).toBe("TEST");
      expect((bag as LargeBag).largeArray).toHaveLength(100);
      expect(Object.keys((bag as LargeBag).largeObject)).toHaveLength(10);
    });

    it("should isolate multiple simultaneous workflow instances", async () => {
      vi.useFakeTimers();
      try {
        const step1 = createTestStep("step1", ["input"], ["processed"], async (bag) => {
          // Add small delay to simulate real work
          await new Promise((resolve) => setTimeout(resolve, 5));
          return { processed: bag.input.toUpperCase() };
        });

        const workflow = createWorkflow<TestBag>("concurrent-workflow")
          .requires("input")
          .build([step1]);

        // Run multiple workflow instances simultaneously
        const promises = [
          testComposer.runSyncWorkflow(workflow, { input: "test1" }),
          testComposer.runSyncWorkflow(workflow, { input: "test2" }),
          testComposer.runSyncWorkflow(workflow, { input: "test3" }),
        ];

        await vi.runAllTimersAsync();
        const results = await Promise.all(promises);

        // Each workflow instance should complete successfully with correct results
        expect(results[0]!.bag).toEqual({ input: "test1", processed: "TEST1" });
        expect(results[1]!.bag).toEqual({ input: "test2", processed: "TEST2" });
        expect(results[2]!.bag).toEqual({ input: "test3", processed: "TEST3" });
      } finally {
        vi.useRealTimers();
      }
    });

    it("should handle workflows with many sequential steps", async () => {
      type SequentialBag = { input: string } & Record<`step${number}Result`, string>;

      // Create a chain of steps for memory pressure testing
      const steps = Array.from({ length: 5 }, (_, i) => {
        const stepName = `step${i}`;
        const needsKey = i === 0 ? "input" : `step${i - 1}Result`;
        const providesKey = `step${i}Result`;

        const needs = [needsKey] as Array<keyof SequentialBag>;
        const provides = [providesKey] as Array<keyof SequentialBag>;
        const dependencyKey = needsKey as keyof SequentialBag;
        const outputKey = providesKey as keyof SequentialBag;

        return step<SequentialBag>()({
          name: stepName,
          needs,
          provides,
          run: (_context, bag) =>
            ({ [outputKey]: `${stepName}-${bag[dependencyKey]}` }) as Pick<
              SequentialBag,
              typeof outputKey
            >,
        });
      });

      const workflow = createWorkflow<SequentialBag>("sequential-workflow")
        .requires("input")
        .build(steps);

      const { bag } = await testComposer.runSyncWorkflow(workflow, { input: "start" });

      // Should complete successfully
      expect((bag as SequentialBag).step4Result).toContain("step4");
    });
  });

  describe("checkpoint validation", () => {
    const stepA = createTestStep("stepA", [], ["input"], () => ({ input: "hello" }));
    const stepB = createTestStep("stepB", ["input"], ["processed"], (bag) => ({
      processed: bag.input.toUpperCase(),
    }));
    const stepC = createTestStep("stepC", ["processed"], ["result"], (bag) => ({
      result: `Result: ${bag.processed}`,
    }));

    it("should throw for duplicate checkpoint names", () => {
      expect(() => {
        createWorkflow<TestBag>("duplicate-checkpoint-test")
          .build([stepA, stepB, stepC])
          .checkpoint("same", { afterStep: stepA })
          // @ts-expect-error - Duplicate checkpoint name 'same'
          .checkpoint("same", { afterStep: stepB });
      }).toThrow(/Duplicate checkpoint name "same"/);
    });

    it("should store checkpoint with step name resolved from step reference", () => {
      const workflow = createWorkflow<TestBag>("step-resolution-test")
        .build([stepA, stepB])
        .checkpoint("cp", { afterStep: stepB });

      expect(workflow.checkpoints?.[0]?.afterStep).toBe("stepB");
    });

    it("should store checkpoint with optional timeout", () => {
      const workflow = createWorkflow<TestBag>("timeout-test")
        .build([stepA, stepB])
        .checkpoint("withTimeout", { afterStep: stepA, timeout: 60000 })
        .checkpoint("withoutTimeout", { afterStep: stepB });

      expect(workflow.checkpoints).toHaveLength(2);
      expect(workflow.checkpoints?.[0]?.timeout).toBe(60000);
      expect(workflow.checkpoints?.[1]?.timeout).toBeUndefined();
    });

    it("should allow chaining checkpoint with onError", () => {
      const workflow = createWorkflow<TestBag>("chain-test")
        .build([stepA, stepB])
        .checkpoint("cp", { afterStep: stepA })
        .onError((_ctx, _bag, error) => error);

      expect(workflow.checkpoints).toHaveLength(1);
      expect(workflow.errorHandler).toBeDefined();
    });

    it("should ignore checkpoints during sync execution", async () => {
      const workflow = createWorkflow<TestBag>("sync-ignore-test")
        .build([stepA, stepB, stepC])
        .checkpoint("early", { afterStep: stepA });

      // Sync execution should complete fully, ignoring checkpoint
      const { bag, error } = await testComposer.runSyncWorkflow(workflow);

      expect(error).toBeUndefined();
      expect(bag.input).toBe("hello");
      expect(bag.processed).toBe("HELLO");
      expect(bag.result).toBe("Result: HELLO");
    });
  });

  describe("FanOut execution", () => {
    interface ItemBag {
      item: string;
      processed: string;
    }

    const processItem = step<ItemBag, undefined>()({
      name: "processItem",
      needs: ["item"] as const,
      provides: ["processed"] as const,
      run: async (_ctx, bag) => ({ processed: bag.item.toUpperCase() }),
    });

    const childWorkflow = createWorkflow<ItemBag>("child-wf").requires("item").build([processItem]);

    interface FanOutBag {
      items: string[];
      results: string[];
    }

    it("should execute FanOut with successful child workflows", async () => {
      const fo = fanOut<FanOutBag>()({
        name: "processAll",
        needs: ["items"] as const,
        childWorkflow,
        mapInput: (bag) => bag.items.map((item) => ({ item })),
        provides: ["results"] as const,
        aggregateResults: (childResults) => ({
          results: childResults.map((r) => r.processed),
        }),
      });

      const workflow = createWorkflow<FanOutBag>("fanout-wf").requires("items").build([fo]);

      const { bag, error } = await testComposer.runSyncWorkflow(workflow, {
        items: ["hello", "world"],
      });

      expect(error).toBeUndefined();
      expect(bag.results).toEqual(["HELLO", "WORLD"]);
    });

    it("should call aggregateResults with empty array when mapInput returns []", async () => {
      const fo = fanOut<FanOutBag>()({
        name: "processAll",
        needs: ["items"] as const,
        childWorkflow,
        mapInput: () => [],
        provides: ["results"] as const,
        aggregateResults: (childResults) => ({
          results: childResults.map((r) => r.processed),
        }),
      });

      const workflow = createWorkflow<FanOutBag>("empty-fanout").requires("items").build([fo]);

      const { bag, error } = await testComposer.runSyncWorkflow(workflow, { items: [] });

      expect(error).toBeUndefined();
      expect(bag.results).toEqual([]);
    });

    it("should report errors via WorkflowBatchError when child workflows fail", async () => {
      interface FailBag {
        input: string;
        output: string;
      }

      const failingStep = step<FailBag, undefined>()({
        name: "failStep",
        needs: ["input"] as const,
        provides: ["output"] as const,
        run: async () => {
          throw new Error("child failed");
        },
      });

      const failingChild = createWorkflow<FailBag>("failing-child")
        .requires("input")
        .build([failingStep]);

      const fo = fanOut<FanOutBag>()({
        name: "fanOutWithFailures",
        needs: ["items"] as const,
        childWorkflow: failingChild,
        mapInput: (bag) => bag.items.map((item) => ({ input: item })),
        provides: ["results"] as const,
        aggregateResults: () => ({ results: [] }),
      });

      const workflow = createWorkflow<FanOutBag>("fail-fanout").requires("items").build([fo]);

      const { error } = await testComposer.runSyncWorkflow(workflow, { items: ["a", "b"] });

      expect(error).toBeDefined();
      expect(error).toBeInstanceOf(WorkflowBatchError);
    });

    it("should respect concurrency limits", async () => {
      let maxConcurrent = 0;
      let currentConcurrent = 0;

      interface ConcBag {
        id: number;
        done: boolean;
      }

      const trackConcurrency = step<ConcBag, undefined>()({
        name: "track",
        needs: ["id"] as const,
        provides: ["done"] as const,
        run: async () => {
          currentConcurrent++;
          maxConcurrent = Math.max(maxConcurrent, currentConcurrent);
          // Yield to let other lanes start
          await new Promise((resolve) => setTimeout(resolve, 10));
          currentConcurrent--;
          return { done: true };
        },
      });

      const concChild = createWorkflow<ConcBag>("conc-child")
        .requires("id")
        .build([trackConcurrency]);

      interface ConcParentBag {
        ids: number[];
        allDone: boolean;
      }

      const fo = fanOut<ConcParentBag>()({
        name: "concurrent",
        needs: ["ids"] as const,
        childWorkflow: concChild,
        mapInput: (bag) => bag.ids.map((id) => ({ id })),
        provides: ["allDone"] as const,
        aggregateResults: () => ({ allDone: true }),
        concurrency: 2,
      });

      const workflow = createWorkflow<ConcParentBag>("conc-wf").requires("ids").build([fo]);

      const { error } = await testComposer.runSyncWorkflow(workflow, {
        ids: [1, 2, 3, 4, 5],
      });

      expect(error).toBeUndefined();
      expect(maxConcurrent).toBeLessThanOrEqual(2);
    });

    it("should work alongside regular steps in the same workflow", async () => {
      interface MixedBag {
        items: string[];
        results: string[];
        summary: string;
      }

      const fo = fanOut<MixedBag>()({
        name: "processAll",
        needs: ["items"] as const,
        childWorkflow,
        mapInput: (bag) => bag.items.map((item) => ({ item })),
        provides: ["results"] as const,
        aggregateResults: (childResults) => ({
          results: childResults.map((r) => r.processed),
        }),
      });

      const summarize = step<MixedBag, undefined>()({
        name: "summarize",
        needs: ["results"] as const,
        provides: ["summary"] as const,
        run: async (_ctx, bag) => ({ summary: bag.results.join(", ") }),
      });

      const workflow = createWorkflow<MixedBag>("mixed-wf")
        .requires("items")
        .build([fo, summarize]);

      const { bag, error } = await testComposer.runSyncWorkflow(workflow, {
        items: ["x", "y"],
      });

      expect(error).toBeUndefined();
      expect(bag.results).toEqual(["X", "Y"]);
      expect(bag.summary).toBe("X, Y");
    });
  });
});
