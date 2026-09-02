/**
 * Tests for Workflow Factory
 *
 * Tests the factory that creates deterministic Temporal workflow functions.
 * These tests verify workflow execution logic, activity invocation, batch
 * processing, data flow through the bag, and error handling.
 */

import { ApplicationFailure } from "@temporalio/workflow";
import { beforeEach, describe, expect, it, type MockedFunction, vi } from "vitest";

import type {
  FanOutBatchEntry,
  StepActivityConfig,
  StepBatch,
  TemporalWorkflowResult,
  WorkflowInput,
  WorkflowPlan,
} from "../workflow-factory";
import { createWorkflowFunction } from "../workflow-factory";

/**
 * Runs a workflow function and catches ApplicationFailure, extracting the bag
 * and error from its details. This mirrors what executeWorkflowTemporal does
 * on the client side. Workflows with unhandled errors throw ApplicationFailure
 * instead of returning { bag, error }.
 */
async function runWorkflowExpectingResult(
  workflowFn: (input: WorkflowInput) => Promise<TemporalWorkflowResult>,
  input: WorkflowInput,
): Promise<TemporalWorkflowResult> {
  try {
    return await workflowFn(input);
  } catch (err) {
    if (err instanceof ApplicationFailure && err.details) {
      // The failure carries the error and the bag as separate details entries.
      const [errorDetail, bagDetail] = err.details as [
        { error?: TemporalWorkflowResult["error"] },
        { bag?: Record<string, unknown> } | undefined,
      ];
      return { bag: bagDetail?.bag ?? {}, error: errorDetail?.error };
    }
    throw err;
  }
}

// Mock @temporalio/workflow module at module level
type LogFn = (...args: unknown[]) => void;
type ActivityFn = (
  workflowInput: WorkflowInput,
  stepInput: unknown,
  ...extraArgs: unknown[]
) => Promise<unknown>;

function mockActivity<const Args extends readonly unknown[], Result>(
  implementation: (workflowInput: WorkflowInput, ...args: Args) => Promise<Result>,
): MockedFunction<ActivityFn> {
  return vi.fn((workflowInput, ...args) =>
    implementation(workflowInput, ...(args as unknown as Args)),
  );
}

// Use vi.hoisted() to ensure mock state is available before module import
const mocks = vi.hoisted(() => ({
  log: {
    info: vi.fn<LogFn>(),
    debug: vi.fn<LogFn>(),
    error: vi.fn<LogFn>(),
    warn: vi.fn<LogFn>(),
  },
  proxyActivities: vi.fn(
    (_: Record<string, unknown>): Record<string, MockedFunction<ActivityFn>> => ({}),
  ),
  workflowInfo: vi.fn(() => ({
    workflowId: "test-workflow-id",
    runId: "test-run-id",
    workflowType: "test-workflow",
  })),
  defineUpdate: vi.fn((name: string) => Symbol.for(name)),
  setHandler: vi.fn((_update: symbol, _handler: (...args: unknown[]) => unknown) => {}),
  condition: vi.fn(async (_fn: () => boolean) => {}),
  executeChild: vi.fn(async (_workflowType: string, _options?: Record<string, unknown>) => ({})),
  activityFunctions: {} as Record<string, MockedFunction<ActivityFn>>,
  registeredHandlers: new Map<symbol, (...args: unknown[]) => unknown>(),
}));

vi.mock("@temporalio/workflow", async (importOriginal) => {
  const actual = (await importOriginal()) as Record<string, unknown>;
  return {
    ...actual,
    get log() {
      return mocks.log;
    },
    get proxyActivities() {
      return mocks.proxyActivities;
    },
    get workflowInfo() {
      return mocks.workflowInfo;
    },
    get defineUpdate() {
      return mocks.defineUpdate;
    },
    get setHandler() {
      return mocks.setHandler;
    },
    get condition() {
      return mocks.condition;
    },
    get executeChild() {
      return mocks.executeChild;
    },
  };
});

/**
 * Builds the ApplicationFailure shape the activity worker produces for a coded
 * error: the error code lands in `type`, which is the field Temporal matches
 * against `RetryPolicy.nonRetryableErrorTypes`.
 */
function codedApplicationFailure(code: string): ApplicationFailure {
  return ApplicationFailure.create({
    type: code,
    message: `${code}: deterministic failure`,
    details: [{ code, parentCodes: [] }],
    nonRetryable: false,
  });
}

/**
 * Makes proxyActivities hand back activities wrapped in a retry loop that
 * mirrors Temporal's server-side semantics: retry up to `maximumAttempts`,
 * but give up after the first attempt when the failure's `type` is listed in
 * `nonRetryableErrorTypes`. The real loop lives in the Temporal server and
 * cannot be observed from a unit test, so this stand-in checks that the policy
 * the workflow hands to proxyActivities produces the intended behavior.
 * Attempt counts are asserted on the inner mocks in `mocks.activityFunctions`.
 */
function applyTemporalRetrySemantics(): void {
  mocks.proxyActivities.mockImplementation((options: Record<string, unknown>) => {
    const retry = (options.retry ?? {}) as {
      maximumAttempts?: number;
      nonRetryableErrorTypes?: string[];
    };
    const maximumAttempts = retry.maximumAttempts ?? 3;
    const nonRetryable = new Set(retry.nonRetryableErrorTypes ?? []);

    const wrapped: Record<string, MockedFunction<ActivityFn>> = {};
    for (const [name, activityFn] of Object.entries(mocks.activityFunctions)) {
      wrapped[name] = vi.fn(async (workflowInput, ...args) => {
        let lastError: unknown;
        for (let attempt = 1; attempt <= maximumAttempts; attempt++) {
          try {
            return await activityFn(workflowInput, ...args);
          } catch (err) {
            lastError = err;
            const type = (err as { type?: string }).type;
            if (type !== undefined && nonRetryable.has(type)) break;
          }
        }
        throw lastError;
      });
    }
    return wrapped;
  });
}

describe("WorkflowFactory", () => {
  describe("createWorkflowFunction", () => {
    beforeEach(() => {
      // Reset all mocks
      vi.clearAllMocks();

      // Reset activity functions
      mocks.activityFunctions = {};

      // Reset registered handlers
      mocks.registeredHandlers = new Map();

      // Reset proxyActivities to return our activityFunctions
      mocks.proxyActivities.mockImplementation((_: Record<string, unknown>) => {
        return mocks.activityFunctions;
      });

      // Reset workflowInfo
      mocks.workflowInfo.mockReturnValue({
        workflowId: "test-workflow-id",
        runId: "test-run-id",
        workflowType: "test-workflow",
      });

      // Reset setHandler to track registered handlers
      mocks.setHandler.mockImplementation(
        (update: symbol, handler: (...args: unknown[]) => unknown) => {
          mocks.registeredHandlers.set(update, handler);
        },
      );

      // Reset condition to immediately resolve (simulating checkpoint already reached)
      mocks.condition.mockImplementation(async (_fn: () => boolean) => {
        // Default: immediately resolve
      });
    });

    describe("Basic Workflow Execution", () => {
      it("should create a workflow function", () => {
        const plan: WorkflowPlan = {
          name: "testWorkflow",
          batches: [],
        };

        const workflowFn = createWorkflowFunction(plan);

        expect(workflowFn).toBeInstanceOf(Function);
      });

      it("should execute workflow with empty plan", async () => {
        const plan: WorkflowPlan = {
          name: "emptyWorkflow",
          batches: [],
        };

        const workflowFn = createWorkflowFunction(plan);

        const input: WorkflowInput = {
          initialData: { key: "value" },
        };

        const { bag, error } = await workflowFn(input);

        // Should return initial data unchanged
        expect(error).toBeUndefined();
        expect(bag).toEqual({ key: "value" });
      });

      it("should initialize bag with initial data", async () => {
        const plan: WorkflowPlan = {
          name: "initWorkflow",
          batches: [],
        };

        const workflowFn = createWorkflowFunction(plan);

        const input: WorkflowInput = {
          initialData: {
            userId: "user-123",
            orgId: "org-456",
            requestId: "req-789",
          },
        };

        const { bag, error } = await workflowFn(input);

        expect(error).toBeUndefined();
        expect(bag).toHaveProperty("userId", "user-123");
        expect(bag).toHaveProperty("orgId", "org-456");
        expect(bag).toHaveProperty("requestId", "req-789");
      });
    });

    describe("Activity Invocation", () => {
      it("should invoke activity with correct input", async () => {
        const mockActivity = vi.fn(async (_workflowInput, _stepInput) => ({
          output: "test-output",
        }));

        mocks.activityFunctions["testStep-hash123"] = mockActivity;

        const plan: WorkflowPlan = {
          name: "activityWorkflow",
          batches: [
            {
              steps: [
                {
                  name: "testStep",
                  activityName: "testStep-hash123",
                  needs: ["input"],
                  provides: ["output"],
                  taskQueue: "standard-tasks",
                },
              ],
            },
          ],
        };

        const workflowFn = createWorkflowFunction(plan);

        const input: WorkflowInput = {
          initialData: { input: "test-input" },
        };

        await workflowFn(input);

        // Verify activity was called with correct arguments (no workflowId - gets it from Temporal context)
        expect(mockActivity).toHaveBeenCalledWith(
          expect.objectContaining({
            initialData: { input: "test-input" },
          }),
          { input: "test-input" },
        );
      });

      it("should call proxyActivities with correct configuration", async () => {
        mocks.activityFunctions["step1-abc"] = vi.fn(async () => ({
          result: "done",
        }));

        const plan: WorkflowPlan = {
          name: "configWorkflow",
          batches: [
            {
              steps: [
                {
                  name: "step1",
                  activityName: "step1-abc",
                  needs: [],
                  provides: ["result"],
                  taskQueue: "heavy-tasks",
                },
              ],
            },
          ],
        };

        const workflowFn = createWorkflowFunction(plan);

        const input: WorkflowInput = {
          initialData: {},
        };

        await workflowFn(input);

        expect(mocks.proxyActivities).toHaveBeenCalledWith({
          taskQueue: "heavy-tasks",
          startToCloseTimeout: "5 minutes",
          retry: {
            maximumAttempts: 3,
            backoffCoefficient: 2,
            initialInterval: "1s",
            maximumInterval: "60s",
          },
        });
      });

      it("should throw error if activity not found", async () => {
        // Don't register the activity
        mocks.activityFunctions = {};

        const plan: WorkflowPlan = {
          name: "missingActivityWorkflow",
          batches: [
            {
              steps: [
                {
                  name: "missingStep",
                  activityName: "missingStep-xyz",
                  needs: [],
                  provides: [],
                  taskQueue: "standard-tasks",
                },
              ],
            },
          ],
        };

        const workflowFn = createWorkflowFunction(plan);

        const input: WorkflowInput = {
          initialData: {},
        };

        const { error } = await runWorkflowExpectingResult(workflowFn, input);
        expect(error).toBeDefined();
        expect(error?.message).toContain("Batch 1 failed: 1 step(s) failed");
      });
    });

    describe("Non-Retryable Error Types", () => {
      /** Builds a single-step plan whose activity carries the given retry config. */
      function planWithRetry(retry: StepActivityConfig["retry"]): WorkflowPlan {
        return {
          name: "retryWorkflow",
          batches: [
            {
              steps: [
                {
                  name: "step1",
                  activityName: "step1-abc",
                  needs: [],
                  provides: ["result"],
                  taskQueue: "standard-tasks",
                  activityConfig: { retry },
                },
              ],
            },
          ],
        };
      }

      /** Reads the retry policy the workflow handed to proxyActivities. */
      function capturedRetryPolicy(): Record<string, unknown> {
        expect(mocks.proxyActivities).toHaveBeenCalledTimes(1);
        const options = mocks.proxyActivities.mock.calls[0]![0];
        return options.retry as Record<string, unknown>;
      }

      it("should pass nonRetryableErrorTypes through to the Temporal retry policy", async () => {
        mocks.activityFunctions["step1-abc"] = vi.fn(async () => ({ result: "done" }));

        const workflowFn = createWorkflowFunction(
          planWithRetry({
            maximumAttempts: 2,
            nonRetryableErrorTypes: ["CONTENT_REJECTED", "VALIDATION_FAILED"],
          }),
        );
        await workflowFn({ initialData: {} });

        expect(capturedRetryPolicy()).toEqual({
          maximumAttempts: 2,
          backoffCoefficient: 2,
          initialInterval: "1s",
          maximumInterval: "60s",
          nonRetryableErrorTypes: ["CONTENT_REJECTED", "VALIDATION_FAILED"],
        });
      });

      it("should omit nonRetryableErrorTypes when the step does not set it", async () => {
        mocks.activityFunctions["step1-abc"] = vi.fn(async () => ({ result: "done" }));

        const workflowFn = createWorkflowFunction(planWithRetry({ maximumAttempts: 5 }));
        await workflowFn({ initialData: {} });

        const retry = capturedRetryPolicy();
        expect(retry).not.toHaveProperty("nonRetryableErrorTypes");
        expect(retry).toEqual({
          maximumAttempts: 5,
          backoffCoefficient: 2,
          initialInterval: "1s",
          maximumInterval: "60s",
        });
      });

      it("should omit nonRetryableErrorTypes when set to an empty array", async () => {
        mocks.activityFunctions["step1-abc"] = vi.fn(async () => ({ result: "done" }));

        const workflowFn = createWorkflowFunction(
          planWithRetry({ maximumAttempts: 3, nonRetryableErrorTypes: [] }),
        );
        await workflowFn({ initialData: {} });

        expect(capturedRetryPolicy()).not.toHaveProperty("nonRetryableErrorTypes");
      });

      it("should not retry a step whose error code is listed as non-retryable", async () => {
        const failingActivity = vi.fn(async () => {
          throw codedApplicationFailure("CONTENT_REJECTED");
        });
        mocks.activityFunctions["step1-abc"] = failingActivity;
        applyTemporalRetrySemantics();

        const workflowFn = createWorkflowFunction(
          planWithRetry({ maximumAttempts: 3, nonRetryableErrorTypes: ["CONTENT_REJECTED"] }),
        );

        const { error } = await runWorkflowExpectingResult(workflowFn, { initialData: {} });

        expect(failingActivity).toHaveBeenCalledTimes(1);
        expect(error?.errors?.[0]?.code).toBe("CONTENT_REJECTED");
      });

      it("should still retry a step whose error code is not listed", async () => {
        const failingActivity = vi.fn(async () => {
          throw codedApplicationFailure("UPSTREAM_TIMEOUT");
        });
        mocks.activityFunctions["step1-abc"] = failingActivity;
        applyTemporalRetrySemantics();

        const workflowFn = createWorkflowFunction(
          planWithRetry({ maximumAttempts: 3, nonRetryableErrorTypes: ["CONTENT_REJECTED"] }),
        );

        const { error } = await runWorkflowExpectingResult(workflowFn, { initialData: {} });

        expect(failingActivity).toHaveBeenCalledTimes(3);
        expect(error).toBeDefined();
      });
    });

    describe("Data Flow and Bag Management", () => {
      it("should extract only needed fields for step input", async () => {
        const mockActivity = vi.fn(async (_workflowInput, _stepInput) => ({
          result: "computed",
        }));

        mocks.activityFunctions["selectiveStep-abc"] = mockActivity;

        const plan: WorkflowPlan = {
          name: "selectiveWorkflow",
          batches: [
            {
              steps: [
                {
                  name: "selectiveStep",
                  activityName: "selectiveStep-abc",
                  needs: ["field1", "field2"],
                  provides: ["result"],
                  taskQueue: "standard-tasks",
                },
              ],
            },
          ],
        };

        const workflowFn = createWorkflowFunction(plan);

        const input: WorkflowInput = {
          initialData: {
            field1: "value1",
            field2: "value2",
            field3: "value3", // Should NOT be passed to step
            field4: "value4", // Should NOT be passed to step
          },
        };

        await workflowFn(input);

        // Verify only needed fields were passed
        expect(mockActivity).toHaveBeenCalledWith(expect.anything(), {
          field1: "value1",
          field2: "value2",
        });
      });

      it("should merge step outputs into bag", async () => {
        const step1Activity = vi.fn(async () => ({ data1: "output1" }));
        const step2Activity = vi.fn(async () => ({ data2: "output2" }));

        mocks.activityFunctions["step1-abc"] = step1Activity;
        mocks.activityFunctions["step2-def"] = step2Activity;

        const plan: WorkflowPlan = {
          name: "mergeWorkflow",
          batches: [
            {
              steps: [
                {
                  name: "step1",
                  activityName: "step1-abc",
                  needs: [],
                  provides: ["data1"],
                  taskQueue: "standard-tasks",
                },
              ],
            },
            {
              steps: [
                {
                  name: "step2",
                  activityName: "step2-def",
                  needs: ["data1"],
                  provides: ["data2"],
                  taskQueue: "standard-tasks",
                },
              ],
            },
          ],
        };

        const workflowFn = createWorkflowFunction(plan);

        const input: WorkflowInput = {
          initialData: { initial: "value" },
        };

        const { bag, error } = await workflowFn(input);

        // Final bag should contain initial data + all outputs
        expect(error).toBeUndefined();
        expect(bag).toEqual({
          initial: "value",
          data1: "output1",
          data2: "output2",
        });
      });

      it("should allow step outputs to overwrite bag values", async () => {
        const step1Activity = vi.fn(async () => ({ counter: 1 }));
        const step2Activity = vi.fn(async () => ({ counter: 2 }));

        mocks.activityFunctions["step1-abc"] = step1Activity;
        mocks.activityFunctions["step2-def"] = step2Activity;

        const plan: WorkflowPlan = {
          name: "overwriteWorkflow",
          batches: [
            {
              steps: [
                {
                  name: "step1",
                  activityName: "step1-abc",
                  needs: [],
                  provides: ["counter"],
                  taskQueue: "standard-tasks",
                },
              ],
            },
            {
              steps: [
                {
                  name: "step2",
                  activityName: "step2-def",
                  needs: ["counter"],
                  provides: ["counter"],
                  taskQueue: "standard-tasks",
                },
              ],
            },
          ],
        };

        const workflowFn = createWorkflowFunction(plan);

        const input: WorkflowInput = {
          initialData: { counter: 0 },
        };

        const { bag, error } = await workflowFn(input);

        // Final counter should be from step2
        expect(error).toBeUndefined();
        expect(bag.counter).toBe(2);
      });
    });

    describe("Batch Execution", () => {
      it("should execute steps in same batch in parallel", async () => {
        vi.useFakeTimers();
        vi.setSystemTime(0);
        try {
          const executionEvents: Array<{ step: string; event: string; timestamp: number }> = [];
          const startTime = Date.now();

          const step1Activity = vi.fn(async () => {
            executionEvents.push({
              step: "step1",
              event: "start",
              timestamp: Date.now() - startTime,
            });
            await new Promise((resolve) => setTimeout(resolve, 50));
            executionEvents.push({
              step: "step1",
              event: "end",
              timestamp: Date.now() - startTime,
            });
            return { result1: "done" };
          });

          const step2Activity = vi.fn(async () => {
            executionEvents.push({
              step: "step2",
              event: "start",
              timestamp: Date.now() - startTime,
            });
            await new Promise((resolve) => setTimeout(resolve, 50));
            executionEvents.push({
              step: "step2",
              event: "end",
              timestamp: Date.now() - startTime,
            });
            return { result2: "done" };
          });

          mocks.activityFunctions["step1-abc"] = step1Activity;
          mocks.activityFunctions["step2-def"] = step2Activity;

          const plan: WorkflowPlan = {
            name: "parallelWorkflow",
            batches: [
              {
                // These steps should run in parallel
                steps: [
                  {
                    name: "step1",
                    activityName: "step1-abc",
                    needs: ["input"],
                    provides: ["result1"],
                    taskQueue: "standard-tasks",
                  },
                  {
                    name: "step2",
                    activityName: "step2-def",
                    needs: ["input"],
                    provides: ["result2"],
                    taskQueue: "standard-tasks",
                  },
                ],
              },
            ],
          };

          const workflowFn = createWorkflowFunction(plan);

          const input: WorkflowInput = {
            initialData: { input: "data" },
          };

          const startExecutionTime = Date.now();
          const executionPromise = workflowFn(input);
          await vi.runAllTimersAsync();
          await executionPromise;
          const totalExecutionTime = Date.now() - startExecutionTime;

          // Both steps should have been called
          expect(step1Activity).toHaveBeenCalled();
          expect(step2Activity).toHaveBeenCalled();

          // Verify both steps started before either completed (proof of parallelism)
          const step1Start = executionEvents.find((e) => e.step === "step1" && e.event === "start");
          const step2Start = executionEvents.find((e) => e.step === "step2" && e.event === "start");
          const step1End = executionEvents.find((e) => e.step === "step1" && e.event === "end");
          const step2End = executionEvents.find((e) => e.step === "step2" && e.event === "end");

          expect(step1Start).toBeDefined();
          expect(step2Start).toBeDefined();
          expect(step1End).toBeDefined();
          expect(step2End).toBeDefined();

          // Both should start before either ends (parallel execution proof)
          const lastStartTime = Math.max(step1Start!.timestamp, step2Start!.timestamp);
          const firstEndTime = Math.min(step1End!.timestamp, step2End!.timestamp);
          expect(lastStartTime).toBeLessThan(firstEndTime);

          // Total time should be 50ms with fake timers when steps run in parallel.
          expect(totalExecutionTime).toBe(50);
        } finally {
          vi.useRealTimers();
        }
      });

      it("should execute batches sequentially", async () => {
        const executionOrder: string[] = [];

        const batch1Step = vi.fn(async () => {
          executionOrder.push("batch1");
          return { data: "from-batch1" };
        });

        const batch2Step = vi.fn(async () => {
          executionOrder.push("batch2");
          return { result: "from-batch2" };
        });

        mocks.activityFunctions["batch1Step-abc"] = batch1Step;
        mocks.activityFunctions["batch2Step-def"] = batch2Step;

        const plan: WorkflowPlan = {
          name: "sequentialBatchWorkflow",
          batches: [
            {
              steps: [
                {
                  name: "batch1Step",
                  activityName: "batch1Step-abc",
                  needs: [],
                  provides: ["data"],
                  taskQueue: "standard-tasks",
                },
              ],
            },
            {
              steps: [
                {
                  name: "batch2Step",
                  activityName: "batch2Step-def",
                  needs: ["data"],
                  provides: ["result"],
                  taskQueue: "standard-tasks",
                },
              ],
            },
          ],
        };

        const workflowFn = createWorkflowFunction(plan);

        const input: WorkflowInput = {
          initialData: {},
        };

        await workflowFn(input);

        // Batch 1 must complete before batch 2 starts
        expect(executionOrder).toEqual(["batch1", "batch2"]);
      });

      it("should handle workflows with many batches", async () => {
        const batches: StepBatch[] = [];
        const activities: Record<string, MockedFunction<ActivityFn>> = {};

        // Create 10 batches with 1 step each
        for (let i = 0; i < 10; i++) {
          const stepName = `step${i}`;
          const activityName = `step${i}-hash${i}`;

          activities[activityName] = vi.fn(async () => ({
            [`result${i}`]: `value${i}`,
          }));

          batches.push({
            steps: [
              {
                name: stepName,
                activityName,
                needs: i === 0 ? [] : [`result${i - 1}`],
                provides: [`result${i}`],
                taskQueue: "standard-tasks",
              },
            ],
          });
        }

        mocks.activityFunctions = activities;

        const plan: WorkflowPlan = {
          name: "manyBatchesWorkflow",
          batches,
        };

        const workflowFn = createWorkflowFunction(plan);

        const input: WorkflowInput = {
          initialData: {},
        };

        const { bag, error } = await workflowFn(input);

        // Should have all outputs
        expect(error).toBeUndefined();
        for (let i = 0; i < 10; i++) {
          expect(bag).toHaveProperty(`result${i}`, `value${i}`);
        }
      });
    });

    describe("Workflow Input Metadata", () => {
      it("should get workflowId from Temporal execution context", async () => {
        const mockActivity = vi.fn(async () => ({ result: "done" }));
        mocks.activityFunctions["step-abc"] = mockActivity;

        const plan: WorkflowPlan = {
          name: "metadataWorkflow",
          batches: [
            {
              steps: [
                {
                  name: "step",
                  activityName: "step-abc",
                  needs: [],
                  provides: ["result"],
                  taskQueue: "standard-tasks",
                },
              ],
            },
          ],
        };

        const workflowFn = createWorkflowFunction(plan);

        const input: WorkflowInput = {
          initialData: {},
        };

        await workflowFn(input);

        // workflowId is obtained from Temporal's workflowInfo(), not passed in input
        expect(mocks.workflowInfo).toHaveBeenCalled();
        expect(mockActivity).toHaveBeenCalledWith(
          expect.not.objectContaining({
            workflowId: expect.anything(),
          }),
          expect.anything(),
        );
      });

      it("should accept optional environment", async () => {
        const mockActivity = vi.fn(async () => ({ result: "done" }));
        mocks.activityFunctions["step-abc"] = mockActivity;

        const plan: WorkflowPlan = {
          name: "envWorkflow",
          batches: [
            {
              steps: [
                {
                  name: "step",
                  activityName: "step-abc",
                  needs: [],
                  provides: ["result"],
                  taskQueue: "standard-tasks",
                },
              ],
            },
          ],
        };

        const workflowFn = createWorkflowFunction(plan);

        const input: WorkflowInput = {
          initialData: {},
          environment: "production",
        };

        await workflowFn(input);

        expect(mockActivity).toHaveBeenCalledWith(
          expect.objectContaining({
            environment: "production",
          }),
          expect.anything(),
        );
      });

      it("should accept optional metadata object", async () => {
        const mockActivity = vi.fn(async () => ({ result: "done" }));
        mocks.activityFunctions["step-abc"] = mockActivity;

        const plan: WorkflowPlan = {
          name: "metaWorkflow",
          batches: [
            {
              steps: [
                {
                  name: "step",
                  activityName: "step-abc",
                  needs: [],
                  provides: ["result"],
                  taskQueue: "standard-tasks",
                },
              ],
            },
          ],
        };

        const workflowFn = createWorkflowFunction(plan);

        const input: WorkflowInput = {
          initialData: {},
          metadata: {
            requestId: "req-789",
            userId: "user-456",
            source: "api",
          },
        };

        await workflowFn(input);

        expect(mockActivity).toHaveBeenCalledWith(
          expect.objectContaining({
            metadata: {
              requestId: "req-789",
              userId: "user-456",
              source: "api",
            },
          }),
          expect.anything(),
        );
      });
    });

    describe("Task Queue Routing", () => {
      it("should route steps to different task queues", async () => {
        const fastStep = vi.fn(async () => ({ fast: "done" }));
        const heavyStep = vi.fn(async () => ({ heavy: "done" }));

        mocks.activityFunctions["fastStep-abc"] = fastStep;
        mocks.activityFunctions["heavyStep-def"] = heavyStep;

        const proxyCallsPerQueue: Record<string, number> = {};
        mocks.proxyActivities.mockImplementation((config) => {
          const queue = config.taskQueue as string;
          proxyCallsPerQueue[queue] = (proxyCallsPerQueue[queue] || 0) + 1;
          return mocks.activityFunctions;
        });

        const plan: WorkflowPlan = {
          name: "multiQueueWorkflow",
          batches: [
            {
              steps: [
                {
                  name: "fastStep",
                  activityName: "fastStep-abc",
                  needs: [],
                  provides: ["fast"],
                  taskQueue: "fast-tasks",
                },
                {
                  name: "heavyStep",
                  activityName: "heavyStep-def",
                  needs: [],
                  provides: ["heavy"],
                  taskQueue: "heavy-tasks",
                },
              ],
            },
          ],
        };

        const workflowFn = createWorkflowFunction(plan);

        const input: WorkflowInput = {
          initialData: {},
        };

        await workflowFn(input);

        // Verify proxyActivities was called once per unique queue
        expect(proxyCallsPerQueue["fast-tasks"]).toBe(1);
        expect(proxyCallsPerQueue["heavy-tasks"]).toBe(1);
      });

      it("should reuse activity proxy for same task queue in batch", async () => {
        const step1 = vi.fn(async () => ({ result1: "done" }));
        const step2 = vi.fn(async () => ({ result2: "done" }));
        const step3 = vi.fn(async () => ({ result3: "done" }));

        mocks.activityFunctions["step1-abc"] = step1;
        mocks.activityFunctions["step2-def"] = step2;
        mocks.activityFunctions["step3-ghi"] = step3;

        let proxyCallCount = 0;
        mocks.proxyActivities.mockImplementation((_config) => {
          proxyCallCount++;
          return mocks.activityFunctions;
        });

        const plan: WorkflowPlan = {
          name: "sameQueueWorkflow",
          batches: [
            {
              steps: [
                {
                  name: "step1",
                  activityName: "step1-abc",
                  needs: [],
                  provides: ["result1"],
                  taskQueue: "standard-tasks",
                },
                {
                  name: "step2",
                  activityName: "step2-def",
                  needs: [],
                  provides: ["result2"],
                  taskQueue: "standard-tasks",
                },
                {
                  name: "step3",
                  activityName: "step3-ghi",
                  needs: [],
                  provides: ["result3"],
                  taskQueue: "standard-tasks",
                },
              ],
            },
          ],
        };

        const workflowFn = createWorkflowFunction(plan);

        const input: WorkflowInput = {
          initialData: {},
        };

        await workflowFn(input);

        // Should only create one proxy for the shared task queue
        expect(proxyCallCount).toBe(1);
      });
    });

    describe("Edge Cases and Error Scenarios", () => {
      it("should handle steps with empty needs array", async () => {
        const mockActivity = vi.fn(async () => ({ result: "done" }));
        mocks.activityFunctions["step-abc"] = mockActivity;

        const plan: WorkflowPlan = {
          name: "emptyNeedsWorkflow",
          batches: [
            {
              steps: [
                {
                  name: "step",
                  activityName: "step-abc",
                  needs: [],
                  provides: ["result"],
                  taskQueue: "standard-tasks",
                },
              ],
            },
          ],
        };

        const workflowFn = createWorkflowFunction(plan);

        const input: WorkflowInput = {
          initialData: { someData: "value" },
        };

        await workflowFn(input);

        // Should pass empty object as step input
        expect(mockActivity).toHaveBeenCalledWith(expect.anything(), {});
      });

      it("should handle steps with empty provides array", async () => {
        const mockActivity = vi.fn(async () => ({}));
        mocks.activityFunctions["step-abc"] = mockActivity;

        const plan: WorkflowPlan = {
          name: "emptyProvidesWorkflow",
          batches: [
            {
              steps: [
                {
                  name: "step",
                  activityName: "step-abc",
                  needs: [],
                  provides: [],
                  taskQueue: "standard-tasks",
                },
              ],
            },
          ],
        };

        const workflowFn = createWorkflowFunction(plan);

        const input: WorkflowInput = {
          initialData: { initial: "value" },
        };

        const { bag, error } = await workflowFn(input);

        // Bag should remain unchanged
        expect(error).toBeUndefined();
        expect(bag).toEqual({ initial: "value" });
      });

      it("should handle activity returning extra fields beyond provides", async () => {
        const mockActivity = vi.fn(async () => ({
          expected: "value1",
          unexpected: "value2",
          extra: "value3",
        }));
        mocks.activityFunctions["step-abc"] = mockActivity;

        const plan: WorkflowPlan = {
          name: "extraFieldsWorkflow",
          batches: [
            {
              steps: [
                {
                  name: "step",
                  activityName: "step-abc",
                  needs: [],
                  provides: ["expected"], // Only declares one field
                  taskQueue: "standard-tasks",
                },
              ],
            },
          ],
        };

        const workflowFn = createWorkflowFunction(plan);

        const input: WorkflowInput = {
          initialData: {},
        };

        const { bag, error } = await workflowFn(input);

        // All fields should be merged into bag (permissive merge)
        expect(error).toBeUndefined();
        expect(bag).toEqual({
          expected: "value1",
          unexpected: "value2",
          extra: "value3",
        });
      });

      it("should handle undefined values in initial data", async () => {
        const mockActivity = vi.fn(async (_workflowInput, _stepInput) => ({
          result: "done",
        }));
        mocks.activityFunctions["step-abc"] = mockActivity;

        const plan: WorkflowPlan = {
          name: "undefinedValuesWorkflow",
          batches: [
            {
              steps: [
                {
                  name: "step",
                  activityName: "step-abc",
                  needs: ["field1", "field2"],
                  provides: ["result"],
                  taskQueue: "standard-tasks",
                },
              ],
            },
          ],
        };

        const workflowFn = createWorkflowFunction(plan);

        const input: WorkflowInput = {
          initialData: {
            field1: undefined,
            field2: "value",
          },
        };

        await workflowFn(input);

        // Should pass undefined values
        expect(mockActivity).toHaveBeenCalledWith(expect.anything(), {
          field1: undefined,
          field2: "value",
        });
      });

      it("should handle null values in activity output", async () => {
        const mockActivity = vi.fn(async () => ({
          result: null,
        }));
        mocks.activityFunctions["step-abc"] = mockActivity;

        const plan: WorkflowPlan = {
          name: "nullOutputWorkflow",
          batches: [
            {
              steps: [
                {
                  name: "step",
                  activityName: "step-abc",
                  needs: [],
                  provides: ["result"],
                  taskQueue: "standard-tasks",
                },
              ],
            },
          ],
        };

        const workflowFn = createWorkflowFunction(plan);

        const input: WorkflowInput = {
          initialData: {},
        };

        const { bag, error } = await workflowFn(input);

        expect(error).toBeUndefined();
        expect(bag).toEqual({ result: null });
      });

      it("should handle workflow with single batch and single step", async () => {
        const mockActivity = vi.fn(async () => ({ done: true }));
        mocks.activityFunctions["onlyStep-abc"] = mockActivity;

        const plan: WorkflowPlan = {
          name: "singleStepWorkflow",
          batches: [
            {
              steps: [
                {
                  name: "onlyStep",
                  activityName: "onlyStep-abc",
                  needs: [],
                  provides: ["done"],
                  taskQueue: "standard-tasks",
                },
              ],
            },
          ],
        };

        const workflowFn = createWorkflowFunction(plan);

        const input: WorkflowInput = {
          initialData: {},
        };

        const { bag, error } = await workflowFn(input);

        expect(error).toBeUndefined();
        expect(bag).toEqual({ done: true });
        expect(mockActivity).toHaveBeenCalledTimes(1);
      });

      it("should propagate activity errors", async () => {
        const mockActivity = vi.fn(async () => {
          throw new Error("Activity execution failed");
        });
        mocks.activityFunctions["failingStep-abc"] = mockActivity;

        const plan: WorkflowPlan = {
          name: "errorWorkflow",
          batches: [
            {
              steps: [
                {
                  name: "failingStep",
                  activityName: "failingStep-abc",
                  needs: [],
                  provides: ["result"],
                  taskQueue: "standard-tasks",
                },
              ],
            },
          ],
        };

        const workflowFn = createWorkflowFunction(plan);

        const input: WorkflowInput = {
          initialData: {},
        };

        const { error } = await runWorkflowExpectingResult(workflowFn, input);
        expect(error).toBeDefined();
        expect(error?.message).toContain("Batch 1 failed: 1 step(s) failed");
      });

      it("should handle activity error in parallel batch", async () => {
        const successActivity = vi.fn(async () => ({ success: true }));
        const failActivity = vi.fn(async () => {
          throw new Error("Parallel step failed");
        });

        mocks.activityFunctions["successStep-abc"] = successActivity;
        mocks.activityFunctions["failStep-def"] = failActivity;

        const plan: WorkflowPlan = {
          name: "parallelErrorWorkflow",
          batches: [
            {
              steps: [
                {
                  name: "successStep",
                  activityName: "successStep-abc",
                  needs: [],
                  provides: ["success"],
                  taskQueue: "standard-tasks",
                },
                {
                  name: "failStep",
                  activityName: "failStep-def",
                  needs: [],
                  provides: ["failure"],
                  taskQueue: "standard-tasks",
                },
              ],
            },
          ],
        };

        const workflowFn = createWorkflowFunction(plan);

        const input: WorkflowInput = {
          initialData: {},
        };

        const { error } = await runWorkflowExpectingResult(workflowFn, input);
        expect(error).toBeDefined();
        expect(error?.message).toContain("Batch 1 failed: 1 step(s) failed");

        // Success activity should still have been called (allSettled waits for all)
        expect(successActivity).toHaveBeenCalled();
      });

      it("should handle missing fields in bag gracefully", async () => {
        const mockActivity = vi.fn(async (_workflowInput, stepInput) => {
          // Activity receives what's in the bag, even if undefined
          return { result: stepInput.missingField || "default" };
        });
        mocks.activityFunctions["step-abc"] = mockActivity;

        const plan: WorkflowPlan = {
          name: "missingFieldWorkflow",
          batches: [
            {
              steps: [
                {
                  name: "step",
                  activityName: "step-abc",
                  needs: ["missingField"],
                  provides: ["result"],
                  taskQueue: "standard-tasks",
                },
              ],
            },
          ],
        };

        const workflowFn = createWorkflowFunction(plan);

        const input: WorkflowInput = {
          initialData: {},
        };

        const { bag, error } = await workflowFn(input);

        // Should pass undefined for missing field
        expect(error).toBeUndefined();
        expect(mockActivity).toHaveBeenCalledWith(expect.anything(), { missingField: undefined });
        expect(bag.result).toBe("default");
      });

      it("should handle batch error stopping subsequent batches", async () => {
        const batch1Activity = vi.fn(async () => {
          throw new Error("Batch 1 failed");
        });
        const batch2Activity = vi.fn(async () => ({ result: "done" }));

        mocks.activityFunctions["batch1-abc"] = batch1Activity;
        mocks.activityFunctions["batch2-def"] = batch2Activity;

        const plan: WorkflowPlan = {
          name: "batchErrorWorkflow",
          batches: [
            {
              steps: [
                {
                  name: "batch1",
                  activityName: "batch1-abc",
                  needs: [],
                  provides: ["data"],
                  taskQueue: "standard-tasks",
                },
              ],
            },
            {
              steps: [
                {
                  name: "batch2",
                  activityName: "batch2-def",
                  needs: ["data"],
                  provides: ["result"],
                  taskQueue: "standard-tasks",
                },
              ],
            },
          ],
        };

        const workflowFn = createWorkflowFunction(plan);

        const input: WorkflowInput = {
          initialData: {},
        };

        const { error } = await runWorkflowExpectingResult(workflowFn, input);
        expect(error).toBeDefined();
        expect(error?.message).toContain("Batch 1 failed: 1 step(s) failed");

        // Batch 2 should never execute
        expect(batch1Activity).toHaveBeenCalled();
        expect(batch2Activity).not.toHaveBeenCalled();
      });

      it("should handle empty object returned from activity", async () => {
        const mockActivity = vi.fn(async () => ({}));
        mocks.activityFunctions["step-abc"] = mockActivity;

        const plan: WorkflowPlan = {
          name: "emptyReturnWorkflow",
          batches: [
            {
              steps: [
                {
                  name: "step",
                  activityName: "step-abc",
                  needs: [],
                  provides: [],
                  taskQueue: "standard-tasks",
                },
              ],
            },
          ],
        };

        const workflowFn = createWorkflowFunction(plan);

        const input: WorkflowInput = {
          initialData: { initial: "value" },
        };

        const { bag, error } = await workflowFn(input);

        expect(error).toBeUndefined();
        expect(bag).toEqual({ initial: "value" });
      });
    });

    describe("Error Handler Activity", () => {
      it("should invoke error handler activity when batch fails", async () => {
        const failingActivity = vi.fn(async () => {
          throw new Error("Step execution failed");
        });
        // Error handler returns { handled: false } to propagate the error
        const errorHandlerActivity = vi.fn(async () => ({ handled: false }));

        mocks.activityFunctions["failingStep-abc"] = failingActivity;
        mocks.activityFunctions.testWorkflow__errorHandler = errorHandlerActivity;

        const plan: WorkflowPlan = {
          name: "testWorkflow",
          batches: [
            {
              steps: [
                {
                  name: "failingStep",
                  activityName: "failingStep-abc",
                  needs: [],
                  provides: ["output"],
                  taskQueue: "standard-tasks",
                },
              ],
            },
          ],
          errorHandlerActivityName: "testWorkflow__errorHandler",
          errorHandlerTaskQueue: "standard-tasks",
        };

        const workflowFn = createWorkflowFunction(plan);

        const input: WorkflowInput = {
          initialData: { input: "test" },
        };

        const { error } = await runWorkflowExpectingResult(workflowFn, input);
        expect(error).toBeDefined();
        expect(error?.message).toContain("Batch 1 failed");

        // Error handler should have been called
        expect(errorHandlerActivity).toHaveBeenCalledTimes(1);

        // Verify the error info passed to error handler
        const callArgs = errorHandlerActivity.mock.calls[0] as unknown[];
        const passedInput = callArgs[0];
        const passedBag = callArgs[1] as Record<string, unknown>;
        const passedErrorInfo = callArgs[2] as {
          batchNumber: number;
          workflowId: string;
          errors: Array<{ stepName: string; message: string }>;
        };
        expect(passedInput).toEqual(input);
        expect(passedBag).toEqual({ input: "test" }); // Initial data
        expect(passedErrorInfo.batchNumber).toBe(1);
        expect(passedErrorInfo.workflowId).toBe("test-workflow-id");
        expect(passedErrorInfo.errors).toHaveLength(1);
        expect(passedErrorInfo.errors[0]!.stepName).toBe("failingStep");
        // No message: this value is an activity argument and a details entry, so it is
        // twice at rest in event history. Classification travels as code/type instead.
        expect(passedErrorInfo.errors[0]).not.toHaveProperty("message");
      });

      it("should not fail workflow if error handler activity fails", async () => {
        const failingActivity = vi.fn(async () => {
          throw new Error("Original step error");
        });
        const errorHandlerActivity = vi.fn(async () => {
          throw new Error("Error handler also failed");
        });

        mocks.activityFunctions["failingStep-abc"] = failingActivity;
        mocks.activityFunctions.testWorkflow__errorHandler = errorHandlerActivity;

        const plan: WorkflowPlan = {
          name: "testWorkflow",
          batches: [
            {
              steps: [
                {
                  name: "failingStep",
                  activityName: "failingStep-abc",
                  needs: [],
                  provides: ["output"],
                  taskQueue: "standard-tasks",
                },
              ],
            },
          ],
          errorHandlerActivityName: "testWorkflow__errorHandler",
        };

        const workflowFn = createWorkflowFunction(plan);

        const input: WorkflowInput = {
          initialData: {},
        };

        const { error } = await runWorkflowExpectingResult(workflowFn, input);
        expect(error).toBeDefined();
        expect(error?.code).toBe("WORKFLOW_ERROR_HANDLER_FAILURE");

        // Error handler was called (and failed, but that's caught)
        expect(errorHandlerActivity).toHaveBeenCalled();
      });

      it("should not call error handler if not configured", async () => {
        const failingActivity = vi.fn(async () => {
          throw new Error("Step error");
        });
        // Even though we register a mock, it should not be called because the plan doesn't reference it
        const errorHandlerActivity = vi.fn(async () => ({ handled: false }));

        mocks.activityFunctions["failingStep-abc"] = failingActivity;
        mocks.activityFunctions.testWorkflow__errorHandler = errorHandlerActivity;

        // Plan without errorHandlerActivityName
        const plan: WorkflowPlan = {
          name: "testWorkflow",
          batches: [
            {
              steps: [
                {
                  name: "failingStep",
                  activityName: "failingStep-abc",
                  needs: [],
                  provides: ["output"],
                  taskQueue: "standard-tasks",
                },
              ],
            },
          ],
          // No errorHandlerActivityName
        };

        const workflowFn = createWorkflowFunction(plan);

        const input: WorkflowInput = {
          initialData: {},
        };

        const { error } = await runWorkflowExpectingResult(workflowFn, input);
        expect(error).toBeDefined();
        expect(error?.message).toContain("Batch 1 failed");

        // Error handler should NOT have been called
        expect(errorHandlerActivity).not.toHaveBeenCalled();
      });

      it("should include successful step outputs in bag passed to error handler", async () => {
        const successActivity = vi.fn(async () => ({ success: "value" }));
        const failingActivity = vi.fn(async () => {
          throw new Error("Step failed");
        });
        // Error handler returns { handled: false } to propagate the error
        const errorHandlerActivity = vi.fn(async () => ({ handled: false }));

        mocks.activityFunctions["successStep-abc"] = successActivity;
        mocks.activityFunctions["failingStep-def"] = failingActivity;
        mocks.activityFunctions.testWorkflow__errorHandler = errorHandlerActivity;

        const plan: WorkflowPlan = {
          name: "testWorkflow",
          batches: [
            {
              steps: [
                {
                  name: "successStep",
                  activityName: "successStep-abc",
                  needs: [],
                  provides: ["success"],
                  taskQueue: "standard-tasks",
                },
                {
                  name: "failingStep",
                  activityName: "failingStep-def",
                  needs: [],
                  provides: ["failed"],
                  taskQueue: "standard-tasks",
                },
              ],
            },
          ],
          errorHandlerActivityName: "testWorkflow__errorHandler",
        };

        const workflowFn = createWorkflowFunction(plan);

        const input: WorkflowInput = {
          initialData: { initial: "data" },
        };

        const { error } = await runWorkflowExpectingResult(workflowFn, input);
        expect(error).toBeDefined();
        expect(error?.message).toContain("Batch 1 failed");

        // Error handler should receive bag with both initial data and successful step output
        const callArgs = errorHandlerActivity.mock.calls[0] as unknown[];
        const passedBag = callArgs[1] as Record<string, unknown>;
        expect(passedBag).toEqual({
          initial: "data",
          success: "value", // From successful step
        });
      });
    });

    // Everything in this block lands in Temporal event history at rest, where the injected
    // ComposerLogger never reaches and only a failure converter or payload codec can. A step
    // error's message is not Composer's to put there: for MikroORM/knex it is the
    // parameter-inlined SQL, quoting real row values.
    describe("Raw error text kept out of event history", () => {
      const inlinedSql =
        "insert into \"user\" (\"email\") values ('jane@example.com') - duplicate key";

      /** The whole failure as Temporal would persist it: message, type and every detail. */
      async function failureFrom(plan: WorkflowPlan): Promise<ApplicationFailure> {
        try {
          await createWorkflowFunction(plan)({ initialData: {} });
        } catch (err) {
          return err as ApplicationFailure;
        }
        throw new Error("workflow was expected to throw ApplicationFailure");
      }

      function planFor(steps: WorkflowPlan["batches"][number]["steps"]): WorkflowPlan {
        return { name: "leakyWorkflow", batches: [{ steps }] };
      }

      const failingStep = {
        name: "failingStep",
        activityName: "failingStep-abc",
        needs: [],
        provides: ["failure"],
        taskQueue: "standard-tasks",
      };

      beforeEach(() => {
        mocks.activityFunctions["failingStep-abc"] = vi.fn(async () => {
          throw Object.assign(new Error(inlinedSql), { code: "23505" });
        });
      });

      it("puts no step message anywhere in the thrown failure", async () => {
        const failure = await failureFrom(planFor([failingStep]));

        expect(JSON.stringify(failure.details)).not.toContain("jane@example.com");
        expect(JSON.stringify(failure.details)).not.toContain("insert into");
        expect(failure.message).not.toContain("insert into");
      });

      it("keeps the step's code so a failure is still classifiable", async () => {
        const failure = await failureFrom(planFor([failingStep]));

        const { error } = failure.details?.[0] as { error: TemporalWorkflowResult["error"] };
        expect(error?.errors?.[0]).toMatchObject({ stepName: "failingStep", code: "23505" });
      });

      it("carries the bag as its own details entry, not a key on the error", async () => {
        const okStep = {
          name: "okStep",
          activityName: "okStep-abc",
          needs: [],
          provides: ["ok"],
          taskQueue: "standard-tasks",
        };
        mocks.activityFunctions["okStep-abc"] = vi.fn(async () => ({ ok: "value" }));

        const failure = await failureFrom(planFor([okStep, failingStep]));

        // A scrubbing failure converter has to be able to drop the bag whole without
        // rebuilding the error that sits beside it.
        expect(failure.details?.[0]).not.toHaveProperty("bag");
        expect(failure.details?.[0]).toHaveProperty("error");
        expect(failure.details?.[1]).toMatchObject({ bag: { ok: "value" } });
      });

      // Temporal wraps an activity error ActivityFailure -> ApplicationFailure -> original,
      // and the chain is walked to depth 5. Dropping the message only at the top would
      // leave every message below it in the details.
      it("carries no message anywhere down the cause chain", async () => {
        mocks.activityFunctions["failingStep-abc"] = vi.fn(async () => {
          const root = new Error(inlinedSql);
          throw Object.assign(new Error("ActivityFailure"), {
            code: "WRAPPER",
            cause: Object.assign(new Error("ApplicationFailure"), {
              type: "23505",
              cause: root,
            }),
          });
        });

        const failure = await failureFrom(planFor([failingStep]));

        const { error } = failure.details?.[0] as { error: TemporalWorkflowResult["error"] };
        expect(error?.errors?.[0]?.cause).toBeDefined();
        expect(JSON.stringify(failure.details)).not.toContain("insert into");
        expect(JSON.stringify(failure.details)).not.toContain("ApplicationFailure");
        // The chain still classifies: the wrapper's type survives as a code.
        expect(JSON.stringify(failure.details)).toContain("23505");
      });

      // `code` is emitted where the message is withheld, so what may become a code matters.
      // A bare length cutoff turned any short `prefix: rest` message into a code.
      it("does not turn a message prefix into a code", async () => {
        mocks.activityFunctions["failingStep-abc"] = vi.fn(async () => {
          throw new Error('constraint "u_email": Key (email)=(jane@example.com) exists');
        });

        const failure = await failureFrom(planFor([failingStep]));

        const { error } = failure.details?.[0] as { error: TemporalWorkflowResult["error"] };
        expect(error?.errors?.[0]?.code).toBeUndefined();
        expect(JSON.stringify(failure.details)).not.toContain("u_email");
      });

      it("still recovers an identifier-shaped code from a LotiError message", async () => {
        mocks.activityFunctions["failingStep-abc"] = vi.fn(async () => {
          throw new Error("CONTENT_REJECTED: the reference image was rejected");
        });

        const failure = await failureFrom(planFor([failingStep]));

        const { error } = failure.details?.[0] as { error: TemporalWorkflowResult["error"] };
        expect(error?.errors?.[0]?.code).toBe("CONTENT_REJECTED");
      });

      it("does not repeat a crashed error handler's message", async () => {
        mocks.activityFunctions["errorHandler"] = vi.fn(async () => {
          throw Object.assign(new Error(inlinedSql), { code: "23505" });
        });

        const failure = await failureFrom({
          ...planFor([failingStep]),
          errorHandlerActivityName: "errorHandler",
        });

        expect(failure.message).not.toContain("insert into");
        expect(JSON.stringify(failure.details)).not.toContain("insert into");
        // The failure still says which handler broke and on which batch.
        expect(failure.type).toBe("WorkflowErrorHandlerFailure");
        expect(failure.message).toContain("batch 1");
      });
    });

    describe("FanOut Execution", () => {
      /** Helper to build a plan with a single FanOut batch entry. */
      function planWithFanOut(
        fanOut: FanOutBatchEntry,
        extraSteps: WorkflowPlan["batches"][number]["steps"] = [],
      ): WorkflowPlan {
        return {
          name: "fanout-workflow",
          batches: [
            {
              steps: extraSteps,
              fanOuts: [fanOut],
            },
          ],
        };
      }

      const baseFanOut: FanOutBatchEntry = {
        name: "processBatch",
        childWorkflowName: "child-wf-abc123",
        mapInputActivityName: "processBatch__mapInput-abc123",
        aggregateResultsActivityName: "processBatch__aggregateResults-abc123",
        needs: ["items"],
        provides: ["results"],
        concurrency: Infinity,
        taskQueue: "standard-tasks",
      };

      it("should call mapInput, executeChild for each input, and aggregateResults", async () => {
        const mapInputActivity = vi.fn(async () => [{ item: "a" }, { item: "b" }, { item: "c" }]);
        const aggregateResultsActivity = mockActivity(
          async (_wfInput, results: Array<{ processed: string }>) => ({
            results: results.map((r) => r.processed),
          }),
        );
        mocks.activityFunctions[baseFanOut.mapInputActivityName] = mapInputActivity;
        mocks.activityFunctions[baseFanOut.aggregateResultsActivityName] = aggregateResultsActivity;

        mocks.executeChild.mockImplementation(async (_type: string, opts?: Record<string, any>) => {
          const input = (opts?.args as WorkflowInput[])?.[0];
          const item = (input?.initialData as Record<string, string>)?.item;
          return { bag: { item, processed: item?.toUpperCase() }, error: undefined };
        });

        const plan = planWithFanOut(baseFanOut);
        const workflowFn = createWorkflowFunction(plan);

        const { bag, error } = await workflowFn({ initialData: { items: ["a", "b", "c"] } });

        expect(error).toBeUndefined();
        expect(bag.results).toEqual(["A", "B", "C"]);

        // Verify mapInput was called with the bag slice
        expect(mapInputActivity).toHaveBeenCalledOnce();
        const mapInputArgs = mapInputActivity.mock.calls[0] as unknown[];
        expect((mapInputArgs[1] as Record<string, unknown>).items).toEqual(["a", "b", "c"]);

        // Verify executeChild was called 3 times with correct child workflow name
        expect(mocks.executeChild).toHaveBeenCalledTimes(3);
        for (const call of mocks.executeChild.mock.calls) {
          expect(call[0]).toBe("child-wf-abc123");
        }

        // Verify deterministic child workflow IDs
        const childIds = mocks.executeChild.mock.calls.map(
          (call: unknown[]) => (call[1] as Record<string, string>).workflowId,
        );
        expect(childIds).toEqual([
          "test-workflow-id/processBatch/0",
          "test-workflow-id/processBatch/1",
          "test-workflow-id/processBatch/2",
        ]);

        // Verify aggregateResults was called with child bags
        expect(aggregateResultsActivity).toHaveBeenCalledOnce();
        const aggArgs = aggregateResultsActivity.mock.calls[0] as unknown[];
        const childBags = aggArgs[1] as Record<string, unknown>[];
        expect(childBags).toHaveLength(3);
      });

      it("should call aggregateResults with empty array when mapInput returns []", async () => {
        const mapInputActivity = vi.fn(async () => []);
        const aggregateResultsActivity = vi.fn(async () => ({ results: [] }));
        mocks.activityFunctions[baseFanOut.mapInputActivityName] = mapInputActivity;
        mocks.activityFunctions[baseFanOut.aggregateResultsActivityName] = aggregateResultsActivity;

        const plan = planWithFanOut(baseFanOut);
        const workflowFn = createWorkflowFunction(plan);

        const { bag, error } = await workflowFn({ initialData: { items: [] } });

        expect(error).toBeUndefined();
        expect(bag.results).toEqual([]);

        expect(mocks.executeChild).not.toHaveBeenCalled();
        expect(aggregateResultsActivity).toHaveBeenCalledOnce();
        const aggArgs = aggregateResultsActivity.mock.calls[0] as unknown[];
        expect(aggArgs[1]).toEqual([]);
      });

      it("should throw when a child workflow fails", async () => {
        const mapInputActivity = vi.fn(async () => [{ item: "a" }, { item: "b" }]);
        const aggregateResultsActivity = vi.fn(async () => ({ results: [] }));
        mocks.activityFunctions[baseFanOut.mapInputActivityName] = mapInputActivity;
        mocks.activityFunctions[baseFanOut.aggregateResultsActivityName] = aggregateResultsActivity;

        // First child succeeds, second throws ChildWorkflowFailure wrapping ApplicationFailure
        let callIndex = 0;
        mocks.executeChild.mockImplementation(async () => {
          const idx = callIndex++;
          if (idx === 1) {
            // Import the real classes from the mock module (spread from actual)
            const { ChildWorkflowFailure, ApplicationFailure: AppFailure } = await import(
              "@temporalio/workflow"
            );
            const appFailure = AppFailure.create({
              message: "child step failed",
              type: "WorkflowBatchError",
              nonRetryable: true,
              details: [
                {
                  error: {
                    message: "child step failed",
                    code: "WORKFLOW_BATCH_ERROR",
                    type: "WorkflowBatchError",
                  },
                },
              ],
            });
          throw new ChildWorkflowFailure(
            "default",
            { workflowId: "child-wf-abc123", runId: "child-run-id" },
            "child-wf-abc123",
            "NON_RETRYABLE_FAILURE",
            appFailure,
          );
          }
          return { bag: { item: "a", processed: "A" }, error: undefined };
        });

        const plan = planWithFanOut(baseFanOut);
        const workflowFn = createWorkflowFunction(plan);

        const { error } = await runWorkflowExpectingResult(workflowFn, {
          initialData: { items: ["a", "b"] },
        });

        expect(error).toBeDefined();
        // The fan-out error is caught by the batch-level allSettled and wrapped
        expect(error?.message).toContain("Batch 1 failed");
        expect(error?.message).toContain("processBatch");

        // aggregateResults should not be called when children fail
        expect(aggregateResultsActivity).not.toHaveBeenCalled();
      });

      // The failure summary is inlined into the thrown Error's message, which becomes a step
      // failure and so reaches event history. A child's raw message would be at rest twice.
      it("names failed children by code, never by message", async () => {
        mocks.activityFunctions[baseFanOut.mapInputActivityName] = vi.fn(async () => [
          { item: "a" },
        ]);
        mocks.activityFunctions[baseFanOut.aggregateResultsActivityName] = vi.fn(async () => ({
          results: [],
        }));

        const inlinedSql = "insert into \"user\" values ('jane@example.com')";
        mocks.executeChild.mockImplementation(async () => {
          const { ChildWorkflowFailure, ApplicationFailure: AppFailure } = await import(
            "@temporalio/workflow"
          );
          throw new ChildWorkflowFailure(
            "default",
            { workflowId: "child-wf", runId: "child-run" },
            "child-wf",
            "NON_RETRYABLE_FAILURE",
            AppFailure.create({
              message: inlinedSql,
              type: "WorkflowBatchError",
              nonRetryable: true,
              details: [{ error: { message: inlinedSql, code: "23505", type: "DbError" } }],
            }),
          );
        });

        let failure: ApplicationFailure | undefined;
        try {
          await createWorkflowFunction(planWithFanOut(baseFanOut))({
            initialData: { items: ["a"] },
          });
        } catch (err) {
          failure = err as ApplicationFailure;
        }

        const serialized = JSON.stringify([failure?.message, failure?.details]);
        expect(serialized).not.toContain("jane@example.com");
        expect(serialized).not.toContain("insert into");
        // The FanOut's own code, not one guessed from its message prefix.
        expect(serialized).toContain("FANOUT_CHILD_FAILURE");

        // The per-child codes stay on the workflow log line, which does not persist.
        expect(mocks.log.error).toHaveBeenCalledWith(
          "FanOut: child workflow failures",
          expect.objectContaining({ failures: "child 0: 23505" }),
        );
      });

      it("should extract structured error from ChildWorkflowFailure -> ApplicationFailure", async () => {
        const mapInputActivity = vi.fn(async () => [{ item: "a" }]);
        const aggregateResultsActivity = vi.fn(async () => ({ results: [] }));
        mocks.activityFunctions[baseFanOut.mapInputActivityName] = mapInputActivity;
        mocks.activityFunctions[baseFanOut.aggregateResultsActivityName] = aggregateResultsActivity;

        mocks.executeChild.mockImplementation(async () => {
          const { ChildWorkflowFailure, ApplicationFailure: AppFailure } = await import(
            "@temporalio/workflow"
          );
          const appFailure = AppFailure.create({
            message: "step blew up",
            type: "WorkflowBatchError",
            nonRetryable: true,
            details: [
              {
                error: {
                  message: "step blew up",
                  code: "CUSTOM_CODE",
                  type: "WorkflowBatchError",
                },
              },
            ],
          });
        throw new ChildWorkflowFailure(
          "default",
          { workflowId: "child-wf", runId: "run-id" },
          "child-wf",
          "NON_RETRYABLE_FAILURE",
          appFailure,
        );
        });

        const plan = planWithFanOut(baseFanOut);
        const workflowFn = createWorkflowFunction(plan);

        const { error } = await runWorkflowExpectingResult(workflowFn, {
          initialData: { items: ["a"] },
        });

        expect(error).toBeDefined();
        // The fan-out error is caught by the batch-level allSettled and wrapped
        expect(error?.message).toContain("Batch 1 failed");
        expect(error?.message).toContain("processBatch");

        // The error details should be preserved (via the errors array on the result)
        const resultError = error as unknown as Error & { errors?: Array<{ stepName: string }> };
        expect(resultError.errors).toBeDefined();
        expect(resultError.errors).toHaveLength(1);
        expect(resultError.errors![0]!.stepName).toBe("processBatch");
      });

      it("should execute fan-out alongside regular steps in the same batch", async () => {
        const regularActivity = vi.fn(async () => ({ stepOutput: "regular-done" }));
        mocks.activityFunctions["regularStep-abc123"] = regularActivity;

        const mapInputActivity = vi.fn(async () => [{ item: "x" }]);
        const aggregateResultsActivity = vi.fn(async () => ({ results: ["X"] }));
        mocks.activityFunctions[baseFanOut.mapInputActivityName] = mapInputActivity;
        mocks.activityFunctions[baseFanOut.aggregateResultsActivityName] = aggregateResultsActivity;

        mocks.executeChild.mockImplementation(async () => ({
          bag: { item: "x", processed: "X" },
          error: undefined,
        }));

        const plan = planWithFanOut(baseFanOut, [
          {
            name: "regularStep",
            activityName: "regularStep-abc123",
            needs: [],
            provides: ["stepOutput"],
            taskQueue: "standard-tasks",
          },
        ]);
        const workflowFn = createWorkflowFunction(plan);

        const { bag, error } = await workflowFn({ initialData: { items: ["x"] } });

        expect(error).toBeUndefined();
        expect(bag.stepOutput).toBe("regular-done");
        expect(bag.results).toEqual(["X"]);

        expect(regularActivity).toHaveBeenCalledOnce();
        expect(mocks.executeChild).toHaveBeenCalledOnce();
      });

      it("should respect concurrency limit for child workflows", async () => {
        const concurrencyLimit = 2;
        const fanOut: FanOutBatchEntry = { ...baseFanOut, concurrency: concurrencyLimit };

        const mapInputActivity = vi.fn(async () =>
          Array.from({ length: 5 }, (_, i) => ({ item: String(i) })),
        );
        const aggregateResultsActivity = vi.fn(async () => ({ results: [] }));
        mocks.activityFunctions[fanOut.mapInputActivityName] = mapInputActivity;
        mocks.activityFunctions[fanOut.aggregateResultsActivityName] = aggregateResultsActivity;

        // Track concurrency via a counter that increments/decrements around an async gap
        let maxConcurrent = 0;
        let currentConcurrent = 0;
        mocks.executeChild.mockImplementation(async () => {
          currentConcurrent++;
          maxConcurrent = Math.max(maxConcurrent, currentConcurrent);
          // Yield to allow other lanes to start if they can
          await new Promise((resolve) => setTimeout(resolve, 5));
          currentConcurrent--;
          return { bag: { done: true }, error: undefined };
        });

        const plan = planWithFanOut(fanOut);
        const workflowFn = createWorkflowFunction(plan);

        await workflowFn({ initialData: { items: [] } });

        // Verify all 5 children were executed
        expect(mocks.executeChild).toHaveBeenCalledTimes(5);
        // Concurrency should never exceed the limit
        expect(maxConcurrent).toBeLessThanOrEqual(concurrencyLimit);
      });

      it("should handle null concurrency from JSON round-trip (Infinity serializes to null)", async () => {
        const fanOutWithNull: FanOutBatchEntry = {
          ...baseFanOut,
          concurrency: null as unknown as number,
        };

        const mapInputActivity = vi.fn(async () => [{ item: "a" }, { item: "b" }]);
        const aggregateResultsActivity = mockActivity(
          async (_wfInput, results: Array<{ processed: string }>) => ({
            results: results.map((r) => r.processed),
          }),
        );
        mocks.activityFunctions[fanOutWithNull.mapInputActivityName] = mapInputActivity;
        mocks.activityFunctions[fanOutWithNull.aggregateResultsActivityName] =
          aggregateResultsActivity;

        mocks.executeChild.mockImplementation(async (_type: string, opts?: Record<string, any>) => {
          const input = (opts?.args as WorkflowInput[])?.[0];
          const item = (input?.initialData as Record<string, string>)?.item;
          return { bag: { item, processed: item?.toUpperCase() }, error: undefined };
        });

        const plan = planWithFanOut(fanOutWithNull);
        const workflowFn = createWorkflowFunction(plan);

        const { bag, error } = await workflowFn({ initialData: { items: ["a", "b"] } });

        expect(error).toBeUndefined();
        expect(bag.results).toEqual(["A", "B"]);
        expect(mocks.executeChild).toHaveBeenCalledTimes(2);
      });

      it("should merge fan-out output into bag for subsequent batches", async () => {
        const mapInputActivity = vi.fn(async () => [{ item: "hello" }]);
        const aggregateResultsActivity = vi.fn(async () => ({ results: ["HELLO"] }));
        mocks.activityFunctions[baseFanOut.mapInputActivityName] = mapInputActivity;
        mocks.activityFunctions[baseFanOut.aggregateResultsActivityName] = aggregateResultsActivity;

        mocks.executeChild.mockImplementation(async () => ({
          bag: { item: "hello", processed: "HELLO" },
          error: undefined,
        }));

        // Second batch step reads "results" produced by the fan-out
        const summarizeActivity = mockActivity(async (_wfInput, input: { results: string[] }) => ({
          summary: input.results.join(", "),
        }));
        mocks.activityFunctions["summarize-abc123"] = summarizeActivity;

        const plan: WorkflowPlan = {
          name: "multi-batch-fanout",
          batches: [
            {
              steps: [],
              fanOuts: [baseFanOut],
            },
            {
              steps: [
                {
                  name: "summarize",
                  activityName: "summarize-abc123",
                  needs: ["results"],
                  provides: ["summary"],
                  taskQueue: "standard-tasks",
                },
              ],
            },
          ],
        };
        const workflowFn = createWorkflowFunction(plan);

        const { bag, error } = await workflowFn({ initialData: { items: ["hello"] } });

        expect(error).toBeUndefined();
        expect(bag.results).toEqual(["HELLO"]);
        expect(bag.summary).toBe("HELLO");

        // Verify the summarize step received the fan-out output
        const summarizeArgs = summarizeActivity.mock.calls[0] as unknown[];
        expect((summarizeArgs[1] as Record<string, unknown>).results).toEqual(["HELLO"]);
      });
    });

    describe("Checkpoint Update Handler", () => {
      it("should register update handler when workflow has checkpoints", async () => {
        const mockActivity = vi.fn(async () => ({ result: "done" }));
        mocks.activityFunctions["step-abc"] = mockActivity;

        const plan: WorkflowPlan = {
          name: "checkpointWorkflow",
          batches: [
            {
              steps: [
                {
                  name: "step",
                  activityName: "step-abc",
                  needs: [],
                  provides: ["result"],
                  taskQueue: "standard-tasks",
                },
              ],
            },
          ],
          checkpoints: [{ name: "afterStep", afterBatch: 0, timeout: 30000 }],
        };

        const workflowFn = createWorkflowFunction(plan);

        const input: WorkflowInput = {
          initialData: {},
        };

        await workflowFn(input);

        // setHandler should have been called to register the checkpoint handler
        expect(mocks.setHandler).toHaveBeenCalledTimes(1);
      });

      it("should NOT register update handler when workflow has no checkpoints", async () => {
        const mockActivity = vi.fn(async () => ({ result: "done" }));
        mocks.activityFunctions["step-abc"] = mockActivity;

        const plan: WorkflowPlan = {
          name: "noCheckpointWorkflow",
          batches: [
            {
              steps: [
                {
                  name: "step",
                  activityName: "step-abc",
                  needs: [],
                  provides: ["result"],
                  taskQueue: "standard-tasks",
                },
              ],
            },
          ],
          // No checkpoints
        };

        const workflowFn = createWorkflowFunction(plan);

        const input: WorkflowInput = {
          initialData: {},
        };

        await workflowFn(input);

        // setHandler should NOT have been called
        expect(mocks.setHandler).not.toHaveBeenCalled();
      });

      it("should NOT register update handler when checkpoints array is empty", async () => {
        const mockActivity = vi.fn(async () => ({ result: "done" }));
        mocks.activityFunctions["step-abc"] = mockActivity;

        const plan: WorkflowPlan = {
          name: "emptyCheckpointWorkflow",
          batches: [
            {
              steps: [
                {
                  name: "step",
                  activityName: "step-abc",
                  needs: [],
                  provides: ["result"],
                  taskQueue: "standard-tasks",
                },
              ],
            },
          ],
          checkpoints: [], // Empty array
        };

        const workflowFn = createWorkflowFunction(plan);

        const input: WorkflowInput = {
          initialData: {},
        };

        await workflowFn(input);

        // setHandler should NOT have been called
        expect(mocks.setHandler).not.toHaveBeenCalled();
      });

      it("should mark checkpoints as reached after batch completes", async () => {
        const batch1Activity = vi.fn(async () => ({ data1: "value1" }));
        const batch2Activity = vi.fn(async () => ({ data2: "value2" }));

        mocks.activityFunctions["step1-abc"] = batch1Activity;
        mocks.activityFunctions["step2-def"] = batch2Activity;

        // Track when condition is called and what it checks
        const conditionCalls: Array<() => boolean> = [];
        mocks.condition.mockImplementation(async (fn: () => boolean) => {
          conditionCalls.push(fn);
          // Simulate immediate resolution for testing
        });

        const plan: WorkflowPlan = {
          name: "multiCheckpointWorkflow",
          batches: [
            {
              steps: [
                {
                  name: "step1",
                  activityName: "step1-abc",
                  needs: [],
                  provides: ["data1"],
                  taskQueue: "standard-tasks",
                },
              ],
            },
            {
              steps: [
                {
                  name: "step2",
                  activityName: "step2-def",
                  needs: ["data1"],
                  provides: ["data2"],
                  taskQueue: "standard-tasks",
                },
              ],
            },
          ],
          checkpoints: [
            { name: "checkpoint1", afterBatch: 0, timeout: 30000 },
            { name: "checkpoint2", afterBatch: 1, timeout: 30000 },
          ],
        };

        const workflowFn = createWorkflowFunction(plan);

        const input: WorkflowInput = {
          initialData: {},
        };

        await workflowFn(input);

        // Both activities should have been called (workflow completed)
        expect(batch1Activity).toHaveBeenCalled();
        expect(batch2Activity).toHaveBeenCalled();

        // setHandler should have been called to register the handler
        expect(mocks.setHandler).toHaveBeenCalled();
      });

      it("should mark checkpoints even when batch has failures", async () => {
        const successActivity = vi.fn(async () => ({ success: "value" }));
        const failingActivity = vi.fn(async () => {
          throw new Error("Step failed");
        });

        mocks.activityFunctions["successStep-abc"] = successActivity;
        mocks.activityFunctions["failingStep-def"] = failingActivity;

        const plan: WorkflowPlan = {
          name: "failingCheckpointWorkflow",
          batches: [
            {
              steps: [
                {
                  name: "successStep",
                  activityName: "successStep-abc",
                  needs: [],
                  provides: ["success"],
                  taskQueue: "standard-tasks",
                },
                {
                  name: "failingStep",
                  activityName: "failingStep-def",
                  needs: [],
                  provides: ["failure"],
                  taskQueue: "standard-tasks",
                },
              ],
            },
          ],
          checkpoints: [{ name: "checkpoint1", afterBatch: 0, timeout: 30000 }],
        };

        const workflowFn = createWorkflowFunction(plan);

        const input: WorkflowInput = {
          initialData: {},
        };

        const { error, bag } = await runWorkflowExpectingResult(workflowFn, input);

        // Workflow should have error
        expect(error).toBeDefined();

        // But the checkpoint handler should have been registered
        expect(mocks.setHandler).toHaveBeenCalled();

        // And the successful step's output should be in the bag
        expect(bag.success).toBe("value");
      });
    });
  });
});
