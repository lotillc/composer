/**
 * Tests for long-lived workflow support in the workflow factory: stage boundaries
 * (continue-as-new), resuming a continued run, durable signal waits, and the
 * workflow-declared versioning behavior.
 */

import { ApplicationFailure } from "@temporalio/workflow";
import { beforeEach, describe, expect, it, type MockedFunction, vi } from "vitest";

import { SIGNAL_WAIT_TIMEOUT_CODE } from "../../../workflow-codes.js";
import type {
  StepBatch,
  TemporalWorkflowResult,
  WorkflowContinuation,
  WorkflowInput,
  WorkflowPlan,
} from "../workflow-factory.js";
import { createWorkflowFunction } from "../workflow-factory.js";

type ActivityFn = (workflowInput: WorkflowInput, ...args: unknown[]) => Promise<unknown>;

/** Thrown by the mocked continue-as-new function in place of Temporal's own signal. */
class ContinuedAsNew extends Error {
  constructor(
    readonly options: Record<string, unknown>,
    readonly next: WorkflowInput,
  ) {
    super("continued as new");
  }
}

const mocks = vi.hoisted(() => ({
  log: { info: vi.fn(), debug: vi.fn(), error: vi.fn(), warn: vi.fn() },
  activityFunctions: {} as Record<string, MockedFunction<ActivityFn>>,
  setHandler: vi.fn(),
  defaultSignalHandler: undefined as
    | ((signalName: string, ...args: unknown[]) => void)
    | undefined,
  /** Called each time the workflow blocks on a condition; may deliver signals. */
  onBlock: undefined as ((timeoutMs: number | undefined) => void) | undefined,
  conditionTimeouts: [] as Array<number | undefined>,
  allHandlersFinished: vi.fn(() => true),
  continueAsNewOptions: [] as Array<Record<string, unknown>>,
  details: [] as string[],
  nowMs: 0,
}));

vi.mock("@temporalio/workflow", async (importOriginal) => {
  const actual = (await importOriginal()) as Record<string, unknown>;
  return {
    ...actual,
    get log() {
      return mocks.log;
    },
    proxyActivities: () => mocks.activityFunctions,
    workflowInfo: () => ({ workflowId: "wf-1", runId: "run-1", workflowType: "staged" }),
    defineUpdate: (name: string) => Symbol.for(name),
    get setHandler() {
      return mocks.setHandler;
    },
    setDefaultSignalHandler: (handler: (signalName: string, ...args: unknown[]) => void) => {
      mocks.defaultSignalHandler = handler;
    },
    setCurrentDetails: (details: string) => {
      mocks.details.push(details);
    },
    get allHandlersFinished() {
      return mocks.allHandlersFinished;
    },
    // A scripted stand-in for Temporal's condition: evaluate, let the test deliver
    // signals or advance the clock, then keep re-evaluating while other promises in the
    // workflow settle -- as Temporal does between microtasks -- before reporting a
    // timeout or giving up.
    condition: async (fn: () => boolean, timeoutMs?: number) => {
      if (fn()) return true;
      mocks.conditionTimeouts.push(timeoutMs);
      mocks.onBlock?.(timeoutMs);
      for (let tick = 0; tick < 50; tick++) {
        if (fn()) return true;
        await new Promise((resolve) => setTimeout(resolve, 0));
      }
      if (fn()) return true;
      if (timeoutMs === undefined) {
        throw new Error("test: workflow would block forever");
      }
      mocks.nowMs += timeoutMs;
      return false;
    },
    makeContinueAsNewFunc: (options: Record<string, unknown>) => {
      mocks.continueAsNewOptions.push(options);
      return async (next: WorkflowInput) => {
        throw new ContinuedAsNew(options, next);
      };
    },
  };
});

function activity(output: Record<string, unknown>): MockedFunction<ActivityFn> {
  return vi.fn(async () => output);
}

function step(name: string, needs: string[], provides: string[]): StepBatch["steps"][number] {
  return { name, activityName: name, needs, provides, taskQueue: "standard-tasks" };
}

/** a -> [boundary "second"] -> b -> [boundary "third"] -> c */
function stagedPlan(overrides: Partial<WorkflowPlan> = {}): WorkflowPlan {
  return {
    name: "staged",
    batches: [
      { steps: [step("a", ["id"], ["x"])] },
      { steps: [step("b", ["x"], ["y"])] },
      { steps: [step("c", ["y"], ["z"])] },
    ],
    stageBoundaries: [
      { name: "second", afterBatch: 0, initialVersioningBehavior: "AUTO_UPGRADE" },
      { name: "third", afterBatch: 1, initialVersioningBehavior: "AUTO_UPGRADE" },
    ],
    ...overrides,
  };
}

/** stagedPlan with a later-stage wait on "approval", so that signal name is declared. */
function stagedPlanWithWait(): WorkflowPlan {
  const plan = stagedPlan();
  plan.batches[2]!.signalWaits = [
    { name: "awaitApproval", signalName: "approval", provides: "approval", hasTimeoutValue: false },
  ];
  return plan;
}

async function runUntilContinued(
  plan: WorkflowPlan,
  input: WorkflowInput,
): Promise<ContinuedAsNew> {
  const result = await createWorkflowFunction(plan)(input).then(
    () => undefined,
    (err: unknown) => err,
  );
  expect(result).toBeInstanceOf(ContinuedAsNew);
  return result as ContinuedAsNew;
}

function continuation(overrides: Partial<WorkflowContinuation> = {}): WorkflowContinuation {
  return { stage: "second", bag: {}, completedSteps: [], signals: {}, ...overrides };
}

beforeEach(() => {
  vi.clearAllMocks();
  mocks.activityFunctions = {
    a: activity({ x: "X" }),
    b: activity({ y: "Y" }),
    c: activity({ z: "Z" }),
  };
  mocks.defaultSignalHandler = undefined;
  mocks.onBlock = undefined;
  mocks.conditionTimeouts = [];
  mocks.continueAsNewOptions = [];
  mocks.details = [];
  mocks.allHandlersFinished.mockReturnValue(true);
  mocks.nowMs = 0;
  vi.spyOn(Date, "now").mockImplementation(() => mocks.nowMs);
});

describe("versioning behavior", () => {
  it("declares the workflow's behavior on the workflow function", () => {
    const fn = createWorkflowFunction(stagedPlan({ versioningBehavior: "AUTO_UPGRADE" }));
    expect((fn as { workflowDefinitionOptions?: unknown }).workflowDefinitionOptions).toEqual({
      versioningBehavior: "AUTO_UPGRADE",
    });
  });

  it("declares nothing when the workflow leaves it to the worker default", () => {
    const fn = createWorkflowFunction(stagedPlan());
    expect(fn).not.toHaveProperty("workflowDefinitionOptions");
  });
});

describe("stage boundaries", () => {
  it("continues as new after the boundary batch, without running later batches", async () => {
    const continued = await runUntilContinued(stagedPlan(), { initialData: { id: "1" } });

    expect(mocks.activityFunctions.a).toHaveBeenCalledTimes(1);
    expect(mocks.activityFunctions.b).not.toHaveBeenCalled();
    expect(continued.options).toEqual({ initialVersioningBehavior: "AUTO_UPGRADE" });
    expect(continued.next).toEqual({
      initialData: { id: "1" },
      continuation: {
        version: 1,
        stage: "second",
        bag: { x: "X" },
        completedSteps: ["a"],
        signals: {},
      },
    });
  });

  it("sets no next-run versioning behavior when the plan carries none", async () => {
    const plan = stagedPlan();
    delete plan.stageBoundaries![0]!.initialVersioningBehavior;
    const continued = await runUntilContinued(plan, { initialData: { id: "1" } });
    expect(continued.options).toEqual({});
  });

  it("uses a boundary's own initial versioning behavior", async () => {
    const plan = stagedPlan();
    plan.stageBoundaries![0]!.initialVersioningBehavior = "USE_RAMPING_VERSION";
    const continued = await runUntilContinued(plan, { initialData: { id: "1" } });
    expect(continued.options).toEqual({ initialVersioningBehavior: "USE_RAMPING_VERSION" });
  });

  it("waits for in-flight handlers before continuing as new", async () => {
    await runUntilContinued(stagedPlan(), { initialData: { id: "1" } });
    // The mocked condition evaluates its predicate; allHandlersFinished is that predicate.
    expect(mocks.allHandlersFinished).toHaveBeenCalled();
  });

  it("carries only the listed produced fields when the boundary restricts carry", async () => {
    mocks.activityFunctions.a = activity({ x: "X", scratch: "big" });
    const plan = stagedPlan();
    plan.batches[0]!.steps[0]!.provides = ["x", "scratch"];
    plan.stageBoundaries![0]!.carry = ["x"];

    const continued = await runUntilContinued(plan, { initialData: { id: "1" } });
    expect(continued.next.continuation?.bag).toEqual({ x: "X" });
  });

  it("keeps metadata and environment on the next run", async () => {
    const continued = await runUntilContinued(stagedPlan(), {
      initialData: { id: "1" },
      metadata: { tenant: "t" },
      environment: "staging",
    });
    expect(continued.next.metadata).toEqual({ tenant: "t" });
    expect(continued.next.environment).toBe("staging");
  });

  it("does not continue as new when the boundary batch fails unhandled", async () => {
    mocks.activityFunctions.a = vi.fn(async () => {
      throw new Error("boom");
    });
    const outcome = await createWorkflowFunction(stagedPlan())({ initialData: { id: "1" } }).then(
      () => undefined,
      (err: unknown) => err,
    );
    expect(outcome).toBeInstanceOf(ApplicationFailure);
    expect(mocks.continueAsNewOptions).toHaveLength(0);
  });
});

describe("resuming a continued run", () => {
  it("resumes after the named stage with the carried bag", async () => {
    const continued = await runUntilContinued(stagedPlan(), {
      initialData: { id: "1" },
      continuation: continuation({ bag: { x: "X" }, completedSteps: ["a"] }),
    });

    expect(mocks.activityFunctions.a).not.toHaveBeenCalled();
    expect(mocks.activityFunctions.b).toHaveBeenCalledWith({ initialData: { id: "1" } }, { x: "X" });
    expect(continued.next.continuation).toEqual({
      version: 1,
      stage: "third",
      bag: { x: "X", y: "Y" },
      completedSteps: ["a", "b"],
      signals: {},
    });
  });

  it("finishes the workflow from the last stage", async () => {
    const result = (await createWorkflowFunction(stagedPlan())({
      initialData: { id: "1" },
      continuation: continuation({ stage: "third", bag: { x: "X", y: "Y" } }),
    })) as TemporalWorkflowResult;

    expect(result).toEqual({ bag: { id: "1", x: "X", y: "Y", z: "Z" }, error: undefined });
    expect(mocks.activityFunctions.c).toHaveBeenCalledTimes(1);
  });

  it("never re-runs a step an earlier run completed, even if the plan moved it", async () => {
    // A newer build moved "a" after the "second" boundary.
    const plan = stagedPlan();
    plan.batches[1]!.steps.push(step("a", ["id"], ["x"]));
    const continued = await runUntilContinued(plan, {
      initialData: { id: "1" },
      continuation: continuation({ bag: { x: "X" }, completedSteps: ["a"] }),
    });
    expect(mocks.activityFunctions.a).not.toHaveBeenCalled();
    expect(continued.next.continuation?.completedSteps).toEqual(["a", "b"]);
  });

  it("resumes a run that ended at a stage under an earlier name", async () => {
    const plan = stagedPlan();
    plan.stageBoundaries![0] = { ...plan.stageBoundaries![0]!, name: "renamed", aliases: ["second"] };

    const continued = await runUntilContinued(plan, {
      initialData: { id: "1" },
      continuation: continuation({ stage: "second", bag: { x: "X" }, completedSteps: ["a"] }),
    });

    expect(mocks.activityFunctions.a).not.toHaveBeenCalled();
    expect(mocks.activityFunctions.b).toHaveBeenCalledTimes(1);
    expect(continued.next.continuation?.stage).toBe("third");
  });

  it("fails the workflow task, not the workflow, on an unknown stage", async () => {
    const outcome = await createWorkflowFunction(stagedPlan())({
      initialData: { id: "1" },
      continuation: continuation({ stage: "renamed" }),
    }).then(
      () => undefined,
      (err: unknown) => err,
    );
    expect(outcome).toBeInstanceOf(Error);
    expect(outcome).not.toBeInstanceOf(ApplicationFailure);
    expect((outcome as Error).message).toContain('unknown stage "renamed"');
  });

  it("refuses, without blocking, a checkpoint a predecessor run already passed", async () => {
    const plan = stagedPlan({
      checkpoints: [{ name: "started", afterBatch: 0, timeout: 30_000 }],
    });
    let handler: ((args: { checkpointName: string }) => Promise<unknown>) | undefined;
    mocks.setHandler.mockImplementation((_def: unknown, fn: typeof handler) => {
      handler = fn;
    });

    const run = createWorkflowFunction(plan)({
      initialData: { id: "1" },
      continuation: continuation({ bag: { x: "X" }, completedSteps: ["a"] }),
    });
    // Rejects at once: the mocked condition would throw "block forever" if it had to wait.
    await expect(handler!({ checkpointName: "started" })).rejects.toMatchObject({
      type: "COMPOSER_CHECKPOINT_PASSED",
    });
    await run.catch(() => undefined);
  });

  it("never passes the continuation to activities", async () => {
    await runUntilContinued(stagedPlan(), {
      initialData: { id: "1" },
      continuation: continuation({ bag: { x: "X" }, completedSteps: ["a"] }),
    });
    const [workflowInput] = mocks.activityFunctions.b!.mock.calls[0]!;
    expect(workflowInput).not.toHaveProperty("continuation");
  });
});

describe("signal waits", () => {
  function waitPlan(wait: Partial<NonNullable<StepBatch["signalWaits"]>[number]> = {}) {
    return {
      name: "waiting",
      batches: [
        { steps: [step("a", ["id"], ["x"])] },
        {
          steps: [],
          signalWaits: [
            {
              name: "awaitApproval",
              signalName: "approval",
              provides: "approval",
              hasTimeoutValue: false,
              ...wait,
            },
          ],
        },
        { steps: [step("b", ["approval"], ["y"])] },
      ],
    } satisfies WorkflowPlan;
  }

  it("provides the payload of a signal delivered while waiting", async () => {
    mocks.onBlock = () => mocks.defaultSignalHandler!("approval", { ok: true });
    const result = (await createWorkflowFunction(waitPlan())({
      initialData: { id: "1" },
    })) as TemporalWorkflowResult;

    expect(result.bag.approval).toEqual({ ok: true });
    expect(mocks.activityFunctions.b).toHaveBeenCalledWith(expect.anything(), {
      approval: { ok: true },
    });
  });

  it("consumes a signal that arrived before the wait began", async () => {
    mocks.activityFunctions.a = vi.fn(async () => {
      mocks.defaultSignalHandler!("approval", "early");
      return { x: "X" };
    });
    const result = (await createWorkflowFunction(waitPlan())({
      initialData: { id: "1" },
    })) as TemporalWorkflowResult;
    expect(result.bag.approval).toBe("early");
    expect(mocks.conditionTimeouts).toEqual([]);
  });

  it("waits on a durable timer bounded by the timeout", async () => {
    mocks.onBlock = () => mocks.defaultSignalHandler!("approval", "late");
    await createWorkflowFunction(waitPlan({ timeout: "21 days" }))({ initialData: { id: "1" } });
    expect(mocks.conditionTimeouts).toEqual([21 * 86_400_000]);
  });

  it("uses the timeout value when no signal arrives", async () => {
    const result = (await createWorkflowFunction(
      waitPlan({ timeout: "1 day", hasTimeoutValue: true, timeoutValue: { ok: false } }),
    )({ initialData: { id: "1" } })) as TemporalWorkflowResult;
    expect(result.bag.approval).toEqual({ ok: false });
  });

  it("fails with SIGNAL_WAIT_TIMEOUT when no signal arrives and there is no fallback", async () => {
    const outcome = await createWorkflowFunction(waitPlan({ timeout: "1 day" }))({
      initialData: { id: "1" },
    }).then(
      () => undefined,
      (err: unknown) => err,
    );
    expect(outcome).toBeInstanceOf(ApplicationFailure);
    const detail = (outcome as ApplicationFailure).details?.[0] as {
      error: TemporalWorkflowResult["error"];
    };
    expect(detail.error?.errors?.[0]).toMatchObject({
      stepName: "awaitApproval",
      code: SIGNAL_WAIT_TIMEOUT_CODE,
    });
    expect(mocks.activityFunctions.b).not.toHaveBeenCalled();
  });

  it("fails the node, not the task, on a hand-built plan with a non-positive timeout", async () => {
    const outcome = await createWorkflowFunction(waitPlan({ timeout: "0s" }))({
      initialData: { id: "1" },
    }).then(
      () => undefined,
      (err: unknown) => err,
    );
    expect(outcome).toBeInstanceOf(ApplicationFailure);
    const detail = (outcome as ApplicationFailure).details?.[0] as {
      error: TemporalWorkflowResult["error"];
    };
    expect(detail.error?.errors?.[0]).toMatchObject({
      stepName: "awaitApproval",
      code: "COMPOSER_INVALID_SIGNAL_WAIT",
    });
  });

  it("routes a wait timeout to the error handler", async () => {
    mocks.activityFunctions["waiting__errorHandler"] = vi.fn(async () => ({
      handled: true,
      bag: { approval: "fallback" },
    }));
    const plan: WorkflowPlan = {
      ...waitPlan({ timeout: "1 day" }),
      errorHandlerActivityName: "waiting__errorHandler",
    };
    const result = (await createWorkflowFunction(plan)({
      initialData: { id: "1" },
    })) as TemporalWorkflowResult;
    expect(result.error).toBeUndefined();
    expect(result.bag.approval).toBe("fallback");
  });

  it("does not install a signal handler for workflows without waits or stages", async () => {
    await createWorkflowFunction({ name: "plain", batches: [{ steps: [step("a", [], ["x"])] }] })({
      initialData: {},
    });
    expect(mocks.defaultSignalHandler).toBeUndefined();
  });

  it("carries unconsumed declared signals across continue-as-new", async () => {
    mocks.activityFunctions.a = vi.fn(async () => {
      mocks.defaultSignalHandler!("approval", "early");
      return { x: "X" };
    });
    const continued = await runUntilContinued(stagedPlanWithWait(), { initialData: { id: "1" } });
    expect(continued.next.continuation?.signals).toEqual({ approval: ["early"] });
  });

  it("drops, with a warning, a signal no wait declares", async () => {
    mocks.activityFunctions.a = vi.fn(async () => {
      mocks.defaultSignalHandler!("not-declared", 42);
      return { x: "X" };
    });
    const continued = await runUntilContinued(stagedPlanWithWait(), { initialData: { id: "1" } });
    expect(continued.next.continuation?.signals).toEqual({});
    expect(mocks.log.warn).toHaveBeenCalledWith(
      "Dropped a signal no wait in this workflow declares",
      { signalName: "not-declared" },
    );
  });

  it("keeps a carried queue this build no longer declares", async () => {
    const continued = await runUntilContinued(stagedPlanWithWait(), {
      initialData: { id: "1" },
      continuation: continuation({
        bag: { x: "X" },
        completedSteps: ["a"],
        signals: { retired: ["kept"] },
      }),
    });
    expect(continued.next.continuation?.signals).toEqual({ retired: ["kept"] });
  });

  it("never confuses two different name and key pairs that join to the same text", async () => {
    const plan = stagedPlanWithWait();
    plan.batches[2]!.signalWaits!.push({
      name: "awaitOther",
      signalName: "approval:b",
      provides: "other",
      hasTimeoutValue: false,
    });
    mocks.activityFunctions.a = vi.fn(async () => {
      mocks.defaultSignalHandler!("approval", "first", { dedupeKey: "b:c" });
      mocks.defaultSignalHandler!("approval:b", "second", { dedupeKey: "c" });
      return { x: "X" };
    });
    const continued = await runUntilContinued(plan, { initialData: { id: "1" } });
    expect(continued.next.continuation?.signals).toEqual({
      approval: ["first"],
      "approval:b": ["second"],
    });
  });

  it("drops a second signal with the same dedupe key, across stages", async () => {
    mocks.activityFunctions.a = vi.fn(async () => {
      mocks.defaultSignalHandler!("approval", "first", { dedupeKey: "evt-1" });
      mocks.defaultSignalHandler!("approval", "retry", { dedupeKey: "evt-1" });
      mocks.defaultSignalHandler!("approval", "other", { dedupeKey: "evt-2" });
      return { x: "X" };
    });
    const first = await runUntilContinued(stagedPlanWithWait(), { initialData: { id: "1" } });
    expect(first.next.continuation?.signals).toEqual({ approval: ["first", "other"] });
    expect(first.next.continuation?.seenSignalKeys).toEqual([
      '["approval","evt-1"]',
      '["approval","evt-2"]',
    ]);

    // The next run still remembers the key.
    mocks.activityFunctions.b = vi.fn(async () => {
      mocks.defaultSignalHandler!("approval", "late retry", { dedupeKey: "evt-1" });
      return { y: "Y" };
    });
    const second = await runUntilContinued(stagedPlanWithWait(), first.next);
    expect(second.next.continuation?.signals).toEqual({ approval: ["first", "other"] });
  });

  it("resumes with carried signals available to a wait", async () => {
    const plan: WorkflowPlan = {
      ...waitPlan(),
      stageBoundaries: [{ name: "waiting", afterBatch: 0, initialVersioningBehavior: "AUTO_UPGRADE" }],
    };
    const result = (await createWorkflowFunction(plan)({
      initialData: { id: "1" },
      continuation: continuation({
        stage: "waiting",
        bag: { x: "X" },
        completedSteps: ["a"],
        signals: { approval: ["carried"] },
      }),
    })) as TemporalWorkflowResult;
    expect(result.bag.approval).toBe("carried");
  });

  it("caps each signal's pending queue at its wait's limit, dropping the oldest", async () => {
    const plan = stagedPlanWithWait();
    plan.batches[2]!.signalWaits![0]!.maxPendingSignals = 3;
    mocks.activityFunctions.a = vi.fn(async () => {
      for (let i = 0; i < 5; i++) mocks.defaultSignalHandler!("approval", i);
      return { x: "X" };
    });
    const continued = await runUntilContinued(plan, { initialData: { id: "1" } });
    expect(continued.next.continuation?.signals?.approval).toEqual([2, 3, 4]);
    expect(mocks.log.warn).toHaveBeenCalledWith(
      "Signal inbox full; dropped the oldest pending payload",
      { signalName: "approval", limit: 3 },
    );
  });

  it("caps at 100 by default", async () => {
    mocks.activityFunctions.a = vi.fn(async () => {
      for (let i = 0; i < 105; i++) mocks.defaultSignalHandler!("approval", i);
      return { x: "X" };
    });
    const continued = await runUntilContinued(stagedPlanWithWait(), { initialData: { id: "1" } });
    const queue = continued.next.continuation?.signals?.approval ?? [];
    expect(queue).toHaveLength(100);
    expect(queue[0]).toBe(5);
  });
});

describe("continuation format", () => {
  it("resumes from a minimal continuation, defaulting every optional field", async () => {
    const continued = await runUntilContinued(stagedPlan(), {
      initialData: { id: "1" },
      continuation: { stage: "second" },
    });
    expect(mocks.activityFunctions.b).toHaveBeenCalledTimes(1);
    expect(continued.next.continuation?.completedSteps).toEqual(["b"]);
  });

  it("resumes from a newer build's continuation, ignoring fields it does not know", async () => {
    await runUntilContinued(stagedPlan(), {
      initialData: { id: "1" },
      continuation: { version: 2, stage: "second", bag: { x: "X" }, futureField: true } as never,
    });
    expect(mocks.activityFunctions.b).toHaveBeenCalledWith(expect.anything(), { x: "X" });
    expect(mocks.log.warn).toHaveBeenCalledWith(
      "Resuming from a continuation written by a newer build",
      { version: 2, supported: 1 },
    );
  });

  it("carries an optional input field a step later provides, so it does not revert", async () => {
    // Callers may pass any bag field; only required and configured ones are protected.
    mocks.activityFunctions.a = activity({ x: "from step" });
    const continued = await runUntilContinued(stagedPlan(), { initialData: { id: "1", x: "from caller" } });
    expect(continued.next.continuation?.bag).toEqual({ x: "from step" });

    mocks.activityFunctions.b = vi.fn(async (_input, stepInput) => ({ y: stepInput }));
    const second = await runUntilContinued(stagedPlan(), continued.next);
    expect(second.next.continuation?.bag?.y).toEqual({ x: "from step" });
  });

  it("carries an initial field the workflow changed, so it does not revert", async () => {
    mocks.activityFunctions["staged__errorHandler"] = vi.fn(async () => ({
      handled: true,
      bag: { id: "corrected" },
    }));
    mocks.activityFunctions.a = vi.fn(async () => {
      throw new Error("boom");
    });
    const plan = stagedPlan({ errorHandlerActivityName: "staged__errorHandler" });
    const continued = await runUntilContinued(plan, { initialData: { id: "1" } });
    expect(continued.next.initialData).toEqual({ id: "1" });
    expect(continued.next.continuation?.bag).toEqual({ id: "corrected" });
  });
});

describe("waits beside failures", () => {
  function batchWithWait(wait: Partial<NonNullable<StepBatch["signalWaits"]>[number]> = {}) {
    return {
      name: "mixed",
      batches: [
        {
          steps: [step("fails", ["id"], ["x"])],
          signalWaits: [
            {
              name: "awaitApproval",
              signalName: "approval",
              provides: "approval",
              hasTimeoutValue: false,
              ...wait,
            },
          ],
        },
        { steps: [step("after", ["approval"], ["y"])] },
      ],
    } satisfies WorkflowPlan;
  }

  function failureOf(outcome: unknown) {
    expect(outcome).toBeInstanceOf(ApplicationFailure);
    return ((outcome as ApplicationFailure).details?.[0] as { error: TemporalWorkflowResult["error"] })
      .error;
  }

  beforeEach(() => {
    mocks.activityFunctions.fails = vi.fn(async () => {
      throw new Error("boom");
    });
  });

  it("fails the batch on a sibling failure instead of waiting forever on a signal", async () => {
    const outcome = await createWorkflowFunction(batchWithWait())({ initialData: { id: "1" } }).then(
      () => undefined,
      (err: unknown) => err,
    );
    const error = failureOf(outcome);
    expect(error?.errors?.map((e) => e.stepName)).toEqual(["fails"]);
    expect(mocks.activityFunctions.after).toBeUndefined();
    expect(mocks.log.warn).toHaveBeenCalledWith(
      "Released signal waits because their batch failed",
      expect.objectContaining({ releasedWaits: ["awaitApproval"] }),
    );
  });

  it("surfaces a wait's own timeout beside another wait with no timeout", async () => {
    const plan: WorkflowPlan = {
      name: "two-waits",
      batches: [
        {
          steps: [],
          signalWaits: [
            { name: "bounded", signalName: "a", provides: "a", timeout: "1 day", hasTimeoutValue: false },
            { name: "unbounded", signalName: "b", provides: "b", hasTimeoutValue: false },
          ],
        },
      ],
    };
    const outcome = await createWorkflowFunction(plan)({ initialData: {} }).then(
      () => undefined,
      (err: unknown) => err,
    );
    expect(failureOf(outcome)?.errors).toEqual([
      expect.objectContaining({ stepName: "bounded", code: SIGNAL_WAIT_TIMEOUT_CODE }),
    ]);
  });

  it("waits again on a released wait when the failure is handled, before moving on", async () => {
    mocks.activityFunctions["mixed__errorHandler"] = vi.fn(async () => ({ handled: true }));
    mocks.activityFunctions.after = vi.fn(async (_input, stepInput) => ({ y: stepInput }));
    let blocks = 0;
    // First block: the sibling fails and releases the wait. Second: the signal arrives.
    mocks.onBlock = () => {
      blocks++;
      if (blocks === 2) mocks.defaultSignalHandler!("approval", { ok: true });
    };
    const plan: WorkflowPlan = { ...batchWithWait(), errorHandlerActivityName: "mixed__errorHandler" };

    const result = (await createWorkflowFunction(plan)({
      initialData: { id: "1" },
    })) as TemporalWorkflowResult;

    expect(result.error).toBeUndefined();
    expect(mocks.activityFunctions.after).toHaveBeenCalledWith(expect.anything(), {
      approval: { ok: true },
    });
  });

  it("keeps a re-run wait's original deadline", async () => {
    mocks.activityFunctions["mixed__errorHandler"] = vi.fn(async () => ({ handled: true }));
    mocks.activityFunctions.fails = vi.fn(async () => {
      await new Promise((resolve) => setTimeout(resolve, 0)); // let the wait start first
      mocks.nowMs += 3 * 3_600_000; // the failing sibling took three hours
      throw new Error("boom");
    });
    const plan: WorkflowPlan = {
      ...batchWithWait({ timeout: "1 day", hasTimeoutValue: true, timeoutValue: "none" }),
      errorHandlerActivityName: "mixed__errorHandler",
    };
    mocks.activityFunctions.after = activity({ y: "Y" });

    await createWorkflowFunction(plan)({ initialData: { id: "1" } });

    expect(mocks.conditionTimeouts).toEqual([86_400_000, 75_600_000]);
  });
});

describe("current details", () => {
  it("shows the stage and the signal being waited on", async () => {
    const plan: WorkflowPlan = {
      name: "waiting",
      batches: [
        { steps: [step("a", ["id"], ["x"])] },
        {
          steps: [],
          signalWaits: [
            { name: "awaitApproval", signalName: "approval", provides: "approval", hasTimeoutValue: false },
          ],
        },
      ],
      stageBoundaries: [{ name: "waiting", afterBatch: 0, initialVersioningBehavior: "AUTO_UPGRADE" }],
    };
    mocks.onBlock = () => mocks.defaultSignalHandler!("approval", { ok: true });
    await createWorkflowFunction(plan)({
      initialData: { id: "1" },
      continuation: continuation({ stage: "waiting", bag: { x: "X" }, completedSteps: ["a"] }),
    });
    expect(mocks.details).toEqual([
      "Stage: waiting",
      'Stage: waiting\nWaiting for signal: "approval"',
      "Stage: waiting",
    ]);
  });

  it("logs an unknown stage as an error before failing the task", async () => {
    await createWorkflowFunction(stagedPlan())({
      initialData: { id: "1" },
      continuation: continuation({ stage: "gone" }),
    }).catch(() => undefined);
    expect(mocks.log.error).toHaveBeenCalledWith("Cannot resume at unknown stage", {
      workflowName: "staged",
      stage: "gone",
      knownStages: "second, third",
    });
  });
});
