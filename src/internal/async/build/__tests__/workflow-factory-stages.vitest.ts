/**
 * Tests for long-lived workflow support in the workflow factory: stage boundaries
 * (continue-as-new), resuming a continued run, durable signal waits, and the
 * workflow-declared versioning behavior.
 */

import { ApplicationFailure } from "@temporalio/workflow";
import { beforeEach, describe, expect, it, type MockedFunction, vi } from "vitest";

import { CONTINUATION_TOO_LARGE_CODE, SIGNAL_WAIT_TIMEOUT_CODE } from "../../../workflow-codes.js";
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

/** A batch holding one signal wait, which is how every plan places a wait. */
function waitBatch(name: string, signalName: string, provides: string): StepBatch {
  return { steps: [], signalWaits: [{ name, signalName, provides, hasTimeoutValue: false }] };
}

/** stagedPlan with a final wait on "approval", so that signal name is declared. */
function stagedPlanWithWait(): WorkflowPlan {
  const plan = stagedPlan();
  plan.batches.push(waitBatch("awaitApproval", "approval", "approval"));
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

  it("carries every produced field into the next run", async () => {
    mocks.activityFunctions.a = activity({ x: "X", scratch: "kept" });
    const plan = stagedPlan();
    plan.batches[0]!.steps[0]!.provides = ["x", "scratch"];

    const continued = await runUntilContinued(plan, { initialData: { id: "1" } });
    expect(continued.next.continuation?.bag).toEqual({ x: "X", scratch: "kept" });
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
    plan.batches.push(waitBatch("awaitOther", "approval:b", "other"));
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
    plan.batches[3]!.signalWaits![0]!.maxPendingSignals = 3;
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

  it("trims a carried queue to a cap this build lowered", async () => {
    const plan = stagedPlanWithWait();
    plan.batches[3]!.signalWaits![0]!.maxPendingSignals = 2;
    const continued = await runUntilContinued(plan, {
      initialData: { id: "1" },
      continuation: continuation({
        bag: { x: "X" },
        completedSteps: ["a"],
        signals: { approval: [1, 2, 3, 4, 5], retired: [1, 2, 3] },
      }),
    });
    expect(continued.next.continuation?.signals).toEqual({
      approval: [4, 5],
      retired: [1, 2, 3],
    });
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

  it("ignores fields it does not know in a continuation of its own version", async () => {
    await runUntilContinued(stagedPlan(), {
      initialData: { id: "1" },
      continuation: { version: 1, stage: "second", bag: { x: "X" }, futureField: true } as never,
    });
    expect(mocks.activityFunctions.b).toHaveBeenCalledWith(expect.anything(), { x: "X" });
  });

  it("fails the workflow task, not the workflow, on a newer format version", async () => {
    const outcome = await createWorkflowFunction(stagedPlan())({
      initialData: { id: "1" },
      continuation: { version: 2, stage: "second", bag: { x: "X" } },
    }).then(
      () => undefined,
      (err: unknown) => err,
    );
    expect(outcome).toBeInstanceOf(Error);
    expect(outcome).not.toBeInstanceOf(ApplicationFailure);
    expect(mocks.activityFunctions.b).not.toHaveBeenCalled();
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

describe("a wait alone in its batch", () => {
  it.each<[string, StepBatch]>([
    ["a step", { steps: [step("other", [], ["x"])] }],
    [
      "a second wait",
      { steps: [], signalWaits: [{ name: "w2", signalName: "b", provides: "b", hasTimeoutValue: false }] },
    ],
  ])("refuses a hand-built plan that puts a wait beside %s", async (_label, extra) => {
    const plan: WorkflowPlan = {
      name: "mixed",
      batches: [
        {
          steps: extra.steps,
          signalWaits: [
            { name: "w1", signalName: "a", provides: "a", hasTimeoutValue: false },
            ...(extra.signalWaits ?? []),
          ],
        },
      ],
    };
    const outcome = await createWorkflowFunction(plan)({ initialData: {} }).then(
      () => undefined,
      (err: unknown) => err,
    );
    expect(outcome).toBeInstanceOf(ApplicationFailure);
    expect((outcome as ApplicationFailure).type).toBe("COMPOSER_INVALID_SIGNAL_WAIT");
    expect(mocks.conditionTimeouts).toEqual([]);
  });
});

describe("carried state limits", () => {
  it("fails, without continuing as new, when the carried state is over budget", async () => {
    mocks.activityFunctions.a = activity({ x: "a".repeat(1_600_000) });
    const outcome = await createWorkflowFunction(stagedPlan())({ initialData: { id: "1" } }).then(
      () => undefined,
      (err: unknown) => err,
    );
    expect(outcome).toBeInstanceOf(ApplicationFailure);
    expect((outcome as ApplicationFailure).type).toBe(CONTINUATION_TOO_LARGE_CODE);
    // Same layout as a failed batch, so the client reports the code.
    expect((outcome as ApplicationFailure).details?.[0]).toMatchObject({
      error: { code: CONTINUATION_TOO_LARGE_CODE, type: CONTINUATION_TOO_LARGE_CODE },
    });
    // The oversized state stays out of the failure, which has the same size cap.
    expect(JSON.stringify((outcome as ApplicationFailure).details).length).toBeLessThan(1_000);
    expect(mocks.continueAsNewOptions).toHaveLength(0);
  });

  it("counts multi-byte characters by their UTF-8 size", async () => {
    // 400,000 three-byte characters: 1.2 MB as UTF-8, though only 400,000 UTF-16 units.
    mocks.activityFunctions.a = activity({ x: "€".repeat(400_000) });
    await runUntilContinued(stagedPlan(), { initialData: { id: "1" } });
    mocks.activityFunctions.a = activity({ x: "€".repeat(550_000) });
    const outcome = await createWorkflowFunction(stagedPlan())({ initialData: { id: "1" } }).catch(
      (err: unknown) => err,
    );
    expect((outcome as ApplicationFailure).type).toBe(CONTINUATION_TOO_LARGE_CODE);
  });

  it("measures bigint, Map and Set values instead of skipping the budget", async () => {
    // A custom converter can carry these; JSON.stringify alone would throw on the bigint.
    mocks.activityFunctions.a = activity({
      x: { big: 10n ** 30n, map: new Map([["k", "a".repeat(800_000)]]), set: new Set(["b".repeat(800_000)]) },
    });
    const outcome = await createWorkflowFunction(stagedPlan())({ initialData: { id: "1" } }).catch(
      (err: unknown) => err,
    );
    expect((outcome as ApplicationFailure).type).toBe(CONTINUATION_TOO_LARGE_CODE);
  });

  it("fails clearly when the carried state cannot be measured at all", async () => {
    const cyclic: Record<string, unknown> = {};
    cyclic.self = cyclic;
    mocks.activityFunctions.a = activity({ x: cyclic });
    const outcome = await createWorkflowFunction(stagedPlan())({ initialData: { id: "1" } }).catch(
      (err: unknown) => err,
    );
    expect((outcome as ApplicationFailure).type).toBe(CONTINUATION_TOO_LARGE_CODE);
    expect((outcome as ApplicationFailure).details?.[0]).toMatchObject({
      error: { data: { measurable: false } },
    });
    expect(mocks.continueAsNewOptions).toHaveLength(0);
  });

  it("warns when the carried state is large but within budget", async () => {
    mocks.activityFunctions.a = activity({ x: "a".repeat(600_000) });
    await runUntilContinued(stagedPlan(), { initialData: { id: "1" } });
    expect(mocks.log.warn).toHaveBeenCalledWith(
      "Carried state is large; keep big data out of the bag",
      expect.objectContaining({ stage: "second" }),
    );
  });

  it("carries a field and a signal named __proto__ as data", async () => {
    const plan = stagedPlanWithWait();
    plan.batches.push(waitBatch("awaitProto", "__proto__", "other"));
    mocks.activityFunctions.a = vi.fn(async () => {
      mocks.defaultSignalHandler!("__proto__", "early");
      return JSON.parse('{"x": "X", "__proto__": "kept"}') as Record<string, unknown>;
    });
    const continued = await runUntilContinued(plan, { initialData: { id: "1" } });
    const continuationJson = JSON.parse(JSON.stringify(continued.next.continuation));
    expect(Object.hasOwn(continuationJson.signals, "__proto__")).toBe(true);
    expect(continuationJson.signals.__proto__).toEqual(["early"]);
  });

  it("keeps a step output named __proto__ in the bag, for later steps and the next run", async () => {
    mocks.activityFunctions.a = vi.fn(async () =>
      JSON.parse('{"x": "X", "__proto__": "kept"}') as Record<string, unknown>,
    );
    const plan = stagedPlan();
    plan.batches[0]!.steps[0]!.provides = ["x", "__proto__"];
    plan.batches[1]!.steps[0]!.needs = ["x", "__proto__"];
    plan.stageBoundaries = [];
    mocks.activityFunctions.b = vi.fn(async (_input, stepInput) => ({ y: stepInput }));

    const result = (await createWorkflowFunction(plan)({
      initialData: { id: "1" },
    })) as TemporalWorkflowResult;

    const stepInput = JSON.parse(JSON.stringify(result.bag.y));
    expect(Object.hasOwn(stepInput, "__proto__")).toBe(true);
    expect(stepInput.__proto__).toBe("kept");
    expect(Object.hasOwn(result.bag, "__proto__")).toBe(true);
  });

  it("counts a dedupe key's size as escaped JSON bytes, not characters", async () => {
    // 50 control characters: 50 characters, but 302 bytes once JSON escapes them.
    const escaped = "\u0001".repeat(50);
    mocks.activityFunctions.a = vi.fn(async () => {
      mocks.defaultSignalHandler!("approval", "first", { dedupeKey: escaped });
      return { x: "X" };
    });
    const continued = await runUntilContinued(stagedPlanWithWait(), { initialData: { id: "1" } });
    expect(continued.next.continuation).not.toHaveProperty("seenSignalKeys");
    expect(continued.next.continuation?.signals?.approval).toEqual(["first"]);
  });

  it("ignores an over-long dedupe key and accepts the signal without it", async () => {
    mocks.activityFunctions.a = vi.fn(async () => {
      mocks.defaultSignalHandler!("approval", "first", { dedupeKey: "k".repeat(257) });
      mocks.defaultSignalHandler!("approval", "second", { dedupeKey: "k".repeat(257) });
      return { x: "X" };
    });
    const continued = await runUntilContinued(stagedPlanWithWait(), { initialData: { id: "1" } });
    expect(continued.next.continuation?.signals?.approval).toEqual(["first", "second"]);
    expect(continued.next.continuation).not.toHaveProperty("seenSignalKeys");
  });
});

describe("checkpoint requests the workflow cannot answer", () => {
  it("fails only the request, not the workflow task, for an unknown checkpoint", async () => {
    let handler: ((args: { checkpointName: string }) => Promise<unknown>) | undefined;
    mocks.setHandler.mockImplementation((_def: unknown, fn: typeof handler) => {
      handler = fn;
    });
    const plan = stagedPlan({ checkpoints: [{ name: "started", afterBatch: 0, timeout: 30_000 }] });
    const run = createWorkflowFunction(plan)({ initialData: { id: "1" } }).catch(() => undefined);

    // An ApplicationFailure fails the Update alone; a plain Error would fail the task and
    // stall the run on every retry.
    const outcome = await handler!({ checkpointName: "renamed" }).catch((err: unknown) => err);
    expect(outcome).toBeInstanceOf(ApplicationFailure);
    expect((outcome as ApplicationFailure).type).toBe("COMPOSER_UNKNOWN_CHECKPOINT");
    expect((outcome as ApplicationFailure).nonRetryable).toBe(true);
    await run;
  });
});

describe("checkpoint snapshots", () => {
  it("answers a late request with the bag as it was at the checkpoint", async () => {
    let handler: ((args: { checkpointName: string }) => Promise<unknown>) | undefined;
    mocks.setHandler.mockImplementation((_def: unknown, fn: typeof handler) => {
      handler = fn;
    });
    const plan = stagedPlan({
      checkpoints: [{ name: "after-a", afterBatch: 0, timeout: 30_000 }],
      stageBoundaries: [],
    });

    await createWorkflowFunction(plan)({ initialData: { id: "1" } });

    // Arrives after batches 1 and 2 produced y and z; the snapshot predates them.
    await expect(handler!({ checkpointName: "after-a" })).resolves.toEqual({ id: "1", x: "X" });
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
