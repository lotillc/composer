/**
 * Definition-side tests for long-lived workflows: the stage boundary and versioning
 * builder methods, their validation, how they and signal waits reach the workflow plan,
 * and how sync execution treats them.
 */

import { describe, expect, expectTypeOf, it } from "vitest";
import { testComposer } from "../../__tests__/test-utils.js";
import { generateWorkflowPlan } from "../async/register/generate-workflow-source.js";
import { fanOut } from "../dag-sync-fanout.js";
import { defineWorkflowSignal, waitForSignal } from "../dag-sync-signal.js";
import { type DurationString, step } from "../dag-sync-step.js";
import { createWorkflow, use } from "../dag-sync-workflow.js";

interface Bag {
  requestId: string;
  sentAt: string;
  scratch: string;
  approval: { approved: boolean };
  verified: boolean;
}

const approvalSignal = defineWorkflowSignal<{ approved: boolean }>("approval");

const sendRequest = step<Bag>()({
  name: "sendRequest",
  needs: ["requestId"],
  provides: ["sentAt", "scratch"],
  run: async () => ({ sentAt: "now", scratch: "s" }),
});

const awaitApproval = waitForSignal<Bag>()({
  name: "awaitApproval",
  signal: approvalSignal,
  needs: ["sentAt"],
  provides: ["approval"],
  timeout: "21 days",
  timeoutValue: { approved: false },
});

const verify = step<Bag>()({
  name: "verify",
  needs: ["requestId", "approval"],
  provides: ["verified"],
  run: async (_ctx, bag) => ({ verified: bag.approval.approved }),
});

function baseWorkflow() {
  return createWorkflow<Bag>("long-review")
    .requires("requestId")
    .build([sendRequest, awaitApproval, verify]);
}

describe("waitForSignal", () => {
  it("rejects a provides list that is not exactly one field", () => {
    expect(() =>
      waitForSignal<Bag>()({
        name: "bad",
        signal: approvalSignal,
        provides: [] as unknown as readonly ["approval"],
      }),
    ).toThrow("exactly one field");
  });

  it.each([
    ["a Date", new Date(0)],
    ["a Map", new Map()],
    ["a bigint", 1n],
    ["a nested non-finite number", { approved: false, score: Number.NaN }],
  ])("rejects %s as a timeout value, since the plan stores it as JSON", (_label, value) => {
    expect(() =>
      waitForSignal<Bag>()({
        name: "w",
        signal: approvalSignal,
        provides: ["approval"],
        timeoutValue: value as never,
      }),
    ).toThrow("timeoutValue must be plain JSON data");
  });

  it.each([
    ["an array with an extra property", Object.assign([false], { reason: "expired" })],
    ["negative zero", { approved: false, score: -0 }],
    ["a non-enumerable property", Object.defineProperty({ approved: false }, "hidden", { value: 1 })],
    ["a class instance", new (class Approval { approved = false })()],
    ["a getter", { approved: false, get note() { return "x"; } }],
  ])("rejects %s as a timeout value, since it does not survive JSON", (_label, value) => {
    expect(() =>
      waitForSignal<Bag>()({
        name: "w",
        signal: approvalSignal,
        provides: ["approval"],
        timeoutValue: value as never,
      }),
    ).toThrow("timeoutValue must be plain JSON data");
  });

  it("accepts plain JSON data nested deeper than a hundred levels", () => {
    let deep: unknown = { approved: false };
    for (let i = 0; i < 150; i++) deep = [deep];
    expect(() =>
      waitForSignal<Bag>()({
        name: "w",
        signal: approvalSignal,
        provides: ["approval"],
        timeoutValue: deep as never,
      }),
    ).not.toThrow();
  });

  it("rejects an undefined property in a timeout value, since JSON drops the key", () => {
    expect(() =>
      waitForSignal<Bag>()({
        name: "w",
        signal: approvalSignal,
        provides: ["approval"],
        timeoutValue: { approved: false, note: undefined } as never,
      }),
    ).toThrow("timeoutValue must be plain JSON data");
  });

  it("rejects a sparse array as a timeout value, since JSON writes its holes as null", () => {
    expect(() =>
      waitForSignal<Bag>()({
        name: "w",
        signal: approvalSignal,
        provides: ["approval"],
        timeoutValue: { approved: false, list: new Array(1) } as never,
      }),
    ).toThrow("timeoutValue must be plain JSON data");
  });

  it.each(["has space", "__proto__", "1st"])("rejects the wait name %j", (name) => {
    expect(() =>
      waitForSignal<Bag>()({ name, signal: approvalSignal, provides: ["approval"] }),
    ).toThrow(/SignalWait name/);
  });

  it("accepts plain JSON data, and undefined, as a timeout value", () => {
    for (const timeoutValue of [{ approved: false }, undefined]) {
      expect(() =>
        waitForSignal<Bag>()({
          name: "w",
          signal: approvalSignal,
          provides: ["approval"],
          timeoutValue: timeoutValue as never,
        }),
      ).not.toThrow();
    }
  });

  it.each(["0s", "-1 day", "1e999 days"] as DurationString[])(
    "rejects a timeout of %s where the wait is defined",
    (timeout) => {
      expect(() =>
        waitForSignal<Bag>()({ name: "w", signal: approvalSignal, provides: ["approval"], timeout }),
      ).toThrow("timeout must be a positive duration");
    },
  );

  it("rejects two waits on one signal that declare different caps", () => {
    interface CapBag {
      requestId: string;
      sentAt: string;
      a: { approved: boolean };
      b: { approved: boolean };
    }
    const sent = step<CapBag>()({
      name: "sent",
      needs: ["requestId"],
      provides: ["sentAt"],
      run: async () => ({ sentAt: "t" }),
    });
    const waitA = waitForSignal<CapBag>()({
      name: "waitA",
      signal: approvalSignal,
      needs: ["sentAt"],
      provides: ["a"],
    });
    const waitB = waitForSignal<CapBag>()({
      name: "waitB",
      signal: approvalSignal,
      needs: ["a"],
      provides: ["b"],
      maxPendingSignals: 1,
    });
    expect(() =>
      createWorkflow<CapBag>("caps").requires("requestId").build([sent, waitA, waitB]),
    ).toThrow(
      'waits "waitA" and "waitB" both wait on signal "approval" but declare maxPendingSignals 100 and 1',
    );
  });

  it("rejects two waits on one signal name that use different definitions", () => {
    interface DefBag {
      requestId: string;
      sentAt: string;
      a: { approved: boolean };
      b: { approved: boolean };
    }
    const otherApproval = defineWorkflowSignal<{ approved: boolean }>("approval");
    const sent = step<DefBag>()({
      name: "sent",
      needs: ["requestId"],
      provides: ["sentAt"],
      run: async () => ({ sentAt: "t" }),
    });
    const waitA = waitForSignal<DefBag>()({
      name: "waitA",
      signal: approvalSignal,
      needs: ["sentAt"],
      provides: ["a"],
    });
    const waitB = waitForSignal<DefBag>()({
      name: "waitB",
      signal: otherApproval,
      needs: ["a"],
      provides: ["b"],
    });
    expect(() =>
      createWorkflow<DefBag>("defs").requires("requestId").build([sent, waitA, waitB]),
    ).toThrow('both wait on signal "approval" but use different definitions of it');
  });

  it("rejects a maxPendingSignals that is not a positive integer", () => {
    expect(() =>
      waitForSignal<Bag>()({
        name: "w",
        signal: approvalSignal,
        provides: ["approval"],
        maxPendingSignals: 0,
      }),
    ).toThrow("maxPendingSignals must be a positive integer");
  });

  it("infers a signal's payload type from its parse function", () => {
    const parsed = defineWorkflowSignal("approval", {
      parse: (value: unknown) => ({ approved: Boolean((value as { approved?: unknown }).approved) }),
    });
    expectTypeOf(parsed).toEqualTypeOf<
      ReturnType<typeof defineWorkflowSignal<{ approved: boolean }>>
    >();
    expect(parsed.parse?.({ approved: 1 })).toEqual({ approved: true });
  });

  it("records a missing timeout value distinctly from an undefined one", () => {
    const noFallback = waitForSignal<Bag>()({
      name: "w",
      signal: approvalSignal,
      provides: ["approval"],
    });
    expect(noFallback.__signalWait).toEqual({
      signalName: "approval",
      signal: approvalSignal,
      hasTimeoutValue: false,
    });
  });

  it("types the signal payload against the provided field", () => {
    const wrongSignal = defineWorkflowSignal<string>("approval");
    waitForSignal<Bag>()({
      name: "w",
      // @ts-expect-error -- a string signal cannot provide { approved: boolean }
      signal: wrongSignal,
      provides: ["approval"],
    });
    waitForSignal<Bag>()({
      name: "w",
      signal: approvalSignal,
      provides: ["approval"],
      // @ts-expect-error -- the timeout value must match the provided field
      timeoutValue: "nope",
    });
    // @ts-expect-error -- provides must name a bag field
    waitForSignal<Bag>()({ name: "w", signal: approvalSignal, provides: ["missing"] });
    expectTypeOf(awaitApproval.provides).toEqualTypeOf<readonly ["approval"]>();
  });
});

describe("a wait alone in its batch", () => {
  // Needs nothing, so it would run alongside sendRequest in the first batch.
  const eagerWait = waitForSignal<Bag>()({
    name: "eagerWait",
    signal: approvalSignal,
    provides: ["approval"],
  });

  it("rejects a wait that would share its batch, naming what it shares it with", () => {
    expect(() =>
      createWorkflow<Bag>("crowded").requires("requestId").build([sendRequest, eagerWait, verify]),
    ).toThrow(
      'signal wait "eagerWait" would run alongside [sendRequest]. A wait must be alone in its batch',
    );
  });

  it("accepts the same wait once a need orders it after its sibling", () => {
    expect(() => baseWorkflow()).not.toThrow();
  });

  it("checks a wait that a composed workflow brings in", () => {
    const inner = createWorkflow<Bag>("inner").build([eagerWait]);
    expect(() =>
      createWorkflow<Bag>("outer").requires("requestId").build([sendRequest, use(inner), verify]),
    ).toThrow("A wait must be alone in its batch");
  });

  it("rejects a hand-assembled workflow when its plan is generated", () => {
    const wf = { ...baseWorkflow(), steps: [sendRequest, eagerWait, verify] };
    expect(() => generateWorkflowPlan(wf as never)).toThrow("A wait must be alone in its batch");
  });

  it("does not hold sync-only workflows without waits to the rule", () => {
    const parallel = step<Bag>()({
      name: "parallel",
      needs: ["requestId"],
      provides: ["scratch"],
      run: async () => ({ scratch: "s" }),
    });
    expect(() =>
      createWorkflow<Bag>("plain").requires("requestId").build([sendRequest, parallel]),
    ).not.toThrow();
  });
});

describe("builder", () => {
  it("records stage boundaries and versioning, keeping checkpoints alongside", () => {
    const wf = baseWorkflow()
      .checkpoint("sent", { afterStep: sendRequest })
      .versioning("AUTO_UPGRADE")
      .continueAsNew("awaiting-approval", { afterStep: sendRequest })
      .continueAsNew("verifying", {
        afterStep: awaitApproval,
        initialVersioningBehavior: "USE_RAMPING_VERSION",
      });

    expect(wf.versioningBehavior).toBe("AUTO_UPGRADE");
    expect(wf.checkpoints).toEqual([{ name: "sent", afterStep: "sendRequest", timeout: undefined }]);
    expect(wf.stageBoundaries).toEqual([
      { name: "awaiting-approval", afterStep: "sendRequest" },
      {
        name: "verifying",
        afterStep: "awaitApproval",
        initialVersioningBehavior: "USE_RAMPING_VERSION",
      },
    ]);
  });

  it("keeps everything when onError ends the chain", () => {
    const wf = baseWorkflow()
      .versioning("AUTO_UPGRADE")
      .continueAsNew("awaiting-approval", { afterStep: sendRequest })
      .onError(() => undefined);
    expect(wf.versioningBehavior).toBe("AUTO_UPGRADE");
    expect(wf.stageBoundaries).toHaveLength(1);
  });

  it("records a stage's earlier names", () => {
    const wf = baseWorkflow().continueAsNew("awaiting-approval", {
      afterStep: sendRequest,
      aliases: ["awaiting-response"],
    });
    expect(wf.stageBoundaries?.[0]).toEqual({
      name: "awaiting-approval",
      afterStep: "sendRequest",
      aliases: ["awaiting-response"],
    });
  });

  it("rejects an alias that names another stage", () => {
    expect(() =>
      baseWorkflow()
        .continueAsNew("a", { afterStep: sendRequest })
        .continueAsNew("b", { afterStep: awaitApproval, aliases: ["a"] }),
    ).toThrow('"a" names both stage "a" and stage "b"');
  });

  it("rejects an alias shared by two stages", () => {
    expect(() =>
      baseWorkflow()
        .continueAsNew("a", { afterStep: sendRequest, aliases: ["old"] })
        .continueAsNew("b", { afterStep: awaitApproval, aliases: ["old"] }),
    ).toThrow('"old" names both stage "a" and stage "b"');
  });

  it("rejects an alias repeated or equal to the stage's own name", () => {
    expect(() =>
      baseWorkflow().continueAsNew("a", { afterStep: sendRequest, aliases: ["a"] }),
    ).toThrow('stage "a" lists "a" more than once');
  });

  it.each([
    ["a stage name", "has space", undefined],
    ["a reserved stage name", "constructor", undefined],
    ["an alias", "ok", ["bad alias"]],
  ])("rejects %s that is not a safe name", (_label, name, aliases) => {
    expect(() =>
      baseWorkflow().continueAsNew(name, {
        afterStep: sendRequest,
        ...(aliases && { aliases }),
      }),
    ).toThrow(/Stage name/);
  });

  it("places a boundary after the composed node passed, not a parent node of the same name", () => {
    interface SameBag {
      requestId: string;
      sentAt: string;
      outer: { approved: boolean };
      inner: { approved: boolean };
      done: boolean;
    }
    const sent = step<SameBag>()({
      name: "sent",
      needs: ["requestId"],
      provides: ["sentAt"],
      run: async () => ({ sentAt: "t" }),
    });
    const parentWait = waitForSignal<SameBag>()({
      name: "await",
      signal: approvalSignal,
      needs: ["sentAt"],
      provides: ["outer"],
    });
    const childWait = waitForSignal<SameBag>()({
      name: "await",
      signal: approvalSignal,
      needs: ["outer"],
      provides: ["inner"],
    });
    const done = step<SameBag>()({
      name: "done",
      needs: ["inner"],
      provides: ["done"],
      run: async () => ({ done: true }),
    });
    const inner = createWorkflow<SameBag>("inner").requires("outer").build([childWait]);
    const wf = createWorkflow<SameBag>("same-names")
      .requires("requestId")
      .build([sent, parentWait, use(inner), done])
      .continueAsNew("after-inner", { afterStep: childWait });

    expect(wf.stageBoundaries?.[0]?.afterStep).toBe("inner.await");
  });

  it("resolves a composed node whose own name contains dots", () => {
    interface DotBag {
      requestId: string;
      sentAt: string;
      mailed: string;
      done: boolean;
    }
    const parentSend = step<DotBag>()({
      name: "send.email",
      needs: ["requestId"],
      provides: ["sentAt"],
      run: async () => ({ sentAt: "t" }),
    });
    const childSend = step<DotBag>()({
      name: "send.email",
      needs: ["sentAt"],
      provides: ["mailed"],
      run: async () => ({ mailed: "m" }),
    });
    const done = step<DotBag>()({
      name: "done",
      needs: ["mailed"],
      provides: ["done"],
      run: async () => ({ done: true }),
    });
    const inner = createWorkflow<DotBag>("inner").requires("sentAt").build([childSend]);
    const wf = createWorkflow<DotBag>("dots")
      .requires("requestId")
      .build([parentSend, use(inner), done])
      .checkpoint("mailed", { afterStep: childSend });
    expect(wf.checkpoints?.[0]?.afterStep).toBe("inner.send.email");
  });

  it("falls back to the name for hand-built steps that share one run function", () => {
    const run = async () => ({});
    const handA = { name: "handA", needs: ["requestId"], provides: [], run } as never;
    const handB = { name: "handB", needs: ["requestId"], provides: [], run } as never;
    const wf = createWorkflow<Bag>("hand-built")
      .requires("requestId")
      .build([handA, handB, sendRequest, awaitApproval, verify] as never)
      .checkpoint("after-a", { afterStep: handA });
    expect(wf.checkpoints?.[0]?.afterStep).toBe("handA");
  });

  it("refuses a boundary after a step that occurs twice", () => {
    const notify = step<Bag>()({
      name: "notify",
      needs: ["requestId"],
      provides: [],
      run: async () => ({}),
    });
    const first = createWorkflow<Bag>("first").requires("requestId").build([notify]);
    const second = createWorkflow<Bag>("second").requires("requestId").build([notify]);
    const wf = createWorkflow<Bag>("twice")
      .requires("requestId")
      .build([use(first), use(second), sendRequest, awaitApproval, verify]);
    expect(() => wf.continueAsNew("s", { afterStep: notify })).toThrow(
      'Step "notify" occurs more than once in the workflow',
    );
  });

  it("rejects a duplicate stage name", () => {
    expect(() =>
      baseWorkflow()
        .continueAsNew("s", { afterStep: sendRequest })
        .continueAsNew("s", { afterStep: awaitApproval }),
    ).toThrow('Duplicate stage name "s"');
  });

  it("rejects a boundary after the last batch", () => {
    expect(() => baseWorkflow().continueAsNew("s", { afterStep: verify })).toThrow(
      "follows the last batch",
    );
  });

  it("rejects two boundaries on one batch", () => {
    const sibling = step<Bag>()({
      name: "sibling",
      needs: ["requestId"],
      provides: [],
      run: async () => ({}),
    });
    const wf = createWorkflow<Bag>("siblings")
      .requires("requestId")
      .build([sendRequest, sibling, awaitApproval, verify]);
    expect(() =>
      wf.continueAsNew("a", { afterStep: sendRequest }).continueAsNew("b", { afterStep: sibling }),
    ).toThrow("follow the same batch");
  });

  it("rejects a checkpoint after the first boundary, in either order", () => {
    expect(() =>
      baseWorkflow()
        .checkpoint("approved", { afterStep: awaitApproval })
        .continueAsNew("s", { afterStep: sendRequest }),
    ).toThrow("Checkpoints must be reached before the first continue-as-new");
    expect(() =>
      baseWorkflow()
        .continueAsNew("s", { afterStep: sendRequest })
        .checkpoint("approved", { afterStep: awaitApproval }),
    ).toThrow("Checkpoints must be reached before the first continue-as-new");
  });

  it("allows a checkpoint on the first boundary's own batch", () => {
    expect(() =>
      baseWorkflow()
        .continueAsNew("s", { afterStep: sendRequest })
        .checkpoint("sent", { afterStep: sendRequest }),
    ).not.toThrow();
  });

  it("rejects an unknown versioning behavior at runtime", () => {
    expect(() => baseWorkflow().versioning("LATEST" as never)).toThrow(
      'Unknown versioning behavior "LATEST"',
    );
  });

  it("resolves a boundary on a step of a composed workflow", () => {
    const inner = createWorkflow<Bag>("inner").requires("requestId").build([sendRequest]);
    const outer = createWorkflow<Bag>("outer")
      .requires("requestId")
      .build([use(inner), awaitApproval, verify])
      .continueAsNew("s", { afterStep: sendRequest });
    expect(outer.stageBoundaries?.[0]?.afterStep).toBe("inner.sendRequest");
  });
});

describe("generateWorkflowPlan", () => {
  it("threads versioning, stage boundaries and signal waits into the plan", () => {
    const plan = generateWorkflowPlan(
      baseWorkflow()
        .versioning("AUTO_UPGRADE")
        .continueAsNew("awaiting-approval", { afterStep: sendRequest, aliases: ["requested"] })
        .continueAsNew("verifying", { afterStep: awaitApproval }),
    );

    expect(plan.versioningBehavior).toBe("AUTO_UPGRADE");
    expect(plan.stageBoundaries).toEqual([
      {
        name: "awaiting-approval",
        afterBatch: 0,
        initialVersioningBehavior: "AUTO_UPGRADE",
        aliases: ["requested"],
      },
      { name: "verifying", afterBatch: 1, initialVersioningBehavior: "AUTO_UPGRADE" },
    ]);
    expect(plan.batches[1]).toEqual({
      steps: [],
      signalWaits: [
        {
          name: "awaitApproval",
          signalName: "approval",
          provides: "approval",
          timeout: "21 days",
          timeoutValue: { approved: false },
          hasTimeoutValue: true,
        },
      ],
    });
  });

  it("threads a wait's maxPendingSignals into the plan", () => {
    const capped = waitForSignal<Bag>()({
      name: "awaitApproval",
      signal: approvalSignal,
      needs: ["sentAt"],
      provides: ["approval"],
      maxPendingSignals: 5,
    });
    const plan = generateWorkflowPlan(
      createWorkflow<Bag>("capped").requires("requestId").build([sendRequest, capped, verify]),
    );
    expect(plan.batches[1]?.signalWaits?.[0]?.maxPendingSignals).toBe(5);
  });

  it("keeps composed waits that share a local name distinct in the plan", () => {
    interface TwoBag {
      requestId: string;
      sentAt: string;
      first: { approved: boolean };
      second: { approved: boolean };
    }
    const sent = step<TwoBag>()({
      name: "sent",
      needs: ["requestId"],
      provides: ["sentAt"],
      run: async () => ({ sentAt: "t" }),
    });
    const outerWait = waitForSignal<TwoBag>()({
      name: "await",
      signal: approvalSignal,
      needs: ["sentAt"],
      provides: ["first"],
    });
    const innerWait = waitForSignal<TwoBag>()({
      name: "await",
      signal: approvalSignal,
      needs: ["first"],
      provides: ["second"],
    });
    const inner = createWorkflow<TwoBag>("inner").requires("first").build([innerWait]);
    const outer = createWorkflow<TwoBag>("outer")
      .requires("requestId")
      .build([sent, outerWait, use(inner)]);

    const names = generateWorkflowPlan(outer).batches.flatMap((b) =>
      (b.signalWaits ?? []).map((w) => w.name),
    );
    expect(names).toEqual(["await", "inner.await"]);
  });

  it("rejects two different composed steps that share a bare name", () => {
    const inner = createWorkflow<Bag>("inner").requires("requestId").build([sendRequest]);
    const again = step<Bag>()({
      name: "sendRequest",
      needs: ["requestId"],
      provides: ["scratch"],
      run: async () => ({ scratch: "s" }),
    });
    const outer = createWorkflow<Bag>("outer").requires("requestId").build([again, use(inner)]);
    expect(() => generateWorkflowPlan(outer)).toThrow(
      'two different steps, "sendRequest" and "inner.sendRequest", both register activity "sendRequest"',
    );
  });

  it("plans the same step reused in two children as two nodes sharing one activity", () => {
    // A side-effect-only step, reused: composition namespaces each use.
    const notify = step<Bag>()({
      name: "notify",
      needs: ["requestId"],
      provides: [],
      run: async () => ({}),
    });
    const first = createWorkflow<Bag>("first").requires("requestId").build([notify]);
    const second = createWorkflow<Bag>("second").requires("requestId").build([notify]);
    const outer = createWorkflow<Bag>("outer")
      .requires("requestId")
      .build([use(first), use(second)]);

    const steps = generateWorkflowPlan(outer).batches.flatMap((b) => b.steps);
    expect(steps.map((s) => [s.name, s.activityName])).toEqual([
      ["first.notify", "notify"],
      ["second.notify", "notify"],
    ]);
  });

  it("places a checkpoint after a fan-out inside a composed workflow", () => {
    interface ChildBag {
      item: string;
      doubled: string;
    }
    interface FanBag {
      items: string[];
      doubled: string[];
      done: boolean;
    }
    const double = step<ChildBag>()({
      name: "double",
      needs: ["item"],
      provides: ["doubled"],
      run: async (_ctx, bag) => ({ doubled: bag.item + bag.item }),
    });
    const child = createWorkflow<ChildBag>("child").requires("item").build([double]);
    const fan = fanOut<FanBag>()({
      name: "fan",
      needs: ["items"],
      childWorkflow: child,
      mapInput: (bag) => bag.items.map((item) => ({ item })),
      provides: ["doubled"],
      aggregateResults: (results) => ({ doubled: results.map((r) => r.doubled) }),
    });
    const finish = step<FanBag>()({
      name: "finish",
      needs: ["doubled"],
      provides: ["done"],
      run: async () => ({ done: true }),
    });
    const inner = createWorkflow<FanBag>("inner").requires("items").build([fan]);
    const outer = createWorkflow<FanBag>("outer")
      .requires("items")
      .build([use(inner), finish])
      .checkpoint("fanned", { afterStep: fan });

    expect(outer.checkpoints?.[0]?.afterStep).toBe("inner.fan");
    expect(generateWorkflowPlan(outer).checkpoints).toEqual([
      { name: "fanned", afterBatch: 0, timeout: 30_000 },
    ]);
  });

  it("places a checkpoint on a composed step", () => {
    const inner = createWorkflow<Bag>("inner").requires("requestId").build([sendRequest]);
    const outer = createWorkflow<Bag>("outer")
      .requires("requestId")
      .build([use(inner), awaitApproval, verify])
      .checkpoint("sent", { afterStep: sendRequest });
    expect(generateWorkflowPlan(outer).checkpoints).toEqual([
      { name: "sent", afterBatch: 0, timeout: 30_000 },
    ]);
  });

  it("leaves the new plan fields out for a workflow that uses none of them", () => {
    const plan = generateWorkflowPlan(
      createWorkflow<Bag>("plain").requires("requestId").build([sendRequest]),
    );
    expect(plan).not.toHaveProperty("stageBoundaries");
    expect(plan).not.toHaveProperty("versioningBehavior");
    expect(plan.batches[0]).not.toHaveProperty("signalWaits");
  });

  it("revalidates boundaries on a hand-assembled workflow", () => {
    const wf = {
      ...baseWorkflow(),
      stageBoundaries: [{ name: "s", afterStep: "verify" }],
    };
    expect(() => generateWorkflowPlan(wf)).toThrow("follows the last batch");
  });
});

describe("sync execution", () => {
  it("keeps a step output named __proto__ in the bag and the next step's input", async () => {
    interface ProtoBag {
      __proto__: string;
      seen: string;
    }
    const writer = step<ProtoBag>()({
      name: "writer",
      needs: [],
      provides: ["__proto__"],
      run: async () => JSON.parse('{"__proto__": "kept"}') as { __proto__: string },
    });
    const reader = step<ProtoBag>()({
      name: "reader",
      needs: ["__proto__"],
      provides: ["seen"],
      run: async (_ctx, bag) => ({ seen: String(Object.getOwnPropertyDescriptor(bag, "__proto__")?.value) }),
    });
    const result = await testComposer.runSyncWorkflow(
      createWorkflow<ProtoBag>("proto").build([writer, reader]),
    );
    expect(result.error).toBeUndefined();
    expect(result.bag.seen).toBe("kept");
  });

  it("ignores stage boundaries", async () => {
    // Needs sendRequest's output, so it runs in the batch after the boundary.
    const confirm = step<Bag>()({
      name: "confirm",
      needs: ["sentAt", "approval"],
      provides: ["verified"],
      run: async (_ctx, bag) => ({ verified: bag.approval.approved }),
    });
    const wf = createWorkflow<Bag>("sync-staged")
      .requires("requestId", "approval")
      .build([sendRequest, confirm])
      .continueAsNew("s", { afterStep: sendRequest });

    const result = await testComposer.runSyncWorkflow(wf, {
      requestId: "r",
      approval: { approved: true },
    });
    expect(result.error).toBeUndefined();
    expect(result.bag.verified).toBe(true);
  });

  it("fails a signal wait with an error naming async execution", async () => {
    const result = await testComposer.runSyncWorkflow(baseWorkflow(), { requestId: "r" });
    expect(result.error).toBeDefined();
    const cause = (result.error as { errors?: Array<{ originalError?: Error }> }).errors?.[0]
      ?.originalError;
    expect(String(cause?.message ?? result.error?.message)).toMatch(/runAsyncWorkflow|Temporal/);
  });
});
