/**
 * Definition-side tests for long-lived workflows: the stage boundary and versioning
 * builder methods, their validation, how they and signal waits reach the workflow plan,
 * and how sync execution treats them.
 */

import { describe, expect, expectTypeOf, it } from "vitest";
import { testComposer } from "../../__tests__/test-utils.js";
import { generateWorkflowPlan } from "../async/register/generate-workflow-source.js";
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
    expect(noFallback.__signalWait).toEqual({ signalName: "approval", hasTimeoutValue: false });
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
