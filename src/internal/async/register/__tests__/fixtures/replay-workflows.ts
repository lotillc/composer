/**
 * Workflows whose recorded histories pin down the commands the workflow factory emits.
 *
 * Between them they reach every command-producing path in the factory: sequential and
 * parallel batches, a checkpoint, a handled step failure, workflow-side polling, a fan-out
 * with child workflows, a durable signal wait, and two stage boundaries. The histories in
 * `replay-histories/` were recorded from these definitions with
 * `record-replay-histories.vitest.ts`; `replay-histories.vitest.ts` replays them on every
 * test run, so a factory change that would break in-flight runs fails CI.
 *
 * Changing a definition here invalidates its histories: re-record them in the same change.
 */

import {
  createWorkflow,
  defineWorkflowSignal,
  fanOut,
  StepNotReadyError,
  step,
  waitForSignal,
  type Workflow,
} from "../../../../../index.js";

// --- Staged workflow: a step, a signal wait, a step, with a boundary between each ---

export interface StagedBag {
  requestId: string;
  sentAt: string;
  approval: { approved: boolean };
  verified: boolean;
}

export const approvalSignal = defineWorkflowSignal<{ approved: boolean }>("approval");

const sendRequest = step<StagedBag>()({
  name: "sendRequest",
  needs: ["requestId"],
  provides: ["sentAt"],
  run: async () => ({ sentAt: "2026-01-01T00:00:00Z" }),
});

const awaitApproval = waitForSignal<StagedBag>()({
  name: "awaitApproval",
  signal: approvalSignal,
  needs: ["sentAt"],
  provides: ["approval"],
  timeout: "1 day",
  timeoutValue: { approved: false },
});

const verifyOutcome = step<StagedBag>()({
  name: "verifyOutcome",
  needs: ["requestId", "approval"],
  provides: ["verified"],
  run: async (_ctx, bag) => ({ verified: bag.approval.approved }),
});

export const stagedWorkflow = createWorkflow<StagedBag>("replay-fixture-staged")
  .requires("requestId")
  .build([sendRequest, awaitApproval, verifyOutcome])
  .versioning("AUTO_UPGRADE")
  .continueAsNew("awaiting-approval", { afterStep: sendRequest })
  .continueAsNew("verifying", { afterStep: awaitApproval, carry: ["approval"] });

// --- DAG workflow: parallel batch, checkpoint, handled failure, polling, fan-out ---

export interface ChildBag {
  item: string;
  doubled: string;
}

const double = step<ChildBag>()({
  name: "double",
  needs: ["item"],
  provides: ["doubled"],
  run: async (_ctx, bag) => ({ doubled: bag.item + bag.item }),
});

export const childWorkflow = createWorkflow<ChildBag>("replay-fixture-child")
  .requires("item")
  .build([double]);

export interface DagBag {
  items: string[];
  x: string;
  left: string;
  right: string;
  flaky: string;
  polled: string;
  doubled: string[];
}

const first = step<DagBag>()({
  name: "first",
  needs: ["items"],
  provides: ["x"],
  run: async () => ({ x: "x" }),
});

const left = step<DagBag>()({
  name: "left",
  needs: ["x"],
  provides: ["left"],
  run: async () => ({ left: "l" }),
});

const right = step<DagBag>()({
  name: "right",
  needs: ["x"],
  provides: ["right"],
  run: async () => ({ right: "r" }),
});

// Fails without retrying, so the error handler runs; the handler marks it handled.
const flaky = step<DagBag>()({
  name: "flaky",
  needs: ["x"],
  provides: ["flaky"],
  asyncRetry: { maximumAttempts: 1 },
  run: async () => {
    throw Object.assign(new Error("deterministic failure"), { code: "FIXTURE_FAILURE" });
  },
});

// Not ready on its first call, so the workflow sleeps on a timer and calls it again.
let pollCalls = 0;
const polled = step<DagBag>()({
  name: "polled",
  needs: ["left", "right"],
  provides: ["polled"],
  asyncPoll: { initialInterval: "1s", maximumInterval: "1s", timeout: "1 minute" },
  run: async () => {
    pollCalls++;
    if (pollCalls === 1) throw new StepNotReadyError({ reason: "first call" });
    return { polled: "p" };
  },
});

const fanOutDouble = fanOut<DagBag>()({
  name: "fanOutDouble",
  needs: ["items", "polled"],
  childWorkflow,
  mapInput: (bag) => bag.items.map((item) => ({ item })),
  provides: ["doubled"],
  aggregateResults: (results) => ({ doubled: results.map((r) => r.doubled) }),
  concurrency: 2,
});

export const dagWorkflow = createWorkflow<DagBag>("replay-fixture-dag")
  .requires("items")
  .build([first, left, right, flaky, polled, fanOutDouble])
  .checkpoint("first-done", { afterStep: first })
  .onError(() => undefined);

// Workers take `Workflow<any, any, any>[]`, which a workflow with named checkpoints does not
// satisfy at the type level; the cast is the same one a consumer with checkpoints needs.
export const replayFixtureWorkflows = [
  stagedWorkflow,
  dagWorkflow,
  childWorkflow,
] as unknown as Workflow<any, any, any>[];
