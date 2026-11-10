---
"@lotiai/composer": minor
---

Long-lived workflows can loop, bound a wait by a deadline known only at run time, and fail fast on non-determinism.

- `.continueAsNew(name, { afterStep, resumeAt, when, carry })` declares a loop boundary. When its batch ends and `bag[when] === true`, the run continues as new at the earlier stage `resumeAt` names, with the body's outputs dropped from the bag and `carry` mapping each field the next pass needs to the field this pass computed it in. Otherwise the run goes on, so a loop boundary may follow the last batch. The builder checks that `resumeAt` is an earlier stage, that a step in the loop provides `when`, that every `carry` source exists by the boundary, that no step before `resumeAt` reads a carried field, and that loops nest rather than overlap. A carry whose source is missing when the loop is taken fails the workflow with `COMPOSER_LOOP_CARRY_MISSING`. `runSyncWorkflow` fails a workflow with a loop. The continuation format is unchanged: a looping run writes `stage: resumeAt`.
- `waitForSignal` accepts `timeoutAt`, a number field holding the instant the wait ends at in epoch milliseconds, instead of a fixed `timeout`. The field joins the wait's `needs`; an instant already past ends the wait at once, and a field holding anything else fails the node with `COMPOSER_INVALID_SIGNAL_WAIT`.
- `temporal.workflowFailureErrorTypes` is passed to the workflow workers, so `{ "*": ["NondeterminismError"] }` fails a run whose history the current build cannot replay instead of retrying its task until a build that can is deployed. Temporal marks the option experimental; it needs `@temporalio/worker` 1.18 or later.
