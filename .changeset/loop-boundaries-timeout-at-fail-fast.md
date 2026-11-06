---
"@lotiai/composer": minor
---

Long-lived workflows can loop, bound a wait by a runtime deadline, and fail fast on non-determinism.

- `.continueAsNew(name, { afterStep, resumeAt, when, carry })` declares a loop boundary. When its batch ends and `bag[when] === true`, the run continues as new at the earlier stage `resumeAt` names, with the body's outputs dropped from the bag and `carry` mapping each field the next run needs to the field this run computed it in. Otherwise the run goes on, so a loop boundary may follow the last batch. The builder checks that `resumeAt` is an earlier stage, that `when` and every `carry` source are available by the boundary, and that `when` is provided inside the loop or carried, so it can change. `runSyncWorkflow` refuses a workflow with a loop. The continuation format is unchanged: a looping run writes `stage: resumeAt`.
- `waitForSignal` accepts `timeoutAt`, a bag field holding the instant the wait ends at (an ISO 8601 date-time with its offset, or epoch milliseconds), instead of a static `timeout`. The field joins the wait's `needs`; an instant already past ends the wait at once, and a field holding anything else fails the node with `COMPOSER_INVALID_SIGNAL_WAIT`. A bounded wait shows the instant it ends at in the run's details.
- `temporal.workflowFailureErrorTypes` is passed to the workflow workers and the replay check, so `{ "*": ["NondeterminismError"] }` fails a run whose history the current build cannot replay instead of retrying its task until a build that can is deployed. The `@temporalio/*` dependencies move to `^1.21.1` (the option exists from 1.18).
