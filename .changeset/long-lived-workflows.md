---
"@lotiai/composer": minor
---

Async workflows can now run for days or weeks without pinning the build that started them.

- `.continueAsNew(name, { afterStep })` splits a workflow into stages. After that step's batch, the run continues as new with the bag carried forward, and the next run starts on the current build (`initialVersioningBehavior: "AUTO_UPGRADE"` by default). A resumed run finds its stage by name, or by one of the stage's `aliases` after a rename, and never repeats a step an earlier run ran.
- `waitForSignal` adds a durable wait that holds no worker slot, with an optional `timeout` and `timeoutValue`. Each wait must be alone in its batch; the builder rejects one that would run alongside other nodes. You deliver the signal with `composer.signalAsyncWorkflow(workflowId, signal, payload)`, typed through `defineWorkflowSignal`. Signals that arrive early are kept, including across stages.
- `.versioning("AUTO_UPGRADE" | "PINNED")` declares a workflow's Worker Versioning behavior on the definition. Workflows that declare `AUTO_UPGRADE`, or that have stage boundaries, are now started without a build pin, because a start-time pin outranks both the declaration and each boundary's routing and is inherited across continue-as-new. Other workflows are pinned as before.
- `startAsyncWorkflow` accepts an optional `{ workflowId, ifRunning }`. With `ifRunning: "useExisting"`, a retried start resolves against the running workflow instead of failing; `runAsyncWorkflow` accepts it too.
- Signals are bounded and safe to retry. A workflow accepts only signal names its waits declare, keeps up to `maxPendingSignals` (default 100) per name, and drops a repeat of a `dedupeKey` passed to `signalAsyncWorkflow`. `defineWorkflowSignal(name, { parse })` validates a payload before it is sent. Everything a run carries forward is measured: above 1.5 MB the workflow fails with `COMPOSER_CONTINUATION_TOO_LARGE` instead of retrying forever against Temporal's 2 MB payload limit. Lower `temporal.continuationBudgetBytes` if your data converter grows payloads.
- `replayWorkflowHistories` (standalone, and on the composer) replays saved histories against a build offline, so a change that would break in-flight runs, or the rollback to them, is caught in CI before it ships.

Within a stage, an `AUTO_UPGRADE` workflow must replay across builds; the README's "Long-Lived Workflows" section covers the rules.

Under Temporal, a step inside a workflow composed with `use()` is now identified by its namespaced name (`child.step`), as it already was in sync execution. That name is what an error handler sees in `errors[].stepName`, what a checkpoint or stage boundary refers to, and what is recorded as already run. Its activity is still registered under the bare name. A fan-out keeps its bare name everywhere, since its child workflow IDs are built from it.

The activity worker now refuses to start when two different steps, in any of the workflows it serves, would register the same activity name. Activities are registered by a step's name without composition prefixes, so a collision used to leave whichever registered last running for both, silently.
