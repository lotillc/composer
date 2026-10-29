---
"@lotiai/composer": minor
---

Async workflows can now run for days or weeks without pinning the build that started them.

- `.continueAsNew(name, { afterStep, carry? })` splits a workflow into stages. After that step's batch, the run continues as new with the bag carried forward, and the next run starts on the current build (`initialVersioningBehavior: "AUTO_UPGRADE"` by default). A resumed run finds its stage by name, or by one of the stage's `aliases` after a rename, and never repeats a step an earlier run ran.
- `waitForSignal` adds a durable wait that holds no worker slot, with an optional `timeout` and `timeoutValue`. You deliver the signal with `composer.signalAsyncWorkflow(workflowId, signal, payload)`, typed through `defineWorkflowSignal`. Signals that arrive early are kept, including across stages.
- `.versioning("AUTO_UPGRADE" | "PINNED")` declares a workflow's Worker Versioning behavior on the definition. Workflows that declare `AUTO_UPGRADE` are now started without a build pin, because a start-time pin outranks the declaration and is inherited across continue-as-new. Workflows that declare nothing are pinned as before.
- `startAsyncWorkflow` accepts an optional `{ workflowId, ifRunning }`. With `ifRunning: "useExisting"`, a retried start resolves against the running workflow instead of failing; `runAsyncWorkflow` accepts it too.
- Signals are bounded and safe to retry. A workflow accepts only signal names its waits declare, keeps up to `maxPendingSignals` (default 100) per name, and drops a repeat of a `dedupeKey` passed to `signalAsyncWorkflow`. `defineWorkflowSignal(name, { parse })` validates a payload before it is sent.
- `replayWorkflowHistories` (standalone, and on the composer) replays saved histories against a build offline, so a change that would break in-flight runs, or the rollback to them, is caught in CI before it ships.

Within a stage, an `AUTO_UPGRADE` workflow must replay across builds; the README's "Long-Lived Workflows" section covers the rules.
