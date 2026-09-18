---
"@lotiai/composer": minor
---

Add `asyncPoll`, so a step that waits on an external system stops holding an activity slot while it waits.

Composer offered exactly two ways to wait for an external system, and both cost a slot for the whole wait: block inside the activity while heartbeating, or throw and let Temporal's retry backoff re-schedule the attempt. The first is what consumers reach for, and it does not scale -- a worker's `maxConcurrentActivityTaskExecutions` is small (15 on the `standard` profile), so when the dependency degrades every slot fills with sleeping pollers and unrelated activities starve behind them. The workflow side, which is where a Temporal timer belongs, had no way to express the wait: the generated workflow never called `wf.sleep`, and a step could only signal by failing.

- **A step signals "not done yet" by throwing `StepNotReadyError`.** The workflow catches it, waits on a real Temporal timer, and re-invokes the activity. Each attempt is an ordinary activity invocation that ends, so the worker releases its slot across the wait and a waiting step costs a slot only for the calls it actually makes.
- **`asyncPoll` sets the schedule**: `{ initialInterval, maximumInterval, timeout, backoffCoefficient? }`. The interval grows by the coefficient (default 2), is clamped to `maximumInterval`, and carries 20% jitter so a shared dependency is not polled in lockstep. `timeout` bounds the total elapsed wait and is checked *before* each sleep, so it is a real ceiling rather than one the final wait may overshoot. Exceeding it fails the step with `COMPOSER_STEP_POLL_TIMEOUT` -- map that code if you classify failures, or it lands in whatever your default is.
- **Polling does not consume the step's retry budget.** When `asyncPoll` is set, `COMPOSER_STEP_NOT_READY` is appended to the activity's `nonRetryableErrorTypes`, so Temporal returns control to the workflow loop on the first not-ready signal instead of burning `maximumAttempts` first. `asyncRetry` therefore keeps covering genuine transient failures, and any `nonRetryableErrorTypes` the step already declared are preserved alongside the signal.
- **Replay stays deterministic.** Elapsed time accumulates the intervals actually slept rather than reading a clock, and the jitter draws from the workflow sandbox's seeded PRNG.

Purely additive: a step without `asyncPoll` produces the same plan, the same `proxyActivities` options and the same execution as before. `StepNotReadyError`, `StepPollTimeoutError`, `STEP_NOT_READY_CODE`, `STEP_POLL_TIMEOUT_CODE` and the `StepPollPolicy` type are exported from the package root. In sync execution `asyncPoll` is ignored and a `StepNotReadyError` is an ordinary failure, matching how the other `async*` options behave.
