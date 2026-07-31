---
"@lotiai/composer": minor
---

Add `nonRetryableErrorTypes` to `StepRetryPolicy`, so a step can declare which error codes must not be retried at the Temporal activity boundary. Codes listed in `asyncRetry.nonRetryableErrorTypes` are threaded through the workflow plan into the activity's Temporal `RetryPolicy`, where they are matched against `ApplicationFailure.type` — which the framework already populates from the thrown error's `code`. A step throwing a listed code fails on its first attempt instead of burning the remaining `maximumAttempts` on work that is guaranteed to fail identically (content-policy rejections, validation failures, permanent 4xx responses).

Fully backward compatible: the field is optional, and when it is unset or empty the retry policy handed to Temporal is unchanged. Errors that are not listed remain retryable.
