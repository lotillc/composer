# @lotiai/composer

## 0.4.0

### Minor Changes

- [#17](https://github.com/lotillc/composer/pull/17) [`449764f`](https://github.com/lotillc/composer/commit/449764fc3918268ce8494237bec908a781ac349f) Thanks [@john-goloti](https://github.com/john-goloti)! - Add `nonRetryableErrorTypes` to `StepRetryPolicy`, so a step can declare which error codes must not be retried at the Temporal activity boundary. Codes listed in `asyncRetry.nonRetryableErrorTypes` are threaded through the workflow plan into the activity's Temporal `RetryPolicy`, where they are matched against `ApplicationFailure.type` — which the framework already populates from the thrown error's `code`. A step throwing a listed code fails on its first attempt instead of burning the remaining `maximumAttempts` on work that is guaranteed to fail identically (content-policy rejections, validation failures, permanent 4xx responses).

  The field is optional, and when it is unset or empty the retry policy handed to Temporal is unchanged. Errors that are not listed remain retryable.

  Also makes `parentCodes` optional when detecting coded errors at the activity boundary. Previously an error was only converted to a coded `ApplicationFailure` if it carried **both** a string `code` and a `parentCodes` array, so an error with just a `code` reached Temporal with its class name as the failure type and its code lost — which would have silently defeated `nonRetryableErrorTypes` for those errors. Errors carrying a string `code` but no `parentCodes` now get the same treatment as fully coded ones (`parentCodes` defaults to `[]`), which means such an error's `code` — rather than its class name — is now what surfaces in `StepFailureInfo.code` and in workflow error results. Errors with no `code` at all are still passed through untouched, and retryability is unaffected for anything that does not opt in.

## 0.3.0

### Minor Changes

- [#13](https://github.com/lotillc/composer/pull/13) [`b52f56e`](https://github.com/lotillc/composer/commit/b52f56e287e1687ef9eee7335df506ea5698d9c2) Thanks [@john-goloti](https://github.com/john-goloti)! - Add `describeWorkflow(workflowId)` and `isWorkflowRunning(workflowId)` to the `Composer` interface, and re-export `WorkflowExecutionStatusName`. These wrap the existing internal Temporal handle/describe logic so reconciliation and reaper sweeps can check whether a previously-launched workflow is still alive without taking a direct `@temporalio/client` dependency.
