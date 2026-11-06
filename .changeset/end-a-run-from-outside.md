---
"@lotiai/composer": minor
---

A run can now be ended from outside, and a sweep can see how long it has been open.

- `startAsyncWorkflow` and `runAsyncWorkflow` accept `executionTimeout`, a limit on the whole execution across every continue-as-new run, after which Temporal ends the workflow as `TIMED_OUT`. A limit that is not a positive duration is refused before anything starts.
- `composer.terminateAsyncWorkflow(workflowId, { reason, runId })` ends a workflow at once, without running any of its code. It resolves `"terminated"`, or `"notRunning"` when nothing under that ID is running, so a retried call is safe. With the `runId` from `describeWorkflow`, it ends only that run, leaving a workflow that has since moved on alone.
- `describeWorkflow` now reports the current run's `runId`, `startTime`, `closeTime` and `historyLength` beside its `status` (the exported `WorkflowDescription`), so a sweep can tell a run that just started from one that has been open too long.
