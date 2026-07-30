---
"@lotiai/composer": minor
---

Add `describeWorkflow(workflowId)` and `isWorkflowRunning(workflowId)` to the `Composer` interface, and re-export `WorkflowExecutionStatusName`. These wrap the existing internal Temporal handle/describe logic so reconciliation and reaper sweeps can check whether a previously-launched workflow is still alive without taking a direct `@temporalio/client` dependency.
