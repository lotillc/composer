# @lotiai/composer

## 0.3.0

### Minor Changes

- [#13](https://github.com/lotillc/composer/pull/13) [`b52f56e`](https://github.com/lotillc/composer/commit/b52f56e287e1687ef9eee7335df506ea5698d9c2) Thanks [@john-goloti](https://github.com/john-goloti)! - Add `describeWorkflow(workflowId)` and `isWorkflowRunning(workflowId)` to the `Composer` interface, and re-export `WorkflowExecutionStatusName`. These wrap the existing internal Temporal handle/describe logic so reconciliation and reaper sweeps can check whether a previously-launched workflow is still alive without taking a direct `@temporalio/client` dependency.
