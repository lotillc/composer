---
"@lotiai/composer": minor
---

Keep asynchronous workflows on the build that started them.

Two changes, both about Temporal Worker Versioning:

- `runAsyncWorkflow` dropped the `versioningOverride` on its `startOnly` and
  await-completion paths, so fire-and-forget and awaited starts routed to whatever
  version was Current instead of the caller's build. Only `awaitCheckpoint` and
  `startAsyncWorkflow` pinned correctly. All four paths now pin.
- A service's workflow and activity workers registered under two separate Worker
  Deployments (`<svc>-workflows` and `<svc>-activities`). A workflow pinned to the
  workflow deployment therefore had every one of its steps treated as an Independent
  Activity and routed to the Current version of the activity deployment, which defeats
  the pin. Both worker types now register under one deployment, `<svc>-workers`, so
  their task queues belong to the same Worker Deployment Version.

Breaking: `getDeploymentSeriesNames` and `WORKER_DEPLOYMENT_SUFFIXES` are replaced by
`getWorkerDeploymentName` and `WORKER_DEPLOYMENT_SUFFIX` on the `temporal-naming`
subpath. `LEGACY_WORKER_DEPLOYMENT_SUFFIXES` is exported so deployment tooling can
still parse `-workflows`/`-activities` version records while they drain; remove its use
once none remain. Callers that set the current version per worker type must set one
deployment instead of two.

## Migrating an already-versioned service

Temporal has no rename or merge API, and publishes no runbook for moving a task queue
between Worker Deployments, so the sequence matters. Moving one is supported — the server
recognises "Task Queues that are moved to another Worker Deployment" when validating a
cutover — but two of its safeguards do not apply here.

1. Bring up workers registered under `<svc>-workers` polling **every** task queue, and
   every queue type, that the outgoing version polls. They register without receiving
   traffic.
2. **Verify that coverage yourself.** The server's missing-task-queue check is skipped
   entirely when the deployment has no previous current version, which is exactly the
   case on a first cutover to a new deployment name. Even afterwards it only protects
   queues with a non-zero backlog or add rate, so an idle queue is never checked.
3. `SetWorkerDeploymentCurrentVersion` on `<svc>-workers`. Routing for every shared queue
   moves at this point.
4. Leave the legacy workers running until their versions drain, then delete the versions
   and the legacy deployments.

Repointing Current before both worker types have registered is the failure this is
guarding against: a pinned workflow whose activity task queue has not registered the
pinned version has that activity treated as *independent* and dispatched to the activity
queue's own Current version — the old build — with no error. Matching re-evaluates at
dispatch, so a queue that registers late is forgiven; a queue with a live old-build
poller is not, because it matches immediately.

Ordering is timestamp-based: a later `SetWorkerDeploymentCurrentVersion` on a legacy
deployment silently moves routing back.
