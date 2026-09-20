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
