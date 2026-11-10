---
"@lotiai/composer": minor
---

The Temporal SDK packages (`@temporalio/activity`, `client`, `common`, `worker` and `workflow`) are now peer dependencies. Install them alongside Composer: the host then owns the one copy of each, which Temporal requires, since `Runtime.install` is a per-module singleton and every `@temporalio/*` package in a process must share a version. Before, a host whose own Temporal resolution differed from Composer's ended up with two copies, and Composer's workers ran on a `Runtime` the host had not configured.

Peer ranges are checked one package at a time, so they cannot require the five to match. Composer now checks when it creates a worker, a client or a replay, and throws `TemporalVersionMismatchError` (code `COMPOSER_TEMPORAL_VERSION_MISMATCH`), naming each package's version, when they differ.
