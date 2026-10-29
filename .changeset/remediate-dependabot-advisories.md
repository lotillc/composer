---
"@lotiai/composer": patch
---

Remediate open security advisories in dependencies.

**`zod` (runtime).** Raises the floor to `^4.6.6`, the first release patched for
GHSA-mrhx-c7xc-m3w5. Because `zod` is a direct dependency, consumers get the fix
from the published range.

**`@grpc/grpc-js` and `source-map-js` (runtime, transitive).** Refreshes the
lockfile to `@grpc/grpc-js@1.14.7` and `source-map-js@1.2.2`. The ranges declared
by `@temporalio/*`, `source-map-loader` and `postcss` already admit the patched
releases, so a fresh consumer install resolves them without any change here.

**`vite`, `vitest` and `fast-uri` (development and build tooling).** Bumps `vite`
to 8.3.5 and `vitest` / `@vitest/coverage-v8` to 5.0.5, and advances the
`fast-uri` override to 3.1.9. These affect only this repository's own tooling
and nothing package consumers install.
