---
"@lotiai/composer": patch
---

Remediate open security advisories in dependencies.

**`zod` (runtime).** Raises the floor to `^4.6.6`, the first release patched for
GHSA-mrhx-c7xc-m3w5. Because `zod` is a direct dependency, consumers get the fix
from the published range.

**`@grpc/grpc-js`, `source-map-js` and `fast-uri` (runtime, transitive).**
Refreshes the lockfile to `@grpc/grpc-js@1.14.7` and `source-map-js@1.2.2`, and
advances the existing `fast-uri` override to 3.1.9. The ranges declared by the
packages that pull these in (`@temporalio/*`, `source-map-loader`, `postcss`,
`ajv`) already admit the patched releases, so a fresh consumer install resolves
them without any change here.

**`vite` and `vitest` (development).** Bumps `vite` to 8.3.5 and `vitest` /
`@vitest/coverage-v8` to 5.0.5. These affect only this repository's own tooling
and nothing package consumers install.
