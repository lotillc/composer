---
"@lotiai/composer": patch
---

Remediate three open Dependabot advisories in transitive dependencies.

**`fflate` (runtime).** Raises the `@aws-sdk/client-cloudwatch` and
`@aws-sdk/client-lambda` floors to `^3.1136.0`, which requires
`@smithy/middleware-compression@^4.6.2` and so brings `fflate@0.8.3`. The fix is
carried by the published dependency graph, so package consumers get it too —
verified by installing the packed tarball with npm, which resolves
`fflate@0.8.3`.

**`js-yaml` (development).** Advances the existing overrides to 3.15.2 / 4.3.2.
These are reached only through `@changesets/*`, a development dependency that
package consumers never install, so this affects this repository's own tooling
and nothing downstream.

Also aligns `@vitest/coverage-v8` with `vitest` 5, which had been leaving an
unmet peer dependency.
