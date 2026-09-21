---
"@lotiai/composer": minor
---

**Breaking:** the package is now ESM-only and requires Node >= 24.14.1.

Staying on CommonJS pinned this package to the last CJS-compatible release of a
growing number of dependencies, which meant security advisories that could only
be dismissed rather than remediated. Publishing as ESM removes that constraint.

**For ESM consumers:** no change.

**For CommonJS consumers:** `require("@lotiai/composer")` continues to work.
Node supports `require()` of an ES module well below this package's minimum
version, and none of the entrypoints use top-level `await`.

**For TypeScript consumers compiling to CommonJS:** set `module` and
`moduleResolution` to `node16` or `nodenext` in `tsconfig.json`. Note that
switching a call site to `await import(...)` is *not* a workaround — under
`"module": "commonjs"` TypeScript downlevels it back into a `require()` call.

**Minimum Node version** rises from 22 to 24.14.1.

Also in this release:

- `./package.json` is now resolvable from the `exports` map.
- `./temporal-naming` and `./schedule-sync` are documented in the README; they
  were already exported.
- Removed the unused internal `importModuleFromFile` helper.
