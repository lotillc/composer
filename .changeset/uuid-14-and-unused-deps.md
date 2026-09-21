---
"@lotiai/composer": patch
---

Upgrade `uuid` to v14 and drop three unused dependencies.

`uuid` was pinned at v11 because v12 became ESM-only, which this package could
not consume. That constraint is gone, so the pin is lifted. Only the `v7` API is
used and its behaviour is unchanged.

`glob`, `es-toolkit`, and `@lifeomic/attempt` were declared as runtime
dependencies but imported nowhere in the package. Removing them shrinks the
install footprint and retires two more CommonJS-only packages from the tree.
