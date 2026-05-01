# Contributing to @lotiai/composer

Thanks for your interest in contributing.

## Development

```bash
pnpm install
pnpm compile
pnpm test
```

The package targets Node 22+ and uses pnpm (see `packageManager` in `package.json`).

## Pull requests

- Open against `main`. CI runs `pnpm compile` and `pnpm test` on Node 22 and 24.
- One logical change per PR. Keep diffs reviewable.
- Add or update tests for any behavioral change.
- Avoid introducing new dependencies unless necessary; prefer the standard library or existing deps.

## Reporting issues

File issues at https://github.com/lotillc/composer/issues. Include a minimal reproduction where possible.

## Code of conduct

By participating you agree to abide by the [Contributor Covenant](https://www.contributor-covenant.org/version/2/1/code_of_conduct/).

## Releases

Releases are published to npm by tagging `vX.Y.Z` on `main`. The `release.yml` workflow validates that the tag matches `package.json` and runs `pnpm publish --provenance`. Bump the version in `package.json` in the same commit (or PR) that you tag.
