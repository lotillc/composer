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

Releases are automated with [Changesets](https://github.com/changesets/changesets) — versions are never bumped by hand.

- Any PR with a user-facing change should include a changeset: run `pnpm changeset`, pick the bump type (patch/minor/major), and write a short summary. Commit the generated `.changeset/*.md` file with your PR.
- On merge to `main`, `ci.yml`'s `version` job opens (or updates) a "Version Packages" PR that bumps `package.json`, updates `CHANGELOG.md`, and removes the consumed changesets. This job never touches npm.
- Merging that PR tags the release (`vX.Y.Z`) and dispatches `release.yml`, which does the actual `pnpm publish --provenance` (OIDC trusted publishing) behind the `npm-publish` environment's reviewer gate. `release.yml` is kept as its own tag-triggered workflow deliberately: npm's Trusted Publisher registration for this package is bound to that exact filename, so it can't be merged into `ci.yml` without re-registering on npmjs.com.
