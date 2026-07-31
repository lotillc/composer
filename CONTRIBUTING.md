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
- On merge to `main`, CI opens (or updates) a "Version Packages" PR that bumps `package.json`, updates `CHANGELOG.md`, and removes the consumed changesets.
- Merging that PR triggers the same `ci.yml` workflow to publish the new version to npm (via `pnpm publish --provenance`, using OIDC trusted publishing) and push the matching `vX.Y.Z` tag.
