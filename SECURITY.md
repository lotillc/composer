# Security policy

## Trust chain

This package is published and distributed under multiple identifiers. Auditors and consumers should verify the following mapping is consistent before trusting a release:

| Layer | Identifier |
|---|---|
| npm scope | [`@lotiai`](https://www.npmjs.com/org/lotiai) |
| npm package | [`@lotiai/composer`](https://www.npmjs.com/package/@lotiai/composer) |
| GitHub org | [`lotillc`](https://github.com/lotillc) |
| GitHub repo | [`lotillc/composer`](https://github.com/lotillc/composer) |
| Maintainer (legal) | Loti, LLC |
| Maintainer (web) | [lotiai.com](https://lotiai.com), [goloti.com](https://goloti.com) |

The `@lotiai` npm scope and the `lotillc` GitHub org are intentionally distinct names — `@loti` was already taken on npm when this package was first published, and the `lotillc` GitHub org predates the rebrand to "Loti AI". Both identifiers are owned by the same legal entity (Loti, LLC).

## Release integrity

Releases are published exclusively from GitHub Actions in [`lotillc/composer`](https://github.com/lotillc/composer) using a [Trusted Publisher](https://docs.npmjs.com/trusted-publishers) configuration with [npm provenance](https://docs.npmjs.com/generating-provenance-statements) enabled. Every published version on npm should:

1. Carry a `provenance` attestation linking back to a specific tag and workflow run in `lotillc/composer`.
2. Match a signed git tag of the form `vX.Y.Z` on `main`.
3. Have `package.json` `version` equal to the tag (verified by the release workflow before publishing).

If you find an `@lotiai/composer` version on npm that does not satisfy all three properties, treat it as suspect and report it.

## Reporting a vulnerability

Please do **not** open a public GitHub issue for security reports.

Use GitHub's private vulnerability reporting at https://github.com/lotillc/composer/security/advisories/new.

We aim to respond within 5 business days and to publish a fix or mitigation within 30 days for confirmed vulnerabilities.
