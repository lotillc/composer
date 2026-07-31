# Changesets

This directory is managed by [Changesets](https://github.com/changesets/changesets). Read the [documentation](https://github.com/changesets/changesets#documentation) to learn what a changeset is, how to write one, and how versioning works in this repo.

Run `pnpm changeset` after making a user-facing change and follow the prompts. Commit the generated `.changeset/*.md` file with your PR — CI uses it to open/update a "Version Packages" PR, and merging that PR publishes the new version to npm automatically.
