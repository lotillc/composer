---
"@lotiai/composer": minor
---

Consumers can declare their own worker profiles alongside `standard` and `heavy`, so a single step can run on a dedicated worker, for example one whose task role alone holds a sensitive permission.

Register the name by augmenting `WorkerProfileRegistry`, then declare it in `composer.build-config.ts`:

```typescript
// worker-profiles.ts
declare module "@lotiai/composer" {
  interface WorkerProfileRegistry {
    "request-signing": true;
  }
}
export {}; // keeps the file a module; a .d.ts without one would shadow the package

// composer.build-config.ts
export default defineBuildConfig({
  workerProfiles: { "request-signing": { minCount: 1, initialCount: 1, maxCount: 1 } },
});
```

A step with `workerProfile: "request-signing"` routes to the `request-signing-tasks` queue. The profile starts from the `standard` resources, appears in `resolveProfilesForEnvironment`'s `dedicatedProfiles` and in `composer profiles`, and is accepted by `resolveActivityWorkerRuntimeConfig`. An unregistered name in a step, a `workerProfiles` key or a `fallbackProfile` fails typecheck. `findUndeclaredWorkerProfiles(workflows, config)` reports steps pinned to a profile the build config does not declare.

Existing configs and calls behave as before: `getAllTaskQueues()` and `isValidWorkerProfile(name)` without a config still cover only the built-in profiles. `workerProfiles` keys must now be valid profile names (lowercase letters, digits and inner hyphens, at most 32 characters, not `workflow` or an `Object.prototype` property such as `constructor`); before, other keys were accepted and ignored.
