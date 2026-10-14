/**
 * Worker profile name rules, shared by the build-config schema and the
 * profile resolution in worker-profiles.ts.
 */

/**
 * Pattern a worker profile name must match. Profile names become task queue
 * names and, in infrastructure, parts of resource names, so they are kept to
 * lowercase letters, digits and inner hyphens.
 */
export const WORKER_PROFILE_NAME_PATTERN = /^[a-z][a-z0-9]*(?:-[a-z0-9]+)*$/;

/** Longest accepted worker profile name. */
export const MAX_WORKER_PROFILE_NAME_LENGTH = 32;

/**
 * Names a profile may not take, besides `Object.prototype` properties such as
 * `constructor`, which would collide with inherited keys in the plain-object
 * maps profiles are looked up in. `workflow` would route its activities onto
 * `workflow-tasks`, the queue workflow workers poll.
 */
const RESERVED_WORKER_PROFILE_NAMES: ReadonlySet<string> = new Set(["workflow"]);

/**
 * Checks whether a string is usable as a worker profile name, whether or not
 * it is declared anywhere.
 */
export function isWorkerProfileName(name: string): boolean {
  return (
    name.length <= MAX_WORKER_PROFILE_NAME_LENGTH &&
    WORKER_PROFILE_NAME_PATTERN.test(name) &&
    !RESERVED_WORKER_PROFILE_NAMES.has(name) &&
    !(name in Object.prototype)
  );
}
