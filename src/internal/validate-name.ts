/**
 * Definition-time validation for developer-chosen names (workflows, steps,
 * fan-outs, checkpoints, signal waits, stages).
 *
 * These names become Temporal workflow types and activity names, keys of
 * plain objects, and entries in generated worker source, so they are held to a
 * conservative character set and may not shadow `Object.prototype` members.
 */

const SAFE_NAME = /^[A-Za-z_][A-Za-z0-9_.-]*$/;

const RESERVED_NAMES = new Set(["__proto__", "constructor", "prototype"]);

export type NameKind = "Workflow" | "Step" | "FanOut" | "Checkpoint" | "SignalWait" | "Stage";

/**
 * Throws if `name` is reserved or uses characters outside the safe set: it must
 * start with a letter or `_` and contain only letters, digits, `_`, `.` and `-`.
 */
export function assertValidName(kind: NameKind, name: unknown): void {
  if (typeof name !== "string" || !SAFE_NAME.test(name)) {
    throw new Error(
      `${kind} name ${JSON.stringify(name)} is invalid: names must start with a letter or "_" ` +
        `and contain only letters, digits, "_", "." and "-".`,
    );
  }
  if (RESERVED_NAMES.has(name)) {
    throw new Error(
      `${kind} name "${name}" is reserved: "__proto__", "constructor" and "prototype" cannot be used as names.`,
    );
  }
}
