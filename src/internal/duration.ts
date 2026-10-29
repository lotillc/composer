/**
 * Duration parsing shared by the workflow factory and the definitions that validate their
 * durations up front.
 *
 * Kept in a leaf module with no runtime imports: the workflow factory runs in Temporal's
 * deterministic sandbox and bundles whatever it imports.
 *
 * @module duration
 */

import type { DurationString } from "./dag-sync-step.js";

const DURATION_UNIT_MS: Record<string, number> = {
  ms: 1,
  s: 1000,
  second: 1000,
  seconds: 1000,
  m: 60_000,
  minute: 60_000,
  minutes: 60_000,
  h: 3_600_000,
  hour: 3_600_000,
  hours: 3_600_000,
  d: 86_400_000,
  day: 86_400_000,
  days: 86_400_000,
};

/**
 * Parses a `DurationString` to milliseconds.
 *
 * Hand-rolled rather than a dependency, since the sandbox bundles it. The numeric part
 * accepts everything the type's `${number}` admits -- sign, leading dot, exponent --
 * because the type permits `".5s"` and `"1e3ms"` and a narrower parser would reject
 * valid input.
 */
export function durationToMs(duration: DurationString): number {
  const match = /^([+-]?(?:\d+\.?\d*|\.\d+)(?:e[+-]?\d+)?)\s*([a-z]+)$/i.exec(duration.trim());
  const unitMs = match ? DURATION_UNIT_MS[match[2]!.toLowerCase()] : undefined;
  if (!match || unitMs === undefined) {
    throw new Error(`Unparseable duration: ${duration}`);
  }
  return Number(match[1]) * unitMs;
}
