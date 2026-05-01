/**
 * Zod schema for `ScheduleDefinition`.
 *
 * Used at trust boundaries where schedule definitions arrive as JSON from
 * another process -- notably the `schedule-sync` Lambda's event payload. The
 * schema mirrors the `ScheduleDefinition` interface and matches the shape
 * produced by `defineSchedule()`.
 *
 * @module schedule-definition-schema
 */
import { z } from "zod";

/**
 * Overlap policy values accepted by `ScheduleDefinition.overlap`.
 *
 * Mirrors the string values exported by `@temporalio/client`'s
 * `ScheduleOverlapPolicy` (excluding the deprecated `UNSPECIFIED`). Hand-listed
 * rather than derived from the SDK object because the SDK includes an
 * `UNSPECIFIED: undefined` entry that is not a valid Zod enum value.
 */
export const scheduleOverlapPolicySchema = z.enum([
  "SKIP",
  "BUFFER_ONE",
  "BUFFER_ALL",
  "CANCEL_OTHER",
  "TERMINATE_OTHER",
  "ALLOW_ALL",
]);

/**
 * Zod schema for `ScheduleSpec`.
 *
 * Passes unrecognized fields through untouched so detailed `ScheduleSpec`
 * shape validation stays with the Temporal client (which runs downstream of
 * this schema). The known `Date`-valued fields (`startAt`, `endAt`) are
 * explicitly coerced because schedule definitions are `JSON.stringify`ed
 * when shipped to the schedule-sync Lambda, which loses `Date` identity --
 * without this coercion, `startAt`/`endAt` would arrive as ISO strings and
 * the Temporal client would reject them when constructing the protobuf
 * payload.
 *
 * `z.coerce.date()` accepts both real `Date` instances (direct path, where
 * this schema is not strictly needed but may still be applied) and ISO
 * strings (Lambda path, after JSON round-trip).
 */
const scheduleSpecSchema = z.looseObject({
  startAt: z.coerce.date().optional(),
  endAt: z.coerce.date().optional(),
});

/**
 * Zod schema for `ScheduleDefinition`.
 *
 * Structurally mirrors the `ScheduleDefinition` interface produced by
 * `defineSchedule()`. Used at trust boundaries where schedule definitions
 * arrive as JSON from another process -- notably the schedule-sync Lambda.
 *
 * Note: the inferred output type preserves structural compatibility with
 * `ScheduleDefinition`, but `spec` widens to a loose object. Callers handing
 * the parsed value to `syncSchedules()` should cast the array as
 * `ScheduleDefinition[]`.
 */
export const scheduleDefinitionSchema = z.strictObject({
  __scheduleDefinition: z.literal(true),
  scheduleId: z.string().min(1),
  workflowName: z.string().min(1),
  initialData: z.record(z.string(), z.unknown()),
  configuredValues: z.record(z.string(), z.unknown()),
  spec: scheduleSpecSchema,
  overlap: scheduleOverlapPolicySchema,
  catchupWindow: z.union([z.number(), z.string()]).optional(),
  paused: z.boolean(),
  note: z.string().optional(),
  taskQueue: z.string().min(1),
  environments: z.array(z.string().min(1)).min(1),
});
