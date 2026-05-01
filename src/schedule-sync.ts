/**
 * Schedule sync entry point.
 *
 * Exposed as a dedicated subpath export (@lotiai/composer/schedule-sync) so
 * infrastructure Lambdas can import the reconciler and its payload schema
 * without pulling in the full Composer framework surface (worker runtime,
 * webpack, swc native bindings, chokidar, jiti, etc.) that the root barrel
 * and ./internal both drag in via create-composer + register helpers.
 *
 * We import directly from leaf modules here -- NOT via ./internal -- so
 * esbuild never traces through create-composer.ts and its worker imports.
 * Keep this module narrowly focused on server-side reconciliation.
 */

export type { ScheduleDefinition } from "./internal/async/schedule/define-schedule";
export {
  scheduleDefinitionSchema,
  scheduleOverlapPolicySchema,
} from "./internal/async/schedule/schedule-definition-schema";
export {
  type SyncSchedulesResult,
  syncSchedules,
} from "./internal/async/schedule/sync-schedules";
