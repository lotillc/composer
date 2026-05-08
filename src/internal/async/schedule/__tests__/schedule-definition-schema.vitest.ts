/**
 * Tests for `scheduleDefinitionSchema`.
 *
 * The schema runs at the Lambda trust boundary; its main non-trivial job is
 * rehydrating `Date`-valued spec fields that do not survive the
 * `JSON.stringify` hop between the composer-instance CLI and the
 * schedule-sync Lambda.
 */

import { ScheduleOverlapPolicy } from "@temporalio/client";
import { describe, expect, it } from "vitest";
import type { ScheduleDefinition } from "../define-schedule";
import { scheduleDefinitionSchema } from "../schedule-definition-schema";

function makeDefinition(overrides?: Partial<ScheduleDefinition>): ScheduleDefinition {
  return {
    __scheduleDefinition: true,
    scheduleId: "date-bound",
    workflowName: "wf",
    initialData: {},
    configuredValues: {},
    spec: { intervals: [{ every: "1h" }] },
    overlap: ScheduleOverlapPolicy.SKIP,
    paused: false,
    taskQueue: "workflow-tasks",
    environments: ["prod"],
    ...overrides,
  };
}

describe("scheduleDefinitionSchema", () => {
  describe("spec.startAt / spec.endAt JSON round-trip", () => {
    it("rehydrates ISO-string startAt and endAt back to Date instances after JSON round-trip", () => {
      const startAt = new Date("2026-01-01T00:00:00.000Z");
      const endAt = new Date("2026-12-31T23:59:59.000Z");

      const definition = makeDefinition({
        spec: { intervals: [{ every: "1h" }], startAt, endAt },
      });

      const roundTripped = JSON.parse(JSON.stringify(definition));
      expect(typeof roundTripped.spec.startAt).toBe("string");
      expect(typeof roundTripped.spec.endAt).toBe("string");

      const parsed = scheduleDefinitionSchema.parse(roundTripped);

      expect(parsed.spec.startAt).toBeInstanceOf(Date);
      expect(parsed.spec.endAt).toBeInstanceOf(Date);
      expect((parsed.spec.startAt as Date).toISOString()).toBe(startAt.toISOString());
      expect((parsed.spec.endAt as Date).toISOString()).toBe(endAt.toISOString());
    });

    it("accepts Date instances without modification (direct path)", () => {
      const startAt = new Date("2026-06-01T12:00:00.000Z");

      const parsed = scheduleDefinitionSchema.parse(
        makeDefinition({ spec: { intervals: [{ every: "1h" }], startAt } }),
      );

      expect(parsed.spec.startAt).toBeInstanceOf(Date);
      expect((parsed.spec.startAt as Date).getTime()).toBe(startAt.getTime());
    });

    it("treats startAt and endAt as optional", () => {
      const parsed = scheduleDefinitionSchema.parse(
        makeDefinition({ spec: { calendars: [{ hour: 9, minute: 0 }] } }),
      );

      expect(parsed.spec.startAt).toBeUndefined();
      expect(parsed.spec.endAt).toBeUndefined();
    });

    it("rejects unparseable startAt strings", () => {
      const definition = makeDefinition({
        spec: { intervals: [{ every: "1h" }], startAt: "not-a-date" as unknown as Date },
      });

      expect(() => scheduleDefinitionSchema.parse(definition)).toThrow();
    });
  });

  describe("spec pass-through", () => {
    it("preserves unknown spec fields untouched (looseObject)", () => {
      const parsed = scheduleDefinitionSchema.parse(
        makeDefinition({
          spec: {
            calendars: [{ hour: 9, minute: 0 }],
            cronExpressions: ["0 9 * * *"],
            timezoneName: "UTC",
          } as unknown as ReturnType<typeof makeDefinition>["spec"],
        }),
      );

      expect(parsed.spec).toMatchObject({
        calendars: [{ hour: 9, minute: 0 }],
        cronExpressions: ["0 9 * * *"],
        timezoneName: "UTC",
      });
    });
  });
});
