import { ScheduleOverlapPolicy } from "@temporalio/client";
import { describe, expect, it } from "vitest";
import type { Workflow } from "../../../dag-sync-workflow";
import { defineSchedule, type ScheduleDefinition } from "../define-schedule";

function makeWorkflow(name: string): Workflow<Record<string, unknown>, never, object, readonly []> {
  return {
    name,
    steps: [],
    requiredInitial: undefined,
  } as unknown as Workflow<Record<string, unknown>, never, object, readonly []>;
}

describe("defineSchedule", () => {
  it("returns a ScheduleDefinition with all required fields", () => {
    const workflow = makeWorkflow("test-workflow");

    const result = defineSchedule({
      scheduleId: "my-schedule",
      workflow,
      initialData: {},
      spec: { calendars: [{ hour: 8 }] },
      environments: ["prod"],
    });

    expect(result).toEqual({
      __scheduleDefinition: true,
      scheduleId: "my-schedule",
      workflowName: "test-workflow",
      initialData: {},
      configuredValues: {},
      spec: { calendars: [{ hour: 8 }] },
      overlap: ScheduleOverlapPolicy.SKIP,
      catchupWindow: undefined,
      paused: false,
      note: undefined,
      taskQueue: "workflow-tasks",
      environments: ["prod"],
    } satisfies ScheduleDefinition);
  });

  it("uses provided optional fields", () => {
    const workflow = makeWorkflow("report-wf");

    const result = defineSchedule({
      scheduleId: "daily-report",
      workflow,
      initialData: { reportType: "daily" },
      spec: { intervals: [{ every: "1h" }] },
      overlap: ScheduleOverlapPolicy.BUFFER_ONE,
      catchupWindow: "1 day",
      paused: true,
      note: "Daily report schedule",
      taskQueue: "report-tasks",
      environments: ["preview", "prod"],
    });

    expect(result.overlap).toBe(ScheduleOverlapPolicy.BUFFER_ONE);
    expect(result.catchupWindow).toBe("1 day");
    expect(result.paused).toBe(true);
    expect(result.note).toBe("Daily report schedule");
    expect(result.taskQueue).toBe("report-tasks");
    expect(result.initialData).toEqual({ reportType: "daily" });
    expect(result.environments).toEqual(["preview", "prod"]);
  });

  it("defaults overlap to SKIP", () => {
    const result = defineSchedule({
      scheduleId: "test",
      workflow: makeWorkflow("wf"),
      initialData: {},
      spec: { intervals: [{ every: "1h" }] },
      environments: ["prod"],
    });

    expect(result.overlap).toBe(ScheduleOverlapPolicy.SKIP);
  });

  it("defaults paused to false", () => {
    const result = defineSchedule({
      scheduleId: "test",
      workflow: makeWorkflow("wf"),
      initialData: {},
      spec: { intervals: [{ every: "1h" }] },
      environments: ["prod"],
    });

    expect(result.paused).toBe(false);
  });

  it("defaults taskQueue to workflow-tasks", () => {
    const result = defineSchedule({
      scheduleId: "test",
      workflow: makeWorkflow("wf"),
      initialData: {},
      spec: { intervals: [{ every: "1h" }] },
      environments: ["prod"],
    });

    expect(result.taskQueue).toBe("workflow-tasks");
  });

  it("treats undefined initialData as empty object", () => {
    const result = defineSchedule({
      scheduleId: "test",
      workflow: makeWorkflow("wf"),
      initialData: undefined,
      spec: { intervals: [{ every: "1h" }] },
      environments: ["prod"],
    });

    expect(result.initialData).toEqual({});
  });

  it("extracts workflow name from the workflow definition", () => {
    const workflow = makeWorkflow("specific-name");

    const result = defineSchedule({
      scheduleId: "test",
      workflow,
      initialData: {},
      spec: { intervals: [{ every: "1h" }] },
      environments: ["prod"],
    });

    expect(result.workflowName).toBe("specific-name");
  });

  it("sets __scheduleDefinition marker", () => {
    const result = defineSchedule({
      scheduleId: "test",
      workflow: makeWorkflow("wf"),
      initialData: {},
      spec: { intervals: [{ every: "1h" }] },
      environments: ["prod"],
    });

    expect(result.__scheduleDefinition).toBe(true);
  });

  it("copies environments to prevent external mutation", () => {
    const envs = ["prod"];
    const result = defineSchedule({
      scheduleId: "test",
      workflow: makeWorkflow("wf"),
      initialData: {},
      spec: { intervals: [{ every: "1h" }] },
      environments: envs,
    });

    expect(result.environments).toEqual(["prod"]);
    expect(result.environments).not.toBe(envs);
  });

  it("throws when scheduleId is empty", () => {
    expect(() =>
      defineSchedule({
        scheduleId: "",
        workflow: makeWorkflow("wf"),
        initialData: {},
        spec: { intervals: [{ every: "1h" }] },
        environments: ["prod"],
      }),
    ).toThrow("scheduleId is required");
  });

  it("throws when workflow is invalid", () => {
    expect(() =>
      defineSchedule({
        scheduleId: "test",
        workflow: {} as unknown as ReturnType<typeof makeWorkflow>,
        initialData: {},
        spec: { intervals: [{ every: "1h" }] },
        environments: ["prod"],
      }),
    ).toThrow("workflow is required");
  });

  it("throws when spec is missing", () => {
    expect(() =>
      defineSchedule({
        scheduleId: "test",
        workflow: makeWorkflow("wf"),
        initialData: {},
        spec: undefined as never,
        environments: ["prod"],
      }),
    ).toThrow("spec is required");
  });

  it("throws when environments is missing", () => {
    expect(() =>
      defineSchedule({
        scheduleId: "test",
        workflow: makeWorkflow("wf"),
        initialData: {},
        spec: { intervals: [{ every: "1h" }] },
        environments: undefined as never,
      }),
    ).toThrow("environments is required");
  });

  it("throws when environments is an empty array", () => {
    expect(() =>
      defineSchedule({
        scheduleId: "test",
        workflow: makeWorkflow("wf"),
        initialData: {},
        spec: { intervals: [{ every: "1h" }] },
        environments: [],
      }),
    ).toThrow("environments is required");
  });

  it("throws when environments contains a non-string value", () => {
    expect(() =>
      defineSchedule({
        scheduleId: "test",
        workflow: makeWorkflow("wf"),
        initialData: {},
        spec: { intervals: [{ every: "1h" }] },
        environments: ["prod", 42 as unknown as string],
      }),
    ).toThrow("environments is required");
  });

  it("captures configuredValues from the workflow", () => {
    const workflow = {
      ...makeWorkflow("configured-wf"),
      configuredValues: { region: "us-east-1", mode: "production" },
    } as unknown as Workflow<Record<string, unknown>, never, object, readonly []>;

    const result = defineSchedule({
      scheduleId: "test",
      workflow,
      initialData: {},
      spec: { intervals: [{ every: "1h" }] },
      environments: ["prod"],
    });

    expect(result.configuredValues).toEqual({ region: "us-east-1", mode: "production" });
  });

  it("defaults configuredValues to empty object when workflow has none", () => {
    const result = defineSchedule({
      scheduleId: "test",
      workflow: makeWorkflow("wf"),
      initialData: {},
      spec: { intervals: [{ every: "1h" }] },
      environments: ["prod"],
    });

    expect(result.configuredValues).toEqual({});
  });
});
