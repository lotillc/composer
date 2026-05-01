import { ScheduleOverlapPolicy } from "@temporalio/client";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { MANAGED_BY_MEMO_KEY, MANAGED_BY_MEMO_VALUE } from "../constants";
import type { ScheduleDefinition } from "../define-schedule";

const { mockCreate, mockGetHandle, mockScheduleList, mockConnectionClose } = vi.hoisted(() => ({
  mockCreate: vi.fn(),
  mockGetHandle: vi.fn(),
  mockScheduleList: vi.fn(),
  mockConnectionClose: vi.fn().mockResolvedValue(undefined),
}));

vi.mock("@temporalio/client", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@temporalio/client")>();
  return {
    ...actual,
    Connection: {
      connect: vi.fn().mockResolvedValue({ close: mockConnectionClose }),
    },
    Client: class {
      schedule = {
        create: mockCreate,
        getHandle: mockGetHandle,
        list: mockScheduleList,
      };
    },
  };
});

function createMockListIterator(
  summaries: Array<{ scheduleId: string; memo?: Record<string, unknown> }>,
) {
  return (async function* () {
    for (const s of summaries) {
      yield s;
    }
  })();
}

function makeDefinition(overrides?: Partial<ScheduleDefinition>): ScheduleDefinition {
  return {
    __scheduleDefinition: true,
    scheduleId: "test-schedule",
    workflowName: "test-workflow",
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

const silentLogger = {
  info: vi.fn(),
  warn: vi.fn(),
  error: vi.fn(),
  debug: vi.fn(),
};

describe("syncSchedules", () => {
  beforeEach(() => {
    mockCreate.mockReset();
    mockGetHandle.mockReset();
    mockScheduleList.mockReset();
    silentLogger.info.mockReset();
  });

  it("creates a new schedule when it does not exist", async () => {
    mockScheduleList.mockReturnValue(createMockListIterator([]));
    mockCreate.mockResolvedValue(undefined);

    const { syncSchedules } = await import("../sync-schedules");

    const result = await syncSchedules({
      temporalConfig: { address: "localhost:7233", namespace: "default" },
      schedules: [makeDefinition({ scheduleId: "new-schedule" })],
      logger: silentLogger,
    });

    expect(result.created).toEqual(["new-schedule"]);
    expect(result.updated).toEqual([]);
    expect(result.deleted).toEqual([]);
    expect(result.errors).toEqual([]);

    expect(mockCreate).toHaveBeenCalledWith(
      expect.objectContaining({
        scheduleId: "new-schedule",
        action: expect.objectContaining({
          type: "startWorkflow",
          workflowType: "test-workflow",
          taskQueue: "workflow-tasks",
        }),
        memo: { [MANAGED_BY_MEMO_KEY]: MANAGED_BY_MEMO_VALUE },
      }),
    );
  });

  it("updates an existing composer-managed schedule", async () => {
    const mockUpdate = vi.fn().mockResolvedValue(undefined);
    mockScheduleList.mockReturnValue(
      createMockListIterator([
        { scheduleId: "existing-schedule", memo: { [MANAGED_BY_MEMO_KEY]: MANAGED_BY_MEMO_VALUE } },
      ]),
    );
    mockGetHandle.mockReturnValue({ update: mockUpdate, delete: vi.fn() });

    const { syncSchedules } = await import("../sync-schedules");

    const result = await syncSchedules({
      temporalConfig: { address: "localhost:7233", namespace: "default" },
      schedules: [makeDefinition({ scheduleId: "existing-schedule" })],
      logger: silentLogger,
    });

    expect(result.updated).toEqual(["existing-schedule"]);
    expect(result.created).toEqual([]);
    expect(result.deleted).toEqual([]);
    expect(mockUpdate).toHaveBeenCalled();
  });

  it("deletes composer-managed schedules not in definitions", async () => {
    const mockDelete = vi.fn().mockResolvedValue(undefined);
    mockScheduleList.mockReturnValue(
      createMockListIterator([
        { scheduleId: "stale-schedule", memo: { [MANAGED_BY_MEMO_KEY]: MANAGED_BY_MEMO_VALUE } },
      ]),
    );
    mockGetHandle.mockReturnValue({ delete: mockDelete });

    const { syncSchedules } = await import("../sync-schedules");

    const result = await syncSchedules({
      temporalConfig: { address: "localhost:7233", namespace: "default" },
      schedules: [],
      logger: silentLogger,
    });

    expect(result.deleted).toEqual(["stale-schedule"]);
    expect(mockDelete).toHaveBeenCalled();
  });

  it("ignores non-composer schedules when deleting", async () => {
    mockScheduleList.mockReturnValue(
      createMockListIterator([{ scheduleId: "external-schedule", memo: { someOther: "value" } }]),
    );

    const { syncSchedules } = await import("../sync-schedules");

    const result = await syncSchedules({
      temporalConfig: { address: "localhost:7233", namespace: "default" },
      schedules: [],
      logger: silentLogger,
    });

    expect(result.deleted).toEqual([]);
    expect(mockGetHandle).not.toHaveBeenCalled();
  });

  it("ignores schedules with no memo when deleting", async () => {
    mockScheduleList.mockReturnValue(createMockListIterator([{ scheduleId: "no-memo-schedule" }]));

    const { syncSchedules } = await import("../sync-schedules");

    const result = await syncSchedules({
      temporalConfig: { address: "localhost:7233", namespace: "default" },
      schedules: [],
      logger: silentLogger,
    });

    expect(result.deleted).toEqual([]);
  });

  it("uses plain workflow name", async () => {
    mockScheduleList.mockReturnValue(createMockListIterator([]));
    mockCreate.mockResolvedValue(undefined);

    const { syncSchedules } = await import("../sync-schedules");

    await syncSchedules({
      temporalConfig: { address: "localhost:7233", namespace: "default" },
      schedules: [makeDefinition({ workflowName: "my-workflow" })],
      logger: silentLogger,
    });

    expect(mockCreate).toHaveBeenCalledWith(
      expect.objectContaining({
        action: expect.objectContaining({
          workflowType: "my-workflow",
        }),
      }),
    );
  });

  it("reports dry-run actions without making changes", async () => {
    mockScheduleList.mockReturnValue(
      createMockListIterator([
        { scheduleId: "existing", memo: { [MANAGED_BY_MEMO_KEY]: MANAGED_BY_MEMO_VALUE } },
        { scheduleId: "to-delete", memo: { [MANAGED_BY_MEMO_KEY]: MANAGED_BY_MEMO_VALUE } },
      ]),
    );

    const { syncSchedules } = await import("../sync-schedules");

    const result = await syncSchedules({
      temporalConfig: { address: "localhost:7233", namespace: "default" },
      schedules: [
        makeDefinition({ scheduleId: "existing" }),
        makeDefinition({ scheduleId: "brand-new" }),
      ],
      dryRun: true,
      logger: silentLogger,
    });

    expect(result.created).toEqual(["brand-new"]);
    expect(result.updated).toEqual(["existing"]);
    expect(result.deleted).toEqual(["to-delete"]);
    expect(mockCreate).not.toHaveBeenCalled();
    expect(mockGetHandle).not.toHaveBeenCalled();
  });

  it("throws on duplicate scheduleIds in definitions", async () => {
    mockScheduleList.mockReturnValue(createMockListIterator([]));

    const { syncSchedules } = await import("../sync-schedules");

    await expect(
      syncSchedules({
        temporalConfig: { address: "localhost:7233", namespace: "default" },
        schedules: [makeDefinition({ scheduleId: "dup" }), makeDefinition({ scheduleId: "dup" })],
        logger: silentLogger,
      }),
    ).rejects.toThrow('Duplicate scheduleId "dup"');
  });

  it("captures errors for individual schedules without stopping others", async () => {
    mockScheduleList.mockReturnValue(createMockListIterator([]));
    mockCreate
      .mockRejectedValueOnce(new Error("connection refused"))
      .mockResolvedValueOnce(undefined);

    const { syncSchedules } = await import("../sync-schedules");

    const result = await syncSchedules({
      temporalConfig: { address: "localhost:7233", namespace: "default" },
      schedules: [
        makeDefinition({ scheduleId: "will-fail" }),
        makeDefinition({ scheduleId: "will-succeed" }),
      ],
      logger: silentLogger,
    });

    expect(result.errors).toEqual([{ scheduleId: "will-fail", error: "connection refused" }]);
    expect(result.created).toContain("will-succeed");
  });

  it("wraps initialData in WorkflowInput format", async () => {
    mockScheduleList.mockReturnValue(createMockListIterator([]));
    mockCreate.mockResolvedValue(undefined);

    const { syncSchedules } = await import("../sync-schedules");

    await syncSchedules({
      temporalConfig: { address: "localhost:7233", namespace: "default" },
      schedules: [makeDefinition({ initialData: { key: "value" } })],
      logger: silentLogger,
    });

    expect(mockCreate).toHaveBeenCalledWith(
      expect.objectContaining({
        action: expect.objectContaining({
          args: [{ initialData: { key: "value" } }],
        }),
      }),
    );
  });

  it("closes the Temporal connection after sync completes", async () => {
    mockScheduleList.mockReturnValue(createMockListIterator([]));

    const { syncSchedules } = await import("../sync-schedules");

    await syncSchedules({
      temporalConfig: { address: "localhost:7233", namespace: "default" },
      schedules: [],
      logger: silentLogger,
    });

    expect(mockConnectionClose).toHaveBeenCalled();
  });

  it("closes the Temporal connection even when sync throws", async () => {
    mockScheduleList.mockReturnValue(createMockListIterator([]));

    const { syncSchedules } = await import("../sync-schedules");

    await expect(
      syncSchedules({
        temporalConfig: { address: "localhost:7233", namespace: "default" },
        schedules: [makeDefinition({ scheduleId: "dup" }), makeDefinition({ scheduleId: "dup" })],
        logger: silentLogger,
      }),
    ).rejects.toThrow("Duplicate");

    expect(mockConnectionClose).toHaveBeenCalled();
  });

  it("refuses to overwrite a non-composer schedule on ScheduleAlreadyRunning", async () => {
    const { ScheduleAlreadyRunning } = await import("@temporalio/client");
    mockScheduleList.mockReturnValue(createMockListIterator([]));
    mockCreate.mockRejectedValue(new ScheduleAlreadyRunning("already running", "ext-schedule"));
    mockGetHandle.mockReturnValue({
      describe: vi.fn().mockResolvedValue({ memo: { someOther: "value" } }),
      update: vi.fn(),
      delete: vi.fn(),
    });

    const { syncSchedules } = await import("../sync-schedules");

    const result = await syncSchedules({
      temporalConfig: { address: "localhost:7233", namespace: "default" },
      schedules: [makeDefinition({ scheduleId: "ext-schedule" })],
      logger: silentLogger,
    });

    expect(result.errors).toHaveLength(1);
    expect(result.errors[0]!.error).toContain("not managed by composer");
  });

  it("falls back to update on ScheduleAlreadyRunning for composer-managed schedule", async () => {
    const { ScheduleAlreadyRunning } = await import("@temporalio/client");
    const mockUpdate = vi.fn().mockResolvedValue(undefined);
    mockScheduleList.mockReturnValue(createMockListIterator([]));
    mockCreate.mockRejectedValue(new ScheduleAlreadyRunning("already running", "race-schedule"));
    mockGetHandle.mockReturnValue({
      describe: vi
        .fn()
        .mockResolvedValue({ memo: { [MANAGED_BY_MEMO_KEY]: MANAGED_BY_MEMO_VALUE } }),
      update: mockUpdate,
      delete: vi.fn(),
    });

    const { syncSchedules } = await import("../sync-schedules");

    const result = await syncSchedules({
      temporalConfig: { address: "localhost:7233", namespace: "default" },
      schedules: [makeDefinition({ scheduleId: "race-schedule" })],
      logger: silentLogger,
    });

    expect(result.created).toEqual(["race-schedule"]);
    expect(result.errors).toEqual([]);
    expect(mockUpdate).toHaveBeenCalled();
  });

  it("merges configuredValues into initialData for workflow args", async () => {
    mockScheduleList.mockReturnValue(createMockListIterator([]));
    mockCreate.mockResolvedValue(undefined);

    const { syncSchedules } = await import("../sync-schedules");

    await syncSchedules({
      temporalConfig: { address: "localhost:7233", namespace: "default" },
      schedules: [
        makeDefinition({
          initialData: { key: "from-initial", extra: "initial-only" },
          configuredValues: { key: "from-config", config: "config-only" },
        }),
      ],
      logger: silentLogger,
    });

    expect(mockCreate).toHaveBeenCalledWith(
      expect.objectContaining({
        action: expect.objectContaining({
          args: [
            {
              initialData: {
                key: "from-config",
                extra: "initial-only",
                config: "config-only",
              },
            },
          ],
        }),
      }),
    );
  });
});
