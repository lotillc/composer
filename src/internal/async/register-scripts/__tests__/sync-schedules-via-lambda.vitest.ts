/**
 * Tests for syncSchedulesViaLambda()
 */

import { ScheduleOverlapPolicy } from "@temporalio/client";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { Composer } from "../../../context-provider";
import type { ScheduleDefinition } from "../../schedule/define-schedule";

const mocks = vi.hoisted(() => ({
  lambdaSend: vi.fn(),
}));

vi.mock("@aws-sdk/client-lambda", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@aws-sdk/client-lambda")>();
  return {
    ...actual,
    LambdaClient: class {
      send = mocks.lambdaSend;
    },
  };
});

const { syncSchedulesViaLambda } = await import("../sync-schedules-via-lambda");

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

function createMockComposer(): Composer<unknown> {
  return {
    contextProvider: {
      beforeStep: vi.fn(),
      afterStep: vi.fn(),
    },
    logger: {
      info: vi.fn(),
      warn: vi.fn(),
      error: vi.fn(),
      debug: vi.fn(),
    },
    temporal: {
      serverAddress: "localhost:7233",
      namespace: "test-namespace",
      serviceName: "test-service",
    },
    runSyncWorkflow: vi.fn() as Composer<unknown>["runSyncWorkflow"],
    runAsyncWorkflow: vi.fn() as Composer<unknown>["runAsyncWorkflow"],
    startAsyncWorkflow: vi.fn() as Composer<unknown>["startAsyncWorkflow"],
    runActivityWorkers: vi.fn().mockResolvedValue(undefined),
    runWorkflowWorkers: vi.fn().mockResolvedValue(undefined),
    syncSchedules: vi.fn() as Composer<unknown>["syncSchedules"],
  } as unknown as Composer<unknown>;
}

function encodePayload(value: unknown): Uint8Array {
  return new TextEncoder().encode(JSON.stringify(value));
}

function getInvokeInput(): { FunctionName: string; Payload: Uint8Array } {
  expect(mocks.lambdaSend).toHaveBeenCalledTimes(1);
  const firstCall = mocks.lambdaSend.mock.calls[0]?.[0];
  expect(firstCall).toBeDefined();
  return (firstCall as { input: { FunctionName: string; Payload: Uint8Array } }).input;
}

function decodeSentPayload(): {
  namespace: string;
  schedules: ScheduleDefinition[];
  dryRun: boolean;
} {
  const { Payload } = getInvokeInput();
  return JSON.parse(new TextDecoder().decode(Payload));
}

describe("syncSchedulesViaLambda", () => {
  let mockExit: ReturnType<typeof vi.spyOn>;

  beforeEach(() => {
    mocks.lambdaSend.mockReset();
    mockExit = vi.spyOn(process, "exit").mockImplementation((code?: string | number | null) => {
      throw new Error(`process.exit(${String(code ?? 0)})`);
    });
  });

  describe("environment filtering", () => {
    it("sends only schedules matching the current environment", async () => {
      mocks.lambdaSend.mockResolvedValue({
        Payload: encodePayload({ created: [], updated: [], deleted: [], errors: [] }),
      });

      const composer = createMockComposer();
      await syncSchedulesViaLambda(composer, {
        schedules: [
          makeDefinition({ scheduleId: "prod-only", environments: ["prod"] }),
          makeDefinition({ scheduleId: "preview-only", environments: ["preview"] }),
          makeDefinition({ scheduleId: "both", environments: ["preview", "prod"] }),
        ],
        lambdaFunctionName: "test-schedule-sync",
        currentEnvironment: "preview",
      });

      const sent = decodeSentPayload();
      expect(sent.namespace).toBe("test-namespace");
      expect(sent.dryRun).toBe(false);
      expect(sent.schedules.map((s) => s.scheduleId)).toEqual(["preview-only", "both"]);
    });

    it("sends an empty schedules list when no schedules target the current environment", async () => {
      mocks.lambdaSend.mockResolvedValue({
        Payload: encodePayload({ created: [], updated: [], deleted: [], errors: [] }),
      });

      await syncSchedulesViaLambda(createMockComposer(), {
        schedules: [makeDefinition({ environments: ["prod"] })],
        lambdaFunctionName: "test-schedule-sync",
        currentEnvironment: "preview",
      });

      expect(decodeSentPayload().schedules).toEqual([]);
    });

    it("exits 1 when no current environment is resolvable and schedules are provided", async () => {
      vi.stubEnv("ENVIRONMENT_NAME", undefined);

      await expect(
        syncSchedulesViaLambda(createMockComposer(), {
          schedules: [makeDefinition()],
          lambdaFunctionName: "test-schedule-sync",
          currentEnvironment: null,
        }),
      ).rejects.toThrow("process.exit(1)");

      expect(mockExit).toHaveBeenCalledWith(1);
      expect(mocks.lambdaSend).not.toHaveBeenCalled();
    });
  });

  describe("emit mode", () => {
    it("writes the payload to stdout and does not invoke the Lambda", async () => {
      const stdoutSpy = vi.spyOn(process.stdout, "write").mockImplementation(() => true);

      await syncSchedulesViaLambda(createMockComposer(), {
        schedules: [makeDefinition({ scheduleId: "emitted", environments: ["prod"] })],
        lambdaFunctionName: "test-schedule-sync",
        currentEnvironment: "prod",
        mode: "emit",
      });

      expect(mocks.lambdaSend).not.toHaveBeenCalled();
      expect(stdoutSpy).toHaveBeenCalledTimes(1);

      const written = stdoutSpy.mock.calls[0]?.[0] as string;
      expect(written.endsWith("\n")).toBe(true);
      const parsed = JSON.parse(written);
      expect(parsed).toEqual({
        namespace: "test-namespace",
        dryRun: false,
        schedules: [expect.objectContaining({ scheduleId: "emitted" })],
      });
    });
  });

  describe("invoke mode", () => {
    it("logs a success summary when the Lambda returns no errors", async () => {
      mocks.lambdaSend.mockResolvedValue({
        Payload: encodePayload({
          namespace: "test-namespace",
          dryRun: false,
          created: ["a"],
          updated: ["b", "c"],
          deleted: [],
          errors: [],
        }),
      });

      const composer = createMockComposer();
      await syncSchedulesViaLambda(composer, {
        schedules: [makeDefinition({ scheduleId: "a", environments: ["prod"] })],
        lambdaFunctionName: "test-schedule-sync",
        currentEnvironment: "prod",
      });

      expect(getInvokeInput().FunctionName).toBe("test-schedule-sync");
      expect(mockExit).not.toHaveBeenCalled();
      expect(composer.logger.info).toHaveBeenCalledWith(
        "Schedule sync complete",
        expect.objectContaining({ created: 1, updated: 2, deleted: 0, errors: 0 }),
      );
    });

    it("exits 1 and logs each per-schedule error returned by the Lambda", async () => {
      mocks.lambdaSend.mockResolvedValue({
        Payload: encodePayload({
          created: [],
          updated: [],
          deleted: [],
          errors: [
            { scheduleId: "broken-1", error: "boom" },
            { scheduleId: "broken-2", error: "kaboom" },
          ],
        }),
      });

      const composer = createMockComposer();

      await expect(
        syncSchedulesViaLambda(composer, {
          schedules: [makeDefinition({ environments: ["prod"] })],
          lambdaFunctionName: "test-schedule-sync",
          currentEnvironment: "prod",
        }),
      ).rejects.toThrow("process.exit(1)");

      expect(mockExit).toHaveBeenCalledWith(1);
      expect(composer.logger.error).toHaveBeenCalledWith("Schedule failed to sync", {
        scheduleId: "broken-1",
        error: "boom",
      });
      expect(composer.logger.error).toHaveBeenCalledWith("Schedule failed to sync", {
        scheduleId: "broken-2",
        error: "kaboom",
      });
    });

    it("exits 1 when the Lambda returns a FunctionError", async () => {
      mocks.lambdaSend.mockResolvedValue({
        FunctionError: "Unhandled",
        Payload: encodePayload({ errorType: "ValidationError", errorMessage: "bad payload" }),
      });

      const composer = createMockComposer();

      await expect(
        syncSchedulesViaLambda(composer, {
          schedules: [makeDefinition({ environments: ["prod"] })],
          lambdaFunctionName: "test-schedule-sync",
          currentEnvironment: "prod",
        }),
      ).rejects.toThrow("process.exit(1)");

      expect(mockExit).toHaveBeenCalledWith(1);
      expect(composer.logger.error).toHaveBeenCalledWith(
        "Schedule-sync Lambda returned FunctionError",
        expect.objectContaining({ functionError: "Unhandled" }),
      );
    });

    it("exits 1 when the AWS SDK throws during invocation", async () => {
      mocks.lambdaSend.mockRejectedValue(new Error("network unreachable"));

      const composer = createMockComposer();

      await expect(
        syncSchedulesViaLambda(composer, {
          schedules: [makeDefinition({ environments: ["prod"] })],
          lambdaFunctionName: "test-schedule-sync",
          currentEnvironment: "prod",
        }),
      ).rejects.toThrow("process.exit(1)");

      expect(mockExit).toHaveBeenCalledWith(1);
      expect(composer.logger.error).toHaveBeenCalledWith(
        "Failed to invoke schedule-sync Lambda",
        expect.objectContaining({
          lambdaFunctionName: "test-schedule-sync",
          error: "network unreachable",
        }),
      );
    });
  });

  describe("options defaulting", () => {
    it("falls back to ENVIRONMENT_NAME when currentEnvironment is not passed", async () => {
      vi.stubEnv("ENVIRONMENT_NAME", "prod");
      mocks.lambdaSend.mockResolvedValue({
        Payload: encodePayload({ created: [], updated: [], deleted: [], errors: [] }),
      });

      await syncSchedulesViaLambda(createMockComposer(), {
        schedules: [makeDefinition({ scheduleId: "s1", environments: ["prod"] })],
        lambdaFunctionName: "test-schedule-sync",
      });

      expect(decodeSentPayload().schedules.map((s) => s.scheduleId)).toEqual(["s1"]);
    });

    it("falls back to COMPOSER_SCHEDULE_SYNC_DRY_RUN=true for dryRun", async () => {
      vi.stubEnv("COMPOSER_SCHEDULE_SYNC_DRY_RUN", "true");
      mocks.lambdaSend.mockResolvedValue({
        Payload: encodePayload({ created: [], updated: [], deleted: [], errors: [] }),
      });

      await syncSchedulesViaLambda(createMockComposer(), {
        schedules: [makeDefinition({ environments: ["prod"] })],
        lambdaFunctionName: "test-schedule-sync",
        currentEnvironment: "prod",
      });

      expect(decodeSentPayload().dryRun).toBe(true);
    });
  });
});
