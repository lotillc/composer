/**
 * Tests for Temporal Activity Worker
 *
 * Tests the declarative workflow registration approach where workflows
 * are passed directly and steps are extracted from them.
 */

import { ApplicationFailure } from "@temporalio/activity";
import type { DataConverter } from "@temporalio/common";
import type { MockedFunction } from "vitest";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  createActivityWorkers,
  runActivityWorkers,
  type ActivityWorkerConfig,
} from "../activity-worker";
import type { StepContextProvider } from "../../../context-provider";
import type { ComposerLogger } from "../../../types";

type AsyncVoidFn = () => Promise<void>;
type MockConnection = { close: MockedFunction<AsyncVoidFn> };
type MockWorkerInstance = {
  run: MockedFunction<AsyncVoidFn>;
  shutdown: MockedFunction<() => void>;
};
type MockMetricsHandle = {
  stop: MockedFunction<AsyncVoidFn>;
  activityStarted: MockedFunction<() => void>;
  activityFinished: MockedFunction<() => void>;
};
type ConnectionOptions = { address: string };
type WorkerCreateOptions = {
  connection: MockConnection;
  namespace: string;
  taskQueue: string;
  activities: Record<string, unknown>;
  maxConcurrentActivityTaskExecutions: number;
  dataConverter?: DataConverter;
  interceptors?: { activity?: unknown[] };
};
type MockActivityContext = {
  info: {
    workflowExecution: {
      workflowId: string;
      runId: string;
    };
    activityId: string;
    attempt: number;
  };
};
type MockStepContext = { em: Record<string, unknown> };
type MockContextProvider = StepContextProvider<MockStepContext> & {
  beforeStep: MockedFunction<(stepName: string) => Promise<MockStepContext>>;
  afterStep: MockedFunction<(ctx: MockStepContext, error?: Error) => Promise<void>>;
};
type MockLogger = {
  info: MockedFunction<ComposerLogger["info"]>;
  warn: MockedFunction<ComposerLogger["warn"]>;
  error: MockedFunction<ComposerLogger["error"]>;
  debug: MockedFunction<ComposerLogger["debug"]>;
};

const mockActivityCurrent = vi.hoisted(() => vi.fn<() => MockActivityContext>());
const mockConnect = vi.hoisted(() => vi.fn<(options: ConnectionOptions) => Promise<MockConnection>>());
const mockWorkerCreate = vi.hoisted(() =>
  vi.fn<(options: WorkerCreateOptions) => Promise<MockWorkerInstance>>(),
);
const mockStartTaskQueueMetrics = vi.hoisted(() =>
  vi.fn<
    (options: {
      connection: MockConnection;
      taskQueues: string[];
      temporalNamespace: string;
      logger: unknown;
    }) => MockMetricsHandle
  >(),
);

vi.mock("@temporalio/worker", () => ({
  NativeConnection: { connect: mockConnect },
  Worker: { create: mockWorkerCreate },
}));
// importOriginal keeps the real ApplicationFailure, which the worker uses to
// wrap coded step errors; only the activity Context is replaced.
vi.mock("@temporalio/activity", async (importOriginal) => ({
  ...((await importOriginal()) as Record<string, unknown>),
  Context: { current: mockActivityCurrent },
}));
vi.mock("@temporalio/common", () => ({
  VersioningBehavior: { PINNED: 2 },
}));
vi.mock("../../metrics/task-queue-metrics", () => ({
  startTaskQueueMetrics: mockStartTaskQueueMetrics,
}));

const createMockContextProvider = (): MockContextProvider => ({
  beforeStep: vi.fn<(stepName: string) => Promise<MockStepContext>>().mockResolvedValue({ em: {} }),
  afterStep: vi
    .fn<(context: MockStepContext, error?: Error) => Promise<void>>()
    .mockResolvedValue(undefined),
});

const mockStepRun = vi.fn().mockResolvedValue({ output: "test-result" });
const anotherStepRun = vi.fn().mockResolvedValue({ output2: "another-result" });
const mockErrorHandler = vi.fn().mockResolvedValue(undefined);

const mockWorkflows = [
  {
    name: "test-workflow",
    steps: [
      {
        name: "testStep",
        needs: ["input"] as const,
        provides: ["output"] as const,
        run: mockStepRun,
      },
      {
        name: "anotherStep",
        needs: ["input2"] as const,
        provides: ["output2"] as const,
        run: anotherStepRun,
      },
    ],
    errorHandler: mockErrorHandler,
  },
] as unknown as ActivityWorkerConfig<MockStepContext>["workflows"];

const createTestConfig = (
  overrides: Partial<ActivityWorkerConfig<MockStepContext>> = {},
): ActivityWorkerConfig<MockStepContext> => ({
  serverAddress: "localhost:7233",
  namespace: "default",
  deploymentSeriesName: "test-activities",
  taskQueues: ["fast-tasks", "standard-tasks", "heavy-tasks"],
  maxConcurrentActivityTaskExecutions: 100,
  workflows: mockWorkflows,
  contextProvider: createMockContextProvider(),
  ...overrides,
});

const makeLogger = (): MockLogger => ({
  info: vi.fn<ComposerLogger["info"]>(),
  warn: vi.fn<ComposerLogger["warn"]>(),
  error: vi.fn<ComposerLogger["error"]>(),
  debug: vi.fn<ComposerLogger["debug"]>(),
});

describe("Activity Worker", () => {
  let mockConnection: MockConnection;
  let mockWorkerInstance: MockWorkerInstance;
  let mockMetricsHandle: MockMetricsHandle;

  beforeEach(() => {
    vi.stubEnv("ENVIRONMENT_NAME", "local");

    vi.spyOn(console, "log").mockImplementation(() => undefined);
    vi.spyOn(console, "info").mockImplementation(() => undefined);
    vi.spyOn(console, "warn").mockImplementation(() => undefined);
    vi.spyOn(console, "error").mockImplementation(() => undefined);
    vi.spyOn(console, "debug").mockImplementation(() => undefined);

    mockStepRun.mockReset().mockResolvedValue({ output: "test-result" });

    mockActivityCurrent.mockReturnValue({
      info: {
        workflowExecution: {
          workflowId: "test-workflow-id",
          runId: "test-run-id",
        },
        activityId: "test-activity-id",
        attempt: 2,
      },
    });

    mockConnection = {
      close: vi.fn<AsyncVoidFn>().mockResolvedValue(undefined),
    };
    mockConnect.mockResolvedValue(mockConnection);

    mockWorkerInstance = {
      run: vi.fn<AsyncVoidFn>().mockResolvedValue(undefined),
      shutdown: vi.fn<() => void>(),
    };
    mockWorkerCreate.mockResolvedValue(mockWorkerInstance);

    mockMetricsHandle = {
      stop: vi.fn<AsyncVoidFn>().mockResolvedValue(undefined),
      activityStarted: vi.fn<() => void>(),
      activityFinished: vi.fn<() => void>(),
    };
    mockStartTaskQueueMetrics.mockReturnValue(mockMetricsHandle);

    vi.spyOn(process, "exit").mockImplementation(() => undefined as never);
  });

  afterEach(() => {
    process.removeAllListeners("SIGINT");
    process.removeAllListeners("SIGTERM");
    vi.restoreAllMocks();
  });

  describe("createActivityWorkers", () => {
    it("should create workers from provided workflows", async () => {
      const { workers, connection } = await createActivityWorkers(createTestConfig());

      expect(workers).toHaveLength(3);
      expect(connection).toBe(mockConnection);
    });

    it("should register step activities extracted from workflows", async () => {
      await createActivityWorkers(createTestConfig());

      const createCall = mockWorkerCreate.mock.calls[0]?.[0];
      expect(createCall).toBeDefined();
      const activityNames = Object.keys(createCall!.activities);

      expect(activityNames).toContain("testStep");
      expect(activityNames).toContain("anotherStep");
    });

    it("should register error handler activities from workflows", async () => {
      await createActivityWorkers(createTestConfig());

      const createCall = mockWorkerCreate.mock.calls[0]?.[0];
      expect(createCall).toBeDefined();
      const activityNames = Object.keys(createCall!.activities);

      expect(activityNames).toContain("test-workflow__errorHandler");
    });

    it("should create workers for all specified task queues", async () => {
      const taskQueues = ["queue-1", "queue-2", "queue-3"];
      await createActivityWorkers(createTestConfig({ taskQueues }));

      expect(mockWorkerCreate).toHaveBeenCalledTimes(3);
      for (const taskQueue of taskQueues) {
        expect(mockWorkerCreate).toHaveBeenCalledWith(expect.objectContaining({ taskQueue }));
      }
    });

    it("should throw error if no task queues configured", async () => {
      await expect(createActivityWorkers(createTestConfig({ taskQueues: [] }))).rejects.toThrow(
        "No task queues configured",
      );
    });

    it("should start task queue metrics for the worker namespace and queues", async () => {
      const taskQueues = ["queue-1", "queue-2"];
      await createActivityWorkers(createTestConfig({ namespace: "test-namespace", taskQueues }));

      expect(mockStartTaskQueueMetrics).toHaveBeenCalledWith(
        expect.objectContaining({
          connection: mockConnection,
          taskQueues,
          temporalNamespace: "test-namespace",
        }),
      );
    });

    describe("Activity Execution", () => {
      it("should create activity function that calls step.run with correct arguments", async () => {
        const mockContextProvider = createMockContextProvider();
        await createActivityWorkers(createTestConfig({ contextProvider: mockContextProvider }));

        const createCall = mockWorkerCreate.mock.calls[0]?.[0];
        expect(createCall).toBeDefined();
        const activityFn = createCall!.activities.testStep;

        const result = await (activityFn as (a: unknown, b: unknown) => Promise<unknown>)(
          { correlationId: "123" },
          { input: "test-value" },
        );

        expect(mockContextProvider.beforeStep).toHaveBeenCalledWith("testStep");
        expect(mockContextProvider.afterStep).toHaveBeenCalled();
        expect(mockMetricsHandle.activityStarted).toHaveBeenCalledTimes(1);
        expect(mockMetricsHandle.activityFinished).toHaveBeenCalledTimes(1);
        expect(result).toEqual({ output: "test-result" });
      });

      it("should work without context provider", async () => {
        await createActivityWorkers(createTestConfig({ contextProvider: undefined }));

        const createCall = mockWorkerCreate.mock.calls[0]?.[0];
        expect(createCall).toBeDefined();
        const activityFn = createCall!.activities.testStep;

        const result = await (activityFn as (a: unknown, b: unknown) => Promise<unknown>)(
          {},
          { input: "test-value" },
        );

        expect(result).toEqual({ output: "test-result" });
      });

      it("should call afterStep even when step throws error", async () => {
        const mockContextProvider = createMockContextProvider();
        const stepError = new Error("Step failed");
        mockStepRun.mockRejectedValueOnce(stepError);

        await createActivityWorkers(createTestConfig({ contextProvider: mockContextProvider }));

        const createCall = mockWorkerCreate.mock.calls[0]?.[0];
        expect(createCall).toBeDefined();
        const activityFn = createCall!.activities.testStep;

        await expect(
          (activityFn as (a: unknown, b: unknown) => Promise<unknown>)({}, { input: "test" }),
        ).rejects.toThrow("Step failed");

        expect(mockContextProvider.afterStep).toHaveBeenCalledWith(expect.anything(), stepError);
        expect(mockMetricsHandle.activityStarted).toHaveBeenCalledTimes(1);
        expect(mockMetricsHandle.activityFinished).toHaveBeenCalledTimes(1);
      });

      it("should finish metrics tracking when beforeStep throws", async () => {
        const setupError = new Error("Context setup failed");
        const mockContextProvider = createMockContextProvider();
        mockContextProvider.beforeStep.mockRejectedValueOnce(setupError);

        await createActivityWorkers(createTestConfig({ contextProvider: mockContextProvider }));

        const createCall = mockWorkerCreate.mock.calls[0]?.[0];
        expect(createCall).toBeDefined();
        const activityFn = createCall!.activities.testStep;

        await expect(
          (activityFn as (a: unknown, b: unknown) => Promise<unknown>)({}, { input: "test" }),
        ).rejects.toThrow("Context setup failed");

        expect(mockContextProvider.afterStep).not.toHaveBeenCalled();
        expect(mockMetricsHandle.activityStarted).toHaveBeenCalledTimes(1);
        expect(mockMetricsHandle.activityFinished).toHaveBeenCalledTimes(1);
      });

      it("should propagate step error even when afterStep throws cleanup error", async () => {
        const mockContextProvider = createMockContextProvider();
        const stepError = new Error("Step business logic failed");
        mockStepRun.mockRejectedValueOnce(stepError);
        mockContextProvider.afterStep.mockRejectedValueOnce(new Error("Cleanup failed"));

        await createActivityWorkers(createTestConfig({ contextProvider: mockContextProvider }));

        const createCall = mockWorkerCreate.mock.calls[0]?.[0];
        expect(createCall).toBeDefined();
        const activityFn = createCall!.activities.testStep;

        await expect(
          (activityFn as (a: unknown, b: unknown) => Promise<unknown>)({}, { input: "test" }),
        ).rejects.toThrow("Step business logic failed");
      });

      it("does not stringify a non-Error step rejection for logger metadata", async () => {
        const logger = makeLogger();
        const rejection = { toString: () => "customer-secret@example.com" };
        mockStepRun.mockRejectedValueOnce(rejection);

        await createActivityWorkers(createTestConfig({ logger }));

        const createCall = mockWorkerCreate.mock.calls[0]?.[0];
        expect(createCall).toBeDefined();
        const activityFn = createCall!.activities.testStep;

        await expect(
          (activityFn as (a: unknown, b: unknown) => Promise<unknown>)({}, { input: "test" }),
        ).rejects.toBe(rejection);
        expect(logger.error).toHaveBeenCalledWith(
          "Activity execution failed",
          expect.objectContaining({
            error: expect.objectContaining({ message: "A non-Error value was thrown" }),
          }),
        );
        const loggedError = logger.error.mock.calls[0]?.[1]?.error as Error;
        expect(loggedError.message).not.toContain("customer-secret@example.com");
      });

      // The error code has to land in ApplicationFailure.type: that is the only
      // field Temporal compares against RetryPolicy.nonRetryableErrorTypes, so
      // this mapping is what makes `asyncRetry.nonRetryableErrorTypes` match.
      it("should map a coded step error's code onto ApplicationFailure.type", async () => {
        const codedError = Object.assign(new Error("Reference image was rejected"), {
          code: "CONTENT_REJECTED",
          parentCodes: ["POLICY_ERROR"],
        });
        mockStepRun.mockRejectedValueOnce(codedError);

        await createActivityWorkers(createTestConfig());

        const createCall = mockWorkerCreate.mock.calls[0]?.[0];
        expect(createCall).toBeDefined();
        const activityFn = createCall!.activities.testStep;

        let failure: ApplicationFailure | undefined;
        try {
          await (activityFn as (a: unknown, b: unknown) => Promise<unknown>)(
            {},
            { input: "test" },
          );
        } catch (err) {
          failure = err as ApplicationFailure;
        }

        expect(failure).toBeInstanceOf(ApplicationFailure);
        expect(failure?.type).toBe("CONTENT_REJECTED");
        expect(failure?.details?.[0]).toMatchObject({
          code: "CONTENT_REJECTED",
          parentCodes: ["POLICY_ERROR"],
        });
        // Existing errors stay retryable unless a step lists their code in
        // asyncRetry.nonRetryableErrorTypes.
        expect(failure?.nonRetryable).toBe(false);
      });

      // parentCodes is optional. Requiring it would drop the code for plain
      // coded errors, and since nonRetryableErrorTypes matches on that code,
      // such a step would keep retrying however it configured asyncRetry.
      it("should map the code of an error that has no parentCodes", async () => {
        const codedError = Object.assign(new Error("Input failed validation"), {
          code: "VALIDATION_FAILED",
        });
        mockStepRun.mockRejectedValueOnce(codedError);

        await createActivityWorkers(createTestConfig());

        const createCall = mockWorkerCreate.mock.calls[0]?.[0];
        expect(createCall).toBeDefined();
        const activityFn = createCall!.activities.testStep;

        let failure: ApplicationFailure | undefined;
        try {
          await (activityFn as (a: unknown, b: unknown) => Promise<unknown>)(
            {},
            { input: "test" },
          );
        } catch (err) {
          failure = err as ApplicationFailure;
        }

        expect(failure).toBeInstanceOf(ApplicationFailure);
        expect(failure?.type).toBe("VALIDATION_FAILED");
        expect(failure?.details?.[0]).toMatchObject({
          code: "VALIDATION_FAILED",
          parentCodes: [],
        });
      });

      it("should leave errors with no code as-is", async () => {
        const plainError = new Error("Something broke");
        mockStepRun.mockRejectedValueOnce(plainError);

        await createActivityWorkers(createTestConfig());

        const createCall = mockWorkerCreate.mock.calls[0]?.[0];
        expect(createCall).toBeDefined();
        const activityFn = createCall!.activities.testStep;

        await expect(
          (activityFn as (a: unknown, b: unknown) => Promise<unknown>)({}, { input: "test" }),
        ).rejects.toBe(plainError);
      });

      // A driver error's structured fields are what a consumer's serializer classifies on:
      // flattening to `error.message` here leaves it only the inlined SQL to pattern-match.
      it("should log the failure as the Error itself, structured fields intact", async () => {
        const logger = makeLogger();
        const driverError = Object.assign(new Error('insert into "user" ... - detail: Key ...'), {
          code: "23505",
          severity: "ERROR",
          table: "user",
          constraint: "user_email_unique",
        });
        mockStepRun.mockRejectedValueOnce(driverError);

        await createActivityWorkers(createTestConfig({ logger }));
        const activityFn = mockWorkerCreate.mock.calls[0]?.[0].activities.testStep;

        // A coded error becomes an ApplicationFailure -- whose message is the raw one, which
        // is what the failureConverter seam exists to rewrite.
        await expect(
          (activityFn as (a: unknown, b: unknown) => Promise<unknown>)({}, { input: "test" }),
        ).rejects.toBeInstanceOf(ApplicationFailure);

        const [, metadata] = logger.error.mock.calls.find(
          ([message]) => message === "Activity execution failed",
        )!;
        expect(metadata?.error).toBe(driverError);
        expect(metadata?.error).toMatchObject({
          code: "23505",
          severity: "ERROR",
          table: "user",
          constraint: "user_email_unique",
        });
      });

      it("should log an afterStep cleanup failure as the Error, naming the step error only", async () => {
        const logger = makeLogger();
        const contextProvider = createMockContextProvider();
        const cleanupFailure = new Error("flush failed");
        contextProvider.afterStep.mockRejectedValueOnce(cleanupFailure);
        mockStepRun.mockRejectedValueOnce(new TypeError("step blew up"));

        await createActivityWorkers(createTestConfig({ logger, contextProvider }));
        const activityFn = mockWorkerCreate.mock.calls[0]?.[0].activities.testStep;

        await expect(
          (activityFn as (a: unknown, b: unknown) => Promise<unknown>)({}, { input: "test" }),
        ).rejects.toThrow("step blew up");

        expect(logger.error).toHaveBeenCalledWith(
          "afterStep cleanup failed",
          expect.objectContaining({
            error: cleanupFailure,
            originalStepErrorName: "TypeError",
          }),
        );
      });
    });

    // A stack opens with `name: message`, so copying `error.stack` whole put a second copy
    // of the message in details -- where a converter scrubbing `message` and `stackTrace`
    // would not think to look.
    describe("Stack in failure details", () => {
      const inlinedSql =
        "insert into \"user\" (\"email\") values ('jane@example.com') - duplicate key";

      async function failureFor(
        error: Error,
        logger?: ComposerLogger,
      ): Promise<ApplicationFailure> {
        mockStepRun.mockRejectedValueOnce(error);
        await createActivityWorkers(createTestConfig({ logger }));
        const activityFn = mockWorkerCreate.mock.calls[0]![0]!.activities.testStep;
        try {
          await (activityFn as (a: unknown, b: unknown) => Promise<unknown>)({}, {});
        } catch (err) {
          return err as ApplicationFailure;
        }
        throw new Error("activity was expected to reject");
      }

      it("carries stack frames without the message header line", async () => {
        const failure = await failureFor(
          Object.assign(new Error(inlinedSql), { code: "DB_ERROR" }),
        );

        const { stack } = failure.details?.[0] as { stack?: string };
        expect(stack).toBeDefined();
        expect(stack).not.toContain(inlinedSql);
        expect(stack).not.toContain("Error:");
        // Frames survive -- this withholds the message, it does not discard the stack.
        expect(stack).toMatch(/^\s*at /);
      });

      it("does not mistake multiline message content for a stack frame", async () => {
        const secret = "customer-secret@example.com";
        const failure = await failureFor(
          Object.assign(new Error(`database failed\n    at ${secret}`), { code: "DB_ERROR" }),
        );

        const { stack } = failure.details?.[0] as { stack?: string };
        expect(stack).toBeDefined();
        expect(stack).not.toContain(secret);
        expect(stack).toMatch(/^\s*at /);
      });

      it("omits a custom stack getter even if it resembles formatted frames", async () => {
        const secret = "customer-secret@example.com";
        const customStack = Object.assign(new Error("database failed"), { code: "DB_ERROR" });
        Object.defineProperty(customStack, "stack", {
          configurable: true,
          get: () => `__composer_stack_frames__    at ${secret}`,
          set: () => undefined,
        });

        const failure = await failureFor(customStack);

        expect((failure.details?.[0] as { stack?: string }).stack).toBeUndefined();
      });

      it("preserves the host's formatted stack for the logger", async () => {
        const originalPrepareStackTrace = Error.prepareStackTrace;
        const logger = makeLogger();
        const error = Object.assign(new Error(inlinedSql), { code: "DB_ERROR" });
        let loggedStack: unknown;
        logger.error.mockImplementation((_message, metadata) => {
          loggedStack = (metadata?.error as Error | undefined)?.stack;
        });
        Error.prepareStackTrace = (stackError, callSites) =>
          `host-formatted: ${stackError.message} (${callSites.length} frames)`;

        try {
          const failure = await failureFor(error, logger);

          expect(loggedStack).toBe(`host-formatted: ${inlinedSql} (10 frames)`);
          expect((failure.details?.[0] as { stack?: string }).stack).not.toContain(inlinedSql);
        } finally {
          Error.prepareStackTrace = originalPrepareStackTrace;
        }
      });

      it("preserves a non-string host-formatted stack for the logger", async () => {
        const originalPrepareStackTrace = Error.prepareStackTrace;
        const logger = makeLogger();
        const error = Object.assign(new Error(inlinedSql), { code: "DB_ERROR" });
        const hostStack = { formatted: "host stack" };
        let loggedStack: unknown;
        logger.error.mockImplementation((_message, metadata) => {
          loggedStack = (metadata?.error as Error | undefined)?.stack;
        });
        Error.prepareStackTrace = () => hostStack;

        try {
          const failure = await failureFor(error, logger);

          expect(loggedStack).toBe(hostStack);
          expect((failure.details?.[0] as { stack?: string }).stack).not.toContain(inlinedSql);
        } finally {
          Error.prepareStackTrace = originalPrepareStackTrace;
        }
      });

      it("calls the host stack formatter with Error as its receiver", async () => {
        const originalPrepareStackTrace = Error.prepareStackTrace;
        const logger = makeLogger();
        const error = Object.assign(new Error(inlinedSql), { code: "DB_ERROR" });
        let formatterThis: unknown;
        Error.prepareStackTrace = function (stackError, callSites) {
          formatterThis = this;
          return `host-formatted: ${stackError.message} (${callSites.length} frames)`;
        };

        try {
          await failureFor(error, logger);

          expect(formatterThis).toBe(Error);
        } finally {
          Error.prepareStackTrace = originalPrepareStackTrace;
        }
      });

      it("restores the host-formatted stack on a frozen error", async () => {
        const originalPrepareStackTrace = Error.prepareStackTrace;
        const logger = makeLogger();
        const error = Object.freeze(Object.assign(new Error(inlinedSql), { code: "DB_ERROR" }));
        let loggedStack: unknown;
        logger.error.mockImplementation((_message, metadata) => {
          loggedStack = (metadata?.error as Error | undefined)?.stack;
        });
        Error.prepareStackTrace = () => "host-formatted stack";

        try {
          const failure = await failureFor(error, logger);

          expect(loggedStack).toBe("host-formatted stack");
          expect((failure.details?.[0] as { stack?: string }).stack).not.toContain(inlinedSql);
        } finally {
          Error.prepareStackTrace = originalPrepareStackTrace;
        }
      });

      it("omits the stack entirely rather than emitting a bare header", async () => {
        const stackless = Object.assign(new Error(inlinedSql), { code: "DB_ERROR" });
        stackless.stack = `Error: ${inlinedSql}`;

        const failure = await failureFor(stackless);

        expect(failure.details?.[0]).toMatchObject({ code: "DB_ERROR" });
        expect((failure.details?.[0] as { stack?: string }).stack).toBeUndefined();
      });
    });

    describe("Failure scrubbing seams", () => {
      it("should forward a dataConverter to every worker", async () => {
        const dataConverter: DataConverter = {
          failureConverterPath: "/srv/scrubbing-failure-converter.js",
        };

        await createActivityWorkers(createTestConfig({ dataConverter }));

        expect(mockWorkerCreate).toHaveBeenCalledTimes(3);
        for (const [options] of mockWorkerCreate.mock.calls) {
          expect(options.dataConverter).toBe(dataConverter);
        }
      });

      it("should forward activity interceptors to every worker", async () => {
        const interceptorFactory = vi.fn();

        await createActivityWorkers(
          createTestConfig({ interceptors: { activity: [interceptorFactory] } }),
        );

        expect(mockWorkerCreate).toHaveBeenCalledTimes(3);
        for (const [options] of mockWorkerCreate.mock.calls) {
          expect(options.interceptors).toEqual({ activity: [interceptorFactory] });
        }
      });

      // Omitted rather than passed as undefined: Temporal's own defaults have to stand.
      it("should omit both keys when neither seam is configured", async () => {
        await createActivityWorkers(createTestConfig());

        const options = mockWorkerCreate.mock.calls[0]?.[0];
        expect(options).toBeDefined();
        expect(options).not.toHaveProperty("dataConverter");
        expect(options).not.toHaveProperty("interceptors");
      });

      // workflowModules drives the workflow sandbox, which this worker never loads.
      it("should not forward workflowModules to an activity worker", async () => {
        await createActivityWorkers(
          createTestConfig({ interceptors: { workflowModules: ["./wf-interceptors"] } }),
        );

        const options = mockWorkerCreate.mock.calls[0]?.[0];
        expect(options).not.toHaveProperty("interceptors");
      });
    });
  });

  describe("runActivityWorkers", () => {
    it("should create and run workers", async () => {
      const runPromise = runActivityWorkers(createTestConfig());
      await new Promise((resolve) => setImmediate(resolve));
      expect(mockWorkerCreate).toHaveBeenCalledTimes(3);
      expect(mockWorkerInstance.run).toHaveBeenCalled();
      await runPromise;
    });

    it("should throw error if worker run fails", async () => {
      mockWorkerInstance.run.mockRejectedValue(new Error("Worker failed"));
      await expect(runActivityWorkers(createTestConfig())).rejects.toThrow("Worker failed");
    });

    it("should wait for workers to stop before closing the connection on SIGTERM", async () => {
      let resolveRun: (() => void) | undefined;
      mockWorkerInstance.run.mockReturnValue(
        new Promise<void>((resolve) => {
          resolveRun = resolve;
        }),
      );

      void runActivityWorkers(createTestConfig());
      await new Promise((resolve) => setImmediate(resolve));

      process.emit("SIGTERM");
      await new Promise((resolve) => setImmediate(resolve));

      expect(mockWorkerInstance.shutdown).toHaveBeenCalledTimes(3);
      expect(mockConnection.close).not.toHaveBeenCalled();
      expect(mockMetricsHandle.stop).not.toHaveBeenCalled();

      resolveRun?.();
      await new Promise((resolve) => setImmediate(resolve));

      expect(mockMetricsHandle.stop).toHaveBeenCalledTimes(1);
      expect(mockConnection.close).toHaveBeenCalledTimes(1);
      expect(process.exit).toHaveBeenCalledWith(0);
    });

    it("should log active activities when receiving SIGTERM", async () => {
      const logger = makeLogger();
      let resolveRun: (() => void) | undefined;
      let resolveStep: ((value: { output: string }) => void) | undefined;
      mockWorkerInstance.run.mockReturnValue(
        new Promise<void>((resolve) => {
          resolveRun = resolve;
        }),
      );
      mockStepRun.mockReturnValueOnce(
        new Promise<{ output: string }>((resolve) => {
          resolveStep = resolve;
        }),
      );

      void runActivityWorkers(createTestConfig({ logger }));
      await new Promise((resolve) => setImmediate(resolve));

      const createCall = mockWorkerCreate.mock.calls[0]?.[0];
      expect(createCall).toBeDefined();
      const activityFn = createCall!.activities.testStep as (
        workflowInput: unknown,
        stepInput: unknown,
      ) => Promise<unknown>;
      const activityPromise = activityFn({}, { input: "test" });
      await new Promise((resolve) => setImmediate(resolve));

      process.emit("SIGTERM");
      await new Promise((resolve) => setImmediate(resolve));

      expect(logger.info).toHaveBeenCalledWith(
        "Activity Workers shutdown signal received",
        expect.objectContaining({
          signal: "SIGTERM",
          activeActivityCount: 1,
          activeActivities: [
            expect.objectContaining({
              activityName: "testStep",
              stepName: "testStep",
              workflowId: "test-workflow-id",
              runId: "test-run-id",
              activityId: "test-activity-id",
              attempt: 2,
            }),
          ],
        }),
      );

      resolveStep?.({ output: "test-result" });
      await activityPromise;
      resolveRun?.();
      await new Promise((resolve) => setImmediate(resolve));
    });
  });
});
