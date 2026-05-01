import { beforeEach, describe, expect, it, vi } from "vitest";

import { createComposer, createWorkflow } from "../internal";
import { mockLogger, mockMetrics, mockTracer, resetMocks } from "./observability-mocks";
import { createTestStep, noOpContextProvider, type TestBag } from "./test-utils";

// Mock @opentelemetry/api so trace.getTracer() returns our mockTracer
vi.mock("@opentelemetry/api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@opentelemetry/api")>();
  const mocks =
    await vi.importActual<typeof import("./observability-mocks")>("./observability-mocks");
  const mockTrace = Object.create(actual.trace);
  mockTrace.getTracer = () => mocks.mockTracer;
  return { ...actual, trace: mockTrace };
});

// Mock defaults so createDefaultMetrics() returns our mockMetrics
vi.mock("../internal/defaults", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../internal/defaults")>();
  const mocks =
    await vi.importActual<typeof import("./observability-mocks")>("./observability-mocks");
  return { ...actual, createDefaultMetrics: () => mocks.mockMetrics };
});

/**
 * Composer with mock logger + mock tracer/metrics for testing system failures.
 */
const composerWithMockLogger = createComposer({
  contextProvider: noOpContextProvider,
  logger: mockLogger,
});

describe("Observability System Failures", () => {
  beforeEach(() => {
    // Reset to default behavior
    resetMocks();
  });

  it("should handle span creation failures gracefully", async () => {
    // Make tracer.startSpan throw an error
    mockTracer.startSpan.mockImplementation(() => {
      throw new Error("Span creation failed");
    });

    const step1 = createTestStep("step1", ["input"], ["processed"], (bag) => ({
      processed: bag.input.toUpperCase(),
    }));

    const workflow = createWorkflow<TestBag>("span-failure-workflow")
      .requires("input")
      .build([step1]);

    // Workflow should fail due to span creation failure
    const { error: error1 } = await composerWithMockLogger.runSyncWorkflow(workflow, {
      input: "test",
    });
    expect(error1).toBeDefined();
    expect(error1?.message).toContain("Span creation failed");

    // Verify that span creation was attempted
    expect(mockTracer.startSpan).toHaveBeenCalled();
  });

  it("should handle metrics collection failures gracefully", async () => {
    // Make metrics.counter throw errors
    mockMetrics.counter.mockImplementation(() => {
      throw new Error("Counter creation failed");
    });

    const step1 = createTestStep("step1", ["input"], ["processed"], (bag) => ({
      processed: bag.input.toUpperCase(),
    }));

    const workflow = createWorkflow<TestBag>("metrics-failure-workflow")
      .requires("input")
      .build([step1]);

    // Workflow should fail due to metrics creation failure
    const { error: error2 } = await composerWithMockLogger.runSyncWorkflow(workflow, {
      input: "test",
    });
    expect(error2).toBeDefined();
    expect(error2?.message).toContain("Counter creation failed");

    // Verify that metrics creation was attempted
    expect(mockMetrics.counter).toHaveBeenCalled();
  });

  it("should handle logger failures gracefully", async () => {
    // Make logger.info throw errors
    mockLogger.info.mockImplementation(() => {
      throw new Error("Logging failed");
    });

    const step1 = createTestStep("step1", ["input"], ["processed"], (bag) => ({
      processed: bag.input.toUpperCase(),
    }));

    const workflow = createWorkflow<TestBag>("logger-failure-workflow")
      .requires("input")
      .build([step1]);

    // Workflow should fail due to logging failure during success logging
    const { error: error3 } = await composerWithMockLogger.runSyncWorkflow(workflow, {
      input: "test",
    });
    expect(error3).toBeDefined();
    expect(error3?.message).toContain("Logging failed");

    // Verify that logging was attempted
    expect(mockLogger.info).toHaveBeenCalled();
  });

  it("should handle span.end() failures gracefully", async () => {
    // Mock span with failing end() method
    const failingSpan = {
      setAttributes: vi.fn(),
      recordException: vi.fn(),
      setStatus: vi.fn(),
      end: vi.fn().mockImplementation(() => {
        throw new Error("Span end failed");
      }),
    };

    mockTracer.startSpan.mockReturnValue(failingSpan);

    const step1 = createTestStep("step1", ["input"], ["processed"], (bag) => ({
      processed: bag.input.toUpperCase(),
    }));

    const workflow = createWorkflow<TestBag>("span-end-failure-workflow")
      .requires("input")
      .build([step1]);

    // Observability cleanup failures in the finally block still throw
    // (they occur after the result is prepared but before it's returned)
    await expect(
      composerWithMockLogger.runSyncWorkflow(workflow, { input: "test" }),
    ).rejects.toThrow("Span end failed");

    // Verify that span.end() was called
    expect(failingSpan.end).toHaveBeenCalled();
  });

  it("should handle errors thrown during observability cleanup", async () => {
    // Mock span that throws during setAttributes
    const failingSpan = {
      setAttributes: vi.fn().mockImplementation(() => {
        throw new Error("SetAttributes failed");
      }),
      recordException: vi.fn(),
      setStatus: vi.fn(),
      end: vi.fn(),
    };

    mockTracer.startSpan.mockReturnValue(failingSpan);

    const step1 = createTestStep("step1", ["input"], ["processed"], (bag) => ({
      processed: bag.input.toUpperCase(),
    }));

    const workflow = createWorkflow<TestBag>("cleanup-error-workflow")
      .requires("input")
      .build([step1]);

    // Observability cleanup failures in the finally block still throw
    await expect(
      composerWithMockLogger.runSyncWorkflow(workflow, { input: "test" }),
    ).rejects.toThrow("SetAttributes failed");

    expect(failingSpan.setAttributes).toHaveBeenCalled();
  });
});
