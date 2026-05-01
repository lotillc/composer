import { vi } from "vitest";

// Shared mock factory for observability tests.
// Provides reusable mock tracer/metrics/logger objects so individual test files
// don't have to recreate them.

// Mock environment functions
export const ENVIRONMENTS = {
  PROD: "prod",
  LOCAL: "local",
};

export const mockGetCurrentEnvironment = vi.fn().mockReturnValue("local");

// Create mock span
export const createMockSpan = () => ({
  setAttributes: vi.fn(),
  recordException: vi.fn(),
  setStatus: vi.fn(),
  end: vi.fn(),
});

// Create mock counter
export const createMockCounter = () => ({
  add: vi.fn(),
});

// Create mock histogram
export const createMockHistogram = () => ({
  record: vi.fn(),
});

// Create the mock tracer
export const mockTracer = {
  startSpan: vi.fn().mockImplementation(() => createMockSpan()),
};

// Create the mock metrics collector
export const mockMetrics = {
  counter: vi.fn().mockImplementation(() => createMockCounter()),
  histogram: vi.fn().mockImplementation(() => createMockHistogram()),
};

// Create the mock logger
export const mockLogger = {
  info: vi.fn(),
  warn: vi.fn(),
  error: vi.fn(),
  debug: vi.fn(),
};

// Create the createLazyLogger mock that returns a getter for the logger
export const mockCreateLazyLogger = vi.fn().mockImplementation(() => () => mockLogger);

// Create the getTelemetry mock that returns all three
export const mockGetTelemetry = vi.fn().mockImplementation(() => ({
  tracer: mockTracer,
  metrics: mockMetrics,
  getLogger: () => mockLogger,
}));

// Reset all mocks to default behavior
export const resetMocks = () => {
  mockTracer.startSpan.mockImplementation(() => createMockSpan());
  mockMetrics.counter.mockImplementation(() => createMockCounter());
  mockMetrics.histogram.mockImplementation(() => createMockHistogram());
  mockLogger.info.mockImplementation(() => {});
  mockLogger.warn.mockImplementation(() => {});
  mockLogger.error.mockImplementation(() => {});
  mockLogger.debug.mockImplementation(() => {});
};
