/**
 * Core types for the Composer framework.
 *
 * These types decouple the framework from any specific application or infrastructure
 * packages, allowing the composer to be used as a standalone npm package.
 */

import type { Attributes, Context } from "@opentelemetry/api";

// ============================================================================
// Identity Types
// ============================================================================

/**
 * A UUIDv7 string type.
 *
 * UUIDv7 is a time-ordered UUID format that provides natural sorting by creation time.
 * This branded type prevents accidental assignment of arbitrary strings.
 */
export type UUIDV7 = string & { readonly __brand: "UUIDv7" };

// ============================================================================
// Metrics Types (thin wrappers over OpenTelemetry API)
// ============================================================================

/**
 * Counter metric interface for tracking cumulative values.
 *
 * Wraps the OpenTelemetry Counter with a simplified interface.
 */
export interface Counter {
  /** Increment the counter by the given value (default: 1) */
  add(value?: number, attributes?: Attributes, context?: Context): void;
}

/**
 * Histogram metric interface for tracking distributions of values.
 *
 * Wraps the OpenTelemetry Histogram with a simplified interface.
 */
export interface Histogram {
  /** Record a value in the histogram */
  record(value: number, attributes?: Attributes, context?: Context): void;
}

/**
 * Metrics collector that provides access to Counter and Histogram instruments.
 *
 * The default implementation uses `@opentelemetry/api` directly. If the user has
 * configured an OpenTelemetry SDK, metrics are automatically collected. If not,
 * the OTel API provides safe no-op implementations.
 */
export interface MetricsCollector {
  /** Create or get a counter metric */
  counter(name: string, description?: string): Counter;
  /** Create or get a histogram metric */
  histogram(name: string, description?: string): Histogram;
}

// ============================================================================
// Logger Types
// ============================================================================

/**
 * Minimal logger interface for the Composer framework.
 *
 * Compatible with `console`, `pino`, `winston`, and most logging libraries.
 * The default implementation logs to `console`. Users can inject their own
 * logger via `createComposer({ logger: myLogger })`.
 *
 * @example
 * ```typescript
 * // Using console (default)
 * const composer = createComposer({ contextProvider });
 *
 * // Using pino
 * import pino from "pino";
 * const logger = pino();
 * const composer = createComposer({
 *   contextProvider,
 *   logger: {
 *     info: (msg, meta) => logger.info(meta, msg),
 *     warn: (msg, meta) => logger.warn(meta, msg),
 *     error: (msg, meta) => logger.error(meta, msg),
 *     debug: (msg, meta) => logger.debug(meta, msg),
 *   },
 * });
 * ```
 */
export interface ComposerLogger {
  info(message: string, metadata?: Record<string, unknown>): void;
  warn(message: string, metadata?: Record<string, unknown>): void;
  error(message: string, metadata?: Record<string, unknown>): void;
  debug(message: string, metadata?: Record<string, unknown>): void;
}

// ============================================================================
// Trace Redaction
// ============================================================================

/**
 * Renders an error into the text Composer attaches to a trace span.
 *
 * Span attributes are written straight to the trace backend, so nothing a logger
 * redacts applies to them. Composer cannot know whether a step's error message is
 * safe -- a database driver, for one, inlines row values into it -- so by default it
 * attaches no message at all and spans carry only the error's name. Supply this to
 * put text back on the span, scrubbed or raw as you decide.
 *
 * Returning `undefined` for a given error keeps that error message off the span.
 *
 * The error handed to this is Composer's own `WorkflowStepError` / `WorkflowBatchError`
 * wrapper, whose message is Composer's own context and never the wrapped error's. Reading
 * `.message` is therefore safe but says nothing about why the step failed; the original is
 * on `.originalError` / `.cause`, and it is that message a renderer has to decide about.
 *
 * @example
 * ```typescript
 * createComposer({
 *   contextProvider,
 *   traceErrorMessage: (error) => {
 *     const cause = error instanceof WorkflowStepError ? error.originalError : error;
 *     return isDriverError(cause) ? `${cause.name} ${cause.code}` : cause.message;
 *   },
 * });
 * ```
 */
export type TraceErrorMessage = (error: Error) => string | undefined;
