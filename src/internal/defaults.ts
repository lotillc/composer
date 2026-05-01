/**
 * Default implementations for injectable Composer dependencies.
 *
 * These defaults allow the framework to work out of the box without any
 * configuration. Users can override them via `createComposer()` options.
 *
 * - Logger: Console-based (works everywhere, zero config)
 * - Metrics: OpenTelemetry API (no-op if no SDK is configured, automatic collection if one is)
 */

import {
  type Attributes,
  type Context,
  metrics,
  type Counter as OTelCounter,
  type Histogram as OTelHistogram,
} from "@opentelemetry/api";
import type { ComposerLogger, Counter, Histogram, MetricsCollector } from "./types";

// ============================================================================
// Default Logger
// ============================================================================

/**
 * Console-based logger used when no custom logger is provided.
 *
 * Formats output as: message + JSON metadata (if present).
 * Compatible with all JavaScript runtimes.
 */
export const defaultLogger: ComposerLogger = {
  info: (message, metadata) => {
    metadata ? console.info(message, metadata) : console.info(message);
  },
  warn: (message, metadata) => {
    metadata ? console.warn(message, metadata) : console.warn(message);
  },
  error: (message, metadata) => {
    metadata ? console.error(message, metadata) : console.error(message);
  },
  debug: (message, metadata) => {
    metadata ? console.debug(message, metadata) : console.debug(message);
  },
};

// ============================================================================
// Default Metrics (OpenTelemetry API)
// ============================================================================

/** Wraps an OTel Counter to satisfy the local Counter interface. */
class CounterImpl implements Counter {
  constructor(private otelCounter: OTelCounter) {}

  add(value: number = 1, attributes?: Attributes, context?: Context): void {
    this.otelCounter.add(value, attributes, context);
  }
}

/** Wraps an OTel Histogram to satisfy the local Histogram interface. */
class HistogramImpl implements Histogram {
  constructor(private otelHistogram: OTelHistogram) {}

  record(value: number, attributes?: Attributes, context?: Context): void {
    this.otelHistogram.record(value, attributes, context);
  }
}

/**
 * MetricsCollector backed by the OpenTelemetry API.
 *
 * If the user has configured an OTel SDK (e.g., Prometheus exporter),
 * metrics are automatically collected. If not, the OTel API provides
 * safe no-op implementations -- no errors, no overhead.
 *
 * Instruments are cached by name so repeated calls return the same instance.
 */
class MetricsCollectorImpl implements MetricsCollector {
  private counters = new Map<string, Counter>();
  private histograms = new Map<string, Histogram>();
  private readonly meter;

  constructor(serviceName: string) {
    this.meter = metrics.getMeter(serviceName);
  }

  counter(name: string, description?: string): Counter {
    let cached = this.counters.get(name);
    if (!cached) {
      cached = new CounterImpl(this.meter.createCounter(name, { description }));
      this.counters.set(name, cached);
    }
    return cached;
  }

  histogram(name: string, description?: string): Histogram {
    let cached = this.histograms.get(name);
    if (!cached) {
      cached = new HistogramImpl(this.meter.createHistogram(name, { description }));
      this.histograms.set(name, cached);
    }
    return cached;
  }
}

/**
 * Creates a MetricsCollector that uses the OpenTelemetry API directly.
 *
 * @param serviceName - Name used to identify the meter (e.g., "composer-workflow")
 */
export function createDefaultMetrics(serviceName: string): MetricsCollector {
  return new MetricsCollectorImpl(serviceName);
}
