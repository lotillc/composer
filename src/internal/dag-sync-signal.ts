import type { DurationString, Step } from "./dag-sync-step.js";
import { durationToMs } from "./duration.js";
import { assertValidName } from "./validate-name.js";

/**
 * A named signal a workflow can wait on, typed by its payload.
 *
 * Define it once and share it between the workflow (`waitForSignal`) and the code that
 * delivers it (`composer.signalAsyncWorkflow`), so both sides agree on name and payload.
 *
 * @example
 * ```typescript
 * export const approvalSignal = defineWorkflowSignal<{ approved: boolean }>("approval");
 * ```
 */
export interface WorkflowSignal<Payload> {
  readonly name: string;
  /**
   * Validates and normalizes a payload before `signalAsyncWorkflow` sends it. Throwing
   * rejects the send, so a malformed payload never reaches the workflow.
   */
  readonly parse?: (payload: unknown) => Payload;
  /**
   * @internal Type-level only; never set at runtime. A function type in both positions
   * makes the payload invariant, so a signal of one payload type cannot stand in for another.
   */
  readonly __payload?: (payload: Payload) => Payload;
}

/**
 * Defines a typed workflow signal. See {@link WorkflowSignal}.
 *
 * The payload type is otherwise only checked at compile time, and the signal may carry it
 * for weeks before a step reads it. Pass `parse` -- a schema's parse function, say -- to
 * validate it at the boundary instead; the payload type is then inferred from it.
 *
 * @example
 * ```typescript
 * const approvalSignal = defineWorkflowSignal("approval", { parse: ApprovalSchema.parse });
 * ```
 */
export function defineWorkflowSignal<Payload>(
  name: string,
  options: { parse?: (payload: unknown) => Payload } = {},
): WorkflowSignal<Payload> {
  if (typeof name !== "string" || name.length === 0) {
    throw new Error("defineWorkflowSignal: name must be a non-empty string");
  }
  return { name, ...(options.parse && { parse: options.parse }) };
}

/** Unconsumed payloads kept per signal name when a wait sets no `maxPendingSignals`. */
export const DEFAULT_MAX_PENDING_SIGNALS = 100;

/**
 * Runtime metadata for a signal wait node.
 */
export interface SignalWaitMetadata {
  signalName: string;
  /**
   * The definition this wait was declared with. Never serialized: it lets the builder
   * require every wait on a name to use one definition, since they share one queue.
   */
  signal: WorkflowSignal<unknown>;
  timeout?: DurationString;
  maxPendingSignals?: number;
  timeoutValue?: unknown;
  /** Distinguishes an explicit `timeoutValue: undefined` from no fallback at all. */
  hasTimeoutValue: boolean;
}

/**
 * A SignalWait is a Step with additional `__signalWait` metadata.
 *
 * It takes part in the DAG like any step, but the Temporal runtime does not run it as an
 * activity: the workflow blocks on a durable condition until the signal arrives, then
 * places its payload in the one field the node provides. Its `run` always throws, so sync
 * execution reports a durable wait as an ordinary step failure.
 */
export type SignalWait<
  Bag extends Record<string, any>,
  Needs extends readonly (keyof Bag)[],
  Field extends keyof Bag,
  Name extends string = string,
> = Step<Bag, Needs, readonly [Field], unknown, Name> & {
  __signalWait: SignalWaitMetadata;
};

/**
 * Runtime type guard for detecting signal wait nodes within a step array.
 */
export function isSignalWaitStep(
  value: Step<any, any, any, any, any>,
): value is SignalWait<any, any, any, any> {
  return "__signalWait" in value && (value as SignalWait<any, any, any>).__signalWait != null;
}

/**
 * True when `value` survives `JSON.stringify` / `JSON.parse` unchanged. The plan a wait's
 * `timeoutValue` travels in is serialized as JSON, so a `Date`, `Map`, `bigint`, class
 * instance or non-finite number would arrive altered, or stop the worker from starting.
 */
function isJsonSafe(value: unknown, depth = 0): boolean {
  if (depth > 100) return false;
  if (value === null || typeof value === "string" || typeof value === "boolean") return true;
  if (typeof value === "number") return Number.isFinite(value);
  if (Array.isArray(value)) {
    // Indexed, not `every`: a hole would be skipped, and JSON writes it back as null.
    for (let i = 0; i < value.length; i++) {
      if (!(i in value) || !isJsonSafe(value[i], depth + 1)) return false;
    }
    return true;
  }
  if (typeof value !== "object") return false;
  const proto = Object.getPrototypeOf(value);
  if (proto !== Object.prototype && proto !== null) return false;
  // An `undefined` property is dropped by JSON, and reads back as `undefined` all the same.
  return Object.values(value).every((item) => item === undefined || isJsonSafe(item, depth + 1));
}

/**
 * Factory for a durable wait on a signal.
 *
 * The workflow waits, without holding a worker slot, until the signal is delivered with
 * `composer.signalAsyncWorkflow`, then writes the payload to the single `provides` field.
 * A signal delivered before the wait begins -- even in an earlier continue-as-new run --
 * is kept and consumed when the wait is reached.
 *
 * `timeout` bounds the wait. When it elapses, the field gets `timeoutValue` if one was
 * given (it must be plain JSON data); otherwise the node fails with `SIGNAL_WAIT_TIMEOUT_CODE`, which the workflow's
 * `onError` handler sees like any step failure. With no `timeout`, the wait has no limit.
 *
 * `needs` only orders the wait after the steps that produce those fields.
 *
 * Only signal names a wait declares are accepted; any other signal is dropped with a
 * warning. Up to `maxPendingSignals` (default 100) unconsumed payloads of this signal are
 * kept, the oldest dropped first.
 *
 * Only Temporal (async) execution can wait; under `runSyncWorkflow` the node fails.
 *
 * @example
 * ```typescript
 * const awaitApproval = waitForSignal<Bag>()({
 *   name: "awaitApproval",
 *   signal: approvalSignal,
 *   needs: ["requestSentAt"],
 *   provides: ["approval"],
 *   timeout: "21 days",
 *   timeoutValue: { approved: false },
 * });
 * ```
 */
export function waitForSignal<Bag extends Record<string, any>>() {
  return <
    const Name extends string,
    const Field extends keyof Bag,
    const Needs extends readonly (keyof Bag)[] = readonly [],
  >(definition: {
    name: Name;
    signal: WorkflowSignal<NoInfer<Bag[Field]>>;
    needs?: Needs;
    provides: readonly [Field];
    timeout?: DurationString;
    timeoutValue?: NoInfer<Bag[Field]>;
    maxPendingSignals?: number;
  }): SignalWait<Bag, Needs, Field, Name> => {
    assertValidName("SignalWait", definition.name);
    if (typeof definition.signal?.name !== "string") {
      throw new Error(
        `waitForSignal "${definition.name}": signal must be a definition from defineWorkflowSignal`,
      );
    }
    if (!Array.isArray(definition.provides) || definition.provides.length !== 1) {
      throw new Error(
        `waitForSignal "${definition.name}": provides must list exactly one field`,
      );
    }
    if (definition.timeout !== undefined) {
      let timeoutMs: number;
      try {
        timeoutMs = durationToMs(definition.timeout);
      } catch {
        timeoutMs = Number.NaN;
      }
      // The type admits "0s" and "-1 day"; neither is a wait.
      if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) {
        throw new Error(
          `waitForSignal "${definition.name}": timeout must be a positive duration, ` +
            `got "${definition.timeout}"`,
        );
      }
    }
    // `undefined` is allowed: a missing value is what an omitted JSON key reads back as.
    if (definition.timeoutValue !== undefined && !isJsonSafe(definition.timeoutValue)) {
      throw new Error(
        `waitForSignal "${definition.name}": timeoutValue must be plain JSON data ` +
          `(objects, arrays, strings, finite numbers, booleans, null); it is stored in the ` +
          `workflow plan as JSON, so a Date, Map, bigint or class instance would not survive`,
      );
    }
    const maxPending = definition.maxPendingSignals;
    if (maxPending !== undefined && (!Number.isInteger(maxPending) || maxPending < 1)) {
      throw new Error(
        `waitForSignal "${definition.name}": maxPendingSignals must be a positive integer`,
      );
    }

    return {
      name: definition.name,
      needs: (definition.needs ?? []) as unknown as Needs,
      provides: definition.provides,
      run: async () => {
        throw new Error(
          `waitForSignal "${definition.name}" can only run under Temporal (runAsyncWorkflow or ` +
            `startAsyncWorkflow); sync execution cannot wait for a signal.`,
        );
      },
      __signalWait: {
        signalName: definition.signal.name,
        signal: definition.signal as WorkflowSignal<unknown>,
        ...(definition.timeout !== undefined && { timeout: definition.timeout }),
        ...(maxPending !== undefined && { maxPendingSignals: maxPending }),
        ...("timeoutValue" in definition && { timeoutValue: definition.timeoutValue }),
        hasTimeoutValue: "timeoutValue" in definition,
      },
    };
  };
}
