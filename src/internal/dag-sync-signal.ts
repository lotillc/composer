import type { DurationString, Step } from "./dag-sync-step.js";

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
   * @internal Type-level only; never set at runtime. A function type in both positions
   * makes the payload invariant, so a signal of one payload type cannot stand in for another.
   */
  readonly __payload?: (payload: Payload) => Payload;
}

/** Defines a typed workflow signal. See {@link WorkflowSignal}. */
export function defineWorkflowSignal<Payload>(name: string): WorkflowSignal<Payload> {
  if (typeof name !== "string" || name.length === 0) {
    throw new Error("defineWorkflowSignal: name must be a non-empty string");
  }
  return { name };
}

/**
 * Runtime metadata for a signal wait node.
 */
export interface SignalWaitMetadata {
  signalName: string;
  timeout?: DurationString;
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
 * Factory for a durable wait on a signal.
 *
 * The workflow waits, without holding a worker slot, until the signal is delivered with
 * `composer.signalAsyncWorkflow`, then writes the payload to the single `provides` field.
 * A signal delivered before the wait begins -- even in an earlier continue-as-new run --
 * is kept and consumed when the wait is reached.
 *
 * `timeout` bounds the wait. When it elapses, the field gets `timeoutValue` if one was
 * given; otherwise the node fails with `SIGNAL_WAIT_TIMEOUT_CODE`, which the workflow's
 * `onError` handler sees like any step failure. With no `timeout`, the wait has no limit.
 *
 * `needs` only orders the wait after the steps that produce those fields.
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
  }): SignalWait<Bag, Needs, Field, Name> => {
    if (!Array.isArray(definition.provides) || definition.provides.length !== 1) {
      throw new Error(
        `waitForSignal "${definition.name}": provides must list exactly one field`,
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
        ...(definition.timeout !== undefined && { timeout: definition.timeout }),
        ...("timeoutValue" in definition && { timeoutValue: definition.timeoutValue }),
        hasTimeoutValue: "timeoutValue" in definition,
      },
    };
  };
}
