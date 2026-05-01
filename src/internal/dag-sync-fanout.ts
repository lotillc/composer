import type { AssertSerializable, Step, StrictStepReturn } from "./dag-sync-step";
import type {
  ExtractWorkflowBag,
  ExtractWorkflowRequiredInitial,
  InferWorkflowResult,
  SafeConfiguredKeys,
  Workflow,
} from "./dag-sync-workflow";

/**
 * Computes the precise result type of a workflow from its type parameter.
 *
 * Given a Workflow<Bag, RequiredInitial, Config, Steps, CheckpointNames>, produces
 * Pick<Bag, (all step provides) | RequiredInitial | ConfiguredKeys> -- the exact
 * set of fields available after the workflow completes.
 */
export type InferWorkflowResultFromWorkflow<W> =
  W extends Workflow<infer B, infer R, infer C, infer S, infer _CN>
    ? InferWorkflowResult<B, S, R, SafeConfiguredKeys<C, B>>
    : never;

/**
 * Runtime metadata for a FanOut node. Carries the child workflow reference,
 * data transformation functions, and concurrency configuration.
 *
 * Types are erased at the metadata level (Record<string, unknown>) because
 * the runtimes call these functions with dynamically-typed data. Full type
 * safety is enforced at the factory level during construction.
 */
export interface FanOutMetadata {
  childWorkflow: Workflow<any, any, any, any, any>;
  mapInput: (bag: Record<string, unknown>) => Record<string, unknown>[];
  aggregateResults: (results: Record<string, unknown>[]) => Record<string, unknown>;
  concurrency: number;
}

/**
 * A FanOut is a Step with additional __fanOut metadata.
 *
 * It participates in the DAG like a regular step (has name, needs, provides)
 * but the runtimes detect __fanOut and execute it specially: calling mapInput
 * to produce child workflow inputs, running child workflows with concurrency
 * control (lane-based), then calling aggregateResults to merge the child
 * results back into the parent bag.
 *
 * The `run` function on a FanOut always throws -- it exists only to satisfy
 * the Step interface for the DAG planner. Actual execution is handled by the
 * sync and async runtimes.
 */
export type FanOut<
  Bag extends Record<string, any>,
  Needs extends readonly (keyof Bag)[],
  Provides extends readonly (keyof Bag)[],
  Name extends string = string,
> = Step<Bag, Needs, Provides, unknown, Name> & {
  __fanOut: FanOutMetadata;
};

/**
 * Runtime type guard for detecting FanOut nodes within a step array.
 */
export function isFanOutStep(
  value: Step<any, any, any, any, any>,
): value is FanOut<any, any, any, any> {
  return "__fanOut" in value && (value as FanOut<any, any, any, any>).__fanOut != null;
}

/**
 * Factory function for creating type-safe FanOut nodes.
 *
 * Usage:
 *   const fo = fanOut<ParentBag>()({
 *     name: "startTldIngestions",
 *     needs: ["tlds"] as const,
 *     childWorkflow: ingestCzdsTld,
 *     mapInput: (bag) => bag.tlds.map(tld => ({ tld })),
 *     provides: ["tldResults"] as const,
 *     aggregateResults: (results) => ({ tldResults: results }),
 *     concurrency: 5,
 *   });
 *
 * The double-call pattern (fanOut<ParentBag>()({...})) matches the step()
 * factory -- ParentBag is explicit, all other generics are inferred from
 * the definition object.
 *
 * Generic parameters:
 * - ParentBag (explicit): The parent workflow's bag type
 * - Name (inferred): Literal string name of this fanOut node
 * - Needs (inferred): Bag keys this fanOut requires as input
 * - Provides (inferred): Bag keys this fanOut produces as output
 * - W (inferred): The child workflow type (carries Bag, RequiredInitial, etc.)
 */
export function fanOut<ParentBag extends Record<string, any>>() {
  return <
    const Name extends string,
    const Needs extends readonly (keyof ParentBag)[],
    const Provides extends readonly (keyof ParentBag)[],
    W extends Workflow<any, any, any, any, any>,
  >(definition: {
    name: Name;
    needs: Needs;
    childWorkflow: W;
    mapInput: (
      bag: Pick<ParentBag, Needs[number]>,
    ) => Pick<ExtractWorkflowBag<W>, ExtractWorkflowRequiredInitial<W>>[];
    provides: Provides;
    aggregateResults: (
      results: InferWorkflowResultFromWorkflow<W>[],
    ) => AssertSerializable<StrictStepReturn<ParentBag, Provides[number]>>;
    concurrency?: number;
  }): FanOut<ParentBag, Needs, Provides, Name> => {
    if (
      definition.concurrency != null &&
      (!Number.isFinite(definition.concurrency) || definition.concurrency < 1)
    ) {
      throw new Error(
        `FanOut "${definition.name}": concurrency must be a positive finite number, got ${definition.concurrency}`,
      );
    }

    return {
      name: definition.name,
      needs: definition.needs,
      provides: definition.provides,
      run: async () => {
        throw new Error(
          `FanOut "${definition.name}" cannot be executed directly. ` +
            `Use the workflow runtime (runSyncWorkflow or Temporal).`,
        );
      },
      __fanOut: {
        childWorkflow: definition.childWorkflow,
        mapInput: definition.mapInput as unknown as FanOutMetadata["mapInput"],
        aggregateResults:
          definition.aggregateResults as unknown as FanOutMetadata["aggregateResults"],
        concurrency: definition.concurrency ?? Infinity,
      },
    };
  };
}
