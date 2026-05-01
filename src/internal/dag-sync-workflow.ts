/**
 * DAG-based Workflow Engine with Compile-time Validation
 *
 * This module implements a directed acyclic graph (DAG) workflow system that:
 * - Validates step dependencies at compile-time using TypeScript
 * - Executes steps in parallel when dependencies allow
 * - Uses topological sorting with batching for optimal performance
 * - Supports workflow composition for modularity and reusability
 *
 * ## Core Concepts
 *
 * **Step**: A unit of work with declared inputs (needs) and outputs (provides)
 * **Workflow**: A collection of steps with automatic dependency resolution
 * **Bag**: The data container that flows through the workflow
 *
 * ## Execution Model
 *
 * The engine uses **batch-based parallel execution**:
 * 1. **Dependency Analysis**: Builds a dependency graph using topological sorting
 * 2. **Batch Formation**: Groups steps that can run simultaneously
 * 3. **Parallel Execution**: Runs each batch with Promise.all()
 * 4. **Dependency Tracking**: Uses indegree counters to trigger next batch
 *
 * ## Example Execution Flow
 *
 * Given workflow: [stepA, stepB, stepC, stepD]
 * - stepA: needs [] → provides ["x"]
 * - stepB: needs ["x"] → provides ["y"]
 * - stepC: needs ["x"] → provides ["z"]
 * - stepD: needs ["y", "z"] → provides ["result"]
 *
 * Execution batches:
 * - Batch 1: [stepA] (sequential)
 * - Batch 2: [stepB, stepC] (parallel - both need "x")
 * - Batch 3: [stepD] (sequential - needs both "y" and "z")
 *
 * ## Workflow Composition
 *
 * Workflows can be composed using the `use()` helper to create modular, reusable components:
 *
 * ```typescript
 * // Define a reusable child workflow
 * const authWorkflow = createWorkflow<Bag>("authentication")
 *   .requires("userId")
 *   .build([validateUser, checkPermissions]);
 *
 * // Compose into parent workflow
 * const mainWorkflow = createWorkflow<Bag>("main")
 *   .requires("userId")
 *   .build([
 *     fetchUserData,      // Provides userId
 *     use(authWorkflow),  // Reuses authentication logic
 *     processRequest,     // Uses auth results
 *   ]);
 * ```
 *
 * **Key Composition Features:**
 * - **Step Flattening**: Child workflow steps are merged into parent's dependency graph
 * - **Maximum Parallelization**: Steps from different workflows can execute in the same batch
 * - **Automatic Namespacing**: Child steps are prefixed as "childName.stepName" to prevent conflicts
 * - **Dependency Propagation**: Child initial requirements become regular dependencies in parent
 * - **Nested Composition**: Workflows can compose other composed workflows to any depth
 * - **Observability Preservation**: SubWorkflow spans maintain logical boundaries in traces
 *
 * **Composition Benefits:**
 * - Code reusability across different workflows
 * - Easier testing of workflow components in isolation
 * - Better organization of complex business logic
 * - Maintains compile-time type safety across composition boundaries
 *
 * ## Compile-time Safety
 *
 * The type system validates:
 * - All step dependencies are satisfied by previous steps or initial data
 * - Required initial fields are provided at runtime
 * - Clear error messages for missing dependencies
 *
 * ## Data Protection & Overwrite Prevention
 *
 * The engine enforces strict data integrity rules:
 * - **Step-to-Step Protection**: Each field can have only one producer step (duplicate producers are rejected)
 * - **Initial Data Protection**: Steps cannot overwrite fields provided in initial data
 * - **Immutable Outputs**: Once a step produces a field, it cannot be modified by other steps
 * - **Early Detection**: All conflicts are caught at workflow execution time, not during step execution
 *
 * ## Performance Characteristics
 *
 * - **Automatic Parallelization**: Steps run concurrently when safe
 * - **Batch Synchronization**: All steps in a batch complete before next batch starts
 * - **Zero Polling**: Event-driven execution using dependency counters
 * - **Optimal Scheduling**: Steps execute as soon as dependencies are satisfied
 */

import { context as otelContext } from "@opentelemetry/api";
import { v7 as uuidv7 } from "uuid";
import type { StepContextProvider } from "./context-provider";
import type { FanOutMetadata } from "./dag-sync-fanout";
import { isFanOutStep } from "./dag-sync-fanout";
import type { AsyncStepRuntime, Step } from "./dag-sync-step";
import { defaultLogger } from "./defaults";
import { WorkflowBatchError, WorkflowErrorHandlerFailure, WorkflowStepError } from "./errors";
import {
  type ExecutionContext,
  endBatchObservability,
  endStepObservability,
  endWorkflowObservability,
  startBatchObservability,
  startStepObservability,
  startWorkflowObservability,
} from "./observability";
import type { ComposerLogger, UUIDV7 } from "./types";
import { planWorkflowBatches } from "./workflow-planning";

// ============================================================================
// Checkpoint Types
// ============================================================================

/**
 * Default timeout for checkpoints in milliseconds.
 *
 * Checkpoints are designed for fast early returns within HTTP timeout windows.
 * The default of 30 seconds provides a reasonable window for initial step(s)
 * to complete while leaving headroom before typical HTTP timeouts (60s).
 */
export const DEFAULT_CHECKPOINT_TIMEOUT_MS = 30_000; // 30 seconds

/**
 * Checkpoint definition for async workflows.
 *
 * Checkpoints allow async (Temporal) workflows to return partial bag results early
 * while continuing execution in the background. This enables patterns like:
 *
 * ```
 * HTTP Request --> Start Workflow --> Checkpoint "created" --> HTTP Response (fast)
 *                                           |
 *                                           v
 *                                     Continue: content acquisition, etc. (slow)
 * ```
 *
 * ## Scope and Limitations
 *
 * Checkpoints are designed for **fast early returns within HTTP timeout windows**.
 * They use Temporal's Update API which holds a connection open until the checkpoint
 * is reached. This works for checkpoints reached in seconds, not minutes.
 *
 * For long-running workflows, use the callback pattern instead: fire-and-forget
 * the workflow and have the final step send a webhook/callback when complete.
 *
 * ## Behavior
 *
 * - In **async (Temporal) mode**: Checkpoints work as designed - caller can await
 *   a checkpoint and receive partial bag state while workflow continues.
 * - In **sync mode**: Checkpoints are ignored - workflow always runs to completion.
 */
export interface Checkpoint {
  /** Unique name for this checkpoint (used when awaiting) */
  name: string;

  /**
   * Step name that triggers this checkpoint.
   *
   * The checkpoint fires after the **entire batch** containing this step completes.
   * This ensures all parallel steps in the batch have finished before the checkpoint.
   *
   * Supports both namespaced (`workflow.stepName`) and unnamespaced (`stepName`)
   * names - unnamespaced names are auto-resolved.
   */
  afterStep: string;

  /**
   * Optional timeout in milliseconds for this checkpoint.
   * If not specified, uses DEFAULT_CHECKPOINT_TIMEOUT_MS (30 seconds).
   */
  timeout?: number;
}

/**
 * Options for defining a checkpoint on a workflow.
 *
 * @template S - The Step type (constrained by WorkflowBuilder.checkpoint())
 */
export interface CheckpointOptions<S extends Step<any, any, any, any, any>> {
  /**
   * Step reference that triggers this checkpoint.
   *
   * Pass the actual step object, not a string name. This enables compile-time
   * validation that the step exists in the workflow.
   *
   * @example
   * ```typescript
   * .checkpoint("persisted", { afterStep: persistPromptStep })
   * ```
   */
  afterStep: S;

  /**
   * Optional timeout in milliseconds.
   * Defaults to DEFAULT_CHECKPOINT_TIMEOUT_MS if not specified.
   */
  timeout?: number;
}

/**
 * IMMUTABILITY PROTECTION FEATURE FLAG
 *
 * When enabled, all step outputs are deeply frozen before being merged into the shared bag.
 * This prevents mutation bugs that can cause non-deterministic workflow behavior.
 *
 * ## What This Protects Against:
 *
 * 1. **Parallel batch mutations**: Two steps in the same batch mutating shared references
 *    Example: Both steps call bag.identities.push(...) → race condition
 *
 * 2. **Downstream mutations**: A step mutates data it received from a previous step
 *    Example: step.run((ctx, bag) => { bag.rules.sort(); ... }) → changes data for peers
 *
 * 3. **Post-return mutations**: A step keeps a reference and mutates it later
 *    Example: setTimeout(() => myData.length = 0, 100) → corrupts bag after batch completes
 *
 * 4. **Array/nested mutations**: TypeScript's Readonly<> is shallow and can't prevent these
 *    Example: bag.identities[0].name = "x" → compiles but violates immutability
 *
 * 5. **Third-party library side effects**: SDKs that mutate arguments in place
 *    Example: someLibrary.normalize(bag.config) → unexpected mutations
 *
 * ## Why It's Feature-Flagged:
 *
 * Deep freeze has a performance cost (~0.5-10ms per output depending on object size).
 * For workflows where steps do I/O (database, API calls), this overhead is negligible.
 * For CPU-intensive workflows with large objects, it may be noticeable.
 *
 * Recommendation:
 * - Enable in development/test to catch mutation bugs early
 * - Enable in production unless profiling shows it's a bottleneck
 * - If disabled in prod, ensure code reviews catch mutation patterns
 *
 * Deep freeze is now controlled by the `deepFreeze` option in ComposerConfig.
 *
 * TODO(nate): Add observability for cost of deep freeze + any runtime validation.
 */

/**
 * Recursively freezes an object and all nested objects/arrays.
 *
 * This provides runtime immutability guarantees that TypeScript's type system cannot enforce.
 * After deep freezing, any attempt to mutate the object or its nested properties will throw
 * an error in strict mode or silently fail in non-strict mode.
 *
 * @param obj - The object to freeze
 * @returns The frozen object (same reference, now immutable)
 *
 * Performance characteristics:
 * - O(n) where n is the total number of nodes in the object graph
 * - Typical cost: 0.5-10ms for objects with 100-1000 nodes
 * - Skips already-frozen objects for efficiency
 * - Handles null/undefined/primitives safely
 */
function deepFreeze<T>(obj: T): T {
  // Skip primitives, null, undefined, and already-frozen objects
  if (!obj || typeof obj !== "object" || Object.isFrozen(obj)) {
    return obj;
  }

  // Freeze this level
  Object.freeze(obj);

  // Recursively freeze all nested values
  for (const value of Object.values(obj)) {
    deepFreeze(value);
  }

  return obj;
}

/**
 * Error handler function type for workflows.
 *
 * The error handler is invoked when a workflow step or batch fails. It controls
 * what ends up in `result.error` via its return value:
 *
 * - Return `undefined` → `result.error` is `undefined` (error fully handled)
 * - Return an `Error` → `result.error` is that error (propagate or transform)
 * - Throw → `result.error` is `WorkflowErrorHandlerFailure` (handler crashed)
 *
 * The handler can update the bag to provide result data for the caller.
 *
 * Use `extractIfPresent` to check error types through workflow wrappers:
 *
 * ```typescript
 * .onError(async (ctx, bag, error) => {
 *   const match = MyExpectedError.extractIfPresent(error);
 *   if (match) {
 *     // Update bag with fallback data
 *     bag.result = getFallbackResult();
 *     return undefined; // Handled - no error to propagate
 *   }
 *   return error; // Unknown error - propagate as-is
 * })
 * ```
 *
 * @param ctx - The workflow execution context
 * @param bag - The current bag state (includes successful outputs from the failing batch)
 * @param error - The workflow error (WorkflowStepError or WorkflowBatchError)
 * @returns `undefined` if handled, or an `Error` to propagate
 */
export type ErrorHandler<Bag extends Record<string, any>, TContext> = (
  ctx: TContext,
  bag: Bag,
  error: WorkflowStepError<Bag> | WorkflowBatchError<Bag>,
) => Error | undefined | Promise<Error | undefined>;

/**
 * Result type for workflow execution.
 *
 * Workflows never throw - all errors are captured in the `error` property.
 * This enables cleaner caller code without try/catch.
 *
 * @example
 * ```typescript
 * const result = await runWorkflow(myWorkflow, data);
 *
 * if (result.error) {
 *   // Handle error case
 *   return reply.code(500).send({ error: "Something went wrong" });
 * }
 *
 * // Success - use result.bag
 * return reply.code(200).send(result.bag.output);
 * ```
 */
export type WorkflowResult<Bag> = {
  /** The workflow bag containing all step outputs and initial data */
  bag: Bag;
  /**
   * Error if workflow failed, undefined if successful.
   * - `WorkflowBatchError`: One or more steps in a batch failed
   * - `WorkflowStepError`: Legacy single-step error (less common with allSettled)
   * - `WorkflowErrorHandlerFailure`: The error handler itself threw unexpectedly
   * - Other `Error`: Unexpected error during workflow execution
   */
  error?: WorkflowBatchError<Bag> | WorkflowStepError<Bag> | WorkflowErrorHandlerFailure | Error;
};

/**
 * A Workflow represents a collection of steps with their execution metadata.
 *
 * Generic Parameters:
 * - Bag: The complete data type that flows through the workflow
 * - RequiredInitial: Fields that must be provided when running the workflow
 * - ConfiguredValues: Fields that are pre-configured with default values
 * - Steps: The exact array of steps (used for precise return type inference)
 * - CheckpointNames: Union of checkpoint names (for compile-time validation of awaitCheckpoint)
 *
 * The workflow doesn't validate dependencies at creation time - this happens
 * at runtime in runSyncWorkflow() to account for initial fields that may satisfy dependencies.
 */
export type Workflow<
  Bag extends Record<string, any>,
  RequiredInitial extends keyof Bag = never,
  ConfiguredValues extends Partial<Bag> = Partial<Bag>,
  Steps extends readonly Step<Bag, any, any, any, any>[] = readonly Step<Bag, any, any, any, any>[],
  CheckpointNames extends string = never,
> = {
  name: string;
  steps: Steps;
  requiredInitial?: RequiredInitial[];
  configuredValues?: ConfiguredValues;
  errorHandler?: ErrorHandler<Bag, unknown>;
  /**
   * Checkpoints for async (Temporal) workflows.
   * In sync mode, checkpoints are ignored.
   */
  checkpoints?: Checkpoint[];
  /**
   * @internal Type-level tracking of checkpoint names for compile-time validation.
   * This field is never set at runtime - it's purely for TypeScript's type system.
   */
  _checkpointNames?: CheckpointNames;
};

/**
 * A WorkflowBuilder wraps a Workflow and provides the `.checkpoint()` and `.onError()` methods.
 *
 * This builder pattern allows workflows to be used directly (they satisfy the Workflow type)
 * while also allowing checkpoints and error handlers to be attached via method chaining.
 *
 * @example
 * ```typescript
 * const workflow = createWorkflow<Bag>("my-workflow")
 *   .requires("textInput")
 *   .build([persistStep, processStep, finalizeStep])
 *   .checkpoint("persisted", { afterStep: persistStep })
 *   .checkpoint("processed", { afterStep: processStep, timeout: 60000 })
 *   .onError(async (ctx, bag, error) => { ... });
 * ```
 */
export type WorkflowBuilder<
  Bag extends Record<string, any>,
  RequiredInitial extends keyof Bag = never,
  ConfiguredValues extends Partial<Bag> = Partial<Bag>,
  Steps extends readonly Step<Bag, any, any, any, any>[] = readonly Step<Bag, any, any, any, any>[],
  CheckpointNames extends string = never,
> = Workflow<Bag, RequiredInitial, ConfiguredValues, Steps, CheckpointNames> & {
  /**
   * Define a checkpoint for async (Temporal) workflows.
   *
   * Checkpoints allow async workflows to return partial bag results early while
   * continuing execution in the background. Checkpoints are ignored in sync mode.
   *
   * @param name - Unique name for this checkpoint (used when awaiting)
   * @param options - Checkpoint options including afterStep (Step reference) and optional timeout
   * @returns WorkflowBuilder for continued chaining
   *
   * @example
   * ```typescript
   * .checkpoint("created", { afterStep: persistPromptStep })
   * .checkpoint("enriched", { afterStep: acquireContentStep, timeout: 60000 })
   * ```
   */
  checkpoint<S extends Steps[number], Name extends string>(
    name: Name extends CheckpointNames ? `ERROR: Duplicate checkpoint name '${Name}'` : Name,
    options: CheckpointOptions<S>,
  ): WorkflowBuilder<Bag, RequiredInitial, ConfiguredValues, Steps, CheckpointNames | Name>;

  /**
   * Attach an error handler to the workflow.
   *
   * The handler is invoked when any step fails. Use `extractIfPresent` to check
   * error types through workflow wrappers and transform errors as needed.
   *
   * @param handler - Function to handle workflow errors
   * @returns The workflow with the error handler attached
   */
  onError<TContext = unknown>(
    handler: ErrorHandler<Bag, TContext>,
  ): Workflow<Bag, RequiredInitial, ConfiguredValues, Steps, CheckpointNames>;
};

/**
 * Runtime validation that ensures all step names are unique.
 *
 * Step names are used as primary keys in dependency maps (stepsByName, children, indegree).
 * Duplicate names would cause silent overwrites in these maps, corrupting the DAG structure
 * and leading to wrong execution order or missing dependencies.
 *
 * This function provides clear error messages that help developers identify which steps
 * have conflicting names and where they appear in the workflow definition.
 */
function assertUniqueStepNames<Bag extends Record<string, any>>(
  steps: readonly Step<Bag, any, any, any, any>[],
): void {
  const seen = new Map<string, number>(); // Track first occurrence index for better errors

  steps.forEach((step, index) => {
    const stepName = step.name;
    if (seen.has(stepName)) {
      const firstIndex = seen.get(stepName)!;
      throw new Error(
        `Duplicate step name "${stepName}" found at positions ${firstIndex} and ${index}. ` +
          `Step names must be unique within a workflow.\n\n` +
          `💡 Fix: Rename one of the steps to have a unique name.`,
      );
    }
    seen.set(stepName, index);
  });
}

/**
 * Helper function to find the first duplicate key in an array.
 *
 * Returns undefined if no duplicates exist, or the duplicated key if found.
 */
function findDuplicateKey(keys: readonly (string | number | symbol)[]): string | undefined {
  const seen = new Set<string>();
  for (const key of keys) {
    const keyStr = String(key);
    if (seen.has(keyStr)) {
      return keyStr;
    }
    seen.add(keyStr);
  }
  return undefined;
}

/**
 * Runtime validation that ensures no duplicates exist in step needs/provides arrays.
 *
 * Duplicate needs cause deadlocks in the topological sort (indegree is incremented twice
 * but only decremented once, so the step never reaches zero and never executes).
 *
 * Duplicate provides violate the semantic contract and make observability confusing,
 * even though they don't break execution (the field only exists once in the return object).
 */
function assertNoDuplicatesInStepArrays<Bag extends Record<string, any>>(
  steps: readonly Step<Bag, any, any, any, any>[],
): void {
  for (const step of steps) {
    const duplicateNeed = findDuplicateKey(step.needs as readonly (string | number | symbol)[]);
    if (duplicateNeed) {
      throw new Error(
        `Step "${step.name}" lists "${duplicateNeed}" twice in "needs". ` +
          `Each dependency should only be declared once.\n\n` +
          `💡 Fix: Remove the duplicate "${duplicateNeed}" from the needs array.`,
      );
    }

    const duplicateProvide = findDuplicateKey(
      step.provides as readonly (string | number | symbol)[],
    );
    if (duplicateProvide) {
      throw new Error(
        `Step "${step.name}" lists "${duplicateProvide}" twice in "provides". ` +
          `Each output field should only be declared once.\n\n` +
          `💡 Fix: Remove the duplicate "${duplicateProvide}" from the provides array.`,
      );
    }
  }
}

/**
 * Marker type for composed workflows.
 * Used by the `use()` helper to enable workflow composition.
 * This marker is detected during workflow building and the child workflow's steps are flattened.
 */
export type ComposedWorkflow<
  Bag extends Record<string, any>,
  RequiredInitial extends keyof Bag = never,
  ConfiguredValues extends Partial<Bag> = Partial<Bag>,
  Steps extends readonly Step<Bag, any, any, any, any>[] = readonly Step<Bag, any, any, any, any>[],
> = {
  __composedWorkflow: true;
  workflow: Workflow<Bag, RequiredInitial, ConfiguredValues, Steps>;
};

/**
 * Composes a child workflow into a parent workflow.
 *
 * When building a workflow, use `use(childWorkflow)` to include all steps from the child.
 * The steps will be:
 * - Flattened into the parent's dependency graph for maximum parallelization
 * - Automatically namespaced as "childWorkflowName.stepName" to prevent conflicts
 * - Tracked with workflow provenance metadata for proper observability
 *
 * Any initial requirements from the child workflow become regular dependencies that
 * must be satisfied by the parent's steps or initial data.
 *
 * Example:
 * ```typescript
 * const childWf = createWorkflow<Bag>("child").build([stepA, stepB]);
 * const parentWf = createWorkflow<Bag>("parent").build([
 *   stepX,
 *   use(childWf),  // Includes stepA and stepB with proper namespacing
 *   stepY,
 * ]);
 *
 * ## Observability
 *
 * Composed workflows create SubWorkflow spans in traces, making it easy to see logical
 * boundaries even though execution is flattened:
 * ```
 * Workflow: main
 *   └─ Batch 1
 *      └─ SubWorkflow: authentication
 *         └─ Step: authentication.validateUser
 * ```
 */
export function use<
  Bag extends Record<string, any>,
  RequiredInitial extends keyof Bag = never,
  ConfiguredValues extends Partial<Bag> = Partial<Bag>,
  Steps extends readonly Step<Bag, any, any, any, any>[] = readonly Step<Bag, any, any, any, any>[],
>(
  workflow: Workflow<Bag, RequiredInitial, ConfiguredValues, Steps>,
): ComposedWorkflow<Bag, RequiredInitial, ConfiguredValues, Steps> {
  return {
    __composedWorkflow: true,
    workflow,
  };
}

function workflow<
  Bag extends Record<string, any>,
  RequiredInitial extends keyof Bag = never,
  ConfiguredValues extends Partial<Bag> = Partial<Bag>,
  Steps extends readonly Step<Bag, any, any, any, any>[] = readonly Step<Bag, any, any, any, any>[],
>(
  name: string,
  steps: Steps,
  requiredInitial?: RequiredInitial[],
  configuredValues?: ConfiguredValues,
  checkpoints?: Checkpoint[],
): WorkflowBuilder<Bag, RequiredInitial, ConfiguredValues, Steps, never> {
  // Explicitly specify all 5 type parameters to ensure CheckpointNames is `never`
  const wf: Workflow<Bag, RequiredInitial, ConfiguredValues, Steps, never> = {
    name,
    steps,
    ...(requiredInitial && requiredInitial.length > 0 ? { requiredInitial } : {}),
    ...(configuredValues && Object.keys(configuredValues).length > 0 ? { configuredValues } : {}),
    ...(checkpoints && checkpoints.length > 0 ? { checkpoints } : {}),
  };

  // Create a builder function that returns a new WorkflowBuilder with updated checkpoints
  // The CheckpointNames type parameter is tracked at compile-time only; runtime uses currentCheckpoints
  function createBuilder<CurrentCheckpointNames extends string = never>(
    currentWf: Workflow<Bag, RequiredInitial, ConfiguredValues, Steps, CurrentCheckpointNames>,
    currentCheckpoints: Checkpoint[],
  ): WorkflowBuilder<Bag, RequiredInitial, ConfiguredValues, Steps, CurrentCheckpointNames> {
    // Implementation uses 'any' for internal assignment since TypeScript can't track
    // the conditional types through Object.assign. The public interface types are correct.
    const builder = {
      ...currentWf,
      checkpoint<S extends Steps[number], Name extends string>(
        checkpointName: Name,
        options: CheckpointOptions<S>,
      ): WorkflowBuilder<
        Bag,
        RequiredInitial,
        ConfiguredValues,
        Steps,
        CurrentCheckpointNames | Name
      > {
        // Validate checkpoint name is unique (runtime validation as safety net)
        if (currentCheckpoints.some((cp) => cp.name === checkpointName)) {
          throw new Error(
            `Duplicate checkpoint name "${checkpointName}". Checkpoint names must be unique within a workflow.`,
          );
        }

        // Extract step name from step reference
        const stepName = options.afterStep.name;

        // Validate step exists (supports both namespaced and unnamespaced)
        // The TypeScript constraint S extends Steps[number] provides compile-time validation,
        // but we still do runtime validation for composed workflows where step names get namespaced
        const resolvedStepName = resolveStepName(steps, stepName);
        if (!resolvedStepName) {
          const availableSteps = steps.map((s) => s.name).join(", ");
          throw new Error(
            `Checkpoint "${checkpointName}" references unknown step "${stepName}". ` +
              `Available steps: [${availableSteps}]`,
          );
        }

        const newCheckpoint: Checkpoint = {
          name: checkpointName,
          afterStep: resolvedStepName, // Use resolved (namespaced) name
          timeout: options.timeout,
        };

        const newCheckpoints = [...currentCheckpoints, newCheckpoint];
        const newWf = {
          ...currentWf,
          checkpoints: newCheckpoints,
        };

        return createBuilder<CurrentCheckpointNames | Name>(
          newWf as Workflow<
            Bag,
            RequiredInitial,
            ConfiguredValues,
            Steps,
            CurrentCheckpointNames | Name
          >,
          newCheckpoints,
        );
      },

      onError<TContext = unknown>(
        handler: ErrorHandler<Bag, TContext>,
      ): Workflow<Bag, RequiredInitial, ConfiguredValues, Steps, CurrentCheckpointNames> {
        return {
          ...currentWf,
          checkpoints: currentCheckpoints.length > 0 ? currentCheckpoints : undefined,
          errorHandler: handler as ErrorHandler<Bag, unknown>,
        };
      },
    };
    return builder as WorkflowBuilder<
      Bag,
      RequiredInitial,
      ConfiguredValues,
      Steps,
      CurrentCheckpointNames
    >;
  }

  // Explicitly pass 'never' as the type parameter since no checkpoints are defined yet
  return createBuilder<never>(wf, checkpoints ?? []);
}

/**
 * Resolves a step name, handling both namespaced and unnamespaced names.
 *
 * - If `stepName` exactly matches a step, returns it as-is.
 * - If `stepName` doesn't match but matches the suffix of a namespaced step
 *   (e.g., "validateUser" matches "auth.validateUser"), returns the full namespaced name.
 * - If multiple steps match the unnamespaced suffix, throws an error (ambiguous).
 * - Returns null if no match is found.
 */
function resolveStepName<Bag extends Record<string, any>>(
  steps: readonly Step<Bag, any, any, any, any>[],
  stepName: string,
): string | null {
  // Direct match - return as-is
  const directMatch = steps.find((s) => s.name === stepName);
  if (directMatch) {
    return directMatch.name;
  }

  // Try to find by suffix (unnamespaced name matching namespaced step)
  const suffixMatches = steps.filter((s) => {
    const parts = s.name.split(".");
    const unnamespaced = parts[parts.length - 1];
    return unnamespaced === stepName;
  });

  if (suffixMatches.length === 1) {
    return suffixMatches[0]!.name;
  }

  if (suffixMatches.length > 1) {
    const matchingNames = suffixMatches.map((s) => s.name).join(", ");
    throw new Error(
      `Ambiguous step name "${stepName}" matches multiple steps: [${matchingNames}]. ` +
        `Use the full namespaced name to specify which step.`,
    );
  }

  return null;
}

/**
 * Type guard to check if an item is a ComposedWorkflow marker.
 */
function isComposedWorkflow(item: any): item is ComposedWorkflow<any, any, any> {
  return item && typeof item === "object" && item.__composedWorkflow === true;
}

/**
 * Flattens a step array that may contain ComposedWorkflow markers.
 *
 * This function:
 * - Recursively processes the array to handle nested composition
 * - Namespaces steps as "workflowName.stepName" to prevent conflicts
 * - Tracks workflow provenance in the workflowPath property
 * - Returns a flat array of steps ready for execution
 *
 * @param items - Array of steps and/or ComposedWorkflow markers
 * @param parentPath - The workflow path of the parent (for nested composition)
 * @returns Flattened array of steps with namespacing and provenance
 */
function flattenSteps<Bag extends Record<string, any>>(
  items: readonly (Step<Bag, any, any, any, any> | ComposedWorkflow<Bag, any, any>)[],
  parentPath: string[] = [],
): Step<Bag, any, any, any, any>[] {
  const result: Step<Bag, any, any, any, any>[] = [];

  for (const item of items) {
    if (isComposedWorkflow(item)) {
      const childWorkflow = item.workflow;
      const childPath = [...parentPath, childWorkflow.name];

      // Recursively flatten child workflow's steps
      const childSteps = flattenSteps(childWorkflow.steps, childPath);

      // Namespace each step and set workflow path
      for (const step of childSteps) {
        // Build the namespaced name: if step already has path, preserve it
        const namespacedName = step.workflowPath
          ? step.name // Already namespaced from nested composition
          : `${childWorkflow.name}.${step.name}`;

        // For workflow path: if step already has one from nested composition,
        // it starts with the child workflow's name, so we need to prepend the parent path
        let workflowPath: string[];
        if (step.workflowPath && step.workflowPath.length > 0) {
          // Step already has a path from being in a composed child workflow
          // Prepend parent path (excluding the child workflow name since it's already in step.workflowPath)
          workflowPath = [...parentPath, ...step.workflowPath];
        } else {
          // Step is directly in this child workflow
          workflowPath = childPath;
        }

        result.push({
          ...step,
          name: namespacedName,
          workflowPath,
        });
      }
    } else {
      // Regular step - add as-is
      result.push(item as Step<Bag, any, any, any, any>);
    }
  }

  return result;
}

/**
 * Creates a workflow builder with compile-time dependency validation.
 *
 * This builder provides three modes:
 * 1. build() - for workflows that don't require initial data
 * 2. requires(fields).build() - for workflows that need the caller to provide specific fields
 * 3. configure(configuration).requires(fields).build() - for workflows that need the bag
 *    configured with specific values, as well as initial values provided at runtime
 *
 * The compile-time validation works by:
 * - Analyzing the step dependencies using recursive conditional types
 * - If validation fails, injecting an error property that causes a compile error
 * - If validation passes, allowing the workflow to be created normally
 *
 * Usage:
 *   // No initial data required
 *   const wf1 = createWorkflow<Bag>("my-workflow").build([stepA, stepB]);
 *
 *   // Requires 'promptText' to be provided at runtime
 *   const wf2 = createWorkflow<Bag>("prompt-workflow").requires("promptText").build([stepA, stepB]);
 *
 *   // Sets lotiContentType to "prompt_media" and requires 'promptText' to be
 *   provided at runtime.
 *   const wf3 = createWorkflow<Bag>("configured-prompt-workflow")
 *     .configure({ lotiContentType: "prompt_media" })
 *     .requires("promptText")
 *     .build([stepA, stepB]);
 */
export function createWorkflow<Bag extends Record<string, any>>(name: string) {
  return {
    // Build workflow without required initial fields
    build<
      const Steps extends readonly (
        | Step<Bag, any, any, any, any>
        | ComposedWorkflow<Bag, any, any>
      )[],
    >(
      steps: Steps &
        // If validation fails, inject an error property to cause compile-time error
        (ValidateWorkflowDeps<Bag, Steps> extends string
          ? { __WORKFLOW_ERROR__: ValidateWorkflowDeps<Bag, Steps> }
          : unknown),
    ): WorkflowBuilder<Bag, never, never, FlattenStepsType<Bag, Steps>, never> {
      const flattenedSteps = flattenSteps(steps, [name]);
      assertUniqueStepNames(flattenedSteps);
      assertNoDuplicatesInStepArrays(flattenedSteps);
      return workflow(name, flattenedSteps, undefined) as unknown as WorkflowBuilder<
        Bag,
        never,
        never,
        FlattenStepsType<Bag, Steps>,
        never
      >;
    },

    // Allow specifying initial fields that will be provided at runtime (rest parameters)
    requires<Initial extends keyof Bag>(...fields: Initial[]) {
      return {
        // Build workflow with required initial fields
        build<
          const Steps extends readonly (
            | Step<Bag, any, any, any, any>
            | ComposedWorkflow<Bag, any, any>
          )[],
        >(
          steps: Steps &
            // Validate dependencies considering the initial fields will be available
            (ValidateWorkflowDepsWithInitial<Bag, Steps, Initial> extends string
              ? { __WORKFLOW_ERROR__: ValidateWorkflowDepsWithInitial<Bag, Steps, Initial> }
              : unknown),
        ): WorkflowBuilder<Bag, Initial, never, FlattenStepsType<Bag, Steps>, never> {
          const flattenedSteps = flattenSteps(steps, [name]);
          assertUniqueStepNames(flattenedSteps);
          assertNoDuplicatesInStepArrays(flattenedSteps);
          return workflow<Bag, Initial, never, typeof flattenedSteps>(
            name,
            flattenedSteps,
            fields,
          ) as unknown as WorkflowBuilder<Bag, Initial, never, FlattenStepsType<Bag, Steps>, never>;
        },
      };
    },

    configure<Configuration extends Partial<Bag>>(configuration: Configuration) {
      type ConfigKeys = Extract<keyof Configuration, keyof Bag>;

      return {
        build<
          const Steps extends readonly (
            | Step<Bag, any, any, any, any>
            | ComposedWorkflow<Bag, any, any>
          )[],
        >(
          steps: Steps &
            (ValidateWorkflowDepsWithInitial<Bag, Steps, ConfigKeys> extends string
              ? {
                  __WORKFLOW_ERROR__: ValidateWorkflowDepsWithInitial<Bag, Steps, ConfigKeys>;
                }
              : unknown),
        ): WorkflowBuilder<Bag, never, Configuration, FlattenStepsType<Bag, Steps>, never> {
          const flattenedSteps = flattenSteps(steps, [name]);
          assertUniqueStepNames(flattenedSteps);
          assertNoDuplicatesInStepArrays(flattenedSteps);
          return workflow(name, flattenedSteps, undefined, configuration) as WorkflowBuilder<
            Bag,
            never,
            Configuration,
            FlattenStepsType<Bag, Steps>,
            never
          >;
        },
        // Exclude ConfigKeys to prevent requiring fields that are already configured
        requires<Initial extends Exclude<keyof Bag, ConfigKeys>>(...fields: Initial[]) {
          type Combined = Initial | ConfigKeys;
          return {
            build<
              const Steps extends readonly (
                | Step<Bag, any, any, any, any>
                | ComposedWorkflow<Bag, any, any>
              )[],
            >(
              steps: Steps &
                (ValidateWorkflowDepsWithInitial<Bag, Steps, Combined> extends string
                  ? {
                      __WORKFLOW_ERROR__: ValidateWorkflowDepsWithInitial<Bag, Steps, Combined>;
                    }
                  : unknown),
            ): WorkflowBuilder<Bag, Initial, Configuration, FlattenStepsType<Bag, Steps>, never> {
              const flattenedSteps = flattenSteps(steps, [name]);
              assertUniqueStepNames(flattenedSteps);
              assertNoDuplicatesInStepArrays(flattenedSteps);
              return workflow(
                name,
                flattenedSteps,
                fields,
                configuration as Configuration,
              ) as WorkflowBuilder<
                Bag,
                Initial,
                Configuration,
                FlattenStepsType<Bag, Steps>,
                never
              >;
            },
          };
        },
      };
    },
  };
}

/**
 * Recursively flattens a steps array that may contain ComposedWorkflow markers.
 * This mirrors the runtime flattenSteps() function but at the type level.
 */
type FlattenStepsType<
  Bag extends Record<string, any>,
  Items extends readonly (Step<Bag, any, any, any, any> | ComposedWorkflow<Bag, any, any>)[],
> = Items extends readonly [infer Head, ...infer Tail]
  ? Head extends ComposedWorkflow<Bag, any, any, infer ChildSteps>
    ? Tail extends readonly (Step<Bag, any, any, any, any> | ComposedWorkflow<Bag, any, any>)[]
      ? [...ChildSteps, ...FlattenStepsType<Bag, Tail>]
      : ChildSteps
    : Head extends Step<Bag, any, any, any, any>
      ? Tail extends readonly (Step<Bag, any, any, any, any> | ComposedWorkflow<Bag, any, any>)[]
        ? [Head, ...FlattenStepsType<Bag, Tail>]
        : [Head]
      : []
  : [];

/**
 * Compile-time dependency validation for workflows with initial fields.
 *
 * This delegates to ValidateStepsInOrder with the initial fields marked as both:
 * - Available: Can be used to satisfy step dependencies
 * - Initial: Protected from being overwritten by steps
 *
 * Composed workflows are flattened, and their initial requirements must be satisfied
 * by either the parent workflow's initial data or preceding steps.
 */
type ValidateWorkflowDepsWithInitial<
  Bag extends Record<string, any>,
  Steps extends readonly (Step<Bag, any, any, any, any> | ComposedWorkflow<Bag, any, any>)[],
  Initial extends keyof Bag,
> = ValidateStepsInOrder<Bag, FlattenStepsType<Bag, Steps>, Initial, Initial>;

/**
 * Main compile-time dependency validation for workflows without initial fields.
 *
 * Special case handling:
 * - Single step workflows: Check if the step has dependencies (if so, error)
 * - Multi-step workflows: Use recursive ValidateStepsInOrder starting with no available fields
 *
 * This ensures clear error messages for the common case of single-step workflows.
 * Composed workflows are flattened, and their initial requirements must be satisfied
 * by preceding steps (since there are no initial fields in this workflow).
 */
type ValidateWorkflowDeps<
  Bag extends Record<string, any>,
  Steps extends readonly (Step<Bag, any, any, any, any> | ComposedWorkflow<Bag, any, any>)[],
> =
  FlattenStepsType<Bag, Steps> extends readonly [infer Head]
    ? Head extends Step<Bag, infer Needs, any, any>
      ? Needs[number] extends never
        ? Steps // No dependencies, valid
        : WorkflowValidationResult<Needs[number] extends string ? Needs[number] : "unknown", "none">
      : Steps
    : ValidateStepsInOrder<Bag, FlattenStepsType<Bag, Steps>, never, never>;

/**
 * Template literal type that creates detailed, human-readable error messages
 * for dependency validation failures.
 *
 * Example output:
 * "❌ WORKFLOW ERROR: A step needs identities but it is not available. Available fields: [promptText]
 *  → FIX: Add a step that provides identities before the step that needs it"
 */
type WorkflowValidationResult<
  MissingField extends string,
  AvailableFields extends string,
> = `❌ WORKFLOW ERROR: A step needs ${MissingField} but it is not available. Available fields: [${AvailableFields extends "none" ? "none" : AvailableFields}] → FIX: Add a step that provides ${MissingField} before the step that needs it`;

// Error type for initial field overwrite attempts
type InitialOverwriteError<FieldName extends string> =
  `❌ WORKFLOW ERROR: A step cannot overwrite initial field ${FieldName}. Initial data is protected from modification. → FIX: Remove ${FieldName} from the step's provides array or from initial data`;

/**
 * Helper types for precise return type inference.
 *
 * These types work together to determine exactly which fields will be present
 * in the workflow result, enabling compile-time checking when accessing results.
 */

/**
 * Recursively extracts all 'provides' fields from an array of steps.
 * This creates a union type of all fields that will be produced by the workflow.
 *
 * Example:
 * - Step1 provides: ["a", "b"]
 * - Step2 provides: ["c"]
 * - Result: "a" | "b" | "c"
 */
type ExtractStepProvides<T> =
  T extends Step<any, any, infer Provides, any> ? Provides[number] : never;

type ExtractAllProvides<Steps extends readonly any[]> = Steps extends readonly [
  infer Head,
  ...infer Tail,
]
  ? ExtractStepProvides<Head> | ExtractAllProvides<Tail>
  : never;

/**
 * Infers the precise return type of a workflow execution.
 *
 * The result includes:
 * - All fields provided by workflow steps (ExtractAllProvides<Steps>)
 * - All initial fields passed to the workflow (Initial)
 *
 * This allows TypeScript to know exactly which properties exist on the result,
 * enabling compile-time checking when accessing workflow results.
 *
 * Note: Steps parameter should already be flattened when this type is used.
 */
export type InferWorkflowResult<
  Bag extends Record<string, any>,
  Steps extends readonly Step<Bag, any, any, any, any>[],
  Initial extends keyof Bag = never,
  Configured extends keyof Bag = never,
> = Pick<Bag, ExtractAllProvides<Steps> | Initial | Configured>;

/**
 * Safely extracts configured keys from a Config type parameter.
 *
 * This handles the edge case where Config is `never` (no configured values).
 * When Config is `never`, `keyof never` returns `string | number | symbol`,
 * which would incorrectly include all keys. This type returns `never` in that case.
 */
export type SafeConfiguredKeys<Config, Bag extends Record<string, any>> = [Config] extends [never]
  ? never
  : Extract<keyof Config, keyof Bag>;

/**
 * Extracts the ConfiguredValues type from a Workflow type.
 * Returns never if the workflow has no configured values.
 */
export type ExtractWorkflowConfig<W> = W extends Workflow<any, any, infer C, any, any> ? C : never;

/**
 * Extracts the Bag type from a Workflow type.
 */
export type ExtractWorkflowBag<W> = W extends Workflow<infer B, any, any, any, any> ? B : never;

/**
 * Extracts the Steps type from a Workflow type.
 */
export type ExtractWorkflowSteps<W> = W extends Workflow<any, any, any, infer S, any> ? S : never;

/**
 * Extracts the RequiredInitial type from a Workflow type.
 */
export type ExtractWorkflowRequiredInitial<W> =
  W extends Workflow<any, infer R, any, any, any> ? R : never;

/**
 * Extracts the CheckpointNames type from a Workflow type.
 * Returns `never` if no checkpoints are defined.
 *
 * Note: We use property access via _checkpointNames rather than type parameter
 * inference because TypeScript's `infer` can return the constraint (`string`)
 * instead of the actual value (`never`) in some cases.
 */
export type ExtractWorkflowCheckpointNames<W> = W extends { _checkpointNames?: infer C }
  ? C extends string
    ? C
    : never
  : never;

/**
 * Recursive type that validates step dependencies in order.
 *
 * This is the core validation logic that:
 * 1. Processes steps one by one from left to right
 * 2. Checks if each step's 'needs' are satisfied by available fields
 * 3. Adds each step's 'provides' to the available fields for subsequent steps
 * 4. Prevents steps from overwriting initial fields
 * 5. Accumulates provided fields for subsequent steps
 *
 * Parameters:
 * - Bag: The complete data type
 * - Steps: Remaining steps to validate
 * - Available: Fields currently available (from previous steps + initial)
 * - Initial: Fields provided initially (protected from overwriting)
 *
 * Returns:
 * - Steps (unchanged) if validation passes
 * - Error message string if validation fails
 */
type ValidateStepsInOrder<
  Bag extends Record<string, any>,
  Steps extends readonly Step<Bag, any, any, any, any>[],
  Available extends keyof Bag,
  Initial extends keyof Bag = never,
> = Steps extends readonly [infer Head, ...infer Tail]
  ? Head extends Step<Bag, infer Needs, infer Provides, any>
    ? Tail extends readonly Step<Bag, any, any, any, any>[]
      ? // First check if step tries to overwrite initial fields
        // Use Extract to check if ANY provided field intersects with Initial
        Extract<Provides[number], Initial> extends never
        ? // No overlap - proceed with validation
          // Then check if all needs are satisfied by available fields
          Needs[number] extends Available
          ? ValidateStepsInOrder<Bag, Tail, Available | Provides[number], Initial>
          : // Return detailed error if validation fails
            WorkflowValidationResult<
              Exclude<Needs[number], Available> extends string
                ? Exclude<Needs[number], Available>
                : "unknown",
              Available extends never ? "none" : Available extends string ? Available : "unknown"
            >
        : // Error: step is trying to overwrite at least one initial field
          InitialOverwriteError<
            Extract<Provides[number], Initial> extends string
              ? Extract<Provides[number], Initial>
              : "unknown"
          >
      : Steps
    : Steps
  : Steps; // Return Steps if validation passes

// Overload for workflows without required initial fields
export async function runSyncWorkflow<
  Bag extends Record<string, any>,
  Config extends Partial<Bag>,
  Steps extends readonly Step<Bag, any, any, any, any>[],
>(
  wf: Workflow<Bag, never, Config, Steps>,
  initialData?: Partial<Bag>,
  contextProvider?: StepContextProvider<unknown>,
  logger?: ComposerLogger,
  enableDeepFreeze?: boolean,
): Promise<
  WorkflowResult<
    InferWorkflowResult<Bag, Steps, never, Extract<keyof Config, keyof Bag>> & Partial<Bag>
  >
>;

// Overload for workflows with required initial fields
export async function runSyncWorkflow<
  Bag extends Record<string, any>,
  RequiredInitial extends keyof Bag,
  Config extends Partial<Bag>,
  Steps extends readonly Step<Bag, any, any, any, any>[],
>(
  wf: Workflow<Bag, RequiredInitial, Config, Steps>,
  initialData: Partial<Bag> & Pick<Bag, RequiredInitial>,
  contextProvider?: StepContextProvider<unknown>,
  logger?: ComposerLogger,
  enableDeepFreeze?: boolean,
): Promise<
  WorkflowResult<InferWorkflowResult<Bag, Steps, RequiredInitial, Extract<keyof Config, keyof Bag>>>
>;

// Implementation
export async function runSyncWorkflow<
  Bag extends Record<string, any>,
  RequiredInitialData extends keyof Bag = never,
  ConfiguredValues extends Partial<Bag> = Partial<Bag>,
  Steps extends readonly Step<Bag, any, any, any, any>[] = readonly Step<Bag, any, any, any, any>[],
>(
  wf: Workflow<Bag, RequiredInitialData, ConfiguredValues, Steps>,
  initialData: RequiredInitialData extends never
    ? Partial<Bag> | undefined
    : Partial<Bag> & Pick<Bag, RequiredInitialData>,
  contextProvider?: StepContextProvider<unknown>,
  logger?: ComposerLogger,
  enableDeepFreeze?: boolean,
): Promise<
  WorkflowResult<
    InferWorkflowResult<Bag, Steps, RequiredInitialData, Extract<keyof ConfiguredValues, keyof Bag>>
  >
> {
  // Resolve defaults for optional parameters
  const log = logger ?? defaultLogger;
  const deepFreeze_ = enableDeepFreeze ?? false;

  // Generate workflowId for observability tracking
  const workflowId = uuidv7() as UUIDV7;

  // Initialize bag outside try block so it's accessible in catch block
  const configuredValues = (wf.configuredValues ?? {}) as Partial<Bag>;
  const bag: Bag = {
    ...initialData,
    ...configuredValues,
  } as Bag;

  // Start workflow observability, catching any initialization errors
  let workflowHandle: ReturnType<typeof startWorkflowObservability>;
  try {
    workflowHandle = startWorkflowObservability(workflowId, wf, initialData, log);
  } catch (observabilityError) {
    // Observability initialization failed - return error without throwing
    const error =
      observabilityError instanceof Error
        ? observabilityError
        : new Error(String(observabilityError));
    return {
      bag: bag as InferWorkflowResult<
        Bag,
        Steps,
        RequiredInitialData,
        Extract<keyof ConfiguredValues, keyof Bag>
      >,
      error: new WorkflowStepError({
        workflowId,
        stepName: "__observability_init__",
        batchNumber: 0,
        originalError: error,
        bagState: bag,
        workflowPath: [],
      }),
    };
  }

  // Track execution context for enhanced error logging
  const executionContext: ExecutionContext = {
    stepName: undefined,
    stepNumber: undefined,
    totalSteps: wf.steps.length,
    batchNumber: 0,
    stepStartTime: undefined,
  };

  try {
    // === PHASE 1: VALIDATION & PLANNING ===
    // Validate that all step names are unique (defense-in-depth for dynamically constructed workflows)
    assertUniqueStepNames(wf.steps);

    // Validate that no step has duplicate entries in needs/provides arrays
    assertNoDuplicatesInStepArrays(wf.steps);

    // Plan workflow execution using topological sort
    // This validates dependencies, detects cycles, and organizes steps into parallel execution batches
    const initialFields = new Set<keyof Bag>([
      ...(Object.keys(configuredValues) as (keyof Bag)[]),
      ...(Object.keys(initialData ?? {}) as (keyof Bag)[]),
    ]);
    const plan = planWorkflowBatches([...wf.steps], initialFields);

    // === PHASE 2: EXECUTE BATCHES ===
    // Each batch contains steps that can run in parallel
    for (let batchIndex = 0; batchIndex < plan.batches.length; batchIndex++) {
      const batch = plan.batches[batchIndex]!; // Safe: within bounds check
      const batchNumber = batchIndex + 1;

      // Update execution context for current batch
      executionContext.batchNumber = batchNumber;

      // Start batch observability
      const batchHandle = startBatchObservability(
        workflowHandle,
        batchNumber,
        batch.map((step) => step.name),
      );

      // Execute all steps in this batch in parallel using allSettled
      // This ensures all steps complete (even if some fail) so we can:
      // 1. Collect outputs from successful steps
      // 2. Aggregate all failures into a single WorkflowBatchError
      const settledResults = await Promise.allSettled(
        batch.map(async (step) => {
          // Update execution context for current step
          executionContext.stepName = step.name;
          executionContext.stepNumber =
            wf.steps.findIndex((workflowStep) => workflowStep.name === step.name) + 1; // 1-based indexing
          executionContext.stepStartTime = Date.now();

          // Start step observability
          const stepHandle = startStepObservability(workflowHandle, batchHandle, step);

          // Create context via beforeStep hook if provider exists
          let ctx: unknown;
          if (contextProvider) {
            ctx = await contextProvider.beforeStep(step.name);
          }

          let stepError: Error | undefined;
          try {
            let stepOutput: Record<string, unknown>;

            if (isFanOutStep(step)) {
              stepOutput = await executeFanOut(
                step.__fanOut,
                step.name,
                bag as Record<string, unknown>,
                contextProvider,
                log,
                deepFreeze_,
              );
            } else {
              // Build input object with only the fields this step needs
              const stepInput = step.needs.reduce(
                (inputAcc: Partial<Bag>, neededField: keyof Bag) => {
                  inputAcc[neededField] = bag[neededField];
                  return inputAcc;
                },
                {} as Partial<Bag>,
              );

              // Freeze the step input if immutability protection is enabled
              // This prevents steps from modifying their input bag
              if (deepFreeze_) {
                Object.freeze(stepInput);
              }

              const syncRuntime: AsyncStepRuntime = {
                heartbeat: () => {},
                getHeartbeatDetails: () => undefined,
              };
              // Object.assign preserves context identity and prototype chain when ctx is
              // a class instance, so afterStep receives the same object the step mutated.
              // Falls back to spread when ctx is undefined (no context provider).
              const stepCtx =
                ctx != null && typeof ctx === "object"
                  ? Object.assign(ctx, syncRuntime)
                  : syncRuntime;

              // Execute the step within the OTel step context for proper span hierarchy
              stepOutput = (await otelContext.with(stepHandle.stepContext, () =>
                step.run(stepCtx, stepInput),
              )) as Record<string, unknown>;
            }

            // End step observability - success
            endStepObservability(stepHandle, {
              success: true,
              outputFields: Object.keys(stepOutput),
            });

            return { stepName: step.name, output: stepOutput };
          } catch (error) {
            // Wrap error with workflow context
            stepError = error instanceof Error ? error : new Error(String(error));
            const workflowError = new WorkflowStepError({
              workflowId: workflowId,
              stepName: step.name,
              batchNumber,
              originalError: stepError,
              bagState: bag,
              workflowPath: step.workflowPath,
            });

            // End step observability - error
            endStepObservability(stepHandle, {
              success: false,
              error: workflowError,
            });
            throw workflowError;
          } finally {
            // Call afterStep hook for cleanup if provider exists.
            // Wrapped in try/catch to prevent cleanup errors (e.g., EM flush failures)
            // from swallowing the original step error. The step's business logic error
            // is more important to surface than infrastructure cleanup errors.
            if (contextProvider && ctx !== undefined) {
              try {
                await contextProvider.afterStep(ctx, stepError);
              } catch (cleanupError) {
                log.error("afterStep cleanup failed", {
                  stepName: step.name,
                  workflowId,
                  cleanupError:
                    cleanupError instanceof Error ? cleanupError.message : String(cleanupError),
                  originalStepError: stepError?.message,
                });
              }
            }
          }
        }),
      );

      // Separate fulfilled and rejected results
      const fulfilled = settledResults.filter(
        (r): r is PromiseFulfilledResult<{ stepName: string; output: any }> =>
          r.status === "fulfilled",
      );
      const rejected = settledResults.filter(
        (r): r is PromiseRejectedResult => r.status === "rejected",
      );

      // First, merge successful step outputs into the bag
      // This ensures the bag has all available data even if some steps failed
      for (const result of fulfilled) {
        const { output } = result.value;
        if (deepFreeze_) {
          for (const [key, value] of Object.entries(output)) {
            (bag as any)[key] = deepFreeze(value);
          }
        } else {
          Object.assign(bag, output);
        }
      }

      // If any steps failed, aggregate into WorkflowBatchError
      if (rejected.length > 0) {
        const stepErrors = rejected.map((r) => r.reason as WorkflowStepError<Bag>);

        // End batch observability - error
        endBatchObservability(batchHandle, {
          success: false,
          error: stepErrors[0]!, // Primary error for observability (safe: length > 0)
        });

        throw new WorkflowBatchError({
          errors: stepErrors,
          bagState: bag,
          batchNumber,
          workflowId,
        });
      }

      // End batch observability - success
      endBatchObservability(batchHandle, {
        success: true,
        outputFields: fulfilled.flatMap((r) => Object.keys(r.value.output)),
      });
    }

    // End workflow observability - success
    endWorkflowObservability(
      workflowHandle,
      {
        success: true,
        batchCount: plan.batches.length,
        outputFields: Object.keys(bag),
      },
      executionContext,
      bag,
    );

    return {
      bag: bag as InferWorkflowResult<
        Bag,
        Steps,
        RequiredInitialData,
        Extract<keyof ConfiguredValues, keyof Bag>
      >,
      error: undefined,
    };
  } catch (error) {
    // End workflow observability - error
    endWorkflowObservability(
      workflowHandle,
      {
        success: false,
        error: error instanceof Error ? error : new Error(String(error)),
      },
      executionContext,
      bag,
    );

    // Cast bag for return type
    const resultBag = bag as InferWorkflowResult<
      Bag,
      Steps,
      RequiredInitialData,
      Extract<keyof ConfiguredValues, keyof Bag>
    >;

    // Invoke error handler if present
    if (
      wf.errorHandler &&
      (error instanceof WorkflowStepError || error instanceof WorkflowBatchError)
    ) {
      // Get context for error handler
      let ctx: unknown;
      if (contextProvider) {
        try {
          ctx = await contextProvider.beforeStep("__errorHandler__");
        } catch (contextError) {
          // If we can't create context, proceed without it
          log.warn("Failed to create context for error handler", {
            workflowId,
            contextError:
              contextError instanceof Error ? contextError.message : String(contextError),
          });
        }
      }

      try {
        // Handler returns Error | undefined
        // - undefined = handled, no error to propagate
        // - Error = propagate that error (possibly transformed)
        const handlerResult = await wf.errorHandler(ctx, bag, error);

        // Clean up context if we created one
        if (contextProvider && ctx !== undefined) {
          try {
            await contextProvider.afterStep(ctx, undefined);
          } catch (cleanupError) {
            log.error("afterStep cleanup failed for error handler", {
              workflowId,
              cleanupError:
                cleanupError instanceof Error ? cleanupError.message : String(cleanupError),
            });
          }
        }

        // Return based on handler result
        return {
          bag: resultBag,
          error: handlerResult, // undefined = handled, Error = propagate
        };
      } catch (handlerError) {
        // Handler threw unexpectedly - wrap both errors
        // Clean up context if we created one
        if (contextProvider && ctx !== undefined) {
          try {
            await contextProvider.afterStep(
              ctx,
              handlerError instanceof Error ? handlerError : undefined,
            );
          } catch (cleanupError) {
            log.error("afterStep cleanup failed for error handler", {
              workflowId,
              cleanupError:
                cleanupError instanceof Error ? cleanupError.message : String(cleanupError),
            });
          }
        }

        // Handler failed due to its own error (e.g., DB failure during cleanup)
        return {
          bag: resultBag,
          error: new WorkflowErrorHandlerFailure({
            originalError: error,
            handlerError:
              handlerError instanceof Error ? handlerError : new Error(String(handlerError)),
            workflowId,
          }),
        };
      }
    }

    // No handler - return original error
    return {
      bag: resultBag,
      error: error instanceof Error ? error : new Error(String(error)),
    };
  }
}

/**
 * Executes a FanOut step using the lane-based concurrency pattern.
 *
 * 1. Calls mapInput to produce child workflow inputs from the current bag
 * 2. Spawns min(concurrency, N) async lanes that pull work from a shared index
 * 3. Each lane runs child workflows via runSyncWorkflow, collecting results
 * 4. After all lanes complete, separates successes from failures
 * 5. If all children succeed, calls aggregateResults to produce the step output
 * 6. If any children fail, throws AggregateError containing child errors
 *    (compatible with extractErrorCandidates via .errors[])
 */
async function executeFanOut(
  metadata: FanOutMetadata,
  fanOutName: string,
  currentBag: Record<string, unknown>,
  contextProvider?: StepContextProvider<unknown>,
  logger?: ComposerLogger,
  enableDeepFreeze?: boolean,
): Promise<Record<string, unknown>> {
  const { childWorkflow, mapInput, aggregateResults, concurrency } = metadata;
  const inputs = mapInput(currentBag);

  if (inputs.length === 0) {
    return aggregateResults([]);
  }

  const laneCount = Math.min(concurrency, inputs.length);
  const results: ({ bag: Record<string, unknown> } | { error: Error })[] = new Array(inputs.length);
  let nextIndex = 0;

  const lane = async () => {
    while (true) {
      const index = nextIndex++;
      if (index >= inputs.length) break;

      // Cast: type safety is enforced at the fanOut() factory level, not here
      const childResult = await runSyncWorkflow(
        childWorkflow as Workflow<Record<string, unknown>, never, Partial<Record<string, unknown>>>,
        inputs[index] as Partial<Record<string, unknown>>,
        contextProvider,
        logger,
        enableDeepFreeze,
      );

      if (childResult.error) {
        results[index] = { error: childResult.error };
      } else {
        results[index] = { bag: childResult.bag as Record<string, unknown> };
      }
    }
  };

  await Promise.all(Array.from({ length: laneCount }, () => lane()));

  const errors: Error[] = [];
  const successfulBags: Record<string, unknown>[] = [];

  for (const result of results) {
    if (!result) continue;
    if ("error" in result) {
      errors.push(result.error);
    } else {
      successfulBags.push(result.bag);
    }
  }

  if (errors.length > 0) {
    throw new AggregateError(
      errors,
      `FanOut "${fanOutName}": ${errors.length} of ${inputs.length} child workflow(s) failed`,
    );
  }

  return aggregateResults(successfulBags);
}
