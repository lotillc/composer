/**
 * Workflow Planning Logic for DAG Execution
 *
 * This module extracts the topological sort and batch planning logic from the
 * synchronous workflow engine, making it reusable for both sync and async (Temporal)
 * execution strategies.
 *
 * ## Execution Model
 *
 * The planner performs dependency analysis to determine which steps can run in parallel:
 *
 * 1. **Dependency Analysis**: Build a directed acyclic graph (DAG) of step dependencies
 * 2. **Topological Sort**: Use Kahn's algorithm to find valid execution order
 * 3. **Batch Formation**: Group steps with no dependencies into parallel "batches"
 * 4. **Validation**: Detect cycles, missing dependencies, and conflicts
 *
 * ## Example
 *
 * Given workflow:
 * - stepA: needs [] → provides ["x"]
 * - stepB: needs ["x"] → provides ["y"]
 * - stepC: needs ["x"] → provides ["z"]
 * - stepD: needs ["y", "z"] → provides ["result"]
 *
 * Planning output:
 * ```
 * [
 *   [stepA],           // Batch 0: No dependencies
 *   [stepB, stepC],    // Batch 1: Both need "x" (parallel)
 *   [stepD]            // Batch 2: Needs both "y" and "z"
 * ]
 * ```
 *
 * ## Usage
 *
 * For Temporal workflows:
 * ```ts
 * const plan = planWorkflowBatches(workflow.steps, initialFields);
 * // Convert each batch to Temporal activities that can run in parallel
 * ```
 *
 * For synchronous workflows:
 * ```ts
 * const plan = planWorkflowBatches(workflow.steps, initialFields);
 * for (const batch of plan.batches) {
 *   await Promise.all(batch.map(step => step.run(ctx, bag)));
 * }
 * ```
 */

import type { Step } from "./dag-sync-step";

/**
 * Result of workflow planning - steps organized into parallel execution batches.
 */
export interface WorkflowPlan<Bag extends Record<string, any>> {
  /** Steps organized by execution batch (each batch can execute in parallel) */
  batches: Step<Bag, any, any, any, any>[][];

  /** Map of field name to the step that produces it */
  producers: Map<keyof Bag, string>;

  /** All fields that will be available after workflow completion */
  providedFields: Set<keyof Bag>;
}

/**
 * Plans the execution of a workflow by performing topological sort with batch grouping.
 *
 * This function:
 * 1. Validates that all dependencies can be satisfied
 * 2. Detects duplicate providers and cycles
 * 3. Groups steps into parallel execution batches using Kahn's algorithm
 * 4. Returns an execution plan optimized for maximum parallelism
 *
 * @param steps - Array of steps in the workflow
 * @param initialFields - Fields provided as initial data (satisfy dependencies without producers)
 * @returns Workflow execution plan with steps organized into parallel batches
 * @throws Error if dependencies cannot be satisfied, cycles exist, or duplicate providers found
 *
 * @example
 * ```ts
 * const plan = planWorkflowBatches(
 *   [stepA, stepB, stepC, stepD],
 *   new Set(["userId"])
 * );
 *
 * // Execute each batch in sequence, steps within a batch in parallel
 * for (const batch of plan.batches) {
 *   await Promise.all(batch.map(step => executeStep(step)));
 * }
 * ```
 */
export function planWorkflowBatches<Bag extends Record<string, any>>(
  steps: Step<Bag, any, any, any, any>[],
  initialFields: Set<keyof Bag> = new Set(),
): WorkflowPlan<Bag> {
  // Build a map of which step produces each field (detect duplicates)
  const producers = new Map<keyof Bag, string>();
  for (const step of steps) {
    for (const fieldKey of step.provides) {
      if (producers.has(fieldKey)) {
        throw new Error(
          `Duplicate producer for field "${String(fieldKey)}": ` +
            `${producers.get(fieldKey)} and ${step.name}. ` +
            `Each field can only be produced by one step.`,
        );
      }
      producers.set(fieldKey, step.name);
    }
  }

  // Validate that steps don't try to overwrite initial fields
  for (const step of steps) {
    for (const fieldKey of step.provides) {
      if (initialFields.has(fieldKey)) {
        throw new Error(
          `Step "${step.name}" cannot overwrite initial field "${String(fieldKey)}". ` +
            `Initial data is protected from modification.`,
        );
      }
    }
  }

  // Validate that all dependencies can be satisfied
  const allProvidedFields = new Set<keyof Bag>(initialFields);
  for (const step of steps) {
    for (const fieldKey of step.provides) {
      allProvidedFields.add(fieldKey);
    }
  }

  for (const step of steps) {
    for (const neededField of step.needs) {
      if (!allProvidedFields.has(neededField)) {
        throw new Error(
          `Step "${step.name}" requires field "${String(neededField)}" but no previous step provides it. ` +
            `Available fields: [${Array.from(allProvidedFields).map(String).join(", ")}]. ` +
            `Make sure a previous step provides "${String(neededField)}" or include it in initial data.`,
        );
      }
    }
  }

  // Build dependency graph using adjacency lists
  const stepsByName = new Map(steps.map((step) => [step.name, step]));
  // children[stepName] = list of steps that depend on this step's outputs
  const children = new Map<string, string[]>();
  // indegree[stepName] = number of unmet dependencies
  const indegree = new Map<string, number>(steps.map((step) => [step.name, 0]));

  // Build the graph by connecting producers to consumers
  for (const step of steps) {
    let unmetDependencies = 0;
    for (const neededField of step.needs) {
      // Skip fields provided in initial data
      if (initialFields.has(neededField)) continue;

      const providerStep = producers.get(neededField);
      if (!providerStep) {
        throw new Error(
          `No producer for required field "${String(neededField)}" used by step "${step.name}"`,
        );
      }

      // Add this step as a dependent of the provider
      children.set(providerStep, [...(children.get(providerStep) ?? []), step.name]);
      unmetDependencies++;
    }
    indegree.set(step.name, unmetDependencies);
  }

  // Perform topological sort with batch grouping (Kahn's algorithm)
  // Each iteration processes one "batch" of steps that can run in parallel
  const batches: Step<Bag, any, any, any, any>[][] = [];
  let readySteps = steps
    .filter((step) => (indegree.get(step.name) ?? 0) === 0)
    .map((step) => step.name);

  while (readySteps.length > 0) {
    // Validate that no two steps in the same batch produce the same field
    // This prevents race conditions in parallel execution
    const fieldsProducedInBatch = new Set<string>();
    for (const stepName of readySteps) {
      const step = stepsByName.get(stepName)!;
      for (const fieldKey of step.provides) {
        const fieldName = String(fieldKey);
        if (fieldsProducedInBatch.has(fieldName)) {
          throw new Error(
            `Conflict: Multiple steps in the same execution batch produce field "${fieldName}". ` +
              `This would cause non-deterministic behavior. ` +
              `Steps in batch: [${readySteps.join(", ")}]`,
          );
        }
        fieldsProducedInBatch.add(fieldName);
      }
    }

    // Add this batch to the plan
    const batchSteps = readySteps.map((stepName) => stepsByName.get(stepName)!);
    batches.push(batchSteps);

    // Update dependency graph: reduce indegree for dependent steps
    const nextReadySteps: string[] = [];
    for (const completedStepName of readySteps) {
      for (const dependentStepName of children.get(completedStepName) ?? []) {
        const newIndegree = (indegree.get(dependentStepName) ?? 0) - 1;
        indegree.set(dependentStepName, newIndegree);

        // If this step now has no unmet dependencies, it's ready for the next batch
        if (newIndegree === 0) {
          nextReadySteps.push(dependentStepName);
        }
      }
    }

    readySteps = nextReadySteps;
  }

  // Validate that all steps were processed (detect cycles)
  const unfinishedSteps = [...indegree.entries()].filter(([, degree]) => (degree ?? 0) > 0);
  if (unfinishedSteps.length > 0) {
    const unfinishedNames = unfinishedSteps.map(([stepName]) => stepName);
    throw new Error(
      `Unable to resolve dependencies for steps: [${unfinishedNames.join(", ")}]. ` +
        `This indicates a circular dependency in the workflow.`,
    );
  }

  return {
    batches,
    producers,
    providedFields: allProvidedFields,
  };
}

/**
 * Validates a workflow plan for common issues.
 *
 * This is a convenience function that runs the planner and returns
 * validation errors without throwing.
 *
 * @param steps - Array of steps in the workflow
 * @param initialFields - Fields provided as initial data
 * @returns Validation result with success flag and optional error message
 */
export function validateWorkflowPlan<Bag extends Record<string, any>>(
  steps: Step<Bag, any, any, any, any>[],
  initialFields: Set<keyof Bag> = new Set(),
): { success: true; plan: WorkflowPlan<Bag> } | { success: false; error: string } {
  try {
    const plan = planWorkflowBatches(steps, initialFields);
    return { success: true, plan };
  } catch (error) {
    return {
      success: false,
      error: error instanceof Error ? error.message : String(error),
    };
  }
}
