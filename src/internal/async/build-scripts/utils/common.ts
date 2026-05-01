/**
 * Common Utilities for Build Scripts
 *
 * Shared utility functions used across the async build system.
 */

import type { Step } from "../../../dag-sync-step";

/**
 * Restores real step names from synthetic namespaced names.
 *
 * The sync runtime's workflow() function flattens composed workflows and creates
 * synthetic steps with:
 * - Namespaced names like "childWorkflow.stepName" or "parent.child.stepName"
 * - A 'workflowPath' property indicating they came from a composed workflow
 *
 * For async/Temporal, we need to map these back to their real step names
 * so we can look them up in the step manifest.
 *
 * @example
 * ```typescript
 * // Input (from sync runtime, already flattened):
 * [stepA, { name: "child.step1", workflowPath: ["child"], ... }]
 *
 * // Output (real step names restored):
 * [stepA, { name: "step1", _syntheticName: "child.step1", ... }]
 * ```
 */
export function denamespaceSyntheticSteps(
  steps: readonly Step<any, any, any>[],
): Step<any, any, any>[] {
  return steps.map((step: any) => {
    // If this step has a workflowPath, it's a synthetic step from a composed workflow
    if (step.workflowPath && step.workflowPath.length > 0) {
      // Extract the real step name from the namespaced name
      // e.g., "subjectIdentification.subjectIdentificationMerge" -> "subjectIdentificationMerge"
      // e.g., "parent.child.stepName" -> "stepName"
      const parts = step.name.split(".");
      const realName = parts[parts.length - 1];

      // Return a new step object with the real name, preserving needs/provides
      return {
        ...step,
        name: realName,
        // Keep the synthetic name for debugging
        _syntheticName: step.name,
        _workflowPath: step.workflowPath,
      };
    }

    return step;
  });
}
