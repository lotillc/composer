/**
 * Type Guards for Step and Workflow Definitions
 *
 * Provides runtime type guards for validating step and workflow definitions
 * during build-time file scanning and dynamic module loading.
 *
 * ## Purpose
 *
 * These type guards are used by the build system to:
 * 1. Distinguish valid step/workflow exports from other exported values
 * 2. Filter out helper functions, constants, and types during collection
 * 3. Validate that dynamically imported modules contain properly structured definitions
 *
 * ## Usage
 *
 * ```typescript
 * import { isStep, isWorkflow } from './type-guards';
 *
 * // During file scanning
 * const module = await import('./some-file.ts');
 * for (const value of Object.values(module)) {
 *   if (isStep(value)) {
 *     console.log('Found step:', value.name);
 *   } else if (isWorkflow(value)) {
 *     console.log('Found workflow:', value.name);
 *   }
 * }
 * ```
 *
 * ## Type Guard Criteria
 *
 * ### Step Definition
 * A valid step must have:
 * - `name` (string): Unique identifier
 * - `needs` (array): Input dependencies
 * - `provides` (array): Output fields
 * - `run` (function): Execution logic
 *
 * ### Workflow Definition
 * A valid workflow must have:
 * - `name` (string): Unique identifier
 * - `steps` (array): Step sequence
 * - `requiredInitial` (array or undefined): Optional initial data requirements
 *
 * ## Implementation Notes
 *
 * These type guards intentionally use `any` types because they operate on
 * dynamically loaded module exports where the actual types are not known
 * until runtime. The guards perform structural validation to ensure the
 * objects match the expected Step/Workflow interface shape.
 */

import type { FanOutMetadata } from "../../../dag-sync-fanout";
import type { Step } from "../../../dag-sync-step";
import type { Workflow } from "../../../dag-sync-workflow";
import type { ScheduleDefinition } from "../../schedule/define-schedule";

/**
 * Type guard to check if a value is a step definition.
 *
 * Validates that the value has all required step properties:
 * - name (string)
 * - needs (array)
 * - provides (array)
 * - run (function)
 *
 * @param value - Value to check
 * @returns True if the value is a valid Step definition
 *
 * @example
 * ```typescript
 * const exported = { name: 'myStep', needs: [], provides: ['result'], run: async () => {} };
 * if (isStep(exported)) {
 *   console.log('Valid step:', exported.name);
 * }
 * ```
 */
export function isStep(value: any): value is Step<any, any, any> {
  return (
    value &&
    typeof value === "object" &&
    typeof value.name === "string" &&
    Array.isArray(value.needs) &&
    Array.isArray(value.provides) &&
    typeof value.run === "function"
  );
}

/**
 * Type guard to check if a value is a workflow definition.
 *
 * Validates that the value has all required workflow properties:
 * - name (string)
 * - steps (array)
 * - requiredInitial (array OR undefined - this field is optional)
 *
 * Note: `requiredInitial` is optional in the Workflow type definition.
 * Workflows that consist only of independent steps or steps that depend
 * on each other's outputs don't require initial data.
 *
 * @param value - Value to check
 * @returns True if the value is a valid Workflow definition
 *
 * @example
 * ```typescript
 * // Workflow with required initial data
 * const wf1 = { name: 'myWorkflow', steps: [], requiredInitial: ['input'] };
 * console.log(isWorkflow(wf1)); // true
 *
 * // Workflow without required initial data
 * const wf2 = { name: 'independentWorkflow', steps: [] };
 * console.log(isWorkflow(wf2)); // true (requiredInitial is optional)
 * ```
 */
/**
 * Type guard to check if a value is a FanOut step definition.
 *
 * A FanOut is a Step that also has a `__fanOut` metadata object containing
 * childWorkflow, mapInput, aggregateResults, and concurrency.
 */
export function isFanOut(value: any): value is Step<any, any, any> & { __fanOut: FanOutMetadata } {
  return isStep(value) && "__fanOut" in value && value.__fanOut != null;
}

export function isWorkflow(value: any): value is Workflow<any, any, any> {
  return (
    value &&
    typeof value === "object" &&
    typeof value.name === "string" &&
    Array.isArray(value.steps) &&
    (Array.isArray(value.requiredInitial) || value.requiredInitial === undefined)
  );
}

/**
 * Type guard to check if a value is a schedule definition.
 *
 * Validates that the value has the `__scheduleDefinition` marker
 * and required schedule properties.
 *
 * @param value - Value to check
 * @returns True if the value is a valid ScheduleDefinition
 */
export function isScheduleDefinition(value: unknown): value is ScheduleDefinition {
  return (
    value != null &&
    typeof value === "object" &&
    (value as Record<string, unknown>).__scheduleDefinition === true &&
    typeof (value as Record<string, unknown>).scheduleId === "string" &&
    typeof (value as Record<string, unknown>).workflowName === "string" &&
    (value as Record<string, unknown>).spec != null
  );
}
