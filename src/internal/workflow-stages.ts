/**
 * Stage boundary resolution for long-lived async workflows.
 *
 * A stage boundary sits after a batch. Resolving it maps the boundary's step to that batch
 * and checks the rules continue-as-new depends on: something must run after it, at most one
 * boundary per batch, checkpoints only in the first stage, and every carried field a later
 * step needs.
 *
 * Shared by the workflow builder, so a mistake fails where the workflow is defined, and by
 * plan generation, which turns the result into the serialized plan.
 *
 * @module workflow-stages
 */

import type {
  StageBoundary,
  StageInitialVersioningBehavior,
  Workflow,
} from "./dag-sync-workflow.js";
import { planWorkflowBatches } from "./workflow-planning.js";

/** A stage boundary placed on the batch it follows. */
export interface ResolvedStageBoundary {
  name: string;
  /** 0-indexed batch after which the run continues as new. */
  afterBatch: number;
  carry?: string[];
  initialVersioningBehavior: StageInitialVersioningBehavior;
}

/** Fields a workflow starts with: what the caller must pass plus configured values. */
export function initialFieldsOf(workflow: Workflow<any, any, any, any, any>): Set<string> {
  return new Set<string>(
    (workflow.requiredInitial ?? [])
      .concat(Object.keys(workflow.configuredValues || {}))
      .map(String),
  );
}

/**
 * Resolves and validates a workflow's stage boundaries, in batch order.
 *
 * Returns an empty array for a workflow without boundaries and plans nothing in that case,
 * so workflows that never declare one are not held to the stricter planning.
 *
 * @throws Error describing the first violated rule
 */
export function resolveStageBoundaries(
  workflow: Workflow<any, any, any, any, any>,
): ResolvedStageBoundary[] {
  const boundaries: StageBoundary[] = workflow.stageBoundaries ?? [];
  if (boundaries.length === 0) return [];

  const initialFields = initialFieldsOf(workflow);
  const { batches } = planWorkflowBatches([...workflow.steps], initialFields);

  const batchOfStep = new Map<string, number>();
  batches.forEach((batch, index) => {
    for (const step of batch) batchOfStep.set(step.name, index);
  });

  const prefix = `Workflow "${workflow.name}"`;
  const resolved = boundaries.map((boundary): ResolvedStageBoundary => {
    const afterBatch = batchOfStep.get(boundary.afterStep);
    if (afterBatch === undefined) {
      throw new Error(
        `${prefix}: stage "${boundary.name}" references step "${boundary.afterStep}" ` +
          `which is not in the workflow`,
      );
    }
    if (afterBatch === batches.length - 1) {
      throw new Error(
        `${prefix}: stage "${boundary.name}" follows the last batch, so nothing would run ` +
          `after it. Remove the boundary or move it to an earlier step.`,
      );
    }
    return {
      name: boundary.name,
      afterBatch,
      ...(boundary.carry && { carry: [...boundary.carry] }),
      initialVersioningBehavior: boundary.initialVersioningBehavior ?? "AUTO_UPGRADE",
    };
  });

  resolved.sort((a, b) => a.afterBatch - b.afterBatch);

  for (let i = 1; i < resolved.length; i++) {
    if (resolved[i]!.afterBatch === resolved[i - 1]!.afterBatch) {
      throw new Error(
        `${prefix}: stages "${resolved[i - 1]!.name}" and "${resolved[i]!.name}" follow the ` +
          `same batch; a batch can end at most one stage`,
      );
    }
  }

  // A checkpoint is answered by an update to the run that reaches it, and that run's
  // handler does not survive continue-as-new. Only the first stage can have one.
  const firstBoundary = resolved[0]!;
  for (const checkpoint of workflow.checkpoints ?? []) {
    const batch = batchOfStep.get(checkpoint.afterStep);
    if (batch !== undefined && batch > firstBoundary.afterBatch) {
      throw new Error(
        `${prefix}: checkpoint "${checkpoint.name}" comes after stage "${firstBoundary.name}". ` +
          `Checkpoints must be reached before the first continue-as-new.`,
      );
    }
  }

  // Walk the stages in order, tracking which produced fields survive each boundary.
  // Initial data always carries; a `carry` list restricts the produced fields.
  let available = new Set<string>();
  let batchIndex = 0;
  for (const boundary of resolved) {
    for (; batchIndex <= boundary.afterBatch; batchIndex++) {
      for (const step of batches[batchIndex]!) {
        for (const field of step.provides) available.add(String(field));
      }
    }
    if (!boundary.carry) continue;

    const carry = new Set(boundary.carry);
    for (const field of carry) {
      if (!available.has(field) && !initialFields.has(field)) {
        throw new Error(
          `${prefix}: stage "${boundary.name}" carries "${field}", which no step before it provides`,
        );
      }
    }
    for (const later of batches.slice(boundary.afterBatch + 1).flat()) {
      for (const need of later.needs) {
        const field = String(need);
        if (available.has(field) && !carry.has(field)) {
          throw new Error(
            `${prefix}: stage "${boundary.name}" does not carry "${field}", which step ` +
              `"${later.name}" needs. Add it to carry.`,
          );
        }
      }
    }
    available = new Set([...available].filter((field) => carry.has(field)));
  }

  return resolved;
}
