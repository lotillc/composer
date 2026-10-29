/**
 * Stage boundary resolution for long-lived async workflows.
 *
 * A stage boundary sits after a batch. Resolving it maps the boundary's step to that batch
 * and checks the rules continue-as-new depends on: something must run after it, at most one
 * boundary per batch, a name or alias for exactly one boundary, and checkpoints only in the
 * first stage.
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
import { DEFAULT_MAX_PENDING_SIGNALS, isSignalWaitStep } from "./dag-sync-signal.js";
import type { Step } from "./dag-sync-step.js";
import { planWorkflowBatches } from "./workflow-planning.js";

/** A stage boundary placed on the batch it follows. */
export interface ResolvedStageBoundary {
  name: string;
  /** 0-indexed batch after which the run continues as new. */
  afterBatch: number;
  initialVersioningBehavior: StageInitialVersioningBehavior;
  /** Earlier names a resuming run may carry instead of `name`. */
  aliases?: string[];
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
      initialVersioningBehavior: boundary.initialVersioningBehavior ?? "AUTO_UPGRADE",
      ...(boundary.aliases && boundary.aliases.length > 0 && { aliases: [...boundary.aliases] }),
    };
  });

  // A resuming run names the stage it ended at; every name and alias must pick out exactly
  // one boundary, or a run could resume at the wrong place.
  const stageOfName = new Map<string, string>();
  for (const boundary of resolved) {
    for (const name of [boundary.name, ...(boundary.aliases ?? [])]) {
      if (typeof name !== "string" || name.length === 0) {
        throw new Error(`${prefix}: stage "${boundary.name}" has an empty alias`);
      }
      const owner = stageOfName.get(name);
      if (owner !== undefined) {
        throw new Error(
          owner === boundary.name
            ? `${prefix}: stage "${boundary.name}" lists "${name}" more than once`
            : `${prefix}: "${name}" names both stage "${owner}" and stage "${boundary.name}"`,
        );
      }
      stageOfName.set(name, boundary.name);
    }
  }

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

  return resolved;
}

/**
 * Checks that every signal wait is alone in its batch.
 *
 * A wait can block for weeks, so a batch it shares would wait on it: a sibling's failure
 * could not be handled until the signal arrived, and a checkpoint after the batch would not
 * fire. Alone, a wait's batch ends exactly when the wait does, and its only failure is its
 * own timeout. Plans nothing for a workflow without waits.
 *
 * @throws Error naming the wait and what shares its batch
 */
export function assertSignalWaitsAlone(workflow: Workflow<any, any, any, any, any>): void {
  const steps: Step<any, any, any, any, any>[] = [...workflow.steps];
  if (!steps.some((step) => isSignalWaitStep(step))) return;

  const { batches } = planWorkflowBatches(steps, initialFieldsOf(workflow));
  for (const batch of batches) {
    const wait = batch.find((step) => isSignalWaitStep(step));
    if (!wait || batch.length === 1) continue;
    const others = batch.filter((step) => step !== wait).map((step) => step.name);
    throw new Error(
      `Workflow "${workflow.name}": signal wait "${wait.name}" would run alongside ` +
        `[${others.join(", ")}]. A wait must be alone in its batch. Give it a \`needs\` on a ` +
        `field one of them provides, so it runs after them, or have them need its field.`,
    );
  }
}

/**
 * Checks that every wait on one signal declares the same `maxPendingSignals`.
 *
 * Signals queue per name for the whole workflow, before any particular wait is reached, so
 * a queue has one cap. Waits declaring different caps for one name would each expect
 * their own, and a later wait could find a queue its cap says should not exist.
 *
 * @throws Error naming the signal and the waits that disagree
 */
export function assertSignalCapsAgree(workflow: Workflow<any, any, any, any, any>): void {
  const caps = new Map<string, { cap: number; wait: string }>();
  for (const step of workflow.steps as Step<any, any, any, any, any>[]) {
    if (!isSignalWaitStep(step)) continue;
    const { signalName, maxPendingSignals } = step.__signalWait;
    const cap = maxPendingSignals ?? DEFAULT_MAX_PENDING_SIGNALS;
    const earlier = caps.get(signalName);
    if (earlier && earlier.cap !== cap) {
      throw new Error(
        `Workflow "${workflow.name}": waits "${earlier.wait}" and "${step.name}" both wait on ` +
          `signal "${signalName}" but declare maxPendingSignals ${earlier.cap} and ${cap}. ` +
          `A signal has one queue, so give them the same cap.`,
      );
    }
    caps.set(signalName, { cap, wait: step.name });
  }
}
