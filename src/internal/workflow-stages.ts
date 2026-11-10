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
  /** For a loop boundary: the earlier stage the next run resumes after. */
  resumeAt?: string;
  /** For a loop boundary: the bag field that must be `true` for the loop to be taken. */
  when?: string;
  /** For a loop boundary: next-iteration fields, each from a field of this iteration. */
  carry?: Record<string, string>;
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
    // A loop boundary may end the workflow's last batch: not taken, the run just finishes.
    if (afterBatch === batches.length - 1 && boundary.resumeAt === undefined) {
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
      ...(boundary.resumeAt !== undefined && { resumeAt: boundary.resumeAt }),
      ...(boundary.when !== undefined && { when: boundary.when }),
      ...(boundary.carry && Object.keys(boundary.carry).length > 0 && { carry: { ...boundary.carry } }),
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

  // A loop resumes after an earlier stage and runs the batches between, its body, again.
  const byName = new Map(resolved.map((boundary) => [boundary.name, boundary]));
  const loops: { name: string; from: number; to: number }[] = [];
  for (const boundary of resolved) {
    if (boundary.resumeAt === undefined) {
      if (boundary.when !== undefined || boundary.carry !== undefined) {
        throw new Error(
          `${prefix}: stage "${boundary.name}" sets when or carry without resumeAt, which ` +
            `only a loop boundary takes`,
        );
      }
      continue;
    }
    const target = byName.get(boundary.resumeAt);
    if (!target) {
      throw new Error(
        `${prefix}: stage "${boundary.name}" resumes at unknown stage "${boundary.resumeAt}". ` +
          `Known stages: [${[...byName.keys()].join(", ")}]`,
      );
    }
    if (target.afterBatch >= boundary.afterBatch) {
      throw new Error(
        `${prefix}: stage "${boundary.name}" resumes at stage "${boundary.resumeAt}", which ` +
          `does not come before it. A loop resumes at an earlier stage.`,
      );
    }
    const when = boundary.when;
    if (when === undefined) {
      throw new Error(
        `${prefix}: stage "${boundary.name}" resumes at stage "${boundary.resumeAt}" but names ` +
          `no when field to decide the loop on`,
      );
    }
    const before = batches.slice(0, target.afterBatch + 1).flat();
    const body = batches.slice(target.afterBatch + 1, boundary.afterBatch + 1).flat();
    const bodyProvides = (field: string) =>
      body.some((step) => step.provides.some((provided: unknown) => String(provided) === field));
    // Read before any carry applies, so only a field the body provides can change per pass.
    if (!bodyProvides(when)) {
      throw new Error(
        `${prefix}: stage "${boundary.name}" decides on field "${when}", which no step after ` +
          `stage "${target.name}" provides, so it could not change from one pass to the next`,
      );
    }
    const available = new Set<string>(initialFields);
    for (const step of [...before, ...body]) {
      for (const field of step.provides) available.add(String(field));
    }
    for (const [field, source] of Object.entries(boundary.carry ?? {})) {
      // The plan reaches the workflow bundle as an object literal, where this key sets the
      // prototype instead of naming a field: the carry would vanish.
      if (field === "__proto__") {
        throw new Error(
          `${prefix}: stage "${boundary.name}" cannot carry a field named "__proto__"`,
        );
      }
      if (!available.has(source)) {
        throw new Error(
          `${prefix}: stage "${boundary.name}" carries field "${field}" from "${source}", which ` +
            `nothing before it provides. Available fields: [${[...available].join(", ")}]`,
        );
      }
      if (bodyProvides(field)) {
        throw new Error(
          `${prefix}: stage "${boundary.name}" carries field "${field}", which a step after ` +
            `stage "${target.name}" provides again; carry only fields from before that stage`,
        );
      }
      // A step before the stage runs once, so the next pass would never read the carried value.
      const reader = before.find((step) =>
        step.needs.some((need: unknown) => String(need) === field),
      );
      if (reader) {
        throw new Error(
          `${prefix}: stage "${boundary.name}" carries field "${field}", which step ` +
            `"${reader.name}" reads before stage "${target.name}" and so never again. Give that ` +
            `step a need on a field provided after the stage, which moves it into the loop.`,
        );
      }
    }
    loops.push({ name: boundary.name, from: target.afterBatch, to: boundary.afterBatch });
  }
  // Overlapping loops would each rerun part of the other's body without the steps that decide it.
  for (const a of loops) {
    for (const b of loops) {
      if (a.from < b.from && b.from < a.to && a.to < b.to) {
        throw new Error(
          `${prefix}: loops "${a.name}" and "${b.name}" overlap; a loop must lie wholly ` +
            `inside another or apart from it`,
        );
      }
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
 * Checks that every wait on one signal uses the same definition and the same
 * `maxPendingSignals`.
 *
 * Signals queue per name for the whole workflow, before any particular wait is reached, so
 * all waits on a name share one queue. Two definitions of one name could declare different
 * payload types, and a payload sent for one wait could be consumed by another and written
 * into a field typed differently. Waits declaring different caps would each expect their
 * own, and a later wait could find a queue its cap says should not exist.
 *
 * @throws Error naming the signal and the waits that disagree
 */
export function assertSignalWaitsAgree(workflow: Workflow<any, any, any, any, any>): void {
  const first = new Map<string, { cap: number; wait: string; signal: unknown }>();
  for (const step of workflow.steps as Step<any, any, any, any, any>[]) {
    if (!isSignalWaitStep(step)) continue;
    const { signalName, maxPendingSignals, signal } = step.__signalWait;
    const cap = maxPendingSignals ?? DEFAULT_MAX_PENDING_SIGNALS;
    const earlier = first.get(signalName);
    if (!earlier) {
      first.set(signalName, { cap, wait: step.name, signal });
      continue;
    }
    if (earlier.signal !== signal) {
      throw new Error(
        `Workflow "${workflow.name}": waits "${earlier.wait}" and "${step.name}" both wait on ` +
          `signal "${signalName}" but use different definitions of it. Waits on one signal ` +
          `share one queue, so define the signal once and use that definition for both.`,
      );
    }
    if (earlier.cap !== cap) {
      throw new Error(
        `Workflow "${workflow.name}": waits "${earlier.wait}" and "${step.name}" both wait on ` +
          `signal "${signalName}" but declare maxPendingSignals ${earlier.cap} and ${cap}. ` +
          `A signal has one queue, so give them the same cap.`,
      );
    }
  }
}
