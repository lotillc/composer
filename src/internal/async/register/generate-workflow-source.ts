/**
 * Runtime Workflow Plan Generation
 *
 * Generates workflow plans from provided workflow definitions and writes
 * a temporary CJS file that Temporal's Webpack can bundle for the deterministic
 * sandbox.
 *
 * The generated file imports `createWorkflowFunction` from the adjacent
 * workflow-factory module using an absolute path resolved at runtime.
 *
 * @module generate-workflow-source
 */

import { existsSync, realpathSync } from "node:fs";
import { writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import type {
  DurationString,
  Step,
  StepPollPolicy,
  StepRetryPolicy,
} from "../../dag-sync-step.js";
import { DEFAULT_CHECKPOINT_TIMEOUT_MS, type Workflow } from "../../dag-sync-workflow.js";
import { planWorkflowBatches } from "../../workflow-planning.js";
import {
  assertSignalWaitsAgree,
  assertSignalWaitsAlone,
  initialFieldsOf,
  resolveStageBoundaries,
} from "../../workflow-stages.js";
import type {
  SignalWaitBatchEntry,
  StepActivityConfig,
  WorkflowPlan,
} from "../build/workflow-factory.js";
import { denamespaceSyntheticSteps } from "../build-scripts/utils/common.js";
import { isFanOut, isSignalWait } from "../build-scripts/utils/type-guards.js";
import {
  DEFAULT_WORKER_PROFILE,
  getTaskQueueForProfile,
  isWorkerProfileName,
  type WorkerProfile,
} from "../config/worker-profiles.js";

function buildActivityConfig(step: Step<any, any, any>): StepActivityConfig | undefined {
  const typed = step as Step<any, any, any> & {
    asyncStartToCloseTimeout?: DurationString;
    asyncHeartbeatTimeout?: DurationString;
    asyncRetry?: StepRetryPolicy;
    asyncPoll?: StepPollPolicy;
  };
  if (
    !typed.asyncStartToCloseTimeout &&
    !typed.asyncHeartbeatTimeout &&
    !typed.asyncRetry &&
    !typed.asyncPoll
  ) {
    return undefined;
  }
  return {
    ...(typed.asyncStartToCloseTimeout && {
      startToCloseTimeout: typed.asyncStartToCloseTimeout,
    }),
    ...(typed.asyncHeartbeatTimeout && { heartbeatTimeout: typed.asyncHeartbeatTimeout }),
    ...(typed.asyncRetry && { retry: typed.asyncRetry }),
    ...(typed.asyncPoll && { poll: typed.asyncPoll }),
  };
}

/**
 * Generates a serializable WorkflowPlan from a workflow definition.
 *
 * This is the runtime equivalent of the build-time plan generation in
 * bundle-generators.ts, but without git-hash versioning (activity names
 * and workflow names are always unversioned).
 */
export function generateWorkflowPlan(workflow: Workflow<any, any, any, any, any>): WorkflowPlan {
  const initialFields = initialFieldsOf(workflow);
  assertSignalWaitsAlone(workflow);
  assertSignalWaitsAgree(workflow);

  // A node's name in the plan is its identity: what `completedSteps` records and what a
  // checkpoint or stage boundary refers to. Composition prefixes it so nodes from different
  // children stay distinct, so steps and waits keep that namespaced name. A step's activity
  // is registered under its bare name, kept beside it as `activityName`. Fan-outs keep their
  // bare name: their child workflow IDs and helper activities are built from it, and
  // in-flight runs depend on those staying the same.
  const bareSteps = denamespaceSyntheticSteps(workflow.steps);
  const flattenedSteps = bareSteps.map((bare, index) =>
    isFanOut(bare)
      ? bare
      : { ...bare, name: workflow.steps[index]!.name, activityName: bare.name },
  );

  // Two nodes with one identity would collapse into one in the plan, and the other would
  // silently never run. Two different steps sharing an activity name would run whichever
  // registered last. Both are refused here; the same step reused in two children is fine.
  const seenNames = new Set<string>();
  const runByActivityName = new Map<string, unknown>();
  for (const step of flattenedSteps) {
    if (seenNames.has(step.name)) {
      throw new Error(
        `Workflow "${workflow.name}": two nodes are both named "${step.name}". Fan-outs are ` +
          `identified by their name without composition prefixes, so rename one.`,
      );
    }
    seenNames.add(step.name);

    const activityName = (step as { activityName?: string }).activityName;
    if (activityName === undefined || isSignalWait(step)) continue;
    const run = runByActivityName.get(activityName);
    if (run !== undefined && run !== step.run) {
      throw new Error(
        `Workflow "${workflow.name}": two different steps are both named "${activityName}". ` +
          `Activities are registered by that name, so rename one.`,
      );
    }
    runByActivityName.set(activityName, step.run);
  }
  const planResult = planWorkflowBatches([...flattenedSteps], initialFields);

  const plan: WorkflowPlan = {
    name: workflow.name,
    batches: planResult.batches.map((batch) => {
      const steps: WorkflowPlan["batches"][number]["steps"] = [];
      const signalWaits: SignalWaitBatchEntry[] = [];
      const fanOuts: Array<{
        name: string;
        childWorkflowName: string;
        mapInputActivityName: string;
        aggregateResultsActivityName: string;
        needs: string[];
        provides: string[];
        concurrency: number | null;
        taskQueue: string;
      }> = [];

      for (const step of batch) {
        // A wait runs no activity, so it has no worker profile to route.
        if (isSignalWait(step)) {
          const wait = step.__signalWait;
          signalWaits.push({
            name: step.name,
            signalName: wait.signalName,
            provides: String(step.provides[0]),
            ...(wait.timeout !== undefined && { timeout: wait.timeout }),
            ...(wait.hasTimeoutValue && { timeoutValue: wait.timeoutValue }),
            hasTimeoutValue: wait.hasTimeoutValue,
            ...(wait.maxPendingSignals !== undefined && {
              maxPendingSignals: wait.maxPendingSignals,
            }),
          });
          continue;
        }

        const rawProfile =
          (step as Step<any, any, any> & { workerProfile?: string }).workerProfile ??
          DEFAULT_WORKER_PROFILE;
        // Any well-formed name routes: the queue derives from the name, and the
        // workflow worker has no build config to check declarations against.
        if (!isWorkerProfileName(rawProfile)) {
          throw new Error(
            `Step "${step.name}" in workflow "${workflow.name}": invalid workerProfile "${rawProfile}"`,
          );
        }
        const workerProfile = rawProfile as WorkerProfile;

        const activityConfig = buildActivityConfig(step);

        if (isFanOut(step)) {
          fanOuts.push({
            name: step.name,
            childWorkflowName: step.__fanOut.childWorkflow.name,
            mapInputActivityName: `${step.name}__mapInput`,
            aggregateResultsActivityName: `${step.name}__aggregateResults`,
            needs: [...step.needs].map(String),
            provides: [...step.provides].map(String),
            concurrency: Number.isFinite(step.__fanOut.concurrency)
              ? step.__fanOut.concurrency
              : null,
            taskQueue: getTaskQueueForProfile(workerProfile),
            ...(activityConfig && { activityConfig }),
          });
        } else {
          steps.push({
            name: step.name,
            activityName: (step as { activityName?: string }).activityName ?? step.name,
            needs: [...step.needs].map(String),
            provides: [...step.provides].map(String),
            taskQueue: getTaskQueueForProfile(workerProfile),
            ...(activityConfig && { activityConfig }),
          });
        }
      }

      return {
        steps,
        ...(fanOuts.length > 0 ? { fanOuts } : {}),
        ...(signalWaits.length > 0 ? { signalWaits } : {}),
      };
    }),
  };

  if (workflow.errorHandler) {
    plan.errorHandlerActivityName = `${workflow.name}__errorHandler`;
    plan.errorHandlerTaskQueue = "standard-tasks";
  }

  if (workflow.checkpoints && workflow.checkpoints.length > 0) {
    const stepToBatchIndex = new Map<string, number>();
    plan.batches.forEach((batch, batchIndex) => {
      for (const step of batch.steps) {
        stepToBatchIndex.set(step.name, batchIndex);
      }
      for (const fanOut of batch.fanOuts ?? []) {
        stepToBatchIndex.set(fanOut.name, batchIndex);
      }
      for (const wait of batch.signalWaits ?? []) {
        stepToBatchIndex.set(wait.name, batchIndex);
      }
    });
    // A checkpoint names its step as the builder resolved it, which for a composed fan-out
    // is the namespaced name; the plan keeps fan-outs bare, so index that name too.
    planResult.batches.forEach((batch, batchIndex) => {
      for (const node of batch) {
        const syntheticName = (node as { _syntheticName?: string })._syntheticName;
        if (syntheticName) stepToBatchIndex.set(syntheticName, batchIndex);
      }
    });

    plan.checkpoints = workflow.checkpoints.map((checkpoint) => {
      const batchIndex = stepToBatchIndex.get(checkpoint.afterStep);
      if (batchIndex === undefined) {
        throw new Error(
          `Checkpoint "${checkpoint.name}" references step "${checkpoint.afterStep}" ` +
            `which was not found in the workflow plan. ` +
            `Available steps: [${Array.from(stepToBatchIndex.keys()).join(", ")}]`,
        );
      }
      return {
        name: checkpoint.name,
        afterBatch: batchIndex,
        timeout: checkpoint.timeout ?? DEFAULT_CHECKPOINT_TIMEOUT_MS,
      };
    });
  }

  const stageBoundaries = resolveStageBoundaries(workflow);
  if (stageBoundaries.length > 0) {
    plan.stageBoundaries = stageBoundaries;
  }

  if (workflow.versioningBehavior) {
    plan.versioningBehavior = workflow.versioningBehavior;
  }

  return plan;
}

/**
 * Resolves the absolute path to the framework's workflow-factory.js file.
 *
 * Resolves relative to this module instead of the package main entry so source
 * checkout tests and compiled package runtime both work.
 */
function resolveWorkflowFactoryPath(): string {
  // realpathSync matters here: the result is interpolated into the generated
  // require() below, and Temporal's bundler compares it against paths webpack
  // has already symlink-resolved. Under pnpm the package directory is itself a
  // symlink, so an unresolved path produces the mismatch documented in
  // writeWorkflowSourceFile.
  for (const ext of [".js", ".ts"]) {
    const candidate = fileURLToPath(new URL(`../build/workflow-factory${ext}`, import.meta.url));
    if (existsSync(candidate)) {
      return realpathSync(candidate);
    }
  }
  throw new Error(`Cannot resolve workflow-factory relative to ${import.meta.url}`);
}

/**
 * Writes a temporary CJS workflow source file that Temporal's Webpack can bundle.
 *
 * The generated file:
 * 1. Requires `createWorkflowFunction` from the framework's workflow-factory.js
 * 2. Defines serialized WorkflowPlan objects for each workflow
 * 3. Exports workflow functions created by calling createWorkflowFunction(plan)
 *
 * Written to os.tmpdir() since it's a runtime artifact, not a build output.
 *
 * @param workflows - Workflow definitions to generate plans for
 * @returns Absolute path to the generated file
 */
/**
 * Recursively collects all workflows reachable from the provided roots,
 * including child workflows referenced by fanOut steps.
 */
export function collectAllWorkflows(
  roots: Workflow<any, any, any, any, any>[],
): Workflow<any, any, any, any, any>[] {
  const seen = new Map<string, Workflow<any, any, any, any, any>>();
  const queue: Workflow<any, any, any, any, any>[] = [...roots];

  while (queue.length > 0) {
    const wf = queue.pop()!;
    if (seen.has(wf.name)) {
      if (seen.get(wf.name) !== wf) {
        throw new Error(
          `Workflow name collision: "${wf.name}" is defined by multiple distinct workflow objects`,
        );
      }
      continue;
    }
    seen.set(wf.name, wf);

    for (const step of wf.steps) {
      if (isFanOut(step)) {
        queue.push(step.__fanOut.childWorkflow);
      }
    }
  }

  return Array.from(seen.values());
}

export async function writeWorkflowSourceFile(
  workflows: Workflow<any, any, any, any, any>[],
  options: {
    /**
     * Whether the worker that bundles this source runs with Worker Versioning. Temporal's
     * core SDK rejects every task completion that carries a versioning behavior when the
     * worker is unversioned, so declarations are dropped unless this is set. A stage
     * boundary's next-run behavior means nothing without versioning, so it goes too.
     */
    workerVersioning?: boolean;
    /**
     * File name within the temp directory. Defaults to the one the workflow worker uses;
     * a caller bundling alongside a live worker in the same process passes its own.
     */
    fileName?: string;
    /** Carried-state budget written into staged plans; unset leaves the default. */
    continuationBudgetBytes?: number;
  } = {},
): Promise<string> {
  const budget = options.continuationBudgetBytes;
  if (budget !== undefined && (!Number.isInteger(budget) || budget <= 0)) {
    throw new Error(
      `continuationBudgetBytes must be a positive integer, got ${String(budget)}`,
    );
  }
  const factoryPath = resolveWorkflowFactoryPath().replaceAll("\\", "/");
  // realpathSync resolves OS-level symlinks (e.g. macOS /tmp -> /private/tmp).
  // Temporal's bundler writes an entrypoint to memfs using this path, but webpack
  // resolves symlinks when reading, causing a mismatch if the path contains symlinks.
  const outputPath = resolve(realpathSync(tmpdir()), options.fileName ?? "__workflow-source.js");

  const allWorkflows = collectAllWorkflows(workflows);

  let code = `"use strict";\n`;
  code += `// AUTO-GENERATED at runtime - DO NOT EDIT\n`;
  code += `const { createWorkflowFunction } = require("${factoryPath}");\n\n`;

  // Workflow names are never interpolated raw: plans get indexed identifiers and
  // export keys are JSON string literals, defined as own properties so a name
  // like "__proto__" cannot reach the prototype setter.
  for (const [index, workflow] of allWorkflows.entries()) {
    const plan = generateWorkflowPlan(workflow);
    if (!options.workerVersioning) {
      delete plan.versioningBehavior;
      for (const boundary of plan.stageBoundaries ?? []) delete boundary.initialVersioningBehavior;
    }
    if (budget !== undefined && plan.stageBoundaries) plan.continuationBudgetBytes = budget;
    const planId = `plan${index}`;

    code += `const ${planId} = ${JSON.stringify(plan)};\n`;
    code += `Object.defineProperty(exports, ${JSON.stringify(workflow.name)}, { value: createWorkflowFunction(${planId}), enumerable: true });\n\n`;
  }

  await writeFile(outputPath, code, "utf-8");
  return outputPath;
}
