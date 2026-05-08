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

import { realpathSync } from "node:fs";
import { writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { resolve } from "node:path";
import type { DurationString, Step, StepRetryPolicy } from "../../dag-sync-step";
import { DEFAULT_CHECKPOINT_TIMEOUT_MS, type Workflow } from "../../dag-sync-workflow";
import { planWorkflowBatches } from "../../workflow-planning";
import type { StepActivityConfig, WorkflowPlan } from "../build/workflow-factory";
import { denamespaceSyntheticSteps } from "../build-scripts/utils/common";
import { isFanOut } from "../build-scripts/utils/type-guards";
import {
  DEFAULT_WORKER_PROFILE,
  getTaskQueueForProfile,
  isValidWorkerProfile,
  type WorkerProfile,
} from "../config/worker-profiles";

function buildActivityConfig(step: Step<any, any, any>): StepActivityConfig | undefined {
  const typed = step as Step<any, any, any> & {
    asyncStartToCloseTimeout?: DurationString;
    asyncHeartbeatTimeout?: DurationString;
    asyncRetry?: StepRetryPolicy;
  };
  if (!typed.asyncStartToCloseTimeout && !typed.asyncHeartbeatTimeout && !typed.asyncRetry) {
    return undefined;
  }
  return {
    ...(typed.asyncStartToCloseTimeout && {
      startToCloseTimeout: typed.asyncStartToCloseTimeout,
    }),
    ...(typed.asyncHeartbeatTimeout && { heartbeatTimeout: typed.asyncHeartbeatTimeout }),
    ...(typed.asyncRetry && { retry: typed.asyncRetry }),
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
  const initialFields = new Set<string>(
    (workflow.requiredInitial ?? [])
      .concat(Object.keys(workflow.configuredValues || {}))
      .map(String),
  );

  const flattenedSteps = denamespaceSyntheticSteps(workflow.steps);
  const planResult = planWorkflowBatches([...flattenedSteps], initialFields);

  const plan: WorkflowPlan = {
    name: workflow.name,
    batches: planResult.batches.map((batch) => {
      const steps: WorkflowPlan["batches"][number]["steps"] = [];
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
        const rawProfile =
          (step as Step<any, any, any> & { workerProfile?: string }).workerProfile ??
          DEFAULT_WORKER_PROFILE;
        if (!isValidWorkerProfile(rawProfile)) {
          throw new Error(
            `Step "${step.name}" in workflow "${workflow.name}": invalid workerProfile "${rawProfile}"`,
          );
        }
        const workerProfile: WorkerProfile = rawProfile;

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
            activityName: step.name,
            needs: [...step.needs].map(String),
            provides: [...step.provides].map(String),
            taskQueue: getTaskQueueForProfile(workerProfile),
            ...(activityConfig && { activityConfig }),
          });
        }
      }

      return { steps, ...(fanOuts.length > 0 ? { fanOuts } : {}) };
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

  return plan;
}

/**
 * Resolves the absolute path to the framework's workflow-factory.js file.
 *
 * Resolves relative to this module instead of the package main entry so source
 * checkout tests and compiled package runtime both work.
 */
function resolveWorkflowFactoryPath(): string {
  try {
    return require.resolve("../build/workflow-factory");
  } catch (error) {
    try {
      return require.resolve("../build/workflow-factory.ts");
    } catch {
      throw error;
    }
  }
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
): Promise<string> {
  const factoryPath = resolveWorkflowFactoryPath().replaceAll("\\", "/");
  // realpathSync resolves OS-level symlinks (e.g. macOS /tmp -> /private/tmp).
  // Temporal's bundler writes an entrypoint to memfs using this path, but webpack
  // resolves symlinks when reading, causing a mismatch if the path contains symlinks.
  const outputPath = resolve(realpathSync(tmpdir()), "__workflow-source.js");

  const allWorkflows = collectAllWorkflows(workflows);

  let code = `"use strict";\n`;
  code += `// AUTO-GENERATED at runtime - DO NOT EDIT\n`;
  code += `const { createWorkflowFunction } = require("${factoryPath}");\n\n`;

  for (const workflow of allWorkflows) {
    const plan = generateWorkflowPlan(workflow);
    const safeName = workflow.name.replace(/-/g, "_").replace(/\./g, "_");

    code += `const ${safeName}Plan = ${JSON.stringify(plan)};\n`;
    code += `exports["${workflow.name}"] = createWorkflowFunction(${safeName}Plan);\n\n`;
  }

  await writeFile(outputPath, code, "utf-8");
  return outputPath;
}
