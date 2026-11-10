/**
 * Replay check for workflow histories.
 *
 * A workflow that declares AUTO_UPGRADE can move to a new build in the middle of a stage,
 * and a rollback moves it back. Either way, the build it lands on must reproduce the
 * commands the earlier build recorded, or the run fails every workflow task until the code
 * is fixed. Replaying saved histories against a build before it is deployed is how that is
 * caught while it is still a value change -- don't ship the build -- rather than new code
 * written under pressure.
 *
 * Replay runs offline: no Temporal server, no activities.
 *
 * @module replay-histories
 */

import { unlink } from "node:fs/promises";
import type { DataConverter, HistoryAndWorkflowId } from "@temporalio/common";
import { Worker } from "@temporalio/worker";
import type { ComposerWorkerInterceptors } from "../../context-provider.js";
import type { Workflow } from "../../dag-sync-workflow.js";
import { writeWorkflowSourceFile } from "./generate-workflow-source.js";
import { workflowBundlerOptions } from "./workflow-worker.js";

/**
 * Options for {@link replayWorkflowHistories}.
 */
export interface ReplayWorkflowHistoriesOptions {
  /** The workflow definitions of the build under test, as passed to its workflow worker. */
  workflows: Workflow<any, any, any, any, any>[];
  /**
   * Histories to replay, each with its workflow ID. A history may be a `History` object or
   * its JSON form, as written by `historyToJSON` or `temporal workflow show --output json`.
   * `client.workflow.list({ query }).intoHistories()` yields these directly.
   */
  histories: Iterable<HistoryAndWorkflowId> | AsyncIterable<HistoryAndWorkflowId>;
  /** Must match the workers' converter, or payloads in the histories cannot be decoded. */
  dataConverter?: DataConverter;
  /** Workflow interceptor modules the workers load; they take part in replay too. */
  interceptors?: Pick<ComposerWorkerInterceptors, "workflowModules">;
  /**
   * The workers' carried-state budget. It decides whether a stage boundary continues as new
   * or fails, so replaying under a different one can report histories that are fine.
   */
  continuationBudgetBytes?: number;
}

/** A history the build under test could not replay. */
export interface ReplayFailure {
  workflowId: string;
  runId: string;
  error: Error;
}

/** Outcome of {@link replayWorkflowHistories}. */
export interface ReplayWorkflowHistoriesResult {
  /** Histories replayed, failed or not. */
  replayed: number;
  /** Histories the build could not replay; empty when it is compatible with all of them. */
  failures: ReplayFailure[];
}

let replayCounter = 0;

/**
 * Replays recorded workflow histories against the given workflow definitions and reports
 * every history the definitions cannot reproduce.
 *
 * Bundles exactly what a versioned workflow worker runs, versioning declarations included:
 * the workflow's behavior and each stage boundary's next-run behavior. Replay matches a
 * continue-as-new command to its event by type and never compares these attributes; the
 * recorded fixtures, which an unversioned worker wrote without them, replay against this
 * bundle.
 *
 * Run it in CI before deploying a build, against histories captured from the build
 * currently serving long-lived workflows, so a change that would break in-flight runs, or
 * the rollback to that build, is caught before it ships.
 *
 * @example
 * ```typescript
 * const { failures } = await replayWorkflowHistories({
 *   workflows,
 *   histories: client.workflow.list({ query: "WorkflowType = 'review'" }).intoHistories(),
 * });
 * if (failures.length > 0) throw new Error(`${failures.length} histories no longer replay`);
 * ```
 */
export async function replayWorkflowHistories(
  options: ReplayWorkflowHistoriesOptions,
): Promise<ReplayWorkflowHistoriesResult> {
  // A file of its own, so a replay never rewrites the source a worker in this process loads.
  const workflowsPath = await writeWorkflowSourceFile(options.workflows, {
    workerVersioning: true,
    fileName: `__workflow-source-replay-${process.pid}-${++replayCounter}.js`,
    continuationBudgetBytes: options.continuationBudgetBytes,
  });

  try {
    const results = Worker.runReplayHistories(
      {
        workflowsPath,
        bundlerOptions: workflowBundlerOptions(),
        ...(options.dataConverter ? { dataConverter: options.dataConverter } : {}),
        ...(options.interceptors?.workflowModules
          ? { interceptors: { workflowModules: options.interceptors.workflowModules } }
          : {}),
      },
      options.histories,
    );

    let replayed = 0;
    const failures: ReplayFailure[] = [];
    for await (const result of results) {
      replayed++;
      if (result.error) {
        failures.push({ workflowId: result.workflowId, runId: result.runId, error: result.error });
      }
    }
    return { replayed, failures };
  } finally {
    await unlink(workflowsPath).catch(() => undefined);
  }
}
