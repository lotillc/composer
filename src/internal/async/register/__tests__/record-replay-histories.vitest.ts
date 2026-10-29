/**
 * Records the replay fixtures in `fixtures/replay-histories/`. Skipped unless
 * `COMPOSER_RECORD_REPLAY_HISTORIES` names a Temporal server to record against.
 *
 * Re-record whenever a fixture workflow changes, or when a workflow-factory change is meant
 * to alter the commands it emits (and so must reach consumers behind `patched()`):
 *
 *   temporal server start-dev --port 7299 --headless
 *   COMPOSER_RECORD_REPLAY_HISTORIES=localhost:7299 pnpm vitest run record-replay-histories
 *
 * Use a fresh server: workers here are unversioned, and a namespace whose task queues a
 * versioned deployment already claims will not hand them tasks. Needs the `temporal` CLI,
 * which writes the histories as proto3 JSON.
 */

import { execFileSync } from "node:child_process";
import { mkdir, rm, writeFile } from "node:fs/promises";
import { homedir, hostname } from "node:os";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { Client, Connection } from "@temporalio/client";
import type { Worker } from "@temporalio/worker";
import { describe, expect, it } from "vitest";
import { createComposer } from "../../../create-composer.js";
import { createActivityWorkers } from "../activity-worker.js";
import { createWorkflowWorkers } from "../workflow-worker.js";
import {
  approvalSignal,
  dagWorkflow,
  replayFixtureWorkflows,
  stagedWorkflow,
} from "./fixtures/replay-workflows.js";

const address = process.env.COMPOSER_RECORD_REPLAY_HISTORIES;
const outDir = fileURLToPath(new URL("./fixtures/replay-histories/", import.meta.url));
const quiet = { info() {}, warn() {}, error: console.error, debug() {} };
const contextProvider = { beforeStep: async () => ({}), afterStep: async () => {} };

describe.runIf(address)("record replay histories", () => {
  it("records every run of each fixture workflow", { timeout: 180_000 }, async () => {
    process.env.ENVIRONMENT_NAME ??= "local";
    const common = { serverAddress: address!, namespace: "default", deploymentSeriesName: "unused", logger: quiet };
    const workflowWorkers = await createWorkflowWorkers({
      ...common,
      workflows: replayFixtureWorkflows,
      taskQueues: ["workflow-tasks"],
      maxConcurrentWorkflowTaskExecutions: 10,
    });
    const activityWorkers = await createActivityWorkers({
      ...common,
      workflows: replayFixtureWorkflows,
      taskQueues: ["standard-tasks"],
      maxConcurrentActivityTaskExecutions: 10,
      contextProvider,
    });
    const workers: Worker[] = [...workflowWorkers.workers, ...activityWorkers.workers];
    const running = workers.map((worker) => worker.run());

    const composer = createComposer({
      contextProvider,
      logger: quiet,
      temporal: { serverAddress: address!, namespace: "default", serviceName: "replay-fixtures" },
    });
    const connection = await Connection.connect({ address: address! });
    const client = new Client({ connection, namespace: "default" });

    try {
      const dagId = "replay-fixture-dag-1";
      const dag = await composer.runAsyncWorkflow(dagWorkflow, { items: ["a", "b"] }, { workflowId: dagId });
      expect(dag.error).toBeUndefined();

      const stagedId = "replay-fixture-staged-1";
      await composer.startAsyncWorkflow(stagedWorkflow, { requestId: "r1" }, { workflowId: stagedId });
      // Signal once the waiting stage's run is up, so the wait is recorded blocking.
      for (let i = 0; i < 100; i++) {
        const runs = await listRuns(client, stagedId);
        if (runs.length === 2) break;
        await new Promise((r) => setTimeout(r, 200));
      }
      await composer.signalAsyncWorkflow(stagedId, approvalSignal, { approved: true }, {
        dedupeKey: "fixture-1",
      });
      await client.workflow.getHandle(stagedId).result();

      await rm(outDir, { recursive: true, force: true });
      await mkdir(outDir, { recursive: true });
      const ids = [dagId, `${dagId}/fanOutDouble/0`, `${dagId}/fanOutDouble/1`, stagedId];
      for (const workflowId of ids) {
        const runs = (await listRuns(client, workflowId)).sort((a, b) =>
          a.startTime.getTime() - b.startTime.getTime(),
        );
        for (const [index, run] of runs.entries()) {
          const history = scrub(
            JSON.parse(
            execFileSync(
              "temporal",
              ["workflow", "show", "--address", address!, "--workflow-id", workflowId,
                "--run-id", run.runId, "--output", "json"],
              { encoding: "utf-8", maxBuffer: 64 * 1024 * 1024 },
            ),
          ),
          );
          const file = `${workflowId.replaceAll("/", "__")}--run-${index + 1}.json`;
          await writeFile(
            resolve(outDir, file),
            `${JSON.stringify({ workflowId, history }, null, 2)}\n`,
          );
        }
      }
    } finally {
      for (const worker of workers) worker.shutdown();
      await Promise.allSettled(running);
      await connection.close();
      await workflowWorkers.connection.close();
      await activityWorkers.connection.close();
    }
  });
});

/**
 * The fixtures are committed to a public repository, so nothing about the recording
 * machine may stay in them: worker identities and sticky task queue names carry its
 * hostname, and stack traces its paths. None of these take part in replay.
 */
function scrub(value: unknown, key?: string): unknown {
  if (key === "stackTrace" && typeof value === "string") return "";
  if (typeof value === "string") {
    let scrubbed = value;
    for (const host of new Set([hostname(), hostname().split(".")[0]!])) {
      scrubbed = scrubbed.replaceAll(host, "recorder-host");
    }
    return scrubbed.replaceAll(homedir(), "~");
  }
  if (Array.isArray(value)) return value.map((item) => scrub(item));
  if (value !== null && typeof value === "object") {
    return Object.fromEntries(Object.entries(value).map(([k, v]) => [k, scrub(v, k)]));
  }
  return value;
}

async function listRuns(client: Client, workflowId: string) {
  const runs = [];
  for await (const run of client.workflow.list({ query: `WorkflowId = '${workflowId}'` })) {
    runs.push(run);
  }
  return runs;
}
