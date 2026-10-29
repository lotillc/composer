/**
 * Replays the recorded fixture histories against the current workflow factory.
 *
 * A failure here means a change to the factory alters the commands it emits for a history
 * already in flight. Consumers' AUTO_UPGRADE workflows would then fail mid-stage on upgrade,
 * or on rollback. Gate the change behind `patched()`, or, if it is meant to change replay,
 * re-record the fixtures (see `record-replay-histories.vitest.ts`) and say so in the
 * changeset.
 */

import { readdir, readFile } from "node:fs/promises";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import type { HistoryAndWorkflowId } from "@temporalio/common";
import { describe, expect, it } from "vitest";
import { createWorkflow, step } from "../../../../index.js";
import { replayWorkflowHistories } from "../replay-histories.js";
import { childWorkflow, type DagBag, replayFixtureWorkflows, stagedWorkflow } from "./fixtures/replay-workflows.js";

const historiesDir = fileURLToPath(new URL("./fixtures/replay-histories/", import.meta.url));

async function loadHistories(): Promise<HistoryAndWorkflowId[]> {
  const files = (await readdir(historiesDir)).filter((file) => file.endsWith(".json")).sort();
  return Promise.all(
    files.map(async (file) =>
      JSON.parse(await readFile(resolve(historiesDir, file), "utf-8")) as HistoryAndWorkflowId,
    ),
  );
}

describe("replayWorkflowHistories", () => {
  it("replays every recorded fixture history without a determinism failure", { timeout: 120_000 }, async () => {
    const histories = await loadHistories();
    // Three staged runs, the DAG run and its two children.
    expect(histories.length).toBe(6);

    const { replayed, failures } = await replayWorkflowHistories({
      workflows: replayFixtureWorkflows,
      histories,
    });

    expect(failures).toEqual([]);
    expect(replayed).toBe(histories.length);
  });

  it("reports a definition change that breaks an in-flight history", { timeout: 120_000 }, async () => {
    // One more step in the DAG's first batch schedules an extra activity the history lacks.
    const extra = step<DagBag>()({
      name: "extra",
      needs: ["items"],
      provides: [],
      run: async () => ({}),
    });
    const changedDag = createWorkflow<DagBag>("replay-fixture-dag")
      .requires("items")
      .build([extra]);
    const histories = (await loadHistories()).filter((h) => h.workflowId === "replay-fixture-dag-1");

    const { failures } = await replayWorkflowHistories({
      workflows: [changedDag, childWorkflow, stagedWorkflow],
      histories,
    });

    expect(failures.map((f) => f.workflowId)).toEqual(["replay-fixture-dag-1"]);
  });
});
