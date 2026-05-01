import { describe, expect, it } from "vitest";

import { fanOut, isFanOutStep } from "../dag-sync-fanout";
import { step } from "../dag-sync-step";
import { createWorkflow } from "../dag-sync-workflow";

interface ItemBag {
  item: string;
  processed: string;
}

const processItem = step<ItemBag, undefined>()({
  name: "processItem",
  needs: ["item"] as const,
  provides: ["processed"] as const,
  run: async (_ctx, bag) => ({ processed: bag.item.toUpperCase() }),
});

const childWorkflow = createWorkflow<ItemBag>("child-wf").requires("item").build([processItem]);

interface ParentBag {
  items: string[];
  results: string[];
}

describe("dag-sync-fanout", () => {
  describe("fanOut factory", () => {
    const fo = fanOut<ParentBag>()({
      name: "processAll",
      needs: ["items"] as const,
      childWorkflow,
      mapInput: (bag) => bag.items.map((item) => ({ item })),
      provides: ["results"] as const,
      aggregateResults: (childResults) => ({
        results: childResults.map((r) => r.processed),
      }),
      concurrency: 3,
    });

    it("creates a FanOut with correct step metadata", () => {
      expect(fo.name).toBe("processAll");
      expect(fo.needs).toEqual(["items"]);
      expect(fo.provides).toEqual(["results"]);
    });

    it("attaches __fanOut metadata with child workflow reference", () => {
      expect(fo.__fanOut).toBeDefined();
      expect(fo.__fanOut.childWorkflow).toBe(childWorkflow);
      expect(fo.__fanOut.concurrency).toBe(3);
      expect(typeof fo.__fanOut.mapInput).toBe("function");
      expect(typeof fo.__fanOut.aggregateResults).toBe("function");
    });

    it("run() throws a descriptive error", async () => {
      await expect(fo.run(undefined, {})).rejects.toThrow(
        'FanOut "processAll" cannot be executed directly',
      );
    });

    it("rejects concurrency of 0", () => {
      expect(() =>
        fanOut<ParentBag>()({
          name: "bad",
          needs: ["items"] as const,
          childWorkflow,
          mapInput: (bag) => bag.items.map((item) => ({ item })),
          provides: ["results"] as const,
          aggregateResults: () => ({ results: [] }),
          concurrency: 0,
        }),
      ).toThrow("concurrency must be a positive finite number, got 0");
    });

    it("rejects negative concurrency", () => {
      expect(() =>
        fanOut<ParentBag>()({
          name: "bad",
          needs: ["items"] as const,
          childWorkflow,
          mapInput: (bag) => bag.items.map((item) => ({ item })),
          provides: ["results"] as const,
          aggregateResults: () => ({ results: [] }),
          concurrency: -1,
        }),
      ).toThrow("concurrency must be a positive finite number, got -1");
    });

    it("rejects NaN concurrency", () => {
      expect(() =>
        fanOut<ParentBag>()({
          name: "bad",
          needs: ["items"] as const,
          childWorkflow,
          mapInput: (bag) => bag.items.map((item) => ({ item })),
          provides: ["results"] as const,
          aggregateResults: () => ({ results: [] }),
          concurrency: NaN,
        }),
      ).toThrow("concurrency must be a positive finite number, got NaN");
    });

    it("defaults concurrency to Infinity when not specified", () => {
      const unbounded = fanOut<ParentBag>()({
        name: "unbounded",
        needs: ["items"] as const,
        childWorkflow,
        mapInput: (bag) => bag.items.map((item) => ({ item })),
        provides: ["results"] as const,
        aggregateResults: (childResults) => ({
          results: childResults.map((r) => r.processed),
        }),
      });

      expect(unbounded.__fanOut.concurrency).toBe(Infinity);
    });

    it("mapInput and aggregateResults closures work correctly", () => {
      const inputs = fo.__fanOut.mapInput({ items: ["a", "b", "c"] });
      expect(inputs).toEqual([{ item: "a" }, { item: "b" }, { item: "c" }]);

      const aggregated = fo.__fanOut.aggregateResults([{ processed: "X" }, { processed: "Y" }]);
      expect(aggregated).toEqual({ results: ["X", "Y"] });
    });
  });

  describe("isFanOutStep", () => {
    it("returns true for FanOut objects", () => {
      const fo = fanOut<ParentBag>()({
        name: "test",
        needs: ["items"] as const,
        childWorkflow,
        mapInput: (bag) => bag.items.map((item) => ({ item })),
        provides: ["results"] as const,
        aggregateResults: () => ({ results: [] }),
      });

      expect(isFanOutStep(fo)).toBe(true);
    });

    it("returns false for regular steps", () => {
      const regularStep = step<ParentBag, undefined>()({
        name: "regular",
        needs: [] as const,
        provides: ["items"] as const,
        run: async () => ({ items: [] }),
      });

      expect(isFanOutStep(regularStep)).toBe(false);
    });
  });

  describe("FanOut integrates into createWorkflow", () => {
    it("builds a workflow containing a FanOut step", () => {
      const fo = fanOut<ParentBag>()({
        name: "processAll",
        needs: ["items"] as const,
        childWorkflow,
        mapInput: (bag) => bag.items.map((item) => ({ item })),
        provides: ["results"] as const,
        aggregateResults: (childResults) => ({
          results: childResults.map((r) => r.processed),
        }),
      });

      const workflow = createWorkflow<ParentBag>("parent-wf").requires("items").build([fo]);

      expect(workflow.name).toBe("parent-wf");
      expect(workflow.steps).toHaveLength(1);
      expect(workflow.steps[0]!.name).toBe("processAll");
    });

    it("mixes FanOut steps with regular steps in a workflow", () => {
      const seedStep = step<ParentBag, undefined>()({
        name: "seedItems",
        needs: [] as const,
        provides: ["items"] as const,
        run: async () => ({ items: ["a", "b"] }),
      });

      const fo = fanOut<ParentBag>()({
        name: "processAll",
        needs: ["items"] as const,
        childWorkflow,
        mapInput: (bag) => bag.items.map((item) => ({ item })),
        provides: ["results"] as const,
        aggregateResults: (childResults) => ({
          results: childResults.map((r) => r.processed),
        }),
      });

      const workflow = createWorkflow<ParentBag>("mixed-wf").build([seedStep, fo]);

      expect(workflow.steps).toHaveLength(2);
    });
  });
});
