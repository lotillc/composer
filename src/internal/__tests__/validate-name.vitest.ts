import { describe, expect, it } from "vitest";
import { createWorkflow, fanOut, step } from "../../index.js";

interface Bag {
  input: string;
  output: string;
}

const makeStep = (name: string) =>
  step<Bag>()({
    name,
    needs: ["input"] as const,
    provides: ["output"] as const,
    run: async () => ({ output: "done" }),
  });

const childWorkflow = createWorkflow<Bag>("child-workflow")
  .requires("input")
  .build([makeStep("childStep")]);

const makeFanOut = (name: string) =>
  fanOut<{ items: string[]; results: string[] }>()({
    name,
    needs: ["items"] as const,
    childWorkflow,
    mapInput: (bag) => bag.items.map((input) => ({ input })),
    provides: ["results"] as const,
    aggregateResults: (results) => ({ results: results.map((r) => r.output) }),
  });

const makeCheckpoint = (name: string) => {
  const s = makeStep("checkpointedStep");
  return createWorkflow<Bag>("checkpoint-workflow")
    .requires("input")
    .build([s])
    .checkpoint(name, { afterStep: s });
};

const definers = [
  ["Workflow", (name: string) => createWorkflow<Bag>(name)],
  ["Step", makeStep],
  ["FanOut", makeFanOut],
  ["Checkpoint", makeCheckpoint],
] as const;

describe("definition-time name validation", () => {
  describe.each(definers)("%s names", (kind, define) => {
    it.each(["__proto__", "constructor", "prototype"])("rejects reserved name %j", (name) => {
      expect(() => define(name)).toThrow(
        `${kind} name "${name}" is reserved: "__proto__", "constructor" and "prototype" cannot be used as names.`,
      );
    });

    it.each([
      "has space",
      'has"quote',
      "has'quote",
      "1leadingDigit",
      "-leadingDash",
      ".leadingDot",
      "back\\slash",
      "new\nline",
      "semi;colon",
      "",
    ])("rejects unsafe name %j", (name) => {
      expect(() => define(name)).toThrow(
        `${kind} name ${JSON.stringify(name)} is invalid: names must start with a letter or "_" ` +
          `and contain only letters, digits, "_", "." and "-".`,
      );
    });

    it.each(["myStep", "my-workflow", "parent.child.step", "_private", "step_2", "Mixed-case.v_1"])(
      "accepts %j",
      (name) => {
        expect(() => define(name)).not.toThrow();
      },
    );
  });
});
