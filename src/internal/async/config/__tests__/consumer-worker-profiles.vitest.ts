import { describe, expect, expectTypeOf, it } from "vitest";
import { defineBuildConfig } from "../../../../build-config/index.js";
import { ComposerBuildConfigSchema } from "../../../../build-config/schema.js";
import { createWorkflow, fanOut, step, type WorkerProfile } from "../../../../index.js";
import { generateWorkflowPlan } from "../../register/generate-workflow-source.js";
import {
  findUndeclaredWorkerProfiles,
  getAllTaskQueues,
  getEffectiveProfileConfig,
  getTaskQueueForProfile,
  getWorkerProfiles,
  isValidWorkerProfile,
  isWorkerProfileName,
  resolveActivityWorkerRuntimeConfig,
  resolveProfilesForEnvironment,
  WORKER_PROFILES,
} from "../worker-profiles.js";

// What a consumer writes as `declare module "@lotiai/composer"`. Inside this
// package the specifier must be the source entry point, not the package name,
// which resolves to dist/.
declare module "../../../../index.js" {
  interface WorkerProfileRegistry {
    "request-signing": true;
  }
}

interface Bag {
  userId: string;
  signed: boolean;
  items: string[];
  results: boolean[];
}

const config = defineBuildConfig({
  workerProfiles: {
    "request-signing": { minCount: 1, initialCount: 1, maxCount: 1 },
  },
});

const signStep = step<Bag>()({
  name: "signRequest",
  needs: ["userId"] as const,
  provides: ["signed"] as const,
  workerProfile: "request-signing",
  run: async () => ({ signed: true }),
});

describe("consumer-defined worker profiles: typing", () => {
  it("accepts a registered profile and rejects an unregistered one", () => {
    expectTypeOf<"request-signing">().toExtend<WorkerProfile>();
    expectTypeOf<"standard" | "heavy">().toExtend<WorkerProfile>();
    expectTypeOf<"request-signign">().not.toExtend<WorkerProfile>();

    step<Bag>()({
      name: "typo",
      needs: ["userId"] as const,
      provides: ["signed"] as const,
      // @ts-expect-error -- not registered in WorkerProfileRegistry
      workerProfile: "request-signign",
      run: async () => ({ signed: true }),
    });
  });

  it("types build-config profile keys and fallbackProfile against the registry", () => {
    // @ts-expect-error -- not registered in WorkerProfileRegistry
    expect(() => defineBuildConfig({ workerProfiles: { "request-signign": {} } })).not.toThrow();

    defineBuildConfig({
      workerProfiles: {
        heavy: {
          envExclusiveOptIn: {
            environments: ["prod"],
            // @ts-expect-error -- not registered in WorkerProfileRegistry
            fallbackProfile: "stndard",
          },
        },
      },
    });
  });
});

describe("consumer-defined worker profiles: declaration", () => {
  it("lists declared profiles after the built-ins", () => {
    expect(getWorkerProfiles()).toEqual(["standard", "heavy"]);
    expect(getWorkerProfiles(config)).toEqual(["standard", "heavy", "request-signing"]);
  });

  it("validates a declared profile only against a config that declares it", () => {
    expect(isValidWorkerProfile("request-signing")).toBe(false);
    expect(isValidWorkerProfile("request-signing", config)).toBe(true);
    expect(isValidWorkerProfile("standard", config)).toBe(true);
  });

  it("leaves the built-in table unchanged", () => {
    expect(Object.keys(WORKER_PROFILES)).toEqual(["standard", "heavy"]);
  });

  it("rejects malformed and reserved profile names in the build config", () => {
    const names = ["Signing", "request_signing", "-signing", "signing-", "a".repeat(33), "workflow"];
    for (const name of names) {
      expect(isWorkerProfileName(name)).toBe(false);
      const result = ComposerBuildConfigSchema.safeParse({ workerProfiles: { [name]: {} } });
      expect(result.success, name).toBe(false);
    }
    expect(isWorkerProfileName("request-signing")).toBe(true);
  });
});

describe("consumer-defined worker profiles: routing", () => {
  it("derives the task queue from the profile name", () => {
    expect(getTaskQueueForProfile("request-signing")).toBe("request-signing-tasks");
    expect(getTaskQueueForProfile("standard")).toBe("standard-tasks");
    expect(getTaskQueueForProfile("heavy")).toBe("heavy-tasks");
  });

  it("includes declared queues in getAllTaskQueues only when given the config", () => {
    expect(getAllTaskQueues()).toEqual(["standard-tasks", "heavy-tasks"]);
    expect(getAllTaskQueues(config)).toEqual([
      "standard-tasks",
      "heavy-tasks",
      "request-signing-tasks",
    ]);
  });

  it("routes a pinned step to the profile's queue", () => {
    const workflow = createWorkflow<Bag>("sign").requires("userId").build([signStep]);
    expect(generateWorkflowPlan(workflow).batches[0]?.steps[0]?.taskQueue).toBe(
      "request-signing-tasks",
    );
  });

  it("rejects a malformed profile name at plan generation", () => {
    const bad = { ...signStep, workerProfile: "Bad_Name" } as unknown as typeof signStep;
    const workflow = createWorkflow<Bag>("bad-profile").requires("userId").build([bad]);
    expect(() => generateWorkflowPlan(workflow)).toThrow(/invalid workerProfile "Bad_Name"/);
  });
});

describe("consumer-defined worker profiles: topology and workers", () => {
  it("deploys a declared profile as dedicated, starting from standard resources", () => {
    const profile = getEffectiveProfileConfig("request-signing", config);
    expect(profile).toEqual({
      name: "request-signing",
      taskQueue: "request-signing-tasks",
      description: "Consumer-defined worker profile",
      resources: {
        ...WORKER_PROFILES.standard.resources,
        minCount: 1,
        initialCount: 1,
        maxCount: 1,
      },
    });

    const topology = resolveProfilesForEnvironment("prod", config);
    expect(Object.keys(topology.dedicatedProfiles)).toEqual([
      "standard",
      "heavy",
      "request-signing",
    ]);
    expect(topology.dedicatedProfiles["request-signing"]).toEqual(profile);
  });

  it("throws for an undeclared profile", () => {
    expect(() => getEffectiveProfileConfig("request-signing")).toThrow(/not declared/);
    expect(() =>
      resolveActivityWorkerRuntimeConfig({
        environment: "prod",
        workerProfile: "request-signing",
        config: defineBuildConfig({}),
      }),
    ).toThrow(/Invalid worker profile: "request-signing"/);
  });

  it("starts a worker that polls only the declared profile's queue", () => {
    expect(
      resolveActivityWorkerRuntimeConfig({
        environment: "prod",
        workerProfile: "request-signing",
        config,
      }),
    ).toEqual({
      workerProfile: "request-signing",
      taskQueues: ["request-signing-tasks"],
      maxConcurrentActivityTaskExecutions:
        WORKER_PROFILES.standard.resources.maxConcurrentActivities,
    });

    // The standard worker does not pick up the dedicated profile's queue.
    expect(
      resolveActivityWorkerRuntimeConfig({ environment: "prod", workerProfile: "standard", config })
        .taskQueues,
    ).toEqual(["standard-tasks"]);
  });

  it("never resolves an inherited Object.prototype key as a profile", () => {
    expect(isValidWorkerProfile("constructor", config)).toBe(false);
    expect(() => getTaskQueueForProfile("constructor" as WorkerProfile)).toThrow(
      /Invalid worker profile name/,
    );
  });

  it("supports envExclusiveOptIn on a declared profile, in both directions", () => {
    const optInConfig = defineBuildConfig({
      workerProfiles: {
        "request-signing": {
          envExclusiveOptIn: { environments: ["prod"], fallbackProfile: "standard" },
        },
      },
    });

    const preview = resolveProfilesForEnvironment("preview", optInConfig);
    expect(preview.dedicatedProfiles["request-signing"]).toBeUndefined();
    expect(preview.fallbackQueues.standard).toEqual(["request-signing-tasks"]);

    const absorbing = defineBuildConfig({
      workerProfiles: {
        "request-signing": {},
        heavy: { envExclusiveOptIn: { environments: ["prod"], fallbackProfile: "request-signing" } },
      },
    });
    expect(resolveProfilesForEnvironment("preview", absorbing).fallbackQueues).toEqual({
      "request-signing": ["heavy-tasks"],
    });
  });
});

describe("findUndeclaredWorkerProfiles", () => {
  it("reports steps, including fan-out children, pinned to a profile the config omits", () => {
    const child = createWorkflow<Bag>("child").requires("userId").build([signStep]);
    const parent = createWorkflow<Bag>("parent")
      .requires("items")
      .build([
        fanOut<Bag>()({
          name: "signAll",
          needs: ["items"] as const,
          childWorkflow: child,
          mapInput: (bag) => bag.items.map((userId) => ({ userId })),
          provides: ["results"] as const,
          aggregateResults: (results) => ({ results: results.map((r) => r.signed) }),
        }),
      ]);

    expect(findUndeclaredWorkerProfiles([parent], config)).toEqual([]);
    expect(findUndeclaredWorkerProfiles([parent], defineBuildConfig({}))).toEqual([
      { workflow: "child", step: "signRequest", workerProfile: "request-signing" },
    ]);
  });
});
