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
    "liveness-erase": true;
  }
}

interface Bag {
  userId: string;
  erased: boolean;
  items: string[];
  results: boolean[];
}

const config = defineBuildConfig({
  workerProfiles: {
    "liveness-erase": { minCount: 1, initialCount: 1, maxCount: 1 },
  },
});

const eraseStep = step<Bag>()({
  name: "eraseLiveness",
  needs: ["userId"] as const,
  provides: ["erased"] as const,
  workerProfile: "liveness-erase",
  run: async () => ({ erased: true }),
});

describe("consumer-defined worker profiles: typing", () => {
  it("accepts a registered profile and rejects an unregistered one", () => {
    expectTypeOf<"liveness-erase">().toExtend<WorkerProfile>();
    expectTypeOf<"standard" | "heavy">().toExtend<WorkerProfile>();
    expectTypeOf<"liveness-erse">().not.toExtend<WorkerProfile>();

    step<Bag>()({
      name: "typo",
      needs: ["userId"] as const,
      provides: ["erased"] as const,
      // @ts-expect-error -- not registered in WorkerProfileRegistry
      workerProfile: "liveness-erse",
      run: async () => ({ erased: true }),
    });
  });

  it("types build-config profile keys and fallbackProfile against the registry", () => {
    // @ts-expect-error -- not registered in WorkerProfileRegistry
    expect(() => defineBuildConfig({ workerProfiles: { "liveness-erse": {} } })).not.toThrow();

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
    expect(getWorkerProfiles(config)).toEqual(["standard", "heavy", "liveness-erase"]);
  });

  it("validates a declared profile only against a config that declares it", () => {
    expect(isValidWorkerProfile("liveness-erase")).toBe(false);
    expect(isValidWorkerProfile("liveness-erase", config)).toBe(true);
    expect(isValidWorkerProfile("standard", config)).toBe(true);
  });

  it("leaves the built-in table unchanged", () => {
    expect(Object.keys(WORKER_PROFILES)).toEqual(["standard", "heavy"]);
  });

  it("rejects malformed and reserved profile names in the build config", () => {
    const names = ["Liveness", "liveness_erase", "-erase", "erase-", "a".repeat(33), "workflow"];
    for (const name of names) {
      expect(isWorkerProfileName(name)).toBe(false);
      const result = ComposerBuildConfigSchema.safeParse({ workerProfiles: { [name]: {} } });
      expect(result.success, name).toBe(false);
    }
    expect(isWorkerProfileName("liveness-erase")).toBe(true);
  });
});

describe("consumer-defined worker profiles: routing", () => {
  it("derives the task queue from the profile name", () => {
    expect(getTaskQueueForProfile("liveness-erase")).toBe("liveness-erase-tasks");
    expect(getTaskQueueForProfile("standard")).toBe("standard-tasks");
    expect(getTaskQueueForProfile("heavy")).toBe("heavy-tasks");
  });

  it("includes declared queues in getAllTaskQueues only when given the config", () => {
    expect(getAllTaskQueues()).toEqual(["standard-tasks", "heavy-tasks"]);
    expect(getAllTaskQueues(config)).toEqual([
      "standard-tasks",
      "heavy-tasks",
      "liveness-erase-tasks",
    ]);
  });

  it("routes a pinned step to the profile's queue", () => {
    const workflow = createWorkflow<Bag>("erase").requires("userId").build([eraseStep]);
    expect(generateWorkflowPlan(workflow).batches[0]?.steps[0]?.taskQueue).toBe(
      "liveness-erase-tasks",
    );
  });

  it("rejects a malformed profile name at plan generation", () => {
    const bad = { ...eraseStep, workerProfile: "Bad_Name" } as unknown as typeof eraseStep;
    const workflow = createWorkflow<Bag>("bad-profile").requires("userId").build([bad]);
    expect(() => generateWorkflowPlan(workflow)).toThrow(/invalid workerProfile "Bad_Name"/);
  });
});

describe("consumer-defined worker profiles: topology and workers", () => {
  it("deploys a declared profile as dedicated, starting from standard resources", () => {
    const profile = getEffectiveProfileConfig("liveness-erase", config);
    expect(profile).toEqual({
      name: "liveness-erase",
      taskQueue: "liveness-erase-tasks",
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
      "liveness-erase",
    ]);
    expect(topology.dedicatedProfiles["liveness-erase"]).toEqual(profile);
  });

  it("throws for an undeclared profile", () => {
    expect(() => getEffectiveProfileConfig("liveness-erase")).toThrow(/not declared/);
    expect(() =>
      resolveActivityWorkerRuntimeConfig({
        environment: "prod",
        workerProfile: "liveness-erase",
        config: defineBuildConfig({}),
      }),
    ).toThrow(/Invalid worker profile: "liveness-erase"/);
  });

  it("starts a worker that polls only the declared profile's queue", () => {
    expect(
      resolveActivityWorkerRuntimeConfig({
        environment: "prod",
        workerProfile: "liveness-erase",
        config,
      }),
    ).toEqual({
      workerProfile: "liveness-erase",
      taskQueues: ["liveness-erase-tasks"],
      maxConcurrentActivityTaskExecutions:
        WORKER_PROFILES.standard.resources.maxConcurrentActivities,
    });

    // The standard worker does not pick up the dedicated profile's queue.
    expect(
      resolveActivityWorkerRuntimeConfig({ environment: "prod", workerProfile: "standard", config })
        .taskQueues,
    ).toEqual(["standard-tasks"]);
  });

  it("supports envExclusiveOptIn on a declared profile, in both directions", () => {
    const optInConfig = defineBuildConfig({
      workerProfiles: {
        "liveness-erase": {
          envExclusiveOptIn: { environments: ["prod"], fallbackProfile: "standard" },
        },
      },
    });

    const preview = resolveProfilesForEnvironment("preview", optInConfig);
    expect(preview.dedicatedProfiles["liveness-erase"]).toBeUndefined();
    expect(preview.fallbackQueues.standard).toEqual(["liveness-erase-tasks"]);

    const absorbing = defineBuildConfig({
      workerProfiles: {
        "liveness-erase": {},
        heavy: { envExclusiveOptIn: { environments: ["prod"], fallbackProfile: "liveness-erase" } },
      },
    });
    expect(resolveProfilesForEnvironment("preview", absorbing).fallbackQueues).toEqual({
      "liveness-erase": ["heavy-tasks"],
    });
  });
});

describe("findUndeclaredWorkerProfiles", () => {
  it("reports steps, including fan-out children, pinned to a profile the config omits", () => {
    const child = createWorkflow<Bag>("child").requires("userId").build([eraseStep]);
    const parent = createWorkflow<Bag>("parent")
      .requires("items")
      .build([
        fanOut<Bag>()({
          name: "eraseAll",
          needs: ["items"] as const,
          childWorkflow: child,
          mapInput: (bag) => bag.items.map((userId) => ({ userId })),
          provides: ["results"] as const,
          aggregateResults: (results) => ({ results: results.map((r) => r.erased) }),
        }),
      ]);

    expect(findUndeclaredWorkerProfiles([parent], config)).toEqual([]);
    expect(findUndeclaredWorkerProfiles([parent], defineBuildConfig({}))).toEqual([
      { workflow: "child", step: "eraseLiveness", workerProfile: "liveness-erase" },
    ]);
  });
});
