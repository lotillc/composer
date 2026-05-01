import { describe, expect, it } from "vitest";
import { defineBuildConfig } from "../../../../build-config";
import {
  getEffectiveProfileConfig,
  resolveActivityWorkerRuntimeConfig,
  resolveProfilesForEnvironment,
} from "../worker-profiles";

const config = defineBuildConfig({
  workerProfiles: {
    heavy: {
      envExclusiveOptIn: {
        environments: ["prod"],
        fallbackProfile: "standard",
      },
    },
  },
});

describe("worker profile topology", () => {
  it("routes heavy queue to standard in non-prod environments", () => {
    const topology = resolveProfilesForEnvironment("preview", config);

    expect(topology.dedicatedProfiles.standard?.taskQueue).toBe("standard-tasks");
    expect(topology.dedicatedProfiles.heavy).toBeUndefined();
    expect(topology.fallbackQueues.standard).toEqual(["heavy-tasks"]);
  });

  it("configures the standard worker to consume absorbed queues in non-prod", () => {
    const runtimeConfig = resolveActivityWorkerRuntimeConfig({
      environment: "preview",
      workerProfile: "standard",
      config,
    });

    expect(runtimeConfig.workerProfile).toBe("standard");
    expect(runtimeConfig.taskQueues).toEqual(["standard-tasks", "heavy-tasks"]);
    expect(runtimeConfig.maxConcurrentActivityTaskExecutions).toBe(15);
  });

  it("merges initialCount overrides into the effective profile config", () => {
    const overrideConfig = defineBuildConfig({
      workerProfiles: {
        standard: {
          minCount: 1,
          initialCount: 3,
          maxCount: 7,
        },
      },
    });

    const profileConfig = getEffectiveProfileConfig("standard", overrideConfig);

    expect(profileConfig.resources.minCount).toBe(1);
    expect(profileConfig.resources.initialCount).toBe(3);
    expect(profileConfig.resources.maxCount).toBe(7);
  });
});
