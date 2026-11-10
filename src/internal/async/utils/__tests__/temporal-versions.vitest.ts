import { describe, expect, it } from "vitest";
import {
  assertTemporalVersionsAligned,
  TemporalVersionMismatchError,
} from "../temporal-versions.js";

describe("assertTemporalVersionsAligned", () => {
  it("passes for the installed peers, which the dev install keeps on one version", () => {
    expect(() => assertTemporalVersionsAligned()).not.toThrow();
  });

  it("names every peer and its version when they differ", () => {
    const versions: Record<string, string> = { "@temporalio/worker": "1.17.2" };
    let error: unknown;
    try {
      assertTemporalVersionsAligned((name) => versions[name] ?? "1.25.0");
    } catch (caught) {
      error = caught;
    }
    expect(error).toBeInstanceOf(TemporalVersionMismatchError);
    expect(error).toMatchObject({
      code: "COMPOSER_TEMPORAL_VERSION_MISMATCH",
      versions: {
        "@temporalio/activity": "1.25.0",
        "@temporalio/client": "1.25.0",
        "@temporalio/common": "1.25.0",
        "@temporalio/worker": "1.17.2",
        "@temporalio/workflow": "1.25.0",
      },
    });
  });
});
