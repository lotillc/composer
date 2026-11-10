import { createRequire } from "node:module";
import { TEMPORAL_VERSION_MISMATCH_CODE } from "../../workflow-codes.js";

export const TEMPORAL_PEERS = ["activity", "client", "common", "worker", "workflow"] as const;

const require = createRequire(import.meta.url);

export class TemporalVersionMismatchError extends Error {
  readonly code = TEMPORAL_VERSION_MISMATCH_CODE;

  constructor(readonly versions: Readonly<Record<string, string>>) {
    const found = Object.entries(versions)
      .map(([name, version]) => `${name}@${version}`)
      .join(", ");
    super(
      `Composer needs every @temporalio/* package on one version, found ${found}. ` +
        "Align them in your dependencies.",
    );
    this.name = "TemporalVersionMismatchError";
  }
}

/** Throws when the peers Composer resolves are not all one version; peer ranges cannot say that. */
export function assertTemporalVersionsAligned(
  readVersion: (name: string) => string = (name) =>
    (require(`${name}/package.json`) as { version: string }).version,
): void {
  const versions = Object.fromEntries(
    TEMPORAL_PEERS.map((peer) => {
      const name = `@temporalio/${peer}`;
      return [name, readVersion(name)];
    }),
  );
  if (new Set(Object.values(versions)).size > 1) throw new TemporalVersionMismatchError(versions);
}
