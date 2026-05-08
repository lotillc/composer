import { createComposer, type AsyncStepRuntime, type StepContextProvider, step } from "../internal";
import type { AssertSerializable, StrictStepReturn } from "../internal/dag-sync-step";

/**
 * No-op context provider for tests that don't need database access.
 * Steps receive `undefined` as their context.
 */
export const noOpContextProvider: StepContextProvider<undefined> = {
  beforeStep: async () => undefined,
  afterStep: async () => {},
};

export const testAsyncStepRuntime: AsyncStepRuntime = {
  heartbeat: () => {},
  getHeartbeatDetails: () => undefined,
};

/**
 * Test composer (sync-only) with no-op context provider.
 * Use this for unit tests that test workflow mechanics without needing real context.
 */
export const testComposer = createComposer({ contextProvider: noOpContextProvider });

/**
 * Test composer (sync + async) with no-op context provider and dummy temporal config.
 * Use this for compile-time tests that need to validate async workflow type signatures.
 * Do NOT use for runtime async tests — the temporal address is fake.
 */
export const testAsyncComposer = createComposer({
  contextProvider: noOpContextProvider,
  temporal: { serverAddress: "localhost:7233", namespace: "test", serviceName: "test-service" },
});

// Test bag type for all tests
export type TestBag = {
  configured: string;
  input: string;
  processed: string;
  doubled: string;
  count: number;
  result: string;
  error?: string;
  // Non-serializable fields for testing compile-time serializability validation
  timestamp: Date;
  nested: { date: Date };
  callback: () => void;
};

type NonSerializableTestBagKey = "timestamp" | "nested" | "callback";
type SerializableTestBagKey = Exclude<keyof TestBag, NonSerializableTestBagKey>;
type TestStepInput<Needs extends readonly (keyof TestBag)[]> = Pick<TestBag, Needs[number]>;
type TestStepOutput<Provides extends readonly SerializableTestBagKey[]> = AssertSerializable<
  StrictStepReturn<TestBag, Provides[number]>
>;
type TestStepRun<
  Needs extends readonly (keyof TestBag)[],
  Provides extends readonly SerializableTestBagKey[],
> = (bag: TestStepInput<Needs>) => TestStepOutput<Provides> | Promise<TestStepOutput<Provides>>;

// Helper function to create test steps (supports both sync and async run functions)
// The Name type parameter preserves the literal type for compile-time step identity
export const createTestStep = <
  const Name extends string,
  const Needs extends readonly (keyof TestBag)[],
  const Provides extends readonly SerializableTestBagKey[],
>(
  name: Name,
  needs: Needs,
  provides: Provides,
  run: TestStepRun<Needs, Provides>,
) =>
  step<TestBag, unknown>()({
    name,
    needs,
    provides,
    run: async (_context, bag) => {
      return run(bag);
    },
  });
