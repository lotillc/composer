import { createComposer, type StepContextProvider, step } from "../internal";

/**
 * No-op context provider for tests that don't need database access.
 * Steps receive `undefined` as their context.
 */
export const noOpContextProvider: StepContextProvider<undefined> = {
  beforeStep: async () => undefined,
  afterStep: async () => {},
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
  temporal: { serverAddress: "localhost:7233", namespace: "test" },
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

// Helper function to create test steps (supports both sync and async run functions)
// The Name type parameter preserves the literal type for compile-time step identity
export const createTestStep = <
  const Name extends string,
  const Needs extends readonly (keyof TestBag)[],
  const Provides extends readonly (keyof TestBag)[],
>(
  name: Name,
  needs: Needs,
  provides: Provides,
  run:
    | ((bag: Pick<TestBag, Needs[number]>) => Pick<TestBag, Provides[number]>)
    | ((bag: Pick<TestBag, Needs[number]>) => Promise<Pick<TestBag, Provides[number]>>),
) =>
  step<TestBag, unknown>()({
    name,
    needs,
    provides,
    run: async (_context, bag) => {
      // Handle both sync and async implementations
      const result = await Promise.resolve(run(bag));
      // Cast the result to satisfy the ExactReturn type
      // This is safe because we know the implementation returns exactly the right shape
      return result as Pick<TestBag, Provides[number]>;
    },
  });
