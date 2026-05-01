import type { WorkerProfile } from "./async/config/worker-profiles";

// ============================================================================
// DURATION AND ACTIVITY CONFIGURATION TYPES
// ============================================================================

type LongDurationUnit =
  | "second"
  | "seconds"
  | "minute"
  | "minutes"
  | "hour"
  | "hours"
  | "day"
  | "days";
type ShortDurationUnit = "ms" | "s" | "m" | "h" | "d";

/**
 * Compile-time validated Temporal duration string.
 * Supports long form ("6 hours", "30 minutes") and short form ("6h", "30m").
 */
export type DurationString = `${number} ${LongDurationUnit}` | `${number}${ShortDurationUnit}`;

/**
 * Async (Temporal) runtime utilities injected into the step context.
 * Available on `ctx` via intersection with the application context type.
 *
 * In async execution, these methods delegate to the Temporal activity context.
 * In sync execution and tests, they are no-ops.
 */
export interface AsyncStepRuntime {
  /**
   * Report progress to Temporal. Call periodically in long-running activities to:
   * 1. Prove liveness -- Temporal cancels the activity if no heartbeat arrives
   *    within `asyncHeartbeatTimeout`.
   * 2. Record progress -- on retry, the new attempt can read the last heartbeat's
   *    details via `getHeartbeatDetails()` to resume work.
   *
   * `details` is any JSON-serializable value describing progress, e.g.
   * `{ domainsIngested: 42000 }` or `{ phase: "uploading_to_s3" }`.
   */
  heartbeat(details?: unknown): void;

  /**
   * Retrieve the most recent heartbeat details from a previous attempt.
   * Returns `undefined` on the first attempt or when no heartbeat was recorded.
   * Use with `heartbeat()` to implement resumable long-running activities.
   */
  getHeartbeatDetails<T = unknown>(): T | undefined;
}

/**
 * Per-step retry policy override. All fields are optional; unset fields
 * fall back to the framework defaults (3 attempts, coefficient 2, 1s initial, 60s max).
 */
export interface StepRetryPolicy {
  maximumAttempts?: number;
  backoffCoefficient?: number;
  initialInterval?: DurationString;
  maximumInterval?: DurationString;
}

/**
 * A Step represents a single unit of work in the workflow.
 *
 * Generic Parameters:
 * - Bag: The complete data type that flows through the workflow
 * - Needs: Array of field names this step requires as input
 * - Provides: Array of field names this step will output
 * - Context: The execution context available to this step (default: unknown)
 * - Name: The literal string type of the step name (enables compile-time step identity)
 *
 * The type system ensures:
 * - Steps can only access fields they declare in 'needs'
 * - Steps must return exactly the fields they declare in 'provides'
 * - Dependencies are validated at compile-time
 * - Each step has a unique type based on its name (for checkpoint validation)
 */
export type Step<
  Bag extends Record<string, any>,
  Needs extends readonly (keyof Bag)[],
  Provides extends readonly (keyof Bag)[],
  Context = unknown,
  Name extends string = string,
> = {
  name: Name;
  needs: Needs;
  provides: Provides;
  run: (
    context: Context & AsyncStepRuntime,
    bag: Pick<Bag, Needs[number]>,
  ) => Promise<{ [K in Provides[number]]: Bag[K] }> | { [K in Provides[number]]: Bag[K] };
  /**
   * Worker profile for Temporal execution - determines which worker pool this step runs on.
   * If not specified, defaults to "standard".
   * Only used for Temporal (async) execution. Sync execution ignores it.
   */
  workerProfile?: WorkerProfile;
  /**
   * Maximum time the activity may run from start to completion.
   * If not specified, defaults to "5 minutes".
   * Only used for Temporal (async) execution. Sync execution ignores it.
   */
  asyncStartToCloseTimeout?: DurationString;
  /**
   * Maximum time between heartbeats before Temporal considers the activity dead.
   * When set, the step should call `ctx.heartbeat()` periodically.
   * If not specified, no heartbeat timeout is enforced.
   * Only used for Temporal (async) execution. Sync execution ignores it.
   */
  asyncHeartbeatTimeout?: DurationString;
  /**
   * Per-step retry policy override. Unset fields fall back to framework defaults.
   * Only used for Temporal (async) execution. Sync execution ignores it.
   */
  asyncRetry?: StepRetryPolicy;
  /**
   * Workflow path tracks the logical ancestry of this step for observability.
   * Used to create proper span hierarchy even when steps are flattened.
   * Example: ["parentWorkflow", "childWorkflow"] for a step in a composed child workflow
   */
  workflowPath?: string[];
  /**
   * Original run function before validation wrapper was applied.
   * Used for versioning/hashing to detect actual implementation changes.
   * This is an internal field and should not be called directly - use `run` instead.
   * @internal
   */
  _originalRun?: (
    context: Context & AsyncStepRuntime,
    bag: Pick<Bag, Needs[number]>,
  ) => Promise<{ [K in Provides[number]]: Bag[K] }> | { [K in Provides[number]]: Bag[K] };
};

/**
 * Validates that a value is JSON-serializable for Temporal activity boundaries.
 * Returns detailed error information about non-serializable values.
 */
function validateSerializable(
  value: any,
  path: string = "root",
): { valid: true } | { valid: false; path: string; type: string; value: any } {
  // Handle primitives and special cases using a switch statement
  if (value === null || value === undefined) {
    return { valid: true };
  }

  const valueType = typeof value;
  switch (valueType) {
    case "string":
    case "number":
    case "boolean":
      return { valid: true };

    case "object":
      // Check for Date
      if (value instanceof Date) {
        return {
          valid: false,
          path,
          type: "Date",
          value,
        };
      }
      // Check for Map/Set - these serialize to {} and lose all data
      if (value instanceof Map) {
        return {
          valid: false,
          path,
          type: "Map",
          value: `Map with ${value.size} entries`,
        };
      }
      if (value instanceof Set) {
        return {
          valid: false,
          path,
          type: "Set",
          value: `Set with ${value.size} entries`,
        };
      }
      // Handle arrays
      if (Array.isArray(value)) {
        for (let i = 0; i < value.length; i++) {
          const result = validateSerializable(value[i], `${path}[${i}]`);
          if (!result.valid) {
            return result;
          }
        }
        return { valid: true };
      }
      // Handle plain objects
      for (const [key, val] of Object.entries(value)) {
        const result = validateSerializable(val, `${path}.${key}`);
        if (!result.valid) {
          return result;
        }
      }
      return { valid: true };

    default:
      // Unknown type
      return {
        valid: false,
        path,
        type: valueType,
        value: String(value).slice(0, 100),
      };
  }
}

/**
 * Runtime validation function that ensures step return objects contain exactly
 * the properties declared in the 'provides' array - no more, no less.
 *
 * This provides a safety net to catch:
 * 1. Excess properties that TypeScript's structural typing might allow through
 * 2. Missing properties when TypeScript is bypassed with 'as any'
 * 3. Non-serializable values that would fail when crossing Temporal boundaries
 *
 * The return type strips compile-time assertion wrappers (AssertSerializable, StrictStepReturn)
 * and returns the clean mapped type. This is safe because:
 * - If the input had non-serializable types, we'd have a compile error at the call site
 * - The runtime validation throws if the actual value doesn't match provides
 */
function validateStepReturn<Bag extends Record<string, any>, Keys extends keyof Bag>(
  result: AssertSerializable<StrictStepReturn<Bag, Keys>>,
  provides: readonly Keys[],
): { [K in Keys]: Bag[K] } {
  const expected = result as Record<string, unknown>;
  const returnKeys = Object.keys(expected);
  const providesKeys = provides.map(String);

  // Check for unexpected extra properties
  for (const key of returnKeys) {
    if (!providesKeys.includes(key)) {
      throw new Error(
        `Step returned unexpected property "${key}". Only properties declared in "provides" are allowed: [${providesKeys.join(", ")}]\n\n` +
          `💡 Fix: Either remove "${key}" from the return object, or add "${key}" to the "provides" array.`,
      );
    }
  }

  // Check for missing required properties
  for (const key of providesKeys) {
    if (!(key in expected)) {
      throw new Error(
        `Step failed to return required property "${key}". All properties declared in "provides" must be returned: [${providesKeys.join(", ")}]\n\n` +
          `💡 Fix: Make sure your step returns an object containing "${key}".`,
      );
    }
  }

  // Validate serializability (critical for async Temporal workflows)
  const serializationResult = validateSerializable(expected);
  if (!serializationResult.valid) {
    throw new Error(
      `Step returned non-serializable value at path "${serializationResult.path}".\n` +
        `Type: ${serializationResult.type}\n` +
        `Value: ${JSON.stringify(serializationResult.value, null, 2)}\n\n` +
        `💡 Fix: Use toSerializable() to convert values before returning from steps.\n` +
        `   All values must be JSON-serializable because they cross Temporal activity boundaries.\n\n` +
        `   - Date objects: toSerializable() converts these to ISO strings automatically\n` +
        `   - MikroORM entities: toSerializable() handles Refs, Collections, and getters\n` +
        `   - Map/Set: Convert to objects or arrays first (Object.fromEntries() / Array.from())\n` +
        `   - Functions: Return their results, not the functions themselves\n` +
        `   - BigInt: Convert to number or string first`,
    );
  }

  // Cast is safe: we've validated the object matches the provides array exactly
  return expected as { [K in Keys]: Bag[K] };
}

// ============================================================================
// COMPILE-TIME SERIALIZABILITY VALIDATION
// ============================================================================

/**
 * Type that represents valid JSON primitive values
 */
type JSONPrimitive = string | number | boolean | null;

/**
 * Type representing non-serializable types that should be caught at compile time.
 * Must stay in sync with the runtime check in validateSerializable() above.
 */
type NonSerializableType =
  | Date
  | Map<any, any>
  | Set<any>
  | ((...args: any[]) => any)
  | symbol
  | bigint;

/**
 * Depth-limited serializability checking using tuple-based recursion.
 *
 * Uses a depth counter pattern to avoid TypeScript's "excessive stack depth" error
 * while still catching nested non-serializable types up to 4 levels deep.
 *
 * This catches patterns like:
 * - { foo: Date }                    (Level 1)
 * - { foo: { bar: Date } }           (Level 2)
 * - { foo: { bar: { baz: Date } } }  (Level 3)
 * - Arrays at any level: [{ nested: { date: Date } }]
 *
 * Runtime validateSerializable() provides comprehensive checking for deeper nesting.
 */

/** Depth decrement: Decr[D] = D-1, with Decr[0] = never as stop signal. */
type Decr = [never, 0, 1, 2];

/** Depth increment for NonSerializableProp search. */
type Incr = [1, 2, 3, 4];

/**
 * Checks if a type is JSON-serializable up to D levels deep.
 *
 * Depth behavior:
 * - D=0: Only checks if T itself is a primitive or NonSerializableType
 * - D=1: Checks T and its direct children
 * - D=2: Checks T, children, and grandchildren
 * - D=3: Checks 4 levels deep (default)
 */
type IsSerializable<T, D extends number = 3> = T extends JSONPrimitive | undefined
  ? true
  : T extends NonSerializableType
    ? false
    : D extends 0
      ? true
      : T extends Array<infer U>
        ? IsSerializable<U, Decr[D]>
        : T extends object
          ? false extends IsSerializable<T[keyof T], Decr[D]>
            ? false
            : true
          : true;

// ============================================================================
// ERROR MESSAGE GENERATION - Find which property is non-serializable
// ============================================================================

/**
 * Find a top-level property that fails serialization at depth D.
 * D=0 checks for directly non-serializable types; D>0 uses IsSerializable.
 */
type FindPropFailingAt<T, D extends number> = T extends object
  ? D extends 0
    ? { [K in keyof T]: T[K] extends NonSerializableType ? K : never }[keyof T]
    : { [K in keyof T]: IsSerializable<T[K], Decr[D]> extends false ? K : never }[keyof T]
  : never;

/**
 * Find a top-level property containing non-serializable data.
 * Searches from shallowest (D=0) to deepest (D=3) to find the most relevant property.
 */
type NonSerializableProp<T, D extends number = 0> = T extends object
  ? D extends 4
    ? never
    : FindPropFailingAt<T, D> extends never
      ? NonSerializableProp<T, Incr[D]>
      : FindPropFailingAt<T, D>
  : never;

/**
 * Error type that includes the offending property name in the error message.
 */
type SerializationErrorWithProp<PropName> = PropName extends string
  ? {
      [K in `SERIALIZATION_ERROR: Property '${PropName}' contains non-serializable type (Date/Map/Set/Function/Symbol/BigInt). Use toSerializable()`]: never;
    }
  : {
      "SERIALIZATION_ERROR: Contains non-serializable type. Use toSerializable()": never;
    };

/**
 * Type assertion that produces a compile error if T contains non-serializable types.
 * Returns T unchanged if serializable, otherwise returns an error type with the property name.
 *
 * This provides compile-time feedback with a clear, actionable error message.
 */
export type AssertSerializable<T> =
  IsSerializable<T> extends true ? T : SerializationErrorWithProp<NonSerializableProp<T>>;

/**
 * COMPILE-TIME vs RUNTIME VALIDATION:
 *
 * What we CAN catch at compile-time:
 * ✅ Properties that exist in the Bag type but are not in the 'provides' array
 *    Example: If Bag has 'foo' but provides is ['rules'], returning { rules, foo } will error
 *
 * What we CANNOT catch at compile-time (TypeScript limitation):
 * ❌ Properties that don't exist in the Bag type at all
 *    Example: If 'baz' is not in Bag, returning { priceCents, baz } will NOT error at compile time
 *    This is due to TypeScript's structural typing allowing extra properties in object literals
 *
 * Solution: We use a hybrid approach:
 * 1. Compile-time checking for properties in Bag (StrictStepReturn type)
 * 2. Runtime validation for ALL excess properties (validateStepReturn function)
 */

// Create error messages that include the property name for clarity
type PropertyNotInProvidesError<K extends PropertyKey> =
  `❌ ERROR: Property '${K extends string ? K : string}' is not in the 'provides' array. Either remove it from the return value or add it to 'provides'.`;

// Type that creates better error messages for properties not in provides
export type StrictStepReturn<Bag, Keys extends keyof Bag> = {
  [K in Keys]: Bag[K];
} & {
  [K in keyof Bag as K extends Keys ? never : K]?: PropertyNotInProvidesError<K>;
};

/**
 * Factory function for creating type-safe steps with validation.
 *
 * This function provides:
 * 1. Clean generic inference - you don't need to specify types manually
 * 2. Compile-time validation via StrictStepReturn type
 * 3. Runtime validation wrapper for additional safety
 *
 * Usage:
 *   const myStep = step<Bag, MyContext>()({
 *     name: "processData",
 *     needs: ["input"],
 *     provides: ["output"],
 *     run: async (context, bag) => ({ output: processInput(bag.input) })
 *   });
 *
 * The double function call pattern allows TypeScript to infer the Bag type
 * from the first call, then infer Needs/Provides from the step definition.
 */
export function step<Bag extends Record<string, any>, Context = unknown>() {
  return <
    const Name extends string,
    const Needs extends readonly (keyof Bag)[],
    const Provides extends readonly (keyof Bag)[],
  >(stepDefinition: {
    name: Name;
    needs: Needs;
    provides: Provides;
    workerProfile?: WorkerProfile;
    asyncStartToCloseTimeout?: DurationString;
    asyncHeartbeatTimeout?: DurationString;
    asyncRetry?: StepRetryPolicy;
    run: (
      context: Context & AsyncStepRuntime,
      bag: Pick<Bag, Needs[number]>,
    ) =>
      | Promise<AssertSerializable<StrictStepReturn<Bag, Provides[number]>>>
      | AssertSerializable<StrictStepReturn<Bag, Provides[number]>>;
  }): Step<Bag, Needs, Provides, Context, Name> => {
    const originalRun = stepDefinition.run;
    const wrappedRun = async (
      context: Context & AsyncStepRuntime,
      bag: Pick<Bag, Needs[number]>,
    ) => {
      const result = await originalRun(context, bag);
      return validateStepReturn<Bag, Provides[number]>(result, stepDefinition.provides);
    };

    return {
      ...stepDefinition,
      run: wrappedRun,
      // Cast needed: originalRun's return type includes compile-time assertion wrappers
      // (AssertSerializable, StrictStepReturn) but _originalRun expects the clean type.
      // The wrappers only produce errors at compile-time; at runtime it's always the valid type.
      _originalRun: originalRun as Step<Bag, Needs, Provides, Context, Name>["_originalRun"],
    };
  };
}
