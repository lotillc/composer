# @lotiai/composer

[![CI](https://github.com/lotillc/composer/actions/workflows/ci.yml/badge.svg)](https://github.com/lotillc/composer/actions/workflows/ci.yml)
[![npm version](https://img.shields.io/npm/v/@lotiai/composer.svg)](https://www.npmjs.com/package/@lotiai/composer)

A framework for building type-safe, DAG-based workflows with optional Temporal integration.

Composer lets you define steps with explicit inputs and outputs, compose them into workflows with automatic dependency resolution and parallel execution, and run them synchronously (in-process) or asynchronously (via [Temporal](https://temporal.io/)).

> **Status:** pre-1.0. The public API may change between minor versions until 1.0.

## Installation

```bash
pnpm add @lotiai/composer
# or
npm install @lotiai/composer
```

## Core Concepts

### Bag

The **bag** is a typed object that flows through a workflow. Each step declares which fields it reads from the bag (`needs`) and which fields it writes (`provides`). The bag type is defined once and shared across all steps and workflows.

```typescript
interface MyBag {
  userId: string;
  user: { name: string; email: string };
  greeting: string;
}
```

### Step

A **step** is a single unit of work. Steps declare their dependencies and outputs, and the framework validates them at both compile-time and runtime.

```typescript
import { step } from "@lotiai/composer";

// The double function call pattern lets TypeScript infer Needs/Provides from the definition.
// The first call binds Bag and Context; the second binds the step shape.
export const fetchUser = step<MyBag, MyContext>()({
  name: "fetchUser",
  needs: ["userId"],
  provides: ["user"],
  run: async (ctx, bag) => {
    const user = await ctx.db.findUser(bag.userId);
    return { user };
  },
});

export const greet = step<MyBag, MyContext>()({
  name: "greet",
  needs: ["user"],
  provides: ["greeting"],
  run: async (_ctx, bag) => {
    return { greeting: `Hello, ${bag.user.name}!` };
  },
});
```

**Type safety guarantees:**

- Steps can only access fields declared in `needs`
- Steps must return exactly the fields declared in `provides`
- Return values must be JSON-serializable (enforced at compile-time and runtime)
- Excess or missing return properties are caught at both compile-time and runtime

### Workflow

A **workflow** is a collection of steps with automatic dependency resolution. The framework builds a DAG, plans parallel execution batches, and validates all dependencies at compile-time.

```typescript
import { createWorkflow } from "@lotiai/composer";

// Workflow without required initial data
const simpleWorkflow = createWorkflow<MyBag>("simple")
  .build([fetchUser, greet]);

// Workflow requiring initial data at runtime
const greetWorkflow = createWorkflow<MyBag>("greet-user")
  .requires("userId")
  .build([fetchUser, greet]);

// Workflow with pre-configured values
const configuredWorkflow = createWorkflow<MyBag>("configured")
  .configure({ userId: "default-user" })
  .build([fetchUser, greet]);

// Configure + require (configure provides defaults, requires demands runtime values)
const hybridWorkflow = createWorkflow<MyBag>("hybrid")
  .configure({ someDefault: "value" })
  .requires("userId")
  .build([fetchUser, greet]);
```

**Compile-time dependency validation:** If a step needs a field that no prior step provides and it's not in initial/configured data, you get a clear compile error:

```
"WORKFLOW ERROR: A step needs user but it is not available. Available fields: [userId]
 -> FIX: Add a step that provides user before the step that needs it"
```

### Workflow Composition

Workflows can be composed into other workflows using the `use()` helper. Child workflow steps are flattened into the parent's DAG, enabling maximum parallelization while maintaining logical boundaries for observability.

```typescript
import { createWorkflow, use } from "@lotiai/composer";

const authWorkflow = createWorkflow<MyBag>("auth")
  .requires("userId")
  .build([validateUser, checkPermissions]);

const mainWorkflow = createWorkflow<MyBag>("main")
  .requires("userId")
  .build([
    fetchData,
    use(authWorkflow),   // Steps are flattened into parent DAG
    processRequest,
  ]);
```

Composed workflows:
- Flatten steps for maximum parallelization across workflow boundaries
- Auto-namespace step names (e.g. `auth.validateUser`) to prevent conflicts
- Preserve observability via SubWorkflow spans in traces
- Support arbitrary nesting depth

### Execution Model

Workflows execute using batch-based parallelism:

1. **Dependency analysis** -- topological sort builds a DAG
2. **Batch formation** -- groups steps that can run simultaneously
3. **Parallel execution** -- each batch runs with `Promise.allSettled`
4. **Progressive merge** -- successful outputs merge into the bag even if sibling steps fail

```
Given: stepA(needs:[]) -> stepB(needs:["x"]) -> stepC(needs:["x"]) -> stepD(needs:["y","z"])

Batch 1: [stepA]           -- sequential (no deps)
Batch 2: [stepB, stepC]    -- parallel (both need only "x")
Batch 3: [stepD]           -- sequential (needs "y" and "z")
```

## Usage

### Creating a Composer

The `createComposer` function creates a configured instance that manages context lifecycle and execution:

```typescript
import { createComposer } from "@lotiai/composer";

// Sync-only composer (no Temporal config)
const syncComposer = createComposer({
  contextProvider: {
    beforeStep: async (stepName) => ({
      db: await getDbConnection(),
    }),
    afterStep: async (ctx, error) => {
      if (!error) await ctx.db.flush();
      ctx.db.release();
    },
  },
});

// Full composer with Temporal support
const composer = createComposer({
  contextProvider: myContextProvider,
  temporal: {
    serverAddress: "localhost:7233",
    namespace: "my-namespace",
  },
  // Optional settings:
  logger: myPinoLogger,  // defaults to console
  deepFreeze: true,      // freeze step outputs for immutability protection
});
```

### Running Workflows

```typescript
// Synchronous execution (in-process)
const { bag, error } = await composer.runSyncWorkflow(greetWorkflow, { userId: "u_123" });

if (error) {
  console.error("Workflow failed:", error);
} else {
  console.log(bag.greeting); // "Hello, Alice!"
}

// Asynchronous execution (via Temporal) -- requires temporal config
const { bag: asyncBag, error: asyncError } = await composer.runAsyncWorkflow(greetWorkflow, { userId: "u_123" });
```

Workflows never throw. Errors are returned in `error`, and `bag` always contains any data produced by successful steps.

### Checkpoints (Async Only)

Checkpoints allow async workflows to return partial results early while continuing execution in the background:

```typescript
const workflow = createWorkflow<MyBag>("with-checkpoint")
  .requires("userId")
  .build([persistStep, processStep, finalizeStep])
  .checkpoint("persisted", { afterStep: persistStep })
  .checkpoint("processed", { afterStep: processStep, timeout: 60000 });

// Get partial result after persistStep completes
const { bag, error } = await composer.runAsyncWorkflow(workflow, data, {
  awaitCheckpoint: "persisted",
});
// bag contains outputs up through the batch containing persistStep
// The workflow continues running in the background
```

Checkpoint names are validated at compile-time -- using a name that doesn't exist on the workflow produces a type error.

### Error Handling

Attach an error handler to workflows for domain-specific error recovery:

```typescript
const workflow = createWorkflow<MyBag>("resilient")
  .requires("userId")
  .build([fetchUser, processData])
  .onError(async (ctx, bag, error) => {
    // Check error type
    if (isExpectedError(error)) {
      bag.result = getFallbackResult();
      return undefined; // Error handled, error will be undefined in the result
    }
    return error; // Unknown error, propagate as-is
  });
```

Error handler return values:
- `undefined` -- error fully handled, `error` will be `undefined` in the result
- `Error` -- propagate or transform the error into the result's `error`
- Throwing -- `error` will be a `WorkflowErrorHandlerFailure` wrapping both the original error and the handler error

### Context Provider

The context provider manages per-step resources (e.g. database connections, loggers). It is configured once via `createComposer` and used automatically for every step execution.

```typescript
import { type StepContextProvider } from "@lotiai/composer";

interface MyContext {
  em: SqlEntityManager;
}

const contextProvider: StepContextProvider<MyContext> = {
  beforeStep: async (stepName) => ({
    em: await DatabaseConnection.getInstance().getForkedEntityManager(),
  }),
  afterStep: async (ctx, error) => {
    if (!error) await ctx.em.flush();
    ctx.em.clear();
  },
};
```

### Custom Logger

Composer accepts any logger with `info`, `warn`, `error`, and `debug` methods. Compatible with `console`, `pino`, `winston`, and most logging libraries.

```typescript
import pino from "pino";

const composer = createComposer({
  contextProvider,
  logger: {
    info: (msg, meta) => pino().info(meta, msg),
    warn: (msg, meta) => pino().warn(meta, msg),
    error: (msg, meta) => pino().error(meta, msg),
    debug: (msg, meta) => pino().debug(meta, msg),
  },
});
```

## CLI

The package includes a CLI for building and managing Temporal workflow/activity bundles. It reads configuration from a `composer.build-config.ts` file in your package root.

```bash
npx @lotiai/composer build                  # Build bundles (fast unversioned flow)
npx @lotiai/composer build --git-hash=abc   # Versioned build (vendor bundle + version copies + manifest)
npx @lotiai/composer validate               # Validate config and definitions
npx @lotiai/composer dev                    # Watch mode: compile, bundle, restart workers on changes
npx @lotiai/composer dev --git-hash=abc     # Watch mode with versioned bundle flow
npx @lotiai/composer dev:clean              # Remove versioned bundle artifacts
npx @lotiai/composer dev:up                 # Start Temporal dev server (PostgreSQL + Temporal + UI)
npx @lotiai/composer dev:down               # Stop Temporal dev server
npx @lotiai/composer merge --git-hash=abc   # Merge versioned bundles into combined bundles for workers
npx @lotiai/composer upload --git-hash=abc --type=versions  # Upload version bundles to S3
npx @lotiai/composer upload --git-hash=abc --type=merged    # Upload merged bundles to S3
npx @lotiai/composer cleanup                # Remove old versioned bundles (retention policy)
```

### Output Directory

The CLI resolves the output directory automatically from your `tsconfig.json`'s `compilerOptions.outDir`. If `outDir` is not set in the `tsconfig.json` (sibling file to `composer.build-config.ts`), it falls back to `"dist"`. This means bundle output (workflow.js, activity.js, vendor bundles, version copies) is placed alongside your compiled TypeScript output without any extra configuration.

### Build Configuration File

Create a `composer.build-config.ts` in your package root:

```typescript
import { defineBuildConfig } from "@lotiai/composer/build-config";

export default defineBuildConfig({
  // Required: directories containing step and workflow definitions
  stepsDir: "src/steps",
  workflowsDir: "src/workflows",

  // Optional: S3 config for bundle storage (required for upload/merge/cleanup commands)
  s3: {
    bucketName: "composer-bundles",
    // Optional: provide a custom S3Client (e.g. for LocalStack or custom credentials)
    // customClient: new S3Client({ region: "us-east-1", endpoint: "http://localhost:4566", forcePathStyle: true }),
  },

  // Optional: override worker profile defaults
  workerProfiles: {
    standard: {
      cpu: 1024,
      memory: 4096,
      maxConcurrentActivities: 20,
    },
  },

  // Optional: build settings
  build: {
    minify: true, // default: true
  },

  // Optional: dev command settings
  dev: {
    startAllWorkersJsScript: "dist/scripts/start-all-workers.js",
    // watchPatterns: ["src/**/*.ts"],  // default: ["src/**/*.ts"]
  },
});
```

Note: Temporal connection config (`serverAddress`, `namespace`) is passed to `createComposer()` at runtime, not in `composer.build-config.ts`. The config file is for build and tooling concerns only.

### Monorepo Consumers and Vendor Bundles

When using versioned builds (`--git-hash`), the CLI creates a **vendor bundle** containing shared npm dependencies. In a monorepo, you typically want to exclude your own workspace packages from the vendor bundle (they contain business logic, not shared vendor deps) while still including their transitive npm dependencies.

The CLI auto-detects which packages to exclude by looking for dependencies declared with pnpm's `workspace:` protocol in your `package.json`:

```json
{
  "dependencies": {
    "@myorg/shared-lib": "workspace:*",
    "@myorg/db": "workspace:*",
    "es-toolkit": "^1.43.0",
    "uuid": "^11.1.0"
  }
}
```

In this example, `@myorg/shared-lib` and `@myorg/db` are excluded from the vendor bundle, but their transitive npm dependencies are still discovered and included. `es-toolkit` and `uuid` are included directly. Dependencies using pnpm's `catalog:` protocol are treated as normal npm dependencies and included in the vendor bundle.

## Starting Temporal Workers

For production or local development with Temporal, start workers using the helper functions:

```typescript
import { startAllWorkers } from "@lotiai/composer";
import { composer } from "./my-app-composer";

// Start both workflow and activity workers in one process (local dev)
await startAllWorkers(composer, {
  workflow: {
    taskQueues: ["workflow-tasks"],
    maxConcurrentWorkflowTaskExecutions: 100,
  },
  activity: {
    taskQueues: ["standard-tasks"],
    maxConcurrentActivityTaskExecutions: 15,
  },
});
```

For production, run workflow and activity workers in separate processes for independent scaling:

```typescript
import { startWorkflowWorker, startActivityWorker } from "@lotiai/composer";

// In workflow worker process:
await startWorkflowWorker(composer, {
  taskQueues: ["workflow-tasks"],
  maxConcurrentWorkflowTaskExecutions: 100,
});

// In activity worker process:
await startActivityWorker(composer, {
  taskQueues: ["standard-tasks"],
  maxConcurrentActivityTaskExecutions: 15,
});
```

## Worker Profiles

Steps can declare a `workerProfile` to control which Temporal worker pool they run on. Currently, there is a single `"standard"` profile:

| Profile    | Task Queue       | CPU     | Memory | Concurrent Activities |
|------------|------------------|---------|--------|-----------------------|
| `standard` | standard-tasks   | 0.5 vCPU| 2 GB   | 15                    |

```typescript
export const heavyStep = step<MyBag, MyContext>()({
  name: "heavyComputation",
  needs: ["input"],
  provides: ["output"],
  workerProfile: "standard", // default if omitted
  run: async (ctx, bag) => { /* ... */ },
});
```

Defaults can be overridden per-deployment via `workerProfiles` in `composer.build-config.ts`.

## Exports

### Main entry point (`@lotiai/composer`)

| Export | Description |
|--------|-------------|
| `createComposer` | Create a configured Composer instance |
| `createWorkflow` | Create a workflow with compile-time dependency validation |
| `step` | Factory for creating type-safe steps |
| `use` | Compose a child workflow into a parent |
| `defineBuildConfig` | Type-safe build config helper (also available from `@lotiai/composer/build-config`) |
| `startAllWorkers` | Start both Temporal workers in one process |
| `startWorkflowWorker` | Start a Temporal workflow worker |
| `startActivityWorker` | Start a Temporal activity worker |
| `getAllTaskQueues` | Get all configured task queue names |
| `getTaskQueueForProfile` | Get the task queue for a worker profile |
| `WorkflowStepError` | Error class for step failures |
| `WorkflowBatchError` | Error class for batch failures |
| `WorkflowErrorHandlerFailure` | Error class for error handler failures |

### Build config entry point (`@lotiai/composer/build-config`)

Build configuration schema, loader, and `defineBuildConfig` helper for `composer.build-config.ts` files.

## Deep Freeze (Immutability Protection)

Enable `deepFreeze: true` in `createComposer` to freeze all step outputs before merging them into the bag. This catches mutation bugs that can cause non-deterministic behavior:

- Parallel batch mutations (two steps mutating shared references)
- Downstream mutations (a step modifying data from a previous step)
- Post-return mutations (keeping a reference and mutating it later)

Recommended for development and testing. The overhead (~0.5--10ms per output) is negligible for I/O-bound workflows.
