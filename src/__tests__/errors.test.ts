import { createWorkflow, step } from "../internal";
import {
  matchesError,
  WorkflowBatchError,
  WorkflowErrorHandlerFailure,
  WorkflowStepError,
} from "../internal/errors";

function makeErrorClass(code: string) {
  return class extends Error {
    static readonly code = code;
    readonly code = code;
  };
}

describe("WorkflowStepError", () => {
  const createStepError = (originalError: Error) =>
    new WorkflowStepError({
      workflowId: "test-workflow",
      stepName: "testStep",
      batchNumber: 1,
      originalError,
      bagState: { foo: "bar" },
    });

  it("has static code property", () => {
    expect(WorkflowStepError.code).toBe("WORKFLOW_STEP_ERROR");
  });

  it("uses originalError.code when present", () => {
    const CustomError = makeErrorClass("CUSTOM_ERROR");
    const originalError = new CustomError("Something failed");
    const stepError = createStepError(originalError);

    expect(stepError.code).toBe("CUSTOM_ERROR");
  });

  it("uses default code when originalError has no code", () => {
    const originalError = new Error("Something failed");
    const stepError = createStepError(originalError);

    expect(stepError.code).toBe("WORKFLOW_STEP_ERROR");
  });

  it("works with matchesError by code string", () => {
    const CustomError = makeErrorClass("CUSTOM_ERROR");
    const stepError = createStepError(new CustomError("test"));

    expect(matchesError(stepError, "CUSTOM_ERROR")).toBe(true);
    expect(matchesError(stepError, "OTHER_ERROR")).toBe(false);
  });

  it("works with matchesError by error class", () => {
    const CustomError = makeErrorClass("CUSTOM_ERROR");
    const stepError = createStepError(new CustomError("test"));

    expect(matchesError(stepError, CustomError)).toBe(true);
  });

  it("works with matchesError for default code", () => {
    const stepError = createStepError(new Error("plain error"));

    expect(matchesError(stepError, WorkflowStepError)).toBe(true);
    expect(matchesError(stepError, "WORKFLOW_STEP_ERROR")).toBe(true);
  });

  it("includes code in toLogContext", () => {
    const CustomError = makeErrorClass("CUSTOM_ERROR");
    const stepError = createStepError(new CustomError("test"));

    const context = stepError.toLogContext();
    expect(context.code).toBe("CUSTOM_ERROR");
    expect(context.originalError.code).toBe("CUSTOM_ERROR");
  });
});

describe("WorkflowBatchError", () => {
  const createBatchError = (stepErrors: WorkflowStepError[]) =>
    new WorkflowBatchError({
      errors: stepErrors,
      bagState: { foo: "bar" },
      batchNumber: 2,
      workflowId: "test-workflow",
    });

  it("has static code property", () => {
    expect(WorkflowBatchError.code).toBe("WORKFLOW_BATCH_ERROR");
  });

  it("has instance code property", () => {
    const batchError = createBatchError([]);
    expect(batchError.code).toBe("WORKFLOW_BATCH_ERROR");
  });

  it("works with matchesError for batch code", () => {
    const batchError = createBatchError([]);

    expect(matchesError(batchError, WorkflowBatchError)).toBe(true);
    expect(matchesError(batchError, "WORKFLOW_BATCH_ERROR")).toBe(true);
  });

  it("works with matchesError to find errors inside batch", () => {
    const CustomError = makeErrorClass("CUSTOM_ERROR");
    const OtherError = makeErrorClass("OTHER_ERROR");

    const stepError1 = new WorkflowStepError({
      workflowId: "test",
      stepName: "step1",
      batchNumber: 1,
      originalError: new CustomError("first"),
      bagState: {},
    });
    const stepError2 = new WorkflowStepError({
      workflowId: "test",
      stepName: "step2",
      batchNumber: 1,
      originalError: new OtherError("second"),
      bagState: {},
    });

    const batchError = createBatchError([stepError1, stepError2]);

    expect(matchesError(batchError, CustomError)).toBe(true);
    expect(matchesError(batchError, OtherError)).toBe(true);
    expect(matchesError(batchError, "CUSTOM_ERROR")).toBe(true);
    expect(matchesError(batchError, "OTHER_ERROR")).toBe(true);
    expect(matchesError(batchError, "UNKNOWN_ERROR")).toBe(false);
  });

  it("creates descriptive message", () => {
    const stepError = new WorkflowStepError({
      workflowId: "test",
      stepName: "failingStep",
      batchNumber: 2,
      originalError: new Error("oops"),
      bagState: {},
    });
    const batchError = createBatchError([stepError]);

    expect(batchError.message).toContain("test-workflow");
    expect(batchError.message).toContain("batch 2");
    expect(batchError.message).toContain("1 step(s) failed");
    expect(batchError.message).toContain("failingStep");
  });

  it("includes all step errors in toLogContext", () => {
    const stepError1 = new WorkflowStepError({
      workflowId: "test",
      stepName: "step1",
      batchNumber: 2,
      originalError: new Error("first"),
      bagState: {},
    });
    const stepError2 = new WorkflowStepError({
      workflowId: "test",
      stepName: "step2",
      batchNumber: 2,
      originalError: new Error("second"),
      bagState: {},
    });

    const batchError = createBatchError([stepError1, stepError2]);
    const context = batchError.toLogContext();

    expect(context.code).toBe("WORKFLOW_BATCH_ERROR");
    expect(context.failedStepCount).toBe(2);
    expect(context.failedSteps).toHaveLength(2);
    expect(context.failedSteps[0].stepName).toBe("step1");
    expect(context.failedSteps[1].stepName).toBe("step2");
  });
});

describe("WorkflowErrorHandlerFailure", () => {
  const createHandlerFailure = (
    originalError: WorkflowStepError | WorkflowBatchError,
    handlerError: Error,
  ) =>
    new WorkflowErrorHandlerFailure({
      originalError,
      handlerError,
      workflowId: "test-workflow",
    });

  it("has static code property", () => {
    expect(WorkflowErrorHandlerFailure.code).toBe("WORKFLOW_ERROR_HANDLER_FAILURE");
  });

  it("has instance code property", () => {
    const stepError = new WorkflowStepError({
      workflowId: "test",
      stepName: "step1",
      batchNumber: 1,
      originalError: new Error("original"),
      bagState: {},
    });
    const failure = createHandlerFailure(stepError, new Error("handler failed"));

    expect(failure.code).toBe("WORKFLOW_ERROR_HANDLER_FAILURE");
  });

  it("works with matchesError", () => {
    const stepError = new WorkflowStepError({
      workflowId: "test",
      stepName: "step1",
      batchNumber: 1,
      originalError: new Error("original"),
      bagState: {},
    });
    const failure = createHandlerFailure(stepError, new Error("handler failed"));

    expect(matchesError(failure, WorkflowErrorHandlerFailure)).toBe(true);
    expect(matchesError(failure, "WORKFLOW_ERROR_HANDLER_FAILURE")).toBe(true);
  });

  it("preserves both original error and handler error", () => {
    const CustomError = makeErrorClass("CUSTOM_ERROR");
    const stepError = new WorkflowStepError({
      workflowId: "test",
      stepName: "step1",
      batchNumber: 1,
      originalError: new CustomError("original"),
      bagState: { foo: "bar" },
    });
    const handlerError = new Error("handler exploded");
    const failure = createHandlerFailure(stepError, handlerError);

    expect(failure.originalError).toBe(stepError);
    expect(failure.handlerError).toBe(handlerError);
    expect(failure.cause).toBe(handlerError);
  });

  it("creates descriptive message", () => {
    const stepError = new WorkflowStepError({
      workflowId: "test",
      stepName: "step1",
      batchNumber: 1,
      originalError: new Error("step failed"),
      bagState: {},
    });
    const failure = createHandlerFailure(stepError, new Error("handler died"));

    expect(failure.message).toContain("test-workflow");
    expect(failure.message).toContain("error handler failed");
    expect(failure.message).toContain("handler died");
    expect(failure.message).toContain("step failed");
  });

  it("includes full context in toLogContext", () => {
    const CustomError = makeErrorClass("CUSTOM_ERROR");
    const stepError = new WorkflowStepError({
      workflowId: "test",
      stepName: "failingStep",
      batchNumber: 1,
      originalError: new CustomError("original"),
      bagState: {},
    });
    const HandlerError = makeErrorClass("HANDLER_ERROR");
    const handlerError = new HandlerError("handler failed");
    const failure = createHandlerFailure(stepError, handlerError);

    const context = failure.toLogContext();
    expect(context.code).toBe("WORKFLOW_ERROR_HANDLER_FAILURE");
    expect(context.workflowId).toBe("test-workflow");
    expect(context.originalError.stepName).toBe("failingStep");
    expect(context.originalError.code).toBe("CUSTOM_ERROR");
    expect(context.handlerError.code).toBe("HANDLER_ERROR");
  });
});

describe("WorkflowBuilder.onError()", () => {
  type TestBag = {
    input: string;
    output?: string;
  };

  const testStep = step<TestBag>()({
    name: "testStep",
    needs: ["input"],
    provides: ["output"],
    run: async (_ctx, bag) => ({ output: bag.input.toUpperCase() }),
  });

  it("returns a Workflow with errorHandler when onError is called", () => {
    const handler = async () => undefined;
    const wf = createWorkflow<TestBag>("test-wf")
      .requires("input")
      .build([testStep])
      .onError(handler);

    expect(wf.name).toBe("test-wf");
    expect(wf.errorHandler).toBe(handler);
    expect(wf.steps).toHaveLength(1);
  });

  it("returns a WorkflowBuilder that can be used directly as Workflow", () => {
    const wfBuilder = createWorkflow<TestBag>("test-wf").requires("input").build([testStep]);

    // WorkflowBuilder has all Workflow properties
    expect(wfBuilder.name).toBe("test-wf");
    expect(wfBuilder.steps).toHaveLength(1);
    expect(wfBuilder.requiredInitial).toEqual(["input"]);

    // WorkflowBuilder has onError method
    expect(typeof wfBuilder.onError).toBe("function");
  });

  it("works with configure().requires() pattern", () => {
    type ConfigBag = TestBag & { config: string };

    const configStep = step<ConfigBag>()({
      name: "configStep",
      needs: ["config", "input"],
      provides: ["output"],
      run: async (_ctx, bag) => ({ output: `${bag.config}: ${bag.input}` }),
    });

    const handler = async () => undefined;
    const wf = createWorkflow<ConfigBag>("configured-wf")
      .configure({ config: "PREFIX" })
      .requires("input")
      .build([configStep])
      .onError(handler);

    expect(wf.name).toBe("configured-wf");
    expect(wf.errorHandler).toBe(handler);
    expect(wf.configuredValues).toEqual({ config: "PREFIX" });
  });

  it("works with requires().configure() pattern", () => {
    type ConfigBag = TestBag & { config: string };

    const configStep = step<ConfigBag>()({
      name: "configStep",
      needs: ["config", "input"],
      provides: ["output"],
      run: async (_ctx, bag) => ({ output: `${bag.config}: ${bag.input}` }),
    });

    const handler = async () => undefined;
    const wf = createWorkflow<ConfigBag>("configured-wf")
      .requires("input")
      .configure({ config: "PREFIX" })
      .build([configStep])
      .onError(handler);

    expect(wf.name).toBe("configured-wf");
    expect(wf.errorHandler).toBe(handler);
    expect(wf.requiredInitial).toEqual(["input"]);
    expect(wf.configuredValues).toEqual({ config: "PREFIX" });
  });
});
