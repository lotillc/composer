/**
 * Workflow Execution Integration for Temporal
 *
 * This module provides the integration layer between the synchronous workflow API
 * (runWorkflow) and the Temporal async execution runtime. It bridges the gap by:
 * - Converting sync workflow definitions to Temporal inputs
 * - Integrating with the observability system (metrics/tracing)
 * - Managing workflow lifecycle (start/success/failure)
 * - Supporting early returns via checkpoints
 *
 * This is the glue layer that makes Temporal execution transparent to users of
 * the synchronous runWorkflow() API.
 *
 * @module workflow-execution
 */

import { ApplicationFailure, WorkflowFailedError } from "@temporalio/client";
import type { VersioningOverride } from "@temporalio/common";
import {
  DEFAULT_CHECKPOINT_TIMEOUT_MS,
  type Workflow,
  type WorkflowResult,
} from "../../dag-sync-workflow";
import type { UUIDV7 } from "../../types";
import type { TemporalWorkflowResult } from "../build/workflow-factory";
import { awaitCheckpointUpdate } from "../build/workflow-factory";
import { createTemporalClient, executeWorkflow, executeWorkflowAndWait } from "./temporal-client";

/**
 * Options for Temporal workflow execution.
 */
export interface TemporalExecutionOptions {
  workflowId: UUIDV7;

  /**
   * Temporal server connection configuration.
   */
  clientConfig: {
    address: string;
    namespace: string;
  };

  /**
   * Versioning override for Worker Versioning.
   */
  versioningOverride?: VersioningOverride;

  /**
   * Optional checkpoint name to await.
   * If specified, returns partial bag state when checkpoint is reached
   * instead of waiting for full workflow completion.
   *
   * The checkpoint must exist in the workflow definition.
   * Timeout is determined by the checkpoint's configured timeout
   * or DEFAULT_CHECKPOINT_TIMEOUT_MS (30s).
   */
  awaitCheckpoint?: string;

  /**
   * Start the workflow and return as soon as Temporal acknowledges execution.
   *
   * Unlike awaitCheckpoint, this does not wait for any workflow progress beyond
   * successful handoff to Temporal.
   */
  startOnly?: boolean;
}

/**
 * Executes a workflow using Temporal runtime.
 *
 * This function:
 * 1. Creates a WorkflowInput from the initial data
 * 2. Calls the Temporal client to execute the workflow
 * 3. Returns WorkflowResult with bag and optional error (never throws)
 *
 * ## Checkpoints:
 *
 * If `options.awaitCheckpoint` is specified, the function returns early when
 * that checkpoint is reached, providing partial results while the workflow
 * continues executing in the background. Useful for fast HTTP responses.
 *
 * ## Observability:
 *
 * Temporal workflows rely on Temporal's built-in observability (metrics, traces, logs).
 * Activity-level observability (spans, metrics) should be added in the activity workers.
 * For now, use Temporal UI and Prometheus metrics for workflow monitoring.
 * Cross-span correlation is handled automatically via OpenTelemetry trace context propagation.
 *
 * @param wf - The workflow definition to execute
 * @param initialData - Initial data for the workflow
 * @param options - Temporal execution options
 * @returns WorkflowResult with bag and optional error
 *
 * @internal
 */
export async function executeWorkflowTemporal<Bag extends Record<string, any>>(
  wf: Workflow<Bag, any, any>,
  initialData: Partial<Bag>,
  options: TemporalExecutionOptions,
): Promise<WorkflowResult<Bag>> {
  // Merge configured values with initial data (like sync path does)
  // This ensures workflows that use .configure() work the same in both modes
  const mergedInitialData = {
    ...initialData,
    ...(wf.configuredValues ?? {}),
  } as Partial<Bag>;

  try {
    if (options.startOnly && options.awaitCheckpoint) {
      throw new Error("Cannot use startOnly and awaitCheckpoint together");
    }

    // Validate checkpoint name and resolve timeout if awaitCheckpoint is specified
    let checkpointTimeoutMs: number | undefined;
    if (options.awaitCheckpoint) {
      const checkpoint = wf.checkpoints?.find((c) => c.name === options.awaitCheckpoint);
      if (!checkpoint) {
        const validNames = wf.checkpoints?.map((c) => c.name).join(", ") ?? "none";
        throw new Error(
          `Unknown checkpoint "${options.awaitCheckpoint}" for workflow "${wf.name}". ` +
            `Valid checkpoints: [${validNames}]`,
        );
      }
      checkpointTimeoutMs = checkpoint.timeout ?? DEFAULT_CHECKPOINT_TIMEOUT_MS;
    }

    const clientConfig = options.clientConfig;

    const workflowInput = {
      initialData: mergedInitialData,
    };

    if (options.startOnly) {
      await executeWorkflow({
        workflowName: wf.name,
        workflowId: options.workflowId,
        input: workflowInput,
        taskQueue: "workflow-tasks",
        clientConfig,
      });

      return { bag: mergedInitialData as Bag, error: undefined };
    }

    if (options.awaitCheckpoint) {
      // Start the workflow (don't wait for completion)
      await executeWorkflow({
        workflowName: wf.name,
        workflowId: options.workflowId,
        input: workflowInput,
        taskQueue: "workflow-tasks",
        clientConfig,
        versioningOverride: options.versioningOverride,
      });

      // Use Temporal Update to wait for checkpoint (no polling!)
      // The workflow's update handler blocks until the checkpoint is reached
      const client = await createTemporalClient(clientConfig);
      const typedHandle = client.workflow.getHandle(options.workflowId);

      // Race the update against the configured checkpoint timeout so callers
      // get a timely error instead of blocking indefinitely when the workflow
      // never reaches the checkpoint (e.g. a step hangs or fails earlier).
      const updatePromise = typedHandle.executeUpdate(awaitCheckpointUpdate, {
        args: [{ checkpointName: options.awaitCheckpoint }],
      });

      const partialBag = await new Promise<Record<string, unknown>>((resolve, reject) => {
        const timeoutId = setTimeout(() => {
          reject(
            new Error(
              `Checkpoint "${options.awaitCheckpoint}" for workflow "${wf.name}" ` +
                `timed out after ${checkpointTimeoutMs}ms`,
            ),
          );
        }, checkpointTimeoutMs);

        updatePromise.then(
          (result) => {
            clearTimeout(timeoutId);
            resolve(result);
          },
          (err) => {
            clearTimeout(timeoutId);
            reject(err);
          },
        );
      });

      return { bag: partialBag as Bag, error: undefined };
    }

    // Wait for full workflow completion (existing behavior)
    const result = await executeWorkflowAndWait({
      workflowName: wf.name,
      workflowId: options.workflowId,
      input: workflowInput,
      taskQueue: "workflow-tasks",
      clientConfig,
    });

    return result as WorkflowResult<Bag>;
  } catch (err) {
    // Temporal throws WorkflowFailedError when a workflow exits via ApplicationFailure.
    // We pack the bag and structured error into ApplicationFailure.details so we can
    // reconstruct a WorkflowResult here instead of losing the partial bag state.
    if (err instanceof WorkflowFailedError && err.cause instanceof ApplicationFailure) {
      const detail = err.cause.details?.[0] as
        | { bag?: Record<string, unknown>; error?: TemporalWorkflowResult["error"] }
        | undefined;
      if (detail) {
        const errorInfo = detail.error;
        const error = errorInfo
          ? Object.assign(new Error(errorInfo.message), {
              code: errorInfo.code,
              type: errorInfo.type,
              batchNumber: errorInfo.batchNumber,
              errors: errorInfo.errors,
            })
          : new Error(err.message);
        return { bag: (detail.bag ?? mergedInitialData) as Bag, error };
      }
    }
    const error = err instanceof Error ? err : new Error(String(err));
    return { bag: mergedInitialData as Bag, error };
  }
}

/**
 * Starts a workflow on Temporal without waiting for completion (fire-and-forget).
 *
 * Returns the workflowId immediately. The workflow executes in the background
 * on Temporal workers. Use this for 202 Accepted HTTP responses.
 *
 * @param wf - The workflow definition to execute
 * @param initialData - Initial data for the workflow
 * @param options - Temporal execution options
 * @returns The workflowId of the started workflow
 *
 * @internal
 */
export async function startWorkflowTemporal<Bag extends Record<string, any>>(
  wf: Workflow<Bag, any, any>,
  initialData: Partial<Bag>,
  options: Pick<TemporalExecutionOptions, "workflowId" | "clientConfig" | "versioningOverride">,
): Promise<{ workflowId: string }> {
  // Merge configured values with initial data (like sync path does)
  const mergedInitialData = {
    ...initialData,
    ...(wf.configuredValues ?? {}),
  } as Partial<Bag>;

  const workflowInput = {
    initialData: mergedInitialData,
  };

  // Use unversioned workflow name -- Temporal Worker Versioning routes to the
  // correct worker deployment via workerDeploymentOptions.buildId, not the
  // workflow type name.
  await executeWorkflow({
    workflowName: wf.name,
    workflowId: options.workflowId,
    input: workflowInput,
    taskQueue: "workflow-tasks",
    clientConfig: options.clientConfig,
    versioningOverride: options.versioningOverride,
  });

  return { workflowId: options.workflowId };
}
