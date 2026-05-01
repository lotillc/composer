/**
 * Temporal Client for Workflow Execution
 *
 * This module provides the client-side API for starting and managing
 * Temporal workflows. Use this from your application code (API handlers,
 * cron jobs, etc.) to initiate workflow executions.
 *
 * ## Usage:
 *
 * ```typescript
 * import { executeWorkflow } from "@lotiai/composer/internal/async/execute/temporal-client";
 *
 * // Start a workflow
 * const handle = await executeWorkflow({
 *   workflowName: "api-prompt-025b323f",  // Versioned name from workflow-bundle
 *   input: {
 *     initialData: { promptText: "Hello" },
 *     workflowId: "prompt-123",
 *     correlationId: "req-456",
 *   },
 *   taskQueue: "workflow-tasks",
 * });
 *
 * // Wait for result
 * const result = await handle.result();
 * ```
 *
 * @module temporal-client
 */

import type { WorkflowHandle } from "@temporalio/client";
import { Client, Connection } from "@temporalio/client";
import type { VersioningOverride } from "@temporalio/common";
import type { UUIDV7 } from "../../types";
import type { WorkflowInput } from "../build/workflow-factory";

/**
 * Configuration for connecting to Temporal Server.
 */
export interface TemporalClientConfig {
  /**
   * Temporal Server address
   */
  address: string;

  /**
   * Temporal namespace
   */
  namespace: string;
}

/**
 * Options for starting a workflow execution.
 */
export interface ExecuteWorkflowOptions {
  /**
   * Workflow name (e.g., "api-prompt")
   */
  workflowName: string;

  /**
   * Unique workflow execution ID.
   * Should be a meaningful business identifier or a generated UUID.
   * This ID is used for idempotency and workflow reconnection.
   */
  workflowId: UUIDV7;

  /**
   * Input data for the workflow
   */
  input: WorkflowInput;

  /**
   * Task queue to execute workflow on
   * @default "workflow-tasks"
   */
  taskQueue?: string;

  /**
   * Versioning override for Worker Versioning. When set, pins the workflow
   * to a specific deployment version (buildId + deploymentName).
   */
  versioningOverride?: VersioningOverride;

  /**
   * Override Temporal client config for this execution
   */
  clientConfig: TemporalClientConfig;
}

const cachedClients = new Map<string, Promise<Client>>();

/**
 * Creates or returns a cached Temporal client connection.
 * Caches the connection Promise (not the resolved client) so concurrent
 * callers with the same config share a single in-flight connection attempt
 * instead of racing and leaking extra gRPC connections.
 *
 * @param config - Client configuration
 * @returns Temporal Client instance
 */
export function createTemporalClient(config: TemporalClientConfig): Promise<Client> {
  const cacheKey = `${config.address}:${config.namespace}`;

  const existing = cachedClients.get(cacheKey);
  if (existing) {
    return existing;
  }

  const clientPromise = (async () => {
    const connection = await Connection.connect({
      address: config.address,
    });

    // TODO: Add OpenTelemetry interceptor for trace context propagation
    // Once @temporalio/interceptors-opentelemetry is installed, add:
    // import { OpenTelemetryWorkflowClientCallsInterceptor } from "@temporalio/interceptors-opentelemetry";
    // Then add to Client options: interceptors: { workflow: [() => new OpenTelemetryWorkflowClientCallsInterceptor()] }

    return new Client({
      connection,
      namespace: config.namespace,
    });
  })();

  cachedClients.set(cacheKey, clientPromise);

  clientPromise.catch(() => {
    cachedClients.delete(cacheKey);
  });

  return clientPromise;
}

/**
 * Starts a Temporal workflow execution.
 *
 * This is the primary function for executing workflows asynchronously.
 * It returns a handle that can be used to query status, wait for results,
 * cancel the workflow, etc.
 *
 * ## Example:
 *
 * ```typescript
 * const handle = await executeWorkflow({
 *   workflowName: "api-prompt-025b323f",
 *   input: {
 *     initialData: { promptText: "Hello", identitiesInput: [...] },
 *     workflowId: "prompt-abc123",
 *     correlationId: "req-xyz789",
 *     environment: "production",
 *   },
 * });
 *
 * // Option 1: Fire and forget
 * console.log(`Workflow started: ${handle.workflowId}`);
 *
 * // Option 2: Wait for result
 * const result = await handle.result();
 * console.log("Workflow completed:", result);
 * ```
 *
 * @param options - Workflow execution options
 * @returns WorkflowHandle for the started workflow
 */
export async function executeWorkflow(
  options: ExecuteWorkflowOptions,
): Promise<WorkflowHandle<any>> {
  const client = await createTemporalClient(options.clientConfig);

  const handle = await client.workflow.start(options.workflowName, {
    taskQueue: options.taskQueue ?? "workflow-tasks",
    workflowId: options.workflowId,
    args: [options.input],
    ...(options.versioningOverride ? { versioningOverride: options.versioningOverride } : {}),
  });

  return handle;
}

/**
 * Gets a handle to an existing workflow execution.
 *
 * Use this to reconnect to a workflow that was started previously,
 * allowing you to query its status, wait for completion, or send signals.
 *
 * @param workflowId - The workflow execution ID
 * @param config - Optional client configuration
 * @returns WorkflowHandle for the existing workflow
 */
export async function getWorkflowHandle(
  workflowId: UUIDV7,
  config: TemporalClientConfig,
): Promise<WorkflowHandle<any>> {
  const client = await createTemporalClient(config);
  return client.workflow.getHandle(workflowId);
}

/**
 * Executes a workflow and waits for its result.
 *
 * This is a convenience function that combines starting a workflow
 * and waiting for its completion. Use this when you need the result
 * synchronously (e.g., in an API handler that must return the result).
 *
 * **Warning:** This will block until the workflow completes, which could
 * be seconds or minutes depending on the workflow. Consider using
 * `executeWorkflow` with fire-and-forget for long-running workflows.
 *
 * @param options - Workflow execution options
 * @returns The workflow result
 */
export async function executeWorkflowAndWait<T = any>(options: ExecuteWorkflowOptions): Promise<T> {
  const handle = await executeWorkflow(options);
  return (await handle.result()) as T;
}

/**
 * Cancels a running workflow execution.
 *
 * @param workflowId - The workflow execution ID
 * @param config - Optional client configuration
 */
export async function cancelWorkflow(
  workflowId: UUIDV7,
  config: TemporalClientConfig,
): Promise<void> {
  const handle = await getWorkflowHandle(workflowId, config);
  await handle.cancel();
}

/**
 * Queries a workflow's status without affecting its execution.
 *
 * **Note:** Workflow queries require the workflow to define query handlers.
 * This is not currently implemented in our workflow-factory, but can be
 * added in the future.
 *
 * @param workflowId - The workflow execution ID
 * @param queryName - The query to execute
 * @param config - Optional client configuration
 * @returns The query result
 */
export async function queryWorkflow<T = any>(
  workflowId: UUIDV7,
  queryName: string,
  config: TemporalClientConfig,
): Promise<T> {
  const handle = await getWorkflowHandle(workflowId, config);
  return await handle.query(queryName);
}

/**
 * Sends a signal to a running workflow.
 *
 * **Note:** Workflow signals require the workflow to define signal handlers.
 * This is not currently implemented in our workflow-factory, but can be
 * added in the future for interactive workflows.
 *
 * @param workflowId - The workflow execution ID
 * @param signalName - The signal to send
 * @param data - Signal payload
 * @param config - Optional client configuration
 */
export async function signalWorkflow(
  workflowId: UUIDV7,
  signalName: string,
  data: any,
  config: TemporalClientConfig,
): Promise<void> {
  const handle = await getWorkflowHandle(workflowId, config);
  await handle.signal(signalName, data);
}
