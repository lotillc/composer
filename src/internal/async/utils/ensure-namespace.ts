/**
 * Ensures the Temporal namespace exists before workers start.
 * This is idempotent - safe to call multiple times.
 *
 * @module ensure-namespace
 */

import { Connection } from "@temporalio/client";
import Long from "long";

/**
 * Ensures the specified namespace exists on the Temporal server.
 * Creates it if it doesn't exist. This operation is idempotent.
 *
 * @param address - Temporal server address
 * @param namespace - Namespace name to ensure exists
 * @param retentionDays - Workflow history retention period (default: 90 days for production)
 */
export async function ensureNamespaceExists(
  address: string,
  namespace: string,
  retentionDays: number = 90,
): Promise<void> {
  const connection = await Connection.connect({ address });

  try {
    // Check if namespace exists by trying to describe it
    await connection.workflowService.describeNamespace({ namespace });
    console.log(`Namespace "${namespace}" already exists`);
  } catch (error: unknown) {
    const grpcError = error as { code?: number; details?: string };

    // gRPC NOT_FOUND code = 5
    if (grpcError.code === 5) {
      console.log(`Creating namespace "${namespace}"...`);
      const retentionSeconds = retentionDays * 24 * 60 * 60;
      try {
        await connection.workflowService.registerNamespace({
          namespace,
          workflowExecutionRetentionPeriod: {
            seconds: Long.fromNumber(retentionSeconds),
          },
        });
        console.log(`Namespace "${namespace}" created successfully`);
      } catch (registerError: unknown) {
        const registerGrpcError = registerError as { code?: number };
        // gRPC ALREADY_EXISTS code = 6 - another process won the race, namespace exists now
        if (registerGrpcError.code === 6) {
          console.log(`Namespace "${namespace}" was created by another process`);
        } else {
          throw registerError;
        }
      }
    } else {
      throw error;
    }
  } finally {
    await connection.close();
  }
}
