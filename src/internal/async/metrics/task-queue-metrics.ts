/**
 * Temporal Task Queue Metrics Publisher
 *
 * Each activity worker container can poll one or more Temporal task queues and
 * emits CloudWatch metrics used by ECS autoscaling. The two published metrics
 * look similar at first glance because they share the same queue dimensions,
 * but they represent different kinds of truth and therefore must be aggregated
 * differently:
 *
 * 1. TaskQueueBacklogCount -- queue-level state from DescribeTaskQueue.
 *    This answers "how many activity tasks are waiting in queue X right now?"
 *    That answer belongs to the queue itself, not to any specific container.
 *    If five containers all poll "standard-tasks", they will all observe the
 *    same backlog value and all publish, for example, "12" for the same
 *    CloudWatch dimensions. Those five samples are duplicate observations of
 *    one queue, not five separate chunks of work. Consumers therefore must
 *    read backlog per queue with the Maximum statistic, which preserves the
 *    true queue backlog of 12 instead of accidentally treating it like 60.
 *    Once each queue has been reduced to its true backlog, autoscaling can sum
 *    across distinct queues to answer "how much pending work does this ECS
 *    service need to handle overall?"
 *
 * 2. RunningActivitiesCount -- container-level state for this process.
 *    This answers "how many activity tasks is this container executing right
 *    now?" The counter is process-wide and does not care which polled queue
 *    each activity originally came from. If one container is polling both
 *    "standard-tasks" and "heavy-tasks" and currently has 7 running
 *    activities total, this publisher emits:
 *
 *      RunningActivitiesCount{TaskQueue="standard-tasks"} = 7
 *      RunningActivitiesCount{TaskQueue="heavy-tasks"} = 7
 *
 *    Those are not two independent groups of 7 running tasks. They are the
 *    same process-wide total written under two queue labels so that each polled
 *    queue has a metric stream. Consumers therefore must NOT sum running count
 *    across all queue dimensions for a multi-queue worker, or that one
 *    container would be double-counted. Instead, autoscaling must choose one
 *    canonical queue dimension for the service (the primary queue) and sum
 *    RunningActivitiesCount across containers only within that single
 *    dimension. That produces the real fleet-wide in-flight activity count
 *    without inflating totals for workers that poll absorbed fallback queues.
 */

import {
  CloudWatchClient,
  type MetricDatum,
  PutMetricDataCommand,
  StandardUnit,
} from "@aws-sdk/client-cloudwatch";
import type { NativeConnection } from "@temporalio/worker";
import type { ComposerLogger } from "../../types";

const DEFAULT_CLOUDWATCH_NAMESPACE = "Composer";
const DEFAULT_POLL_INTERVAL_MS = 60_000;
const DEFAULT_TASK_PROTECTION_EXPIRES_IN_MINUTES = 360;
const DEFAULT_TASK_PROTECTION_RENEW_INTERVAL_MS = 30 * 60 * 1000;

type TaskProtectionFetch = (
  input: string,
  init: {
    method: "PUT";
    headers: { "Content-Type": "application/json" };
    body: string;
  },
) => Promise<{ ok: boolean; status: number; text: () => Promise<string> }>;

function getNextPublishTimestampMs(nowMs: number, intervalMs: number): number {
  const remainder = nowMs % intervalMs;
  return remainder === 0 ? nowMs + intervalMs : nowMs + (intervalMs - remainder);
}

export interface TaskQueueMetricsConfig {
  connection: NativeConnection;
  taskQueues: string[];
  temporalNamespace: string;
  logger: ComposerLogger;
  /** Environment name used as a CloudWatch dimension to avoid collisions. */
  environmentName?: string;
  /** CloudWatch publishing interval in ms. Defaults to 60000 (60s). */
  pollIntervalMs?: number;
  /** CloudWatch metric namespace. Defaults to "Composer". */
  cloudWatchNamespace?: string;
  /** Injected for testing. Defaults to a real CloudWatchClient. */
  cloudWatchClient?: CloudWatchClient;
  /** Injected for testing. Defaults to ECS_AGENT_URI when running in ECS. */
  taskProtectionAgentUri?: string;
  /** Injected for testing. Defaults to global fetch. */
  taskProtectionFetch?: TaskProtectionFetch;
  /** Task protection lease duration. Defaults to 6 hours. */
  taskProtectionExpiresInMinutes?: number;
  /** How often to renew task protection while activities are running. Defaults to 30 minutes. */
  taskProtectionRenewIntervalMs?: number;
}

export interface TaskQueueMetricsHandle {
  /** Stop the polling loop and clean up. */
  stop: () => Promise<void>;
  /** Report that an activity started on this container. */
  activityStarted: () => void;
  /** Report that an activity finished on this container. */
  activityFinished: () => void;
}

function buildTaskProtectionUrl(agentUri: string): string {
  return `${agentUri.replace(/\/$/, "")}/task-protection/v1/state`;
}

function startTaskScaleInProtection(config: {
  agentUri?: string;
  fetchFn?: TaskProtectionFetch;
  logger: ComposerLogger;
  expiresInMinutes: number;
  renewIntervalMs: number;
}): { protect: () => void; unprotect: () => void; stop: () => Promise<void> } {
  const {
    agentUri = process.env.ECS_AGENT_URI,
    logger,
    expiresInMinutes,
    renewIntervalMs,
  } = config;
  const fetchFn = config.fetchFn ?? globalThis.fetch;

  let renewTimer: ReturnType<typeof setTimeout> | undefined;
  let missingEndpointLogged = false;
  let isProtected = false;
  let updatePromise: Promise<void> = Promise.resolve();

  const clearRenewTimer = () => {
    if (renewTimer) {
      clearTimeout(renewTimer);
      renewTimer = undefined;
    }
  };

  const updateProtection = async (protectedState: boolean): Promise<void> => {
    if (!agentUri || !fetchFn) {
      if (!missingEndpointLogged) {
        missingEndpointLogged = true;
        logger.warn("ECS task scale-in protection unavailable", {
          hasAgentUri: !!agentUri,
          hasFetch: !!fetchFn,
        });
      }
      return;
    }

    const body = protectedState
      ? { ProtectionEnabled: true, ExpiresInMinutes: expiresInMinutes }
      : { ProtectionEnabled: false };

    try {
      const response = await fetchFn(buildTaskProtectionUrl(agentUri), {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });

      if (!response.ok) {
        logger.warn("Failed to update ECS task scale-in protection", {
          protected: protectedState,
          status: response.status,
          body: await response.text(),
        });
        return;
      }

      logger.info("Updated ECS task scale-in protection", { protected: protectedState });
    } catch (error) {
      logger.warn("Failed to update ECS task scale-in protection", {
        protected: protectedState,
        error: error instanceof Error ? error.message : String(error),
      });
    }
  };

  const enqueueProtectionUpdate = (protectedState: boolean): Promise<void> => {
    updatePromise = updatePromise.then(
      () => updateProtection(protectedState),
      () => updateProtection(protectedState),
    );
    return updatePromise;
  };

  const scheduleRenewal = () => {
    clearRenewTimer();
    renewTimer = setTimeout(() => {
      if (!isProtected) return;
      void enqueueProtectionUpdate(true);
      scheduleRenewal();
    }, renewIntervalMs);
    renewTimer.unref();
  };

  return {
    protect: () => {
      if (isProtected) return;
      isProtected = true;
      void enqueueProtectionUpdate(true);
      scheduleRenewal();
    },
    unprotect: () => {
      if (!isProtected) return;
      isProtected = false;
      clearRenewTimer();
      void enqueueProtectionUpdate(false);
    },
    stop: async () => {
      clearRenewTimer();
      if (isProtected) {
        isProtected = false;
        await enqueueProtectionUpdate(false);
        return;
      }
      await updatePromise;
    },
  };
}

/**
 * Starts a background polling loop that publishes task queue metrics to CloudWatch.
 *
 * Returns a handle to stop the loop and to track running activity count.
 */
export function startTaskQueueMetrics(config: TaskQueueMetricsConfig): TaskQueueMetricsHandle {
  const {
    connection,
    taskQueues,
    temporalNamespace,
    environmentName = process.env.ENVIRONMENT_NAME ?? "unknown",
    logger,
    pollIntervalMs = DEFAULT_POLL_INTERVAL_MS,
    cloudWatchNamespace = DEFAULT_CLOUDWATCH_NAMESPACE,
    cloudWatchClient = new CloudWatchClient({}),
    taskProtectionAgentUri,
    taskProtectionFetch,
    taskProtectionExpiresInMinutes = DEFAULT_TASK_PROTECTION_EXPIRES_IN_MINUTES,
    taskProtectionRenewIntervalMs = DEFAULT_TASK_PROTECTION_RENEW_INTERVAL_MS,
  } = config;

  let runningActivities = 0;
  let stopped = false;
  let timer: ReturnType<typeof setTimeout> | undefined;
  let nextPublishTimestampMs = getNextPublishTimestampMs(Date.now(), pollIntervalMs);
  const taskProtection = startTaskScaleInProtection({
    agentUri: taskProtectionAgentUri,
    fetchFn: taskProtectionFetch,
    logger,
    expiresInMinutes: taskProtectionExpiresInMinutes,
    renewIntervalMs: taskProtectionRenewIntervalMs,
  });

  const scheduleNextPublish = () => {
    if (stopped) return;

    const nowMs = Date.now();
    while (nextPublishTimestampMs <= nowMs) {
      nextPublishTimestampMs += pollIntervalMs;
    }

    timer = setTimeout(() => {
      const publishTimestampMs = nextPublishTimestampMs;
      nextPublishTimestampMs += pollIntervalMs;
      void publishMetrics(new Date(publishTimestampMs));
    }, nextPublishTimestampMs - nowMs);
    timer.unref();
  };

  const publishMetrics = async (timestamp: Date) => {
    if (stopped) return;

    try {
      const metricData: MetricDatum[] = [];

      for (const taskQueue of taskQueues) {
        const dimensions = [
          { Name: "TaskQueue", Value: taskQueue },
          { Name: "TemporalNamespace", Value: temporalNamespace },
          { Name: "Environment", Value: environmentName },
        ];

        let backlogCount = 0;
        try {
          const response = await connection.workflowService.describeTaskQueue({
            namespace: temporalNamespace,
            taskQueue: { name: taskQueue },
            taskQueueType: 2,
            reportStats: true,
          });

          backlogCount = Number(response.stats?.approximateBacklogCount ?? 0);
        } catch (error: unknown) {
          logger.warn("Failed to describe task queue", {
            taskQueue,
            error: error instanceof Error ? error.message : String(error),
          });
        }

        metricData.push(
          {
            MetricName: "TaskQueueBacklogCount",
            Dimensions: dimensions,
            Value: backlogCount,
            Unit: StandardUnit.Count,
            Timestamp: timestamp,
          },
          {
            MetricName: "RunningActivitiesCount",
            Dimensions: dimensions,
            Value: runningActivities,
            Unit: StandardUnit.Count,
            Timestamp: timestamp,
          },
        );
      }

      if (metricData.length > 0) {
        await cloudWatchClient.send(
          new PutMetricDataCommand({
            Namespace: cloudWatchNamespace,
            MetricData: metricData,
          }),
        );
      }
    } catch (error: unknown) {
      logger.warn("Failed to publish task queue metrics", {
        error: error instanceof Error ? error.message : String(error),
      });
    }

    scheduleNextPublish();
  };

  scheduleNextPublish();

  logger.info("Task queue metrics poller started", {
    taskQueues,
    temporalNamespace,
    pollIntervalMs,
  });

  return {
    stop: async () => {
      stopped = true;
      if (timer) {
        clearTimeout(timer);
      }
      await taskProtection.stop();
      logger.info("Task queue metrics poller stopped");
    },
    activityStarted: () => {
      const wasIdle = runningActivities === 0;
      runningActivities++;
      if (wasIdle) {
        taskProtection.protect();
      }
    },
    activityFinished: () => {
      const wasRunning = runningActivities > 0;
      runningActivities = Math.max(0, runningActivities - 1);
      if (wasRunning && runningActivities === 0) {
        taskProtection.unprotect();
      }
    },
  };
}
