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
}

export interface TaskQueueMetricsHandle {
  /** Stop the polling loop and clean up. */
  stop: () => void;
  /** Report that an activity started on this container. */
  activityStarted: () => void;
  /** Report that an activity finished on this container. */
  activityFinished: () => void;
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
  } = config;

  let runningActivities = 0;
  let stopped = false;
  let timer: ReturnType<typeof setTimeout> | undefined;
  let nextPublishTimestampMs = getNextPublishTimestampMs(Date.now(), pollIntervalMs);

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
    stop: () => {
      stopped = true;
      if (timer) {
        clearTimeout(timer);
      }
      logger.info("Task queue metrics poller stopped");
    },
    activityStarted: () => {
      runningActivities++;
    },
    activityFinished: () => {
      runningActivities = Math.max(0, runningActivities - 1);
    },
  };
}
