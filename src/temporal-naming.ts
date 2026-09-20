/**
 * Temporal Worker Versioning deployment naming conventions.
 *
 * Exposed as a dedicated subpath export (@lotiai/composer/temporal-naming) so
 * infrastructure Lambdas can import these lightweight constants without pulling
 * in the full Composer framework or Temporal SDK.
 */

export const WORKER_DEPLOYMENT_SUFFIX = "workers" as const;

/**
 * Deployment suffixes from the retired per-worker-type scheme. Retained only so
 * the drain checker can still parse versions deployed before the merge; delete
 * once no `-workflows`/`-activities` version records remain.
 */
export const LEGACY_WORKER_DEPLOYMENT_SUFFIXES = {
  ACTIVITIES: "activities",
  WORKFLOWS: "workflows",
} as const;

/**
 * Derives the Temporal Worker Deployment name from a service name.
 *
 * A service's workflow and activity workers share one deployment so that their
 * task queues are members of the same Worker Deployment Version. That is what
 * keeps a Pinned Workflow's activities on the workflow's own version instead of
 * making them Independent Activities routed to whatever is Current.
 *
 * @example
 * getWorkerDeploymentName("orders-service") // => "orders-service-workers"
 */
export function getWorkerDeploymentName(serviceName: string): string {
  return `${serviceName}-${WORKER_DEPLOYMENT_SUFFIX}`;
}
