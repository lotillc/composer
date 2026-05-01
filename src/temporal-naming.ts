/**
 * Temporal Worker Versioning deployment series naming conventions.
 *
 * Exposed as a dedicated subpath export (@lotiai/composer/temporal-naming) so
 * infrastructure Lambdas can import these lightweight constants without pulling
 * in the full Composer framework or Temporal SDK.
 */

export const WORKER_DEPLOYMENT_SUFFIXES = {
  ACTIVITIES: "activities",
  WORKFLOWS: "workflows",
} as const;

/**
 * Derives the Temporal Worker Versioning deployment series names from a service name.
 *
 * Each service runs two worker types (activity and workflow), each with its own
 * deployment series for independent versioning. CI uses these same names when
 * calling setCurrentDeployment after a deploy.
 *
 * @example
 * getDeploymentSeriesNames("orders-service")
 * // => { activities: "orders-service-activities", workflows: "orders-service-workflows" }
 */
export function getDeploymentSeriesNames(serviceName: string) {
  return {
    activities: `${serviceName}-${WORKER_DEPLOYMENT_SUFFIXES.ACTIVITIES}`,
    workflows: `${serviceName}-${WORKER_DEPLOYMENT_SUFFIXES.WORKFLOWS}`,
  };
}
