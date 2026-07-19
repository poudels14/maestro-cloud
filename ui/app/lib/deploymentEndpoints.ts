import type { Deployment } from "./types";

export function replicaHostname(
  deployment: Deployment,
  replicaIndex: number,
  reportedHostname?: string | null
): string {
  if (reportedHostname) return reportedHostname;

  const shortDeploymentId = deployment.id.slice(0, 6);
  const base = `${deployment.config.id}-${shortDeploymentId}`;
  return replicaIndex === 0 ? base : `${base}-${replicaIndex}`;
}
