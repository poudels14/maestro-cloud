import type { Deployment, ReplicaState } from "./types";

function sortDeploymentHistory(deployments: Deployment[]): Deployment[] {
  return [...deployments].sort(
    (left, right) =>
      right.status.createdAt - left.status.createdAt || left.meta.id.localeCompare(right.meta.id)
  );
}

function replicaDisplayName(deployment: Deployment, replica: ReplicaState): string {
  return (
    replica.status.workloadId ?? `${deployment.spec.serviceId}-${String(replica.spec.replicaIndex)}`
  );
}

function replicaFailure(replica: ReplicaState): string | null {
  const condition = replica.status.conditions?.find((entry) => entry.status !== "true");
  return condition?.message || condition?.reason || null;
}

export { replicaDisplayName, replicaFailure, sortDeploymentHistory };
