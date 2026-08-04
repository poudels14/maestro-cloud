import type { Deployment, ReplicaState } from "./types";

function sortDeploymentHistory(deployments: Deployment[]): Deployment[] {
  return [...deployments].sort(
    (left, right) =>
      right.status.createdAt - left.status.createdAt || left.meta.id.localeCompare(right.meta.id)
  );
}

function deploymentTitle(deployment: Deployment): string {
  return (
    deployment.status.gitCommit?.title.trim() ||
    deployment.meta.id.split("-").at(-1) ||
    deployment.meta.id
  );
}

function deploymentGitRevision(deployment: Deployment): string | null {
  return deployment.status.gitCommit?.revision ?? null;
}

function deploymentFailure(deployment: Deployment): string | null {
  const condition = deployment.status.conditions?.find((entry) => entry.status === "false");
  return condition?.message || condition?.reason || null;
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

export {
  deploymentFailure,
  deploymentGitRevision,
  deploymentTitle,
  replicaDisplayName,
  replicaFailure,
  sortDeploymentHistory
};
