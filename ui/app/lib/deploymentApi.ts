import type { Deployment, ReplicaState } from "./types";
import { apiClient, apiRequestError } from "./client";
import { sortDeploymentHistory } from "./deploymentView";

async function getDeployments(serviceId: string): Promise<Deployment[]> {
  try {
    return sortDeploymentHistory(await apiClient().listDeployments(serviceId));
  } catch (error) {
    throw apiRequestError(error, "Failed to load deployments");
  }
}

async function getDeploymentReplicas(deployment: Deployment): Promise<ReplicaState[]> {
  try {
    return await apiClient().listReplicas(deployment.spec.serviceId, deployment.meta.id);
  } catch (error) {
    throw apiRequestError(error, "Failed to load deployment replicas");
  }
}

async function restartDeployment(deployment: Deployment): Promise<void> {
  try {
    await apiClient().restartDeployment(
      deployment.spec.serviceId,
      deployment.meta.id,
      { expectedRevision: deployment.meta.revision },
      crypto.randomUUID()
    );
  } catch (error) {
    throw apiRequestError(error, "Failed to restart deployment");
  }
}

async function cancelDeployment(deployment: Deployment): Promise<void> {
  try {
    await apiClient().cancelDeployment(
      deployment.spec.serviceId,
      deployment.meta.id,
      { expectedRevision: deployment.meta.revision },
      crypto.randomUUID()
    );
  } catch (error) {
    throw apiRequestError(error, "Failed to cancel deployment");
  }
}

async function removeDeployment(deployment: Deployment): Promise<void> {
  try {
    await apiClient().removeDeployment(
      deployment.spec.serviceId,
      deployment.meta.id,
      { expectedRevision: deployment.meta.revision },
      crypto.randomUUID()
    );
  } catch (error) {
    throw apiRequestError(error, "Failed to remove deployment");
  }
}

export {
  cancelDeployment,
  getDeploymentReplicas,
  getDeployments,
  removeDeployment,
  restartDeployment
};
