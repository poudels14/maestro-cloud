import type { ApiSchemas } from "@maestro/api-client";
import { sortDeploymentHistory } from "./deploymentView";
import type { Deployment, Service } from "./types";

function attachPreviewResources(
  services: ApiSchemas["Service"][],
  previews: ApiSchemas["Preview"][]
): Service[] {
  const previewsByService = new Map(
    previews.map((preview) => [preview.spec.serviceId, preview] as const)
  );
  return services.map((service) => {
    const previewResource = previewsByService.get(service.meta.id);
    return previewResource ? { ...service, previewResource } : service;
  });
}

function serviceDisplayStatus(service: Service): string {
  if (service.meta.deletionTimestamp != null) return "TERMINATED";
  const failed = service.status.conditions?.some(
    (condition) => condition.status === "false" && condition.type.toLowerCase() === "ready"
  );
  if (failed) return "FAILED";
  return service.status.activeDeploymentId ? "READY" : "IDLE";
}

function previewDeploymentStatus(service: Service, deployments: Deployment[]): string {
  const status = serviceDisplayStatus(service);
  if (status === "TERMINATED" || status === "FAILED") return status;
  const revision = service.previewResource?.spec.headRevision;
  const current = sortDeploymentHistory(deployments).find((deployment) => {
    if (deployment.spec.serviceGeneration !== service.meta.generation) return false;
    if (revision == null) return true;
    const artifact = deployment.spec.service.artifact;
    return (
      artifact.type === "build" &&
      artifact.source.type === "git" &&
      artifact.source.revision === revision
    );
  });
  if (current) return current.status.phase;
  if (service.previewResource?.status.phase === "pending") return "QUEUED";
  return status;
}

function serviceHasBuild(service: Service): boolean {
  return service.spec.artifact.type === "build";
}

function previewPullRequestState(service: Service): ApiSchemas["PullRequestState"] {
  return service.previewResource?.status.pullRequestState ?? "open";
}

function isSystemService(service: Service): boolean {
  return service.meta.id.startsWith("maestro-system-");
}

function nonPreviewServices(services: Service[]): Service[] {
  return services.filter((service) => service.previewResource == null);
}

function userServices(services: Service[]): Service[] {
  return nonPreviewServices(services).filter((service) => !isSystemService(service));
}

function systemServices(services: Service[]): Service[] {
  return nonPreviewServices(services).filter((service) => isSystemService(service));
}

function servicePreviews(services: Service[], baseServiceId: string): Service[] {
  return services
    .filter((service) => service.previewResource?.spec.baseServiceId === baseServiceId)
    .sort(
      (left, right) =>
        (left.previewResource?.spec.pullRequestNumber ?? 0) -
        (right.previewResource?.spec.pullRequestNumber ?? 0)
    );
}

function previewServices(services: Service[]): Service[] {
  return services
    .filter((service) => service.previewResource != null)
    .sort((left, right) => {
      const repository = left.previewResource!.spec.repository.localeCompare(
        right.previewResource!.spec.repository
      );
      return (
        repository ||
        left.previewResource!.spec.pullRequestNumber - right.previewResource!.spec.pullRequestNumber
      );
    });
}

function previewEnabledServices(services: Service[]): Service[] {
  return services
    .filter((service) => service.previewResource == null && service.spec.preview != null)
    .sort(
      (left, right) =>
        left.spec.name.localeCompare(right.spec.name) || left.meta.id.localeCompare(right.meta.id)
    );
}

export {
  attachPreviewResources,
  isSystemService,
  nonPreviewServices,
  previewEnabledServices,
  previewPullRequestState,
  previewServices,
  previewDeploymentStatus,
  serviceDisplayStatus,
  serviceHasBuild,
  servicePreviews,
  systemServices,
  userServices
};
