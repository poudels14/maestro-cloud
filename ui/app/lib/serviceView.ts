import type { ApiSchemas } from "@maestro/api-client";
import type { Service } from "./types";

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

function serviceHasBuild(service: Service): boolean {
  return service.spec.artifact.type === "build";
}

export { attachPreviewResources, serviceDisplayStatus, serviceHasBuild };
