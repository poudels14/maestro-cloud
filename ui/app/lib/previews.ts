import type { Service } from "./types";

function userServices(services: Service[]): Service[] {
  return services.filter((service) => service.previewResource == null);
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

export { servicePreviews, userServices };
