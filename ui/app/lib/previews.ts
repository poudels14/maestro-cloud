import type { Service } from "./types";

function userServices(services: Service[]): Service[] {
  return services.filter((service) => !service.system && !service.previewSource);
}

function servicePreviews(services: Service[], baseServiceId: string): Service[] {
  return services
    .filter((service) => service.previewSource?.baseServiceId === baseServiceId)
    .sort(
      (left, right) => (left.previewSource?.prNumber ?? 0) - (right.previewSource?.prNumber ?? 0)
    );
}

export { servicePreviews, userServices };
