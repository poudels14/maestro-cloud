import type { Service } from "./types";
import { apiClient, apiRequestError } from "./client";
import { attachPreviewResources } from "./serviceView";

async function getServices(): Promise<Service[]> {
  try {
    const [services, previews] = await Promise.all([
      apiClient().listServices(),
      apiClient().listPreviews()
    ]);
    return attachPreviewResources(services, previews);
  } catch (error) {
    throw apiRequestError(error, "Failed to load services");
  }
}

async function deleteService(service: Service): Promise<void> {
  try {
    await apiClient().deleteService(
      service.meta.id,
      { expectedRevision: service.meta.revision },
      crypto.randomUUID()
    );
  } catch (error) {
    throw apiRequestError(error, "Failed to delete service");
  }
}

async function redeployService(service: Service): Promise<void> {
  try {
    await apiClient().redeployService(
      service.meta.id,
      { expectedRevision: service.meta.revision },
      crypto.randomUUID()
    );
  } catch (error) {
    throw apiRequestError(error, "Failed to redeploy service");
  }
}

async function freezeService(service: Service, frozen: boolean): Promise<void> {
  try {
    const request = { expectedRevision: service.meta.revision };
    if (frozen) {
      await apiClient().freezeService(service.meta.id, request, crypto.randomUUID());
    } else {
      await apiClient().unfreezeService(service.meta.id, request, crypto.randomUUID());
    }
  } catch (error) {
    throw apiRequestError(error, "Failed to update freeze status");
  }
}

async function setServiceReplicas(service: Service, replicas: number | null): Promise<void> {
  try {
    await apiClient().setServiceReplicas(
      service.meta.id,
      { expectedRevision: service.meta.revision, replicas },
      crypto.randomUUID()
    );
  } catch (error) {
    throw apiRequestError(error, "Failed to update replicas");
  }
}

async function clearServiceReplicasOverride(service: Service): Promise<void> {
  await setServiceReplicas(service, null);
}

export {
  clearServiceReplicasOverride,
  deleteService,
  freezeService,
  getServices,
  redeployService,
  setServiceReplicas
};
