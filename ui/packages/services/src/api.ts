import type { MaestroApiClient } from "@maestro/api-client";
import { createIdempotencyKey } from "@maestro/sdk";
import { sortDeploymentHistory } from "./deploymentView";
import { attachPreviewResources } from "./serviceView";
import type { Assignment, Deployment, DnsRecord, ReplicaState, Service } from "./types";

type ServicesErrorMapper = (error: unknown, fallback: string) => Error;

interface ServicesApi {
  listServices: () => Promise<Service[]>;
  deleteService: (service: Service) => Promise<void>;
  redeployService: (service: Service) => Promise<void>;
  setFrozen: (service: Service, frozen: boolean) => Promise<void>;
  setReplicas: (service: Service, replicas: number | null) => Promise<void>;
  listDeployments: (serviceId: string) => Promise<Deployment[]>;
  listAssignments: (deployment: Deployment) => Promise<Assignment[]>;
  listReplicas: (deployment: Deployment) => Promise<ReplicaState[]>;
  listDnsRecords: () => Promise<DnsRecord[]>;
  restartDeployment: (deployment: Deployment) => Promise<void>;
  cancelDeployment: (deployment: Deployment) => Promise<void>;
  removeDeployment: (deployment: Deployment) => Promise<void>;
}

function createServicesApi(
  client: () => MaestroApiClient,
  mapError: ServicesErrorMapper
): ServicesApi {
  const mapped = async <Value>(operation: () => Promise<Value>, fallback: string) => {
    try {
      return await operation();
    } catch (error) {
      throw mapError(error, fallback);
    }
  };
  const serviceCommand = (
    service: Service,
    fallback: string,
    operation: (
      client: MaestroApiClient,
      request: { expectedRevision: number },
      idempotencyKey: string
    ) => Promise<unknown>
  ) =>
    mapped(async () => {
      await operation(
        client(),
        { expectedRevision: service.meta.revision },
        createIdempotencyKey()
      );
    }, fallback);
  const deploymentCommand = (
    deployment: Deployment,
    fallback: string,
    operation: (
      client: MaestroApiClient,
      serviceId: string,
      deploymentId: string,
      request: { expectedRevision: number },
      idempotencyKey: string
    ) => Promise<unknown>
  ) =>
    mapped(async () => {
      await operation(
        client(),
        deployment.spec.serviceId,
        deployment.meta.id,
        { expectedRevision: deployment.meta.revision },
        createIdempotencyKey()
      );
    }, fallback);

  return {
    listServices: () =>
      mapped(async () => {
        const [services, previews] = await Promise.all([
          client().listServices(),
          client().listPreviews()
        ]);
        return attachPreviewResources(services, previews);
      }, "Failed to load services"),
    deleteService: (service) =>
      serviceCommand(service, "Failed to delete service", (api, request, key) =>
        api.deleteService(service.meta.id, request, key)
      ),
    redeployService: (service) =>
      serviceCommand(service, "Failed to redeploy service", (api, request, key) =>
        api.redeployService(service.meta.id, request, key)
      ),
    setFrozen: (service, frozen) =>
      serviceCommand(service, "Failed to update freeze status", (api, request, key) =>
        frozen
          ? api.freezeService(service.meta.id, request, key)
          : api.unfreezeService(service.meta.id, request, key)
      ),
    setReplicas: (service, replicas) =>
      mapped(async () => {
        await client().setServiceReplicas(
          service.meta.id,
          { expectedRevision: service.meta.revision, replicas },
          createIdempotencyKey()
        );
      }, "Failed to update replicas"),
    listDeployments: (serviceId) =>
      mapped(
        async () => sortDeploymentHistory(await client().listDeployments(serviceId)),
        "Failed to load deployments"
      ),
    listAssignments: (deployment) =>
      mapped(
        () => client().listAssignments(deployment.spec.serviceId, deployment.meta.id),
        "Failed to load deployment assignments"
      ),
    listReplicas: (deployment) =>
      mapped(
        () => client().listReplicas(deployment.spec.serviceId, deployment.meta.id),
        "Failed to load deployment replicas"
      ),
    listDnsRecords: () => mapped(() => client().listDnsRecords(), "Failed to load DNS records"),
    restartDeployment: (deployment) =>
      deploymentCommand(
        deployment,
        "Failed to restart deployment",
        (api, serviceId, deploymentId, request, key) =>
          api.restartDeployment(serviceId, deploymentId, request, key)
      ),
    cancelDeployment: (deployment) =>
      deploymentCommand(
        deployment,
        "Failed to cancel deployment",
        (api, serviceId, deploymentId, request, key) =>
          api.cancelDeployment(serviceId, deploymentId, request, key)
      ),
    removeDeployment: (deployment) =>
      deploymentCommand(
        deployment,
        "Failed to remove deployment",
        (api, serviceId, deploymentId, request, key) =>
          api.removeDeployment(serviceId, deploymentId, request, key)
      )
  };
}

export { createServicesApi };
export type { ServicesApi, ServicesErrorMapper };
