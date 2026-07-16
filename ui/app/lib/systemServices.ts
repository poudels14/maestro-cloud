import type { Service } from "./types";
import type { ClusterInfo } from "./api";

function isPartOfCluster(cluster: ClusterInfo | undefined) {
  return cluster?.clusterId != null;
}

function visibleSystemServices(services: Service[], cluster: ClusterInfo | undefined) {
  return services.filter(
    (service) =>
      service.system === true && (isPartOfCluster(cluster) || service.id !== "maestro-gateway")
  );
}

export { isPartOfCluster, visibleSystemServices };
