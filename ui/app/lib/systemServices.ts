import type { ClusterInfo } from "./api";

function isPartOfCluster(cluster: ClusterInfo | undefined) {
  return cluster?.clusterId != null;
}

export { isPartOfCluster };
