import type { ClusterInfo } from "./api";
import type { ClusterStats, Deployment, MaskedConfig } from "./types";
import {
  getClusterConfig,
  getClusterInfo,
  getClusterNodes,
  getDeployments,
  getDeploymentReplicas,
  getClusterStats,
  getServices,
  getUnschedulableReplicas,
  listWebhooks
} from "./api";

const isServer = typeof window === "undefined";

function ssrSafe<T>(realFn: () => Promise<T>, ssrFallback: T): () => Promise<T> {
  return isServer ? () => Promise.resolve(ssrFallback) : realFn;
}

const queryKeys = {
  cluster: ["cluster"] as const,
  clusterStats: ["cluster", "stats"] as const,
  clusterNodes: ["cluster", "nodes"] as const,
  unschedulable: ["cluster", "unschedulable"] as const,
  config: ["config"] as const,
  services: ["services"] as const,
  deployments: (serviceId: string) => ["deployments", serviceId] as const,
  deploymentReplicas: (serviceId: string, deploymentId: string) =>
    ["deployments", serviceId, deploymentId, "replicas"] as const,
  webhooks: ["webhooks"] as const
};

const clusterInfoQuery = (opts?: { pollForMaintenance?: boolean }) => ({
  queryKey: queryKeys.cluster,
  queryFn: ssrSafe(getClusterInfo, null as ClusterInfo | null) as () => Promise<ClusterInfo>,
  staleTime: 60_000,
  refetchInterval: opts?.pollForMaintenance ? 5_000 : (false as const)
});

const clusterConfigQuery = () => ({
  queryKey: queryKeys.config,
  queryFn: ssrSafe(getClusterConfig, null as MaskedConfig | null) as () => Promise<MaskedConfig>,
  staleTime: 60_000
});

const clusterStatsQuery = () => ({
  queryKey: queryKeys.clusterStats,
  queryFn: ssrSafe(getClusterStats, null as ClusterStats | null) as () => Promise<ClusterStats>,
  refetchInterval: 10_000
});

const clusterNodesQuery = () => ({
  queryKey: queryKeys.clusterNodes,
  queryFn: ssrSafe(getClusterNodes, []),
  refetchInterval: 5_000
});

const unschedulableQuery = () => ({
  queryKey: queryKeys.unschedulable,
  queryFn: ssrSafe(getUnschedulableReplicas, []),
  refetchInterval: 5_000
});

const servicesQuery = () => ({
  queryKey: queryKeys.services,
  queryFn: ssrSafe(getServices, []),
  refetchInterval: 15_000,
  refetchOnWindowFocus: true,
  staleTime: 5_000
});

const deploymentsQuery = (serviceId: string) => ({
  queryKey: queryKeys.deployments(serviceId),
  queryFn: ssrSafe(() => getDeployments(serviceId), []),
  refetchInterval: 10_000,
  refetchOnWindowFocus: true,
  staleTime: 5_000
});

const deploymentReplicasQuery = (deployment: Deployment) => ({
  queryKey: queryKeys.deploymentReplicas(deployment.spec.serviceId, deployment.meta.id),
  queryFn: ssrSafe(() => getDeploymentReplicas(deployment), []),
  refetchInterval: 5_000,
  refetchOnWindowFocus: true,
  staleTime: 2_000
});

const webhooksQuery = () => ({
  queryKey: queryKeys.webhooks,
  queryFn: ssrSafe(listWebhooks, [])
});

export {
  queryKeys,
  clusterInfoQuery,
  clusterConfigQuery,
  clusterStatsQuery,
  clusterNodesQuery,
  unschedulableQuery,
  servicesQuery,
  deploymentsQuery,
  deploymentReplicasQuery,
  webhooksQuery
};
