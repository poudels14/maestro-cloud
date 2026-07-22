import { keepPreviousData } from "@tanstack/solid-query";
import type { ClusterInfo } from "./api";
import type { ClusterStats, Deployment, MaskedConfig } from "./types";
import {
  getClusterConfig,
  getClusterInfo,
  getClusterNodes,
  getClusterMetrics,
  getContainerMetrics,
  getDeployments,
  getDeploymentReplicas,
  getDisks,
  getIngressRoutes,
  getIngressTraffic,
  getBlockedIngressTraffic,
  getNodeMetrics,
  getClusterStats,
  getIngressBlocklist,
  getServiceMetrics,
  getServiceTraffic,
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
  ingress: ["ingress"] as const,
  disks: ["disks"] as const,
  nodeMetrics: (range: number) => ["metrics", "node", range] as const,
  clusterMetrics: (range: number) => ["metrics", "cluster", range] as const,
  serviceMetrics: (serviceId: string, range: number) =>
    ["metrics", "service", serviceId, range] as const,
  serviceTraffic: (serviceId: string, range: number) =>
    ["traffic", "service", serviceId, range] as const,
  ingressTraffic: (range: number) => ["traffic", "ingress", range] as const,
  blockedIngressTraffic: (range: number) => ["traffic", "blocked", range] as const,
  ingressBlocklist: ["ingress", "blocklist"] as const,
  containerMetrics: (serviceId: string, range: number) =>
    ["metrics", "containers", serviceId, range] as const,
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

const ingressRoutesQuery = () => ({
  queryKey: queryKeys.ingress,
  queryFn: ssrSafe(getIngressRoutes, []),
  staleTime: 60_000
});

const disksQuery = () => ({
  queryKey: queryKeys.disks,
  queryFn: ssrSafe(getDisks, []),
  refetchInterval: 30_000
});

const nodeMetricsQuery = (rangeMs: number) => ({
  queryKey: queryKeys.nodeMetrics(rangeMs),
  queryFn: ssrSafe(() => {
    const now = Date.now();
    return getNodeMetrics(now - rangeMs, now);
  }, []),
  placeholderData: keepPreviousData,
  refetchInterval: 10_000
});

const clusterMetricsQuery = (rangeMs: number) => ({
  queryKey: queryKeys.clusterMetrics(rangeMs),
  queryFn: ssrSafe(() => {
    const now = Date.now();
    return getClusterMetrics(now - rangeMs, now);
  }, []),
  placeholderData: keepPreviousData,
  refetchInterval: 10_000
});

const serviceMetricsQuery = (serviceId: string, rangeMs: number) => ({
  queryKey: queryKeys.serviceMetrics(serviceId, rangeMs),
  queryFn: ssrSafe(() => {
    const now = Date.now();
    return getServiceMetrics(serviceId, now - rangeMs, now);
  }, []),
  placeholderData: keepPreviousData,
  refetchInterval: 10_000
});

const serviceTrafficQuery = (serviceId: string, rangeMs: number) => ({
  queryKey: queryKeys.serviceTraffic(serviceId, rangeMs),
  queryFn: ssrSafe(() => {
    const now = Date.now();
    return getServiceTraffic(serviceId, now - rangeMs, now);
  }, []),
  placeholderData: keepPreviousData,
  refetchInterval: 10_000
});

const ingressTrafficQuery = (rangeMs: number) => ({
  queryKey: queryKeys.ingressTraffic(rangeMs),
  queryFn: ssrSafe(
    () => {
      const now = Date.now();
      return getIngressTraffic(now - rangeMs, now);
    },
    { byIp: [], byPath: [] }
  ),
  placeholderData: keepPreviousData,
  refetchInterval: 15_000
});

const blockedIngressTrafficQuery = (rangeMs: number) => ({
  queryKey: queryKeys.blockedIngressTraffic(rangeMs),
  queryFn: ssrSafe(
    () => {
      const now = Date.now();
      return getBlockedIngressTraffic(now - rangeMs, now);
    },
    { byIp: [], byPath: [] }
  ),
  placeholderData: keepPreviousData,
  refetchInterval: 15_000
});

const ingressBlocklistQuery = () => ({
  queryKey: queryKeys.ingressBlocklist,
  queryFn: ssrSafe(getIngressBlocklist, { blockedIps: [] }),
  refetchInterval: 15_000
});

const containerMetricsQuery = (serviceId: string, rangeMs: number) => ({
  queryKey: queryKeys.containerMetrics(serviceId, rangeMs),
  queryFn: ssrSafe(() => {
    const now = Date.now();
    return getContainerMetrics(serviceId, now - rangeMs, now);
  }, []),
  placeholderData: keepPreviousData,
  refetchInterval: 10_000
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
  ingressRoutesQuery,
  disksQuery,
  nodeMetricsQuery,
  clusterMetricsQuery,
  serviceMetricsQuery,
  serviceTrafficQuery,
  ingressTrafficQuery,
  blockedIngressTrafficQuery,
  ingressBlocklistQuery,
  containerMetricsQuery,
  webhooksQuery
};
