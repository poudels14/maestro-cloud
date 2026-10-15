import { keepPreviousData } from "@tanstack/solid-query";
import type { ClusterConfig, ClusterInfo, Stats } from "./types";
import {
  getClusterConfig,
  getClusterInfo,
  getClusterNodes,
  getClusterMetrics,
  getDeployments,
  getDisks,
  getIngressTraffic,
  getBlockedIngressTraffic,
  getNodeMetrics,
  getClusterStats,
  getIngressBlocklist,
  getServiceMetrics,
  getServiceTraffic,
  getServices,
  getUnschedulableReplicas,
  listSlackWebhooks
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
  disks: ["disks"] as const,
  nodeMetrics: (range: number, nodeId: number | undefined) =>
    ["metrics", "node", range, nodeId] as const,
  clusterMetrics: (range: number) => ["metrics", "cluster", range] as const,
  serviceMetrics: (serviceId: string, range: number) =>
    ["metrics", "service", serviceId, range] as const,
  serviceTraffic: (serviceId: string, range: number) =>
    ["traffic", "service", serviceId, range] as const,
  ingressTraffic: (range: number) => ["traffic", "ingress", range] as const,
  blockedIngressTraffic: (range: number) => ["traffic", "blocked", range] as const,
  ingressBlocklist: ["ingress", "blocklist"] as const,
  slackWebhooks: ["webhooks", "slack"] as const
};

const clusterInfoQuery = (opts?: { pollForMaintenance?: boolean }) => ({
  queryKey: queryKeys.cluster,
  queryFn: ssrSafe(getClusterInfo, null as ClusterInfo | null) as () => Promise<ClusterInfo>,
  staleTime: 60_000,
  refetchInterval: opts?.pollForMaintenance ? 5_000 : (false as const)
});

const clusterConfigQuery = () => ({
  queryKey: queryKeys.config,
  queryFn: ssrSafe(getClusterConfig, null as ClusterConfig | null) as () => Promise<ClusterConfig>,
  staleTime: 60_000
});

const clusterStatsQuery = () => ({
  queryKey: queryKeys.clusterStats,
  queryFn: ssrSafe(getClusterStats, null as Stats.Cluster | null) as () => Promise<Stats.Cluster>,
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

const disksQuery = () => ({
  queryKey: queryKeys.disks,
  queryFn: ssrSafe(getDisks, {}),
  refetchInterval: 30_000
});

const nodeMetricsQuery = (rangeMs: number, nodeId?: number) => ({
  queryKey: queryKeys.nodeMetrics(rangeMs, nodeId),
  queryFn: ssrSafe(() => {
    const now = Date.now();
    return getNodeMetrics(now - rangeMs, now, nodeId);
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
  queryFn: ssrSafe(() => trafficInRange(rangeMs, getIngressTraffic), {
    byIp: [],
    byPath: []
  }),
  placeholderData: keepPreviousData,
  refetchInterval: 15_000
});

const blockedIngressTrafficQuery = (rangeMs: number) => ({
  queryKey: queryKeys.blockedIngressTraffic(rangeMs),
  queryFn: ssrSafe(() => trafficInRange(rangeMs, getBlockedIngressTraffic), {
    byIp: [],
    byPath: []
  }),
  placeholderData: keepPreviousData,
  refetchInterval: 15_000
});

const ingressBlocklistQuery = () => ({
  queryKey: queryKeys.ingressBlocklist,
  queryFn: ssrSafe(getIngressBlocklist, { blockedIps: [] }),
  refetchInterval: 15_000
});

const slackWebhooksQuery = () => ({
  queryKey: queryKeys.slackWebhooks,
  queryFn: ssrSafe(listSlackWebhooks, [])
});

function trafficInRange<T>(rangeMs: number, fetchRange: (from: number, to: number) => Promise<T>) {
  const now = Date.now();
  return fetchRange(now - rangeMs, now);
}

export {
  queryKeys,
  clusterInfoQuery,
  clusterConfigQuery,
  clusterStatsQuery,
  clusterNodesQuery,
  unschedulableQuery,
  servicesQuery,
  deploymentsQuery,
  disksQuery,
  nodeMetricsQuery,
  clusterMetricsQuery,
  serviceMetricsQuery,
  serviceTrafficQuery,
  ingressTrafficQuery,
  blockedIngressTrafficQuery,
  ingressBlocklistQuery,
  slackWebhooksQuery
};
