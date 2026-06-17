import { keepPreviousData } from "@tanstack/solid-query";
import type { ClusterInfo } from "./api";
import type { MaskedConfig } from "./types";
import {
  getClusterConfig,
  getClusterInfo,
  getClusterMetrics,
  getContainerMetrics,
  getDeployments,
  getDisks,
  getIngressRoutes,
  getNodeMetrics,
  getServiceMetrics,
  getServiceTraffic,
  getServices,
  listSlackWebhooks
} from "./api";

const isServer = typeof window === "undefined";

function ssrSafe<T>(realFn: () => Promise<T>, ssrFallback: T): () => Promise<T> {
  return isServer ? () => Promise.resolve(ssrFallback) : realFn;
}

const queryKeys = {
  cluster: ["cluster"] as const,
  config: ["config"] as const,
  services: ["services"] as const,
  deployments: (serviceId: string) => ["deployments", serviceId] as const,
  ingress: ["ingress"] as const,
  disks: ["disks"] as const,
  nodeMetrics: (range: number) => ["metrics", "node", range] as const,
  clusterMetrics: (range: number) => ["metrics", "cluster", range] as const,
  serviceMetrics: (serviceId: string, range: number) =>
    ["metrics", "service", serviceId, range] as const,
  serviceTraffic: (serviceId: string, range: number) =>
    ["traffic", "service", serviceId, range] as const,
  containerMetrics: (serviceId: string, range: number) =>
    ["metrics", "containers", serviceId, range] as const,
  slackWebhooks: ["webhooks", "slack"] as const
};

const clusterInfoQuery = (opts?: { pollWhenUpgrading?: boolean }) => ({
  queryKey: queryKeys.cluster,
  queryFn: ssrSafe(getClusterInfo, null as ClusterInfo | null) as () => Promise<ClusterInfo>,
  staleTime: 60_000,
  refetchInterval: opts?.pollWhenUpgrading
    ? (query: { state: { data?: ClusterInfo } }) =>
        query.state.data?.upgrading ? 5_000 : (false as const)
    : (false as const)
});

const clusterConfigQuery = () => ({
  queryKey: queryKeys.config,
  queryFn: ssrSafe(getClusterConfig, null as MaskedConfig | null) as () => Promise<MaskedConfig>,
  staleTime: 60_000
});

const servicesQuery = () => ({
  queryKey: queryKeys.services,
  queryFn: ssrSafe(getServices, []),
  refetchInterval: 15_000
});

const deploymentsQuery = (serviceId: string) => ({
  queryKey: queryKeys.deployments(serviceId),
  queryFn: ssrSafe(() => getDeployments(serviceId), []),
  refetchInterval: 10_000
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

const containerMetricsQuery = (serviceId: string, rangeMs: number) => ({
  queryKey: queryKeys.containerMetrics(serviceId, rangeMs),
  queryFn: ssrSafe(() => {
    const now = Date.now();
    return getContainerMetrics(serviceId, now - rangeMs, now);
  }, []),
  placeholderData: keepPreviousData,
  refetchInterval: 10_000
});

const slackWebhooksQuery = () => ({
  queryKey: queryKeys.slackWebhooks,
  queryFn: ssrSafe(listSlackWebhooks, [])
});

export {
  queryKeys,
  clusterInfoQuery,
  clusterConfigQuery,
  servicesQuery,
  deploymentsQuery,
  ingressRoutesQuery,
  disksQuery,
  nodeMetricsQuery,
  clusterMetricsQuery,
  serviceMetricsQuery,
  serviceTrafficQuery,
  containerMetricsQuery,
  slackWebhooksQuery
};
