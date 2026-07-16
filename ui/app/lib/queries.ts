import { keepPreviousData } from "@tanstack/solid-query";
import type { ClusterInfo } from "./api";
import type {
  ClusterStats,
  IngressTrafficBreakdown,
  MaskedConfig,
  TrafficBreakdownEntry
} from "./types";
import {
  getClusterConfig,
  getClusterInfo,
  getClusterNodes,
  getClusterMetrics,
  getContainerMetrics,
  getDeployments,
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

const ingressTrafficQuery = (rangeMs: number) => ({
  queryKey: queryKeys.ingressTraffic(rangeMs),
  queryFn: ssrSafe(() => queryTrafficAcrossNodes(rangeMs, getIngressTraffic), {
    byIp: [],
    byPath: []
  }),
  placeholderData: keepPreviousData,
  refetchInterval: 15_000
});

const blockedIngressTrafficQuery = (rangeMs: number) => ({
  queryKey: queryKeys.blockedIngressTraffic(rangeMs),
  queryFn: ssrSafe(() => queryTrafficAcrossNodes(rangeMs, getBlockedIngressTraffic), {
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

async function queryTrafficAcrossNodes(
  rangeMs: number,
  fetchBreakdown: (from: number, to: number, nodeId?: string) => Promise<IngressTrafficBreakdown>
): Promise<IngressTrafficBreakdown> {
  const now = Date.now();
  let nodeIds: string[] = [];
  try {
    nodeIds = (await getClusterNodes()).filter((node) => node.alive).map((node) => node.nodeId);
  } catch {
    // A legacy single-node controller has no cluster topology to enumerate.
  }
  if (nodeIds.length === 0) {
    return fetchBreakdown(now - rangeMs, now);
  }
  const results = await Promise.allSettled(
    nodeIds.map((nodeId) => fetchBreakdown(now - rangeMs, now, nodeId))
  );
  const available = results.flatMap((result) =>
    result.status === "fulfilled" ? [result.value] : []
  );
  if (available.length === 0) {
    const firstFailure = results.find(
      (result): result is PromiseRejectedResult => result.status === "rejected"
    );
    throw firstFailure?.reason ?? new Error("No node returned ingress traffic");
  }
  return {
    ...mergeTrafficBreakdowns(available, 100),
    partial: available.length !== results.length,
    unavailableNodes: results.length - available.length
  };
}

function mergeTrafficBreakdowns(
  breakdowns: IngressTrafficBreakdown[],
  limit: number
): IngressTrafficBreakdown {
  return {
    byIp: mergeBreakdownEntries(
      breakdowns.flatMap((breakdown) => breakdown.byIp),
      limit
    ),
    byPath: mergeBreakdownEntries(
      breakdowns.flatMap((breakdown) => breakdown.byPath),
      limit
    )
  };
}

function mergeBreakdownEntries(
  entries: TrafficBreakdownEntry[],
  limit: number
): TrafficBreakdownEntry[] {
  const merged = new Map<string, TrafficBreakdownEntry>();
  for (const entry of entries) {
    const key = `${entry.value}\0${entry.statusCode}`;
    const current = merged.get(key);
    if (current) {
      current.requests += entry.requests;
      current.lastSeenAtMs = Math.max(current.lastSeenAtMs, entry.lastSeenAtMs);
    } else {
      merged.set(key, { ...entry });
    }
  }
  const totals = new Map<string, { requests: number; lastSeenAtMs: number }>();
  for (const entry of merged.values()) {
    const total = totals.get(entry.value) ?? { requests: 0, lastSeenAtMs: 0 };
    total.requests += entry.requests;
    total.lastSeenAtMs = Math.max(total.lastSeenAtMs, entry.lastSeenAtMs);
    totals.set(entry.value, total);
  }
  const ranks = new Map(
    Array.from(totals.entries())
      .sort(
        (left, right) =>
          right[1].requests - left[1].requests ||
          right[1].lastSeenAtMs - left[1].lastSeenAtMs ||
          left[0].localeCompare(right[0])
      )
      .slice(0, limit)
      .map(([value], rank) => [value, rank])
  );
  return Array.from(merged.values())
    .filter((entry) => ranks.has(entry.value))
    .sort(
      (left, right) =>
        ranks.get(left.value)! - ranks.get(right.value)! || left.statusCode - right.statusCode
    );
}

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
  clusterStatsQuery,
  clusterNodesQuery,
  unschedulableQuery,
  servicesQuery,
  deploymentsQuery,
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
  slackWebhooksQuery
};
