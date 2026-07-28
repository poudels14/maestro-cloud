import type { ClusterApi } from "./api";
import type { ClusterInfo, ClusterStats, MaskedConfig } from "./types";

const isServer = typeof window === "undefined";
const ssrSafe = <Value>(query: () => Promise<Value>, fallback: Value) =>
  isServer ? () => Promise.resolve(fallback) : query;

const clusterQueryKeys = {
  info: ["cluster"] as const,
  stats: ["cluster", "stats"] as const,
  nodes: ["cluster", "nodes"] as const,
  unschedulable: ["cluster", "unschedulable"] as const,
  config: ["config"] as const,
  webhooks: ["webhooks"] as const
};

const clusterInfoQuery = (api: ClusterApi, options?: { pollForMaintenance?: boolean }) => ({
  queryKey: clusterQueryKeys.info,
  queryFn: ssrSafe(api.getInfo, null as ClusterInfo | null) as () => Promise<ClusterInfo>,
  staleTime: 60_000,
  refetchInterval: options?.pollForMaintenance ? 5_000 : (false as const)
});

const clusterConfigQuery = (api: ClusterApi) => ({
  queryKey: clusterQueryKeys.config,
  queryFn: ssrSafe(api.getConfig, null as MaskedConfig | null) as () => Promise<MaskedConfig>,
  staleTime: 60_000
});

const clusterStatsQuery = (api: ClusterApi) => ({
  queryKey: clusterQueryKeys.stats,
  queryFn: ssrSafe(api.getStats, null as ClusterStats | null) as () => Promise<ClusterStats>,
  refetchInterval: 10_000
});

const clusterNodesQuery = (api: ClusterApi) => ({
  queryKey: clusterQueryKeys.nodes,
  queryFn: ssrSafe(api.listNodes, []),
  refetchInterval: 5_000
});

const unschedulableQuery = (api: ClusterApi) => ({
  queryKey: clusterQueryKeys.unschedulable,
  queryFn: ssrSafe(api.listUnschedulableReplicas, []),
  refetchInterval: 5_000
});

const webhooksQuery = (api: ClusterApi) => ({
  queryKey: clusterQueryKeys.webhooks,
  queryFn: ssrSafe(api.listWebhooks, [])
});

export {
  clusterConfigQuery,
  clusterInfoQuery,
  clusterNodesQuery,
  clusterQueryKeys,
  clusterStatsQuery,
  unschedulableQuery,
  webhooksQuery
};
