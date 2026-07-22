import { keepPreviousData } from "@tanstack/solid-query";
import type { MetricsApi } from "./api";

const isServer = typeof window === "undefined";

function ssrSafe<Value>(query: () => Promise<Value>, fallback: Value): () => Promise<Value> {
  return isServer ? () => Promise.resolve(fallback) : query;
}

function metricsRangeQuery<Value>(
  key: readonly unknown[],
  rangeMs: number,
  query: (from: number, to: number) => Promise<Value[]>
) {
  return {
    queryKey: key,
    queryFn: ssrSafe(() => {
      const now = Date.now();
      return query(now - rangeMs, now);
    }, []),
    placeholderData: keepPreviousData,
    refetchInterval: 10_000
  };
}

const disksQuery = (api: MetricsApi) => ({
  queryKey: ["disks"] as const,
  queryFn: ssrSafe(api.listDisks, []),
  refetchInterval: 30_000
});

const nodeMetricsQuery = (api: MetricsApi, rangeMs: number) =>
  metricsRangeQuery(["metrics", "node", rangeMs] as const, rangeMs, api.listNodeMetrics);

const clusterMetricsQuery = (api: MetricsApi, rangeMs: number) =>
  metricsRangeQuery(["metrics", "cluster", rangeMs] as const, rangeMs, api.listClusterMetrics);

const serviceMetricsQuery = (api: MetricsApi, serviceId: string, rangeMs: number) =>
  metricsRangeQuery(["metrics", "service", serviceId, rangeMs] as const, rangeMs, (from, to) =>
    api.listServiceMetrics(serviceId, from, to)
  );

const serviceTrafficQuery = (api: MetricsApi, serviceId: string, rangeMs: number) =>
  metricsRangeQuery(["traffic", "service", serviceId, rangeMs] as const, rangeMs, (from, to) =>
    api.listServiceTraffic(serviceId, from, to)
  );

export {
  clusterMetricsQuery,
  disksQuery,
  nodeMetricsQuery,
  serviceMetricsQuery,
  serviceTrafficQuery
};
