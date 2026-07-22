import { keepPreviousData } from "@tanstack/solid-query";
import type { IngressApi } from "./api";

const isServer = typeof window === "undefined";
const ssrSafe = <Value>(query: () => Promise<Value>, fallback: Value) =>
  isServer ? () => Promise.resolve(fallback) : query;

const ingressQueryKeys = {
  routes: ["ingress", "routes"] as const,
  traffic: (range: number) => ["traffic", "ingress", range] as const,
  blockedTraffic: (range: number) => ["traffic", "blocked", range] as const,
  blocklist: ["ingress", "blocklist"] as const
};

const ingressRoutesQuery = (api: IngressApi) => ({
  queryKey: ingressQueryKeys.routes,
  queryFn: ssrSafe(api.listRoutes, []),
  staleTime: 60_000
});

const trafficQuery = (api: IngressApi, rangeMs: number, blocked = false) => ({
  queryKey: blocked ? ingressQueryKeys.blockedTraffic(rangeMs) : ingressQueryKeys.traffic(rangeMs),
  queryFn: ssrSafe(
    () => {
      const now = Date.now();
      return blocked
        ? api.getBlockedTraffic(now - rangeMs, now)
        : api.getTraffic(now - rangeMs, now);
    },
    { byIp: [], byPath: [] }
  ),
  placeholderData: keepPreviousData,
  refetchInterval: 15_000
});

const ingressTrafficQuery = (api: IngressApi, rangeMs: number) => trafficQuery(api, rangeMs);
const blockedIngressTrafficQuery = (api: IngressApi, rangeMs: number) =>
  trafficQuery(api, rangeMs, true);

const ingressBlocklistQuery = (api: IngressApi) => ({
  queryKey: ingressQueryKeys.blocklist,
  queryFn: ssrSafe(api.getBlocklist, { blockedIps: [] }),
  refetchInterval: 15_000
});

export {
  blockedIngressTrafficQuery,
  ingressBlocklistQuery,
  ingressQueryKeys,
  ingressRoutesQuery,
  ingressTrafficQuery
};
