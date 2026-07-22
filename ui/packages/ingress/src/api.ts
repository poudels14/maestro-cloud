import type { ApiSchemas, MaestroApiClient } from "@maestro/api-client";

type IngressBlocklist = ApiSchemas["BlockedIpsResponse"];
type IngressRoute = ApiSchemas["IngressRouting"];
type IngressTraffic = ApiSchemas["IngressTrafficBreakdown"];
type TrafficBreakdownEntry = ApiSchemas["TrafficBreakdownEntry"];
type IngressErrorMapper = (error: unknown, fallback: string) => Error;

interface IngressApi {
  listRoutes: () => Promise<IngressRoute[]>;
  getTraffic: (from: number, to: number) => Promise<IngressTraffic>;
  getBlockedTraffic: (from: number, to: number) => Promise<IngressTraffic>;
  getBlocklist: () => Promise<IngressBlocklist>;
  setBlockedIp: (ip: string, blocked: boolean) => Promise<IngressBlocklist>;
}

function createIngressApi(
  client: () => MaestroApiClient,
  mapError: IngressErrorMapper
): IngressApi {
  const mapped = async <Value>(operation: () => Promise<Value>, fallback: string) => {
    try {
      return await operation();
    } catch (error) {
      throw mapError(error, fallback);
    }
  };

  return {
    listRoutes: () =>
      mapped(() => client().listActiveIngressRoutes(), "Failed to load ingress routes"),
    getTraffic: (from, to) =>
      mapped(
        () => client().getIngressTraffic({ from, to, limit: 200 }),
        "Failed to load ingress traffic"
      ),
    getBlockedTraffic: (from, to) =>
      mapped(
        () => client().getBlockedIngressTraffic({ from, to, limit: 200 }),
        "Failed to load blocked ingress traffic"
      ),
    getBlocklist: () =>
      mapped(() => client().getIngressBlocklist(), "Failed to load ingress blocklist"),
    setBlockedIp: (ip, blocked) =>
      mapped(() => client().setBlockedIngressIp({ ip, blocked }), "Failed to update blocked IPs")
  };
}

export { createIngressApi };
export type {
  IngressApi,
  IngressBlocklist,
  IngressErrorMapper,
  IngressRoute,
  IngressTraffic,
  TrafficBreakdownEntry
};
