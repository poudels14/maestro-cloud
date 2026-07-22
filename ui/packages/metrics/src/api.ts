import type { ApiSchemas, MaestroApiClient } from "@maestro/api-client";

type DiskInfo = ApiSchemas["DiskInfo"];
type MetricPoint = ApiSchemas["ResourceMetricPoint"];
type TrafficPoint = ApiSchemas["TrafficMetricPoint"];
type MetricsErrorMapper = (error: unknown, fallback: string) => Error;

interface MetricsApi {
  listDisks: () => Promise<DiskInfo[]>;
  listNodeMetrics: (from: number, to: number) => Promise<MetricPoint[]>;
  listClusterMetrics: (from: number, to: number) => Promise<MetricPoint[]>;
  listServiceMetrics: (serviceId: string, from: number, to: number) => Promise<MetricPoint[]>;
  listServiceTraffic: (serviceId: string, from: number, to: number) => Promise<TrafficPoint[]>;
}

function createMetricsApi(
  client: () => MaestroApiClient,
  mapError: MetricsErrorMapper
): MetricsApi {
  const mapped = async <Value>(operation: () => Promise<Value>, fallback: string) => {
    try {
      return await operation();
    } catch (error) {
      throw mapError(error, fallback);
    }
  };

  return {
    listDisks: () => mapped(() => client().listLocalDisks(), "Failed to load disks"),
    listNodeMetrics: (from, to) =>
      mapped(() => client().listNodeMetrics({ from, to }), "Failed to load node metrics"),
    listClusterMetrics: (from, to) =>
      mapped(() => client().listClusterMetrics({ from, to }), "Failed to load cluster metrics"),
    listServiceMetrics: (serviceId, from, to) =>
      mapped(
        () => client().listServiceMetrics(serviceId, { from, to }),
        "Failed to load service metrics"
      ),
    listServiceTraffic: (serviceId, from, to) =>
      mapped(
        () => client().getServiceTraffic(serviceId, { from, to }),
        "Failed to load service traffic"
      )
  };
}

export { createMetricsApi };
export type { DiskInfo, MetricPoint, MetricsApi, MetricsErrorMapper, TrafficPoint };
