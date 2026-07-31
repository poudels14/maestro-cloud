import type { ServicesApi } from "./api";
import type { Deployment } from "./types";

const isServer = typeof window === "undefined";
const ssrSafe = <Value>(query: () => Promise<Value>, fallback: Value) =>
  isServer ? () => Promise.resolve(fallback) : query;

const serviceQueryKeys = {
  all: ["services"] as const,
  deployments: (serviceId: string) => ["deployments", serviceId] as const,
  replicas: (serviceId: string, deploymentId: string) =>
    ["deployments", serviceId, deploymentId, "replicas"] as const,
  dnsRecords: ["dns-records"] as const
};

const servicesQuery = (api: ServicesApi) => ({
  queryKey: serviceQueryKeys.all,
  queryFn: ssrSafe(api.listServices, []),
  refetchInterval: 15_000,
  refetchOnWindowFocus: true,
  staleTime: 5_000
});

const deploymentsQuery = (api: ServicesApi, serviceId: string) => ({
  queryKey: serviceQueryKeys.deployments(serviceId),
  queryFn: ssrSafe(() => api.listDeployments(serviceId), []),
  refetchInterval: 10_000,
  refetchOnWindowFocus: true,
  staleTime: 5_000
});

const dnsRecordsQuery = (api: ServicesApi) => ({
  queryKey: serviceQueryKeys.dnsRecords,
  queryFn: ssrSafe(api.listDnsRecords, []),
  refetchInterval: 30_000,
  refetchOnWindowFocus: true,
  staleTime: 10_000
});

const deploymentReplicasQuery = (api: ServicesApi, deployment: Deployment) => ({
  queryKey: serviceQueryKeys.replicas(deployment.spec.serviceId, deployment.meta.id),
  queryFn: ssrSafe(() => api.listReplicas(deployment), []),
  refetchInterval: 5_000,
  refetchOnWindowFocus: true,
  staleTime: 2_000
});

export {
  deploymentReplicasQuery,
  deploymentsQuery,
  dnsRecordsQuery,
  serviceQueryKeys,
  servicesQuery
};
