export type { components, operations, paths, webhooks } from "./schema";
export type {
  ApiSchemas,
  BuiltinResource,
  BuiltinResourceKind,
  BuiltinResources
} from "./resources";
export { ApiHttpError, createFetchTransport, decodeJson } from "./transport";
export type { ApiRequestOptions, ApiTransport, TransportRequest } from "./transport";

import type { ApiSchemas } from "./resources";
import type { operations } from "./schema";
import { decodeJson } from "./transport";
import type { ApiRequestOptions, ApiTransport, TransportRequest } from "./transport";

export type IngressTrafficQuery = NonNullable<
  operations["getIngressTraffic"]["parameters"]["query"]
>;
export type ClusterMetricQuery = NonNullable<
  operations["listClusterMetrics"]["parameters"]["query"]
>;
export type NodeMetricQuery = NonNullable<operations["listNodeMetrics"]["parameters"]["query"]>;
export type StatsMetricQuery = NonNullable<
  operations["listOperationalStatsMetrics"]["parameters"]["query"]
>;
export type ServiceMetricQuery = NonNullable<
  operations["listServiceMetrics"]["parameters"]["query"]
>;
export type ContainerMetricQuery = NonNullable<
  operations["listContainerMetrics"]["parameters"]["query"]
>;

export interface MaestroApiClient {
  getClusterInfo(options?: ApiRequestOptions): Promise<ApiSchemas["ClusterInfo"]>;
  getClusterStats(options?: ApiRequestOptions): Promise<ApiSchemas["ClusterStatsResponse"]>;
  listLocalDisks(options?: ApiRequestOptions): Promise<ApiSchemas["DiskInfo"][]>;
  listNodeDisks(options?: ApiRequestOptions): Promise<ApiSchemas["NodeDiskMap"]>;
  listNodeMetrics(
    query?: NodeMetricQuery,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["ResourceMetricPoint"][]>;
  listClusterMetrics(
    query?: ClusterMetricQuery,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["ResourceMetricPoint"][]>;
  listOperationalStatsMetrics(
    query?: StatsMetricQuery,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["StatsMetricPoint"][]>;
  listNodes(options?: ApiRequestOptions): Promise<ApiSchemas["Node"][]>;
  listUnschedulableReplicas(
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["UnschedulableReplica"][]>;
  getNode(nodeId: string, options?: ApiRequestOptions): Promise<ApiSchemas["Node"]>;
  drainNode(
    nodeId: string,
    request: ApiSchemas["CommandRequest"],
    idempotencyKey: string,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["NodeCommandResponse"]>;
  restoreNode(
    nodeId: string,
    request: ApiSchemas["CommandRequest"],
    idempotencyKey: string,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["NodeCommandResponse"]>;
  listUpgrades(options?: ApiRequestOptions): Promise<ApiSchemas["UpgradeRun"][]>;
  getUpgrade(upgradeRunId: string, options?: ApiRequestOptions): Promise<ApiSchemas["UpgradeRun"]>;
  startUpgrade(
    request: ApiSchemas["UpgradeCreateRequest"],
    idempotencyKey: string,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["UpgradeCommandResponse"]>;
  cancelUpgrade(
    upgradeRunId: string,
    request: ApiSchemas["CommandRequest"],
    idempotencyKey: string,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["UpgradeCommandResponse"]>;
  listNodeNetworks(options?: ApiRequestOptions): Promise<ApiSchemas["NodeNetwork"][]>;
  getNodeNetwork(
    networkId: string,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["NodeNetwork"]>;
  listNodeFirewalls(options?: ApiRequestOptions): Promise<ApiSchemas["NodeFirewall"][]>;
  getNodeFirewall(
    firewallId: string,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["NodeFirewall"]>;
  listDnsRecords(options?: ApiRequestOptions): Promise<ApiSchemas["DnsRecord"][]>;
  getDnsRecord(recordId: string, options?: ApiRequestOptions): Promise<ApiSchemas["DnsRecord"]>;
  listFirewallPolicies(options?: ApiRequestOptions): Promise<ApiSchemas["FirewallPolicy"][]>;
  getFirewallPolicy(
    policyId: string,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["FirewallPolicy"]>;
  putFirewallPolicy(
    policyId: string,
    request: ApiSchemas["FirewallPolicyWriteRequest"],
    idempotencyKey: string,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["FirewallPolicyCommandResponse"]>;
  deleteFirewallPolicy(
    policyId: string,
    request: ApiSchemas["CommandRequest"],
    idempotencyKey: string,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["FirewallPolicyCommandResponse"]>;
  dryRunFirewallPolicy(
    policyId: string,
    request: ApiSchemas["FirewallDryRunRequest"],
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["FirewallDryRunResponse"]>;
  listPreviews(options?: ApiRequestOptions): Promise<ApiSchemas["Preview"][]>;
  getPreview(previewId: string, options?: ApiRequestOptions): Promise<ApiSchemas["Preview"]>;
  listWebhooks(options?: ApiRequestOptions): Promise<ApiSchemas["Webhook"][]>;
  getWebhook(webhookId: string, options?: ApiRequestOptions): Promise<ApiSchemas["Webhook"]>;
  putWebhook(
    webhookId: string,
    request: ApiSchemas["WebhookWriteRequest"],
    idempotencyKey: string,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["WebhookCommandResponse"]>;
  deleteWebhook(
    webhookId: string,
    request: ApiSchemas["CommandRequest"],
    idempotencyKey: string,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["WebhookCommandResponse"]>;
  testWebhook(
    webhookId: string,
    request: ApiSchemas["WebhookTestRequest"],
    idempotencyKey: string,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["WebhookTestResponse"]>;
  listServices(options?: ApiRequestOptions): Promise<ApiSchemas["Service"][]>;
  getService(serviceId: string, options?: ApiRequestOptions): Promise<ApiSchemas["Service"]>;
  listDeployments(
    serviceId: string,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["Deployment"][]>;
  getDeployment(
    serviceId: string,
    deploymentId: string,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["Deployment"]>;
  listAssignments(
    serviceId: string,
    deploymentId: string,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["Assignment"][]>;
  getAssignment(
    serviceId: string,
    deploymentId: string,
    assignmentId: string,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["Assignment"]>;
  listReplicas(
    serviceId: string,
    deploymentId: string,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["ReplicaState"][]>;
  getReplica(
    serviceId: string,
    deploymentId: string,
    replicaId: string,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["ReplicaState"]>;
  listBuilds(serviceId: string, options?: ApiRequestOptions): Promise<ApiSchemas["Build"][]>;
  getBuild(
    serviceId: string,
    buildId: string,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["Build"]>;
  listIngressRoutes(
    serviceId: string,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["IngressRoute"][]>;
  getIngressRoute(
    serviceId: string,
    routeId: string,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["IngressRoute"]>;
  listServiceMetrics(
    serviceId: string,
    query?: ServiceMetricQuery,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["ResourceMetricPoint"][]>;
  listContainerMetrics(
    serviceId: string,
    query?: ContainerMetricQuery,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["ResourceMetricPoint"][]>;
  listActiveIngressRoutes(options?: ApiRequestOptions): Promise<ApiSchemas["IngressRouting"][]>;
  getIngressBlocklist(options?: ApiRequestOptions): Promise<ApiSchemas["BlockedIpsResponse"]>;
  setBlockedIngressIp(
    request: ApiSchemas["BlockedIpRequest"],
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["BlockedIpsResponse"]>;
  getIngressTraffic(
    query?: IngressTrafficQuery,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["IngressTrafficBreakdown"]>;
  getBlockedIngressTraffic(
    query?: IngressTrafficQuery,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["IngressTrafficBreakdown"]>;
  listTrafficGenerations(
    serviceId: string,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["TrafficGeneration"][]>;
  getTrafficGeneration(
    serviceId: string,
    generationId: string,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["TrafficGeneration"]>;
  redeployService(
    serviceId: string,
    request: ApiSchemas["CommandRequest"],
    idempotencyKey: string,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["ServiceCommandResponse"]>;
  freezeService(
    serviceId: string,
    request: ApiSchemas["CommandRequest"],
    idempotencyKey: string,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["ServiceCommandResponse"]>;
  unfreezeService(
    serviceId: string,
    request: ApiSchemas["CommandRequest"],
    idempotencyKey: string,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["ServiceCommandResponse"]>;
  setServiceReplicas(
    serviceId: string,
    request: ApiSchemas["ServiceReplicaOverrideRequest"],
    idempotencyKey: string,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["ServiceCommandResponse"]>;
  deleteService(
    serviceId: string,
    request: ApiSchemas["CommandRequest"],
    idempotencyKey: string,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["ServiceCommandResponse"]>;
  restartDeployment(
    serviceId: string,
    deploymentId: string,
    request: ApiSchemas["CommandRequest"],
    idempotencyKey: string,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["DeploymentCommandResponse"]>;
  cancelDeployment(
    serviceId: string,
    deploymentId: string,
    request: ApiSchemas["CommandRequest"],
    idempotencyKey: string,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["DeploymentCommandResponse"]>;
  removeDeployment(
    serviceId: string,
    deploymentId: string,
    request: ApiSchemas["CommandRequest"],
    idempotencyKey: string,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["DeploymentCommandResponse"]>;
  putService(
    serviceId: string,
    request: ApiSchemas["ServiceWriteRequest"],
    idempotencyKey: string,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["ServiceWriteResponse"]>;
}

export function createApiClient(transport: ApiTransport): MaestroApiClient {
  function get<Response>(path: string, options?: ApiRequestOptions): Promise<Response> {
    const request: TransportRequest<Response> = {
      method: "GET",
      path,
      decode: decodeJson
    };
    if (options?.headers !== undefined) {
      request.headers = options.headers;
    }
    if (options?.signal !== undefined) {
      request.signal = options.signal;
    }
    return transport.request(request);
  }

  function mutate<Response, Body>(
    method: "DELETE" | "POST" | "PUT",
    path: string,
    body: Body,
    idempotencyKey: string,
    options?: ApiRequestOptions
  ): Promise<Response> {
    const request: TransportRequest<Response, Body> = {
      method,
      path,
      body,
      headers: {
        ...options?.headers,
        "Idempotency-Key": idempotencyKey
      },
      decode: decodeJson
    };
    if (options?.signal !== undefined) {
      request.signal = options.signal;
    }
    return transport.request(request);
  }

  function submit<Response, Body>(
    method: "PATCH" | "POST",
    path: string,
    body: Body,
    options?: ApiRequestOptions
  ): Promise<Response> {
    const request: TransportRequest<Response, Body> = {
      method,
      path,
      body,
      decode: decodeJson
    };
    if (options?.headers !== undefined) {
      request.headers = options.headers;
    }
    if (options?.signal !== undefined) {
      request.signal = options.signal;
    }
    return transport.request(request);
  }

  function withQuery(path: string, parameters?: object): string {
    const query = new URLSearchParams();
    for (const [name, value] of Object.entries(parameters ?? {})) {
      if (value !== undefined && value !== null) query.set(name, String(value));
    }
    const encoded = query.toString();
    return encoded ? `${path}?${encoded}` : path;
  }

  return {
    getClusterInfo: (options) => get("/api/cluster", options),
    getClusterStats: (options) => get("/api/cluster/stats", options),
    listLocalDisks: (options) => get("/api/disks", options),
    listNodeDisks: (options) => get("/api/disks/nodes", options),
    listNodeMetrics: (query, options) => get(withQuery("/api/metrics/node", query), options),
    listClusterMetrics: (query, options) => get(withQuery("/api/metrics/cluster", query), options),
    listOperationalStatsMetrics: (query, options) =>
      get(withQuery("/api/metrics/stats", query), options),
    listNodes: (options) => get("/api/cluster/nodes", options),
    listUnschedulableReplicas: (options) => get("/api/cluster/unschedulable", options),
    getNode: (nodeId, options) => get(`/api/cluster/nodes/${encodeURIComponent(nodeId)}`, options),
    drainNode: (nodeId, request, idempotencyKey, options) =>
      mutate(
        "POST",
        `/api/cluster/nodes/${encodeURIComponent(nodeId)}/drain`,
        request,
        idempotencyKey,
        options
      ),
    restoreNode: (nodeId, request, idempotencyKey, options) =>
      mutate(
        "POST",
        `/api/cluster/nodes/${encodeURIComponent(nodeId)}/restore`,
        request,
        idempotencyKey,
        options
      ),
    listUpgrades: (options) => get("/api/cluster/upgrades", options),
    getUpgrade: (upgradeRunId, options) =>
      get(`/api/cluster/upgrades/${encodeURIComponent(upgradeRunId)}`, options),
    startUpgrade: (request, idempotencyKey, options) =>
      mutate("POST", "/api/cluster/upgrades", request, idempotencyKey, options),
    cancelUpgrade: (upgradeRunId, request, idempotencyKey, options) =>
      mutate(
        "DELETE",
        `/api/cluster/upgrades/${encodeURIComponent(upgradeRunId)}`,
        request,
        idempotencyKey,
        options
      ),
    listNodeNetworks: (options) => get("/api/cluster/networks", options),
    getNodeNetwork: (networkId, options) =>
      get(`/api/cluster/networks/${encodeURIComponent(networkId)}`, options),
    listNodeFirewalls: (options) => get("/api/cluster/node-firewalls", options),
    getNodeFirewall: (firewallId, options) =>
      get(`/api/cluster/node-firewalls/${encodeURIComponent(firewallId)}`, options),
    listDnsRecords: (options) => get("/api/cluster/dns-records", options),
    getDnsRecord: (recordId, options) =>
      get(`/api/cluster/dns-records/${encodeURIComponent(recordId)}`, options),
    listFirewallPolicies: (options) => get("/api/firewall/policies", options),
    getFirewallPolicy: (policyId, options) =>
      get(`/api/firewall/policies/${encodeURIComponent(policyId)}`, options),
    putFirewallPolicy: (policyId, request, idempotencyKey, options) =>
      mutate(
        "PUT",
        `/api/firewall/policies/${encodeURIComponent(policyId)}`,
        request,
        idempotencyKey,
        options
      ),
    deleteFirewallPolicy: (policyId, request, idempotencyKey, options) =>
      mutate(
        "DELETE",
        `/api/firewall/policies/${encodeURIComponent(policyId)}`,
        request,
        idempotencyKey,
        options
      ),
    dryRunFirewallPolicy: (policyId, request, options) =>
      submit(
        "POST",
        `/api/firewall/policies/${encodeURIComponent(policyId)}/dry-run`,
        request,
        options
      ),
    listPreviews: (options) => get("/api/previews", options),
    getPreview: (previewId, options) =>
      get(`/api/previews/${encodeURIComponent(previewId)}`, options),
    listWebhooks: (options) => get("/api/webhooks", options),
    getWebhook: (webhookId, options) =>
      get(`/api/webhooks/${encodeURIComponent(webhookId)}`, options),
    putWebhook: (webhookId, request, idempotencyKey, options) =>
      mutate(
        "PUT",
        `/api/webhooks/${encodeURIComponent(webhookId)}`,
        request,
        idempotencyKey,
        options
      ),
    deleteWebhook: (webhookId, request, idempotencyKey, options) =>
      mutate(
        "DELETE",
        `/api/webhooks/${encodeURIComponent(webhookId)}`,
        request,
        idempotencyKey,
        options
      ),
    testWebhook: (webhookId, request, idempotencyKey, options) =>
      mutate(
        "POST",
        `/api/webhooks/${encodeURIComponent(webhookId)}/test`,
        request,
        idempotencyKey,
        options
      ),
    listServices: (options) => get("/api/services", options),
    getService: (serviceId, options) =>
      get(`/api/services/${encodeURIComponent(serviceId)}`, options),
    listDeployments: (serviceId, options) =>
      get(`/api/services/${encodeURIComponent(serviceId)}/deployments`, options),
    getDeployment: (serviceId, deploymentId, options) =>
      get(
        `/api/services/${encodeURIComponent(serviceId)}/deployments/${encodeURIComponent(deploymentId)}`,
        options
      ),
    listAssignments: (serviceId, deploymentId, options) =>
      get(
        `/api/services/${encodeURIComponent(serviceId)}/deployments/${encodeURIComponent(deploymentId)}/assignments`,
        options
      ),
    getAssignment: (serviceId, deploymentId, assignmentId, options) =>
      get(
        `/api/services/${encodeURIComponent(serviceId)}/deployments/${encodeURIComponent(deploymentId)}/assignments/${encodeURIComponent(assignmentId)}`,
        options
      ),
    listReplicas: (serviceId, deploymentId, options) =>
      get(
        `/api/services/${encodeURIComponent(serviceId)}/deployments/${encodeURIComponent(deploymentId)}/replicas`,
        options
      ),
    getReplica: (serviceId, deploymentId, replicaId, options) =>
      get(
        `/api/services/${encodeURIComponent(serviceId)}/deployments/${encodeURIComponent(deploymentId)}/replicas/${encodeURIComponent(replicaId)}`,
        options
      ),
    listBuilds: (serviceId, options) =>
      get(`/api/services/${encodeURIComponent(serviceId)}/builds`, options),
    getBuild: (serviceId, buildId, options) =>
      get(
        `/api/services/${encodeURIComponent(serviceId)}/builds/${encodeURIComponent(buildId)}`,
        options
      ),
    listIngressRoutes: (serviceId, options) =>
      get(`/api/services/${encodeURIComponent(serviceId)}/routes`, options),
    getIngressRoute: (serviceId, routeId, options) =>
      get(
        `/api/services/${encodeURIComponent(serviceId)}/routes/${encodeURIComponent(routeId)}`,
        options
      ),
    listServiceMetrics: (serviceId, query, options) =>
      get(withQuery(`/api/services/${encodeURIComponent(serviceId)}/metrics`, query), options),
    listContainerMetrics: (serviceId, query, options) =>
      get(
        withQuery(`/api/services/${encodeURIComponent(serviceId)}/metrics/containers`, query),
        options
      ),
    listActiveIngressRoutes: (options) => get("/api/ingress/routes", options),
    getIngressBlocklist: (options) => get("/api/ingress/blocked-ips", options),
    setBlockedIngressIp: (request, options) =>
      submit("PATCH", "/api/ingress/blocked-ips", request, options),
    getIngressTraffic: (query, options) => get(withQuery("/api/ingress/traffic", query), options),
    getBlockedIngressTraffic: (query, options) =>
      get(withQuery("/api/ingress/blocked-traffic", query), options),
    listTrafficGenerations: (serviceId, options) =>
      get(`/api/services/${encodeURIComponent(serviceId)}/traffic-generations`, options),
    getTrafficGeneration: (serviceId, generationId, options) =>
      get(
        `/api/services/${encodeURIComponent(serviceId)}/traffic-generations/${encodeURIComponent(generationId)}`,
        options
      ),
    redeployService: (serviceId, request, idempotencyKey, options) =>
      mutate(
        "POST",
        `/api/services/${encodeURIComponent(serviceId)}/redeploy`,
        request,
        idempotencyKey,
        options
      ),
    freezeService: (serviceId, request, idempotencyKey, options) =>
      mutate(
        "POST",
        `/api/services/${encodeURIComponent(serviceId)}/freeze`,
        request,
        idempotencyKey,
        options
      ),
    unfreezeService: (serviceId, request, idempotencyKey, options) =>
      mutate(
        "POST",
        `/api/services/${encodeURIComponent(serviceId)}/unfreeze`,
        request,
        idempotencyKey,
        options
      ),
    setServiceReplicas: (serviceId, request, idempotencyKey, options) =>
      mutate(
        "PUT",
        `/api/services/${encodeURIComponent(serviceId)}/replicas`,
        request,
        idempotencyKey,
        options
      ),
    deleteService: (serviceId, request, idempotencyKey, options) =>
      mutate(
        "DELETE",
        `/api/services/${encodeURIComponent(serviceId)}`,
        request,
        idempotencyKey,
        options
      ),
    restartDeployment: (serviceId, deploymentId, request, idempotencyKey, options) =>
      mutate(
        "POST",
        `/api/services/${encodeURIComponent(serviceId)}/deployments/${encodeURIComponent(deploymentId)}/restart`,
        request,
        idempotencyKey,
        options
      ),
    cancelDeployment: (serviceId, deploymentId, request, idempotencyKey, options) =>
      mutate(
        "POST",
        `/api/services/${encodeURIComponent(serviceId)}/deployments/${encodeURIComponent(deploymentId)}/cancel`,
        request,
        idempotencyKey,
        options
      ),
    removeDeployment: (serviceId, deploymentId, request, idempotencyKey, options) =>
      mutate(
        "POST",
        `/api/services/${encodeURIComponent(serviceId)}/deployments/${encodeURIComponent(deploymentId)}/remove`,
        request,
        idempotencyKey,
        options
      ),
    putService: (serviceId, request, idempotencyKey, options) =>
      mutate(
        "PUT",
        `/api/services/${encodeURIComponent(serviceId)}`,
        request,
        idempotencyKey,
        options
      )
  };
}
