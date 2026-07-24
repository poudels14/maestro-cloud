import type { MaestroApiClient } from "./client";
import { decodeJson } from "./transport";
import type { ApiRequestOptions, ApiTransport, TransportRequest } from "./transport";

function createApiClient(transport: ApiTransport): MaestroApiClient {
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
    getClusterConfig: (options) => get("/api/config", options),
    getClusterInfo: (options) => get("/api/cluster", options),
    getClusterStats: (options) => get("/api/cluster/stats", options),
    listLocalDisks: (options) => get("/api/disks", options),
    listNodeDisks: (options) => get("/api/disks/nodes", options),
    listNodeMetrics: (query, options) => get(withQuery("/api/metrics/node", query), options),
    listClusterMetrics: (query, options) => get(withQuery("/api/metrics/cluster", query), options),
    listOperationalStatsMetrics: (query, options) =>
      get(withQuery("/api/metrics/stats", query), options),
    listNodes: (options) => get("/api/cluster/nodes", options),
    listClusterAdmissions: (options) => get("/api/cluster/admissions", options),
    approveClusterAdmission: (request, options) =>
      submit("POST", "/api/cluster/admissions", request, options),
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
    removeNode: (nodeId, request, idempotencyKey, options) =>
      mutate(
        "DELETE",
        `/api/cluster/nodes/${encodeURIComponent(nodeId)}`,
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
    listLogs: (query, options) => get(withQuery("/api/logs", query), options),
    listSystemLogs: (query, options) => get(withQuery("/api/system/logs", query), options),
    listServiceLogs: (serviceId, query, options) =>
      get(withQuery(`/api/services/${encodeURIComponent(serviceId)}/logs`, query), options),
    listDeploymentLogs: (serviceId, deploymentId, query, options) =>
      get(
        withQuery(
          `/api/services/${encodeURIComponent(serviceId)}/deployments/${encodeURIComponent(deploymentId)}/logs`,
          query
        ),
        options
      ),
    listBuildLogs: (serviceId, buildId, query, options) =>
      get(
        withQuery(
          `/api/services/${encodeURIComponent(serviceId)}/builds/${encodeURIComponent(buildId)}/logs`,
          query
        ),
        options
      ),
    getLogHistogram: (query, options) => get(withQuery("/api/logs/histogram", query), options),
    getSystemLogHistogram: (query, options) =>
      get(withQuery("/api/system/logs/histogram", query), options),
    getServiceLogHistogram: (serviceId, query, options) =>
      get(
        withQuery(`/api/services/${encodeURIComponent(serviceId)}/logs/histogram`, query),
        options
      ),
    getDeploymentLogHistogram: (serviceId, deploymentId, query, options) =>
      get(
        withQuery(
          `/api/services/${encodeURIComponent(serviceId)}/deployments/${encodeURIComponent(deploymentId)}/logs/histogram`,
          query
        ),
        options
      ),
    getBuildLogHistogram: (serviceId, buildId, query, options) =>
      get(
        withQuery(
          `/api/services/${encodeURIComponent(serviceId)}/builds/${encodeURIComponent(buildId)}/logs/histogram`,
          query
        ),
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
    getServiceTraffic: (serviceId, query, options) =>
      get(withQuery(`/api/services/${encodeURIComponent(serviceId)}/traffic`, query), options),
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

export { createApiClient };
