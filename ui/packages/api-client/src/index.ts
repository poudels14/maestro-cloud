export type { components, paths, webhooks } from "./schema";

import type { components } from "./schema";

export type ApiSchemas = components["schemas"];

export interface BuiltinResources {
  Node: ApiSchemas["Node"];
  NodeNetwork: ApiSchemas["NodeNetwork"];
  Service: ApiSchemas["Service"];
  Deployment: ApiSchemas["Deployment"];
  Assignment: ApiSchemas["Assignment"];
  ReplicaState: ApiSchemas["ReplicaState"];
  IngressRoute: ApiSchemas["IngressRoute"];
  TrafficGeneration: ApiSchemas["TrafficGeneration"];
  FirewallPolicy: ApiSchemas["FirewallPolicy"];
  DnsRecord: ApiSchemas["DnsRecord"];
  Build: ApiSchemas["Build"];
  Preview: ApiSchemas["Preview"];
  UpgradeRun: ApiSchemas["UpgradeRun"];
  Webhook: ApiSchemas["Webhook"];
}

export type BuiltinResourceKind = keyof BuiltinResources;
export type BuiltinResource<Kind extends BuiltinResourceKind> = BuiltinResources[Kind];

export interface TransportRequest<Response, Body = never> {
  method: "DELETE" | "GET" | "PATCH" | "POST" | "PUT";
  path: string;
  body?: Body;
  headers?: Readonly<Record<string, string>>;
  signal?: AbortSignal;
  decode: (response: globalThis.Response) => Promise<Response>;
}

export interface ApiTransport {
  request<Response, Body = never>(request: TransportRequest<Response, Body>): Promise<Response>;
}

export class ApiHttpError extends Error {
  readonly status: number;
  readonly body: string;

  constructor(status: number, body: string) {
    super(`Maestro API request failed with HTTP ${status}`);
    this.name = "ApiHttpError";
    this.status = status;
    this.body = body;
  }
}

export function createFetchTransport(
  baseUrl: string | URL,
  fetcher: typeof globalThis.fetch = globalThis.fetch
): ApiTransport {
  const base = new URL(baseUrl);
  return {
    async request<Response, Body = never>(
      request: TransportRequest<Response, Body>
    ): Promise<Response> {
      const headers = new Headers(request.headers);
      const hasBody = request.body !== undefined;
      if (hasBody && !headers.has("content-type")) {
        headers.set("content-type", "application/json");
      }
      const init: RequestInit = {
        method: request.method,
        headers
      };
      if (hasBody) {
        init.body = JSON.stringify(request.body);
      }
      if (request.signal !== undefined) {
        init.signal = request.signal;
      }
      const response = await fetcher(new URL(request.path, base), init);
      if (!response.ok) {
        throw new ApiHttpError(response.status, await response.text());
      }
      return request.decode(response);
    }
  };
}

export async function decodeJson<Response>(response: globalThis.Response): Promise<Response> {
  return (await response.json()) as Response;
}

export interface ApiRequestOptions {
  headers?: Readonly<Record<string, string>>;
  signal?: AbortSignal;
}

export interface MaestroApiClient {
  listNodes(options?: ApiRequestOptions): Promise<ApiSchemas["Node"][]>;
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
  getUpgrade(
    upgradeRunId: string,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["UpgradeRun"]>;
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
  getDnsRecord(
    recordId: string,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["DnsRecord"]>;
  listFirewallPolicies(options?: ApiRequestOptions): Promise<ApiSchemas["FirewallPolicy"][]>;
  getFirewallPolicy(
    policyId: string,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["FirewallPolicy"]>;
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
  listBuilds(
    serviceId: string,
    options?: ApiRequestOptions
  ): Promise<ApiSchemas["Build"][]>;
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
    request: ApiSchemas["ReplicaOverrideRequest"],
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

  return {
    listNodes: (options) => get("/api/cluster/nodes", options),
    getNode: (nodeId, options) =>
      get(`/api/cluster/nodes/${encodeURIComponent(nodeId)}`, options),
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
    listTrafficGenerations: (serviceId, options) =>
      get(
        `/api/services/${encodeURIComponent(serviceId)}/traffic-generations`,
        options
      ),
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
