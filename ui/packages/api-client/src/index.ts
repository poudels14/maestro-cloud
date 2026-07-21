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
