import type {
  ClusterConfig,
  ClusterInfo,
  ClusterNode,
  Deployment,
  DiskInfo,
  IngressBlocklist,
  IngressTrafficBreakdown,
  LogEntry,
  MetricPoint,
  ReplicaState,
  Service,
  ServiceSpec,
  SlackCategory,
  SlackWebhook,
  Stats,
  TrafficPoint,
  UnschedulableReplica
} from "./types";

type ReplicaPlacement = {
  replicas: Omit<ReplicaState, "index" | "nodeId">[];
  assignments: { replicaId: string; deploymentId: string; nodeId: number; index: number }[];
};

type ServiceSummary = Pick<Service, "id" | "deployFrozen" | "replicasOverride" | "previewSource"> &
  ReplicaPlacement & {
    spec: ServiceSpec;
    latestDeployment?: Omit<Deployment, "replicas"> | null;
  };

type ServiceDetail = ReplicaPlacement & {
  deployments: Omit<Deployment, "replicas">[];
};

type LogRecord = {
  seq: number;
  ts: number;
  replicaId: string;
  serviceId?: string | null;
  deploymentId?: string | null;
  stream: LogEntry["stream"];
  level: string;
  line: string;
  message?: string;
  attributes?: Record<string, unknown>;
  nodeId?: number;
  nodeName?: string;
};

type NodeError = {
  nodeId: number;
  nodeName: string;
  error: string;
};

type LogPage = {
  entries: LogEntry[];
  nextCursor: number | null;
  partial: boolean;
  unavailableNodes: NodeError[];
};

type LogHistogramBucket = {
  ts: number;
  count: number;
  levels?: Record<string, number>;
};

type LogHistogram = {
  from: number;
  to: number;
  bucketMs: number;
  buckets: LogHistogramBucket[];
  partial?: boolean;
  unavailableNodes?: NodeError[];
};

type MaintenanceTarget =
  | { kind: "upgrade"; targetVersion: string }
  | { kind: "restart"; nodeId?: number };

async function getClusterInfo(): Promise<ClusterInfo> {
  return request("/api/cluster");
}

async function getClusterNodes(): Promise<ClusterNode[]> {
  const nodes = await request<ClusterNode[]>("/api/nodes");
  return nodes.filter((node) => node.state !== "removed");
}

async function getUnschedulableReplicas(): Promise<UnschedulableReplica[]> {
  return request("/api/cluster/unschedulable");
}

async function setNodeDrain(nodeId: number, drain: boolean): Promise<void> {
  const operation = drain ? "drain" : "restore";
  await request(`/api/cluster/nodes/${nodeId}/${operation}`, { method: "POST" });
}

async function removeNode(nodeId: number): Promise<{ state: "draining" | "removed" }> {
  return request(`/api/cluster/nodes/${nodeId}`, { method: "DELETE" });
}

async function startMaintenance(target: MaintenanceTarget): Promise<void> {
  const body =
    target.kind === "upgrade" ? { targetVersion: target.targetVersion } : { nodeId: target.nodeId };
  await request(`/api/cluster/${target.kind}`, jsonBody("POST", body));
}

async function unfreezeCluster(): Promise<void> {
  await request("/api/cluster/unfreeze", { method: "POST" });
}

async function getClusterStats(): Promise<Stats.Cluster> {
  return request("/api/cluster/stats");
}

async function getClusterConfig(): Promise<ClusterConfig> {
  return request("/api/config");
}

async function getServices(): Promise<Service[]> {
  const summaries = await request<ServiceSummary[]>("/api/services");
  return summaries.map(({ spec, replicas, assignments, latestDeployment, ...summary }) => ({
    ...spec,
    ...summary,
    latestDeployment: latestDeployment && {
      ...latestDeployment,
      replicas: deploymentReplicas({ replicas, assignments }, latestDeployment.id)
    },
    status: latestDeployment?.status ?? "idle"
  }));
}

async function getDeployments(serviceId: string): Promise<Deployment[]> {
  const detail = await request<ServiceDetail>(`/api/services/${encodeURIComponent(serviceId)}`);
  return detail.deployments.map((deployment) => ({
    ...deployment,
    replicas: deploymentReplicas(detail, deployment.id)
  }));
}

async function getBuildLog(serviceId: string, deploymentId: string): Promise<string | null> {
  const response = await fetch(
    `/api/services/${encodeURIComponent(serviceId)}/deployments/${encodeURIComponent(deploymentId)}/build-log`
  );
  if (response.status === 404) {
    return null;
  } else if (response.ok) {
    return response.text();
  } else {
    throw new Error(await errorMessage(response));
  }
}

async function deleteService(serviceId: string) {
  await request(`/api/services/${encodeURIComponent(serviceId)}`, { method: "DELETE" });
}

async function redeployService(serviceId: string, force?: boolean) {
  await request(serviceAction(serviceId, "redeploy", force), { method: "POST" });
}

async function restartService(serviceId: string, force?: boolean) {
  await request(serviceAction(serviceId, "restart", force), { method: "POST" });
}

async function freezeService(serviceId: string, frozen: boolean) {
  await request(
    `/api/services/${encodeURIComponent(serviceId)}/freeze`,
    jsonBody("PATCH", { frozen })
  );
}

async function setServiceReplicas(serviceId: string, replicas: number) {
  await request(
    `/api/services/${encodeURIComponent(serviceId)}/replicas`,
    jsonBody("PATCH", { replicas })
  );
}

async function clearServiceReplicasOverride(serviceId: string) {
  await request(`/api/services/${encodeURIComponent(serviceId)}/replicas`, { method: "DELETE" });
}

async function cancelDeployment(serviceId: string, deploymentId: string) {
  await request(deploymentAction(serviceId, deploymentId, "cancel"), { method: "PATCH" });
}

async function stopDeployment(serviceId: string, deploymentId: string) {
  await request(deploymentAction(serviceId, deploymentId, "remove"), { method: "PATCH" });
}

async function queryLogs(params: {
  query: string;
  limit: number;
  beforeTs?: number;
  afterTs?: number;
  nodeIds?: string;
}): Promise<LogPage> {
  const page = await request<Omit<LogPage, "entries"> & { records: LogRecord[] }>(
    withParams("/api/logs", params)
  );
  return {
    entries: page.records.map(toLogEntry).reverse(),
    nextCursor: page.nextCursor,
    partial: page.partial,
    unavailableNodes: page.unavailableNodes
  };
}

async function getLogHistogram(params: {
  query: string;
  from: number;
  to: number;
  bucketMs?: number;
  groupBy?: "level" | "status";
  nodeIds?: string;
}): Promise<LogHistogram> {
  return request(withParams("/api/logs/histogram", params));
}

async function getDisks(): Promise<Record<string, DiskInfo[]>> {
  return request("/api/disks/nodes");
}

async function getServiceMetrics(
  serviceId: string,
  from?: number,
  to?: number
): Promise<MetricPoint[]> {
  return request(
    withParams(`/api/services/${encodeURIComponent(serviceId)}/metrics`, { from, to })
  );
}

async function getServiceTraffic(
  serviceId: string,
  from?: number,
  to?: number
): Promise<TrafficPoint[]> {
  return request(
    withParams(`/api/services/${encodeURIComponent(serviceId)}/traffic`, { from, to })
  );
}

async function getIngressTraffic(from: number, to: number): Promise<IngressTrafficBreakdown> {
  return request(withParams("/api/ingress/traffic", { from, to, limit: 200 }));
}

async function getBlockedIngressTraffic(
  from: number,
  to: number
): Promise<IngressTrafficBreakdown> {
  return request(withParams("/api/ingress/blocked-traffic", { from, to, limit: 200 }));
}

async function setBlockedIngressIp(ip: string, blocked: boolean): Promise<IngressBlocklist> {
  return request("/api/ingress/blocked-ips", jsonBody("PATCH", { ip, blocked }));
}

async function getIngressBlocklist(): Promise<IngressBlocklist> {
  return request("/api/ingress/blocked-ips");
}

async function getNodeMetrics(from?: number, to?: number, nodeId?: number): Promise<MetricPoint[]> {
  return request(withParams("/api/metrics/node", { from, to, nodeId }));
}

async function getClusterMetrics(from?: number, to?: number): Promise<MetricPoint[]> {
  return request(withParams("/api/metrics/cluster", { from, to }));
}

async function listSlackWebhooks(): Promise<SlackWebhook[]> {
  return request("/api/webhooks/slack");
}

async function createSlackWebhook(payload: {
  name: string;
  url: string;
  categories: SlackCategory[];
  enabled?: boolean;
}): Promise<SlackWebhook> {
  return request("/api/webhooks/slack", jsonBody("POST", payload));
}

async function updateSlackWebhook(
  id: string,
  patch: Partial<Omit<SlackWebhook, "id">>
): Promise<SlackWebhook> {
  return request(`/api/webhooks/slack/${encodeURIComponent(id)}`, jsonBody("PATCH", patch));
}

async function deleteSlackWebhook(id: string): Promise<void> {
  await request(`/api/webhooks/slack/${encodeURIComponent(id)}`, { method: "DELETE" });
}

async function testSlackWebhook(id: string): Promise<void> {
  await request(`/api/webhooks/slack/${encodeURIComponent(id)}/test`, { method: "POST" });
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(path, init);
  if (!response.ok) throw new Error(await errorMessage(response));
  const body = await response.text();
  return (body ? JSON.parse(body) : undefined) as T;
}

async function errorMessage(response: Response): Promise<string> {
  const body = await response.text();
  try {
    return JSON.parse(body).error ?? body;
  } catch {
    return body || response.statusText;
  }
}

function jsonBody(method: string, body: unknown): RequestInit {
  return {
    method,
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body)
  };
}

function withParams(path: string, params: Record<string, string | number | undefined>): string {
  const search = new URLSearchParams(
    Object.entries(params).flatMap(([name, value]) =>
      value === undefined || value === "" ? [] : [[name, String(value)]]
    )
  ).toString();
  return search ? `${path}?${search}` : path;
}

function serviceAction(serviceId: string, action: string, force?: boolean): string {
  return withParams(`/api/services/${encodeURIComponent(serviceId)}/${action}`, {
    force: force ? "true" : undefined
  });
}

function deploymentAction(serviceId: string, deploymentId: string, action: string): string {
  return `/api/services/${encodeURIComponent(serviceId)}/deployments/${encodeURIComponent(deploymentId)}/${action}`;
}

function toLogEntry(record: LogRecord): LogEntry {
  return {
    seq: record.seq,
    ts: record.ts,
    level: record.level,
    stream: record.stream,
    text: record.line,
    message: record.message,
    source: record.replicaId,
    nodeId: record.nodeId,
    nodeName: record.nodeName,
    serviceId: record.serviceId ?? undefined,
    deploymentId: record.deploymentId ?? undefined,
    attrs: record.attributes ? flattenAttributes(record.attributes, "") : undefined
  };
}

function flattenAttributes(value: Record<string, unknown>, prefix: string): [string, string][] {
  return Object.entries(value).flatMap(([name, nested]): [string, string][] => {
    const path = prefix ? `${prefix}.${name}` : name;
    if (nested !== null && typeof nested === "object" && !Array.isArray(nested)) {
      return flattenAttributes(nested as Record<string, unknown>, path);
    } else {
      return [[path, typeof nested === "string" ? nested : JSON.stringify(nested)]];
    }
  });
}

function deploymentReplicas(placement: ReplicaPlacement, deploymentId: string): ReplicaState[] {
  return placement.assignments
    .filter((assignment) => assignment.deploymentId === deploymentId)
    .map((assignment) => {
      const status = placement.replicas.find(
        (replica) => replica.replicaId === assignment.replicaId
      );
      return {
        replicaId: assignment.replicaId,
        deploymentId: assignment.deploymentId,
        index: assignment.index,
        nodeId: assignment.nodeId,
        phase: status?.phase ?? "pending",
        ip: status?.ip,
        restarts: status?.restarts ?? 0,
        consecutiveFailures: status?.consecutiveFailures ?? 0,
        lastError: status?.lastError
      };
    })
    .sort((left, right) => left.index - right.index);
}

export {
  getClusterInfo,
  getClusterNodes,
  getUnschedulableReplicas,
  setNodeDrain,
  removeNode,
  startMaintenance,
  unfreezeCluster,
  getClusterStats,
  getClusterConfig,
  getServices,
  getDeployments,
  getBuildLog,
  deleteService,
  redeployService,
  restartService,
  freezeService,
  setServiceReplicas,
  clearServiceReplicasOverride,
  cancelDeployment,
  stopDeployment,
  queryLogs,
  getLogHistogram,
  getDisks,
  getServiceMetrics,
  getServiceTraffic,
  getIngressTraffic,
  getBlockedIngressTraffic,
  setBlockedIngressIp,
  getIngressBlocklist,
  getNodeMetrics,
  getClusterMetrics,
  listSlackWebhooks,
  createSlackWebhook,
  updateSlackWebhook,
  deleteSlackWebhook,
  testSlackWebhook
};

export type { NodeError, LogPage, LogHistogramBucket, LogHistogram, MaintenanceTarget };
