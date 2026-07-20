import type {
  ClusterNode,
  Deployment,
  DiskInfo,
  IngressBlocklist,
  IngressTrafficBreakdown,
  IngressRouting,
  LogEntry,
  MaskedConfig,
  MetricPoint,
  ClusterStats,
  Service,
  SlackCategory,
  SlackWebhook,
  StatsMetricPoint,
  TrafficPoint,
  UnschedulableReplica
} from "./types";
import { apiErrorFromResponse } from "./apiError";

export interface ClusterInfo {
  clusterId?: string | null;
  thisNodeId?: string | null;
  leader?: string | null;
  clusterName: string;
  clusterAlias: string;
  canonicalDomain: string;
  aliasDomain: string;
  aliasStatus?: "active" | "conflicted" | "unknown" | "inactive";
  version?: string;
  upgrading?: boolean;
  restarting?: boolean;
  upgradeRun?: ClusterMaintenanceRun | null;
  nodes?: ClusterNode[];
}

export interface ClusterMaintenanceNodeStep {
  nodeId: string;
  hostname: string;
  status: string;
  upgradeStage?: string | null;
}

export interface ClusterMaintenanceRun {
  kind: "upgrade" | "restart";
  targetVersion: string;
  requestedAtMs: number;
  phase: string;
  currentNodeIndex: number;
  nodes: ClusterMaintenanceNodeStep[];
}

export async function getClusterNodes(): Promise<ClusterNode[]> {
  const res = await fetch("/api/cluster/nodes");
  if (!res.ok) throw new Error(`Failed to fetch cluster nodes: ${res.statusText}`);
  return res.json();
}

export async function getUnschedulableReplicas(): Promise<UnschedulableReplica[]> {
  const res = await fetch("/api/cluster/unschedulable");
  if (!res.ok) throw new Error(`Failed to fetch scheduling errors: ${res.statusText}`);
  return res.json();
}

export async function setNodeDrain(nodeId: string, drain: boolean): Promise<void> {
  const operation = drain ? "drain" : "restore";
  const res = await fetch(`/api/cluster/nodes/${encodeURIComponent(nodeId)}/${operation}`, {
    method: "POST"
  });
  if (!res.ok) throw new Error((await res.text()) || `Failed to ${operation} node`);
}

export async function getClusterInfo(): Promise<ClusterInfo> {
  const res = await fetch("/api/cluster");
  if (!res.ok) throw new Error(`Failed to fetch cluster info: ${res.statusText}`);
  return res.json();
}

export async function getClusterStats(): Promise<ClusterStats> {
  const res = await fetch("/api/cluster/stats");
  if (!res.ok) throw new Error(`Failed to fetch cluster stats: ${res.statusText}`);
  return res.json();
}

export async function getStatsMetrics(
  name?: string,
  from?: number,
  to?: number
): Promise<StatsMetricPoint[]> {
  const url = new URL("/api/metrics/stats", location.origin);
  if (name) url.searchParams.set("name", name);
  if (from != null) url.searchParams.set("from", String(from));
  if (to != null) url.searchParams.set("to", String(to));
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Failed to fetch stats metrics: ${res.statusText}`);
  return res.json();
}

export async function getClusterConfig(): Promise<MaskedConfig> {
  const res = await fetch("/api/config");
  if (!res.ok) throw new Error(`Failed to fetch cluster config: ${res.statusText}`);
  return res.json();
}

export async function getServices(): Promise<Service[]> {
  const res = await fetch("/api/services");
  if (!res.ok) throw new Error(`Failed to fetch services: ${res.statusText}`);
  return res.json();
}

export async function getDeployments(serviceId: string): Promise<Deployment[]> {
  const url = `/api/services/${encodeURIComponent(serviceId)}/deployments`;
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Failed to fetch deployments: ${res.statusText}`);
  return res.json();
}

export async function deleteService(serviceId: string) {
  const res = await fetch(`/api/services/${encodeURIComponent(serviceId)}`, {
    method: "DELETE"
  });
  if (!res.ok) throw new Error(`Failed to delete service: ${res.statusText}`);
}

export async function redeployService(serviceId: string, force?: boolean) {
  const url = new URL(`/api/services/${encodeURIComponent(serviceId)}/redeploy`, location.origin);
  if (force) url.searchParams.set("force", "true");
  const res = await fetch(url, { method: "POST" });
  if (!res.ok) throw await apiErrorFromResponse(res, "Failed to redeploy");
}

export async function restartService(serviceId: string, force?: boolean) {
  const url = new URL(`/api/services/${encodeURIComponent(serviceId)}/restart`, location.origin);
  if (force) url.searchParams.set("force", "true");
  const res = await fetch(url, { method: "POST" });
  if (!res.ok) throw await apiErrorFromResponse(res, "Failed to restart");
}

export async function freezeService(serviceId: string, frozen: boolean) {
  const res = await fetch(`/api/services/${encodeURIComponent(serviceId)}/freeze`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ frozen })
  });
  if (!res.ok) throw new Error(`Failed to update freeze status: ${res.statusText}`);
}

export async function setServiceReplicas(serviceId: string, replicas: number) {
  const res = await fetch(`/api/services/${encodeURIComponent(serviceId)}/replicas`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ replicas })
  });
  if (!res.ok) {
    const body = await res.text();
    throw new Error(body || `Failed to update replicas: ${res.statusText}`);
  }
}

export async function clearServiceReplicasOverride(serviceId: string) {
  const res = await fetch(`/api/services/${encodeURIComponent(serviceId)}/replicas`, {
    method: "DELETE"
  });
  if (!res.ok) throw new Error(`Failed to clear replicas override: ${res.statusText}`);
}

export async function cancelDeployment(serviceId: string, deploymentId: string) {
  const url = `/api/services/${encodeURIComponent(serviceId)}/deployments/${encodeURIComponent(deploymentId)}/cancel`;
  const res = await fetch(url, { method: "PATCH" });
  if (!res.ok) throw new Error(`Failed to cancel deployment: ${res.statusText}`);
}

export async function stopDeployment(serviceId: string, deploymentId: string) {
  const url = `/api/services/${encodeURIComponent(serviceId)}/deployments/${encodeURIComponent(deploymentId)}/remove`;
  const res = await fetch(url, { method: "PATCH" });
  if (!res.ok) throw new Error(`Failed to stop deployment: ${res.statusText}`);
}

export async function getLogs(
  serviceId: string,
  deploymentId: string,
  tail?: number,
  afterSeq?: number,
  beforeSeq?: number,
  phase?: "build" | "deploy",
  query?: string,
  from?: number,
  to?: number
): Promise<LogPage> {
  const url = new URL(
    `/api/services/${encodeURIComponent(serviceId)}/deployments/${encodeURIComponent(deploymentId)}/logs`,
    location.origin
  );
  if (tail != null) url.searchParams.set("tail", String(tail));
  if (afterSeq != null) url.searchParams.set("after", String(afterSeq));
  if (beforeSeq != null) url.searchParams.set("before", String(beforeSeq));
  if (phase != null) url.searchParams.set("phase", phase);
  if (query) url.searchParams.set("query", query);
  if (from != null) url.searchParams.set("from", String(from));
  if (to != null) url.searchParams.set("to", String(to));
  const res = await fetch(url);
  if (!res.ok) {
    const body = await res.text();
    throw new Error(body || `Failed to fetch logs: ${res.statusText}`);
  }
  const raw = await res.json();
  return mapLogPage(res, raw);
}

export async function getServiceLogs(
  serviceId: string,
  tail?: number,
  afterSeq?: number,
  beforeSeq?: number,
  phase?: "build" | "deploy",
  query?: string,
  from?: number,
  to?: number
): Promise<LogPage> {
  const url = new URL(`/api/services/${encodeURIComponent(serviceId)}/logs`, location.origin);
  if (tail != null) url.searchParams.set("tail", String(tail));
  if (afterSeq != null) url.searchParams.set("after", String(afterSeq));
  if (beforeSeq != null) url.searchParams.set("before", String(beforeSeq));
  if (phase != null) url.searchParams.set("phase", phase);
  if (query) url.searchParams.set("query", query);
  if (from != null) url.searchParams.set("from", String(from));
  if (to != null) url.searchParams.set("to", String(to));
  const res = await fetch(url);
  if (!res.ok) {
    const body = await res.text();
    throw new Error(body || `Failed to fetch service logs: ${res.statusText}`);
  }
  const raw = await res.json();
  return mapLogPage(res, raw);
}

export async function getSystemLogs(
  name: string,
  tail?: number,
  afterSeq?: number,
  beforeSeq?: number,
  query?: string,
  from?: number,
  to?: number
): Promise<LogPage> {
  const url = new URL(`/api/system/${encodeURIComponent(name)}/logs`, location.origin);
  if (tail != null) url.searchParams.set("tail", String(tail));
  if (afterSeq != null) url.searchParams.set("after", String(afterSeq));
  if (beforeSeq != null) url.searchParams.set("before", String(beforeSeq));
  if (query) url.searchParams.set("query", query);
  if (from != null) url.searchParams.set("from", String(from));
  if (to != null) url.searchParams.set("to", String(to));
  const res = await fetch(url);
  if (!res.ok) {
    const body = await res.text();
    throw new Error(body || `Failed to fetch system logs: ${res.statusText}`);
  }
  const raw = await res.json();
  return mapLogPage(res, raw);
}

export interface LogHistogramBucket {
  ts: number;
  count: number;
  levels?: Record<string, number>;
}

export interface LogHistogram {
  from: number;
  to: number;
  bucketMs: number;
  buckets: LogHistogramBucket[];
}

export async function getServiceLogHistogram(
  serviceId: string,
  from: number,
  to: number,
  phase?: "build" | "deploy",
  query?: string,
  bucketMs?: number,
  groupBy?: "level" | "status"
): Promise<LogHistogram> {
  const url = new URL(
    `/api/services/${encodeURIComponent(serviceId)}/logs/histogram`,
    location.origin
  );
  url.searchParams.set("from", String(from));
  url.searchParams.set("to", String(to));
  if (phase != null) url.searchParams.set("phase", phase);
  if (query) url.searchParams.set("query", query);
  if (bucketMs != null) url.searchParams.set("bucketMs", String(bucketMs));
  if (groupBy) url.searchParams.set("groupBy", groupBy);
  const res = await fetch(url);
  if (!res.ok) {
    const body = await res.text();
    throw new Error(body || `Failed to fetch log histogram: ${res.statusText}`);
  }
  return res.json();
}

export async function getSystemLogHistogram(
  name: string,
  from: number,
  to: number,
  query?: string,
  bucketMs?: number,
  groupBy?: "level" | "status"
): Promise<LogHistogram> {
  const url = new URL(`/api/system/${encodeURIComponent(name)}/logs/histogram`, location.origin);
  url.searchParams.set("from", String(from));
  url.searchParams.set("to", String(to));
  if (query) url.searchParams.set("query", query);
  if (bucketMs != null) url.searchParams.set("bucketMs", String(bucketMs));
  if (groupBy) url.searchParams.set("groupBy", groupBy);
  const res = await fetch(url);
  if (!res.ok) {
    const body = await res.text();
    throw new Error(body || `Failed to fetch log histogram: ${res.statusText}`);
  }
  return res.json();
}

export interface LogPage {
  entries: LogEntry[];
  cursor: number;
}

export interface ClusterLogNodeError {
  nodeId: string;
  nodeName: string;
  error: string;
}

export interface ClusterLogPage {
  entries: LogEntry[];
  cursor: string;
  partial: boolean;
  unavailableNodes: ClusterLogNodeError[];
}

export interface ClusterLogHistogram extends LogHistogram {
  partial: boolean;
  unavailableNodes: ClusterLogNodeError[];
}

export async function getClusterLogs(params: {
  tail?: number;
  cursor?: string;
  nodeId?: string;
  serviceId?: string;
  query?: string;
  from?: number;
  to?: number;
}): Promise<ClusterLogPage> {
  const url = new URL("/api/cluster/logs", location.origin);
  if (params.tail != null) url.searchParams.set("tail", String(params.tail));
  if (params.cursor) url.searchParams.set("cursor", params.cursor);
  if (params.nodeId) url.searchParams.set("nodeId", params.nodeId);
  if (params.serviceId) url.searchParams.set("serviceId", params.serviceId);
  if (params.query) url.searchParams.set("query", params.query);
  if (params.from != null) url.searchParams.set("from", String(params.from));
  if (params.to != null) url.searchParams.set("to", String(params.to));
  const res = await fetch(url);
  if (!res.ok) {
    const body = await res.text();
    throw new Error(body || `Failed to fetch cluster logs: ${res.statusText}`);
  }
  const raw = (await res.json()) as Omit<ClusterLogPage, "entries"> & {
    entries: Record<string, unknown>[];
  };
  return { ...raw, entries: mapLogEntries(raw.entries) };
}

export async function getClusterLogHistogram(params: {
  from: number;
  to: number;
  nodeId?: string;
  serviceId?: string;
  query?: string;
  bucketMs?: number;
  groupBy?: "level" | "status";
}): Promise<ClusterLogHistogram> {
  const url = new URL("/api/cluster/logs/histogram", location.origin);
  url.searchParams.set("from", String(params.from));
  url.searchParams.set("to", String(params.to));
  if (params.nodeId) url.searchParams.set("nodeId", params.nodeId);
  if (params.serviceId) url.searchParams.set("serviceId", params.serviceId);
  if (params.query) url.searchParams.set("query", params.query);
  if (params.bucketMs != null) url.searchParams.set("bucketMs", String(params.bucketMs));
  if (params.groupBy) url.searchParams.set("groupBy", params.groupBy);
  const res = await fetch(url);
  if (!res.ok) {
    const body = await res.text();
    throw new Error(body || `Failed to fetch cluster log histogram: ${res.statusText}`);
  }
  return res.json();
}

function mapLogPage(res: Response, raw: Record<string, unknown>[]): LogPage {
  const entries = mapLogEntries(raw);
  const header = Number(res.headers.get("x-maestro-log-cursor"));
  const lastEntry = entries.at(-1)?.seq ?? 0;
  return {
    entries,
    cursor: Number.isSafeInteger(header) && header >= 0 ? Math.max(header, lastEntry) : lastEntry
  };
}

function mapLogEntries(raw: Record<string, unknown>[]): LogEntry[] {
  return raw.map((entry) => {
    const tags = Array.isArray(entry.tags) ? (entry.tags as string[]) : undefined;
    let hostname: string | undefined;
    if (tags) {
      const match = tags.find((tag) => tag.startsWith("hostname:"));
      if (match) hostname = match.slice("hostname:".length);
    }
    return {
      seq: entry.seq as number,
      ts: entry.ts as number,
      level: entry.level as string,
      stream: entry.stream as LogEntry["stream"],
      text: entry.text as string,
      source: entry.source as string | undefined,
      origin: entry.origin as string | undefined,
      hostname,
      nodeId: entry.nodeId as string | undefined,
      nodeName: entry.nodeName as string | undefined,
      serviceId: entry.serviceId as string | undefined,
      tier: entry.tier as LogEntry["tier"],
      tags,
      attrs: Array.isArray(entry.attrs) ? (entry.attrs as [string, string][]) : undefined
    };
  });
}

export async function getDisks(): Promise<DiskInfo[]> {
  const res = await fetch("/api/disks");
  if (!res.ok) throw new Error(`Failed to fetch disks: ${res.statusText}`);
  return res.json();
}

export async function getIngressRoutes(): Promise<IngressRouting[]> {
  const res = await fetch("/api/ingress/routes");
  if (!res.ok) throw new Error(`Failed to fetch ingress routes: ${res.statusText}`);
  return res.json();
}

export async function getServiceMetrics(
  serviceId: string,
  from?: number,
  to?: number
): Promise<MetricPoint[]> {
  const url = new URL(`/api/services/${encodeURIComponent(serviceId)}/metrics`, location.origin);
  if (from != null) url.searchParams.set("from", String(from));
  if (to != null) url.searchParams.set("to", String(to));
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Failed to fetch metrics: ${res.statusText}`);
  return res.json();
}

export async function getServiceTraffic(
  serviceId: string,
  from?: number,
  to?: number
): Promise<TrafficPoint[]> {
  const url = new URL(`/api/services/${encodeURIComponent(serviceId)}/traffic`, location.origin);
  if (from != null) url.searchParams.set("from", String(from));
  if (to != null) url.searchParams.set("to", String(to));
  const res = await fetch(url);
  if (res.status === 404) return [];
  if (!res.ok) throw new Error(`Failed to fetch traffic: ${res.statusText}`);
  return res.json();
}

export async function getIngressTraffic(
  from: number,
  to: number,
  nodeId?: string
): Promise<IngressTrafficBreakdown> {
  const url = new URL("/api/ingress/traffic", location.origin);
  url.searchParams.set("from", String(from));
  url.searchParams.set("to", String(to));
  url.searchParams.set("limit", "200");
  if (nodeId) url.searchParams.set("nodeId", nodeId);
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Failed to fetch ingress traffic: ${res.statusText}`);
  return res.json();
}

export async function getBlockedIngressTraffic(
  from: number,
  to: number,
  nodeId?: string
): Promise<IngressTrafficBreakdown> {
  const url = new URL("/api/ingress/blocked-traffic", location.origin);
  url.searchParams.set("from", String(from));
  url.searchParams.set("to", String(to));
  url.searchParams.set("limit", "200");
  if (nodeId) url.searchParams.set("nodeId", nodeId);
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Failed to fetch blocked ingress traffic: ${res.statusText}`);
  return res.json();
}

export async function setBlockedIngressIp(ip: string, blocked: boolean) {
  const res = await fetch("/api/ingress/blocked-ips", {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ ip, blocked })
  });
  if (!res.ok) {
    const body = await res.text();
    throw new Error(body || `Failed to update blocked IPs: ${res.statusText}`);
  }
  return res.json() as Promise<IngressBlocklist>;
}

export async function getIngressBlocklist(): Promise<IngressBlocklist> {
  const res = await fetch("/api/ingress/blocked-ips");
  if (!res.ok) throw new Error(`Failed to fetch ingress blocklist: ${res.statusText}`);
  return res.json();
}

export async function getNodeMetrics(from?: number, to?: number): Promise<MetricPoint[]> {
  const url = new URL("/api/metrics/node", location.origin);
  if (from != null) url.searchParams.set("from", String(from));
  if (to != null) url.searchParams.set("to", String(to));
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Failed to fetch node metrics: ${res.statusText}`);
  return res.json();
}

export async function getClusterMetrics(from?: number, to?: number): Promise<MetricPoint[]> {
  const url = new URL("/api/metrics/cluster", location.origin);
  if (from != null) url.searchParams.set("from", String(from));
  if (to != null) url.searchParams.set("to", String(to));
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Failed to fetch cluster metrics: ${res.statusText}`);
  return res.json();
}

export async function getContainerMetrics(
  serviceId: string,
  from?: number,
  to?: number
): Promise<MetricPoint[]> {
  const url = new URL(
    `/api/services/${encodeURIComponent(serviceId)}/metrics/containers`,
    location.origin
  );
  if (from != null) url.searchParams.set("from", String(from));
  if (to != null) url.searchParams.set("to", String(to));
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Failed to fetch container metrics: ${res.statusText}`);
  return res.json();
}

export async function listSlackWebhooks(): Promise<SlackWebhook[]> {
  const res = await fetch("/api/webhooks/slack");
  if (!res.ok) throw new Error(`Failed to load webhooks: ${res.statusText}`);
  return res.json();
}

export async function createSlackWebhook(payload: {
  name: string;
  url: string;
  categories: SlackCategory[];
  enabled?: boolean;
}): Promise<SlackWebhook> {
  const res = await fetch("/api/webhooks/slack", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload)
  });
  if (!res.ok) {
    const body = await res.text();
    throw new Error(body || `Failed to create webhook: ${res.statusText}`);
  }
  return res.json();
}

export async function updateSlackWebhook(
  id: string,
  patch: Partial<{ name: string; url: string; categories: SlackCategory[]; enabled: boolean }>
): Promise<SlackWebhook> {
  const res = await fetch(`/api/webhooks/slack/${encodeURIComponent(id)}`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(patch)
  });
  if (!res.ok) {
    const body = await res.text();
    throw new Error(body || `Failed to update webhook: ${res.statusText}`);
  }
  return res.json();
}

export async function deleteSlackWebhook(id: string): Promise<void> {
  const res = await fetch(`/api/webhooks/slack/${encodeURIComponent(id)}`, { method: "DELETE" });
  if (!res.ok) throw new Error(`Failed to delete webhook: ${res.statusText}`);
}

export async function testSlackWebhook(id: string): Promise<void> {
  const res = await fetch(`/api/webhooks/slack/${encodeURIComponent(id)}/test`, { method: "POST" });
  if (!res.ok) {
    const body = await res.text();
    throw new Error(body || `Test message failed: ${res.statusText}`);
  }
}
