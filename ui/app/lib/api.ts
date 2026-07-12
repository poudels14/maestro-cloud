import type {
  Deployment,
  DiskInfo,
  IngressRouting,
  LogEntry,
  MaskedConfig,
  MetricPoint,
  ClusterStats,
  Service,
  SlackCategory,
  SlackWebhook,
  StatsMetricPoint,
  TrafficPoint
} from "./types";

export interface ClusterInfo {
  clusterName: string;
  clusterAlias: string;
  canonicalDomain: string;
  aliasDomain: string;
  version?: string;
  upgrading?: boolean;
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
  if (!res.ok) throw new Error(`Failed to redeploy: ${res.statusText}`);
}

export async function restartService(serviceId: string, force?: boolean) {
  const url = new URL(`/api/services/${encodeURIComponent(serviceId)}/restart`, location.origin);
  if (force) url.searchParams.set("force", "true");
  const res = await fetch(url, { method: "POST" });
  if (!res.ok) throw new Error(`Failed to restart: ${res.statusText}`);
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
  phase?: "build" | "deploy"
): Promise<LogEntry[]> {
  const url = new URL(
    `/api/services/${encodeURIComponent(serviceId)}/deployments/${encodeURIComponent(deploymentId)}/logs`,
    location.origin
  );
  if (tail != null) url.searchParams.set("tail", String(tail));
  if (afterSeq != null) url.searchParams.set("after", String(afterSeq));
  if (beforeSeq != null) url.searchParams.set("before", String(beforeSeq));
  if (phase != null) url.searchParams.set("phase", phase);
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Failed to fetch logs: ${res.statusText}`);
  const raw = await res.json();
  return mapLogEntries(raw);
}

export async function getServiceLogs(
  serviceId: string,
  tail?: number,
  afterSeq?: number,
  beforeSeq?: number,
  phase?: "build" | "deploy"
): Promise<LogEntry[]> {
  const url = new URL(`/api/services/${encodeURIComponent(serviceId)}/logs`, location.origin);
  if (tail != null) url.searchParams.set("tail", String(tail));
  if (afterSeq != null) url.searchParams.set("after", String(afterSeq));
  if (beforeSeq != null) url.searchParams.set("before", String(beforeSeq));
  if (phase != null) url.searchParams.set("phase", phase);
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Failed to fetch service logs: ${res.statusText}`);
  const raw = await res.json();
  return mapLogEntries(raw);
}

export async function getSystemLogs(
  name: string,
  tail?: number,
  afterSeq?: number,
  beforeSeq?: number
): Promise<LogEntry[]> {
  const url = new URL(`/api/system/${encodeURIComponent(name)}/logs`, location.origin);
  if (tail != null) url.searchParams.set("tail", String(tail));
  if (afterSeq != null) url.searchParams.set("after", String(afterSeq));
  if (beforeSeq != null) url.searchParams.set("before", String(beforeSeq));
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Failed to fetch system logs: ${res.statusText}`);
  const raw = await res.json();
  return mapLogEntries(raw);
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
