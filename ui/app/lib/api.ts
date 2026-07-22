import {
  ApiHttpError,
  createApiClient,
  createFetchTransport,
  type ApiSchemas
} from "@maestro/api-client";
import type {
  ClusterNode,
  ClusterSummary,
  Deployment,
  DiskInfo,
  FirewallDryRun,
  FirewallPolicy,
  FirewallPolicySpec,
  IngressBlocklist,
  IngressTrafficBreakdown,
  IngressRouting,
  LogEntry,
  MaskedConfig,
  MetricPoint,
  ClusterStats,
  Service,
  StatsMetricPoint,
  TrafficPoint,
  UnschedulableReplica,
  UpgradeRun,
  Webhook,
  WebhookEvent
} from "./types";
import { apiErrorFromResponse } from "./apiError";

export interface ClusterInfo extends ClusterSummary {
  nodes: ClusterNode[];
  activeUpgrade: UpgradeRun | null;
}

const NODE_LIVENESS_WINDOW_MS = 30_000;

function apiClient() {
  return createApiClient(createFetchTransport(location.origin));
}

export function projectClusterNodes(
  nodes: ApiSchemas["Node"][],
  networks: ApiSchemas["NodeNetwork"][],
  nowMs = Date.now()
): ClusterNode[] {
  const networksByNode = new Map(networks.map((network) => [network.spec.nodeId, network]));
  return nodes
    .map((node) => {
      const network = networksByNode.get(node.meta.id);
      const meshCondition = network?.status.conditions?.find(
        (condition) => condition.type === "MeshReady"
      );
      const dataPlaneReady =
        network != null &&
        network.status.appliedGeneration === network.meta.generation &&
        meshCondition?.status === "true";
      const placementCondition = ["Maintenance", "Draining"]
        .map((type) =>
          node.status.conditions?.find(
            (condition) => condition.type === type && condition.status === "true"
          )
        )
        .find((condition) => condition != null);
      return {
        nodeId: node.meta.id,
        hostname: node.spec.hostname,
        role: node.spec.role,
        hostAddress: node.spec.hostAddress,
        subnet: network?.spec.workloadSubnet ?? "unavailable",
        dataPlaneReady,
        dataPlaneError: dataPlaneReady ? null : meshReadinessError(network, meshCondition),
        version: node.status.version,
        alive: node.status.lastSeen >= nowMs - NODE_LIVENESS_WINDOW_MS,
        lastSeenAtMs: node.status.lastSeen,
        revision: node.meta.revision,
        state: {
          unschedulable: placementCondition != null,
          drainedAtMs: placementCondition?.lastTransitionTime ?? null,
          reason: placementCondition?.message || placementCondition?.reason || null
        }
      } satisfies ClusterNode;
    })
    .sort((left, right) => left.hostname.localeCompare(right.hostname));
}

function meshReadinessError(
  network: ApiSchemas["NodeNetwork"] | undefined,
  condition: ApiSchemas["Condition"] | undefined
): string {
  if (!network) return "Mesh network is not published";
  if (network.status.appliedGeneration !== network.meta.generation) {
    return "Mesh network generation is not applied";
  }
  return condition?.message || condition?.reason || "Mesh network is not ready";
}

export async function getClusterNodes(): Promise<ClusterNode[]> {
  const [nodes, networks] = await Promise.all([
    apiClient().listNodes(),
    apiClient().listNodeNetworks()
  ]);
  return projectClusterNodes(nodes, networks);
}

export async function getUnschedulableReplicas(): Promise<UnschedulableReplica[]> {
  const res = await fetch("/api/cluster/unschedulable");
  if (!res.ok) throw new Error(`Failed to fetch scheduling errors: ${res.statusText}`);
  return res.json();
}

export async function setNodeDrain(node: ClusterNode, drain: boolean): Promise<void> {
  try {
    const request = { expectedRevision: node.revision };
    if (drain) {
      await apiClient().drainNode(node.nodeId, request, crypto.randomUUID());
    } else {
      await apiClient().restoreNode(node.nodeId, request, crypto.randomUUID());
    }
  } catch (error) {
    throw apiRequestError(error, `Failed to ${drain ? "drain" : "restore"} node`);
  }
}

export async function getClusterInfo(): Promise<ClusterInfo> {
  const [summary, nodes, upgrades] = await Promise.all([
    apiClient().getClusterInfo(),
    getClusterNodes(),
    apiClient().listUpgrades()
  ]);
  const activeUpgrade = upgrades
    .filter((run) => !["completed", "failed", "canceled"].includes(run.status.phase))
    .sort((left, right) => right.meta.revision - left.meta.revision)[0];
  return { ...summary, nodes, activeUpgrade: activeUpgrade ?? null };
}

export async function listFirewallPolicies(): Promise<FirewallPolicy[]> {
  try {
    return await apiClient().listFirewallPolicies();
  } catch (error) {
    throw apiRequestError(error, "Failed to load firewall policies");
  }
}

export async function saveFirewallPolicy(
  policyId: string,
  spec: FirewallPolicySpec,
  expectedRevision?: number
): Promise<void> {
  const request: ApiSchemas["FirewallPolicyWriteRequest"] =
    expectedRevision == null ? { spec } : { spec, expectedRevision };
  try {
    await apiClient().putFirewallPolicy(policyId, request, crypto.randomUUID());
  } catch (error) {
    throw apiRequestError(error, "Failed to save firewall policy");
  }
}

export async function deleteFirewallPolicy(
  policyId: string,
  expectedRevision: number
): Promise<void> {
  try {
    await apiClient().deleteFirewallPolicy(policyId, { expectedRevision }, crypto.randomUUID());
  } catch (error) {
    throw apiRequestError(error, "Failed to delete firewall policy");
  }
}

export async function dryRunFirewallPolicy(
  policyId: string,
  spec: FirewallPolicySpec
): Promise<FirewallDryRun> {
  try {
    return await apiClient().dryRunFirewallPolicy(policyId, { spec });
  } catch (error) {
    throw apiRequestError(error, "Failed to plan firewall policy");
  }
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
  try {
    return await apiClient().listActiveIngressRoutes();
  } catch (error) {
    throw apiRequestError(error, "Failed to load ingress routes");
  }
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
  try {
    return await apiClient().getIngressTraffic({
      from,
      to,
      limit: 200,
      ...(nodeId ? { nodeId } : {})
    });
  } catch (error) {
    throw apiRequestError(error, "Failed to load ingress traffic");
  }
}

export async function getBlockedIngressTraffic(
  from: number,
  to: number,
  nodeId?: string
): Promise<IngressTrafficBreakdown> {
  try {
    return await apiClient().getBlockedIngressTraffic({
      from,
      to,
      limit: 200,
      ...(nodeId ? { nodeId } : {})
    });
  } catch (error) {
    throw apiRequestError(error, "Failed to load blocked ingress traffic");
  }
}

export async function setBlockedIngressIp(ip: string, blocked: boolean): Promise<IngressBlocklist> {
  try {
    return await apiClient().setBlockedIngressIp({ ip, blocked });
  } catch (error) {
    throw apiRequestError(error, "Failed to update blocked IPs");
  }
}

export async function getIngressBlocklist(): Promise<IngressBlocklist> {
  try {
    return await apiClient().getIngressBlocklist();
  } catch (error) {
    throw apiRequestError(error, "Failed to load ingress blocklist");
  }
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

function apiRequestError(error: unknown, fallback: string): Error {
  if (!(error instanceof ApiHttpError)) {
    return error instanceof Error ? error : new Error(fallback);
  }
  let message = error.body || fallback;
  try {
    const payload = JSON.parse(error.body) as {
      error?: { message?: string } | string;
    };
    message = typeof payload.error === "string" ? payload.error : payload.error?.message || message;
  } catch {
    // Preserve a non-JSON response body from the API proxy.
  }
  return new Error(message);
}

export async function listWebhooks(): Promise<Webhook[]> {
  try {
    return await apiClient().listWebhooks();
  } catch (error) {
    throw apiRequestError(error, "Failed to load webhooks");
  }
}

export async function createWebhook(payload: {
  id: string;
  endpoint: string;
  events: WebhookEvent[];
  signingSecret: string;
}): Promise<void> {
  try {
    await apiClient().putWebhook(
      payload.id,
      {
        endpoint: payload.endpoint,
        events: payload.events,
        signingSecret: payload.signingSecret
      },
      crypto.randomUUID()
    );
  } catch (error) {
    throw apiRequestError(error, "Failed to create webhook");
  }
}

export async function deleteWebhook(webhook: Webhook): Promise<void> {
  try {
    await apiClient().deleteWebhook(
      webhook.meta.id,
      { expectedRevision: webhook.meta.revision },
      crypto.randomUUID()
    );
  } catch (error) {
    throw apiRequestError(error, "Failed to delete webhook");
  }
}

export async function testWebhook(id: string): Promise<void> {
  try {
    await apiClient().testWebhook(id, {}, crypto.randomUUID());
  } catch (error) {
    throw apiRequestError(error, "Webhook test delivery failed");
  }
}
