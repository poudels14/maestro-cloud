import type { ApiSchemas } from "@maestro/api-client";
import type {
  ClusterNode,
  ClusterSummary,
  DiskInfo,
  FirewallDryRun,
  FirewallPolicy,
  FirewallPolicySpec,
  IngressBlocklist,
  IngressTrafficBreakdown,
  IngressRouting,
  MaskedConfig,
  MetricPoint,
  ClusterStats,
  StatsMetricPoint,
  TrafficPoint,
  UnschedulableReplica,
  UpgradeRun,
  Webhook,
  WebhookEvent
} from "./types";
import { apiClient, apiRequestError } from "./client";

export {
  clearServiceReplicasOverride,
  deleteService,
  freezeService,
  getServices,
  redeployService,
  setServiceReplicas
} from "./serviceApi";
export {
  cancelDeployment,
  getDeploymentReplicas,
  getDeployments,
  removeDeployment,
  restartDeployment
} from "./deploymentApi";
export { getLogHistogram, getLogPage } from "./logApi";
export type {
  LogEntry,
  LogHistogram,
  LogHistogramBucket,
  LogHistogramRequest,
  LogPage,
  LogPageRequest,
  LogScope
} from "./logApi";

export interface ClusterInfo extends ClusterSummary {
  nodes: ClusterNode[];
  activeUpgrade: UpgradeRun | null;
}

const NODE_LIVENESS_WINDOW_MS = 30_000;

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
  try {
    return await apiClient().listUnschedulableReplicas();
  } catch (error) {
    throw apiRequestError(error, "Failed to load scheduling errors");
  }
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
  try {
    return await apiClient().getClusterStats();
  } catch (error) {
    throw apiRequestError(error, "Failed to load cluster stats");
  }
}

export async function getStatsMetrics(
  name?: string,
  from?: number,
  to?: number
): Promise<StatsMetricPoint[]> {
  try {
    return await apiClient().listOperationalStatsMetrics({
      ...(name ? { name } : {}),
      ...(from != null ? { from } : {}),
      ...(to != null ? { to } : {})
    });
  } catch (error) {
    throw apiRequestError(error, "Failed to load stats metrics");
  }
}

export async function getClusterConfig(): Promise<MaskedConfig> {
  try {
    return await apiClient().getClusterConfig();
  } catch (error) {
    throw apiRequestError(error, "Failed to load cluster config");
  }
}

export async function getDisks(): Promise<DiskInfo[]> {
  try {
    return await apiClient().listLocalDisks();
  } catch (error) {
    throw apiRequestError(error, "Failed to load disks");
  }
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
  try {
    return await apiClient().listServiceMetrics(serviceId, {
      ...(from != null ? { from } : {}),
      ...(to != null ? { to } : {})
    });
  } catch (error) {
    throw apiRequestError(error, "Failed to load service metrics");
  }
}

export async function getServiceTraffic(
  serviceId: string,
  from?: number,
  to?: number
): Promise<TrafficPoint[]> {
  try {
    return await apiClient().getServiceTraffic(serviceId, {
      ...(from != null ? { from } : {}),
      ...(to != null ? { to } : {})
    });
  } catch (error) {
    throw apiRequestError(error, "Failed to load service traffic");
  }
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
  try {
    return await apiClient().listNodeMetrics({
      ...(from != null ? { from } : {}),
      ...(to != null ? { to } : {})
    });
  } catch (error) {
    throw apiRequestError(error, "Failed to load node metrics");
  }
}

export async function getClusterMetrics(from?: number, to?: number): Promise<MetricPoint[]> {
  try {
    return await apiClient().listClusterMetrics({
      ...(from != null ? { from } : {}),
      ...(to != null ? { to } : {})
    });
  } catch (error) {
    throw apiRequestError(error, "Failed to load cluster metrics");
  }
}

export async function getContainerMetrics(
  serviceId: string,
  from?: number,
  to?: number
): Promise<MetricPoint[]> {
  try {
    return await apiClient().listContainerMetrics(serviceId, {
      ...(from != null ? { from } : {}),
      ...(to != null ? { to } : {})
    });
  } catch (error) {
    throw apiRequestError(error, "Failed to load container metrics");
  }
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
