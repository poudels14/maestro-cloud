import type { MaestroApiClient } from "@maestro/api-client";
import { projectClusterNodes } from "./nodes";
import type {
  ClusterInfo,
  ClusterNode,
  ClusterStats,
  MaskedConfig,
  StatsMetricPoint,
  UnschedulableReplica,
  Webhook,
  WebhookCreateRequest,
  WebhookUpdateRequest
} from "./types";

type ClusterErrorMapper = (error: unknown, fallback: string) => Error;

interface ClusterApi {
  listNodes: () => Promise<ClusterNode[]>;
  listUnschedulableReplicas: () => Promise<UnschedulableReplica[]>;
  setNodeDrain: (node: ClusterNode, drain: boolean) => Promise<void>;
  getInfo: () => Promise<ClusterInfo>;
  getStats: () => Promise<ClusterStats>;
  listStatsMetrics: (name?: string, from?: number, to?: number) => Promise<StatsMetricPoint[]>;
  getConfig: () => Promise<MaskedConfig>;
  listWebhooks: () => Promise<Webhook[]>;
  createWebhook: (request: WebhookCreateRequest) => Promise<void>;
  updateWebhook: (webhook: Webhook, request: WebhookUpdateRequest) => Promise<void>;
  deleteWebhook: (webhook: Webhook) => Promise<void>;
  testWebhook: (id: string) => Promise<void>;
}

function createClusterApi(
  client: () => MaestroApiClient,
  mapError: ClusterErrorMapper
): ClusterApi {
  const mapped = async <Value>(operation: () => Promise<Value>, fallback: string) => {
    try {
      return await operation();
    } catch (error) {
      throw mapError(error, fallback);
    }
  };
  const listNodes = () =>
    mapped(async () => {
      const [nodes, networks] = await Promise.all([
        client().listNodes(),
        client().listNodeNetworks()
      ]);
      return projectClusterNodes(nodes, networks);
    }, "Failed to load cluster nodes");

  return {
    listNodes,
    listUnschedulableReplicas: () =>
      mapped(() => client().listUnschedulableReplicas(), "Failed to load scheduling errors"),
    setNodeDrain: (node, drain) =>
      mapped(
        async () => {
          const request = { expectedRevision: node.revision };
          if (drain) {
            await client().drainNode(node.nodeId, request, crypto.randomUUID());
          } else {
            await client().restoreNode(node.nodeId, request, crypto.randomUUID());
          }
        },
        `Failed to ${drain ? "drain" : "restore"} node`
      ),
    getInfo: () =>
      mapped(async () => {
        const [summary, nodes, upgrades] = await Promise.all([
          client().getClusterInfo(),
          listNodes(),
          client().listUpgrades()
        ]);
        const activeUpgrade = upgrades
          .filter((run) => !["completed", "failed", "canceled"].includes(run.status.phase))
          .sort((left, right) => right.meta.revision - left.meta.revision)[0];
        return { ...summary, nodes, activeUpgrade: activeUpgrade ?? null };
      }, "Failed to load cluster info"),
    getStats: () => mapped(() => client().getClusterStats(), "Failed to load cluster stats"),
    listStatsMetrics: (name, from, to) =>
      mapped(
        () =>
          client().listOperationalStatsMetrics({
            ...(name ? { name } : {}),
            ...(from != null ? { from } : {}),
            ...(to != null ? { to } : {})
          }),
        "Failed to load stats metrics"
      ),
    getConfig: () => mapped(() => client().getClusterConfig(), "Failed to load cluster config"),
    listWebhooks: () => mapped(() => client().listWebhooks(), "Failed to load webhooks"),
    createWebhook: (request) =>
      mapped(async () => {
        await client().putWebhook(
          request.id,
          {
            name: request.name,
            endpoint: request.endpoint,
            events: request.events,
            categories: request.categories,
            enabled: request.enabled,
            format: request.format,
            ...(request.signingSecret ? { signingSecret: request.signingSecret } : {})
          },
          crypto.randomUUID()
        );
      }, "Failed to create webhook"),
    updateWebhook: (webhook, request) =>
      mapped(async () => {
        await client().putWebhook(
          webhook.meta.id,
          {
            expectedRevision: webhook.meta.revision,
            name: request.name,
            events: request.events,
            categories: request.categories,
            enabled: request.enabled,
            format: request.format,
            ...(request.endpoint ? { endpoint: request.endpoint } : {}),
            ...(request.signingSecret ? { signingSecret: request.signingSecret } : {})
          },
          crypto.randomUUID()
        );
      }, "Failed to update webhook"),
    deleteWebhook: (webhook) =>
      mapped(async () => {
        await client().deleteWebhook(
          webhook.meta.id,
          { expectedRevision: webhook.meta.revision },
          crypto.randomUUID()
        );
      }, "Failed to delete webhook"),
    testWebhook: (id) =>
      mapped(async () => {
        await client().testWebhook(id, {}, crypto.randomUUID());
      }, "Webhook test delivery failed")
  };
}

export { createClusterApi };
export type { ClusterApi, ClusterErrorMapper };
