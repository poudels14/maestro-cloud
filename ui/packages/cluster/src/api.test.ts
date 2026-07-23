import { expect, test } from "vitest";
import type { ApiSchemas, MaestroApiClient, StatsMetricQuery } from "@maestro/api-client";
import { createClusterApi } from "./api";
import type { ClusterNode, Webhook } from "./types";

const node = {
  nodeId: "node-a",
  hostname: "worker-a",
  role: "worker",
  hostAddress: "10.0.0.11",
  subnet: "10.51.0.0/24",
  dataPlaneReady: true,
  version: "0.5.0",
  alive: true,
  lastSeenAtMs: 100,
  revision: 7,
  state: { unschedulable: false }
} satisfies ClusterNode;

const webhook = {
  meta: { id: "deployments", generation: 1, revision: 9 },
  spec: {
    name: "Deployments",
    endpoint: "••••stro",
    events: ["deploymentTransition"],
    categories: ["info", "error"],
    enabled: true,
    format: "maestro",
    signingSecret: "••••aaaa"
  },
  status: { consecutiveFailures: 0 }
} satisfies Webhook;

test("passes node, metrics, and webhook operations to the generated client", async () => {
  const calls: Array<{ operation: string; args: unknown[] }> = [];
  const record =
    (operation: string) =>
    async (...args: unknown[]) => {
      calls.push({ operation, args });
      return {};
    };
  const client = {
    restoreNode: record("restore"),
    listOperationalStatsMetrics: async (query: StatsMetricQuery) => {
      calls.push({ operation: "metrics", args: [query] });
      return [];
    },
    putWebhook: record("create-webhook"),
    deleteWebhook: record("delete-webhook"),
    testWebhook: record("test-webhook")
  } as unknown as MaestroApiClient;
  const api = createClusterApi(
    () => client,
    (error) => error as Error
  );

  await api.setNodeDrain(node, false);
  await api.listStatsMetrics("controller.reconciles", 10, 20);
  await api.createWebhook({
    id: "deployments",
    name: "Deployments",
    endpoint: "https://events.example.com/maestro",
    events: ["deploymentTransition"],
    categories: ["info", "error"],
    enabled: true,
    format: "maestro",
    signingSecret: "a".repeat(32)
  });
  await api.updateWebhook(webhook, {
    name: "Slack alerts",
    events: ["deploymentTransition", "nodeAvailability"],
    categories: ["error"],
    enabled: false,
    format: "slack"
  });
  await api.deleteWebhook(webhook);
  await api.testWebhook("deployments");

  expect(
    calls.map(({ operation, args }) => ({ operation, args: stripKey(operation, args) }))
  ).toEqual([
    { operation: "restore", args: ["node-a", { expectedRevision: 7 }] },
    {
      operation: "metrics",
      args: [{ name: "controller.reconciles", from: 10, to: 20 }]
    },
    {
      operation: "create-webhook",
      args: [
        "deployments",
        {
          name: "Deployments",
          endpoint: "https://events.example.com/maestro",
          events: ["deploymentTransition"],
          categories: ["info", "error"],
          enabled: true,
          format: "maestro",
          signingSecret: "a".repeat(32)
        }
      ]
    },
    {
      operation: "create-webhook",
      args: [
        "deployments",
        {
          expectedRevision: 9,
          name: "Slack alerts",
          events: ["deploymentTransition", "nodeAvailability"],
          categories: ["error"],
          enabled: false,
          format: "slack"
        }
      ]
    },
    { operation: "delete-webhook", args: ["deployments", { expectedRevision: 9 }] },
    { operation: "test-webhook", args: ["deployments", {}] }
  ]);
  for (const call of calls.filter(({ operation }) => operation !== "metrics")) {
    expect(call.args.at(-1)).toMatch(/^[0-9a-f-]{36}$/);
  }
});

test("selects the newest active upgrade when composing cluster info", async () => {
  const resourceNode = {
    meta: { id: "node-a", generation: 1, revision: 7 },
    spec: { hostname: "worker-a", hostAddress: "10.0.0.11", role: "worker" },
    status: { instanceId: "instance-a", lastSeen: Date.now(), version: "0.5.0" }
  } satisfies ApiSchemas["Node"];
  const active = upgrade("active", 3, "applying");
  const older = upgrade("older", 2, "draining");
  const complete = upgrade("complete", 4, "completed");
  const client = {
    async getClusterInfo() {
      return {
        clusterId: "cluster-a",
        controlPlaneNodeCount: 1,
        nodeCount: 1,
        workloadNodeCount: 1
      };
    },
    async listNodes() {
      return [resourceNode];
    },
    async listNodeNetworks() {
      return [];
    },
    async listUpgrades() {
      return [older, complete, active];
    }
  } as unknown as MaestroApiClient;
  const api = createClusterApi(
    () => client,
    (error) => error as Error
  );

  const info = await api.getInfo();

  expect(info.activeUpgrade?.meta.id).toBe("active");
  expect(info.nodes[0]?.nodeId).toBe("node-a");
});

test("lists and approves node admissions without inventing command fields", async () => {
  const approval = {
    nodeId: "worker-a",
    publicKeySha256: "ab".repeat(32),
    approvedAtUnixMs: 100,
    state: "approved"
  } satisfies ApiSchemas["NodeJoinApproval"];
  const calls: Array<{ operation: string; args: unknown[] }> = [];
  const client = {
    async listClusterAdmissions() {
      calls.push({ operation: "list", args: [] });
      return [approval];
    },
    async approveClusterAdmission(request: ApiSchemas["NodeJoinApprovalRequest"]) {
      calls.push({ operation: "approve", args: [request] });
      return approval;
    }
  } as unknown as MaestroApiClient;
  const api = createClusterApi(
    () => client,
    (error) => error as Error
  );

  await expect(api.listAdmissions()).resolves.toEqual([approval]);
  await expect(
    api.approveAdmission({ nodeId: approval.nodeId, publicKeySha256: approval.publicKeySha256 })
  ).resolves.toEqual(approval);
  expect(calls).toEqual([
    { operation: "list", args: [] },
    {
      operation: "approve",
      args: [{ nodeId: "worker-a", publicKeySha256: "ab".repeat(32) }]
    }
  ]);
});

test("maps generated client failures at the cluster boundary", async () => {
  const client = {
    async getClusterStats() {
      throw new Error("transport detail");
    }
  } as unknown as MaestroApiClient;
  const api = createClusterApi(
    () => client,
    (_error, fallback) => new Error(`${fallback} (mapped)`)
  );

  await expect(api.getStats()).rejects.toThrow("Failed to load cluster stats (mapped)");
});

function stripKey(operation: string, args: unknown[]): unknown[] {
  return operation === "metrics" ? args : args.slice(0, -1);
}

function upgrade(
  id: string,
  revision: number,
  phase: ApiSchemas["UpgradePhase"]
): ApiSchemas["UpgradeRun"] {
  return {
    meta: { id, generation: 1, revision },
    spec: { operation: "upgrade", mode: "rolling", targetVersion: "0.5.1" },
    status: { phase, nodes: [] }
  };
}
