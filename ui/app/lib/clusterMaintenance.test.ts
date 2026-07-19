import assert from "node:assert/strict";
import { test } from "vitest";
import type { ClusterMaintenanceRun } from "./api";
import { activeMaintenanceNode, maintenanceStageLabel } from "./clusterMaintenance.ts";
import type { ClusterNode } from "./types";

const run = {
  kind: "upgrade",
  targetVersion: "0.4.8",
  requestedAtMs: 1,
  phase: "verifying",
  currentNodeIndex: 1,
  nodes: [
    { nodeId: "node-a", hostname: "unknown-host", status: "succeeded" },
    {
      nodeId: "node-b",
      hostname: "unknown-host",
      status: "upgrading",
      upgradeStage: "rebuilding-system"
    }
  ]
} satisfies ClusterMaintenanceRun;

const node = {
  nodeId: "node-b",
  hostname: "unknown-host",
  role: "hybrid",
  clusterHostIp: "10.1.0.12",
  clusterApiPort: 3000,
  adminUrl: "http://10.51.0.250",
  subnet: "10.51.0.0/24",
  dataPlaneReady: true,
  version: "0.4.7",
  alive: true,
  lastSeenAtMs: 1,
  state: { unschedulable: true, reason: "upgrade" }
} satisfies ClusterNode;

test("identifies the active maintenance node by admin address", () => {
  assert.deepEqual(activeMaintenanceNode(run, [node]), {
    nodeId: "node-b",
    label: "10.51.0.250",
    adminUrl: "http://10.51.0.250"
  });
  assert.equal(maintenanceStageLabel(run), "rebuilding system");
});

test("falls back to the node id and coordinator phase", () => {
  const waiting = {
    ...run,
    phase: "awaiting-leadership-transfer",
    nodes: run.nodes.map((step) => ({ ...step, upgradeStage: null }))
  };
  assert.equal(activeMaintenanceNode(waiting, [])?.label, "node-b");
  assert.equal(maintenanceStageLabel(waiting), "transferring leadership");
});

test("shows placement restoration after the node restart completes", () => {
  assert.equal(maintenanceStageLabel({ ...run, phase: "restoring" }), "restoring placement");
});
