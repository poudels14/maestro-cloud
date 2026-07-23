import assert from "node:assert/strict";
import { test } from "vitest";
import { activeMaintenanceNode, maintenanceStageLabel } from "./maintenance";
import type { ClusterNode, UpgradeRun } from "./types";

const run = {
  meta: { id: "upgrade-1", generation: 1, revision: 8 },
  spec: { operation: "upgrade", mode: "rolling", targetVersion: "0.4.8" },
  status: {
    phase: "applying",
    nodes: [
      { nodeId: "node-a", attempts: 1, phase: "completed" },
      { nodeId: "node-b", attempts: 1, phase: "applying" }
    ]
  }
} satisfies UpgradeRun;

const node = {
  nodeId: "node-b",
  hostname: "worker-b",
  role: "hybrid",
  hostAddress: "10.1.0.12",
  subnet: "10.51.0.0/24",
  dataPlaneReady: true,
  version: "0.4.7",
  alive: true,
  lastSeenAtMs: 1,
  revision: 4,
  state: { unschedulable: true, reason: "upgrade" }
} satisfies ClusterNode;

test("identifies the active maintenance node by hostname", () => {
  assert.deepEqual(activeMaintenanceNode(run, [node]), {
    nodeId: "node-b",
    label: "worker-b"
  });
  assert.equal(maintenanceStageLabel(run), "applying upgrade");
});

test("falls back to the node id and coordinator phase", () => {
  const waiting = {
    ...run,
    status: {
      phase: "draining" as const,
      nodes: [
        { ...run.status.nodes[0]!, phase: "completed" as const },
        { ...run.status.nodes[1]!, phase: "pending" as const }
      ]
    }
  } satisfies UpgradeRun;
  assert.equal(activeMaintenanceNode(waiting, [])?.label, "node-b");
  assert.equal(maintenanceStageLabel(waiting), "draining workloads");
});

test("uses the aggregate phase when there is no active node", () => {
  const complete = {
    ...run,
    status: { phase: "completed" as const, nodes: [] }
  } satisfies UpgradeRun;
  assert.equal(activeMaintenanceNode(complete, [node]), null);
  assert.equal(maintenanceStageLabel(complete), "completed");
});

test("labels restart dispatch without implying an upgrade", () => {
  const restart = {
    ...run,
    spec: { operation: "restart" as const, mode: "rolling" as const, targetVersion: "0.0.0" }
  } satisfies UpgradeRun;
  assert.equal(maintenanceStageLabel(restart), "preparing restart");
});
