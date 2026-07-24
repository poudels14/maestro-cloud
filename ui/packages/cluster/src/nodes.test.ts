import assert from "node:assert/strict";
import type { ApiSchemas } from "@maestro/api-client";
import { test } from "vitest";
import { projectClusterNodes } from "./nodes";

function node(
  id: string,
  hostname: string,
  lastSeen: number,
  conditions: ApiSchemas["Condition"][] = []
): ApiSchemas["Node"] {
  return {
    meta: { id, generation: 1, revision: id === "node-b" ? 9 : 4 },
    spec: {
      hostname,
      hostAddress: id === "node-b" ? "10.0.0.12" : "10.0.0.11",
      role: id === "node-b" ? "worker" : "master",
      workloadNetworkMode: "clusterRouted"
    },
    status: { instanceId: `${id}-instance`, lastSeen, version: "0.5.0", conditions }
  };
}

function network(
  nodeId: string,
  appliedGeneration: number,
  condition: ApiSchemas["Condition"]
): ApiSchemas["NodeNetwork"] {
  return {
    meta: { id: `${nodeId}-network`, generation: 2, revision: 3 },
    spec: {
      endpoint: "10.0.0.12:51820",
      mtuBytes: 1_420,
      nodeId,
      publicKey: "public-key",
      workloadSubnet: "10.51.1.0/24"
    },
    status: { appliedGeneration, conditions: [condition] }
  };
}

const meshReady = {
  type: "MeshReady",
  status: "true",
  reason: "Applied",
  message: "mesh configuration is applied",
  observedGeneration: 2,
  lastTransitionTime: 70_000
} satisfies ApiSchemas["Condition"];

test("projects node liveness, mesh readiness, and drain state", () => {
  const draining = {
    type: "Draining",
    status: "true",
    reason: "Requested",
    message: "node drain requested",
    observedGeneration: 1,
    lastTransitionTime: 75_000
  } satisfies ApiSchemas["Condition"];
  const projected = projectClusterNodes(
    [node("node-b", "worker-b", 80_000, [draining]), node("node-a", "control-a", 60_000)],
    [network("node-b", 2, meshReady)],
    100_000
  );

  assert.equal(projected[0]?.nodeId, "node-a");
  assert.equal(projected[0]?.alive, false);
  assert.equal(projected[0]?.dataPlaneError, "Mesh network is not published");
  assert.deepEqual(projected[1], {
    nodeId: "node-b",
    hostname: "worker-b",
    role: "worker",
    hostAddress: "10.0.0.12",
    subnet: "10.51.1.0/24",
    dataPlaneReady: true,
    dataPlaneError: null,
    version: "0.5.0",
    alive: true,
    lastSeenAtMs: 80_000,
    revision: 9,
    state: {
      unschedulable: true,
      drainPending: false,
      drainedAtMs: 75_000,
      reason: "node drain requested"
    }
  });
});

test("keeps a node schedulable while drain artifact replication is pending", () => {
  const pendingDrain = {
    type: "Draining",
    status: "unknown",
    reason: "ReplicatingArtifacts",
    message: "waiting for peer copies",
    observedGeneration: 1,
    lastTransitionTime: 75_000
  } satisfies ApiSchemas["Condition"];
  const projected = projectClusterNodes(
    [node("node-b", "worker-b", 100_000, [pendingDrain])],
    [network("node-b", 2, meshReady)],
    100_000
  );

  assert.equal(projected[0]?.state.unschedulable, false);
  assert.equal(projected[0]?.state.drainPending, true);
  assert.equal(projected[0]?.state.reason, "waiting for peer copies");
});

test("reports a desired mesh generation that has not been applied", () => {
  const projected = projectClusterNodes(
    [node("node-b", "worker-b", 100_000)],
    [network("node-b", 1, meshReady)],
    100_000
  );

  assert.equal(projected[0]?.dataPlaneReady, false);
  assert.equal(projected[0]?.dataPlaneError, "Mesh network generation is not applied");
});
