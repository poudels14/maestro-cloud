import assert from "node:assert/strict";
import test from "node:test";
import type { ClusterNode } from "./types";
import { nodeAdminUrl } from "./nodeAdmin.ts";

const node = {
  nodeId: "node-a",
  hostname: "sandbox-a",
  role: "voter",
  clusterHostIp: "10.0.0.1",
  clusterApiPort: 3000,
  adminUrl: "http://10.51.0.250",
  subnet: "10.51.0.0/24",
  dataPlaneReady: true,
  version: "test",
  alive: true,
  lastSeenAtMs: 1,
  state: { unschedulable: false }
} satisfies ClusterNode;

test("resolves the selected replica node admin homepage", () => {
  assert.equal(nodeAdminUrl([node], "node-a"), "http://10.51.0.250");
  assert.equal(nodeAdminUrl([node], "missing"), null);
  assert.equal(nodeAdminUrl(undefined, "node-a"), null);
});
