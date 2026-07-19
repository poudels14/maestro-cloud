import assert from "node:assert/strict";
import { test } from "vitest";
import type { ClusterNode } from "./types";
import { nodeAdminLabel, nodeAdminUrl } from "./nodeAdmin.ts";

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

test("labels node admin URLs with only nonstandard ports", () => {
  assert.equal(nodeAdminLabel("http://10.51.0.250"), "10.51.0.250");
  assert.equal(nodeAdminLabel("https://admin.example.com"), "admin.example.com");
  assert.equal(nodeAdminLabel("http://10.51.0.250:8080"), "10.51.0.250:8080");
});
