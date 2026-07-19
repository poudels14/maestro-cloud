import assert from "node:assert/strict";
import { test } from "vitest";
import type { Deployment } from "./types";
import { replicaHostname } from "./deploymentEndpoints.ts";

const deployment = {
  id: "vsk9ac-long-id",
  config: { id: "test-service-2" }
} as Deployment;

test("uses the hostname reported by the replica node", () => {
  assert.equal(
    replicaHostname(deployment, 0, "test-service-2-vsk9ac-node-3000"),
    "test-service-2-vsk9ac-node-3000"
  );
});

test("falls back to the legacy deployment hostname while placement is pending", () => {
  assert.equal(replicaHostname(deployment, 0), "test-service-2-vsk9ac");
  assert.equal(replicaHostname(deployment, 2), "test-service-2-vsk9ac-2");
});
