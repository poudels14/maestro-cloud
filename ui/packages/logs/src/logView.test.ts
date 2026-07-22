import assert from "node:assert/strict";
import { test } from "vitest";
import type { ApiSchemas } from "@maestro/api-client";
import { mapClusterLogEntry, sortLogEntries } from "./logView";

test("cluster log entries project normalized workload ownership", () => {
  const wire = clusterEntry({
    nodeId: "node-b",
    sequence: 12,
    eventAt: 200,
    severity: "warn",
    stream: "stderr",
    body: { type: "text", value: "slow request" },
    origin: {
      type: "workload",
      metadata: {
        serviceId: "api",
        workloadId: "api-0",
        labels: { hostname: "api-host", region: "west" }
      }
    },
    attributes: { "http.status_code": "503", method: "GET" }
  });

  assert.deepEqual(mapClusterLogEntry(wire), {
    seq: 12,
    ts: 200,
    level: "warn",
    stream: "stderr",
    text: "slow request",
    nodeId: "node-b",
    origin: "workload",
    tier: "service",
    serviceId: "api",
    source: "api-0/deploy",
    hostname: "api-host",
    tags: ["hostname:api-host", "region:west"],
    attrs: [
      ["http.status_code", "503"],
      ["method", "GET"]
    ]
  });
});

test("binary build logs remain readable and pages sort chronologically", () => {
  const later = mapClusterLogEntry(
    clusterEntry({
      nodeId: "node-b",
      sequence: 2,
      eventAt: 300,
      body: { type: "bytes", value: [98, 117, 105, 108, 100] },
      origin: { type: "build", buildId: "build-1" }
    })
  );
  const earlier = mapClusterLogEntry(
    clusterEntry({
      nodeId: "node-a",
      sequence: 1,
      eventAt: 100,
      body: { type: "text", value: "start" },
      origin: { type: "system", component: "daemon" }
    })
  );

  assert.equal(later.text, "build");
  assert.equal(later.source, "build-1/build");
  assert.deepEqual(
    sortLogEntries([later, earlier]).map((entry) => entry.text),
    ["start", "build"]
  );
});

function clusterEntry(
  overrides: Partial<{
    nodeId: string;
    sequence: number;
    eventAt: number;
    severity: string;
    stream: ApiSchemas["IngestLogEntry"]["stream"];
    body: Record<string, unknown>;
    origin: Record<string, unknown>;
    attributes: Record<string, string>;
  }>
): ApiSchemas["ClusterLogEntry"] {
  return {
    nodeId: overrides.nodeId ?? "node-a",
    sequence: overrides.sequence ?? 1,
    entry: {
      id: {},
      observedAt: overrides.eventAt ?? 100,
      eventAt: overrides.eventAt ?? 100,
      severity: overrides.severity ?? "info",
      stream: overrides.stream ?? "stdout",
      body: overrides.body ?? { type: "text", value: "message" },
      origin: overrides.origin ?? { type: "system", component: "daemon" },
      ...(overrides.attributes ? { attributes: overrides.attributes } : {})
    }
  };
}
