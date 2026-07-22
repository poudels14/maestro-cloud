import assert from "node:assert/strict";
import { test } from "vitest";
import { createApiClient, type ApiTransport, type TransportRequest } from "@maestro/api-client";

test("generated observability helpers preserve paths and query contracts", async () => {
  const requests: Array<{ method: string; path: string }> = [];
  const transport: ApiTransport = {
    async request<Response, Body>(request: TransportRequest<Response, Body>): Promise<Response> {
      requests.push({ method: request.method, path: request.path });
      return {} as Response;
    }
  };
  const client = createApiClient(transport);

  await client.getClusterStats();
  await client.listLocalDisks();
  await client.listNodeDisks();
  await client.listNodeMetrics({ from: 10, to: 20, limit: 100 });
  await client.listClusterMetrics({ from: 30, to: 40, limit: 200, bucketMs: 5_000 });
  await client.listOperationalStatsMetrics({ name: "sink queue", from: 50, to: 60, limit: 300 });
  await client.listServiceMetrics("service/a", { from: 70, to: 80, bucketMs: 10_000 });
  await client.listContainerMetrics("service/a", { from: 90, to: 100, limit: 400 });

  assert.deepEqual(requests, [
    { method: "GET", path: "/api/cluster/stats" },
    { method: "GET", path: "/api/disks" },
    { method: "GET", path: "/api/disks/nodes" },
    { method: "GET", path: "/api/metrics/node?from=10&to=20&limit=100" },
    {
      method: "GET",
      path: "/api/metrics/cluster?from=30&to=40&limit=200&bucketMs=5000"
    },
    {
      method: "GET",
      path: "/api/metrics/stats?name=sink+queue&from=50&to=60&limit=300"
    },
    {
      method: "GET",
      path: "/api/services/service%2Fa/metrics?from=70&to=80&bucketMs=10000"
    },
    {
      method: "GET",
      path: "/api/services/service%2Fa/metrics/containers?from=90&to=100&limit=400"
    }
  ]);
});
