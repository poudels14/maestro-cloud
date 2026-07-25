import assert from "node:assert/strict";
import { test } from "vitest";
import { createApiClient, type ApiTransport, type TransportRequest } from "./index";

test("generated log helpers preserve scopes, cursors, and node selection", async () => {
  const requests: Array<{ method: string; path: string }> = [];
  const transport: ApiTransport = {
    async request<Response, Body>(request: TransportRequest<Response, Body>): Promise<Response> {
      requests.push({ method: request.method, path: request.path });
      return {} as Response;
    }
  };
  const client = createApiClient(transport);

  await client.listLogs({ tail: 50, cursor: '{"node-a":7}', nodeId: "node a" });
  await client.listSystemLogs({ component: "maestro ingress", query: "level:error" });
  await client.listServiceLogs("service/a", { from: 10, to: 20 });
  await client.listDeploymentLogs("service/a", "deploy/b", { tail: 100 });
  await client.listBuildLogs("service/a", "build/b", { nodeId: "node-a" });
  await client.getLogHistogram({ from: 30, to: 40, bucketMs: 5, groupBy: "level" });
  await client.getSystemLogHistogram({ component: "daemon", nodeId: "node-a" });
  await client.getServiceLogHistogram("service/a", { query: "message:slow" });
  await client.getDeploymentLogHistogram("service/a", "deploy/b", { groupBy: "status" });
  await client.getBuildLogHistogram("service/a", "build/b", { from: 50, to: 60 });

  assert.deepEqual(requests, [
    {
      method: "GET",
      path: "/api/logs?tail=50&cursor=%7B%22node-a%22%3A7%7D&nodeId=node+a"
    },
    {
      method: "GET",
      path: "/api/system/logs?component=maestro+ingress&query=level%3Aerror"
    },
    { method: "GET", path: "/api/services/service%2Fa/logs?from=10&to=20" },
    {
      method: "GET",
      path: "/api/services/service%2Fa/deployments/deploy%2Fb/logs?tail=100"
    },
    {
      method: "GET",
      path: "/api/services/service%2Fa/builds/build%2Fb/logs?nodeId=node-a"
    },
    {
      method: "GET",
      path: "/api/logs/histogram?from=30&to=40&bucketMs=5&groupBy=level"
    },
    {
      method: "GET",
      path: "/api/system/logs/histogram?component=daemon&nodeId=node-a"
    },
    {
      method: "GET",
      path: "/api/services/service%2Fa/logs/histogram?query=message%3Aslow"
    },
    {
      method: "GET",
      path: "/api/services/service%2Fa/deployments/deploy%2Fb/logs/histogram?groupBy=status"
    },
    {
      method: "GET",
      path: "/api/services/service%2Fa/builds/build%2Fb/logs/histogram?from=50&to=60"
    }
  ]);
});
