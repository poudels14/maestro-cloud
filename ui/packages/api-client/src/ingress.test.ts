import assert from "node:assert/strict";
import { test } from "vitest";
import { createApiClient, type ApiTransport, type TransportRequest } from "./index";

test("generated ingress helpers preserve query and patch contracts", async () => {
  const requests: Array<{ method: string; path: string; body: unknown }> = [];
  const transport: ApiTransport = {
    async request<Response, Body>(request: TransportRequest<Response, Body>): Promise<Response> {
      requests.push({ method: request.method, path: request.path, body: request.body });
      return {} as Response;
    }
  };
  const client = createApiClient(transport);

  await client.getIngressTraffic({ from: 10, to: 20, limit: 200, nodeId: "node a" });
  await client.getBlockedIngressTraffic({ from: 30, to: 40 });
  await client.setBlockedIngressIp({ ip: "192.0.2.7", blocked: true });
  await client.getIngressBlocklist();

  assert.deepEqual(requests, [
    {
      method: "GET",
      path: "/api/ingress/traffic?from=10&to=20&limit=200&nodeId=node+a",
      body: undefined
    },
    {
      method: "GET",
      path: "/api/ingress/blocked-traffic?from=30&to=40",
      body: undefined
    },
    {
      method: "PATCH",
      path: "/api/ingress/blocked-ips",
      body: { ip: "192.0.2.7", blocked: true }
    },
    { method: "GET", path: "/api/ingress/blocked-ips", body: undefined }
  ]);
});
