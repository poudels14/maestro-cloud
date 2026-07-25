import assert from "node:assert/strict";
import { test } from "vitest";
import { createApiClient, type ApiTransport, type TransportRequest } from "./index";

test("cluster admission helpers use the protected approval resource", async () => {
  const requests: Array<{ method: string; path: string; body: unknown }> = [];
  const transport: ApiTransport = {
    async request<Response, Body>(request: TransportRequest<Response, Body>): Promise<Response> {
      requests.push({ method: request.method, path: request.path, body: request.body });
      return {} as Response;
    }
  };
  const approval = {
    nodeId: "worker-a",
    publicKeySha256: "ab".repeat(32)
  };

  const client = createApiClient(transport);
  await client.listClusterAdmissions();
  await client.approveClusterAdmission(approval);

  assert.deepEqual(requests, [
    { method: "GET", path: "/api/cluster/admissions", body: undefined },
    { method: "POST", path: "/api/cluster/admissions", body: approval }
  ]);
});
