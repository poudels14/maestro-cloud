import assert from "node:assert/strict";
import { test } from "vitest";
import { createApiClient, type ApiTransport, type TransportRequest } from "@maestro/api-client";

test("node removal confirms identity and carries its replay key", async () => {
  const requests: Array<{
    method: string;
    path: string;
    body: unknown;
    idempotencyKey: string | undefined;
  }> = [];
  const transport: ApiTransport = {
    async request<Response, Body>(request: TransportRequest<Response, Body>): Promise<Response> {
      requests.push({
        method: request.method,
        path: request.path,
        body: request.body,
        idempotencyKey: request.headers?.["Idempotency-Key"]
      });
      return {} as Response;
    }
  };

  const client = createApiClient(transport);
  await client.removeNode("worker/a", { nodeId: "worker/a" }, "remove-worker-a");

  assert.deepEqual(requests, [
    {
      method: "DELETE",
      path: "/api/cluster/nodes/worker%2Fa",
      body: { nodeId: "worker/a" },
      idempotencyKey: "remove-worker-a"
    }
  ]);
});
