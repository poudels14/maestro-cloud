import assert from "node:assert/strict";
import { test } from "vitest";
import { createApiClient, type ApiTransport, type TransportRequest } from "@maestro/api-client";

test("generated service commands preserve revisions and idempotency", async () => {
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

  await client.redeployService("service/a", { expectedRevision: 11 }, "redeploy-key");
  await client.freezeService("service/a", { expectedRevision: 12 }, "freeze-key");
  await client.unfreezeService("service/a", { expectedRevision: 13 }, "unfreeze-key");
  await client.setServiceReplicas("service/a", { expectedRevision: 14, replicas: 4 }, "scale-key");
  await client.setServiceReplicas(
    "service/a",
    { expectedRevision: 15, replicas: null },
    "clear-key"
  );
  await client.deleteService("service/a", { expectedRevision: 16 }, "delete-key");

  assert.deepEqual(requests, [
    {
      method: "POST",
      path: "/api/services/service%2Fa/redeploy",
      body: { expectedRevision: 11 },
      idempotencyKey: "redeploy-key"
    },
    {
      method: "POST",
      path: "/api/services/service%2Fa/freeze",
      body: { expectedRevision: 12 },
      idempotencyKey: "freeze-key"
    },
    {
      method: "POST",
      path: "/api/services/service%2Fa/unfreeze",
      body: { expectedRevision: 13 },
      idempotencyKey: "unfreeze-key"
    },
    {
      method: "PUT",
      path: "/api/services/service%2Fa/replicas",
      body: { expectedRevision: 14, replicas: 4 },
      idempotencyKey: "scale-key"
    },
    {
      method: "PUT",
      path: "/api/services/service%2Fa/replicas",
      body: { expectedRevision: 15, replicas: null },
      idempotencyKey: "clear-key"
    },
    {
      method: "DELETE",
      path: "/api/services/service%2Fa",
      body: { expectedRevision: 16 },
      idempotencyKey: "delete-key"
    }
  ]);
});
