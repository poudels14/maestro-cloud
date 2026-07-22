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

test("generated deployment reads and commands preserve resource scope", async () => {
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
      return [] as Response;
    }
  };
  const client = createApiClient(transport);

  await client.listDeployments("service/a");
  await client.listReplicas("service/a", "deployment/b");
  await client.restartDeployment(
    "service/a",
    "deployment/b",
    { expectedRevision: 21 },
    "restart-key"
  );
  await client.cancelDeployment(
    "service/a",
    "deployment/b",
    { expectedRevision: 22 },
    "cancel-key"
  );
  await client.removeDeployment(
    "service/a",
    "deployment/b",
    { expectedRevision: 23 },
    "remove-key"
  );

  assert.deepEqual(requests, [
    {
      method: "GET",
      path: "/api/services/service%2Fa/deployments",
      body: undefined,
      idempotencyKey: undefined
    },
    {
      method: "GET",
      path: "/api/services/service%2Fa/deployments/deployment%2Fb/replicas",
      body: undefined,
      idempotencyKey: undefined
    },
    {
      method: "POST",
      path: "/api/services/service%2Fa/deployments/deployment%2Fb/restart",
      body: { expectedRevision: 21 },
      idempotencyKey: "restart-key"
    },
    {
      method: "POST",
      path: "/api/services/service%2Fa/deployments/deployment%2Fb/cancel",
      body: { expectedRevision: 22 },
      idempotencyKey: "cancel-key"
    },
    {
      method: "POST",
      path: "/api/services/service%2Fa/deployments/deployment%2Fb/remove",
      body: { expectedRevision: 23 },
      idempotencyKey: "remove-key"
    }
  ]);
});
