import assert from "node:assert/strict";
import { test } from "vitest";
import { createApiClient, type ApiTransport, type TransportRequest } from "@maestro/api-client";

test("generated config helper uses the masked operator view", async () => {
  const requests: Array<{ method: string; path: string }> = [];
  const transport: ApiTransport = {
    async request<Response, Body>(request: TransportRequest<Response, Body>): Promise<Response> {
      requests.push({ method: request.method, path: request.path });
      return {} as Response;
    }
  };

  await createApiClient(transport).getClusterConfig();

  assert.deepEqual(requests, [{ method: "GET", path: "/api/config" }]);
});
