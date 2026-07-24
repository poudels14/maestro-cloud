import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { test } from "vitest";
import {
  createApiClient,
  type ApiSchemas,
  type ApiTransport,
  type TransportRequest
} from "@maestro/api-client";

const nonPanelOperations = [
  "discoverClusterCa",
  "getOpenApi",
  "health",
  "joinCluster",
  "uploadArtifactArchive"
] as const;

test("typed client covers every panel-compatible OpenAPI operation", () => {
  const document = JSON.parse(
    readFileSync(new URL("../openapi.json", import.meta.url), "utf8")
  ) as OpenApiDocument;
  const operationIds = Object.values(document.paths)
    .flatMap((item) => Object.values(item))
    .flatMap((operation) => (operation.operationId === undefined ? [] : [operation.operationId]))
    .sort();
  const client = createApiClient({
    request: async () => {
      throw new Error("operation coverage does not execute transport requests");
    }
  });
  const implemented = Object.keys(client).sort();

  assert.deepEqual(
    operationIds.filter((operationId) => !implemented.includes(operationId)),
    nonPanelOperations
  );
});

test("new operator operations preserve paths, queries, bodies, and idempotency", async () => {
  const requests: CapturedRequest[] = [];
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

  await client.listClusterNodeStats();
  await client.listPlacementHistory({
    serviceId: "service/a",
    deploymentId: "deployment/b",
    replicaIndex: 2
  });
  await client.getTailscaleAuthKeyStatus();
  await client.rotateTailscaleAuthKey(
    {} as ApiSchemas["TailscaleAuthKeyRotationRequest"],
    "tailscale-key"
  );
  await client.diffService("service/a", {} as ApiSchemas["ServiceDiffRequest"]);
  await client.applyServiceRollout(
    "service/a",
    {} as ApiSchemas["ServiceRolloutRequest"],
    "rollout-key"
  );
  await client.diffServiceRollout("service/a", {} as ApiSchemas["ServiceRolloutDiffRequest"]);
  await client.getServiceTrafficBreakdown("service/a", {
    from: 10,
    to: 20,
    limit: 30,
    nodeId: "node/b"
  });

  assert.deepEqual(requests, [
    get("/api/cluster/stats/nodes"),
    get("/api/cluster/placements?serviceId=service%2Fa&deploymentId=deployment%2Fb&replicaIndex=2"),
    get("/api/cluster/tailscale/auth-key"),
    mutation("PUT", "/api/cluster/tailscale/auth-key", "tailscale-key"),
    mutation("POST", "/api/services/service%2Fa/diff"),
    mutation("POST", "/api/services/service%2Fa/rollout", "rollout-key"),
    mutation("POST", "/api/services/service%2Fa/rollout/diff"),
    get("/api/services/service%2Fa/traffic/breakdown?from=10&to=20&limit=30&nodeId=node%2Fb")
  ]);
});

interface OpenApiOperation {
  operationId?: string;
}

interface OpenApiDocument {
  paths: Record<string, Record<string, OpenApiOperation>>;
}

interface CapturedRequest {
  method: string;
  path: string;
  body: unknown;
  idempotencyKey: string | undefined;
}

function get(path: string): CapturedRequest {
  return {
    method: "GET",
    path,
    body: undefined,
    idempotencyKey: undefined
  };
}

function mutation(method: string, path: string, idempotencyKey?: string): CapturedRequest {
  return {
    method,
    path,
    body: {},
    idempotencyKey
  };
}
