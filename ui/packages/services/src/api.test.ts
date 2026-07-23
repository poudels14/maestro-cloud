import { expect, test } from "vitest";
import type { ApiSchemas, MaestroApiClient } from "@maestro/api-client";
import { createServicesApi } from "./api";
import type { Deployment, Service } from "./types";

function service(): Service {
  return {
    meta: { id: "service/a", generation: 1, revision: 11 },
    spec: {
      name: "API",
      version: "v1",
      artifact: { type: "image", reference: "registry.example/api@sha256:abc" },
      exec: "denied",
      nodeApi: "disabled",
      placement: {},
      replicas: 1
    },
    status: { rollout: "active" }
  };
}

function deployment(): Deployment {
  return {
    meta: { id: "deployment/a", generation: 1, revision: 17 },
    spec: {
      bypassRolloutFreeze: false,
      goal: "run",
      restartGeneration: 0,
      service: service().spec,
      serviceGeneration: 1,
      serviceId: "service/a"
    },
    status: { createdAt: 10, phase: "READY" }
  };
}

test("passes optimistic service and deployment commands to the generated client", async () => {
  const calls: Array<{ operation: string; args: unknown[] }> = [];
  const record =
    (operation: string) =>
    async (...args: unknown[]) => {
      calls.push({ operation, args });
      return {};
    };
  const client = {
    deleteService: record("delete"),
    unfreezeService: record("unfreeze"),
    setServiceReplicas: record("replicas"),
    restartDeployment: record("restart")
  } as unknown as MaestroApiClient;
  const api = createServicesApi(
    () => client,
    (error) => error as Error
  );

  await api.deleteService(service());
  await api.setFrozen(service(), false);
  await api.setReplicas(service(), 4);
  await api.restartDeployment(deployment());

  expect(calls.map(({ operation, args }) => ({ operation, args: args.slice(0, -1) }))).toEqual([
    { operation: "delete", args: ["service/a", { expectedRevision: 11 }] },
    { operation: "unfreeze", args: ["service/a", { expectedRevision: 11 }] },
    {
      operation: "replicas",
      args: ["service/a", { expectedRevision: 11, replicas: 4 }]
    },
    {
      operation: "restart",
      args: ["service/a", "deployment/a", { expectedRevision: 17 }]
    }
  ]);
  for (const call of calls) {
    expect(call.args.at(-1)).toMatch(/^[0-9a-f-]{36}$/);
  }
});

test("joins preview ownership and sorts deployment reads at the boundary", async () => {
  const base = service();
  const previewService = {
    ...base,
    meta: { ...base.meta, id: "service/a-pr-7" }
  } satisfies ApiSchemas["Service"];
  const preview = {
    meta: { id: "preview-7", generation: 1, revision: 2 },
    spec: {
      baseServiceId: "service/a",
      closeGracePeriodSecs: 60,
      expiresAt: 10_000,
      headRevision: "abc",
      pullRequestNumber: 7,
      repository: "owner/repo",
      serviceId: "service/a-pr-7"
    },
    status: { phase: "active" }
  } satisfies ApiSchemas["Preview"];
  const older = deployment();
  const newer = {
    ...older,
    meta: { ...older.meta, id: "deployment/b" },
    status: { ...older.status, createdAt: 20 }
  } satisfies Deployment;
  const client = {
    async listServices() {
      return [base, previewService];
    },
    async listPreviews() {
      return [preview];
    },
    async listDeployments() {
      return [older, newer];
    }
  } as unknown as MaestroApiClient;
  const api = createServicesApi(
    () => client,
    (error) => error as Error
  );

  const services = await api.listServices();
  const deployments = await api.listDeployments("service/a");

  expect(services[1]?.previewResource?.meta.id).toBe("preview-7");
  expect(deployments.map((entry) => entry.meta.id)).toEqual(["deployment/b", "deployment/a"]);
});

test("maps generated client failures at the services boundary", async () => {
  const client = {
    async listServices() {
      throw new Error("transport detail");
    },
    async listPreviews() {
      return [];
    }
  } as unknown as MaestroApiClient;
  const api = createServicesApi(
    () => client,
    (_error, fallback) => new Error(`${fallback} (mapped)`)
  );

  await expect(api.listServices()).rejects.toThrow("Failed to load services (mapped)");
});
