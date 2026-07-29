import { expect, test } from "vitest";
import type { ApiSchemas } from "@maestro/api-client";
import type { Service } from "./types";
import {
  attachPreviewResources,
  isSystemService,
  serviceDisplayStatus,
  serviceHasBuild,
  servicePreviews,
  userServices
} from "./serviceView";

function serviceResource(id: string): ApiSchemas["Service"] {
  return {
    meta: { id, generation: 1, revision: 2 },
    spec: {
      name: id,
      version: "test",
      artifact: { type: "image", reference: "registry.example/test@sha256:abc" },
      exec: "denied",
      nodeApi: "disabled",
      placement: {},
      replicas: 1
    },
    status: { rollout: "active" }
  };
}

function previewResource(
  id: string,
  serviceId: string,
  baseServiceId: string,
  pullRequestNumber: number
): ApiSchemas["Preview"] {
  return {
    meta: { id, generation: 1, revision: 3 },
    spec: {
      baseServiceId,
      closeGracePeriodSecs: 60,
      expiresAt: 10_000,
      headRevision: "abc",
      pullRequestNumber,
      repository: "owner/repo",
      serviceId
    },
    status: { phase: "active" }
  };
}

test("attaches preview ownership and hides derived services from the primary list", () => {
  const services = attachPreviewResources(
    [serviceResource("api"), serviceResource("api-pr-2")],
    [previewResource("preview-2", "api-pr-2", "api", 2)]
  );

  expect(userServices(services).map((service) => service.meta.id)).toEqual(["api"]);
  expect(services[1]?.previewResource?.spec.baseServiceId).toBe("api");
});

test("groups previews under their base service in pull request order", () => {
  const services = attachPreviewResources(
    [serviceResource("api-pr-20"), serviceResource("web-pr-1"), serviceResource("api-pr-3")],
    [
      previewResource("preview-20", "api-pr-20", "api", 20),
      previewResource("preview-web", "web-pr-1", "web", 1),
      previewResource("preview-3", "api-pr-3", "api", 3)
    ]
  );

  expect(servicePreviews(services, "api").map((service) => service.meta.id)).toEqual([
    "api-pr-3",
    "api-pr-20"
  ]);
});

test("projects service status and artifact capabilities from resource fields", () => {
  const idle = serviceResource("api") as Service;
  const ready = {
    ...idle,
    status: { ...idle.status, activeDeploymentId: "deployment-1" }
  } satisfies Service;
  const deleting = {
    ...ready,
    meta: { ...ready.meta, deletionTimestamp: 12 }
  } satisfies Service;
  const build = {
    ...idle,
    spec: {
      ...idle.spec,
      artifact: {
        type: "build" as const,
        dockerfile: "Dockerfile",
        source: { type: "git" as const, repository: "owner/repo", revision: "main" }
      }
    }
  } satisfies Service;

  expect(serviceDisplayStatus(idle)).toBe("IDLE");
  expect(serviceDisplayStatus(ready)).toBe("READY");
  expect(serviceDisplayStatus(deleting)).toBe("TERMINATED");
  expect(serviceHasBuild(idle)).toBe(false);
  expect(serviceHasBuild(build)).toBe(true);
  expect(isSystemService(serviceResource("maestro-system-traefik") as Service)).toBe(true);
  expect(isSystemService(idle)).toBe(false);
});
