import { expect, test } from "vitest";
import type { ApiSchemas } from "@maestro/api-client";
import type { Deployment, Service } from "./types";
import {
  attachPreviewResources,
  isSystemService,
  previewEnabledServices,
  previewPullRequestState,
  previewServices,
  previewDeploymentStatus,
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
      maxRestartAttempts: 10,
      nodeApi: "disabled",
      placement: {},
      replicas: 1
    },
    status: { rollout: "active" }
  };
}

function deployment(
  id: string,
  createdAt: number,
  phase: Deployment["status"]["phase"]
): Deployment {
  return {
    meta: { id, generation: 1, revision: 2 },
    spec: {
      bypassRolloutFreeze: false,
      goal: "run",
      restartGeneration: 1,
      service: serviceResource("api").spec,
      serviceGeneration: 1,
      serviceId: "api"
    },
    status: { createdAt, phase }
  };
}

function previewService(id: string, revision: string): Service {
  const service = serviceResource(id) as Service;
  service.spec.artifact = {
    type: "build",
    dockerfile: "Dockerfile",
    source: { type: "git", repository: "owner/repo", revision }
  };
  service.previewResource = previewResource("preview-1", id, "api", 1);
  service.previewResource.spec.headRevision = revision;
  service.previewResource.status.phase = "pending";
  return service;
}

function previewResource(
  id: string,
  serviceId: string,
  baseServiceId: string,
  pullRequestNumber: number,
  pullRequestState: ApiSchemas["PullRequestState"] = "open"
): ApiSchemas["Preview"] {
  return {
    meta: { id, generation: 1, revision: 3 },
    spec: {
      baseServiceId,
      closeGracePeriodSecs: 60,
      expiresAt: 10_000,
      headReference: `feature/${pullRequestNumber}`,
      headRevision: "abc",
      author: "octocat",
      pullRequestNumber,
      repository: "owner/repo",
      serviceId,
      title: `Preview ${pullRequestNumber}`
    },
    status: { phase: "active", pullRequestState }
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

test("classifies pull requests by source state rather than preview deployment phase", () => {
  const open = {
    ...serviceResource("api-pr-3"),
    previewResource: previewResource("preview-3", "api-pr-3", "api", 3, "open")
  } satisfies Service;
  open.previewResource.status.phase = "failed";
  const closed = {
    ...serviceResource("api-pr-4"),
    previewResource: previewResource("preview-4", "api-pr-4", "api", 4, "closed")
  } satisfies Service;
  closed.previewResource.status.phase = "active";

  expect(previewPullRequestState(open)).toBe("open");
  expect(previewPullRequestState(closed)).toBe("closed");
});

test("lists previews globally and identifies preview-enabled base services", () => {
  const api = {
    ...serviceResource("api"),
    spec: {
      ...serviceResource("api").spec,
      name: "API",
      preview: { closeGracePeriodSecs: 60, lifetimeSecs: 3600, replicas: 1 }
    }
  } satisfies ApiSchemas["Service"];
  const worker = {
    ...serviceResource("worker"),
    spec: {
      ...serviceResource("worker").spec,
      name: "Worker",
      preview: { closeGracePeriodSecs: 60, lifetimeSecs: 3600, replicas: 2 }
    }
  } satisfies ApiSchemas["Service"];
  const services = attachPreviewResources(
    [worker, serviceResource("worker-pr-20"), api, serviceResource("api-pr-3")],
    [
      previewResource("preview-20", "worker-pr-20", "worker", 20),
      previewResource("preview-3", "api-pr-3", "api", 3)
    ]
  );

  expect(previewServices(services).map((service) => service.meta.id)).toEqual([
    "api-pr-3",
    "worker-pr-20"
  ]);
  expect(previewEnabledServices(services).map((service) => service.meta.id)).toEqual([
    "api",
    "worker"
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

test("projects the latest deployment phase when a service has not activated", () => {
  const idle = previewService("api-pr-1", "new-head");
  const ready = {
    ...idle,
    status: { ...idle.status, activeDeploymentId: "deployment-ready" }
  } satisfies Service;
  const old = deployment("deployment-ready", 10, "READY");
  old.spec.serviceId = idle.meta.id;
  old.spec.serviceGeneration = 0;
  const crashed = deployment("deployment-crashed", 20, "CRASHED");
  crashed.spec.serviceId = idle.meta.id;
  crashed.spec.serviceGeneration = idle.meta.generation;
  crashed.spec.service = idle.spec;
  const deployments = [old, crashed];

  expect(previewDeploymentStatus(idle, deployments)).toBe("CRASHED");
  expect(previewDeploymentStatus(ready, deployments)).toBe("CRASHED");
  expect(previewDeploymentStatus(idle, [])).toBe("QUEUED");
});
