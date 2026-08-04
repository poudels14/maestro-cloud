import { expect, test } from "vitest";
import type { Deployment, ReplicaState } from "./types";
import { replicaDisplayName, replicaFailure, sortDeploymentHistory } from "./deploymentView";

function deployment(id: string, createdAt: number): Deployment {
  return {
    meta: { id, generation: 1, revision: 2 },
    spec: {
      bypassRolloutFreeze: false,
      goal: "run",
      restartGeneration: 1,
      service: {
        name: "API",
        version: "v1",
        artifact: { type: "image", reference: "registry.example/api@sha256:abc" },
        exec: "denied",
        nodeApi: "disabled",
        placement: {},
        replicas: 1
      },
      serviceGeneration: 1,
      serviceId: "api"
    },
    status: { createdAt, phase: "READY" }
  };
}

function replica(workloadId?: string): ReplicaState {
  return {
    meta: { id: "replica-0", generation: 1, revision: 3 },
    spec: {
      assignmentId: "assignment-0",
      deploymentId: "deployment-a",
      replicaIndex: 0,
      serviceId: "api"
    },
    status: {
      healthcheckFailures: 0,
      phase: "READY",
      restartAttempts: 0,
      ...(workloadId ? { workloadId } : {})
    }
  };
}

test("deployment history sorts newest first without mutating API results", () => {
  const older = deployment("deployment-a", 10);
  const newer = deployment("deployment-b", 20);
  const source = [older, newer];

  expect(sortDeploymentHistory(source).map((entry) => entry.meta.id)).toEqual([
    "deployment-b",
    "deployment-a"
  ]);
  expect(source).toEqual([older, newer]);
});

test("replica presentation prefers workload identity and surfaces failed conditions", () => {
  const current = deployment("deployment-a", 10);
  const observed = replica("workload-api-0");
  const pending = {
    ...replica(),
    status: {
      ...replica().status,
      phase: "CRASHED" as const,
      conditions: [
        {
          lastTransitionTime: 10,
          message: "health threshold exhausted",
          observedGeneration: 1,
          reason: "Unhealthy",
          status: "false" as const,
          type: "READY"
        }
      ]
    }
  } satisfies ReplicaState;

  expect(replicaDisplayName(current, observed)).toBe("workload-api-0");
  expect(replicaDisplayName(current, pending)).toBe("api-0");
  expect(replicaFailure(pending)).toBe("health threshold exhausted");
});
