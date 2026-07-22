import assert from "node:assert/strict";
import { test } from "vitest";
import type { Deployment, ReplicaState } from "./types";
import { replicaDisplayName, replicaFailure, sortDeploymentHistory } from "./deploymentView.ts";

function deployment(id: string, createdAt: number): Deployment {
  return {
    meta: { id, generation: 1, revision: 2 },
    spec: {
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

  assert.deepEqual(
    sortDeploymentHistory(source).map((entry) => entry.meta.id),
    ["deployment-b", "deployment-a"]
  );
  assert.deepEqual(source, [older, newer]);
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
          type: "Ready"
        }
      ]
    }
  } satisfies ReplicaState;

  assert.equal(replicaDisplayName(current, observed), "workload-api-0");
  assert.equal(replicaDisplayName(current, pending), "api-0");
  assert.equal(replicaFailure(pending), "health threshold exhausted");
});
