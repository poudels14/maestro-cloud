import type { ApiSchemas } from "@maestro/api-client";
import type { ClusterNode } from "./types";

const NODE_LIVENESS_WINDOW_MS = 30_000;

function projectClusterNodes(
  nodes: ApiSchemas["Node"][],
  networks: ApiSchemas["NodeNetwork"][],
  nowMs = Date.now()
): ClusterNode[] {
  const networksByNode = new Map(networks.map((network) => [network.spec.nodeId, network]));
  return nodes
    .map((node) => {
      const network = networksByNode.get(node.meta.id);
      const meshCondition = network?.status.conditions?.find(
        (condition) => condition.type === "MeshReady"
      );
      const dataPlaneReady =
        network != null &&
        network.status.appliedGeneration === network.meta.generation &&
        meshCondition?.status === "true";
      const placementCondition = ["Maintenance", "Draining"]
        .map((type) =>
          node.status.conditions?.find(
            (condition) => condition.type === type && condition.status === "true"
          )
        )
        .find((condition) => condition != null);
      return {
        nodeId: node.meta.id,
        hostname: node.spec.hostname,
        role: node.spec.role,
        hostAddress: node.spec.hostAddress,
        subnet: network?.spec.workloadSubnet ?? "unavailable",
        dataPlaneReady,
        dataPlaneError: dataPlaneReady ? null : meshReadinessError(network, meshCondition),
        version: node.status.version,
        alive: node.status.lastSeen >= nowMs - NODE_LIVENESS_WINDOW_MS,
        lastSeenAtMs: node.status.lastSeen,
        revision: node.meta.revision,
        state: {
          unschedulable: placementCondition != null,
          drainedAtMs: placementCondition?.lastTransitionTime ?? null,
          reason: placementCondition?.message || placementCondition?.reason || null
        }
      } satisfies ClusterNode;
    })
    .sort((left, right) => left.hostname.localeCompare(right.hostname));
}

function meshReadinessError(
  network: ApiSchemas["NodeNetwork"] | undefined,
  condition: ApiSchemas["Condition"] | undefined
): string {
  if (!network) return "Mesh network is not published";
  if (network.status.appliedGeneration !== network.meta.generation) {
    return "Mesh network generation is not applied";
  }
  return condition?.message || condition?.reason || "Mesh network is not ready";
}

export { projectClusterNodes };
