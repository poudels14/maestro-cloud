import type { ApiSchemas } from "@maestro/api-client";
import type { ClusterNode } from "./types";

const NODE_LIVENESS_WINDOW_MS = 30_000;
const UPGRADE_IN_PROGRESS_LABEL = "Upgrade in progress";
const UPGRADE_RUN_REASON_PREFIX = "UpgradeRun:";

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
        (condition) => condition.type === "MESH_READY"
      );
      const runtimeDelegated = node.spec.workloadNetworkMode === "runtimeDelegated";
      const dataPlaneReady =
        runtimeDelegated ||
        (network != null &&
          network.status.appliedGeneration === network.meta.generation &&
          meshCondition?.status === "true");
      const drainCondition = node.status.conditions?.find(
        (condition) => condition.type === "DRAINING"
      );
      const placementCondition = ["MAINTENANCE", "DRAINING"]
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
        workloadNetworkMode: node.spec.workloadNetworkMode,
        subnet: runtimeDelegated
          ? "runtime delegated"
          : (network?.spec.workloadSubnet ?? "unavailable"),
        dataPlaneReady,
        dataPlaneError:
          dataPlaneReady || runtimeDelegated ? null : meshReadinessError(network, meshCondition),
        version: node.status.version,
        alive: node.status.lastSeen >= nowMs - NODE_LIVENESS_WINDOW_MS,
        lastSeenAtMs: node.status.lastSeen,
        revision: node.meta.revision,
        state: {
          unschedulable: placementCondition != null,
          drainPending:
            drainCondition?.status === "unknown" &&
            drainCondition.reason === "ReplicatingArtifacts",
          drainedAtMs: placementCondition?.lastTransitionTime ?? null,
          reason:
            placementConditionLabel(placementCondition) ||
            (drainCondition?.status === "unknown"
              ? drainCondition.message || drainCondition.reason
              : null) ||
            null
        }
      } satisfies ClusterNode;
    })
    .sort((left, right) => left.hostname.localeCompare(right.hostname));
}

function placementConditionLabel(condition: ApiSchemas["Condition"] | undefined): string | null {
  if (!condition) return null;
  if (
    condition.type === "MAINTENANCE" &&
    condition.status === "true" &&
    condition.reason.startsWith(UPGRADE_RUN_REASON_PREFIX)
  ) {
    return UPGRADE_IN_PROGRESS_LABEL;
  }
  return condition.message || condition.reason || null;
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
