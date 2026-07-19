import type { ClusterMaintenanceRun } from "./api";
import { nodeAdminLabel } from "./nodeAdmin";
import type { ClusterNode } from "./types";

const MAINTENANCE_STAGE_LABELS: Record<string, string> = {
  "awaiting-leadership-transfer": "transferring leadership",
  "upgrade-requested": "requesting upgrade",
  "self-restart-pending": "restarting leader",
  "updating-source": "updating source",
  "validating-source": "validating source",
  "rebuilding-system": "rebuilding system",
  "prebuilding-images": "pre-building system images",
  restarting: "restarting",
  verifying: "verifying health",
  restoring: "restoring placement"
};

interface ActiveMaintenanceNode {
  nodeId: string;
  label: string;
  adminUrl: string | null;
}

function activeMaintenanceNode(
  run: ClusterMaintenanceRun | null | undefined,
  nodes: ClusterNode[] | undefined
): ActiveMaintenanceNode | null {
  const step = run?.nodes[run.currentNodeIndex];
  if (!step) return null;
  const node = nodes?.find((candidate) => candidate.nodeId === step.nodeId);
  const adminUrl = node?.adminUrl ?? null;
  const hostname = node?.hostname || step.hostname;
  const label = adminUrl
    ? nodeAdminLabel(adminUrl)
    : hostname && hostname !== "unknown-host"
      ? hostname
      : step.nodeId;
  return { nodeId: step.nodeId, label, adminUrl };
}

function maintenanceStageLabel(run: ClusterMaintenanceRun | null | undefined) {
  if (!run) return null;
  const step = run.nodes[run.currentNodeIndex];
  const stage =
    run.phase === "restoring" ? run.phase : step?.upgradeStage || run.phase || step?.status;
  if (!stage) return null;
  return MAINTENANCE_STAGE_LABELS[stage] ?? stage.replaceAll("-", " ");
}

export { activeMaintenanceNode, maintenanceStageLabel };
