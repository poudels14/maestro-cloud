import type { ClusterNode, UpgradeRun } from "./types";

const MAINTENANCE_STAGE_LABELS: Record<string, string> = {
  pending: "waiting to start",
  draining: "draining workloads",
  applying: "applying upgrade",
  restarting: "restarting",
  verifying: "verifying health",
  completed: "completed",
  failed: "failed",
  canceled: "canceled"
};

const TERMINAL_PHASES = new Set(["completed", "failed", "canceled"]);

interface ActiveMaintenanceNode {
  nodeId: string;
  label: string;
}

function activeMaintenanceNode(
  run: UpgradeRun | null | undefined,
  nodes: ClusterNode[] | undefined
): ActiveMaintenanceNode | null {
  const steps = run?.status.nodes ?? [];
  const step =
    steps.find(
      (candidate) => candidate.phase !== "pending" && !TERMINAL_PHASES.has(candidate.phase)
    ) ?? steps.find((candidate) => !TERMINAL_PHASES.has(candidate.phase));
  if (!step) return null;
  const node = nodes?.find((candidate) => candidate.nodeId === step.nodeId);
  const label = node?.hostname && node.hostname !== "unknown-host" ? node.hostname : step.nodeId;
  return { nodeId: step.nodeId, label };
}

function maintenanceStageLabel(run: UpgradeRun | null | undefined) {
  if (!run) return null;
  const activePhase = run.status.nodes?.find(
    (candidate) => candidate.phase !== "pending" && !TERMINAL_PHASES.has(candidate.phase)
  )?.phase;
  const phase = activePhase ?? run.status.phase;
  return MAINTENANCE_STAGE_LABELS[phase] ?? phase;
}

export { activeMaintenanceNode, maintenanceStageLabel };
