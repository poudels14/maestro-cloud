import { Show } from "solid-js";
import clsx from "clsx";
import { GitCommitHorizontal } from "lucide-solid";
import type { Deployment, ReplicaState } from "../../lib/types";
import { replicaDisplayName } from "../../lib/deploymentView";
import { DeploymentMenu, STATUS_COLORS, StatusBadge, StatusDot } from "../../lib/ui";
import { formatDateTime } from "../../lib/format";

type Props = {
  deployment: Deployment;
  isLatest: boolean;
  isSelected: boolean;
  onOpen: () => void;
  onCancel: () => void;
  onRemove: () => void;
  onRedeploy: () => void;
  onRestart: () => void;
};

function DeploymentRow(props: Props) {
  const shortId = () => props.deployment.meta.id.split("-").at(-1) ?? props.deployment.meta.id;
  const phase = () => props.deployment.status.phase;
  const sourceRevision = () => {
    const artifact = props.deployment.spec.service.artifact;
    return artifact.type === "build" && artifact.source.type === "git"
      ? artifact.source.revision
      : null;
  };
  const isLive = () => ["BUILDING", "PENDING_READY", "READY"].includes(phase());

  return (
    <div
      role="button"
      tabIndex={0}
      onClick={() => props.onOpen()}
      onKeyDown={(event) => {
        if (event.key === "Enter" || event.key === " ") {
          event.preventDefault();
          props.onOpen();
        }
      }}
      class={clsx("px-4 sm:px-5 py-3 transition-colors cursor-pointer outline-none", {
        "bg-indigo-50/60": props.isSelected,
        "bg-emerald-100 hover:bg-emerald-100/80": props.isLatest && isLive() && !props.isSelected,
        "hover:bg-gray-50": !props.isSelected && !(props.isLatest && isLive())
      })}
    >
      <div class="flex items-start gap-3">
        <div class="pt-1">
          <StatusDot status={phase()} />
        </div>
        <div class="min-w-0 flex-1">
          <div class="flex items-center gap-2 min-w-0">
            <span class="text-sm font-medium text-gray-800 truncate">{shortId()}</span>
          </div>
          <div class="mt-0.5 flex items-center gap-4 flex-wrap text-[11px] text-gray-400">
            <Show when={sourceRevision()}>
              {(revision) => (
                <span class="inline-flex items-center gap-1 font-mono" title={revision()}>
                  <GitCommitHorizontal class="size-3" />
                  {revision().slice(0, 12)}
                </span>
              )}
            </Show>
            <span class="font-mono" title={props.deployment.spec.service.version}>
              {props.deployment.spec.service.version}
            </span>
          </div>
        </div>
        <div class="flex items-center gap-2 shrink-0">
          <Show when={phase() !== "REMOVED"} fallback={<span class="w-24" aria-hidden="true" />}>
            <StatusBadge status={phase()} class="w-24 justify-center" />
          </Show>
          <span
            class="hidden sm:inline text-xs text-gray-400 tabular-nums"
            title={new Date(props.deployment.status.createdAt).toLocaleString()}
          >
            {formatDateTime(props.deployment.status.createdAt)}
          </span>
          <div onClick={(event) => event.stopPropagation()}>
            <DeploymentMenu
              status={phase()}
              onCancel={props.onCancel}
              onRemove={props.onRemove}
              onRedeploy={props.onRedeploy}
              onRestart={props.onRestart}
            />
          </div>
        </div>
      </div>
    </div>
  );
}

function ReplicaRow(props: { deployment: Deployment; replica: ReplicaState }) {
  const phase = () => props.replica.status.phase;
  const colors = () => STATUS_COLORS[phase()] ?? STATUS_COLORS.STOPPED!;

  return (
    <div class="flex items-center gap-2 text-xs">
      <StatusDot status={phase()} />
      <span class="truncate font-mono text-gray-600">
        {replicaDisplayName(props.deployment, props.replica)}
      </span>
      <Show when={props.replica.status.nodeId}>
        {(nodeId) => (
          <span
            class="rounded bg-gray-100 px-1.5 py-0.5 font-mono text-[10px] text-gray-500"
            title="Cluster node"
          >
            <span class="font-semibold">NODE:</span> {nodeId()}
          </span>
        )}
      </Show>
      <Show when={phase() !== "READY"}>
        <span class={clsx("rounded-md border px-1.5 py-px text-[10px] font-medium", colors().pill)}>
          {phase().toLowerCase()}
        </span>
      </Show>
    </div>
  );
}

export { DeploymentRow, ReplicaRow };
