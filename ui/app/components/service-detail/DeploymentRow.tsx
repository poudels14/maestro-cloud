import { For, Show } from "solid-js";
import clsx from "clsx";
import { GitCommitHorizontal } from "lucide-solid";
import type { Deployment } from "../../lib/types";
import { DeploymentMenu, STATUS_COLORS, StatusBadge, StatusDot } from "../../lib/ui";
import { formatDateTime } from "../../lib/format";
import { replicaHostname } from "../../lib/deploymentEndpoints";

type Props = {
  deployment: Deployment;
  isLatest: boolean;
  isSelected: boolean;
  onOpen: () => void;
  onCancel: () => void;
  onStop: () => void;
  onRedeploy: () => void;
  onRestart: () => void;
};

function DeploymentRow(props: Props) {
  const shortId = () => props.deployment.id.split("-").slice(-1)[0] ?? props.deployment.id;
  const isLive = () =>
    ["READY", "RUNNING", "DEPLOYING", "PENDING_READY", "BUILDING"].includes(
      props.deployment.status
    );
  const changedSecrets = () =>
    Object.entries(props.deployment.config.deploy.secrets?.keys ?? {})
      .filter(([, meta]) => meta.changed)
      .map(([key]) => key);
  const showReplicas = () =>
    props.deployment.replicas &&
    props.deployment.replicas.length > 0 &&
    !["TERMINATED", "REMOVED", "CANCELED", "DRAINING"].includes(props.deployment.status);

  return (
    <div
      role="button"
      tabIndex={0}
      onClick={() => props.onOpen()}
      onKeyDown={(e) => {
        if (e.key === "Enter" || e.key === " ") {
          e.preventDefault();
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
          <StatusDot status={props.deployment.status} />
        </div>
        <div class="min-w-0 flex-1">
          <div class="flex items-center gap-2 min-w-0">
            <span class="text-sm font-medium text-gray-800 truncate">
              {props.deployment.gitCommit ? props.deployment.gitCommit.message : shortId()}
            </span>
          </div>
          <div class="mt-0.5 flex items-center gap-4 flex-wrap text-[11px] text-gray-400">
            <Show when={props.deployment.gitCommit}>
              <span class="inline-flex items-center gap-1 font-mono">
                <GitCommitHorizontal class="size-3" />
                {props.deployment.gitCommit!.reference.slice(0, 7)}
              </span>
            </Show>
            <span class="font-mono" title={props.deployment.config.version}>
              {shortId()}
            </span>
            <Show when={changedSecrets().length > 0}>
              <span class="text-amber-600" title={changedSecrets().join(", ")}>
                secrets changed: {changedSecrets().join(", ")}
              </span>
            </Show>
          </div>
        </div>
        <div class="flex items-center gap-2 shrink-0">
          <Show
            when={props.deployment.status !== "REMOVED"}
            fallback={<span class="w-24" aria-hidden="true" />}
          >
            <StatusBadge status={props.deployment.status} class="w-24 justify-center" />
          </Show>
          <span
            class="hidden sm:inline text-xs text-gray-400 tabular-nums"
            title={new Date(props.deployment.createdAt).toLocaleString()}
          >
            {formatDateTime(props.deployment.createdAt)}
          </span>
          <div onClick={(e) => e.stopPropagation()}>
            <DeploymentMenu
              status={props.deployment.status}
              onCancel={props.onCancel}
              onStop={props.onStop}
              onRedeploy={props.onRedeploy}
              onRestart={props.onRestart}
            />
          </div>
        </div>
      </div>
      <Show when={showReplicas()}>
        <div class="mt-1.5 space-y-0.5 pl-5">
          <For each={props.deployment.replicas}>
            {(replica) => (
              <ReplicaRow
                deployment={props.deployment}
                replicaIndex={replica.replicaIndex}
                replicaStatus={replica.status}
                nodeId={replica.nodeId}
                containerHostname={replica.endpoint?.containerHostname}
              />
            )}
          </For>
        </div>
      </Show>
    </div>
  );
}

function ReplicaRow(props: {
  deployment: Deployment;
  replicaIndex: number;
  replicaStatus: string;
  nodeId?: string | null;
  containerHostname?: string | null;
}) {
  const hostname = () =>
    replicaHostname(props.deployment, props.replicaIndex, props.containerHostname);
  const replicaStatusColors = () => STATUS_COLORS[props.replicaStatus] ?? STATUS_COLORS.STOPPED!;

  return (
    <div class="flex items-center gap-2 text-xs">
      <StatusDot status={props.replicaStatus} />
      <span class="truncate font-mono text-gray-600">{hostname()}</span>
      <Show when={props.nodeId}>
        {(nodeId) => (
          <span
            class="rounded bg-gray-100 px-1.5 py-0.5 font-mono text-[10px] text-gray-500"
            title="Cluster node"
          >
            <span class="font-semibold">NODE:</span> {nodeId()}
          </span>
        )}
      </Show>
      <Show when={props.replicaStatus !== "READY"}>
        <span
          class={clsx(
            "rounded-md border px-1.5 py-px text-[10px] font-medium",
            replicaStatusColors().pill
          )}
        >
          {props.replicaStatus.toLowerCase()}
        </span>
      </Show>
    </div>
  );
}

export { DeploymentRow, ReplicaRow };
