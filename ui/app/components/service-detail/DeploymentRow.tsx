import { For, Show } from "solid-js";
import clsx from "clsx";
import { GitCommitHorizontal } from "lucide-solid";
import type { ClusterNode, Deployment, ReplicaState } from "../../lib/types";
import { DeploymentMenu, STATUS_COLORS, StatusBadge, StatusDot } from "../../lib/ui";
import { formatDateTime } from "../../lib/format";

type Props = {
  deployment: Deployment;
  isLatest: boolean;
  isSelected: boolean;
  nodes: ClusterNode[];
  onOpen: () => void;
  onCancel: () => void;
  onStop: () => void;
  onRedeploy: () => void;
  onRestart: () => void;
  onRestartDeployment?: () => void;
};

function DeploymentRow(props: Props) {
  const shortId = () => props.deployment.id;
  const isLive = () => ["ready", "deploying", "building"].includes(props.deployment.status);
  const showReplicas = () =>
    props.deployment.replicas.length > 0 &&
    ["deploying", "ready"].includes(props.deployment.status);

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
              {props.deployment.gitCommit?.message ?? deploymentTitle(props.deployment)}
            </span>
          </div>
          <div class="mt-0.5 flex items-center gap-4 flex-wrap text-[11px] text-gray-400">
            <Show when={props.deployment.gitCommit}>
              <span class="inline-flex items-center gap-1 font-mono">
                <GitCommitHorizontal class="size-3" />
                {props.deployment.gitCommit!.reference.slice(0, 7)}
              </span>
            </Show>
            <span class="font-mono">{shortId()}</span>
            <Show when={props.deployment.failure}>
              {(failure) => <span class="text-red-600 truncate">{failure()}</span>}
            </Show>
          </div>
          <Show when={showReplicas()}>
            <div class="mt-1.5 space-y-0.5">
              <For each={props.deployment.replicas}>
                {(replica) => <ReplicaRow replica={replica} nodes={props.nodes} />}
              </For>
            </div>
          </Show>
        </div>
        <div class="flex items-center gap-2 shrink-0">
          <StatusBadge status={props.deployment.status} class="w-24 justify-center" />
          <span
            class="hidden sm:inline text-xs text-gray-400 tabular-nums"
            title={new Date(props.deployment.createdAtMs).toLocaleString()}
          >
            {formatDateTime(props.deployment.createdAtMs)}
          </span>
          <div onClick={(e) => e.stopPropagation()}>
            <DeploymentMenu
              status={props.deployment.status}
              onCancel={props.onCancel}
              onStop={props.onStop}
              onRedeploy={props.onRedeploy}
              onRestart={props.onRestart}
              onRestartDeployment={props.onRestartDeployment}
            />
          </div>
        </div>
      </div>
    </div>
  );
}

function ReplicaRow(props: { replica: ReplicaState; nodes: ClusterNode[] }) {
  const nodeName = () =>
    props.nodes.find((node) => node.id === props.replica.nodeId)?.name ?? props.replica.nodeId;
  const phaseColors = () => STATUS_COLORS[props.replica.phase] ?? STATUS_COLORS.stopped!;

  return (
    <div class="flex items-center gap-2 text-xs">
      <StatusDot status={props.replica.phase} />
      <span class="font-mono text-gray-600 truncate">{props.replica.replicaId}</span>
      <Show when={props.replica.ip}>
        {(ip) => <span class="font-mono text-gray-400">{ip()}</span>}
      </Show>
      <Show when={nodeName()}>
        {(name) => (
          <span class="rounded bg-gray-100 px-1.5 py-0.5 text-[10px] text-gray-500" title="Node">
            {name()}
          </span>
        )}
      </Show>
      <Show when={props.replica.phase !== "healthy"}>
        <span
          class={clsx("rounded-md border px-1.5 py-px text-[10px] font-medium", phaseColors().pill)}
        >
          {props.replica.phase}
        </span>
      </Show>
      <Show when={props.replica.restarts > 0}>
        <span class="text-[10px] text-gray-400 tabular-nums">
          {props.replica.restarts} {props.replica.restarts === 1 ? "restart" : "restarts"}
        </span>
      </Show>
    </div>
  );
}

function deploymentTitle(deployment: Deployment): string {
  return (deployment.image ?? deployment.spec.image ?? deployment.id).replace(
    /@sha256:[0-9a-f]+$/,
    ""
  );
}

export { DeploymentRow, ReplicaRow, deploymentTitle };
