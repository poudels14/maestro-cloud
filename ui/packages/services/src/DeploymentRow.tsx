import { For, Show } from "solid-js";
import clsx from "clsx";
import { ArrowUpRight, GitCommitHorizontal } from "lucide-solid";
import { useQuery } from "@maestro/sdk";
import { STATUS_COLORS, StatusBadge, StatusDot } from "@maestro/kit";
import type { ServicesApi } from "./api";
import type { Deployment, ReplicaState } from "./types";
import {
  deploymentFailure,
  deploymentGitRevision,
  deploymentTitle,
  replicaDisplayName
} from "./deploymentView";
import { deploymentReplicasQuery } from "./queries";
import { formatDateTime } from "@maestro/kit";
import { DeploymentMenu } from "./DeploymentMenu";

type Props = {
  api: ServicesApi;
  deployment: Deployment;
  actionsEnabled: boolean;
  showReplicas: boolean;
  replicaUrl: (replicaIndex: number) => string | null;
  isLatest: boolean;
  isSelected: boolean;
  onOpen: () => void;
  onCancel: () => void;
  onRemove: () => void;
  onRedeploy: () => void;
  onRestart: () => void;
};

function DeploymentRow(props: Props) {
  const phase = () => props.deployment.status.phase;
  const sourceRevision = () => deploymentGitRevision(props.deployment);
  const failure = () => deploymentFailure(props.deployment);
  const isLive = () => ["BUILDING", "PUBLISHING", "PENDING_READY", "READY"].includes(phase());
  const replicas = useQuery(() => ({
    ...deploymentReplicasQuery(props.api, props.deployment),
    enabled: props.showReplicas && isLive()
  }));

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
      class={clsx("relative px-4 sm:px-5 py-3 transition-colors cursor-pointer outline-none", {
        "bg-brand-light": props.isSelected,
        "bg-emerald-100/60 hover:bg-emerald-100/80":
          props.isLatest && isLive() && !props.isSelected,
        "hover:bg-gray-50": !props.isSelected && !(props.isLatest && isLive())
      })}
    >
      <div class="flex items-start gap-3">
        <div class="pt-1">
          <StatusDot status={phase()} />
        </div>
        <div class="min-w-0 flex-1">
          <div class="flex items-center gap-2 min-w-0">
            <span class="text-sm font-medium text-gray-800 truncate">
              {deploymentTitle(props.deployment)}
            </span>
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
          <Show when={failure()}>
            {(message) => <p class="mt-1 text-[11px] text-red-500 break-words">{message()}</p>}
          </Show>
          <Show when={(replicas.data?.length ?? 0) > 0}>
            <div class="mt-2 space-y-1">
              <For each={replicas.data}>
                {(replica) => (
                  <ReplicaLine
                    deployment={props.deployment}
                    replica={replica}
                    url={props.replicaUrl(replica.spec.replicaIndex)}
                  />
                )}
              </For>
            </div>
          </Show>
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
          <Show when={props.actionsEnabled}>
            <div onClick={(event) => event.stopPropagation()}>
              <DeploymentMenu
                status={phase()}
                onCancel={props.onCancel}
                onRemove={props.onRemove}
                onRedeploy={props.onRedeploy}
                onRestart={props.onRestart}
              />
            </div>
          </Show>
        </div>
      </div>
    </div>
  );
}

function ReplicaLine(props: { deployment: Deployment; replica: ReplicaState; url: string | null }) {
  const hostname = () => props.url?.replace(/^https?:\/\//, "");

  return (
    <div class="flex min-w-0 items-center gap-2 text-[11px]">
      <StatusDot status={props.replica.status.phase} />
      <Show
        when={props.url}
        fallback={
          <span class="truncate font-mono text-gray-500">
            {replicaDisplayName(props.deployment, props.replica)}
          </span>
        }
      >
        {(url) => (
          <a
            href={url()}
            target="_blank"
            rel="noreferrer"
            title={url()}
            onClick={(event) => event.stopPropagation()}
            class="inline-flex min-w-0 items-center gap-1 font-mono text-gray-600 outline-none hover:text-brand hover:underline"
          >
            <span class="truncate">{hostname()}</span>
            <ArrowUpRight class="size-3 shrink-0 text-gray-400" />
          </a>
        )}
      </Show>
      <Show when={props.replica.status.nodeId}>
        {(nodeId) => (
          <span
            class="shrink-0 rounded bg-gray-100 px-1.5 py-0.5 font-mono text-[10px] text-gray-500"
            title="Cluster node"
          >
            <span class="font-semibold">NODE:</span> {nodeId()}
          </span>
        )}
      </Show>
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
