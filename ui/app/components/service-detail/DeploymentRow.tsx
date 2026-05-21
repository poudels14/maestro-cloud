import { For, Show } from "solid-js";
import clsx from "clsx";
import { Clock, ExternalLink, GitCommitHorizontal } from "lucide-solid";
import type { ClusterInfo } from "../../lib/api";
import type { Deployment } from "../../lib/types";
import { DeploymentMenu, StatusBadge, StatusDot, timeAgo } from "../../lib/ui";

type Props = {
  deployment: Deployment;
  isLatest: boolean;
  isSelected: boolean;
  clusterInfo: ClusterInfo | null;
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
      class={clsx("rounded-xl border overflow-hidden transition-all cursor-pointer outline-none", {
        "bg-emerald-50 border-emerald-300": props.isLatest && isLive() && !props.isSelected,
        "bg-white border-indigo-300 shadow-md ring-2 ring-indigo-100": props.isSelected,
        "bg-white border-gray-200 hover:shadow-sm hover:border-gray-300":
          !(props.isLatest && isLive()) && !props.isSelected
      })}
    >
      <div class="px-5 py-4">
        <div class="flex items-start justify-between gap-3 mb-2">
          <div class="min-w-0 flex-1">
            <div class="text-lg font-semibold text-gray-900 truncate leading-snug tracking-tight">
              {props.deployment.gitCommit ? props.deployment.gitCommit.message : shortId()}
            </div>
          </div>
          <div class="flex items-center gap-1.5 shrink-0">
            <span
              class="flex items-center gap-1 text-xs text-gray-400"
              title={new Date(props.deployment.createdAt).toLocaleString()}
            >
              <Clock class="size-3" />
              {timeAgo(props.deployment.createdAt)}
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
        <div class="flex items-center gap-2 flex-wrap text-xs mb-3">
          <StatusBadge status={props.deployment.status} />
          <Show when={props.deployment.gitCommit}>
            <span class="inline-flex items-center gap-1 text-gray-500 font-mono bg-gray-50 border border-gray-200 rounded px-1.5 py-0.5">
              <GitCommitHorizontal class="size-3 text-gray-400" />
              {props.deployment.gitCommit!.reference.slice(0, 7)}
            </span>
          </Show>
          <span class="font-mono text-gray-400" title={props.deployment.config.version}>
            {shortId()}
          </span>
        </div>
        <Show when={showReplicas()}>
          <div class="bg-white rounded-md border border-gray-200 divide-y divide-gray-100 mb-1">
            <For each={props.deployment.replicas}>
              {(replica) => (
                <ReplicaRow
                  deployment={props.deployment}
                  replicaIndex={replica.replicaIndex}
                  replicaStatus={replica.status}
                  clusterInfo={props.clusterInfo}
                />
              )}
            </For>
          </div>
        </Show>
        <Show when={changedSecrets().length > 0}>
          <div class="flex items-center gap-1.5 flex-wrap text-xs mt-2">
            <span class="text-amber-600 font-medium">secrets changed:</span>
            <For each={changedSecrets()}>
              {(key) => (
                <span class="inline-flex items-center bg-amber-50 border border-amber-200 text-amber-700 rounded px-1.5 py-0.5 font-mono text-[11px]">
                  {key}
                </span>
              )}
            </For>
          </div>
        </Show>
      </div>
    </div>
  );
}

function ReplicaRow(props: {
  deployment: Deployment;
  replicaIndex: number;
  replicaStatus: string;
  clusterInfo: ClusterInfo | null;
}) {
  const shortDepId = () => props.deployment.id.slice(0, 6);
  const hostname = () =>
    props.replicaIndex === 0
      ? `${props.deployment.config.id}-${shortDepId()}`
      : `${props.deployment.config.id}-${shortDepId()}-${props.replicaIndex}`;
  const fqdn = () =>
    props.clusterInfo ? `${hostname()}.${props.clusterInfo.canonicalDomain}` : null;
  const href = () => {
    const host = fqdn();
    if (!host) return null;
    const port = props.deployment.config.ingress?.port;
    return port ? `http://${host}:${port}` : `http://${host}`;
  };

  return (
    <div class="flex items-center gap-2 text-xs px-2.5 py-1.5">
      <StatusDot status={props.replicaStatus} />
      <Show
        when={href()}
        fallback={<span class="font-mono text-gray-700 truncate">{fqdn() ?? hostname()}</span>}
      >
        {(url) => (
          <a
            href={url()}
            target="_blank"
            rel="noopener noreferrer"
            onClick={(e) => e.stopPropagation()}
            title="Open in new tab"
            class="group inline-flex items-center gap-1 font-mono text-gray-700 hover:text-indigo-600 truncate"
          >
            <span class="truncate">{fqdn()}</span>
            <ExternalLink class="size-3 text-gray-400 group-hover:text-indigo-500 shrink-0" />
          </a>
        )}
      </Show>
      <span class="text-gray-400 ml-auto">{props.replicaStatus.toLowerCase()}</span>
    </div>
  );
}

export { DeploymentRow };
