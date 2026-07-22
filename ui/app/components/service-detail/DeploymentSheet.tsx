import { createSignal, For, Show, Switch, Match } from "solid-js";
import { Check, Copy, GitCommitHorizontal, X } from "lucide-solid";
import { Dialog } from "@kobalte/core/dialog";
import clsx from "clsx";
import type { Deployment } from "../../lib/types";
import { replicaFailure } from "../../lib/deploymentView";
import { deploymentReplicasQuery } from "../../lib/queries";
import { useQuery } from "../../lib/useQuery";
import { ErrorBanner, StatusBadge, timeAgo } from "../../lib/ui";
import { formatDateTime } from "../../lib/format";
import { LogViewer } from "../logs/LogViewer";
import { ReplicaRow } from "./DeploymentRow";

type SheetTabId = "logs" | "build" | "details";

function DeploymentSheet(props: {
  deployment: Deployment | null;
  tab: SheetTabId;
  onTabChange: (tab: SheetTabId) => void;
  onClose: () => void;
}) {
  return (
    <Dialog
      open={props.deployment !== null}
      onOpenChange={(open) => {
        if (!open) props.onClose();
      }}
    >
      <Dialog.Portal>
        <Dialog.Overlay class="fixed inset-0 bg-black/20 z-40 backdrop-blur-[1px]" />
        <Dialog.Content class="fixed top-0 right-0 bottom-0 w-full max-w-5xl bg-white border-l border-gray-200 shadow-2xl z-50 flex flex-col outline-none">
          <Show when={props.deployment}>
            {(selected) => {
              const deployment = selected();
              const shortId = deployment.meta.id.split("-").at(-1) ?? deployment.meta.id;
              const artifact = deployment.spec.service.artifact;
              const sourceRevision =
                artifact.type === "build" && artifact.source.type === "git"
                  ? artifact.source.revision
                  : null;
              const hasBuild = artifact.type === "build";
              return (
                <>
                  <div class="px-4 sm:px-5 py-3 sm:py-4 border-b border-gray-200 shrink-0">
                    <div class="flex items-start justify-between gap-3 mb-3">
                      <div class="min-w-0 flex-1">
                        <div class="text-xl font-semibold text-gray-900 leading-snug tracking-tight">
                          {shortId}
                        </div>
                        <div class="flex items-center gap-2 flex-wrap text-xs mt-1.5">
                          <StatusBadge status={deployment.status.phase} />
                          <Show when={sourceRevision}>
                            {(revision) => (
                              <span
                                class="inline-flex items-center gap-1 text-gray-500 font-mono"
                                title={revision()}
                              >
                                <GitCommitHorizontal class="size-3 text-gray-400" />
                                {revision().slice(0, 12)}
                              </span>
                            )}
                          </Show>
                          <span class="font-mono text-gray-400">{deployment.meta.id}</span>
                          <span class="text-gray-300">·</span>
                          <span
                            class="text-gray-400 tabular-nums"
                            title={new Date(deployment.status.createdAt).toLocaleString()}
                          >
                            {formatDateTime(deployment.status.createdAt, true)}
                          </span>
                        </div>
                      </div>
                      <Dialog.CloseButton class="p-1 text-gray-400 hover:text-gray-700 hover:bg-gray-100 rounded-md transition-colors outline-none">
                        <X class="size-4" />
                      </Dialog.CloseButton>
                    </div>
                    <div class="flex gap-1 -mb-px">
                      <SheetTab
                        label="Details"
                        active={props.tab === "details"}
                        onClick={() => props.onTabChange("details")}
                      />
                      <Show when={hasBuild}>
                        <SheetTab
                          label="Build"
                          active={props.tab === "build"}
                          onClick={() => props.onTabChange("build")}
                        />
                      </Show>
                      <SheetTab
                        label="Logs"
                        active={props.tab === "logs"}
                        onClick={() => props.onTabChange("logs")}
                      />
                    </div>
                  </div>
                  <div
                    class={clsx("flex-1 min-h-0", {
                      "overflow-y-auto": props.tab === "details",
                      "overflow-hidden": props.tab !== "details"
                    })}
                  >
                    <Switch>
                      <Match when={props.tab === "logs"}>
                        <LogViewer
                          serviceId={deployment.spec.serviceId}
                          deploymentId={deployment.meta.id}
                          buildId={deployment.spec.buildId}
                          isSystem={false}
                          phase="deploy"
                          embedded
                          fillHeight
                        />
                      </Match>
                      <Match when={props.tab === "build" && hasBuild}>
                        <LogViewer
                          serviceId={deployment.spec.serviceId}
                          deploymentId={deployment.meta.id}
                          buildId={deployment.spec.buildId}
                          isSystem={false}
                          phase="build"
                          embedded
                          fillHeight
                        />
                      </Match>
                      <Match when={props.tab === "details"}>
                        <DeploymentDetails deployment={deployment} />
                      </Match>
                    </Switch>
                  </div>
                </>
              );
            }}
          </Show>
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog>
  );
}

function SheetTab(props: { label: string; active: boolean; onClick: () => void }) {
  return (
    <button
      type="button"
      onClick={props.onClick}
      class={clsx("px-3 pb-1 text-sm font-medium border-b-2 transition-colors outline-none", {
        "border-indigo-500 text-indigo-600": props.active,
        "border-transparent text-gray-400 hover:text-gray-600": !props.active
      })}
    >
      {props.label}
    </button>
  );
}

function DeploymentDetails(props: { deployment: Deployment }) {
  const deployment = () => props.deployment;
  const replicas = useQuery(() => deploymentReplicasQuery(deployment()));
  const artifact = () => deployment().spec.service.artifact;
  const environment = () => Object.entries(deployment().spec.service.environment ?? {});
  const secretKeys = () => Object.keys(deployment().spec.service.secrets?.items ?? {}).sort();
  const buildEnvironment = () => {
    const value = artifact();
    return value.type === "build" ? Object.entries(value.environment ?? {}) : [];
  };
  const buildSecretKeys = () => {
    const value = artifact();
    return value.type === "build" ? Object.keys(value.secrets ?? {}).sort() : [];
  };

  return (
    <div class="p-4 sm:p-5 space-y-4 sm:space-y-5">
      <div>
        <h4 class="text-xs font-medium text-gray-400 mb-2">Deployment</h4>
        <div class="bg-white rounded-lg border border-gray-200 divide-y divide-gray-100">
          <DetailRow label="ID" value={deployment().meta.id} />
          <DetailRow
            label="Service generation"
            value={String(deployment().spec.serviceGeneration)}
          />
          <DetailRow
            label="Restart generation"
            value={String(deployment().spec.restartGeneration)}
          />
          <DetailRow label="Goal" value={deployment().spec.goal} />
          <DetailRow label="Created" value={formatTimestamp(deployment().status.createdAt)} />
          <Show when={deployment().status.readyAt}>
            {(readyAt) => <DetailRow label="Ready" value={formatTimestamp(readyAt())} />}
          </Show>
          <Show when={deployment().status.drainingAt}>
            {(drainingAt) => <DetailRow label="Draining" value={formatTimestamp(drainingAt())} />}
          </Show>
          <CopyRow label="Config version" value={deployment().spec.service.version} />
          <Show when={deployment().status.imageDigest}>
            {(imageDigest) => <CopyRow label="Image" value={imageDigest()} />}
          </Show>
        </div>
      </div>

      <Show when={replicas.isError}>
        <ErrorBanner message="Failed to load replicas" onRetry={() => replicas.refetch()} />
      </Show>
      <Show when={(replicas.data?.length ?? 0) > 0}>
        <div>
          <h4 class="text-xs font-medium text-gray-400 mb-2">Replicas</h4>
          <div class="bg-white rounded-lg border border-gray-200 divide-y divide-gray-100">
            <For each={replicas.data}>
              {(replica) => (
                <div class="px-4 py-2.5">
                  <ReplicaRow deployment={deployment()} replica={replica} />
                  <Show when={replicaFailure(replica)}>
                    {(error) => (
                      <p class="mt-1 pl-4 text-[11px] text-red-500 break-words">{error()}</p>
                    )}
                  </Show>
                  <Show when={replica.status.restartAttempts > 0}>
                    <p class="mt-1 pl-4 text-[11px] text-amber-600">
                      {replica.status.restartAttempts} restart attempt
                      {replica.status.restartAttempts === 1 ? "" : "s"}
                    </p>
                  </Show>
                </div>
              )}
            </For>
          </div>
        </div>
      </Show>

      <ConfigValues title="Build environment variables" entries={buildEnvironment()} />
      <SecretKeys title="Build secrets" keys={buildSecretKeys()} />
      <ConfigValues title="Environment variables" entries={environment()} />
      <SecretKeys
        title={`Secrets (${deployment().spec.service.secrets?.mountPath ?? "not mounted"})`}
        keys={secretKeys()}
      />
    </div>
  );
}

function ConfigValues(props: { title: string; entries: [string, string][] }) {
  return (
    <Show when={props.entries.length > 0}>
      <div>
        <h4 class="text-xs font-medium text-gray-400 mb-2">{props.title}</h4>
        <div class="bg-white rounded-lg border border-gray-200 divide-y divide-gray-100">
          <For each={props.entries}>
            {([key, value]) => <DetailRow label={key} value={value} />}
          </For>
        </div>
      </div>
    </Show>
  );
}

function SecretKeys(props: { title: string; keys: string[] }) {
  return (
    <Show when={props.keys.length > 0}>
      <div>
        <h4 class="text-xs font-medium text-gray-400 mb-2">{props.title}</h4>
        <div class="bg-white rounded-lg border border-gray-200 divide-y divide-gray-100">
          <For each={props.keys}>{(key) => <DetailRow label={key} value="••••••••" />}</For>
        </div>
      </div>
    </Show>
  );
}

function formatTimestamp(ms: number) {
  return `${formatDateTime(ms, true)} · ${timeAgo(ms)}`;
}

function CopyRow(props: { label: string; value: string }) {
  const [copied, setCopied] = createSignal(false);
  const truncated = () => (props.value.length > 20 ? `${props.value.slice(0, 20)}…` : props.value);
  const copy = async () => {
    await navigator.clipboard.writeText(props.value);
    setCopied(true);
    setTimeout(() => setCopied(false), 1_500);
  };

  return (
    <div class="px-4 py-2.5 flex items-center justify-between gap-6">
      <span class="text-xs font-medium text-gray-700 shrink-0">{props.label}</span>
      <div class="flex min-w-0 items-center gap-1.5">
        <button
          type="button"
          onClick={copy}
          title="Copy full value"
          aria-label={`Copy ${props.label}`}
          class="rounded p-0.5 text-gray-300 outline-none transition-colors hover:bg-gray-100 hover:text-gray-600"
        >
          <Show when={copied()} fallback={<Copy class="size-3" />}>
            <Check class="size-3 text-emerald-500" />
          </Show>
        </button>
        <span class="text-xs tabular-nums text-gray-600 truncate" title={props.value}>
          {truncated()}
        </span>
      </div>
    </div>
  );
}

function DetailRow(props: { label: string; value: string }) {
  return (
    <div class="px-4 py-2.5 flex items-baseline justify-between gap-6">
      <span class="text-xs font-medium text-gray-700 shrink-0">{props.label}</span>
      <span class="text-xs tabular-nums text-gray-600 text-right truncate" title={props.value}>
        {props.value}
      </span>
    </div>
  );
}

export { DeploymentSheet };
export type { SheetTabId };
