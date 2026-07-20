import { createSignal, For, Show, Switch, Match } from "solid-js";
import { Check, Copy, ExternalLink, Eye, EyeOff, GitCommitHorizontal, X } from "lucide-solid";
import { Dialog } from "@kobalte/core/dialog";
import clsx from "clsx";
import type { Deployment } from "../../lib/types";
import { type ClusterInfo } from "../../lib/api";
import { StatusBadge, timeAgo } from "../../lib/ui";
import { formatDateTime } from "../../lib/format";
import { LogViewer } from "../logs/LogViewer";
import { ReplicaRow } from "./DeploymentRow";
import { replicaHostname } from "../../lib/deploymentEndpoints";

type SheetTabId = "logs" | "build" | "details";

function DeploymentSheet(props: {
  deployment: Deployment | null;
  serviceId: string;
  hasBuild: boolean;
  tab: SheetTabId;
  onTabChange: (tab: SheetTabId) => void;
  onClose: () => void;
  clusterInfo: ClusterInfo | null;
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
            {(deployment) => {
              const d = deployment();
              const shortId = d.id.split("-").slice(-1)[0] ?? d.id;
              return (
                <>
                  <div class="px-4 sm:px-5 py-3 sm:py-4 border-b border-gray-200 shrink-0">
                    <div class="flex items-start justify-between gap-3 mb-3">
                      <div class="min-w-0 flex-1">
                        <div class="text-xl font-semibold text-gray-900 leading-snug tracking-tight">
                          {d.gitCommit ? d.gitCommit.message : shortId}
                        </div>
                        <div class="flex items-center gap-2 flex-wrap text-xs mt-1.5">
                          <StatusBadge status={d.status} />
                          <Show when={d.gitCommit}>
                            <span class="inline-flex items-center gap-1 text-gray-500 font-mono">
                              <GitCommitHorizontal class="size-3 text-gray-400" />
                              {d.gitCommit!.reference.slice(0, 7)}
                            </span>
                          </Show>
                          <span class="font-mono text-gray-400">{shortId}</span>
                          <span class="text-gray-300">·</span>
                          <span
                            class="text-gray-400 tabular-nums"
                            title={new Date(d.createdAt).toLocaleString()}
                          >
                            {formatDateTime(d.createdAt, true)}
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
                      <Show when={props.hasBuild}>
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
                          serviceId={props.serviceId}
                          deploymentId={d.id}
                          isSystem={false}
                          hasBuild={props.hasBuild}
                          phase="deploy"
                          embedded={true}
                          fillHeight
                        />
                      </Match>
                      <Match when={props.tab === "build"}>
                        <LogViewer
                          serviceId={props.serviceId}
                          deploymentId={d.id}
                          isSystem={false}
                          hasBuild={props.hasBuild}
                          phase="build"
                          embedded={true}
                          fillHeight
                        />
                      </Match>
                      <Match when={props.tab === "details"}>
                        <DeploymentDetails deployment={d} clusterInfo={props.clusterInfo} />
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

function DeploymentDetails(props: { deployment: Deployment; clusterInfo: ClusterInfo | null }) {
  const [envRevealed, setEnvRevealed] = createSignal(false);
  const d = props.deployment;
  const envEntries = () =>
    Object.entries(d.config.deploy.env?.items ?? {}).sort(([a], [b]) => a.localeCompare(b));
  const envSource = () => d.config.deploy.env?.source ?? null;
  const secretEntries = () =>
    Object.entries(d.config.deploy.secrets?.keys ?? {}).sort(([a], [b]) => a.localeCompare(b));
  const secretSource = () => d.config.deploy.secrets?.source ?? null;
  const buildEnvEntries = () =>
    Object.entries(d.config.build?.env?.items ?? {}).sort(([a], [b]) => a.localeCompare(b));
  const buildEnvSource = () => d.config.build?.env?.source ?? null;
  const buildSecretKeys = () => Object.keys(d.config.build?.secrets?.items ?? {}).sort();
  const buildSecretSource = () => d.config.build?.secrets?.source ?? null;

  const deploymentDomain = () => {
    const info = props.clusterInfo;
    if (!info) return null;
    const replica = d.replicas?.find((candidate) => candidate.replicaIndex === 0);
    const host = `${replicaHostname(
      d,
      0,
      replica?.endpoint?.containerHostname
    )}.${info.aliasDomain}`;
    const port = d.config.ingress?.port ?? null;
    return port ? `${host}:${port}` : host;
  };

  const hasAnyDetails = () =>
    envEntries().length > 0 ||
    envSource() ||
    secretEntries().length > 0 ||
    secretSource() ||
    buildEnvEntries().length > 0 ||
    buildEnvSource() ||
    buildSecretKeys().length > 0 ||
    buildSecretSource();

  return (
    <div class="p-4 sm:p-5 space-y-4 sm:space-y-5">
      <div>
        <h4 class="text-xs font-medium text-gray-400 mb-2">Deployment</h4>
        <div class="bg-white rounded-lg border border-gray-200 divide-y divide-gray-100">
          <DetailRow label="ID" value={d.id} />
          <Show when={d.gitCommit}>
            {(commit) => <DetailRow label="Git commit" value={commit().reference} />}
          </Show>
          <DetailRow label="Created" value={formatTimestamp(d.createdAt)} />
          <Show when={d.deployedAt}>
            {(deployedAt) => <DetailRow label="Deployed" value={formatTimestamp(deployedAt())} />}
          </Show>
          <Show when={d.drainedAt}>
            {(drainedAt) => <DetailRow label="Drained" value={formatTimestamp(drainedAt())} />}
          </Show>
          <CopyRow label="Config version" value={d.config.version} />
          <Show when={d.build?.dockerImageId}>
            {(imageId) => <DetailRow label="Image" value={imageId()} />}
          </Show>
          <Show when={deploymentDomain()}>
            {(domain) => (
              <div class="px-4 py-2.5 flex items-baseline justify-between gap-6">
                <span class="text-xs font-medium text-gray-700 shrink-0">Endpoint</span>
                <a
                  href={`http://${domain()}`}
                  target="_blank"
                  rel="noopener noreferrer"
                  class="group inline-flex min-w-0 items-center gap-1 text-xs text-gray-600 hover:text-indigo-600 underline decoration-gray-300 underline-offset-2 hover:decoration-indigo-300"
                >
                  <span class="truncate">{domain()}</span>
                  <ExternalLink class="size-3 shrink-0 text-gray-400 group-hover:text-indigo-500" />
                </a>
              </div>
            )}
          </Show>
        </div>
      </div>
      <Show when={(d.replicas?.length ?? 0) > 0}>
        <div>
          <h4 class="text-xs font-medium text-gray-400 mb-2">Replicas</h4>
          <div class="bg-white rounded-lg border border-gray-200 divide-y divide-gray-100">
            <For each={d.replicas}>
              {(replica) => (
                <div class="px-4 py-2.5">
                  <ReplicaRow
                    deployment={d}
                    replicaIndex={replica.replicaIndex}
                    replicaStatus={replica.status}
                    nodeId={replica.nodeId}
                    containerHostname={replica.endpoint?.containerHostname}
                    clusterInfo={props.clusterInfo}
                  />
                  <Show when={replica.error}>
                    {(error) => (
                      <p class="mt-1 pl-4 text-[11px] text-red-500 break-words">{error()}</p>
                    )}
                  </Show>
                </div>
              )}
            </For>
          </div>
        </div>
      </Show>
      <Show when={buildEnvEntries().length > 0 || buildEnvSource()}>
        <SecretsList
          title="Build environment variables"
          source={buildEnvSource()}
          entries={buildEnvEntries()}
          revealed={envRevealed()}
          onToggleReveal={() => setEnvRevealed(!envRevealed())}
        />
      </Show>
      <Show when={buildSecretKeys().length > 0 || buildSecretSource()}>
        <SecretsList
          title="Build Secrets"
          source={buildSecretSource()}
          entries={buildSecretKeys().map((key) => [key, "••••••••"] as [string, string])}
          revealed={false}
        />
      </Show>
      <Show when={envEntries().length > 0 || envSource()}>
        <SecretsList
          title="Deploy environment variables"
          source={envSource()}
          entries={envEntries()}
          revealed={envRevealed()}
          onToggleReveal={() => setEnvRevealed(!envRevealed())}
        />
      </Show>
      <Show when={secretEntries().length > 0 || secretSource()}>
        <div>
          <div class="text-xs font-medium text-gray-400 mb-2">
            Deploy Secrets
            <span class="text-gray-300 ml-1">
              (mounted at {d.config.deploy.secrets?.mountPath})
            </span>
          </div>
          <div class="bg-white rounded-lg border border-gray-200 divide-y divide-gray-100">
            <Show when={secretSource()}>
              {(source) => <DetailRow label="Source" value={source()} class="bg-gray-50" />}
            </Show>
            <For each={secretEntries()}>
              {([key, meta]) => (
                <div class="px-4 py-2.5 flex items-baseline justify-between gap-6">
                  <span class="text-xs text-gray-700">{key}</span>
                  <div class="flex items-center gap-2">
                    <Show when={meta.changed}>
                      <span class="text-amber-600 text-[10px] font-medium">changed</span>
                    </Show>
                    <span class="text-xs text-gray-400">••••••••</span>
                  </div>
                </div>
              )}
            </For>
          </div>
        </div>
      </Show>
      <Show when={!hasAnyDetails()}>
        <div class="text-center py-8 text-sm text-gray-400">
          No environment or secret configuration.
        </div>
      </Show>
    </div>
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

function DetailRow(props: { label: string; value: string; class?: string }) {
  return (
    <div class={clsx("px-4 py-2.5 flex items-baseline justify-between gap-6", props.class)}>
      <span class="text-xs font-medium text-gray-700 shrink-0">{props.label}</span>
      <span class="text-xs tabular-nums text-gray-600 text-right truncate" title={props.value}>
        {props.value}
      </span>
    </div>
  );
}

function SecretsList(props: {
  title: string;
  source: string | null;
  entries: [string, string][];
  revealed: boolean;
  onToggleReveal?: () => void;
}) {
  return (
    <div>
      <div class="flex items-center justify-between mb-2">
        <div class="text-xs font-medium text-gray-400">
          {props.title}
          <Show when={props.source}>
            <span class="ml-1.5 text-gray-300">{props.source}</span>
          </Show>
        </div>
        <Show when={props.entries.length > 0 && props.onToggleReveal}>
          <button
            type="button"
            onClick={props.onToggleReveal}
            class="text-gray-400 hover:text-gray-600 transition-colors outline-none"
          >
            <Show when={props.revealed} fallback={<Eye class="size-3.5" />}>
              <EyeOff class="size-3.5" />
            </Show>
          </button>
        </Show>
      </div>
      <Show when={props.entries.length > 0}>
        <div class="bg-white rounded-lg border border-gray-200 divide-y divide-gray-100">
          <For each={props.entries}>
            {([key, value]) => (
              <div class="px-4 py-2.5 flex items-baseline justify-between gap-6">
                <span class="text-xs text-gray-700">{key}</span>
                <span
                  class={clsx("text-xs truncate text-right", {
                    "font-mono text-gray-600": props.revealed,
                    "text-gray-400": !props.revealed
                  })}
                >
                  {props.revealed ? value : "••••••••"}
                </span>
              </div>
            )}
          </For>
        </div>
      </Show>
    </div>
  );
}

export { DeploymentSheet, type SheetTabId };
