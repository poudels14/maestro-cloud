import { createSignal, For, Show, Switch, Match } from "solid-js";
import { Clock, Eye, EyeOff, GitCommitHorizontal, X } from "lucide-solid";
import { Dialog } from "@kobalte/core/dialog";
import clsx from "clsx";
import type { Deployment } from "../../lib/types";
import { type ClusterInfo } from "../../lib/api";
import { StatusBadge, timeAgo } from "../../lib/ui";
import { LogViewer } from "../logs/LogViewer";

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
                        <div class="text-base font-semibold text-gray-900 leading-snug tracking-tight">
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
                            class="flex items-center gap-1 text-gray-400"
                            title={new Date(d.createdAt).toLocaleString()}
                          >
                            <Clock class="size-3" />
                            {timeAgo(d.createdAt)}
                          </span>
                        </div>
                      </div>
                      <Dialog.CloseButton class="p-1 text-gray-400 hover:text-gray-700 hover:bg-gray-100 rounded-md transition-colors outline-none">
                        <X class="size-4" />
                      </Dialog.CloseButton>
                    </div>
                    <div class="flex gap-1 -mb-px">
                      <SheetTab
                        label="Logs"
                        active={props.tab === "logs"}
                        onClick={() => props.onTabChange("logs")}
                      />
                      <Show when={props.hasBuild}>
                        <SheetTab
                          label="Build"
                          active={props.tab === "build"}
                          onClick={() => props.onTabChange("build")}
                        />
                      </Show>
                      <SheetTab
                        label="Details"
                        active={props.tab === "details"}
                        onClick={() => props.onTabChange("details")}
                      />
                    </div>
                  </div>
                  <div class="flex-1 overflow-y-auto">
                    <Switch>
                      <Match when={props.tab === "logs"}>
                        <LogViewer
                          serviceId={props.serviceId}
                          deploymentId={d.id}
                          isSystem={false}
                          hasBuild={props.hasBuild}
                          phase="deploy"
                          embedded={true}
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
      class={clsx("px-3 pb-2 text-sm font-medium border-b-2 transition-colors outline-none", {
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
    const idPrefix = d.id.slice(0, 6);
    const host = `${d.config.id}-${idPrefix}.${info.canonicalDomain}`;
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
        <h4 class="text-[10px] font-medium text-gray-400 uppercase tracking-wider mb-2">
          Deployment
        </h4>
        <div class="bg-white rounded-lg border border-gray-200 divide-y divide-gray-100">
          <DetailRow label="ID" value={d.id} mono />
          <DetailRow label="Config version" value={d.config.version} mono />
          <Show when={deploymentDomain()}>
            {(domain) => <DetailRow label="Endpoint" value={domain()} mono />}
          </Show>
        </div>
      </div>
      <Show when={buildEnvEntries().length > 0 || buildEnvSource()}>
        <SecretsList
          title="Build Environment"
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
          title="Deploy Environment"
          source={envSource()}
          entries={envEntries()}
          revealed={envRevealed()}
          onToggleReveal={() => setEnvRevealed(!envRevealed())}
        />
      </Show>
      <Show when={secretEntries().length > 0 || secretSource()}>
        <div>
          <div class="text-[10px] font-medium text-gray-400 uppercase tracking-wider mb-2">
            Deploy Secrets
            <span class="normal-case text-gray-300 ml-1">
              ({d.config.deploy.secrets?.mountPath})
            </span>
          </div>
          <Show when={secretSource()}>
            <div class="text-xs font-mono text-gray-400 mb-2">{secretSource()}</div>
          </Show>
          <div class="bg-white rounded-lg border border-gray-200 divide-y divide-gray-100">
            <For each={secretEntries()}>
              {([key, meta]) => (
                <div class="px-4 py-2.5 flex items-baseline justify-between gap-6">
                  <span class="text-xs text-gray-500 font-mono">{key}</span>
                  <div class="flex items-center gap-2">
                    <Show when={meta.changed}>
                      <span class="text-amber-500 text-[10px]">changed</span>
                    </Show>
                    <span class="text-sm font-mono text-gray-400">••••••••</span>
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

function DetailRow(props: { label: string; value: string; mono?: boolean }) {
  return (
    <div class="px-4 py-2.5 flex items-baseline justify-between gap-6">
      <span class="text-xs text-gray-500 shrink-0">{props.label}</span>
      <span class={clsx("text-sm text-gray-800 text-right truncate", { "font-mono": props.mono })}>
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
        <div class="text-[10px] font-medium text-gray-400 uppercase tracking-wider">
          {props.title}
          <Show when={props.source}>
            <span class="normal-case ml-1.5 text-gray-300 font-mono">{props.source}</span>
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
                <span class="text-xs text-gray-500 font-mono">{key}</span>
                <span class="text-sm font-mono text-gray-700 truncate text-right">
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
