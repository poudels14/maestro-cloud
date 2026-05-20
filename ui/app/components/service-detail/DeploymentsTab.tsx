import { createResource, createSignal, For, onCleanup, Show, Suspense } from "solid-js";
import { Clock, ExternalLink, GitCommitHorizontal, Rocket } from "lucide-solid";
import clsx from "clsx";
import {
  cancelDeployment,
  getClusterInfo,
  getDeployments,
  redeployService,
  restartService,
  stopDeployment
} from "../../lib/api";
import { DeploymentMenu, ErrorBanner, StatusBadge, StatusDot, timeAgo } from "../../lib/ui";
import { DeploymentSheet, type SheetTabId } from "./DeploymentSheet";

const INITIAL_VISIBLE = 10;
const LOAD_MORE_STEP = 10;

function DeploymentsTab(props: { serviceId: string; hasBuild: boolean; deployFrozen: boolean }) {
  const [deployments, { refetch }] = createResource(() => props.serviceId, getDeployments);
  const [clusterInfo] = createResource(() => (import.meta.env.SSR ? null : true), getClusterInfo);
  const [freezeConfirmAction, setFreezeConfirmAction] = createSignal<"redeploy" | "restart" | null>(
    null
  );
  const [selectedId, setSelectedId] = createSignal<string | null>(null);
  const [sheetTab, setSheetTab] = createSignal<SheetTabId>("logs");
  const [visibleCount, setVisibleCount] = createSignal(INITIAL_VISIBLE);
  const selectedDeployment = () => deployments()?.find((d) => d.id === selectedId()) ?? null;

  const visibleDeployments = () => deployments()?.slice(0, visibleCount()) ?? [];
  const hasMore = () => (deployments()?.length ?? 0) > visibleCount();

  let sentinelRef: HTMLDivElement | undefined;
  const observerCleanup = () => {
    if (!sentinelRef || typeof IntersectionObserver === "undefined") return;
    const observer = new IntersectionObserver(
      (entries) => {
        for (const entry of entries) {
          if (entry.isIntersecting && hasMore()) {
            setVisibleCount((c) => c + LOAD_MORE_STEP);
          }
        }
      },
      { rootMargin: "200px" }
    );
    observer.observe(sentinelRef);
    onCleanup(() => observer.disconnect());
  };

  return (
    <>
      <Show when={deployments.error}>
        <div class="mb-3">
          <ErrorBanner message="Failed to load deployments" onRetry={refetch} />
        </div>
      </Show>
      <Show when={props.deployFrozen}>
        <div class="mb-3 px-4 py-2.5 bg-amber-50 border border-amber-200 rounded-lg flex items-center justify-between">
          <span class="text-xs text-amber-700 font-medium">
            Deploy is frozen — auto-deploys from git watch are paused
          </span>
        </div>
      </Show>
      <Show when={freezeConfirmAction()}>
        {(action) => (
          <div class="fixed inset-0 bg-black/30 z-50 flex items-center justify-center">
            <div class="bg-white rounded-xl shadow-xl p-6 w-full max-w-sm">
              <h3 class="text-base font-semibold text-gray-900 mb-2">Deploy is frozen</h3>
              <p class="text-sm text-gray-500 mb-5">
                Deploys are frozen for this service. Are you sure you want to force a{" "}
                {action() === "restart" ? "restart" : "redeploy"}?
              </p>
              <div class="flex justify-end gap-2">
                <button
                  type="button"
                  onClick={() => setFreezeConfirmAction(null)}
                  class="px-3 py-1.5 text-sm text-gray-600 hover:bg-gray-100 rounded-lg transition-colors outline-none"
                >
                  Cancel
                </button>
                <button
                  type="button"
                  onClick={async () => {
                    const next = action();
                    setFreezeConfirmAction(null);
                    if (next === "restart") {
                      await restartService(props.serviceId, true);
                    } else {
                      await redeployService(props.serviceId, true);
                    }
                    refetch();
                  }}
                  class="px-3 py-1.5 text-sm text-white bg-amber-600 hover:bg-amber-700 rounded-lg transition-colors outline-none"
                >
                  Force {action() === "restart" ? "restart" : "deploy"}
                </button>
              </div>
            </div>
          </div>
        )}
      </Show>
      <Suspense
        fallback={<div class="text-xs text-gray-400 py-8 text-center">Loading deployments…</div>}
      >
        <Show
          when={deployments()?.length}
          fallback={
            <div class="text-center py-16 bg-white rounded-xl border border-dashed border-gray-200">
              <Rocket class="size-9 text-gray-300 mx-auto mb-3" />
              <p class="text-sm font-medium text-gray-500">No deployments yet</p>
              <p class="text-xs text-gray-400 mt-1">
                Deployments will appear here once this service ships.
              </p>
            </div>
          }
        >
          <div class="space-y-3">
            <For each={visibleDeployments()}>
              {(d, index) => {
                const shortId = d.id.split("-").slice(-1)[0] ?? d.id;
                const isLatest = () => index() === 0;
                const isLive = () =>
                  ["READY", "RUNNING", "DEPLOYING", "PENDING_READY", "BUILDING"].includes(d.status);
                const changedSecrets = () =>
                  Object.entries(d.config.deploy.secrets?.keys ?? {})
                    .filter(([, meta]) => meta.changed)
                    .map(([key]) => key);
                const showReplicas = () =>
                  d.replicas &&
                  d.replicas.length > 0 &&
                  !["TERMINATED", "REMOVED", "CANCELED", "DRAINING"].includes(d.status);
                const isSelected = () => selectedId() === d.id;
                const openSheet = (tab: SheetTabId) => {
                  setSelectedId(d.id);
                  setSheetTab(tab);
                };
                return (
                  <div
                    role="button"
                    tabIndex={0}
                    onClick={() => openSheet("logs")}
                    onKeyDown={(e) => {
                      if (e.key === "Enter" || e.key === " ") {
                        e.preventDefault();
                        openSheet("logs");
                      }
                    }}
                    class={clsx(
                      "rounded-xl border overflow-hidden transition-all cursor-pointer outline-none",
                      {
                        "bg-emerald-50 border-emerald-300": isLatest() && isLive() && !isSelected(),
                        "bg-white border-indigo-300 shadow-md ring-2 ring-indigo-100": isSelected(),
                        "bg-white border-gray-200 hover:shadow-sm hover:border-gray-300":
                          !(isLatest() && isLive()) && !isSelected()
                      }
                    )}
                  >
                    <div class="px-5 py-4">
                      <div class="flex items-start justify-between gap-3 mb-2">
                        <div class="min-w-0 flex-1">
                          <div class="text-lg font-semibold text-gray-900 truncate leading-snug tracking-tight">
                            {d.gitCommit ? d.gitCommit.message : shortId}
                          </div>
                        </div>
                        <div class="flex items-center gap-1.5 shrink-0">
                          <span
                            class="flex items-center gap-1 text-xs text-gray-400"
                            title={new Date(d.createdAt).toLocaleString()}
                          >
                            <Clock class="size-3" />
                            {timeAgo(d.createdAt)}
                          </span>
                          <div onClick={(e) => e.stopPropagation()}>
                            <DeploymentMenu
                              status={d.status}
                              onCancel={async () => {
                                await cancelDeployment(props.serviceId, d.id);
                                refetch();
                              }}
                              onStop={async () => {
                                await stopDeployment(props.serviceId, d.id);
                                refetch();
                              }}
                              onRedeploy={() => {
                                if (props.deployFrozen) {
                                  setFreezeConfirmAction("redeploy");
                                } else {
                                  redeployService(props.serviceId).then(() => refetch());
                                }
                              }}
                              onRestart={() => {
                                if (props.deployFrozen) {
                                  setFreezeConfirmAction("restart");
                                } else {
                                  restartService(props.serviceId).then(() => refetch());
                                }
                              }}
                            />
                          </div>
                        </div>
                      </div>
                      <div class="flex items-center gap-2 flex-wrap text-xs mb-3">
                        <StatusBadge status={d.status} />
                        <Show when={d.gitCommit}>
                          <span class="inline-flex items-center gap-1 text-gray-500 font-mono bg-gray-50 border border-gray-200 rounded px-1.5 py-0.5">
                            <GitCommitHorizontal class="size-3 text-gray-400" />
                            {d.gitCommit!.reference.slice(0, 7)}
                          </span>
                        </Show>
                        <span class="font-mono text-gray-400" title={d.config.version}>
                          {shortId}
                        </span>
                      </div>
                      <Show when={showReplicas()}>
                        <div class="bg-white rounded-md border border-gray-200 divide-y divide-gray-100 mb-1">
                          <For each={d.replicas}>
                            {(replica) => {
                              const shortDepId = d.id.slice(0, 6);
                              const hostname =
                                replica.replicaIndex === 0
                                  ? `${d.config.id}-${shortDepId}`
                                  : `${d.config.id}-${shortDepId}-${replica.replicaIndex}`;
                              const fqdn = () => {
                                const info = clusterInfo();
                                return info ? `${hostname}.${info.canonicalDomain}` : null;
                              };
                              const href = () => {
                                const host = fqdn();
                                if (!host) return null;
                                const port = d.config.ingress?.port;
                                return port ? `http://${host}:${port}` : `http://${host}`;
                              };
                              return (
                                <div class="flex items-center gap-2 text-xs px-2.5 py-1.5">
                                  <StatusDot status={replica.status} />
                                  <Show
                                    when={href()}
                                    fallback={
                                      <span class="font-mono text-gray-700 truncate">
                                        {fqdn() ?? hostname}
                                      </span>
                                    }
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
                                  <span class="text-gray-400 ml-auto">
                                    {replica.status.toLowerCase()}
                                  </span>
                                </div>
                              );
                            }}
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
              }}
            </For>
            <Show when={hasMore()}>
              <div
                ref={(el) => {
                  sentinelRef = el;
                  observerCleanup();
                }}
                class="h-4"
              />
              <div class="text-center text-xs text-gray-400 py-2">
                Showing {visibleCount()} of {deployments()?.length ?? 0} deployments…
              </div>
            </Show>
          </div>
        </Show>
      </Suspense>
      <DeploymentSheet
        deployment={selectedDeployment()}
        serviceId={props.serviceId}
        hasBuild={props.hasBuild}
        tab={sheetTab()}
        onTabChange={setSheetTab}
        onClose={() => setSelectedId(null)}
        clusterInfo={clusterInfo() ?? null}
      />
    </>
  );
}

export { DeploymentsTab };
