import { createFileRoute, useNavigate } from "@tanstack/solid-router";
import {
  createEffect,
  createMemo,
  createResource,
  createSignal,
  For,
  Show,
  Switch,
  Match,
  Suspense,
  on,
  onCleanup
} from "solid-js";
import {
  createSolidTable,
  getCoreRowModel,
  getExpandedRowModel,
  flexRender,
  type ColumnDef
} from "@tanstack/solid-table";
import { Clock, GitCommitHorizontal, Home, Rocket, ChevronRight } from "lucide-solid";
import clsx from "clsx";
import type { LogEntry, Service } from "../lib/types";
import {
  getServices,
  getDeployments,
  getLogs,
  getSystemLogs,
  redeployService,
  cancelDeployment,
  stopDeployment,
  freezeService,
  getServiceMetrics
} from "../lib/api";
import { TimelineChart } from "../components/TimelineChart";
import { StatusBadge, StatusDot, DeploymentMenu, TabButton, timeAgo, ErrorBanner } from "../lib/ui";

const VALID_TABS = new Set(["overview", "deployments", "metrics", "logs"]);
type DetailTab = "overview" | "deployments" | "metrics" | "logs";

export const Route = createFileRoute("/services/$serviceId/$tab")({
  component: ServiceDetailPage
});

function useTab(): DetailTab {
  const params = Route.useParams();
  const raw = () => params().tab;
  return VALID_TABS.has(raw()) ? (raw() as DetailTab) : "overview";
}

function ServiceDetailPage() {
  const params = Route.useParams();
  const navigate = useNavigate();
  const [services, { refetch: refetchServices }] = createResource(
    () => (import.meta.env.SSR ? null : true),
    getServices
  );
  const tab = useTab();

  const selected = () => services()?.find((s) => s.id === params().serviceId);

  const navigateTab = (t: DetailTab) =>
    navigate({
      to: "/services/$serviceId/$tab",
      params: { serviceId: params().serviceId, tab: t }
    });

  const navigateService = (s: Service) =>
    navigate({ to: "/services/$serviceId/$tab", params: { serviceId: s.id, tab } });

  const serviceList = () => services() ?? [];
  const loading = () => services.loading || services() === undefined;

  return (
    <div class="h-screen flex bg-[#fafafa]">
      <ServiceSidebar
        services={serviceList()}
        selected={selected() ?? null}
        onSelect={navigateService}
        onBack={() => navigate({ to: "/" })}
      />
      <Show when={loading()}>
        <div class="flex-1 flex items-center justify-center">
          <span class="text-sm text-gray-400">Loading…</span>
        </div>
      </Show>
      <Show when={!loading() && !selected()}>
        <div class="flex-1 flex items-center justify-center">
          <div class="text-center">
            <p class="text-sm text-gray-500">Service not found.</p>
            <button
              type="button"
              onClick={() => navigate({ to: "/" })}
              class="mt-3 text-sm text-indigo-600 hover:text-indigo-700"
            >
              Back to services
            </button>
          </div>
        </div>
      </Show>
      <Show when={!loading() && selected()}>
        {(service) => (
          <ServiceDetailPanel
            service={service()}
            tab={tab}
            navigateTab={navigateTab}
            onServiceUpdate={refetchServices}
          />
        )}
      </Show>
    </div>
  );
}

function ServiceSidebar(props: {
  services: Service[];
  selected: Service | null;
  onSelect: (s: Service) => void;
  onBack: () => void;
}) {
  return (
    <div class="w-64 shrink-0 bg-white border-r border-gray-200 flex flex-col h-full">
      <button
        type="button"
        onClick={props.onBack}
        class="px-4 h-14 flex items-center gap-2.5 border-b border-gray-200 shrink-0 hover:bg-gray-50 transition-colors outline-none w-full"
      >
        <Home class="size-4 text-gray-400" />
        <span class="text-base font-semibold text-gray-900">Maestro</span>
      </button>
      <div class="flex-1 overflow-y-auto py-1">
        <For each={props.services.filter((s) => !s.system)}>
          {(service) => {
            const isSelected = () => service.id === props.selected?.id;
            return (
              <button
                type="button"
                onClick={() => props.onSelect(service)}
                class={clsx(
                  "w-full text-left px-4 py-2.5 flex items-center gap-2.5 text-sm transition-colors outline-none",
                  {
                    "bg-indigo-50 text-indigo-700 font-medium": isSelected(),
                    "text-gray-700 hover:bg-gray-50": !isSelected()
                  }
                )}
              >
                <StatusDot status={service.status ?? "IDLE"} />
                <span class="truncate">{service.name}</span>
              </button>
            );
          }}
        </For>
        <Show when={props.services.some((s) => s.system)}>
          <div class="px-4 pt-4 pb-1">
            <span class="text-[10px] font-semibold uppercase tracking-wider text-gray-400">
              System
            </span>
          </div>
          <For each={props.services.filter((s) => s.system === true)}>
            {(service) => {
              const isSelected = () => service.id === props.selected?.id;
              return (
                <button
                  type="button"
                  onClick={() => props.onSelect(service)}
                  class={clsx(
                    "w-full text-left px-4 py-2 flex items-center gap-2.5 text-xs transition-colors outline-none",
                    {
                      "bg-indigo-50 text-indigo-700 font-medium": isSelected(),
                      "text-gray-500 hover:bg-gray-50": !isSelected()
                    }
                  )}
                >
                  <StatusDot status="SYSTEM" />
                  <span class="truncate">{service.name}</span>
                </button>
              );
            }}
          </For>
        </Show>
      </div>
    </div>
  );
}

function ServiceDetailPanel(props: {
  service: Service;
  tab: DetailTab;
  navigateTab: (t: DetailTab) => void;
  onServiceUpdate: () => void;
}) {
  const s = props.service;

  createEffect(() => {
    if (s.system && props.tab === "deployments") {
      props.navigateTab("logs");
    }
  });

  return (
    <div class="flex-1 flex flex-col min-w-0 h-full">
      <div class="pt-5 pb-0 shrink-0 bg-white border-b border-gray-200">
        <div class="max-w-4xl mx-auto px-6 flex justify-center gap-4 -mb-px">
          <TabButton
            label="Overview"
            active={props.tab === "overview"}
            onClick={() => props.navigateTab("overview")}
          />
          <TabButton
            label="Metrics"
            active={props.tab === "metrics"}
            onClick={() => props.navigateTab("metrics")}
          />
          <Show when={!s.system}>
            <TabButton
              label="Deployments"
              active={props.tab === "deployments"}
              onClick={() => props.navigateTab("deployments")}
            />
          </Show>
          <TabButton
            label="Logs"
            active={props.tab === "logs"}
            onClick={() => props.navigateTab("logs")}
          />
        </div>
      </div>
      <div class="flex-1 overflow-y-auto py-5 bg-[#fafafa]">
        <div class="max-w-4xl mx-auto px-6">
          <Show when={props.tab === "overview"}>
            <OverviewTab service={s} onServiceUpdate={props.onServiceUpdate} />
          </Show>
          <Show when={props.tab === "deployments"}>
            <DeploymentsTab serviceId={s.id} hasBuild={!!s.build} deployFrozen={!!s.deployFrozen} />
          </Show>
          <Show when={props.tab === "metrics"}>
            <MetricsTab service={s} />
          </Show>
          <Show when={props.tab === "logs"}>
            <LogsTab service={s} />
          </Show>
        </div>
      </div>
    </div>
  );
}

function OverviewTab(props: { service: Service; onServiceUpdate: () => void }) {
  const s = props.service;

  const sourceItems =
    s.build != null
      ? [
          { label: "Git repository", value: s.build.repo },
          ...(s.build.branch ? [{ label: "Branch", value: s.build.branch }] : []),
          { label: "Dockerfile", value: s.build.dockerfile },
          ...(s.build.registry ? [{ label: "Registry", value: s.build.registry }] : []),
          ...(s.build.watch ? [{ label: "Watch", value: "enabled" }] : [])
        ]
      : [{ label: "Image", value: s.image ?? "(not set)" }];

  const buildEnvItems = Object.entries(s.build?.env?.items ?? {}).map(([key, value]) => ({
    label: key,
    value
  }));
  const buildSecretKeys = Object.keys(s.build?.secrets?.items ?? {}).sort();

  const deployItems = [
    { label: "Replicas", value: String(s.deploy.replicas ?? 1) },
    ...(s.deploy.command
      ? [{ label: "Deploy command", value: `${s.deploy.command.command} ${s.deploy.command.args.join(" ")}`.trim() }]
      : []),
    { label: "Healthcheck path", value: s.deploy.healthcheckPath }
  ];

  const ingressItems = s.ingress
    ? [
        { label: "Host", value: s.ingress.host },
        { label: "Port", value: String(s.ingress.port ?? 80) }
      ]
    : null;

  const envItems = Object.entries(s.deploy.env ?? {}).map(([key, value]) => ({
    label: key,
    value
  }));

  const secretKeys = Object.keys(s.deploy.secrets?.keys ?? {}).sort();

  return (
    <div class="space-y-6">
      <ConfigSection title="Source" items={sourceItems} />
      <Show when={buildEnvItems.length > 0}>
        <ConfigSection title="Build Environment" items={buildEnvItems} />
      </Show>
      <Show when={buildSecretKeys.length > 0}>
        <div>
          <h4 class="text-xs font-medium text-gray-400 uppercase tracking-wider mb-2">
            Build Secrets
          </h4>
          <div class="bg-white rounded-lg border border-gray-200 divide-y divide-gray-100">
            <For each={buildSecretKeys}>
              {(key) => (
                <div class="px-4 py-2.5 flex items-baseline justify-between gap-6">
                  <span class="text-xs text-gray-500 shrink-0">{key}</span>
                  <span class="text-sm font-mono text-gray-400">••••••••</span>
                </div>
              )}
            </For>
          </div>
        </div>
      </Show>
      <ConfigSection title="Deploy" items={deployItems} />
      <Show when={ingressItems}>
        {(items) => <ConfigSection title="Ingress" items={items()} />}
      </Show>
      <Show when={envItems.length > 0}>
        <ConfigSection title="Deploy Environment" items={envItems} />
      </Show>
      <Show when={secretKeys.length > 0}>
        <div>
          <h4 class="text-xs font-medium text-gray-400 uppercase tracking-wider mb-2">
            Deploy Secrets
            <span class="ml-1.5 text-gray-300 normal-case">
              (mounted at {s.deploy.secrets?.mountPath})
            </span>
          </h4>
          <div class="bg-white rounded-lg border border-gray-200 divide-y divide-gray-100">
            <For each={secretKeys}>
              {(key) => (
                <div class="px-4 py-2.5 flex items-baseline justify-between gap-6">
                  <span class="text-xs text-gray-500 shrink-0">{key}</span>
                  <span class="text-sm font-mono text-gray-400">••••••••</span>
                </div>
              )}
            </For>
          </div>
        </div>
      </Show>
      <Show when={!s.system}>
        <div>
          <h4 class="text-xs font-medium text-gray-400 uppercase tracking-wider mb-2">
            Deploy freeze
          </h4>
          <div class="bg-white rounded-lg border border-gray-200 px-4 py-3 flex items-center justify-between">
            <div>
              <p class="text-sm text-gray-700">
                {s.deployFrozen ? "Deploys are frozen" : "Deploys are active"}
              </p>
              <p class="text-xs text-gray-400 mt-0.5">
                {s.deployFrozen
                  ? "Auto-deploys from git watch are paused. Manual deploys require force."
                  : "Services will auto-deploy when new commits are detected."}
              </p>
            </div>
            <button
              type="button"
              onClick={async () => {
                await freezeService(s.id, !s.deployFrozen);
                props.onServiceUpdate();
              }}
              class={clsx(
                "px-3 py-1.5 text-xs font-medium rounded-lg transition-colors outline-none",
                {
                  "bg-amber-100 text-amber-700 hover:bg-amber-200": s.deployFrozen,
                  "bg-gray-100 text-gray-600 hover:bg-gray-200": !s.deployFrozen
                }
              )}
            >
              {s.deployFrozen ? "Unfreeze" : "Freeze"}
            </button>
          </div>
        </div>
      </Show>
    </div>
  );
}

function DeploymentsTab(props: { serviceId: string; hasBuild: boolean; deployFrozen: boolean }) {
  const [deployments, { refetch }] = createResource(() => props.serviceId, getDeployments);
  const [logsOpen, setLogsOpen] = createSignal<string | null>(null);
  const [showFreezeConfirm, setShowFreezeConfirm] = createSignal(false);

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
      <Show when={showFreezeConfirm()}>
        <div class="fixed inset-0 bg-black/30 z-50 flex items-center justify-center">
          <div class="bg-white rounded-xl shadow-xl p-6 w-full max-w-sm">
            <h3 class="text-base font-semibold text-gray-900 mb-2">Deploy is frozen</h3>
            <p class="text-sm text-gray-500 mb-5">
              Deploys are frozen for this service. Are you sure you want to force a redeploy?
            </p>
            <div class="flex justify-end gap-2">
              <button
                type="button"
                onClick={() => setShowFreezeConfirm(false)}
                class="px-3 py-1.5 text-sm text-gray-600 hover:bg-gray-100 rounded-lg transition-colors outline-none"
              >
                Cancel
              </button>
              <button
                type="button"
                onClick={async () => {
                  setShowFreezeConfirm(false);
                  await redeployService(props.serviceId, true);
                  refetch();
                }}
                class="px-3 py-1.5 text-sm text-white bg-amber-600 hover:bg-amber-700 rounded-lg transition-colors outline-none"
              >
                Force deploy
              </button>
            </div>
          </div>
        </div>
      </Show>
      <Suspense
        fallback={<div class="text-xs text-gray-400 py-8 text-center">Loading deployments…</div>}
      >
        <Show
          when={deployments()?.length}
          fallback={
            <div class="text-center py-12">
              <Rocket class="size-8 text-gray-300 mx-auto mb-3" />
              <p class="text-sm text-gray-400">No deployments yet.</p>
            </div>
          }
        >
          <div class="space-y-2">
            <For each={deployments()}>
              {(d) => {
                const shortId = d.id.split("-").slice(-1)[0] ?? d.id;
                const [expanded, setExpanded] = createSignal(false);
                const isLogsOpen = () => logsOpen() === d.id;
                const secretKeys = () =>
                  Object.entries(d.config.deploy.secrets?.keys ?? {}).sort(([a], [b]) =>
                    a.localeCompare(b)
                  );
                const changedSecrets = () =>
                  secretKeys()
                    .filter(([, meta]) => meta.prevHash != null && meta.hash !== meta.prevHash)
                    .map(([key]) => key);
                const envEntries = () =>
                  Object.entries(d.config.deploy.env ?? {}).sort(([a], [b]) => a.localeCompare(b));
                const buildEnvEntries = () =>
                  Object.entries(d.config.build?.env?.items ?? {}).sort(([a], [b]) => a.localeCompare(b));
                const buildSecretKeys = () =>
                  Object.keys(d.config.build?.secrets?.items ?? {}).sort();
                const hasDetails = () =>
                  envEntries().length > 0 || secretKeys().length > 0 ||
                  buildEnvEntries().length > 0 || buildSecretKeys().length > 0;
                return (
                  <div class="rounded-lg border border-gray-200 bg-white overflow-hidden">
                    <div class="p-4">
                      <div class="flex items-start justify-between gap-3 mb-1">
                        <div class="text-base font-semibold text-gray-900 truncate min-w-0">
                          {d.gitCommit ? d.gitCommit.message : shortId}
                        </div>
                        <div class="flex items-center gap-1.5 shrink-0">
                          <span class="flex items-center gap-1 text-xs text-gray-400">
                            <Clock class="size-3" />
                            {timeAgo(d.createdAt)}
                          </span>
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
                                setShowFreezeConfirm(true);
                              } else {
                                redeployService(props.serviceId).then(() => refetch());
                              }
                            }}
                          />
                        </div>
                      </div>
                      <div class="flex items-center gap-2 mb-2 text-xs">
                        <Show
                          when={[
                            "TERMINATED",
                            "REMOVED",
                            "CANCELED",
                            "CRASHED",
                            "DRAINING"
                          ].includes(d.status)}
                        >
                          <StatusBadge status={d.status} />
                        </Show>
                        <span class="font-mono text-gray-400">{shortId}</span>
                        <Show when={d.gitCommit}>
                          <span class="text-gray-300">·</span>
                          <GitCommitHorizontal class="size-3 text-gray-400" />
                          <span class="font-mono text-gray-400">
                            {d.gitCommit!.reference.slice(0, 7)}
                          </span>
                        </Show>
                      </div>
                      <Show
                        when={
                          d.replicas &&
                          d.replicas.length > 0 &&
                          !["TERMINATED", "REMOVED", "CANCELED", "DRAINING"].includes(d.status)
                        }
                      >
                        <div class="flex items-center gap-2 mb-2 flex-wrap">
                          <For each={d.replicas}>
                            {(replica) => (
                              <span class="inline-flex items-center gap-1.5 text-xs bg-gray-50 border border-gray-200 rounded px-2 py-0.5">
                                <StatusDot status={replica.status} />
                                <span class="font-mono text-gray-600">
                                  replica {replica.replicaIndex}
                                </span>
                                <span class="text-gray-400">{replica.status.toLowerCase()}</span>
                              </span>
                            )}
                          </For>
                        </div>
                      </Show>
                      <Show when={changedSecrets().length > 0}>
                        <div class="flex items-center gap-1.5 flex-wrap text-xs mb-2">
                          <span class="text-amber-500 font-medium">secrets changed:</span>
                          <For each={changedSecrets()}>
                            {(key) => (
                              <span class="inline-flex items-center bg-amber-50 border border-amber-200 text-amber-700 rounded px-1.5 py-0.5 font-mono">
                                {key}
                              </span>
                            )}
                          </For>
                        </div>
                      </Show>
                      <div class="flex items-center gap-2 text-xs text-gray-400">
                        <span class="font-mono">{d.config.version.slice(0, 12)}</span>
                        <span class="ml-auto" />
                        <Show when={hasDetails()}>
                          <button
                            type="button"
                            onClick={() => setExpanded(!expanded())}
                            class={clsx(
                              "inline-flex items-center gap-1 px-2 py-0.5 rounded text-[11px] font-medium transition-colors outline-none",
                              {
                                "bg-indigo-50 text-indigo-600": expanded(),
                                "bg-gray-100 text-gray-500 hover:bg-gray-200 hover:text-gray-600": !expanded()
                              }
                            )}
                          >
                            {expanded() ? "Hide Details" : "Details"}
                          </button>
                        </Show>
                        <button
                          type="button"
                          onClick={() => setLogsOpen(isLogsOpen() ? null : d.id)}
                          class={clsx(
                            "inline-flex items-center gap-1 px-2 py-0.5 rounded text-[11px] font-medium transition-colors outline-none",
                            {
                              "bg-indigo-50 text-indigo-600": isLogsOpen(),
                              "bg-gray-100 text-gray-500 hover:bg-gray-200 hover:text-gray-600": !isLogsOpen()
                            }
                          )}
                        >
                          {isLogsOpen() ? "Hide Logs" : "Logs"}
                        </button>
                      </div>
                      <Show when={expanded()}>
                        <div class="mt-3 pt-3 border-t border-gray-100 space-y-3">
                          <Show when={buildEnvEntries().length > 0}>
                            <div>
                              <div class="text-[10px] font-medium text-gray-400 uppercase tracking-wider mb-1">
                                Build Environment
                              </div>
                              <div class="space-y-0.5">
                                <For each={buildEnvEntries()}>
                                  {([key, value]) => (
                                    <div class="flex items-baseline gap-2 text-xs">
                                      <span class="text-gray-500 font-mono">{key}</span>
                                      <span class="text-gray-300">=</span>
                                      <span class="text-gray-700 font-mono truncate">{value}</span>
                                    </div>
                                  )}
                                </For>
                              </div>
                            </div>
                          </Show>
                          <Show when={buildSecretKeys().length > 0}>
                            <div>
                              <div class="text-[10px] font-medium text-gray-400 uppercase tracking-wider mb-1">
                                Build Secrets
                              </div>
                              <div class="space-y-0.5">
                                <For each={buildSecretKeys()}>
                                  {(key) => (
                                    <div class="flex items-baseline gap-2 text-xs">
                                      <span class="text-gray-500 font-mono">{key}</span>
                                      <span class="text-gray-300">=</span>
                                      <span class="text-gray-400 font-mono">••••••••</span>
                                    </div>
                                  )}
                                </For>
                              </div>
                            </div>
                          </Show>
                          <Show when={envEntries().length > 0}>
                            <div>
                              <div class="text-[10px] font-medium text-gray-400 uppercase tracking-wider mb-1">
                                Environment
                              </div>
                              <div class="space-y-0.5">
                                <For each={envEntries()}>
                                  {([key, value]) => (
                                    <div class="flex items-baseline gap-2 text-xs">
                                      <span class="text-gray-500 font-mono">{key}</span>
                                      <span class="text-gray-300">=</span>
                                      <span class="text-gray-700 font-mono truncate">{value}</span>
                                    </div>
                                  )}
                                </For>
                              </div>
                            </div>
                          </Show>
                          <Show when={secretKeys().length > 0}>
                            <div>
                              <div class="text-[10px] font-medium text-gray-400 uppercase tracking-wider mb-1">
                                Secrets
                                <span class="normal-case ml-1 text-gray-300">
                                  ({d.config.deploy.secrets?.mountPath})
                                </span>
                              </div>
                              <div class="space-y-0.5">
                                <For each={secretKeys()}>
                                  {([key, meta]) => {
                                    const changed =
                                      meta.prevHash != null && meta.hash !== meta.prevHash;
                                    return (
                                      <div class="flex items-baseline gap-2 text-xs">
                                        <span class="text-gray-500 font-mono">{key}</span>
                                        <span class="text-gray-300">=</span>
                                        <span class="text-gray-400 font-mono">••••••••</span>
                                        {changed && (
                                          <span class="text-amber-500 text-[10px]">changed</span>
                                        )}
                                      </div>
                                    );
                                  }}
                                </For>
                              </div>
                            </div>
                          </Show>
                        </div>
                      </Show>
                    </div>
                    <Show when={isLogsOpen()}>
                      <div class="border-t border-gray-200">
                        <DeploymentLogViewer
                          serviceId={props.serviceId}
                          deploymentId={d.id}
                          isSystem={false}
                          hasBuild={props.hasBuild}
                          embedded={true}
                        />
                      </div>
                    </Show>
                  </div>
                );
              }}
            </For>
          </div>
        </Show>
      </Suspense>
    </>
  );
}

const TIME_RANGES = [
  { label: "1h", ms: 3_600_000 },
  { label: "6h", ms: 21_600_000 },
  { label: "24h", ms: 86_400_000 },
  { label: "7d", ms: 604_800_000 }
];
const METRICS_POLL_MS = 10_000;

function MetricsTab(props: { service: Service }) {
  const [rangeMs, setRangeMs] = createSignal(3_600_000);

  const [metrics, { refetch }] = createResource(
    () => ({ serviceId: props.service.id, range: rangeMs() }),
    ({ serviceId, range }) => {
      const now = Date.now();
      return getServiceMetrics(serviceId, now - range, now);
    }
  );

  const pollTimer = setInterval(refetch, METRICS_POLL_MS);
  onCleanup(() => clearInterval(pollTimer));

  const data = () => metrics() ?? [];
  const xMax = () => Date.now();
  const xMin = () => xMax() - rangeMs();
  const cpuData = () => data().map((m) => ({ ts: m.ts, value: m.cpuPercent }));
  const memData = () => data().map((m) => ({ ts: m.ts, value: m.memoryBytes }));
  const netRxData = () => data().map((m) => ({ ts: m.ts, value: m.netRxBytes }));
  const netTxData = () => data().map((m) => ({ ts: m.ts, value: m.netTxBytes }));

  const formatBytes = (v: number) => {
    if (v >= 1_000_000_000) return `${(v / 1_000_000_000).toFixed(1)} GB`;
    if (v >= 1_000_000) return `${(v / 1_000_000).toFixed(1)} MB`;
    if (v >= 1_000) return `${(v / 1_000).toFixed(1)} KB`;
    return `${Math.round(v)} B`;
  };

  const formatPercent = (v: number) => `${v.toFixed(1)}%`;

  return (
    <div class="space-y-4">
      <Show when={metrics.error}>
        <ErrorBanner message="Failed to load metrics" onRetry={refetch} />
      </Show>
      <div class="flex justify-end">
        <div class="flex gap-1 bg-gray-100 rounded-md p-0.5">
          <For each={TIME_RANGES}>
            {(range) => (
              <button
                type="button"
                onClick={() => setRangeMs(range.ms)}
                class={clsx("text-xs px-3 py-1 rounded outline-none transition-colors", {
                  "bg-white text-gray-900 shadow-sm font-medium": rangeMs() === range.ms,
                  "text-gray-500 hover:text-gray-700": rangeMs() !== range.ms
                })}
              >
                {range.label}
              </button>
            )}
          </For>
        </div>
      </div>

      <div class="bg-white rounded-lg border border-gray-200 p-4">
        <h3 class="text-xs font-medium text-gray-500 uppercase tracking-wider mb-3">CPU Usage</h3>
        <TimelineChart
          data={cpuData()}
          label="CPU"
          color="#6366f1"
          yFormat={formatPercent}
          xMin={xMin()}
          xMax={xMax()}
        />
      </div>

      <div class="bg-white rounded-lg border border-gray-200 p-4">
        <h3 class="text-xs font-medium text-gray-500 uppercase tracking-wider mb-3">Memory</h3>
        <TimelineChart
          data={memData()}
          label="Memory"
          color="#8b5cf6"
          yFormat={formatBytes}
          xMin={xMin()}
          xMax={xMax()}
        />
      </div>

      <div class="bg-white rounded-lg border border-gray-200 p-4">
        <h3 class="text-xs font-medium text-gray-500 uppercase tracking-wider mb-3">Network I/O</h3>
        <TimelineChart
          data={netRxData()}
          label="RX"
          color="#10b981"
          yFormat={formatBytes}
          xMin={xMin()}
          xMax={xMax()}
          secondarySeries={{ data: netTxData(), color: "#f59e0b", label: "TX" }}
        />
      </div>
    </div>
  );
}

const DEFAULT_LOG_TAIL = 1000;
const LOAD_MORE_STEP = 5000;
const POLL_INTERVAL_MS = 5000;

function LogsTab(props: { service: Service }) {
  const isSystem = props.service.system === true;

  const [deployments] = createResource(
    () => (isSystem ? null : props.service.id),
    (id) => getDeployments(id)
  );

  const latestDeployment = () => deployments()?.[0] ?? null;

  return (
    <Show
      when={!isSystem && latestDeployment()}
      fallback={
        isSystem ? (
          <DeploymentLogViewer
            serviceId={props.service.id}
            deploymentId={null}
            isSystem={true}
            hasBuild={false}
          />
        ) : (
          <div class="bg-white rounded-lg border border-gray-200 p-8 text-center">
            <p class="text-sm text-gray-400">
              No deployments yet. Deploy this service to see logs.
            </p>
          </div>
        )
      }
    >
      {(dep) => (
        <DeploymentLogViewer
          serviceId={props.service.id}
          deploymentId={dep().id}
          isSystem={false}
          hasBuild={!!props.service.build}
          label={dep().id.split("-").slice(-1)[0]}
        />
      )}
    </Show>
  );
}

function DeploymentLogViewer(props: {
  serviceId: string;
  deploymentId: string | null;
  isSystem: boolean;
  hasBuild: boolean;
  label?: string;
  embedded?: boolean;
}) {
  const [lines, setLines] = createSignal<LogEntry[]>([]);
  const [loading, setLoading] = createSignal(true);
  const [error, setError] = createSignal<string | null>(null);
  const [hasMore, setHasMore] = createSignal(false);
  const [tail, setTail] = createSignal(DEFAULT_LOG_TAIL);
  const [userPhase, setUserPhase] = createSignal<"deploy" | "build" | null>(null);

  const hasBuildLogs = () => lines().some((l) => l.source?.endsWith("/build"));
  const showTabs = () => props.hasBuild || hasBuildLogs();
  const defaultPhase = createMemo(() => {
    const all = lines().filter((l) => l.text.trim().length > 0);
    const hasDeploy = all.some((l) => !l.source?.endsWith("/build"));
    if (!hasDeploy && hasBuildLogs()) return "build";
    return "deploy";
  });
  const logPhase = () => userPhase() ?? defaultPhase();
  const filteredLines = () => {
    const all = lines().filter((l) => l.text.trim().length > 0);
    if (!showTabs()) return all;
    const phase = logPhase();
    return all.filter((l) => {
      if (phase === "build") return l.source?.endsWith("/build");
      return !l.source?.endsWith("/build");
    });
  };

  const logColumns: ColumnDef<LogEntry>[] = [
    {
      id: "expander",
      size: 28,
      header: () => null,
      cell: ({ row }) => (
        <button
          type="button"
          class="p-0.5 text-gray-300 hover:text-gray-500 transition-colors outline-none"
          onClick={(ev) => {
            ev.stopPropagation();
            row.toggleExpanded();
          }}
        >
          <ChevronRight
            size={12}
            class={clsx("transition-transform", { "rotate-90": row.getIsExpanded() })}
          />
        </button>
      )
    },
    {
      accessorKey: "ts",
      header: "Timestamp",
      size: 155,
      cell: (info) => (
        <span class="text-gray-400 select-none whitespace-nowrap">
          {formatTs(info.getValue<number>())}
        </span>
      )
    },
    {
      id: "host",
      header: "Host",
      size: 160,
      accessorFn: (row) => row.hostname || row.source || "",
      cell: (info) => (
        <span class="text-violet-400 truncate block" title={info.getValue<string>()}>
          {info.getValue<string>()}
        </span>
      )
    },
    {
      accessorKey: "level",
      header: "Level",
      size: 46,
      cell: (info) => {
        const level = info.getValue<string>();
        return (
          <span class={clsx("uppercase whitespace-nowrap", logLevelColor(level))}>{level}</span>
        );
      }
    },
    {
      accessorKey: "text",
      header: "Message",
      cell: (info) => <span class="text-gray-700 break-all">{info.getValue<string>()}</span>
    }
  ];

  const table = createSolidTable({
    get data() {
      return filteredLines();
    },
    columns: logColumns,
    getCoreRowModel: getCoreRowModel(),
    getExpandedRowModel: getExpandedRowModel(),
    getRowCanExpand: () => true
  });

  const fetchLogs = async () => {
    try {
      const t = tail();
      let fetched: LogEntry[];
      if (props.isSystem) {
        fetched = await getSystemLogs(props.serviceId, t);
      } else {
        if (!props.deploymentId) return;
        fetched = await getLogs(props.serviceId, props.deploymentId, t);
      }
      setHasMore(fetched.length >= t);
      setLines(fetched);
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load logs");
    } finally {
      setLoading(false);
    }
  };

  createEffect(
    on(
      () => props.deploymentId,
      () => {
        setLines([]);
        setLoading(true);
        setTail(DEFAULT_LOG_TAIL);
        setUserPhase(null);
        fetchLogs();
      }
    )
  );

  const pollTimer = setInterval(fetchLogs, POLL_INTERVAL_MS);
  onCleanup(() => clearInterval(pollTimer));

  let scrollRef: HTMLDivElement | undefined;
  let wasAtBottom = true;

  createEffect(
    on(filteredLines, () => {
      if (wasAtBottom && scrollRef) {
        requestAnimationFrame(() => {
          scrollRef!.scrollTop = scrollRef!.scrollHeight;
        });
      }
    })
  );

  const onScroll = () => {
    if (!scrollRef) return;
    wasAtBottom = scrollRef.scrollHeight - scrollRef.scrollTop - scrollRef.clientHeight < 50;
  };

  const loadMore = async () => {
    const newTail = tail() + LOAD_MORE_STEP;
    setTail(newTail);
    let fetched: LogEntry[];
    if (props.isSystem) {
      fetched = await getSystemLogs(props.serviceId, newTail);
    } else {
      if (!props.deploymentId) return;
      fetched = await getLogs(props.serviceId, props.deploymentId, newTail);
    }
    setHasMore(fetched.length >= newTail);
    setLines(fetched);
  };

  return (
    <div class={clsx("overflow-hidden", {
      "bg-white rounded-lg border border-gray-200": !props.embedded
    })}>
      <Show when={error()}>
        <div class="p-3">
          <ErrorBanner message={error()!} onRetry={fetchLogs} />
        </div>
      </Show>
      <Show when={showTabs() || props.label}>
        <div class="px-4 py-2.5 border-b border-gray-100 flex items-center gap-4">
          <Show when={props.label}>
            <span class="text-xs text-gray-500 font-medium">{props.label}</span>
          </Show>
          <Show when={showTabs()}>
            <div class="flex gap-1 bg-gray-100 rounded-md p-0.5">
              <button
                type="button"
                onClick={() => setUserPhase("build")}
                class={clsx("text-xs px-3 py-1 rounded outline-none transition-colors", {
                  "bg-white text-gray-900 shadow-sm font-medium": logPhase() === "build",
                  "text-gray-500 hover:text-gray-700": logPhase() !== "build"
                })}
              >
                Build
              </button>
              <button
                type="button"
                onClick={() => setUserPhase("deploy")}
                class={clsx("text-xs px-3 py-1 rounded outline-none transition-colors", {
                  "bg-white text-gray-900 shadow-sm font-medium": logPhase() === "deploy",
                  "text-gray-500 hover:text-gray-700": logPhase() !== "deploy"
                })}
              >
                Deploy
              </button>
            </div>
          </Show>
        </div>
      </Show>
      <div ref={scrollRef} onScroll={onScroll} class="max-h-[600px] overflow-y-auto">
        <Switch>
          <Match when={loading()}>
            <div class="text-gray-400 text-center py-8 font-mono text-xs">Loading logs…</div>
          </Match>
          <Match when={!loading() && filteredLines().length === 0}>
            <div class="text-gray-400 text-center py-8 font-mono text-xs">No logs available.</div>
          </Match>
          <Match when={filteredLines().length > 0}>
            <Show when={hasMore()}>
              <div class="text-center py-3">
                <button
                  type="button"
                  onClick={loadMore}
                  class="text-xs text-indigo-600 hover:text-indigo-700 font-medium outline-none"
                >
                  Load previous logs
                </button>
              </div>
            </Show>
            <table class="w-full font-mono text-xs border-collapse" style="table-layout: fixed">
              <colgroup>
                <col style="width: 28px" />
                <col style="width: 155px" />
                <col style="width: 160px" />
                <col style="width: 46px" />
                <col />
              </colgroup>
              <tbody>
                <For each={table.getRowModel().rows}>
                  {(row) => (
                    <>
                      <tr
                        class={clsx(
                          "align-top border-b border-gray-50 cursor-pointer transition-colors",
                          {
                            "bg-indigo-50/50 hover:bg-indigo-50/70": row.getIsExpanded(),
                            "hover:bg-gray-50/50": !row.getIsExpanded()
                          }
                        )}
                        onClick={() => row.toggleExpanded()}
                      >
                        <For each={row.getVisibleCells()}>
                          {(cell) => (
                            <td
                              class={clsx("py-1", {
                                "pl-2": cell.column.id === "expander",
                                "pr-2": cell.column.id === "ts",
                                "px-2": cell.column.id !== "expander" && cell.column.id !== "text",
                                "pl-2 pr-4": cell.column.id === "text",
                                "text-right": cell.column.id === "level"
                              })}
                            >
                              {flexRender(cell.column.columnDef.cell, cell.getContext())}
                            </td>
                          )}
                        </For>
                      </tr>
                      <Show when={row.getIsExpanded()}>
                        <tr class="border-b border-gray-100 bg-gray-50/80">
                          <td colSpan={5} class="px-4 py-3">
                            <LogDetailPanel entry={row.original} />
                          </td>
                        </tr>
                      </Show>
                    </>
                  )}
                </For>
              </tbody>
            </table>
          </Match>
        </Switch>
      </div>
    </div>
  );
}

function LogDetailPanel(props: { entry: LogEntry }) {
  const baseAttrs = () => {
    const entry = props.entry;
    const attrs: { label: string; value: string }[] = [
      { label: "Timestamp", value: new Date(entry.ts).toISOString() },
      { label: "Sequence", value: String(entry.seq) },
      { label: "Level", value: entry.level.toUpperCase() },
      { label: "Stream", value: entry.stream }
    ];
    if (entry.hostname) {
      attrs.push({ label: "Hostname", value: entry.hostname });
    }
    if (entry.source) {
      attrs.push({ label: "Source", value: entry.source });
    }
    entry.attrs?.forEach(([key, value]) => {
      attrs.push({ label: key, value });
    });
    return attrs;
  };

  const tags = () => props.entry.tags?.filter((tag) => !tag.startsWith("hostname:")) ?? [];

  return (
    <div class="flex flex-col gap-2.5">
      <pre class="text-xs text-gray-800 font-mono whitespace-pre-wrap break-all">
        {props.entry.text}
      </pre>
      <div class="grid grid-cols-[auto_1fr] gap-x-6 gap-y-1.5">
        <For each={baseAttrs()}>
          {(attr) => (
            <>
              <span class="text-[11px] text-gray-400 font-medium whitespace-nowrap">
                {attr.label}
              </span>
              <span class="text-[11px] font-mono text-gray-600">{attr.value}</span>
            </>
          )}
        </For>
      </div>
      <Show when={tags().length > 0}>
        <div class="flex items-center gap-1.5 flex-wrap">
          <span class="text-[11px] text-gray-400 font-medium">Tags</span>
          <For each={tags()}>
            {(tag) => (
              <span class="text-[11px] font-mono text-gray-600 bg-gray-200/70 rounded px-1.5 py-0.5">
                {tag}
              </span>
            )}
          </For>
        </div>
      </Show>
    </div>
  );
}

function formatTs(ms: number) {
  return new Date(ms).toISOString().replace("T", " ").replace("Z", "").slice(0, 19);
}

function logLevelColor(level: string) {
  return {
    "text-red-500": level === "error",
    "text-amber-500": level === "warn",
    "text-gray-400": level === "debug",
    "text-gray-300": level === "trace",
    "text-blue-400": level !== "error" && level !== "warn" && level !== "debug" && level !== "trace"
  };
}

function ConfigSection(props: { title: string; items: { label: string; value: string }[] }) {
  return (
    <div>
      <h4 class="text-xs font-medium text-gray-400 uppercase tracking-wider mb-2">{props.title}</h4>
      <div class="bg-white rounded-lg border border-gray-200 divide-y divide-gray-100">
        <For each={props.items}>
          {(item) => (
            <div class="px-4 py-2.5 flex items-baseline justify-between gap-6">
              <span class="text-xs text-gray-500 shrink-0">{item.label}</span>
              <span class="text-sm font-mono text-gray-800 text-right truncate">{item.value}</span>
            </div>
          )}
        </For>
      </div>
    </div>
  );
}
