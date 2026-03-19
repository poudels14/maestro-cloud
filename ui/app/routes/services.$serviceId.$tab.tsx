import { createFileRoute, useNavigate } from "@tanstack/solid-router";
import {
  createEffect,
  createResource,
  createSignal,
  For,
  Show,
  Suspense,
  on,
  onCleanup
} from "solid-js";
import { Clock, GitCommitHorizontal, Home, Rocket } from "lucide-solid";
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

  const configItems = [
    { label: "Service ID", value: s.id },
    { label: "Version", value: s.version }
  ];

  const sourceItems =
    s.build != null
      ? [
          { label: "Git repository", value: s.build.repo },
          ...(s.build.branch ? [{ label: "Branch", value: s.build.branch }] : []),
          { label: "Dockerfile", value: s.build.dockerfilePath }
        ]
      : [{ label: "Image", value: s.image ?? "(not set)" }];

  const deployCommand = s.deploy.command
    ? `${s.deploy.command.command} ${s.deploy.command.args.join(" ")}`.trim()
    : "(not set)";
  const deployItems = [
    { label: "Replicas", value: String(s.deploy.replicas ?? 1) },
    { label: "Deploy command", value: deployCommand },
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
      <ConfigSection title="General" items={configItems} />
      <ConfigSection title="Source" items={sourceItems} />
      <ConfigSection title="Deploy" items={deployItems} />
      <Show when={ingressItems}>
        {(items) => <ConfigSection title="Ingress" items={items()} />}
      </Show>
      <Show when={envItems.length > 0}>
        <ConfigSection title="Environment" items={envItems} />
      </Show>
      <Show when={secretKeys.length > 0}>
        <div>
          <h4 class="text-xs font-medium text-gray-400 uppercase tracking-wider mb-2">
            Secrets
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
                const hasDetails = () => envEntries().length > 0 || secretKeys().length > 0;
                return (
                  <div class="rounded-lg border border-gray-200 bg-white overflow-hidden">
                    <div class="p-4">
                      <div class="flex items-start justify-between gap-3 mb-1">
                        <div class="text-sm font-semibold text-gray-900 truncate min-w-0">
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
                      <div class="flex items-center gap-3 text-xs text-gray-400">
                        <span class="font-mono">{d.config.version.slice(0, 12)}</span>
                        <Show when={hasDetails()}>
                          <button
                            type="button"
                            onClick={() => setExpanded(!expanded())}
                            class="text-xs text-indigo-500 hover:text-indigo-600 outline-none"
                          >
                            {expanded() ? "hide details" : "details"}
                          </button>
                        </Show>
                        <button
                          type="button"
                          onClick={() => setLogsOpen(isLogsOpen() ? null : d.id)}
                          class="text-xs text-indigo-500 hover:text-indigo-600 outline-none"
                        >
                          {isLogsOpen() ? "hide logs" : "logs"}
                        </button>
                      </div>
                      <Show when={expanded()}>
                        <div class="mt-3 pt-3 border-t border-gray-100 space-y-3">
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
                                    const isNew = meta.prevHash == null;
                                    return (
                                      <div class="flex items-baseline gap-2 text-xs">
                                        <span class="text-gray-500 font-mono">{key}</span>
                                        <span class="text-gray-300">=</span>
                                        <span class="text-gray-400 font-mono">••••••••</span>
                                        {changed && (
                                          <span class="text-amber-500 text-[10px]">changed</span>
                                        )}
                                        {isNew && (
                                          <span class="text-green-500 text-[10px]">new</span>
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
}) {
  const [lines, setLines] = createSignal<LogEntry[]>([]);
  const [loading, setLoading] = createSignal(true);
  const [error, setError] = createSignal<string | null>(null);
  const [hasMore, setHasMore] = createSignal(false);
  const [tail, setTail] = createSignal(DEFAULT_LOG_TAIL);
  const [logPhase, setLogPhase] = createSignal<"deploy" | "build">("deploy");

  const hasBuildLogs = () => lines().some((l) => l.source?.endsWith("/build"));
  const showTabs = () => props.hasBuild || hasBuildLogs();
  const filteredLines = () => {
    const all = lines().filter((l) => l.text.trim().length > 0);
    if (!showTabs()) return all;
    const phase = logPhase();
    return all.filter((l) => {
      if (phase === "build") return l.source?.endsWith("/build");
      return !l.source?.endsWith("/build");
    });
  };

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
    <div class="bg-white rounded-lg border border-gray-200 overflow-hidden">
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
                onClick={() => setLogPhase("build")}
                class={clsx("text-xs px-3 py-1 rounded outline-none transition-colors", {
                  "bg-white text-gray-900 shadow-sm font-medium": logPhase() === "build",
                  "text-gray-500 hover:text-gray-700": logPhase() !== "build"
                })}
              >
                Build
              </button>
              <button
                type="button"
                onClick={() => setLogPhase("deploy")}
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
      <div
        ref={scrollRef}
        onScroll={onScroll}
        class="p-4 font-mono text-xs leading-6 max-h-[600px] overflow-x-auto overflow-y-auto"
      >
        <Show
          when={!loading() && filteredLines().length > 0}
          fallback={
            <div class="text-gray-400 text-center py-8">
              {loading() ? "Loading logs…" : "No logs available."}
            </div>
          }
        >
          <Show when={hasMore()}>
            <div class="text-center pb-3">
              <button
                type="button"
                onClick={loadMore}
                class="text-xs text-indigo-600 hover:text-indigo-700 font-medium outline-none"
              >
                Load previous logs
              </button>
            </div>
          </Show>
          <For each={filteredLines()}>
            {(line) => (
              <div class="flex gap-3 whitespace-nowrap">
                <span class="text-gray-400 select-none shrink-0">{formatTs(line.ts)}</span>
                <Show when={line.hostname}>
                  <span class="text-violet-400 shrink-0 truncate max-w-48" title={line.hostname}>
                    {line.hostname}
                  </span>
                </Show>
                <span
                  class={clsx("w-12 shrink-0 uppercase text-right", {
                    "text-red-500": line.level === "error",
                    "text-amber-500": line.level === "warn",
                    "text-gray-400": line.level === "debug",
                    "text-gray-300": line.level === "trace",
                    "text-blue-400":
                      line.level !== "error" &&
                      line.level !== "warn" &&
                      line.level !== "debug" &&
                      line.level !== "trace"
                  })}
                >
                  {line.level}
                </span>
                <span class="text-gray-700">{line.text}</span>
              </div>
            )}
          </For>
        </Show>
      </div>
    </div>
  );
}

function formatTs(ms: number) {
  return new Date(ms).toISOString().replace("T", " ").replace("Z", "").slice(0, 19);
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
