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
import { ArrowLeft, ChevronDown, Clock, GitCommitHorizontal, Rocket } from "lucide-solid";
import { Select } from "@kobalte/core/select";
import type { LogEntry, Service } from "../lib/types";
import {
  getServices,
  getDeployments,
  getLogs,
  getSystemLogs,
  redeployService,
  cancelDeployment,
  stopDeployment
} from "../lib/api";
import { StatusBadge, StatusDot, DeploymentMenu, TabButton, timeAgo } from "../lib/ui";

const VALID_TABS = new Set(["overview", "deployments", "logs"]);
type DetailTab = "overview" | "deployments" | "logs";

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
  const [services] = createResource(() => (import.meta.env.SSR ? null : true), getServices);
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
          <ServiceDetailPanel service={service()} tab={tab} navigateTab={navigateTab} />
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
      <div class="px-4 h-14 flex items-center gap-2 border-b border-gray-200 shrink-0">
        <button
          type="button"
          onClick={props.onBack}
          class="text-gray-400 hover:text-gray-600 p-1 -ml-1 rounded hover:bg-gray-100 transition-colors outline-none"
        >
          <ArrowLeft class="size-4" />
        </button>
        <span class="text-sm font-semibold text-gray-900">Services</span>
        <span class="text-xs text-gray-400">{props.services.length}</span>
      </div>
      <div class="flex-1 overflow-y-auto py-1">
        <For each={props.services}>
          {(service) => {
            const isSelected = () => service.id === props.selected?.id;
            const isSystem = service.system === true;
            const status = isSystem ? "SYSTEM" : (service.status ?? "IDLE");
            return (
              <button
                type="button"
                onClick={() => props.onSelect(service)}
                class={`w-full text-left px-4 py-2.5 flex items-center gap-2.5 text-sm transition-colors outline-none ${
                  isSelected()
                    ? "bg-indigo-50 text-indigo-700 font-medium"
                    : "text-gray-700 hover:bg-gray-50"
                }`}
              >
                <StatusDot status={status} />
                <span class="truncate">{service.name}</span>
              </button>
            );
          }}
        </For>
      </div>
    </div>
  );
}

function ServiceDetailPanel(props: {
  service: Service;
  tab: DetailTab;
  navigateTab: (t: DetailTab) => void;
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
            <OverviewTab service={s} />
          </Show>
          <Show when={props.tab === "deployments"}>
            <DeploymentsTab serviceId={s.id} />
          </Show>
          <Show when={props.tab === "logs"}>
            <LogsTab service={s} />
          </Show>
        </div>
      </div>
    </div>
  );
}

function OverviewTab(props: { service: Service }) {
  const s = props.service;

  const configItems = [
    { label: "Service ID", value: s.id },
    { label: "Version", value: s.version }
  ];

  const sourceItems =
    s.build != null
      ? [
          { label: "Git repository", value: s.build.repo },
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
    </div>
  );
}

function DeploymentsTab(props: { serviceId: string }) {
  const [deployments, { refetch }] = createResource(() => props.serviceId, getDeployments);

  return (
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
              const secretKeys = () => Object.entries(d.config.deploy.secrets?.keys ?? {}).sort(([a], [b]) => a.localeCompare(b));
              const changedSecrets = () => secretKeys().filter(([, meta]) => meta.prevHash != null && meta.hash !== meta.prevHash).map(([key]) => key);
              const envEntries = () => Object.entries(d.config.deploy.env ?? {}).sort(([a], [b]) => a.localeCompare(b));
              const hasDetails = () => envEntries().length > 0 || secretKeys().length > 0;
              return (
                <div class="rounded-lg border border-gray-200 bg-white p-4 transition-colors hover:bg-gray-50/50">
                  <div class="flex items-center justify-between gap-3 mb-2">
                    <div class="flex items-center gap-2.5 min-w-0">
                      <span class="text-sm font-semibold font-mono text-gray-900">#{shortId}</span>
                    </div>
                    <div class="flex items-center gap-2 shrink-0">
                      <StatusBadge status={d.status} />
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
                        onRedeploy={async () => {
                          await redeployService(props.serviceId);
                          refetch();
                        }}
                      />
                    </div>
                  </div>
                  <Show when={d.replicas && d.replicas.length > 0}>
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
                    <Show when={d.gitCommit}>
                      <span class="flex items-center gap-1">
                        <GitCommitHorizontal class="size-3" />
                        <span class="font-mono truncate">{d.gitCommit}</span>
                      </span>
                    </Show>
                    <Show when={hasDetails()}>
                      <button
                        type="button"
                        onClick={() => setExpanded(!expanded())}
                        class="text-xs text-indigo-500 hover:text-indigo-600 outline-none"
                      >
                        {expanded() ? "hide details" : "details"}
                      </button>
                    </Show>
                    <span class="flex items-center gap-1 ml-auto">
                      <Clock class="size-3" />
                      {timeAgo(d.createdAt)}
                    </span>
                  </div>
                  <Show when={expanded()}>
                    <div class="mt-3 pt-3 border-t border-gray-100 space-y-3">
                      <Show when={envEntries().length > 0}>
                        <div>
                          <div class="text-[10px] font-medium text-gray-400 uppercase tracking-wider mb-1">Environment</div>
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
                            <span class="normal-case ml-1 text-gray-300">({d.config.deploy.secrets?.mountPath})</span>
                          </div>
                          <div class="space-y-0.5">
                            <For each={secretKeys()}>
                              {([key, meta]) => {
                                const changed = meta.prevHash != null && meta.hash !== meta.prevHash;
                                const isNew = meta.prevHash == null;
                                return (
                                  <div class="flex items-baseline gap-2 text-xs">
                                    <span class="text-gray-500 font-mono">{key}</span>
                                    <span class="text-gray-300">=</span>
                                    <span class="text-gray-400 font-mono">••••••••</span>
                                    {changed && <span class="text-amber-500 text-[10px]">changed</span>}
                                    {isNew && <span class="text-green-500 text-[10px]">new</span>}
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
              );
            }}
          </For>
        </div>
      </Show>
    </Suspense>
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
  const [selectedId, setSelectedId] = createSignal<string | null>(null);
  const activeId = () => selectedId() ?? deployments()?.[0]?.id ?? null;

  const [lines, setLines] = createSignal<LogEntry[]>([]);
  const [loading, setLoading] = createSignal(true);
  const [hasMore, setHasMore] = createSignal(false);
  const [tail, setTail] = createSignal(DEFAULT_LOG_TAIL);

  const fetchLogs = async () => {
    const t = tail();
    let fetched: LogEntry[];
    if (isSystem) {
      fetched = await getSystemLogs(props.service.id, t);
    } else {
      const did = activeId();
      if (!did) return;
      fetched = await getLogs(props.service.id, did, t);
    }

    setHasMore(fetched.length >= t);
    setLines(fetched);
    setLoading(false);
  };

  createEffect(
    on(activeId, () => {
      setLines([]);
      setLoading(true);
      setTail(DEFAULT_LOG_TAIL);
      fetchLogs();
    })
  );

  const pollTimer = setInterval(fetchLogs, POLL_INTERVAL_MS);
  onCleanup(() => clearInterval(pollTimer));

  let scrollRef: HTMLDivElement | undefined;
  let wasAtBottom = true;

  createEffect(
    on(lines, () => {
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
    if (isSystem) {
      fetched = await getSystemLogs(props.service.id, newTail);
    } else {
      const did = activeId();
      if (!did) return;
      fetched = await getLogs(props.service.id, did, newTail);
    }
    setHasMore(fetched.length >= newTail);
    setLines(fetched);
  };

  const formatTs = (ms: number) =>
    new Date(ms).toISOString().replace("T", " ").replace("Z", "").slice(0, 19);

  return (
    <div class="bg-white rounded-lg border border-gray-200 overflow-hidden">
      <Show when={!isSystem}>
        <div class="px-4 py-2 border-b border-gray-100">
          <Select
            options={deployments() ?? []}
            optionValue="id"
            optionTextValue={(d) => `#${d.id.split("-").slice(-1)[0]} — ${d.status.toLowerCase()}`}
            value={deployments()?.find((d) => d.id === activeId()) ?? null}
            onChange={(d) => {
              if (d) setSelectedId(d.id);
            }}
            itemComponent={(itemProps) => {
              const d = itemProps.item.rawValue;
              const shortId = d.id.split("-").slice(-1)[0] ?? d.id;
              return (
                <Select.Item
                  item={itemProps.item}
                  class="text-xs px-3 py-1.5 cursor-pointer outline-none rounded data-[highlighted]:bg-indigo-50 data-[highlighted]:text-indigo-700 text-gray-700"
                >
                  <Select.ItemLabel>
                    #{shortId} — {d.status.toLowerCase()}
                  </Select.ItemLabel>
                </Select.Item>
              );
            }}
          >
            <Select.Trigger class="inline-flex items-center gap-1.5 text-xs text-gray-600 bg-gray-50 border border-gray-200 rounded px-2.5 py-1.5 outline-none hover:border-gray-300 transition-colors">
              <Select.Value<typeof import("../lib/types").Deployment>>
                {(state) => {
                  const d = state.selectedOption();
                  if (!d) return "Select deployment";
                  const shortId = d.id.split("-").slice(-1)[0] ?? d.id;
                  return `#${shortId} — ${d.status.toLowerCase()}`;
                }}
              </Select.Value>
              <Select.Icon>
                <ChevronDown class="size-3 text-gray-400" />
              </Select.Icon>
            </Select.Trigger>
            <Select.Portal>
              <Select.Content class="bg-white border border-gray-200 rounded-lg shadow-lg z-50 py-1 overflow-hidden">
                <Select.Listbox class="max-h-48 overflow-y-auto outline-none" />
              </Select.Content>
            </Select.Portal>
          </Select>
        </div>
      </Show>
      <div
        ref={scrollRef}
        onScroll={onScroll}
        class="p-4 font-mono text-xs leading-6 max-h-[600px] overflow-y-auto"
      >
        <Show
          when={!loading() && lines().length > 0}
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
          <For each={lines()}>
            {(line) => {
              const levelColor = () => {
                switch (line.level) {
                  case "error":
                    return "text-red-500";
                  case "warn":
                    return "text-amber-500";
                  case "debug":
                    return "text-gray-400";
                  case "trace":
                    return "text-gray-300";
                  default:
                    return "text-blue-400";
                }
              };
              return (
                <div class="flex gap-3">
                  <span class="text-gray-400 select-none shrink-0 whitespace-nowrap">
                    {formatTs(line.ts)}
                  </span>
                  <Show when={line.hostname}>
                    <span class="text-violet-400 shrink-0 whitespace-nowrap truncate max-w-32">
                      {line.hostname}
                    </span>
                  </Show>
                  <span class={`w-10 shrink-0 whitespace-nowrap uppercase ${levelColor()}`}>
                    {line.level}
                  </span>
                  <span class="text-gray-700 break-words">{line.text}</span>
                </div>
              );
            }}
          </For>
        </Show>
      </div>
    </div>
  );
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
