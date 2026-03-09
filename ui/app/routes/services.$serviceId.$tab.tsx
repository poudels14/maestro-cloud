import { createFileRoute, useNavigate } from "@tanstack/solid-router";
import { createResource, For, Show, Suspense } from "solid-js";
import { ArrowLeft, Clock, GitCommitHorizontal, Rocket } from "lucide-solid";
import type { Service } from "../lib/types";
import {
  getServices,
  getDeployments,
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
  const loading = () => services.loading;

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
            const status = service.status ?? "IDLE";
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
          <TabButton
            label="Deployments"
            active={props.tab === "deployments"}
            onClick={() => props.navigateTab("deployments")}
          />
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
    { label: "Deploy command", value: deployCommand },
    { label: "Healthcheck path", value: s.deploy.healthcheckPath }
  ];

  return (
    <div class="space-y-6">
      <ConfigSection title="General" items={configItems} />
      <ConfigSection title="Source" items={sourceItems} />
      <ConfigSection title="Deploy" items={deployItems} />
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
                  <div class="flex items-center gap-3 text-xs text-gray-400">
                    <span class="font-mono">{d.config.version.slice(0, 12)}</span>
                    <Show when={d.gitCommit}>
                      <span class="flex items-center gap-1">
                        <GitCommitHorizontal class="size-3" />
                        <span class="font-mono truncate">{d.gitCommit}</span>
                      </span>
                    </Show>
                    <span class="flex items-center gap-1 ml-auto">
                      <Clock class="size-3" />
                      {timeAgo(d.createdAt)}
                    </span>
                  </div>
                </div>
              );
            }}
          </For>
        </div>
      </Show>
    </Suspense>
  );
}

function LogsTab(props: { service: Service }) {
  const DUMMY_LOGS = [
    { ts: "2026-03-09T10:00:01Z", level: "INFO", msg: `[${props.service.id}] Service starting...` },
    { ts: "2026-03-09T10:00:02Z", level: "INFO", msg: `[${props.service.id}] Listening on :8080` },
    { ts: "2026-03-09T10:00:05Z", level: "INFO", msg: `[${props.service.id}] Health check passed` },
    { ts: "2026-03-09T10:00:12Z", level: "INFO", msg: `[${props.service.id}] GET / 200 3ms` },
    {
      ts: "2026-03-09T10:00:14Z",
      level: "INFO",
      msg: `[${props.service.id}] GET /api/status 200 1ms`
    },
    {
      ts: "2026-03-09T10:00:20Z",
      level: "WARN",
      msg: `[${props.service.id}] Slow query detected (254ms)`
    },
    { ts: "2026-03-09T10:00:25Z", level: "INFO", msg: `[${props.service.id}] GET / 200 2ms` },
    { ts: "2026-03-09T10:00:30Z", level: "INFO", msg: `[${props.service.id}] Health check passed` }
  ];

  const levelColor = (level: string) => {
    if (level === "WARN") return "text-amber-600";
    if (level === "ERROR") return "text-red-600";
    return "text-gray-400";
  };

  return (
    <div class="bg-white rounded-lg border border-gray-200 overflow-hidden">
      <div class="p-4 font-mono text-xs leading-6 overflow-x-auto">
        <For each={DUMMY_LOGS}>
          {(line) => (
            <div class="flex gap-3 whitespace-nowrap">
              <span class="text-gray-400 select-none shrink-0">
                {line.ts.replace("T", " ").replace("Z", "")}
              </span>
              <span class={`w-10 shrink-0 ${levelColor(line.level)}`}>{line.level}</span>
              <span class="text-gray-700">{line.msg}</span>
            </div>
          )}
        </For>
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
