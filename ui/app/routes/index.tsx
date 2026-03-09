import { createFileRoute } from "@tanstack/solid-router";
import { createResource, createSignal, For, onCleanup, onMount, Show, Suspense } from "solid-js";
import {
  Monitor,
  X,
  EllipsisVertical,
  GitCommitHorizontal,
  FileText,
  HeartPulse,
  Ban,
  Rocket,
  Clock,
  RotateCw,
  Square
} from "lucide-solid";
import type { Deployment, Service } from "../lib/types";

function timeAgo(ms: number): string {
  const seconds = Math.floor((Date.now() - ms) / 1000);
  if (seconds < 60) return "just now";
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  return `${days}d ago`;
}

export const Route = createFileRoute("/")({
  component: ServicesPage
});

async function getServices(): Promise<Service[]> {
  const res = await fetch("/api/services");
  if (!res.ok) throw new Error(`Failed to fetch services: ${res.statusText}`);
  return res.json();
}

async function getDeployments(serviceId: string): Promise<Deployment[]> {
  const url = `/api/services/${serviceId}/deployments`;
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Failed to fetch deployments: ${res.statusText}`);
  return res.json();
}

async function redeployService(serviceId: string) {
  const res = await fetch(`/api/services/${serviceId}/redeploy`, { method: "POST" });
  if (!res.ok) throw new Error(`Failed to redeploy: ${res.statusText}`);
}

async function cancelDeployment(serviceId: string, deploymentId: string) {
  const encodedDeploymentId = encodeURIComponent(deploymentId);
  const url = `/api/services/${serviceId}/deployments/${encodedDeploymentId}/cancel`;
  const res = await fetch(url, { method: "PATCH" });
  if (!res.ok) throw new Error(`Failed to cancel deployment: ${res.statusText}`);
}

async function stopDeployment(serviceId: string, deploymentId: string) {
  const encodedDeploymentId = encodeURIComponent(deploymentId);
  const url = `/api/services/${serviceId}/deployments/${encodedDeploymentId}/remove`;
  const res = await fetch(url, { method: "PATCH" });
  if (!res.ok) throw new Error(`Failed to stop deployment: ${res.statusText}`);
}

const STATUS_COLORS: Record<string, { dot: string; bg: string; text: string }> = {
  QUEUED: { dot: "bg-amber-400", bg: "bg-amber-50", text: "text-amber-700" },
  BUILDING: { dot: "bg-blue-400", bg: "bg-blue-50", text: "text-blue-700" },
  READY: { dot: "bg-emerald-400", bg: "bg-emerald-50", text: "text-emerald-700" },
  DEPLOYING: { dot: "bg-indigo-400", bg: "bg-indigo-50", text: "text-indigo-700" },
  RUNNING: { dot: "bg-emerald-400", bg: "bg-emerald-50", text: "text-emerald-700" },
  FAILED: { dot: "bg-red-400", bg: "bg-red-50", text: "text-red-700" },
  CRASHED: { dot: "bg-red-400", bg: "bg-red-50", text: "text-red-700" },
  TERMINATED: { dot: "bg-red-400", bg: "bg-red-50", text: "text-red-700" },
  CANCELLED: { dot: "bg-red-400", bg: "bg-red-50", text: "text-red-700" },
  CANCELED: { dot: "bg-red-400", bg: "bg-red-50", text: "text-red-700" },
  IDLE: { dot: "bg-gray-400", bg: "bg-gray-100", text: "text-gray-600" },
  STOPPED: { dot: "bg-gray-400", bg: "bg-gray-100", text: "text-gray-600" }
};

function StatusBadge(props: { status: string }) {
  const colors = () => STATUS_COLORS[props.status] ?? STATUS_COLORS.STOPPED!;
  return (
    <span
      class={`inline-flex items-center gap-1.5 text-xs font-medium px-2 py-0.5 rounded-full ${colors().bg} ${colors().text}`}
    >
      <span class={`size-1.5 rounded-full ${colors().dot} inline-block`} />
      {props.status.toLowerCase()}
    </span>
  );
}

type Tab = "deployments" | "overview";

function TabButton(props: { active: boolean; label: string; count?: number; onClick: () => void }) {
  return (
    <button
      type="button"
      onClick={props.onClick}
      class={`px-1 pb-2.5 text-sm font-medium border-b-2 transition-colors outline-none ${
        props.active
          ? "border-indigo-500 text-indigo-600"
          : "border-transparent text-gray-400 hover:text-gray-600"
      }`}
    >
      {props.label}
      <Show when={props.count !== undefined}>
        <span class={`ml-1.5 text-xs ${props.active ? "text-indigo-400" : "text-gray-400"}`}>
          {props.count}
        </span>
      </Show>
    </button>
  );
}

const CANCELLABLE_STATUSES = new Set(["QUEUED", "BUILDING", "DEPLOYING"]);
const STOPPABLE_STATUSES = new Set(["READY", "RUNNING"]);

function DeploymentMenu(props: {
  status: string;
  onCancel: () => void;
  onStop: () => void;
  onRedeploy: () => void;
}) {
  const [open, setOpen] = createSignal(false);
  let menuRef: HTMLDivElement | undefined;

  const handleClickOutside = (e: MouseEvent) => {
    if (menuRef && !menuRef.contains(e.target as Node)) {
      setOpen(false);
    }
  };

  onMount(() => document.addEventListener("click", handleClickOutside));
  onCleanup(() => document.removeEventListener("click", handleClickOutside));

  const canCancel = () => CANCELLABLE_STATUSES.has(props.status);
  const canStop = () => STOPPABLE_STATUSES.has(props.status);

  return (
    <div class="relative" ref={menuRef}>
      <button
        type="button"
        onClick={(e) => {
          e.stopPropagation();
          setOpen(!open());
        }}
        class="text-gray-400 hover:text-gray-600 p-1 rounded hover:bg-gray-100 transition-colors outline-none"
      >
        <EllipsisVertical class="size-4" />
      </button>
      <Show when={open()}>
        <div class="absolute right-0 top-full mt-1 w-48 bg-white border border-gray-200 rounded-lg shadow-lg z-10 py-1.5">
          <Show when={canCancel()}>
            <button
              type="button"
              onClick={() => {
                setOpen(false);
                props.onCancel();
              }}
              class="w-full text-left px-4 py-2.5 text-sm text-red-600 hover:bg-red-50 flex items-center gap-2.5 outline-none"
            >
              <Ban class="size-3" />
              Cancel deployment
            </button>
          </Show>
          <Show when={canStop()}>
            <button
              type="button"
              onClick={() => {
                setOpen(false);
                props.onStop();
              }}
              class="w-full text-left px-4 py-2.5 text-sm text-red-600 hover:bg-red-50 flex items-center gap-2.5 outline-none"
            >
              <Square class="size-3" />
              Stop deployment
            </button>
          </Show>
          <Show when={!canCancel()}>
            <button
              type="button"
              onClick={() => {
                setOpen(false);
                props.onRedeploy();
              }}
              class="w-full text-left px-4 py-2.5 text-sm text-gray-700 hover:bg-gray-50 flex items-center gap-2.5 outline-none"
            >
              <RotateCw class="size-3" />
              Redeploy
            </button>
          </Show>
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
            {(d, i) => {
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
    {
      label: "Deploy command",
      value: deployCommand
    },
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

function ServiceSheet(props: { service: Service; onClose: () => void }) {
  const s = props.service;
  const [tab, setTab] = createSignal<Tab>("deployments");

  return (
    <>
      <div class="fixed inset-0 bg-black/20 z-40" onClick={props.onClose} />

      <div class="fixed inset-y-0 right-0 w-full max-w-2xl bg-white border-l border-gray-200 shadow-lg z-50 flex flex-col">
        <div class="px-6 pt-5 pb-0 shrink-0">
          <div class="flex items-center justify-between mb-4">
            <div class="flex items-center gap-2.5 min-w-0">
              <h2 class="text-base font-semibold text-gray-900 truncate">{s.name}</h2>
              <span class="text-xs font-mono bg-indigo-50 text-indigo-600 px-1.5 py-0.5 rounded truncate max-w-48">
                {s.version}
              </span>
            </div>
            <button
              onClick={props.onClose}
              class="text-gray-400 hover:text-gray-600 p-1 -mr-1 outline-none"
            >
              <X class="size-5" />
            </button>
          </div>

          <div class="flex gap-4 border-b border-gray-200 -mb-px">
            <TabButton
              label="Deployments"
              active={tab() === "deployments"}
              onClick={() => setTab("deployments")}
            />
            <TabButton
              label="Overview"
              active={tab() === "overview"}
              onClick={() => setTab("overview")}
            />
          </div>
        </div>

        <div class="flex-1 overflow-y-auto px-6 py-5">
          <Show when={tab() === "deployments"}>
            <DeploymentsTab serviceId={s.id} />
          </Show>
          <Show when={tab() === "overview"}>
            <OverviewTab service={s} />
          </Show>
        </div>
      </div>
    </>
  );
}

function ServiceCard(props: { service: Service; onClick: () => void }) {
  const s = props.service;
  const status = s.status ?? "IDLE";
  const sourceName =
    s.build != null
      ? s.build.repo
          .replace(/\.git$/, "")
          .split("/")
          .slice(-2)
          .join("/")
      : (s.image ?? "(no source)");

  return (
    <button
      type="button"
      onClick={props.onClick}
      class="bg-white border border-gray-200 rounded-lg p-5 hover:border-indigo-200 hover:shadow-sm transition-all text-left w-full cursor-pointer outline-none focus:outline-none group"
    >
      <div class="flex items-start justify-between gap-4">
        <div class="min-w-0">
          <div class="flex items-center gap-2.5 mb-1">
            <span class="text-base font-semibold text-gray-900 truncate group-hover:text-indigo-600 transition-colors">
              {s.name}
            </span>
          </div>
          <p class="text-sm text-gray-500 truncate">{sourceName}</p>
        </div>
        <div class="shrink-0">
          <StatusBadge status={status} />
        </div>
      </div>
    </button>
  );
}

function ServicesPage() {
  const [services] = createResource(() => (import.meta.env.SSR ? null : true), getServices);
  const [selected, setSelected] = createSignal<Service | null>(null);

  return (
    <div class="min-h-screen bg-[#fafafa]">
      <header class="bg-white border-b border-gray-200">
        <div class="max-w-5xl mx-auto px-6 h-14 flex items-center">
          <div class="flex items-center gap-2.5">
            <div class="size-7 rounded-lg bg-indigo-500 flex items-center justify-center">
              <Monitor class="size-4 text-white" />
            </div>
            <span class="text-sm font-semibold text-gray-900 tracking-tight">Maestro</span>
          </div>
        </div>
      </header>

      <main class="max-w-5xl mx-auto px-6 py-8">
        <Show
          when={!import.meta.env.SSR}
          fallback={<div class="text-sm text-gray-400 py-20 text-center">Loading services…</div>}
        >
          <Suspense
            fallback={<div class="text-sm text-gray-400 py-20 text-center">Loading services…</div>}
          >
            <Show when={services()}>
              {(list) => (
                <>
                  <div class="flex items-baseline gap-2 mb-6">
                    <h1 class="text-xl font-semibold text-gray-900">Services</h1>
                    <span class="text-sm text-gray-400">{list().length}</span>
                  </div>
                  <Show
                    when={list().length > 0}
                    fallback={
                      <div class="text-center py-20">
                        <Rocket class="size-10 text-gray-300 mx-auto mb-3" />
                        <p class="text-sm text-gray-400">No services configured yet.</p>
                      </div>
                    }
                  >
                    <div class="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
                      <For each={list()}>
                        {(service) => (
                          <ServiceCard service={service} onClick={() => setSelected(service)} />
                        )}
                      </For>
                    </div>
                  </Show>
                </>
              )}
            </Show>
          </Suspense>
        </Show>
      </main>

      <Show when={selected()}>
        {(service) => <ServiceSheet service={service()} onClose={() => setSelected(null)} />}
      </Show>
    </div>
  );
}
