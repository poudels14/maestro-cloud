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
} from "lucide-solid";
import type { Deployment, Service } from "../lib/types";

export const Route = createFileRoute("/")({
  component: ServicesPage,
});

const controllerHost = import.meta.env.VITE_MAESTRO_CONTROLLER_HOST;

async function getServices(): Promise<Service[]> {
  const url = import.meta.env.SSR ? `${controllerHost}/api/services` : "/api/services";
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Failed to fetch services: ${res.statusText}`);
  return res.json();
}

async function getDeployments(serviceId: string): Promise<Deployment[]> {
  const url = import.meta.env.SSR
    ? `${controllerHost}/api/service/${serviceId}/deployments`
    : `/api/service/${serviceId}/deployments`;
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Failed to fetch deployments: ${res.statusText}`);
  return res.json();
}

async function cancelDeployment(serviceId: string, deploymentId: string) {
  const url = import.meta.env.SSR
    ? `${controllerHost}/api/service/${serviceId}/deployment/${deploymentId}/cancel`
    : `/api/service/${serviceId}/deployment/${deploymentId}/cancel`;
  const res = await fetch(url, { method: "POST" });
  if (!res.ok) throw new Error(`Failed to cancel deployment: ${res.statusText}`);
}

const STATUS_COLORS: Record<string, { dot: string; bg: string; text: string }> = {
  QUEUED: { dot: "bg-amber-400", bg: "bg-amber-50", text: "text-amber-700" },
  BUILDING: { dot: "bg-blue-400", bg: "bg-blue-50", text: "text-blue-700" },
  DEPLOYING: { dot: "bg-indigo-400", bg: "bg-indigo-50", text: "text-indigo-700" },
  RUNNING: { dot: "bg-emerald-400", bg: "bg-emerald-50", text: "text-emerald-700" },
  FAILED: { dot: "bg-red-400", bg: "bg-red-50", text: "text-red-700" },
  STOPPED: { dot: "bg-gray-400", bg: "bg-gray-100", text: "text-gray-600" },
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

function DeploymentMenu(props: { onCancel: () => void }) {
  const [open, setOpen] = createSignal(false);
  let menuRef: HTMLDivElement | undefined;

  const handleClickOutside = (e: MouseEvent) => {
    if (menuRef && !menuRef.contains(e.target as Node)) {
      setOpen(false);
    }
  };

  onMount(() => document.addEventListener("click", handleClickOutside));
  onCleanup(() => document.removeEventListener("click", handleClickOutside));

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
          <button
            type="button"
            onClick={() => {
              setOpen(false);
              props.onCancel();
            }}
            class="w-full text-left px-4 py-2.5 text-sm text-red-600 hover:bg-red-50 flex items-center gap-2.5 outline-none"
          >
            <Ban class="size-4" />
            Cancel deployment
          </button>
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
              const isLatest = i() === 0;
              return (
                <div
                  class={`rounded-lg border p-4 transition-colors ${
                    isLatest
                      ? "border-indigo-200 bg-indigo-50/30"
                      : "border-gray-200 bg-white hover:bg-gray-50/50"
                  }`}
                >
                  <div class="flex items-center justify-between gap-3 mb-2">
                    <div class="flex items-center gap-2.5 min-w-0">
                      <span
                        class={`text-sm font-semibold font-mono ${isLatest ? "text-indigo-700" : "text-gray-900"}`}
                      >
                        #{shortId}
                      </span>
                      <Show when={isLatest}>
                        <span class="text-[10px] font-medium uppercase tracking-wider text-indigo-500 bg-indigo-100 px-1.5 py-0.5 rounded">
                          latest
                        </span>
                      </Show>
                    </div>
                    <div class="flex items-center gap-2 shrink-0">
                      <StatusBadge status={d.status} />
                      <DeploymentMenu
                        onCancel={async () => {
                          await cancelDeployment(props.serviceId, d.id);
                          refetch();
                        }}
                      />
                    </div>
                  </div>
                  <div class="flex items-center gap-3 text-xs text-gray-400">
                    <span class="font-mono">{d.config.version}</span>
                    <Show when={d.gitCommit}>
                      <span class="flex items-center gap-1">
                        <GitCommitHorizontal class="size-3" />
                        <span class="font-mono truncate">{d.gitCommit}</span>
                      </span>
                    </Show>
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
    { label: "Version", value: s.version },
  ];

  const buildItems = [
    { label: "Git repository", value: s.build.gitRepo },
    { label: "Dockerfile", value: s.build.dockerfilePath },
    {
      label: "Build command",
      value: `${s.build.command.command} ${s.build.command.args.join(" ")}`.trim(),
    },
  ];

  const deployItems = [
    {
      label: "Deploy command",
      value: `${s.deploy.command.command} ${s.deploy.command.args.join(" ")}`.trim(),
    },
    { label: "Healthcheck path", value: s.deploy.healthcheckPath },
  ];

  return (
    <div class="space-y-6">
      <ConfigSection title="General" items={configItems} />
      <ConfigSection title="Build" items={buildItems} />
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
              <span class="text-xs font-mono bg-indigo-50 text-indigo-600 px-1.5 py-0.5 rounded">
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
  const repoName = s.build.gitRepo
    .replace(/\.git$/, "")
    .split("/")
    .slice(-2)
    .join("/");

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
            <span class="text-xs font-mono bg-gray-100 text-gray-500 px-1.5 py-0.5 rounded group-hover:bg-indigo-50 group-hover:text-indigo-500 transition-colors">
              {s.version}
            </span>
          </div>
          <p class="text-sm text-gray-500 truncate">{repoName}</p>
        </div>
        <div class="shrink-0">
          <span class="inline-flex items-center gap-1.5 text-xs font-medium text-gray-500 bg-gray-100 px-2.5 py-1 rounded-full">
            <span class="size-1.5 rounded-full bg-gray-400 inline-block" />
            idle
          </span>
        </div>
      </div>

      <div class="mt-4 pt-4 border-t border-gray-100 flex items-center gap-4 text-xs text-gray-400">
        <span class="flex items-center gap-1.5">
          <FileText class="size-3.5" />
          <span class="font-mono">{s.build.dockerfilePath}</span>
        </span>
        <span class="flex items-center gap-1.5">
          <HeartPulse class="size-3.5" />
          <span class="font-mono">{s.deploy.healthcheckPath}</span>
        </span>
      </div>
    </button>
  );
}

function ServicesPage() {
  const [services] = createResource(getServices);
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
      </main>

      <Show when={selected()}>
        {(service) => <ServiceSheet service={service()} onClose={() => setSelected(null)} />}
      </Show>
    </div>
  );
}
