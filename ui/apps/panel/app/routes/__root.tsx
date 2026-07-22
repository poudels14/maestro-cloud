/// <reference types="vite/client" />
import { HeadContent, Outlet, Scripts, createRootRoute } from "@tanstack/solid-router";
import { QueryClientProvider } from "@tanstack/solid-query";
import { useQuery } from "@maestro/sdk";
import { createSignal, Show, Suspense } from "solid-js";
import type { JSX } from "solid-js";
import { HydrationScript } from "solid-js/web";
import { Loader2, X } from "lucide-solid";
import { queryClient } from "../lib/queryClient";
import {
  activeMaintenanceNode,
  clusterInfoQuery,
  maintenanceStageLabel,
  unschedulableQuery
} from "@maestro/cluster";
import { clusterApi } from "../features";
import { ClientOnly } from "../components/ClientOnly";
import { AppToasts } from "../components/AppToasts";
import "../app.css";

export const Route = createRootRoute({
  ssr: true,
  head: () => ({
    links: [
      { rel: "preconnect", href: "https://fonts.googleapis.com" },
      {
        rel: "preconnect",
        href: "https://fonts.gstatic.com",
        crossorigin: "anonymous"
      },
      {
        rel: "stylesheet",
        href: "https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600&family=JetBrains+Mono:wght@400;500&display=swap"
      }
    ],
    meta: [
      { charset: "utf-8" },
      { name: "viewport", content: "width=device-width, initial-scale=1" },
      { title: "Maestro" }
    ]
  }),
  component: RootComponent,
  notFoundComponent: () => (
    <div class="min-h-screen bg-gray-50 flex items-center justify-center">
      <div class="text-center">
        <h1 class="text-2xl font-semibold text-gray-900">Not found</h1>
        <p class="text-sm text-gray-500 mt-1">This page doesn't exist.</p>
      </div>
    </div>
  )
});

function RootComponent() {
  return (
    <QueryClientProvider client={queryClient}>
      <RootDocument>
        <ClientOnly>
          <AppToasts />
          <MaintenanceBanner />
          <SchedulingBanner />
        </ClientOnly>
        <Outlet />
      </RootDocument>
    </QueryClientProvider>
  );
}

function SchedulingBanner() {
  const scheduling = useQuery(() => unschedulableQuery(clusterApi));
  const count = () => scheduling.data?.length ?? 0;
  return (
    <Show when={count() > 0}>
      <div class="fixed bottom-4 left-1/2 z-40 -translate-x-1/2 rounded-lg border border-amber-200 bg-amber-50 px-4 py-2.5 shadow-lg">
        <span class="text-xs font-medium text-amber-800">
          {count()} replica{count() === 1 ? " is" : "s are"} unschedulable — check the Nodes page
          for placement constraints.
        </span>
      </div>
    </Show>
  );
}

function MaintenanceBanner() {
  const cluster = useQuery(() => clusterInfoQuery(clusterApi, { pollForMaintenance: true }));
  const activeRun = () => cluster.data?.activeUpgrade ?? null;
  const activeNode = () => activeMaintenanceNode(activeRun(), cluster.data?.nodes);
  const stageLabel = () => maintenanceStageLabel(activeRun());
  const [dismissedRunId, setDismissedRunId] = createSignal<string | null>(null);

  return (
    <Show when={activeRun() && dismissedRunId() !== activeRun()?.meta.id}>
      <div class="fixed bottom-4 left-1/2 z-50 flex -translate-x-1/2 items-center gap-2.5 whitespace-nowrap rounded-lg border border-amber-200 bg-amber-50 py-2.5 pl-4 pr-2.5 shadow-lg">
        <Loader2 class="size-3.5 shrink-0 animate-spin text-amber-500" />
        <span class="text-xs font-medium text-amber-700">
          {activeRun()?.spec.mode === "allNodes" ? "All-node" : "Rolling"} cluster upgrade
          <Show when={activeNode()}>
            {(node) => (
              <>
                {" — "}
                <span class="font-mono font-semibold">{node().label}</span>
                <Show when={stageLabel()}>{(stage) => <>: {stage()}</>}</Show>
              </>
            )}
          </Show>{" "}
          — deploys are frozen
        </span>
        <button
          type="button"
          onClick={() => setDismissedRunId(activeRun()?.meta.id ?? null)}
          aria-label="Dismiss maintenance notice"
          class="rounded p-1 text-amber-400 outline-none hover:bg-amber-100 hover:text-amber-600"
        >
          <X class="size-3.5" />
        </button>
      </div>
    </Show>
  );
}

function RootDocument({ children }: { children: JSX.Element }) {
  return (
    <html lang="en">
      <head>
        <HydrationScript />
        <HeadContent />
      </head>
      <body>
        <Suspense>{children}</Suspense>
        <Scripts />
      </body>
    </html>
  );
}
