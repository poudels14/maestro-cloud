/// <reference types="vite/client" />
import { HeadContent, Outlet, Scripts, createRootRoute } from "@tanstack/solid-router";
import { QueryClientProvider } from "@tanstack/solid-query";
import { useQuery } from "../lib/useQuery";
import { createSignal, createEffect, onCleanup, Show, Suspense } from "solid-js";
import type { JSX } from "solid-js";
import { HydrationScript } from "solid-js/web";
import { Loader2, X } from "lucide-solid";
import { queryClient } from "../lib/queryClient";
import { clusterInfoQuery, unschedulableQuery } from "../lib/queries";
import { ClientOnly } from "../components/ClientOnly";
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
          <MaintenanceBanner />
          <SchedulingBanner />
        </ClientOnly>
        <Outlet />
      </RootDocument>
    </QueryClientProvider>
  );
}

function SchedulingBanner() {
  const scheduling = useQuery(() => unschedulableQuery());
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
  const cluster = useQuery(() => clusterInfoQuery({ pollForMaintenance: true }));
  const isUpgrading = () => cluster.data?.upgrading ?? false;
  const isRestarting = () => cluster.data?.restarting ?? false;
  const isMaintaining = () => isUpgrading() || isRestarting();
  const [dismissed, setDismissed] = createSignal(false);
  const [nowMs, setNowMs] = createSignal(Date.now());

  createEffect(() => {
    if (!isMaintaining()) {
      setDismissed(false);
    } else {
      setNowMs(Date.now());
      const timer = setInterval(() => setNowMs(Date.now()), 1_000);
      onCleanup(() => clearInterval(timer));
    }
  });

  const elapsedLabel = () => {
    const startedAtMs = cluster.data?.upgradeRun?.requestedAtMs;
    if (!startedAtMs) return null;
    return formatElapsed(nowMs() - startedAtMs);
  };

  return (
    <Show when={isMaintaining() && !dismissed()}>
      <div class="fixed bottom-4 left-1/2 z-50 flex -translate-x-1/2 items-center gap-2.5 whitespace-nowrap rounded-lg border border-amber-200 bg-amber-50 py-2.5 pl-4 pr-2.5 shadow-lg">
        <Loader2 class="size-3.5 shrink-0 animate-spin text-amber-500" />
        <span class="text-xs font-medium text-amber-700">
          Rolling cluster {isRestarting() ? "restart" : "upgrade"} in progress — deploys are frozen
          <Show when={elapsedLabel()}>
            {(elapsed) => <span class="ml-1.5 font-mono text-amber-600">{elapsed()}</span>}
          </Show>
        </span>
        <button
          type="button"
          onClick={() => setDismissed(true)}
          aria-label="Dismiss maintenance notice"
          class="rounded p-1 text-amber-400 outline-none hover:bg-amber-100 hover:text-amber-600"
        >
          <X class="size-3.5" />
        </button>
      </div>
    </Show>
  );
}

function formatElapsed(elapsedMs: number) {
  const totalSeconds = Math.max(0, Math.floor(elapsedMs / 1_000));
  const hours = Math.floor(totalSeconds / 3_600);
  const minutes = Math.floor((totalSeconds % 3_600) / 60);
  const seconds = totalSeconds % 60;
  if (hours > 0) return `${hours}h ${minutes}m`;
  if (minutes > 0) return `${minutes}m ${seconds}s`;
  return `${seconds}s`;
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
