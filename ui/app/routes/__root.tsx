/// <reference types="vite/client" />
import { HeadContent, Outlet, Scripts, createRootRoute } from "@tanstack/solid-router";
import { QueryClientProvider, useQuery } from "@tanstack/solid-query";
import { createSignal, createEffect, Show, Suspense } from "solid-js";
import type { JSX } from "solid-js";
import { HydrationScript } from "solid-js/web";
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

  createEffect(() => {
    if (!isMaintaining()) setDismissed(false);
  });

  return (
    <Show when={isMaintaining() && !dismissed()}>
      <div class="fixed top-0 left-0 right-0 z-50 bg-amber-50 border-b border-amber-200 px-6 py-2.5 flex items-center justify-center">
        <span class="text-xs font-medium text-amber-700">
          Rolling cluster {isRestarting() ? "restart" : "upgrade"} in progress — deploys are frozen
          while nodes drain and restart
        </span>
        <button
          type="button"
          onClick={() => setDismissed(true)}
          class="absolute right-4 text-amber-400 hover:text-amber-600"
        >
          <svg
            class="size-4"
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            stroke-width="2"
          >
            <path d="M18 6L6 18M6 6l12 12" />
          </svg>
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
