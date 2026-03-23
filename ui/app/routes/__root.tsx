/// <reference types="vite/client" />
import { HeadContent, Outlet, Scripts, createRootRoute } from "@tanstack/solid-router";
import { createEffect, createResource, onCleanup, Show, Suspense } from "solid-js";
import type { JSX } from "solid-js";
import { HydrationScript } from "solid-js/web";
import { getClusterInfo } from "../lib/api";
import appCss from "../app.css?url";

export const Route = createRootRoute({
  head: () => ({
    links: [
      { rel: "stylesheet", href: appCss },
      { rel: "preconnect", href: "https://fonts.googleapis.com" },
      {
        rel: "preconnect",
        href: "https://fonts.gstatic.com",
        crossorigin: "anonymous"
      },
      {
        rel: "stylesheet",
        href: "https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600&display=swap"
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
  const [clusterInfo, { refetch }] = createResource(
    () => (import.meta.env.SSR ? null : true),
    getClusterInfo
  );
  const isUpgrading = () => clusterInfo()?.upgrading ?? false;

  createEffect(() => {
    if (!isUpgrading()) return;
    const interval = setInterval(() => refetch(), 5000);
    onCleanup(() => clearInterval(interval));
  });

  return (
    <RootDocument>
      <Show when={isUpgrading()}>
        <div class="fixed top-0 left-0 right-0 z-50 bg-amber-50 border-b border-amber-200 px-6 py-2.5 text-center">
          <span class="text-xs font-medium text-amber-700">
            System upgrade in progress — the cluster will reboot shortly
          </span>
        </div>
      </Show>
      <Outlet />
    </RootDocument>
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
