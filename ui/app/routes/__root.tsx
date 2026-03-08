/// <reference types="vite/client" />
import { HeadContent, Outlet, Scripts, createRootRoute } from "@tanstack/solid-router";
import { Suspense } from "solid-js";
import type { JSX } from "solid-js";
import { HydrationScript } from "solid-js/web";
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
  return (
    <RootDocument>
      <Outlet />
    </RootDocument>
  );
}

function RootDocument({ children }: { children: JSX.Element }) {
  return (
    <html lang="en">
      <head>
        <HydrationScript />
      </head>
      <body>
        <HeadContent />
        <Suspense>{children}</Suspense>
        <Scripts />
      </body>
    </html>
  );
}
