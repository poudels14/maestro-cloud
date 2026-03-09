import { createFileRoute, useNavigate } from "@tanstack/solid-router";
import { createResource, For, Show, Suspense } from "solid-js";
import { Monitor, Rocket } from "lucide-solid";
import type { Service } from "../lib/types";
import { getServices } from "../lib/api";
import { StatusBadge } from "../lib/ui";

export const Route = createFileRoute("/")({
  component: ServicesPage
});

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
  const navigate = useNavigate();

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
                          <ServiceCard
                            service={service}
                            onClick={() =>
                              navigate({
                                to: "/services/$serviceId/$tab",
                                params: { serviceId: service.id, tab: "overview" }
                              })
                            }
                          />
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
    </div>
  );
}
