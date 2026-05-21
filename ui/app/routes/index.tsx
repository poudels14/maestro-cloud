import { createFileRoute } from "@tanstack/solid-router";
import { useQuery } from "@tanstack/solid-query";
import { Show } from "solid-js";
import { Monitor } from "lucide-solid";
import { clusterInfoQuery } from "../lib/queries";
import { ClientOnly } from "../components/ClientOnly";
import { SlackWebhooks } from "../components/SlackWebhooks";
import { NodeMetricsSection } from "../components/home/NodeMetricsSection";
import { DisksSection } from "../components/home/DisksSection";
import { ServicesGrid } from "../components/home/ServicesGrid";

export const Route = createFileRoute("/")({
  component: HomePage
});

function HomePage() {
  return (
    <div class="min-h-screen bg-[#fafafa]">
      <HomeHeader />
      <main class="max-w-5xl mx-auto px-6 py-8">
        <ClientOnly fallback={<div class="text-sm text-gray-400 py-20 text-center">Loading…</div>}>
          <div class="mb-10 space-y-6">
            <NodeMetricsSection />
            <DisksSection />
          </div>
          <ServicesGrid />
          <SlackWebhooks />
        </ClientOnly>
      </main>
    </div>
  );
}

function HomeHeader() {
  const cluster = useQuery(() => clusterInfoQuery());
  return (
    <header class="bg-white border-b border-gray-200">
      <div class="max-w-5xl mx-auto px-6 h-14 flex items-center justify-between">
        <div class="flex items-center gap-2.5">
          <div class="size-7 rounded-lg bg-indigo-500 flex items-center justify-center">
            <Monitor class="size-4 text-white" />
          </div>
          <span class="text-sm font-semibold text-gray-900 tracking-tight">Maestro</span>
        </div>
        <Show when={cluster.data}>
          {(info) => (
            <div class="flex items-center gap-4 text-xs font-mono">
              <span class="text-gray-400">{info().clusterName}</span>
              <a
                href={`http://${info().canonicalDomain}`}
                title="Canonical domain"
                class="text-gray-500 hover:text-gray-700 no-underline"
              >
                {info().canonicalDomain}
              </a>
              <a
                href={`http://${info().aliasDomain}`}
                title="Alias domain"
                class="text-gray-300 hover:text-gray-500 no-underline"
              >
                {info().aliasDomain}
              </a>
            </div>
          )}
        </Show>
      </div>
    </header>
  );
}
