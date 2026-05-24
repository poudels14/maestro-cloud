import { createFileRoute, Link } from "@tanstack/solid-router";
import { useQuery } from "@tanstack/solid-query";
import { Show } from "solid-js";
import { Crown, Monitor, Network } from "lucide-solid";
import { clusterInfoQuery } from "../lib/queries";
import { ClientOnly } from "../components/ClientOnly";
import { SlackWebhooks } from "../components/SlackWebhooks";
import { NodeMetricsSection } from "../components/home/NodeMetricsSection";
import { DisksSection } from "../components/home/DisksSection";
import { ServicesGrid } from "../components/home/ServicesGrid";
import { ClusterHealthBanner } from "../components/cluster/ClusterHealthBanner";

export const Route = createFileRoute("/")({
  component: HomePage
});

function HomePage() {
  return (
    <div class="min-h-screen bg-[#fafafa]">
      <HomeHeader />
      <main class="max-w-5xl mx-auto px-4 sm:px-6 py-6 sm:py-8">
        <ClientOnly fallback={<div class="text-sm text-gray-400 py-20 text-center">Loading…</div>}>
          <div class="mb-6">
            <ClusterHealthBanner />
          </div>
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
      <div class="max-w-5xl mx-auto px-4 sm:px-6 py-3 sm:h-14 sm:py-0 flex flex-col sm:flex-row sm:items-center sm:justify-between gap-2 sm:gap-3">
        <div class="flex items-center gap-2.5 shrink-0">
          <div class="size-7 rounded-lg bg-indigo-500 flex items-center justify-center">
            <Monitor class="size-4 text-white" />
          </div>
          <span class="text-sm font-semibold text-gray-900 tracking-tight">Maestro</span>
        </div>
        <Show when={cluster.data}>
          {(info) => (
            <div class="flex flex-wrap items-center gap-x-3 sm:gap-x-4 gap-y-0.5 text-xs font-mono min-w-0 overflow-hidden">
              <span class="text-gray-400 truncate">{info().clusterName}</span>
              <Show when={info().nodes && info().nodes!.length > 0}>
                <Link
                  to="/cluster"
                  class="inline-flex items-center gap-1 text-indigo-600 hover:text-indigo-800 no-underline whitespace-nowrap"
                  title="Cluster nodes"
                >
                  <Network class="size-3" />
                  {info().nodes!.length} node{info().nodes!.length === 1 ? "" : "s"}
                </Link>
              </Show>
              <Show when={info().leader}>
                {(leader) => (
                  <span
                    class="inline-flex items-center gap-1 text-amber-700 whitespace-nowrap"
                    title="Scheduling leader"
                  >
                    <Crown class="size-3" />
                    {leader().nodeId}
                  </span>
                )}
              </Show>
              <a
                href={`http://${info().canonicalDomain}`}
                title="Canonical domain"
                class="text-gray-500 hover:text-gray-700 no-underline truncate"
              >
                {info().canonicalDomain}
              </a>
              <a
                href={`http://${info().aliasDomain}`}
                title="Alias domain"
                class="text-gray-400 hover:text-gray-600 no-underline truncate"
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
