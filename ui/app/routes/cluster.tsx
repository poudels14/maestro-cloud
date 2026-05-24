import { createFileRoute, Link } from "@tanstack/solid-router";
import { useQuery } from "@tanstack/solid-query";
import { For, Show } from "solid-js";
import { Crown, Network, Pause, Server } from "lucide-solid";
import { clusterInfoQuery } from "../lib/queries";
import { ClientOnly } from "../components/ClientOnly";
import { ClusterHealthBanner } from "../components/cluster/ClusterHealthBanner";
import { ClusterNodeRow } from "../components/cluster/ClusterNodeRow";
import { ClusterMetricsPanel } from "../components/cluster/ClusterMetricsPanel";
import { ClusterUpgradePanel } from "../components/cluster/ClusterUpgradePanel";
import { ClusterTopology } from "../components/cluster/ClusterTopology";

export const Route = createFileRoute("/cluster")({
  component: ClusterPage
});

function ClusterPage() {
  const cluster = useQuery(() => clusterInfoQuery({ live: true }));

  return (
    <div class="min-h-screen bg-[#fafafa]">
      <header class="bg-white border-b border-gray-200">
        <div class="max-w-5xl mx-auto px-4 sm:px-6 py-3 sm:h-14 sm:py-0 flex items-center justify-between">
          <div class="flex items-center gap-3">
            <Link to="/" class="text-sm text-gray-500 hover:text-gray-900 no-underline">
              ← Home
            </Link>
            <span class="text-sm font-semibold text-gray-900">Cluster</span>
          </div>
          <Show when={cluster.data}>
            {(info) => (
              <span class="text-xs font-mono text-gray-500 truncate">{info().clusterName}</span>
            )}
          </Show>
        </div>
      </header>

      <main class="max-w-5xl mx-auto px-4 sm:px-6 py-6 sm:py-8 space-y-8">
        <ClientOnly fallback={<div class="text-sm text-gray-400 py-12 text-center">Loading…</div>}>
          <ClusterHealthBanner />
          <NodesSection />
          <ClusterTopology />
          <ClusterUpgradePanel />
          <ClusterMetricsPanel />
        </ClientOnly>
      </main>
    </div>
  );
}

function NodesSection() {
  const cluster = useQuery(() => clusterInfoQuery({ live: true }));
  return (
    <section>
      <SectionHeader icon={<Server class="size-4" />} title="Nodes">
        <Show when={cluster.data?.nodes?.length}>
          <span class="text-xs text-gray-500">{cluster.data!.nodes!.length} total</span>
        </Show>
      </SectionHeader>
      <Show
        when={(cluster.data?.nodes?.length ?? 0) > 0}
        fallback={
          <div class="text-sm text-gray-400 py-12 text-center bg-white border border-gray-200 rounded-lg">
            No nodes registered — running in single-node mode.
          </div>
        }
      >
        <div class="bg-white border border-gray-200 rounded-lg divide-y divide-gray-200 overflow-hidden">
          <For each={cluster.data!.nodes!}>
            {(node) => (
              <ClusterNodeRow
                node={node}
                isLeader={cluster.data?.leader?.nodeId === node.nodeId}
                isSelf={cluster.data?.thisNodeId === node.nodeId}
              />
            )}
          </For>
        </div>
        <Show when={cluster.data?.leader}>
          {(leader) => (
            <div class="mt-3 flex items-center gap-2 text-xs text-gray-500">
              <Crown class="size-3.5 text-amber-500" />
              <span>
                Scheduling leader: <span class="font-mono text-gray-700">{leader().nodeId}</span>
              </span>
            </div>
          )}
        </Show>
      </Show>
    </section>
  );
}

function SectionHeader(props: {
  icon: any;
  title: string;
  children?: any;
}) {
  return (
    <div class="flex items-center justify-between mb-3">
      <div class="flex items-center gap-2 text-sm font-semibold text-gray-900">
        <span class="text-gray-500">{props.icon}</span>
        {props.title}
      </div>
      <div>{props.children}</div>
    </div>
  );
}

// reference icons so they're tree-shaken correctly even if unused above
const _icons = { Network, Pause };
void _icons;
