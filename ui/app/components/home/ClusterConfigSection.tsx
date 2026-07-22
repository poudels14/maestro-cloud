import { For, Show } from "solid-js";
import { useQuery } from "../../lib/useQuery";
import type { MaskedConfig } from "../../lib/types";
import { clusterConfigQuery } from "../../lib/queries";
import { ErrorBanner, SectionHeader } from "../../lib/ui";

function ClusterConfigSection() {
  const config = useQuery(() => clusterConfigQuery());

  return (
    <div>
      <SectionHeader class="mb-4">Cluster config</SectionHeader>
      <Show when={config.error}>
        <ErrorBanner message="Failed to load cluster config" onRetry={() => config.refetch()} />
      </Show>
      <Show when={config.data}>
        {(data) => (
          <div class="bg-white rounded-lg border border-gray-200 divide-y divide-gray-100">
            <For each={configRows(data())}>
              {(item) => (
                <div class="px-4 py-2.5 flex items-baseline justify-between gap-6">
                  <span class="text-xs font-medium text-gray-700 shrink-0">{item.label}</span>
                  <span class="text-xs text-gray-600 text-right break-words min-w-0 tabular-nums">
                    {item.value}
                  </span>
                </div>
              )}
            </For>
          </div>
        )}
      </Show>
    </div>
  );
}

function configRows(config: MaskedConfig) {
  const local = config.nodes.find((node) => node.nodeId === config.localNodeId);
  const items: { label: string; value: string }[] = [
    { label: "Cluster name", value: config.name },
    { label: "Cluster ID", value: config.clusterId },
    { label: "Local node", value: local?.hostname ?? config.localNodeId },
    { label: "Node ID", value: config.localNodeId },
    ...(local
      ? [
          { label: "Node role", value: local.role },
          { label: "Host address", value: local.hostAddress },
          { label: "API port", value: String(local.apiPort) },
          { label: "Workload subnet", value: local.workloadSubnet }
        ]
      : []),
    { label: "Cluster members", value: String(config.nodes.length) },
    { label: "Gateway port", value: String(config.ports.gateway) },
    { label: "Store client port", value: String(config.ports.storeClient) },
    { label: "Store peer port", value: String(config.ports.storePeer) },
    { label: "WireGuard port", value: String(config.ports.wireguard) }
  ];
  if (config.controlAllowCidrs.length > 0) {
    items.push({
      label: "Control allow CIDRs",
      value: config.controlAllowCidrs.join(", ")
    });
  }
  return items;
}

export { ClusterConfigSection };
