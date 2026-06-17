import { For, Show } from "solid-js";
import { useQuery } from "@tanstack/solid-query";
import type { MaskedConfig } from "../../lib/types";
import { clusterConfigQuery } from "../../lib/queries";
import { SectionHeader } from "../../lib/ui";

function ClusterConfigSection() {
  const config = useQuery(() => clusterConfigQuery());

  return (
    <Show when={config.data}>
      {(data) => (
        <div>
          <SectionHeader class="mb-4 mt-10">Cluster config</SectionHeader>
          <div class="bg-white rounded-lg border border-gray-200 divide-y divide-gray-100">
            <For each={configRows(data())}>
              {(item) => (
                <div class="px-4 py-2.5 flex items-baseline justify-between gap-6">
                  <span class="text-xs text-gray-500 shrink-0">{item.label}</span>
                  <span class="text-sm font-mono text-gray-800 text-right break-words min-w-0">
                    {item.value}
                  </span>
                </div>
              )}
            </For>
          </div>
        </div>
      )}
    </Show>
  );
}

function configRows(config: MaskedConfig) {
  const items: { label: string; value: string }[] = [
    { label: "Ingress ports", value: (config.ingress?.ports ?? []).join(", ") },
    { label: "Runtime", value: config.runtime },
    { label: "Builder", value: config.depot ? "depot" : "default" }
  ];
  const advertiseRoutes = config.tailscale?.["advertise-routes"] ?? [];
  if (advertiseRoutes.length > 0) {
    items.push({ label: "Tailscale routes", value: advertiseRoutes.join(", ") });
  }
  const egressDeny = config.egress?.deny ?? [];
  if (egressDeny.length > 0) {
    items.push({ label: "Egress deny", value: egressDeny.join(", ") });
  }
  const egressAllow = config.egress?.allow ?? [];
  if (egressAllow.length > 0) {
    items.push({ label: "Egress allow", value: egressAllow.join(", ") });
  }
  if (config.datadog?.site) {
    items.push({ label: "Datadog site", value: config.datadog.site });
  }
  return items;
}

export { ClusterConfigSection };
