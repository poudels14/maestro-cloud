import { For, Show } from "solid-js";
import { useQuery } from "@tanstack/solid-query";
import type { ClusterConfig } from "../../lib/types";
import { clusterConfigQuery } from "../../lib/queries";
import { SectionHeader } from "../../lib/ui";

function ClusterConfigSection() {
  const config = useQuery(() => clusterConfigQuery());

  return (
    <Show when={config.data}>
      {(data) => (
        <div>
          <SectionHeader class="mb-4">Cluster config</SectionHeader>
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
        </div>
      )}
    </Show>
  );
}

function configRows(config: ClusterConfig) {
  const enabled = (value: unknown) => (value ? "enabled" : "disabled");
  const rows: [string, string | null | undefined][] = [
    ["Cluster network", config.cluster.cidr],
    ["Ingress ports", config.ingress.ports.join(", ")],
    [
      "Default limits",
      `${config.defaults.resources.cpu} CPU, ${config.defaults.resources.memory} memory`
    ],
    [
      "Default requests",
      `${config.defaults.resources.request.cpu} CPU, ${config.defaults.resources.request.memory} memory`
    ],
    ["Builder", config.depot ? "Depot" : "BuildKit"],
    ["CLI deployments", enabled(config.allowCliDeployment)],
    ["Exec", enabled(config.allowExec)],
    ["Homepage", config.homepage],
    ["Tags", config.tags?.join(", ")],
    ["Egress deny", config.egress.deny?.join(", ")],
    ["Egress allow", config.egress.allow?.join(", ")],
    ["Tailscale", config.tailscale ? "enabled" : null],
    ["Tailscale routes", config.tailscale?.advertiseRoutes?.join(", ")],
    [
      "Cloudflare tunnel",
      config.cloudflare ? `${config.cloudflare.tunnel.replicas ?? 1} connectors` : null
    ],
    ["PR previews", config.github ? `*.${config.github.previewDomain}` : null],
    ["Datadog site", config.datadog ? (config.datadog.site ?? "datadoghq.com") : null],
    [
      "Datadog health-check logs",
      config.datadog
        ? config.datadog.logs?.includeHealthcheck
          ? "included"
          : "excluded on success"
        : null
    ],
    ["S3 log backup", config.logBackup ? `s3://${config.logBackup.bucket}` : null]
  ];
  return rows
    .filter((row): row is [string, string] => !!row[1])
    .map(([label, value]) => ({ label, value }));
}

export { ClusterConfigSection };
