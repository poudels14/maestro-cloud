import { createMemo, For } from "solid-js";
import { useQuery } from "@tanstack/solid-query";
import { useLocation, useNavigate } from "@tanstack/solid-router";
import { clusterNodesQuery, servicesQuery } from "../../lib/queries";
import { LogViewer } from "../logs/LogViewer";

type ClusterLogsSearch = {
  node?: string;
  service?: string;
  query?: string;
  range?: string;
};

function ClusterLogsSection() {
  const nodes = useQuery(() => clusterNodesQuery());
  const services = useQuery(() => servicesQuery());
  const location = useLocation();
  const navigate = useNavigate();
  const search = () => location().search as ClusterLogsSearch;
  const sortedServices = createMemo(() =>
    [...(services.data ?? [])].sort((left, right) => left.name.localeCompare(right.name))
  );
  const scope = () => {
    const service = search().service;
    return service ? `service:${service}` : "";
  };
  const nodeId = () => {
    const node = search().node;
    return node ? Number(node) : undefined;
  };
  const setUrlSearch = (updates: ClusterLogsSearch) =>
    navigate({
      to: "/cluster/logs",
      search: { ...search(), ...updates },
      replace: true
    });

  return (
    <div class="h-full min-h-0 flex flex-col gap-3">
      <div class="shrink-0 flex flex-wrap items-end justify-between gap-3">
        <div>
          <h1 class="text-lg font-semibold text-gray-900">Cluster logs</h1>
          <p class="mt-1 text-xs text-gray-500">Logs from every service across all live nodes.</p>
        </div>
        <div class="flex flex-wrap items-end gap-2">
          <label class="grid gap-1 text-[11px] font-medium text-gray-500">
            Node
            <select
              value={search().node ?? ""}
              onChange={(event) => setUrlSearch({ node: event.currentTarget.value || undefined })}
              class="min-w-44 rounded-md border border-gray-200 bg-white px-2.5 py-1.5 text-xs font-normal text-gray-700 outline-none focus:border-indigo-300 focus:ring-2 focus:ring-indigo-100"
            >
              <option value="">All nodes</option>
              <For each={nodes.data ?? []}>
                {(node) => (
                  <option value={node.id} disabled={!node.available}>
                    {node.name}
                    {node.available ? "" : " (offline)"}
                  </option>
                )}
              </For>
            </select>
          </label>
          <label class="grid gap-1 text-[11px] font-medium text-gray-500">
            Service
            <select
              value={search().service ?? ""}
              onChange={(event) =>
                setUrlSearch({ service: event.currentTarget.value || undefined })
              }
              class="min-w-48 rounded-md border border-gray-200 bg-white px-2.5 py-1.5 text-xs font-normal text-gray-700 outline-none focus:border-indigo-300 focus:ring-2 focus:ring-indigo-100"
            >
              <option value="">All services</option>
              <For each={sortedServices()}>
                {(service) => <option value={service.id}>{service.name}</option>}
              </For>
            </select>
          </label>
        </div>
      </div>
      <div class="min-h-0 flex-1">
        <LogViewer
          scope={scope()}
          nodeId={nodeId()}
          showHost
          showHistogram
          fillHeight
          query={search().query ?? ""}
          onQueryChange={(value) => setUrlSearch({ query: value || undefined })}
          range={search().range}
          onRangeChange={(value) => setUrlSearch({ range: value === "1h" ? undefined : value })}
        />
      </div>
    </div>
  );
}

export { ClusterLogsSection };
