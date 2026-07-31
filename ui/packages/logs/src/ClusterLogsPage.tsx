import { createMemo, For } from "solid-js";
import { useLocation, useNavigate } from "@tanstack/solid-router";
import { mergeDefinedProperties, useQuery } from "@maestro/sdk";
import type { LogsApi } from "./api";
import { clusterLogNodeLabel, type ClusterLogNodeRef } from "./clusterLogNode";
import { LogViewer } from "./LogViewer";

interface ClusterLogsNode extends ClusterLogNodeRef {
  nodeId: string;
  alive: boolean;
}

interface ClusterLogsService {
  id: string;
  name: string;
}

interface ClusterLogsLoaders {
  listNodes: () => Promise<ClusterLogsNode[]>;
  listServices: () => Promise<ClusterLogsService[]>;
}

interface ClusterLogsSearch {
  node?: string | undefined;
  service?: string | undefined;
  query?: string | undefined;
  range?: string | undefined;
}

interface ClusterLogsPageProps extends ClusterLogsLoaders {
  api: LogsApi;
}

const isServer = typeof window === "undefined";
const ssrSafeList = <Value,>(loader: () => Promise<Value[]>) =>
  isServer ? () => Promise.resolve([] as Value[]) : loader;

function ClusterLogsPage(props: ClusterLogsPageProps) {
  const nodes = useQuery(() => ({
    queryKey: ["logs", "cluster", "nodes"],
    queryFn: ssrSafeList(props.listNodes),
    refetchInterval: 5_000
  }));
  const services = useQuery(() => ({
    queryKey: ["logs", "cluster", "services"],
    queryFn: ssrSafeList(props.listServices),
    refetchInterval: 15_000,
    refetchOnWindowFocus: true,
    staleTime: 5_000
  }));
  const location = useLocation();
  const navigate = useNavigate();
  const search = () => location().search as ClusterLogsSearch;
  const sortedServices = createMemo(() =>
    [...(services.data ?? [])].sort((left, right) => left.name.localeCompare(right.name))
  );
  const setUrlSearch = (updates: ClusterLogsSearch) =>
    navigate({
      to: "/cluster/logs",
      search: mergeDefinedProperties(search(), updates),
      replace: true
    });
  const clusterSelection = () => {
    const nodeId = search().node;
    return nodeId ? { nodeId } : {};
  };

  return (
    <div class="h-full min-h-0 flex flex-col gap-3">
      <div class="shrink-0 flex flex-wrap items-end justify-between gap-3">
        <div>
          <h1 class="text-lg font-semibold text-gray-900">Cluster logs</h1>
        </div>
        <div class="flex flex-wrap items-end gap-2">
          <label class="grid gap-1 text-[11px] font-medium text-gray-500">
            Node
            <select
              value={search().node ?? ""}
              onChange={(event) => setUrlSearch({ node: event.currentTarget.value || undefined })}
              class="min-w-44 rounded-md border border-gray-200 bg-white px-2.5 py-1.5 text-xs font-normal text-gray-700 outline-none focus:border-brand-border focus:ring-2 focus:ring-brand-ring"
            >
              <option value="" selected={!search().node}>
                All nodes
              </option>
              <For each={nodes.data ?? []}>
                {(node) => (
                  <option
                    value={node.nodeId}
                    disabled={!node.alive}
                    selected={node.nodeId === search().node}
                  >
                    {clusterLogNodeLabel(node)}
                    {node.alive ? "" : " (offline)"}
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
              class="min-w-48 rounded-md border border-gray-200 bg-white px-2.5 py-1.5 text-xs font-normal text-gray-700 outline-none focus:border-brand-border focus:ring-2 focus:ring-brand-ring"
            >
              <option value="" selected={!search().service}>
                All services
              </option>
              <optgroup label="User services">
                <For each={sortedServices()}>
                  {(service) => (
                    <option value={service.id} selected={service.id === search().service}>
                      {service.name}
                    </option>
                  )}
                </For>
              </optgroup>
            </select>
          </label>
        </div>
      </div>
      <div class="min-h-0 flex-1">
        <LogViewer
          api={props.api}
          serviceId={search().service ?? ""}
          deploymentId={null}
          isSystem={false}
          showHistogram
          fillHeight
          cluster={clusterSelection()}
          query={search().query ?? ""}
          onQueryChange={(value) => setUrlSearch({ query: value || undefined })}
          range={search().range ?? "1h"}
          onRangeChange={(value) => setUrlSearch({ range: value === "1h" ? undefined : value })}
        />
      </div>
    </div>
  );
}

export { ClusterLogsPage };
export type { ClusterLogsLoaders, ClusterLogsNode, ClusterLogsService };
