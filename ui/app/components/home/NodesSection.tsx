import { For, Show, createSignal } from "solid-js";
import { useQuery } from "@tanstack/solid-query";
import clsx from "clsx";
import { clusterConfigQuery, clusterInfoQuery, clusterNodesQuery } from "../../lib/queries";
import { setNodeDrain } from "../../lib/api";
import { isCurrentMaster } from "../../lib/clusterLeadership";

function NodesSection() {
  const nodes = useQuery(() => clusterNodesQuery());
  const cluster = useQuery(() => clusterInfoQuery({ pollForMaintenance: true }));
  const config = useQuery(() => clusterConfigQuery());
  const [busy, setBusy] = createSignal<string | null>(null);
  const [error, setError] = createSignal<string | null>(null);

  const changeDrain = async (nodeId: string, drain: boolean) => {
    setBusy(nodeId);
    setError(null);
    try {
      await setNodeDrain(nodeId, drain);
      await nodes.refetch();
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : String(cause));
    } finally {
      setBusy(null);
    }
  };

  return (
    <section class="space-y-8">
      <div>
        <div class="flex items-end justify-between gap-3 mb-3">
          <div>
            <h1 class="text-lg font-semibold text-gray-900">Cluster nodes</h1>
            <p class="text-xs text-gray-500 mt-1">Control-plane health and workload placement.</p>
          </div>
          <span class="text-xs text-gray-400 tabular-nums">{nodes.data?.length ?? 0} nodes</span>
        </div>
        <Show when={error()}>
          {(message) => (
            <div class="mb-3 rounded-lg border border-red-200 bg-red-50 px-3 py-2 text-xs text-red-700">
              {message()}
            </div>
          )}
        </Show>
        <Show
          when={
            config.data?.node.role === "master" &&
            (nodes.data?.filter((node) => node.alive).length ?? 0) <
              Object.keys(config.data?.cluster.nodes ?? {}).length
          }
        >
          <div class="mb-3 rounded-lg border border-amber-200 bg-amber-50 px-3 py-3 text-xs text-amber-900">
            <div class="font-medium">Cluster formation is waiting for configured nodes.</div>
            <div class="mt-1 text-amber-800">
              {nodes.data?.filter((node) => node.alive).length ?? 0} of{" "}
              {Object.keys(config.data?.cluster.nodes ?? {}).length} nodes are connected. In the AWS
              security group, allow inbound TCP {config.data?.node["api-port"]},{" "}
              {config.data?.node["gateway-port"]}, {config.data?.node["etcd-client-port"]}, and{" "}
              {config.data?.node["etcd-peer-port"]} from the cluster's private security group, then
              start the remaining nodes.
            </div>
          </div>
        </Show>
        <div class="overflow-hidden rounded-xl border border-gray-200 bg-white">
          <div class="grid grid-cols-[minmax(9rem,1.4fr)_7rem_minmax(8rem,1fr)_minmax(8rem,1fr)_7rem] gap-3 border-b border-gray-200 bg-gray-50 px-4 py-2 text-[11px] font-medium text-gray-500">
            <span>Node</span>
            <span>Role</span>
            <span>Control</span>
            <span>Workload subnet</span>
            <span class="text-right">Action</span>
          </div>
          <For each={nodes.data ?? []}>
            {(node) => (
              <div class="grid grid-cols-[minmax(9rem,1.4fr)_7rem_minmax(8rem,1fr)_minmax(8rem,1fr)_7rem] items-center gap-3 border-b border-gray-100 px-4 py-3 text-xs last:border-b-0">
                <div class="min-w-0">
                  <div class="flex items-center gap-2">
                    <span
                      class={clsx("size-2 rounded-full", {
                        "bg-emerald-500": node.alive && node.dataPlaneReady,
                        "bg-amber-500": node.alive && !node.dataPlaneReady,
                        "bg-gray-300": !node.alive
                      })}
                    />
                    <span class="truncate font-medium text-gray-800">{node.hostname}</span>
                    <Show when={isCurrentMaster(node.nodeId, cluster.data?.leader)}>
                      <span
                        class="shrink-0 rounded-full border border-indigo-200 bg-indigo-50 px-1.5 py-0.5 text-[9px] font-semibold tracking-wide text-indigo-700"
                        title="Current elected master"
                      >
                        CURRENT MASTER
                      </span>
                    </Show>
                  </div>
                  <div class="mt-1 truncate font-mono text-[10px] text-gray-400">{node.nodeId}</div>
                  <Show when={node.state.reason || node.dataPlaneError}>
                    <div class="mt-1 truncate text-[10px] text-amber-600">
                      {node.state.reason || node.dataPlaneError}
                    </div>
                  </Show>
                </div>
                <span class="text-gray-600">{node.role}</span>
                <span class="font-mono text-[11px] text-gray-600">
                  {node.clusterHostIp}:{node.clusterApiPort}
                </span>
                <span class="font-mono text-[11px] text-gray-600">{node.subnet}</span>
                <button
                  type="button"
                  disabled={!node.alive || busy() === node.nodeId}
                  onClick={() => changeDrain(node.nodeId, !node.state.unschedulable)}
                  class={clsx(
                    "justify-self-end rounded-md border px-2 py-1 text-[11px] font-medium disabled:cursor-not-allowed disabled:opacity-50",
                    node.state.unschedulable
                      ? "border-emerald-200 text-emerald-700 hover:bg-emerald-50"
                      : "border-amber-200 text-amber-700 hover:bg-amber-50"
                  )}
                >
                  {node.state.unschedulable ? "Restore" : "Drain"}
                </button>
              </div>
            )}
          </For>
          <Show when={!nodes.isLoading && (nodes.data?.length ?? 0) === 0}>
            <div class="px-4 py-12 text-center text-sm text-gray-400">No cluster nodes found.</div>
          </Show>
        </div>
      </div>
    </section>
  );
}

export { NodesSection };
