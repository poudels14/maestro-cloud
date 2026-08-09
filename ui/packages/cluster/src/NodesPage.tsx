import { For, Show, createSignal } from "solid-js";
import { useQuery } from "@maestro/sdk";
import clsx from "clsx";
import type { ClusterApi } from "./api";
import { isCurrentMaster } from "./leadership";
import { clusterInfoQuery } from "./queries";
import type { ClusterNode } from "./types";

const nodeGridClass =
  "grid min-w-[49rem] grid-cols-[minmax(13rem,1.8fr)_7rem_4.25rem_minmax(7.5rem,1fr)_minmax(7.5rem,1fr)_5.5rem] gap-3";

function NodesPage(props: { api: ClusterApi }) {
  const cluster = useQuery(() => clusterInfoQuery(props.api, { pollForMaintenance: true }));
  const [busy, setBusy] = createSignal<string | null>(null);
  const [error, setError] = createSignal<string | null>(null);

  const changeDrain = async (node: ClusterNode, drain: boolean) => {
    setBusy(node.nodeId);
    setError(null);
    try {
      await props.api.setNodeDrain(node, drain);
      await cluster.refetch();
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : String(cause));
    } finally {
      setBusy(null);
    }
  };

  return (
    <section class="space-y-8">
      <div>
        <h1 class="mb-3 text-lg font-semibold text-gray-900">Cluster nodes</h1>
        <Show when={error()}>
          {(message) => (
            <div class="mb-3 rounded-lg border border-red-200 bg-red-50 px-3 py-2 text-xs text-red-700">
              {message()}
            </div>
          )}
        </Show>
        <div class="overflow-x-auto rounded-xl border border-gray-200 bg-white">
          <div
            class={`${nodeGridClass} border-b border-gray-200 bg-gray-50 px-4 py-2 text-[11px] font-medium text-gray-500`}
          >
            <span>Node</span>
            <span>Configured role</span>
            <span>Version</span>
            <span>Address</span>
            <span>Workload network</span>
            <span class="text-right">Action</span>
          </div>
          <For each={cluster.data?.nodes ?? []}>
            {(node) => (
              <div
                class={`${nodeGridClass} items-center border-b border-gray-100 px-4 py-3 text-xs last:border-b-0`}
              >
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
                    <Show when={isCurrentMaster(node.nodeId, cluster.data?.leaderNodeId)}>
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
                      {node.state.reason ?? node.dataPlaneError}
                    </div>
                  </Show>
                </div>
                <span class="text-gray-600">{node.role}</span>
                <span
                  class="min-w-0 truncate font-mono text-[11px] text-gray-600"
                  title={`v${node.version}`}
                >
                  v{node.version}
                </span>
                <span
                  class="min-w-0 truncate font-mono text-[11px] text-gray-600"
                  title={node.hostAddress}
                >
                  {node.hostAddress}
                </span>
                <span class="font-mono text-[11px] text-gray-600">{node.subnet}</span>
                <button
                  type="button"
                  disabled={!node.alive || busy() === node.nodeId}
                  onClick={() =>
                    changeDrain(node, !(node.state.unschedulable || node.state.drainPending))
                  }
                  class={clsx(
                    "justify-self-end rounded-md border px-2 py-1 text-[11px] font-medium disabled:cursor-not-allowed disabled:opacity-50",
                    node.state.unschedulable
                      ? "border-emerald-200 text-emerald-700 hover:bg-emerald-50"
                      : "border-amber-200 text-amber-700 hover:bg-amber-50"
                  )}
                >
                  {node.state.drainPending
                    ? "Cancel drain"
                    : node.state.unschedulable
                      ? "Restore"
                      : "Drain"}
                </button>
              </div>
            )}
          </For>
          <Show when={!cluster.isLoading && (cluster.data?.nodes.length ?? 0) === 0}>
            <div class="px-4 py-12 text-center text-sm text-gray-400">No cluster nodes found.</div>
          </Show>
        </div>
      </div>
    </section>
  );
}

export { NodesPage };
