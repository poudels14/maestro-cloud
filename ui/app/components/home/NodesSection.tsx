import { For, Match, Show, Switch } from "solid-js";
import { createStore } from "solid-js/store";
import { useQuery } from "@tanstack/solid-query";
import { DropdownMenu } from "@kobalte/core/dropdown-menu";
import { EllipsisVertical, RefreshCw, Trash2 } from "lucide-solid";
import clsx from "clsx";
import type { ClusterNode } from "../../lib/types";
import { clusterInfoQuery, clusterNodesQuery, clusterStatsQuery } from "../../lib/queries";
import { removeNode, setNodeDrain, startMaintenance } from "../../lib/api";
import { formatBytes, quantityBytes } from "../../lib/format";
import { ConfirmDialog } from "./ConfirmDialog";
import { MaintenanceSection } from "./MaintenanceSection";

type PendingAction = { kind: "restart" | "remove"; node: ClusterNode };

function NodesSection() {
  const nodes = useQuery(() => clusterNodesQuery());
  const cluster = useQuery(() => clusterInfoQuery({ pollForMaintenance: true }));
  const stats = useQuery(() => clusterStatsQuery());
  const [state, setState] = createStore({
    busyNodeId: null as number | null,
    error: null as string | null,
    pending: null as PendingAction | null
  });

  const nodeVersion = (nodeId: number) =>
    stats.data?.nodes.find((node) => node.nodeId === nodeId)?.version;

  const run = async (nodeId: number, action: () => Promise<unknown>) => {
    setState({ busyNodeId: nodeId, error: null });
    try {
      await action();
      await Promise.all([nodes.refetch(), cluster.refetch()]);
    } catch (cause) {
      setState("error", cause instanceof Error ? cause.message : String(cause));
    } finally {
      setState({ busyNodeId: null, pending: null });
    }
  };

  const confirmPending = () => {
    const pending = state.pending;
    if (pending?.kind === "restart") {
      void run(pending.node.id, () =>
        startMaintenance({ kind: "restart", nodeId: pending.node.id })
      );
    } else if (pending?.kind === "remove") {
      void run(pending.node.id, () => removeNode(pending.node.id));
    }
  };

  return (
    <section class="space-y-8">
      <MaintenanceSection />
      <div>
        <div class="flex items-end justify-between gap-3 mb-3">
          <div>
            <h1 class="text-lg font-semibold text-gray-900">Nodes</h1>
            <p class="text-xs text-gray-500 mt-1">
              Voters run the control plane; every node can run workloads.
            </p>
          </div>
          <span class="text-xs text-gray-400 tabular-nums">{nodes.data?.length ?? 0} nodes</span>
        </div>
        <Show when={state.error}>
          {(message) => (
            <div class="mb-3 rounded-lg border border-red-200 bg-red-50 px-3 py-2 text-xs text-red-700">
              {message()}
            </div>
          )}
        </Show>
        <div class="overflow-x-auto rounded-xl border border-gray-200 bg-white">
          <div class="min-w-[720px]">
            <div class="grid grid-cols-[minmax(10rem,1.4fr)_5rem_minmax(8rem,1fr)_minmax(9rem,1fr)_5rem_8rem] gap-3 border-b border-gray-200 bg-gray-50 px-4 py-2 text-[11px] font-medium text-gray-500">
              <span>Node</span>
              <span>Role</span>
              <span>Endpoint</span>
              <span>Allocated</span>
              <span>Version</span>
              <span class="text-right">Action</span>
            </div>
            <For each={nodes.data ?? []}>
              {(node) => (
                <NodeRow
                  node={node}
                  isLeader={cluster.data?.leader?.id === node.id}
                  version={nodeVersion(node.id)}
                  busy={state.busyNodeId === node.id}
                  onDrain={(drain) => run(node.id, () => setNodeDrain(node.id, drain))}
                  onRequest={(kind) => setState("pending", { kind, node })}
                />
              )}
            </For>
          </div>
          <Show when={!nodes.isLoading && (nodes.data?.length ?? 0) === 0}>
            <div class="px-4 py-12 text-center text-sm text-gray-400">
              No nodes have joined yet.
            </div>
          </Show>
        </div>
      </div>
      <ConfirmDialog
        open={state.pending !== null}
        title={state.pending?.kind === "remove" ? "Remove node" : "Restart node"}
        description={
          <Switch>
            <Match when={state.pending?.kind === "remove"}>
              {state.pending?.node.name} is drained first; run this again once its replicas have
              moved to finish removing it.
            </Match>
            <Match when={state.pending?.kind === "restart"}>
              {state.pending?.node.name} is drained, Maestro restarts on it, and it is restored once
              it reports back. Deploys are frozen until then.
            </Match>
          </Switch>
        }
        confirmLabel={state.pending?.kind === "remove" ? "Remove node" : "Restart node"}
        destructive={state.pending?.kind === "remove"}
        busy={state.busyNodeId !== null}
        onConfirm={confirmPending}
        onCancel={() => setState("pending", null)}
      />
    </section>
  );
}

function NodeRow(props: {
  node: ClusterNode;
  isLeader: boolean;
  version: string | null | undefined;
  busy: boolean;
  onDrain: (drain: boolean) => void;
  onRequest: (kind: PendingAction["kind"]) => void;
}) {
  const draining = () => props.node.state === "draining";
  const allocatableCpu = () => props.node.capacity.cpu - props.node.reserved.cpu;
  const allocatableMemory = () =>
    quantityBytes(props.node.capacity.memory) - quantityBytes(props.node.reserved.memory);

  return (
    <div class="grid grid-cols-[minmax(10rem,1.4fr)_5rem_minmax(8rem,1fr)_minmax(9rem,1fr)_5rem_8rem] items-center gap-3 border-b border-gray-100 px-4 py-3 text-xs last:border-b-0">
      <div class="min-w-0">
        <div class="flex items-center gap-2">
          <span
            class={clsx("size-2 shrink-0 rounded-full", {
              "bg-emerald-500": props.node.available && !draining(),
              "bg-amber-500": props.node.available && draining(),
              "bg-gray-300": !props.node.available
            })}
          />
          <span class="truncate font-medium text-gray-800">{props.node.name}</span>
          <Show when={props.isLeader}>
            <span class="shrink-0 rounded-full border border-indigo-200 bg-indigo-50 px-1.5 py-0.5 text-[10px] font-medium text-indigo-700">
              Leader
            </span>
          </Show>
        </div>
        <div class="mt-1 text-[11px] text-gray-400">
          <Switch fallback={`${props.node.replicas} replicas`}>
            <Match when={!props.node.available}>Unreachable</Match>
            <Match when={draining()}>Draining, {props.node.replicas} replicas left</Match>
            <Match when={!props.node.runsWorkloads}>Control plane only</Match>
          </Switch>
        </div>
      </div>
      <span class="text-gray-600 capitalize">{props.node.role}</span>
      <span class="truncate font-mono text-[11px] text-gray-600">{props.node.endpoint}</span>
      <div class="text-[11px] text-gray-600 tabular-nums">
        <div>
          {props.node.allocated.cpu.toFixed(2)} / {allocatableCpu().toFixed(1)} CPU
        </div>
        <div class="text-gray-400">
          {formatBytes(quantityBytes(props.node.allocated.memory))} /{" "}
          {formatBytes(allocatableMemory())}
        </div>
      </div>
      <span class="font-mono text-[11px] text-gray-600">{props.version ?? "–"}</span>
      <div class="flex items-center justify-end gap-1">
        <button
          type="button"
          disabled={!props.node.available || props.busy}
          onClick={() => props.onDrain(!draining())}
          class={clsx(
            "rounded-md border px-2 py-1 text-[11px] font-medium disabled:cursor-not-allowed disabled:opacity-50",
            {
              "border-emerald-200 text-emerald-700 hover:bg-emerald-50": draining(),
              "border-amber-200 text-amber-700 hover:bg-amber-50": !draining()
            }
          )}
        >
          {draining() ? "Restore" : "Drain"}
        </button>
        <DropdownMenu placement="bottom-end">
          <DropdownMenu.Trigger
            class="rounded p-1 text-gray-400 outline-none hover:bg-gray-100 hover:text-gray-600"
            aria-label={`More actions for ${props.node.name}`}
          >
            <EllipsisVertical class="size-4" />
          </DropdownMenu.Trigger>
          <DropdownMenu.Portal>
            <DropdownMenu.Content class="z-50 w-44 rounded-lg border border-gray-200 bg-white py-1.5 shadow-lg">
              <DropdownMenu.Item
                disabled={!props.node.available}
                class="flex cursor-pointer items-center gap-2.5 px-4 py-2 text-sm text-gray-700 outline-none hover:bg-gray-50 data-[disabled]:cursor-not-allowed data-[disabled]:opacity-50"
                onSelect={() => props.onRequest("restart")}
              >
                <RefreshCw class="size-3" />
                Restart Maestro
              </DropdownMenu.Item>
              <DropdownMenu.Item
                class="flex cursor-pointer items-center gap-2.5 px-4 py-2 text-sm text-red-600 outline-none hover:bg-red-50"
                onSelect={() => props.onRequest("remove")}
              >
                <Trash2 class="size-3" />
                Remove node
              </DropdownMenu.Item>
            </DropdownMenu.Content>
          </DropdownMenu.Portal>
        </DropdownMenu>
      </div>
    </div>
  );
}

export { NodesSection };
