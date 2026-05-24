import { Show, createSignal } from "solid-js";
import { useQueryClient } from "@tanstack/solid-query";
import { Crown, PauseCircle, Play, User } from "lucide-solid";
import clsx from "clsx";
import type { ClusterNode } from "../../lib/api";
import { drainNode, restoreNode } from "../../lib/api";
import { queryKeys } from "../../lib/queries";

function ClusterNodeRow(props: { node: ClusterNode; isLeader: boolean; isSelf: boolean }) {
  const queryClient = useQueryClient();
  const [busy, setBusy] = createSignal(false);
  const [error, setError] = createSignal<string | null>(null);

  const startedAt = () => {
    if (!props.node.startedAtMs) return "—";
    const seconds = Math.max(1, Math.floor((Date.now() - props.node.startedAtMs) / 1000));
    if (seconds < 60) return `${seconds}s ago`;
    if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`;
    if (seconds < 86400) return `${Math.floor(seconds / 3600)}h ago`;
    return `${Math.floor(seconds / 86400)}d ago`;
  };

  const host = () =>
    props.node.tailscaleIp ||
    props.node.hostname ||
    props.node.nodeId;

  const toggleDrain = async () => {
    if (busy()) return;
    setBusy(true);
    setError(null);
    try {
      const fn = props.node.unschedulable ? restoreNode : drainNode;
      await fn(host(), props.node.apiPort);
      await queryClient.invalidateQueries({ queryKey: queryKeys.cluster });
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setBusy(false);
    }
  };

  return (
    <div class="px-4 py-3 flex items-center gap-4">
      <div class="flex-1 min-w-0">
        <div class="flex items-center gap-2 flex-wrap">
          <span class="text-sm font-mono font-medium text-gray-900 truncate">
            {props.node.nodeId}
          </span>
          <Show when={props.isLeader}>
            <span class="inline-flex items-center gap-1 text-xs font-medium text-amber-700 bg-amber-50 px-1.5 py-0.5 rounded">
              <Crown class="size-3" />
              leader
            </span>
          </Show>
          <Show when={props.isSelf}>
            <span class="inline-flex items-center gap-1 text-xs font-medium text-indigo-700 bg-indigo-50 px-1.5 py-0.5 rounded">
              <User class="size-3" />
              this node
            </span>
          </Show>
          <Show when={props.node.unschedulable}>
            <span class="inline-flex items-center gap-1 text-xs font-medium text-gray-600 bg-gray-100 px-1.5 py-0.5 rounded">
              <PauseCircle class="size-3" />
              drained
            </span>
          </Show>
        </div>
        <div class="mt-1 flex flex-wrap items-center gap-x-4 gap-y-0.5 text-xs text-gray-500">
          <span class="font-mono">{host()}:{props.node.apiPort}</span>
          <span>role: {props.node.role}</span>
          <span>v{props.node.version}</span>
          <span>up {startedAt()}</span>
        </div>
        <Show when={error()}>
          <div class="mt-1 text-xs text-red-600">{error()}</div>
        </Show>
      </div>
      <button
        type="button"
        disabled={busy()}
        onClick={toggleDrain}
        class={clsx(
          "text-xs font-medium px-2.5 py-1 rounded border transition-colors",
          {
            "border-gray-300 text-gray-700 hover:bg-gray-50":
              !props.node.unschedulable && !busy(),
            "border-emerald-300 text-emerald-700 hover:bg-emerald-50":
              props.node.unschedulable && !busy(),
            "opacity-50 cursor-not-allowed": busy()
          }
        )}
        title={props.node.unschedulable ? "Restore (resume scheduling)" : "Drain (stop scheduling new replicas)"}
      >
        <Show
          when={props.node.unschedulable}
          fallback={
            <span class="inline-flex items-center gap-1">
              <PauseCircle class="size-3" /> Drain
            </span>
          }
        >
          <span class="inline-flex items-center gap-1">
            <Play class="size-3" /> Restore
          </span>
        </Show>
      </button>
    </div>
  );
}

export { ClusterNodeRow };
