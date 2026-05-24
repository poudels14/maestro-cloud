import { useQuery } from "@tanstack/solid-query";
import { Show } from "solid-js";
import { AlertTriangle, Check, CircleAlert } from "lucide-solid";
import clsx from "clsx";
import { clusterInfoQuery } from "../../lib/queries";

function ClusterHealthBanner() {
  const cluster = useQuery(() => clusterInfoQuery({ live: true }));

  const state = () => {
    const data = cluster.data;
    if (!data) return null;
    const hasNodes = (data.nodes?.length ?? 0) > 0;
    if (!hasNodes) return null;
    const drained = data.nodes!.filter((node) => node.unschedulable).length;
    if (!data.leader) return "no-leader" as const;
    if (data.upgrading) return "upgrading" as const;
    if (drained > 0) return "drained" as const;
    return "healthy" as const;
  };

  return (
    <Show when={state()}>
      {(s) => (
        <div
          class={clsx("flex items-start gap-3 px-4 py-3 rounded-lg border text-sm", {
            "bg-emerald-50 border-emerald-200 text-emerald-900": s() === "healthy",
            "bg-amber-50 border-amber-200 text-amber-900":
              s() === "drained" || s() === "upgrading",
            "bg-red-50 border-red-200 text-red-900": s() === "no-leader"
          })}
        >
          <span class="mt-0.5">
            <Show when={s() === "healthy"}>
              <Check class="size-4" />
            </Show>
            <Show when={s() === "drained" || s() === "upgrading"}>
              <AlertTriangle class="size-4" />
            </Show>
            <Show when={s() === "no-leader"}>
              <CircleAlert class="size-4" />
            </Show>
          </span>
          <div class="flex-1 leading-snug">
            <Show when={s() === "healthy"}>
              <div class="font-medium">Cluster healthy</div>
              <div class="text-xs opacity-75">
                {cluster.data!.nodes!.length} node(s); scheduling leader{" "}
                <span class="font-mono">{cluster.data!.leader!.nodeId}</span>
              </div>
            </Show>
            <Show when={s() === "drained"}>
              <div class="font-medium">Some nodes are drained</div>
              <div class="text-xs opacity-75">
                {cluster.data!.nodes!.filter((node) => node.unschedulable).length} node(s)
                marked unschedulable; new replicas won't land on them
              </div>
            </Show>
            <Show when={s() === "upgrading"}>
              <div class="font-medium">Cluster upgrade in progress</div>
              <div class="text-xs opacity-75">
                Rolling restart underway; leadership may transition
              </div>
            </Show>
            <Show when={s() === "no-leader"}>
              <div class="font-medium">No scheduling leader elected</div>
              <div class="text-xs opacity-75">
                Mutating operations will return 503 until a leader is elected
              </div>
            </Show>
          </div>
        </div>
      )}
    </Show>
  );
}

export { ClusterHealthBanner };
