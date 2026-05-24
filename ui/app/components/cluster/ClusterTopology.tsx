import { For, Show } from "solid-js";
import { useQuery } from "@tanstack/solid-query";
import { Network } from "lucide-solid";
import clsx from "clsx";
import { clusterInfoQuery } from "../../lib/queries";

function ClusterTopology() {
  const cluster = useQuery(() => clusterInfoQuery({ live: true }));
  const nodes = () => cluster.data?.nodes ?? [];
  const leader = () => cluster.data?.leader?.nodeId;
  const self = () => cluster.data?.thisNodeId;

  return (
    <section>
      <div class="flex items-center gap-2 text-sm font-semibold text-gray-900 mb-3">
        <Network class="size-4 text-gray-500" />
        Topology
      </div>
      <Show
        when={nodes().length > 0}
        fallback={
          <div class="text-sm text-gray-400 py-8 text-center bg-white border border-gray-200 rounded-lg">
            Single-node deployment.
          </div>
        }
      >
        <div class="bg-white border border-gray-200 rounded-lg p-6 overflow-x-auto">
          <RingDiagram
            nodes={nodes()}
            leaderId={leader()}
            selfId={self()}
          />
        </div>
      </Show>
    </section>
  );
}

function RingDiagram(props: {
  nodes: { nodeId: string; unschedulable?: boolean }[];
  leaderId?: string;
  selfId?: string;
}) {
  const size = 320;
  const center = size / 2;
  const radius = 110;
  const slots = () =>
    props.nodes.map((node, index) => {
      const angle = (index / props.nodes.length) * Math.PI * 2 - Math.PI / 2;
      return {
        node,
        x: center + Math.cos(angle) * radius,
        y: center + Math.sin(angle) * radius
      };
    });

  return (
    <svg width={size} height={size} class="mx-auto block">
      {/* connecting circle */}
      <circle
        cx={center}
        cy={center}
        r={radius}
        fill="none"
        stroke="#e5e7eb"
        stroke-width="1.5"
      />
      <For each={slots()}>
        {(slot) => (
          <g>
            <circle
              cx={slot.x}
              cy={slot.y}
              r="28"
              class={clsx({
                "fill-amber-100 stroke-amber-500":
                  props.leaderId === slot.node.nodeId,
                "fill-gray-100 stroke-gray-400":
                  props.leaderId !== slot.node.nodeId && slot.node.unschedulable,
                "fill-indigo-100 stroke-indigo-500":
                  props.leaderId !== slot.node.nodeId &&
                  !slot.node.unschedulable &&
                  props.selfId === slot.node.nodeId,
                "fill-white stroke-gray-300":
                  props.leaderId !== slot.node.nodeId &&
                  !slot.node.unschedulable &&
                  props.selfId !== slot.node.nodeId
              })}
              stroke-width="2"
            />
            <text
              x={slot.x}
              y={slot.y + 5}
              text-anchor="middle"
              font-size="11"
              font-family="ui-monospace, monospace"
              class="fill-gray-700 select-none"
            >
              {slot.node.nodeId.slice(0, 8)}
            </text>
            <Show when={props.leaderId === slot.node.nodeId}>
              <text
                x={slot.x}
                y={slot.y - 38}
                text-anchor="middle"
                font-size="10"
                class="fill-amber-700 font-medium select-none"
              >
                leader
              </text>
            </Show>
            <Show when={slot.node.unschedulable}>
              <text
                x={slot.x}
                y={slot.y + 48}
                text-anchor="middle"
                font-size="10"
                class="fill-gray-500 select-none"
              >
                drained
              </text>
            </Show>
          </g>
        )}
      </For>
    </svg>
  );
}

export { ClusterTopology };
