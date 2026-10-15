import { For, Show } from "solid-js";
import clsx from "clsx";
import { useQuery } from "../../lib/useQuery";
import { clusterNodesQuery, disksQuery } from "../../lib/queries";
import type { DiskInfo } from "../../lib/types";
import { formatBytes } from "../../lib/format";
import { Card, SectionHeader } from "../../lib/ui";

function DisksSection() {
  const disks = useQuery(() => disksQuery());
  const nodes = useQuery(() => clusterNodesQuery());
  const nodeDisks = () =>
    Object.entries(disks.data ?? {})
      .map(([nodeId, nodeDiskList]) => ({
        nodeId,
        name: nodes.data?.find((node) => String(node.id) === nodeId)?.name ?? nodeId,
        disks: nodeDiskList
      }))
      .sort((left, right) => left.name.localeCompare(right.name));

  return (
    <Show when={nodeDisks().length > 0}>
      <div class="space-y-6">
        <For each={nodeDisks()}>
          {(node) => (
            <div>
              <SectionHeader class="mb-4">
                {nodeDisks().length > 1 ? `Disks on ${node.name}` : "Disks"}
              </SectionHeader>
              <div class="grid grid-cols-1 gap-4 sm:grid-cols-2">
                <For each={node.disks}>{(disk) => <DiskCard disk={disk} />}</For>
              </div>
            </div>
          )}
        </For>
      </div>
    </Show>
  );
}

function DiskCard(props: { disk: DiskInfo }) {
  const usedBytes = () => props.disk.totalBytes - props.disk.availableBytes;
  const usedPercent = () =>
    props.disk.totalBytes > 0 ? (usedBytes() / props.disk.totalBytes) * 100 : 0;

  return (
    <Card class="p-4">
      <div class="flex items-baseline justify-between mb-1">
        <h3 class="text-xs font-medium text-gray-700 truncate">{props.disk.mountPoint}</h3>
        <span class="text-xs text-gray-400 shrink-0 ml-2 tabular-nums">
          {formatBytes(usedBytes())} / {formatBytes(props.disk.totalBytes)}
        </span>
      </div>
      <Show when={props.disk.name || props.disk.fileSystem}>
        <p class="text-xs text-gray-400 mb-3 truncate">
          {[props.disk.name, props.disk.fileSystem].filter(Boolean).join(", ")}
        </p>
      </Show>
      <div class="w-full bg-gray-100 rounded-full h-2">
        <div
          class={clsx(
            "h-2 rounded-full transition-[width,background-color] duration-500 ease-out-strong",
            {
              "bg-red-500": usedPercent() > 90,
              "bg-amber-500": usedPercent() > 70 && usedPercent() <= 90,
              "bg-indigo-500": usedPercent() <= 70
            }
          )}
          style={{ width: `${Math.min(usedPercent(), 100)}%` }}
        />
      </div>
      <p class="text-xs text-gray-400 mt-1.5 tabular-nums">{usedPercent().toFixed(1)}% used</p>
    </Card>
  );
}

export { DisksSection };
