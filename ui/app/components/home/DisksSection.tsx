import { For, Show } from "solid-js";
import clsx from "clsx";
import { useQuery } from "@tanstack/solid-query";
import { disksQuery } from "../../lib/queries";
import type { DiskInfo } from "../../lib/types";
import { formatBytes } from "../../lib/format";
import { Card, SectionHeader } from "../../lib/ui";

function DisksSection() {
  const disks = useQuery(() => disksQuery());

  return (
    <Show when={(disks.data ?? []).length > 0}>
      <div>
        <SectionHeader class="mb-4">Disks</SectionHeader>
        <div class="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-3">
          <For each={disks.data}>{(disk) => <DiskCard disk={disk} />}</For>
        </div>
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
        <h3 class="text-xs font-medium text-gray-500 uppercase tracking-wider truncate">
          {props.disk.mountPoint}
        </h3>
        <span class="text-xs text-gray-400 shrink-0 ml-2">
          {formatBytes(usedBytes())} / {formatBytes(props.disk.totalBytes)}
        </span>
      </div>
      <Show when={props.disk.name || props.disk.fileSystem}>
        <p class="text-xs text-gray-400 mb-3 truncate">
          {[props.disk.name, props.disk.fileSystem].filter(Boolean).join(" · ")}
        </p>
      </Show>
      <div class="w-full bg-gray-100 rounded-full h-2">
        <div
          class={clsx("h-2 rounded-full", {
            "bg-red-500": usedPercent() > 90,
            "bg-amber-500": usedPercent() > 70 && usedPercent() <= 90,
            "bg-indigo-500": usedPercent() <= 70
          })}
          style={{ width: `${Math.min(usedPercent(), 100)}%` }}
        />
      </div>
      <p class="text-xs text-gray-400 mt-1.5">{usedPercent().toFixed(1)}% used</p>
    </Card>
  );
}

export { DisksSection };
