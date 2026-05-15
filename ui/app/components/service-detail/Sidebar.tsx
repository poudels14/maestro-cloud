import { For, Show } from "solid-js";
import { Monitor } from "lucide-solid";
import clsx from "clsx";
import type { Service } from "../../lib/types";
import { StatusDot } from "../../lib/ui";

function ServiceSidebar(props: {
  services: Service[];
  selected: Service | null;
  onSelect: (s: Service) => void;
  onBack: () => void;
}) {
  const userServices = () => props.services.filter((s) => !s.system);
  const systemServices = () => props.services.filter((s) => s.system === true);

  return (
    <div class="w-60 shrink-0 bg-white border-r border-gray-200 flex flex-col h-full">
      <button
        type="button"
        onClick={props.onBack}
        class="px-4 h-14 flex items-center gap-2 shrink-0 border-b border-gray-200 hover:bg-gray-50 transition-colors outline-none w-full group"
      >
        <div class="size-7 rounded-lg bg-indigo-500 flex items-center justify-center shadow-sm group-hover:scale-105 transition-transform">
          <Monitor class="size-4 text-white" />
        </div>
        <span class="text-sm font-semibold text-gray-900 tracking-tight">Maestro</span>
      </button>
      <div class="flex-1 overflow-y-auto">
        <Show when={userServices().length > 0}>
          <div class="px-3 pt-4 pb-1.5 flex items-baseline justify-between">
            <span class="text-[10px] font-semibold uppercase tracking-wider text-gray-400">
              Services
            </span>
            <span class="text-[10px] font-mono text-gray-300 tabular-nums">
              {userServices().length}
            </span>
          </div>
          <div class="px-2 space-y-0.5">
            <For each={userServices()}>
              {(service) => (
                <SidebarServiceItem
                  service={service}
                  selected={service.id === props.selected?.id}
                  onClick={() => props.onSelect(service)}
                />
              )}
            </For>
          </div>
        </Show>
        <Show when={systemServices().length > 0}>
          <div class="px-3 pt-5 pb-1.5 flex items-baseline justify-between">
            <span class="text-[10px] font-semibold uppercase tracking-wider text-gray-400">
              System
            </span>
            <span class="text-[10px] font-mono text-gray-300 tabular-nums">
              {systemServices().length}
            </span>
          </div>
          <div class="px-2 space-y-0.5 pb-3">
            <For each={systemServices()}>
              {(service) => (
                <SidebarSystemItem
                  service={service}
                  selected={service.id === props.selected?.id}
                  onClick={() => props.onSelect(service)}
                />
              )}
            </For>
          </div>
        </Show>
      </div>
    </div>
  );
}

function SidebarServiceItem(props: { service: Service; selected: boolean; onClick: () => void }) {
  const status = () => props.service.status ?? "IDLE";
  return (
    <button
      type="button"
      onClick={props.onClick}
      class={clsx(
        "relative w-full text-left pl-2.5 pr-2 py-1.5 flex items-center gap-2 text-sm rounded-md transition-colors outline-none",
        props.selected
          ? "bg-indigo-50 text-indigo-700 font-medium"
          : "text-gray-700 hover:bg-gray-50"
      )}
    >
      <Show when={props.selected}>
        <span class="absolute left-0 top-1.5 bottom-1.5 w-0.5 rounded-r bg-indigo-500" />
      </Show>
      <StatusDot status={status()} />
      <span class="truncate flex-1">{props.service.name}</span>
    </button>
  );
}

function SidebarSystemItem(props: { service: Service; selected: boolean; onClick: () => void }) {
  return (
    <button
      type="button"
      onClick={props.onClick}
      class={clsx(
        "w-full text-left pl-2.5 pr-2 py-1.5 flex items-center gap-2 text-xs rounded-md transition-colors outline-none",
        props.selected
          ? "bg-indigo-50 text-indigo-700 font-medium"
          : "text-gray-500 hover:bg-gray-50 hover:text-gray-700"
      )}
    >
      <StatusDot status="SYSTEM" />
      <span class="truncate">{props.service.name}</span>
    </button>
  );
}

export { ServiceSidebar };
