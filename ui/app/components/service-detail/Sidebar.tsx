import { For, Show } from "solid-js";
import type { Component, JSX } from "solid-js";
import { Dynamic } from "solid-js/web";
import { Monitor, X } from "lucide-solid";
import clsx from "clsx";
import type { Service } from "@maestro/services";
import { serviceDisplayStatus, userServices as visibleUserServices } from "@maestro/services";
import { StatusDot } from "@maestro/kit";

function ServiceSidebar(props: {
  services: Service[];
  selected: Service | null;
  onSelect: (s: Service) => void;
  onBack: () => void;
  topSection?: JSX.Element;
  mobileOpen?: boolean;
  onCloseMobile?: () => void;
}) {
  const userServices = () => visibleUserServices(props.services);

  return (
    <>
      <Show when={props.mobileOpen}>
        <div
          class="fixed inset-0 bg-black/30 z-30 md:hidden"
          onClick={() => props.onCloseMobile?.()}
        />
      </Show>
      <aside
        class={clsx(
          "fixed md:static z-40 inset-y-0 left-0 w-64 md:w-60 shrink-0 bg-white border-r border-gray-200 flex flex-col h-full transform transition-transform duration-200 md:transform-none",
          {
            "translate-x-0": props.mobileOpen,
            "-translate-x-full md:translate-x-0": !props.mobileOpen
          }
        )}
      >
        <div class="h-10 px-4 flex items-center justify-between gap-2 shrink-0 border-b border-gray-200">
          <button
            type="button"
            onClick={props.onBack}
            class="flex items-center gap-2 hover:opacity-80 transition-opacity outline-none group"
          >
            <div class="size-7 rounded-lg bg-indigo-500 flex items-center justify-center shadow-sm group-hover:scale-105 transition-transform">
              <Monitor class="size-4 text-white" />
            </div>
            <span class="text-sm font-semibold text-gray-900 tracking-tight">Maestro</span>
          </button>
          <Show when={props.onCloseMobile}>
            <button
              type="button"
              onClick={() => props.onCloseMobile?.()}
              class="md:hidden p-1.5 text-gray-400 hover:text-gray-700 rounded hover:bg-gray-100 outline-none"
              aria-label="Close menu"
            >
              <X class="size-4" />
            </button>
          </Show>
        </div>
        <div class="flex-1 overflow-y-auto">
          {props.topSection}
          <Show when={userServices().length > 0}>
            <SidebarSection title="Services" count={userServices().length}>
              <For each={userServices()}>
                {(service) => (
                  <SidebarServiceItem
                    service={service}
                    selected={service.meta.id === props.selected?.meta.id}
                    onClick={() => props.onSelect(service)}
                  />
                )}
              </For>
            </SidebarSection>
          </Show>
        </div>
      </aside>
    </>
  );
}

function SidebarSection(props: { title: string; count?: number; children: JSX.Element }) {
  return (
    <div class="pb-3">
      <div class="px-3 pt-4 pb-1.5 flex items-baseline justify-between">
        <span class="text-xs font-medium text-gray-400">{props.title}</span>
        <Show when={props.count !== undefined}>
          <span class="text-[10px] text-gray-300 tabular-nums">{props.count}</span>
        </Show>
      </div>
      <div class="px-2 space-y-0.5">{props.children}</div>
    </div>
  );
}

function SidebarNavItem(props: {
  label: string;
  icon: Component<{ class?: string }>;
  selected: boolean;
  onClick: () => void;
}) {
  return (
    <button
      type="button"
      onClick={props.onClick}
      class={clsx(
        "relative w-full text-left pl-2.5 pr-2 py-1.5 flex items-center gap-2 rounded-md text-sm transition-[transform,background-color,color] duration-150 ease-out-strong active:scale-[0.98] outline-none",
        {
          "bg-indigo-50 text-indigo-700 font-medium": props.selected,
          "text-gray-700 hover:bg-gray-50": !props.selected
        }
      )}
    >
      <Show when={props.selected}>
        <span class="absolute left-0 top-1.5 bottom-1.5 w-0.5 rounded-r bg-indigo-500" />
      </Show>
      <Dynamic component={props.icon} class="size-3.5 shrink-0 opacity-70" />
      <span class="truncate flex-1">{props.label}</span>
    </button>
  );
}

function SidebarServiceItem(props: { service: Service; selected: boolean; onClick: () => void }) {
  const status = () => serviceDisplayStatus(props.service);
  return (
    <button
      type="button"
      onClick={props.onClick}
      class={clsx(
        "relative w-full text-left pl-2.5 pr-2 py-1.5 flex items-center gap-2 rounded-md text-sm transition-[transform,background-color,color] duration-150 ease-out-strong active:scale-[0.98] outline-none",
        {
          "bg-indigo-50 text-indigo-700 font-medium": props.selected,
          "text-gray-700 hover:bg-gray-50": !props.selected
        }
      )}
    >
      <Show when={props.selected}>
        <span class="absolute left-0 top-1.5 bottom-1.5 w-0.5 rounded-r bg-indigo-500" />
      </Show>
      <span class="size-3.5 flex items-center justify-center shrink-0">
        <StatusDot status={status()} />
      </span>
      <span class="truncate flex-1">{props.service.spec.name}</span>
    </button>
  );
}

export { ServiceSidebar, SidebarSection, SidebarNavItem };
