import type { Component, JSX } from "solid-js";
import { Show } from "solid-js";
import { Dynamic } from "solid-js/web";
import clsx from "clsx";

function SidebarSection(props: { title: string; count?: number; children: JSX.Element }) {
  return (
    <div class="pb-3">
      <div class="px-3 pt-4 pb-1.5 flex items-baseline justify-between">
        <span class="text-[10px] font-semibold uppercase tracking-wider text-gray-400">
          {props.title}
        </span>
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
          "bg-brand-light text-brand-hover font-medium": props.selected,
          "text-gray-700 hover:bg-gray-50": !props.selected
        }
      )}
    >
      <Show when={props.selected}>
        <span class="absolute left-0 top-1.5 bottom-1.5 w-0.5 rounded-r bg-brand" />
      </Show>
      <Dynamic component={props.icon} class="size-3.5 shrink-0 opacity-70" />
      <span class="truncate flex-1">{props.label}</span>
    </button>
  );
}

export { SidebarNavItem, SidebarSection };
