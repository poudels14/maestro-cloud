import { For, Show } from "solid-js";
import { DropdownMenu } from "@kobalte/core/dropdown-menu";
import { Check, ChevronDown, ListFilter } from "lucide-solid";
import clsx from "clsx";
import type { ClusterNode, Service } from "../../lib/types";

type LogFilterSelection = {
  nodes: string[];
  services: string[];
};

function LogFilters(props: {
  nodes: ClusterNode[];
  services: Service[];
  selection: LogFilterSelection;
  onChange: (selection: LogFilterSelection) => void;
}) {
  const count = () => props.selection.nodes.length + props.selection.services.length;
  const summary = () => {
    const names = [
      ...props.nodes
        .filter((node) => props.selection.nodes.includes(String(node.id)))
        .map((node) => node.name),
      ...props.services
        .filter((service) => props.selection.services.includes(service.id))
        .map((service) => service.name)
    ];
    return names.length > 2 ? `${names.length} filters` : names.join(", ");
  };
  const toggle = (group: keyof LogFilterSelection, value: string, checked: boolean) =>
    props.onChange({
      ...props.selection,
      [group]: checked
        ? [...props.selection[group], value]
        : props.selection[group].filter((selected) => selected !== value)
    });

  return (
    <DropdownMenu placement="bottom-start">
      <DropdownMenu.Trigger
        class={clsx(
          "shrink-0 inline-flex h-8 max-w-64 items-center gap-1.5 rounded-md border px-2.5 text-xs outline-none transition-colors",
          {
            "border-indigo-200 bg-indigo-50 text-indigo-700 hover:bg-indigo-100": count() > 0,
            "border-gray-200 bg-white text-gray-600 hover:bg-gray-50": count() === 0
          }
        )}
      >
        <ListFilter class="size-3.5 shrink-0" />
        <span class="truncate">{count() > 0 ? summary() : "Filter"}</span>
        <ChevronDown class="size-3 shrink-0 opacity-60" />
      </DropdownMenu.Trigger>
      <DropdownMenu.Portal>
        <DropdownMenu.Content class="z-50 max-h-[70vh] w-64 overflow-y-auto rounded-lg border border-gray-200 bg-white pb-1.5 shadow-lg">
          <div class="sticky top-0 z-10 flex h-9 items-center justify-between border-b border-gray-100 bg-white px-3">
            <span class="text-xs font-medium text-gray-700">
              {count() > 0 ? `${count()} selected` : "Show logs from"}
            </span>
            <DropdownMenu.Item
              class={clsx(
                "rounded px-1.5 py-0.5 text-xs text-indigo-600 outline-none hover:bg-indigo-50 data-[highlighted]:bg-indigo-50",
                { "cursor-pointer": count() > 0, invisible: count() === 0 }
              )}
              disabled={count() === 0}
              closeOnSelect={false}
              onSelect={() => props.onChange({ nodes: [], services: [] })}
            >
              Clear filters
            </DropdownMenu.Item>
          </div>
          <Show when={props.nodes.length > 1}>
            <FilterGroup
              label="Nodes"
              options={props.nodes.map((node) => ({
                value: String(node.id),
                label: node.available ? node.name : `${node.name} (offline)`,
                disabled: !node.available
              }))}
              selected={props.selection.nodes}
              onToggle={(value, checked) => toggle("nodes", value, checked)}
            />
          </Show>
          <FilterGroup
            label="Services"
            options={props.services.map((service) => ({ value: service.id, label: service.name }))}
            selected={props.selection.services}
            onToggle={(value, checked) => toggle("services", value, checked)}
          />
        </DropdownMenu.Content>
      </DropdownMenu.Portal>
    </DropdownMenu>
  );
}

function FilterGroup(props: {
  label: string;
  options: { value: string; label: string; disabled?: boolean }[];
  selected: string[];
  onToggle: (value: string, checked: boolean) => void;
}) {
  return (
    <DropdownMenu.Group class="pt-1.5">
      <DropdownMenu.GroupLabel class="px-3 pb-1 text-[11px] font-medium text-gray-400">
        {props.label}
      </DropdownMenu.GroupLabel>
      <For each={props.options}>
        {(option) => (
          <DropdownMenu.CheckboxItem
            checked={props.selected.includes(option.value)}
            onChange={(checked) => props.onToggle(option.value, checked)}
            disabled={option.disabled}
            closeOnSelect={false}
            class="flex cursor-pointer items-center gap-2 px-3 py-1.5 text-xs text-gray-700 outline-none hover:bg-gray-50 data-[highlighted]:bg-gray-50 data-[disabled]:cursor-not-allowed data-[disabled]:opacity-50"
          >
            <span
              class={clsx("flex size-3.5 shrink-0 items-center justify-center rounded border", {
                "border-indigo-600 bg-indigo-600 text-white": props.selected.includes(option.value),
                "border-gray-300 bg-white": !props.selected.includes(option.value)
              })}
            >
              <DropdownMenu.ItemIndicator>
                <Check class="size-2.5" stroke-width={3} />
              </DropdownMenu.ItemIndicator>
            </span>
            <DropdownMenu.ItemLabel class="truncate">{option.label}</DropdownMenu.ItemLabel>
          </DropdownMenu.CheckboxItem>
        )}
      </For>
    </DropdownMenu.Group>
  );
}

function parseFilterParam(value: string | undefined): string[] {
  return (value ?? "")
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}

function filterParam(values: string[]): string | undefined {
  return values.length > 0 ? values.join(",") : undefined;
}

export { LogFilters, filterParam, parseFilterParam };
export type { LogFilterSelection };
