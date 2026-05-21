import { Show } from "solid-js";
import { DropdownMenu } from "@kobalte/core/dropdown-menu";
import { EllipsisVertical, Trash2 } from "lucide-solid";
import type { Service } from "../../lib/types";
import { StatusBadge } from "../../lib/ui";

function ServiceCard(props: { service: Service; onClick: () => void; onDelete: () => void }) {
  const isSystem = () => props.service.system === true;
  const status = () => (isSystem() ? "SYSTEM" : (props.service.status ?? "IDLE"));
  const sourceName = () => {
    const build = props.service.build;
    if (build) {
      return build.repo
        .replace(/\.git$/, "")
        .split("/")
        .slice(-2)
        .join("/");
    }
    return props.service.image ?? "(no source)";
  };

  return (
    <div
      onClick={props.onClick}
      class="bg-white border border-gray-200 rounded-lg p-5 hover:border-indigo-200 hover:shadow-sm transition-all text-left w-full cursor-pointer outline-none group relative"
    >
      <div class="flex items-start justify-between gap-4">
        <div class="min-w-0">
          <div class="flex items-center gap-2.5 mb-1">
            <span class="text-base font-semibold text-gray-900 truncate group-hover:text-indigo-600 transition-colors">
              {props.service.name}
            </span>
          </div>
          <p class="text-sm text-gray-500 truncate">{sourceName()}</p>
        </div>
        <div class="flex items-center gap-2 shrink-0">
          <Show when={props.service.deployFrozen}>
            <span class="inline-flex items-center text-[10px] font-medium text-amber-700 bg-amber-50 border border-amber-200 rounded px-1.5 py-0.5">
              frozen
            </span>
          </Show>
          <StatusBadge status={status()} />
          <Show when={!isSystem()}>
            <div onClick={(e: MouseEvent) => e.stopPropagation()}>
              <DropdownMenu>
                <DropdownMenu.Trigger class="text-gray-400 hover:text-gray-600 p-1 rounded hover:bg-gray-100 transition-colors outline-none">
                  <EllipsisVertical class="size-4" />
                </DropdownMenu.Trigger>
                <DropdownMenu.Portal>
                  <DropdownMenu.Content class="bg-white border border-gray-200 rounded-lg shadow-lg z-50 py-1 min-w-[160px]">
                    <DropdownMenu.Item
                      class="flex items-center gap-2 px-3 py-2 text-sm text-red-600 hover:bg-red-50 cursor-pointer outline-none"
                      onSelect={() => props.onDelete()}
                    >
                      <Trash2 class="size-3.5" />
                      Remove service
                    </DropdownMenu.Item>
                  </DropdownMenu.Content>
                </DropdownMenu.Portal>
              </DropdownMenu>
            </div>
          </Show>
        </div>
      </div>
    </div>
  );
}

export { ServiceCard };
