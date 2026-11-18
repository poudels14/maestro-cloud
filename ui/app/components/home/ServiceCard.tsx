import { DropdownMenu } from "@kobalte/core/dropdown-menu";
import { Switch as Toggle } from "@kobalte/core/switch";
import { useMutation, useQueryClient } from "@tanstack/solid-query";
import { EllipsisVertical, Trash2 } from "lucide-solid";
import clsx from "clsx";
import type { Service } from "../../lib/types";
import { freezeService } from "../../lib/api";
import { queryKeys } from "../../lib/queries";
import { StatusBadge } from "../../lib/ui";

function ServiceCard(props: { service: Service; onClick: () => void; onDelete: () => void }) {
  const queryClient = useQueryClient();
  const freezeMutation = useMutation(() => ({
    mutationFn: (frozen: boolean) => freezeService(props.service.id, frozen),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: queryKeys.services })
  }));
  const status = () => props.service.status;
  const sourceName = () => {
    const build = props.service.build;
    if (build?.repo) {
      return build.repo
        .replace(/\.git$/, "")
        .split("/")
        .slice(-2)
        .join("/");
    }
    if (build) return "uploaded context";
    return props.service.image ?? "(no source)";
  };

  return (
    <div
      onClick={props.onClick}
      class="bg-white border border-gray-200 rounded-lg p-4 sm:p-5 shadow-[0_1px_2px_rgb(0_0_0/0.04)] hover:border-gray-300 transition-colors duration-150 text-left w-full cursor-pointer outline-none relative"
    >
      <div class="flex items-start justify-between gap-4">
        <div class="min-w-0">
          <div class="flex items-center gap-2.5 mb-1">
            <span class="text-base font-semibold text-gray-900 truncate">{props.service.name}</span>
          </div>
          <p class="text-sm text-gray-500 truncate">{sourceName()}</p>
        </div>
        <div class="flex items-center gap-3 shrink-0">
          <div onClick={(e: MouseEvent) => e.stopPropagation()}>
            <Toggle
              checked={Boolean(props.service.deployFrozen)}
              onChange={(frozen) => freezeMutation.mutate(frozen)}
              disabled={freezeMutation.isPending}
              class="flex items-center gap-2"
            >
              <Toggle.Input class="peer" />
              <Toggle.Label
                class={clsx("text-xs cursor-pointer select-none", {
                  "text-amber-700 font-medium": props.service.deployFrozen,
                  "text-gray-400": !props.service.deployFrozen
                })}
                title="Frozen services skip auto-deploys; manual deploys need force"
              >
                Frozen
              </Toggle.Label>
              <Toggle.Control
                class={clsx(
                  "inline-flex h-4 w-7 shrink-0 cursor-pointer items-center rounded-full p-0.5 transition-colors peer-focus-visible:ring-2 peer-focus-visible:ring-indigo-300",
                  {
                    "bg-amber-500": props.service.deployFrozen,
                    "bg-gray-200": !props.service.deployFrozen
                  }
                )}
              >
                <Toggle.Thumb
                  class={clsx("size-3 rounded-full bg-white shadow-sm transition-transform", {
                    "translate-x-3": props.service.deployFrozen,
                    "translate-x-0": !props.service.deployFrozen
                  })}
                />
              </Toggle.Control>
            </Toggle>
          </div>
          <StatusBadge status={status()} />
          <div onClick={(e: MouseEvent) => e.stopPropagation()}>
            <DropdownMenu>
              <DropdownMenu.Trigger class="text-gray-400 hover:text-gray-600 p-1 rounded hover:bg-gray-100 transition-[color,background-color,transform] duration-150 ease-out-strong active:scale-[0.96] outline-none">
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
        </div>
      </div>
    </div>
  );
}

export { ServiceCard };
