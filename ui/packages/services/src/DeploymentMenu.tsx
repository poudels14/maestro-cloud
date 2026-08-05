import { DropdownMenu } from "@kobalte/core/dropdown-menu";
import { Ban, EllipsisVertical, RefreshCw, RotateCw, Square } from "lucide-solid";
import { Show } from "solid-js";

const CANCELLABLE_STATUSES = new Set(["QUEUED", "BUILDING"]);
const RESTARTABLE_STATUSES = new Set(["BUILDING", "PUBLISHING", "PENDING_READY", "READY"]);

export function DeploymentMenu(props: {
  status: string;
  onCancel: () => void;
  onRemove: () => void;
  onRedeploy: () => void;
  onRestart: () => void;
}) {
  const canCancel = () => CANCELLABLE_STATUSES.has(props.status);
  const canRestart = () => RESTARTABLE_STATUSES.has(props.status);
  const canRemove = () => props.status !== "REMOVED";

  return (
    <DropdownMenu placement="bottom-end">
      <DropdownMenu.Trigger class="text-gray-400 hover:text-gray-600 p-1 rounded hover:bg-gray-100 transition-[color,background-color,transform] duration-150 ease-out-strong active:scale-[0.96] outline-none">
        <EllipsisVertical class="size-4" />
      </DropdownMenu.Trigger>
      <DropdownMenu.Portal>
        <DropdownMenu.Content class="bg-white border border-gray-200 rounded-lg shadow-lg z-50 py-1.5 w-48">
          <Show when={canCancel()}>
            <DropdownMenu.Item
              class="flex items-center gap-2.5 px-4 py-2.5 text-sm text-red-600 hover:bg-red-50 cursor-pointer outline-none"
              onSelect={() => props.onCancel()}
            >
              <Ban class="size-3" />
              Cancel deployment
            </DropdownMenu.Item>
          </Show>
          <Show when={!canCancel() && canRemove()}>
            <DropdownMenu.Item
              class="flex items-center gap-2.5 px-4 py-2.5 text-sm text-red-600 hover:bg-red-50 cursor-pointer outline-none"
              onSelect={() => props.onRemove()}
            >
              <Square class="size-3" />
              Remove deployment
            </DropdownMenu.Item>
          </Show>
          <Show when={!canCancel()}>
            <DropdownMenu.Item
              class="flex items-center gap-2.5 px-4 py-2.5 text-sm text-gray-700 hover:bg-gray-50 cursor-pointer outline-none"
              onSelect={() => props.onRedeploy()}
            >
              <RotateCw class="size-3" />
              Redeploy
            </DropdownMenu.Item>
            <Show when={canRestart()}>
              <DropdownMenu.Item
                class="flex items-center gap-2.5 px-4 py-2.5 text-sm text-gray-700 hover:bg-gray-50 cursor-pointer outline-none"
                onSelect={() => props.onRestart()}
              >
                <RefreshCw class="size-3" />
                Restart
              </DropdownMenu.Item>
            </Show>
          </Show>
        </DropdownMenu.Content>
      </DropdownMenu.Portal>
    </DropdownMenu>
  );
}
