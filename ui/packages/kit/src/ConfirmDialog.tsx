import { Dialog } from "@kobalte/core/dialog";
import { Show, type JSX } from "solid-js";

function ConfirmDialog(props: {
  open: boolean;
  title: string;
  description: JSX.Element;
  confirmLabel?: string;
  confirmBusyLabel?: string;
  busy?: boolean;
  destructive?: boolean;
  onConfirm: () => void;
  onCancel: () => void;
}) {
  const confirmClass = () =>
    props.destructive !== false
      ? "px-3 py-1.5 text-sm text-white bg-red-600 hover:bg-red-700 rounded-lg transition-colors outline-none disabled:bg-red-300"
      : "px-3 py-1.5 text-sm text-white bg-indigo-600 hover:bg-indigo-700 rounded-lg transition-colors outline-none disabled:bg-indigo-300";

  return (
    <Dialog
      open={props.open}
      onOpenChange={(open) => {
        if (!open) props.onCancel();
      }}
    >
      <Dialog.Portal>
        <Dialog.Overlay class="fixed inset-0 bg-black/30 z-50" />
        <Dialog.Content class="fixed top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 bg-white rounded-xl shadow-xl z-50 p-6 w-full max-w-sm outline-none">
          <Dialog.Title class="text-base font-semibold text-gray-900 mb-2">
            {props.title}
          </Dialog.Title>
          <Dialog.Description class="text-sm text-gray-500 mb-5">
            {props.description}
          </Dialog.Description>
          <div class="flex justify-end gap-2">
            <button
              type="button"
              onClick={props.onCancel}
              disabled={props.busy}
              class="px-3 py-1.5 text-sm text-gray-600 hover:bg-gray-100 rounded-lg transition-colors outline-none"
            >
              Cancel
            </button>
            <button
              type="button"
              onClick={props.onConfirm}
              disabled={props.busy}
              class={confirmClass()}
            >
              <Show when={props.busy} fallback={props.confirmLabel ?? "Confirm"}>
                {props.confirmBusyLabel ?? "Working…"}
              </Show>
            </button>
          </div>
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog>
  );
}

export { ConfirmDialog };
