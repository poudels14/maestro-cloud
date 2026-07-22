import { Toast, toaster } from "@kobalte/core/toast";
import { CircleAlert, X } from "lucide-solid";

function AppToasts() {
  return (
    <Toast.Region
      duration={8_000}
      limit={4}
      swipeDirection="right"
      class="fixed right-4 top-4 z-[100] w-[min(28rem,calc(100vw-2rem))] outline-none"
    >
      <Toast.List class="flex flex-col gap-2" />
    </Toast.Region>
  );
}

function showErrorToast(title: string, cause: unknown) {
  const message = cause instanceof Error ? cause.message : String(cause);
  toaster.show((props) => (
    <Toast
      toastId={props.toastId}
      priority="high"
      class="rounded-xl border border-red-200 bg-white p-4 shadow-xl"
    >
      <div class="flex items-start gap-3">
        <CircleAlert class="mt-0.5 size-4 shrink-0 text-red-500" />
        <div class="min-w-0 flex-1">
          <Toast.Title class="text-sm font-semibold text-gray-900">{title}</Toast.Title>
          <Toast.Description class="mt-1 break-words text-xs leading-5 text-gray-600">
            {message}
          </Toast.Description>
        </div>
        <Toast.CloseButton
          aria-label="Dismiss notification"
          class="rounded p-1 text-gray-400 outline-none hover:bg-gray-100 hover:text-gray-600"
        >
          <X class="size-3.5" />
        </Toast.CloseButton>
      </div>
    </Toast>
  ));
}

export { AppToasts, showErrorToast };
