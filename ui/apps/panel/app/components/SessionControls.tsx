import { useNavigate } from "@tanstack/solid-router";
import { createSignal, Show } from "solid-js";
import { Loader2, LogOut } from "lucide-solid";
import { apiClient, apiRequestError, isUnauthenticated } from "../lib/client";
import { queryClient } from "../lib/queryClient";

function SessionControls() {
  const navigate = useNavigate();
  const [pending, setPending] = createSignal(false);
  const [error, setError] = createSignal<string | null>(null);

  const signOut = async () => {
    setPending(true);
    setError(null);
    try {
      await apiClient().deleteBrowserSession();
    } catch (failure) {
      if (!isUnauthenticated(failure)) {
        setError(apiRequestError(failure, "Could not sign out").message);
        setPending(false);
        return;
      }
    }
    queryClient.clear();
    await navigate({ to: "/login", replace: true });
  };

  return (
    <div class="shrink-0 border-t border-gray-200 p-2">
      <Show when={error()}>
        {(message) => <p class="px-2 pb-2 text-xs text-red-600">{message()}</p>}
      </Show>
      <button
        type="button"
        disabled={pending()}
        onClick={() => void signOut()}
        class="flex w-full items-center gap-2 rounded-md px-2 py-1.5 text-sm text-gray-500 outline-none hover:bg-gray-50 hover:text-gray-800 disabled:cursor-wait disabled:opacity-60"
      >
        <Show when={pending()} fallback={<LogOut class="size-3.5" />}>
          <Loader2 class="size-3.5 animate-spin" />
        </Show>
        Sign out
      </button>
    </div>
  );
}

export { SessionControls };
