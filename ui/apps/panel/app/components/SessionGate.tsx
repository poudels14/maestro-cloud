import { useLocation, useNavigate } from "@tanstack/solid-router";
import { createEffect, createSignal, onCleanup, onMount, Show, type JSX } from "solid-js";
import { Loader2, RefreshCw } from "lucide-solid";
import { queryClient } from "../lib/queryClient";
import { apiClient, isUnauthenticated, SESSION_UNAUTHENTICATED_EVENT } from "../lib/client";

type SessionState = "checking" | "authenticated" | "unavailable";

function SessionGate(props: { children: JSX.Element }) {
  const location = useLocation();
  const navigate = useNavigate();
  const [state, setState] = createSignal<SessionState>("checking");
  const [retry, setRetry] = createSignal(0);
  let requestSequence = 0;

  const openLogin = () => {
    requestSequence += 1;
    queryClient.clear();
    void navigate({ to: "/login", replace: true });
  };

  onMount(() => {
    window.addEventListener(SESSION_UNAUTHENTICATED_EVENT, openLogin);
    onCleanup(() => window.removeEventListener(SESSION_UNAUTHENTICATED_EVENT, openLogin));
  });

  createEffect(() => {
    const path = location().pathname;
    retry();
    if (path === "/login") {
      requestSequence += 1;
      return;
    }

    const sequence = ++requestSequence;
    setState("checking");
    void apiClient()
      .getClusterInfo()
      .then(() => {
        if (sequence === requestSequence) setState("authenticated");
      })
      .catch((error: unknown) => {
        if (sequence !== requestSequence) return;
        if (isUnauthenticated(error)) {
          openLogin();
        } else {
          setState("unavailable");
        }
      });
  });

  return (
    <Show when={location().pathname !== "/login"} fallback={props.children}>
      <Show
        when={state() === "authenticated"}
        fallback={<SessionCheck state={state()} retry={setRetry} />}
      >
        {props.children}
      </Show>
    </Show>
  );
}

function SessionCheck(props: {
  state: SessionState;
  retry: (update: (value: number) => number) => void;
}) {
  return (
    <main class="min-h-screen bg-gray-50 flex items-center justify-center px-6">
      <Show
        when={props.state === "unavailable"}
        fallback={
          <div class="flex items-center gap-2 text-sm text-gray-500">
            <Loader2 class="size-4 animate-spin" />
            Checking session…
          </div>
        }
      >
        <div class="max-w-sm text-center">
          <h1 class="text-lg font-semibold text-gray-900">Controller unavailable</h1>
          <p class="mt-1 text-sm text-gray-500">
            Maestro could not verify this browser session. Check the controller and try again.
          </p>
          <button
            type="button"
            onClick={() => props.retry((value) => value + 1)}
            class="mt-4 inline-flex items-center gap-2 rounded-md bg-gray-900 px-3 py-2 text-sm font-medium text-white hover:bg-gray-700"
          >
            <RefreshCw class="size-3.5" />
            Try again
          </button>
        </div>
      </Show>
    </main>
  );
}

export { SessionGate };
