import { createFileRoute, useNavigate } from "@tanstack/solid-router";
import { createSignal, Show } from "solid-js";
import { KeyRound, Loader2, Monitor } from "lucide-solid";
import { apiClient, apiRequestError } from "../lib/client";

export const Route = createFileRoute("/login")({
  component: LoginPage
});

function LoginPage() {
  const navigate = useNavigate();
  const [token, setToken] = createSignal("");
  const [pending, setPending] = createSignal(false);
  const [error, setError] = createSignal<string | null>(null);

  const submit = async (event: SubmitEvent) => {
    event.preventDefault();
    const operatorToken = token().trim();
    if (!operatorToken) {
      setError("Enter an operator token.");
      return;
    }
    setPending(true);
    setError(null);
    try {
      await apiClient().createBrowserSession(operatorToken);
      setToken("");
      await navigate({ to: "/", replace: true });
    } catch (failure) {
      setError(apiRequestError(failure, "Could not start a browser session").message);
      setPending(false);
    }
  };

  return (
    <main class="min-h-screen bg-gray-50 flex items-center justify-center px-6 py-12">
      <section class="w-full max-w-sm rounded-xl border border-gray-200 bg-white p-6 shadow-sm">
        <div class="mb-6 flex items-center gap-3">
          <div class="flex size-9 items-center justify-center rounded-lg bg-brand shadow-sm">
            <Monitor class="size-5 text-white" />
          </div>
          <div>
            <h1 class="text-lg font-semibold text-gray-900">Sign in to Maestro</h1>
            <p class="text-xs text-gray-500">Start a secure controller session.</p>
          </div>
        </div>
        <form onSubmit={(event) => void submit(event)}>
          <label for="operator-token" class="block text-sm font-medium text-gray-700">
            Operator token
          </label>
          <div class="relative mt-1.5">
            <KeyRound class="pointer-events-none absolute left-3 top-2.5 size-4 text-gray-400" />
            <input
              id="operator-token"
              type="password"
              autocomplete="off"
              spellcheck={false}
              value={token()}
              onInput={(event) => setToken(event.currentTarget.value)}
              disabled={pending()}
              autofocus
              class="w-full rounded-md border border-gray-300 py-2 pl-9 pr-3 font-mono text-sm outline-none focus:border-brand focus:ring-2 focus:ring-brand-ring disabled:bg-gray-50"
            />
          </div>
          <p class="mt-2 text-xs leading-5 text-gray-500">
            The token is exchanged for an HttpOnly cookie and is never stored by the panel.
          </p>
          <Show when={error()}>
            {(message) => (
              <p role="alert" class="mt-3 rounded-md bg-red-50 px-3 py-2 text-xs text-red-700">
                {message()}
              </p>
            )}
          </Show>
          <button
            type="submit"
            disabled={pending()}
            class="mt-5 flex w-full items-center justify-center gap-2 rounded-md bg-brand px-3 py-2 text-sm font-medium text-white hover:bg-brand disabled:cursor-wait disabled:opacity-60"
          >
            <Show when={pending()}>
              <Loader2 class="size-4 animate-spin" />
            </Show>
            Sign in
          </button>
        </form>
      </section>
    </main>
  );
}
