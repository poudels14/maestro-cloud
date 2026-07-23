import { For, Show, createSignal } from "solid-js";
import { useQuery } from "@maestro/sdk";
import clsx from "clsx";
import type { ClusterApi } from "./api";
import { parseAdmissionRequest } from "./lib/admissions";
import { clusterAdmissionsQuery } from "./queries";

const admissionGridClass =
  "grid min-w-[43rem] grid-cols-[minmax(9rem,1fr)_minmax(16rem,2fr)_7rem_minmax(10rem,1fr)] gap-3";

function AdmissionsPage(props: { api: ClusterApi }) {
  const admissions = useQuery(() => clusterAdmissionsQuery(props.api));
  const [nodeId, setNodeId] = createSignal("");
  const [fingerprint, setFingerprint] = createSignal("");
  const [busy, setBusy] = createSignal(false);
  const [error, setError] = createSignal<string | null>(null);

  const approve = async () => {
    const parsed = parseAdmissionRequest(nodeId(), fingerprint());
    if ("error" in parsed) {
      setError(parsed.error);
      return;
    }

    setBusy(true);
    setError(null);
    try {
      await props.api.approveAdmission(parsed.request);
      setNodeId("");
      setFingerprint("");
      await admissions.refetch();
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : String(cause));
    } finally {
      setBusy(false);
    }
  };

  const queryError = () => {
    const cause = admissions.error;
    return cause instanceof Error ? cause.message : cause ? String(cause) : null;
  };

  return (
    <section class="space-y-8">
      <div>
        <h1 class="text-lg font-semibold text-gray-900">Node admissions</h1>
        <p class="mt-1 text-xs text-gray-500">
          Approve a prepared join key before the node requests cluster credentials.
        </p>
      </div>

      <form
        class="rounded-xl border border-gray-200 bg-white p-4"
        onSubmit={(event) => {
          event.preventDefault();
          void approve();
        }}
      >
        <div class="mb-4 rounded-lg border border-amber-200 bg-amber-50 px-3 py-2 text-xs text-amber-800">
          Compare the fingerprint with the output of <code>cluster prepare-join</code> over a
          trusted channel. A fingerprint mismatch can admit the wrong machine.
        </div>
        <div class="grid gap-3 lg:grid-cols-[minmax(10rem,1fr)_minmax(20rem,2fr)_auto] lg:items-end">
          <label class="grid gap-1.5 text-xs font-medium text-gray-700">
            Node ID
            <input
              type="text"
              autocomplete="off"
              value={nodeId()}
              onInput={(event) => setNodeId(event.currentTarget.value)}
              placeholder="worker-a"
              class="rounded-md border border-gray-300 px-3 py-2 font-mono text-xs font-normal outline-none focus:border-gray-500"
            />
          </label>
          <label class="grid gap-1.5 text-xs font-medium text-gray-700">
            Join key SHA-256
            <input
              type="text"
              autocomplete="off"
              spellcheck={false}
              value={fingerprint()}
              onInput={(event) => setFingerprint(event.currentTarget.value)}
              placeholder={"0".repeat(64)}
              class="rounded-md border border-gray-300 px-3 py-2 font-mono text-xs font-normal outline-none focus:border-gray-500"
            />
          </label>
          <button
            type="submit"
            disabled={busy()}
            class="rounded-md bg-gray-900 px-4 py-2 text-xs font-medium text-white hover:bg-gray-700 disabled:cursor-not-allowed disabled:opacity-50"
          >
            {busy() ? "Approving…" : "Approve node"}
          </button>
        </div>
        <Show when={error()}>
          {(message) => <p class="mt-3 text-xs text-red-700">{message()}</p>}
        </Show>
      </form>

      <div>
        <div class="mb-3 flex items-end justify-between gap-3">
          <div>
            <h2 class="text-sm font-semibold text-gray-900">Approval history</h2>
            <p class="mt-1 text-xs text-gray-500">Persisted approvals and completed admissions.</p>
          </div>
          <span class="text-xs tabular-nums text-gray-400">
            {admissions.data?.length ?? 0} approvals
          </span>
        </div>
        <Show when={queryError()}>
          {(message) => (
            <div class="mb-3 rounded-lg border border-red-200 bg-red-50 px-3 py-2 text-xs text-red-700">
              {message()}
            </div>
          )}
        </Show>
        <div class="overflow-x-auto rounded-xl border border-gray-200 bg-white">
          <div
            class={`${admissionGridClass} border-b border-gray-200 bg-gray-50 px-4 py-2 text-[11px] font-medium text-gray-500`}
          >
            <span>Node</span>
            <span>Fingerprint</span>
            <span>Status</span>
            <span>Last transition</span>
          </div>
          <For each={admissions.data ?? []}>
            {(approval) => (
              <div
                class={`${admissionGridClass} items-center border-b border-gray-100 px-4 py-3 text-xs last:border-b-0`}
              >
                <span class="truncate font-medium text-gray-800">{approval.nodeId}</span>
                <span
                  class="truncate font-mono text-[11px] text-gray-600"
                  title={approval.publicKeySha256}
                >
                  {approval.publicKeySha256}
                </span>
                <span
                  class={clsx("w-fit rounded-full px-2 py-1 text-[10px] font-medium", {
                    "bg-emerald-50 text-emerald-700": approval.state === "admitted",
                    "bg-amber-50 text-amber-700": approval.state === "approved"
                  })}
                >
                  {approval.state === "admitted" ? "Admitted" : "Awaiting join"}
                </span>
                <span class="text-[11px] text-gray-500">
                  {new Date(
                    approval.admittedAtUnixMs ?? approval.approvedAtUnixMs
                  ).toLocaleString()}
                </span>
              </div>
            )}
          </For>
          <Show when={!admissions.isLoading && (admissions.data?.length ?? 0) === 0}>
            <div class="px-4 py-12 text-center text-sm text-gray-400">
              No node admissions have been approved.
            </div>
          </Show>
        </div>
      </div>
    </section>
  );
}

export { AdmissionsPage };
