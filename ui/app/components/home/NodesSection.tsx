import { For, Show, createSignal } from "solid-js";
import { useQuery } from "@tanstack/solid-query";
import clsx from "clsx";
import { clusterNodesQuery, clusterUpgradeQuery } from "../../lib/queries";
import { setNodeDrain, startClusterUpgrade, unfreezeClusterUpgrade } from "../../lib/api";

function NodesSection() {
  const nodes = useQuery(() => clusterNodesQuery());
  const [busy, setBusy] = createSignal<string | null>(null);
  const [error, setError] = createSignal<string | null>(null);

  const changeDrain = async (nodeId: string, drain: boolean) => {
    setBusy(nodeId);
    setError(null);
    try {
      await setNodeDrain(nodeId, drain);
      await nodes.refetch();
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : String(cause));
    } finally {
      setBusy(null);
    }
  };

  return (
    <section class="space-y-8">
      <UpgradePanel />
      <div>
        <div class="flex items-end justify-between gap-3 mb-3">
          <div>
            <h1 class="text-lg font-semibold text-gray-900">Cluster nodes</h1>
            <p class="text-xs text-gray-500 mt-1">Control-plane health and workload placement.</p>
          </div>
          <span class="text-xs font-mono text-gray-400">{nodes.data?.length ?? 0} nodes</span>
        </div>
        <Show when={error()}>
          {(message) => (
            <div class="mb-3 rounded-lg border border-red-200 bg-red-50 px-3 py-2 text-xs text-red-700">
              {message()}
            </div>
          )}
        </Show>
        <div class="overflow-hidden rounded-xl border border-gray-200 bg-white">
          <div class="grid grid-cols-[minmax(9rem,1.4fr)_7rem_minmax(8rem,1fr)_minmax(8rem,1fr)_7rem] gap-3 border-b border-gray-200 bg-gray-50 px-4 py-2 text-[10px] font-medium uppercase tracking-wide text-gray-400">
            <span>Node</span>
            <span>Role</span>
            <span>Control</span>
            <span>Workload subnet</span>
            <span class="text-right">Action</span>
          </div>
          <For each={nodes.data ?? []}>
            {(node) => (
              <div class="grid grid-cols-[minmax(9rem,1.4fr)_7rem_minmax(8rem,1fr)_minmax(8rem,1fr)_7rem] items-center gap-3 border-b border-gray-100 px-4 py-3 text-xs last:border-b-0">
                <div class="min-w-0">
                  <div class="flex items-center gap-2">
                    <span
                      class={clsx("size-2 rounded-full", {
                        "bg-emerald-500": node.alive && node.dataPlaneReady,
                        "bg-amber-500": node.alive && !node.dataPlaneReady,
                        "bg-gray-300": !node.alive
                      })}
                    />
                    <span class="truncate font-medium text-gray-800">{node.hostname}</span>
                  </div>
                  <div class="mt-1 truncate font-mono text-[10px] text-gray-400">{node.nodeId}</div>
                  <Show when={node.state.reason || node.dataPlaneError}>
                    <div class="mt-1 truncate text-[10px] text-amber-600">
                      {node.state.reason || node.dataPlaneError}
                    </div>
                  </Show>
                </div>
                <span class="text-gray-600">{node.role}</span>
                <span class="font-mono text-[11px] text-gray-600">
                  {node.clusterHostIp}:{node.clusterApiPort}
                </span>
                <span class="font-mono text-[11px] text-gray-600">{node.subnet}</span>
                <button
                  type="button"
                  disabled={!node.alive || busy() === node.nodeId}
                  onClick={() => changeDrain(node.nodeId, !node.state.unschedulable)}
                  class={clsx(
                    "justify-self-end rounded-md border px-2 py-1 text-[11px] font-medium disabled:cursor-not-allowed disabled:opacity-50",
                    node.state.unschedulable
                      ? "border-emerald-200 text-emerald-700 hover:bg-emerald-50"
                      : "border-amber-200 text-amber-700 hover:bg-amber-50"
                  )}
                >
                  {node.state.unschedulable ? "Restore" : "Drain"}
                </button>
              </div>
            )}
          </For>
          <Show when={!nodes.isLoading && (nodes.data?.length ?? 0) === 0}>
            <div class="px-4 py-12 text-center text-sm text-gray-400">No cluster nodes found.</div>
          </Show>
        </div>
      </div>
    </section>
  );
}

function UpgradePanel() {
  const upgrade = useQuery(() => clusterUpgradeQuery());
  const [version, setVersion] = createSignal("");
  const [busy, setBusy] = createSignal(false);
  const [error, setError] = createSignal<string | null>(null);
  const active = () => {
    const phase = upgrade.data?.phase;
    return phase != null && phase !== "succeeded" && phase !== "failed";
  };

  const start = async () => {
    const target = version().trim();
    if (!target) return;
    setBusy(true);
    setError(null);
    try {
      await startClusterUpgrade(target);
      setVersion("");
      await upgrade.refetch();
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : String(cause));
    } finally {
      setBusy(false);
    }
  };

  const unfreeze = async () => {
    const run = upgrade.data;
    if (!run) return;
    setBusy(true);
    setError(null);
    try {
      await unfreezeClusterUpgrade(run.runId);
      await upgrade.refetch();
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : String(cause));
    } finally {
      setBusy(false);
    }
  };

  return (
    <div>
      <div class="mb-3">
        <h1 class="text-lg font-semibold text-gray-900">Rolling upgrade</h1>
        <p class="mt-1 text-xs text-gray-500">
          Drain, upgrade, and verify one node at a time. Deploys remain frozen while a run is
          active.
        </p>
      </div>
      <Show when={error()}>
        {(message) => (
          <div class="mb-3 rounded-lg border border-red-200 bg-red-50 px-3 py-2 text-xs text-red-700">
            {message()}
          </div>
        )}
      </Show>
      <div class="rounded-xl border border-gray-200 bg-white p-4">
        <Show
          when={upgrade.data}
          fallback={
            <div class="flex flex-wrap items-center gap-2">
              <input
                value={version()}
                onInput={(event) => setVersion(event.currentTarget.value)}
                placeholder="Target version, e.g. 0.3.0"
                class="min-w-56 flex-1 rounded-md border border-gray-200 px-3 py-2 text-xs font-mono outline-none focus:border-gray-400"
              />
              <button
                type="button"
                disabled={busy() || !version().trim()}
                onClick={start}
                class="rounded-md bg-gray-900 px-3 py-2 text-xs font-medium text-white disabled:opacity-40"
              >
                Start upgrade
              </button>
            </div>
          }
        >
          {(run) => (
            <div>
              <div class="flex flex-wrap items-center justify-between gap-3">
                <div>
                  <div class="flex items-center gap-2">
                    <span class="text-sm font-medium text-gray-800">
                      Maestro {run().targetVersion}
                    </span>
                    <span
                      class={clsx("rounded px-1.5 py-0.5 text-[10px] font-medium", {
                        "bg-emerald-50 text-emerald-700": run().phase === "succeeded",
                        "bg-red-50 text-red-700": run().phase === "failed",
                        "bg-amber-50 text-amber-700": active()
                      })}
                    >
                      {run().phase}
                    </span>
                  </div>
                  <div class="mt-1 font-mono text-[10px] text-gray-400">{run().runId}</div>
                </div>
                <div class="flex items-center gap-2">
                  <Show when={!active()}>
                    <input
                      value={version()}
                      onInput={(event) => setVersion(event.currentTarget.value)}
                      placeholder="Next version"
                      class="w-32 rounded-md border border-gray-200 px-2 py-1.5 text-[11px] font-mono outline-none"
                    />
                    <button
                      type="button"
                      disabled={busy() || !version().trim()}
                      onClick={start}
                      class="rounded-md bg-gray-900 px-2.5 py-1.5 text-[11px] font-medium text-white disabled:opacity-40"
                    >
                      New run
                    </button>
                  </Show>
                  <Show when={active()}>
                    <button
                      type="button"
                      disabled={busy()}
                      onClick={unfreeze}
                      title="Accepted only when the orchestrator has stopped advancing"
                      class="rounded-md border border-red-200 px-2.5 py-1.5 text-[11px] font-medium text-red-700 hover:bg-red-50 disabled:opacity-40"
                    >
                      Manual unfreeze
                    </button>
                  </Show>
                </div>
              </div>
              <div class="mt-4 grid gap-2 sm:grid-cols-2">
                <For each={run().nodes}>
                  {(node) => (
                    <div class="flex items-center justify-between rounded-lg border border-gray-100 px-3 py-2 text-xs">
                      <div class="min-w-0">
                        <div class="truncate font-medium text-gray-700">{node.hostname}</div>
                        <div class="font-mono text-[10px] text-gray-400">
                          {node.fromVersion} · {node.role}
                        </div>
                      </div>
                      <span class="ml-2 text-[10px] font-medium text-gray-500">{node.status}</span>
                    </div>
                  )}
                </For>
              </div>
              <Show when={run().failure}>
                {(failure) => <p class="mt-3 text-xs text-red-700">{failure()}</p>}
              </Show>
              <Show when={run().history.length > 0}>
                <div class="mt-4 max-h-32 space-y-1 overflow-y-auto border-t border-gray-100 pt-3">
                  <For each={run().history.slice(-8)}>
                    {(event) => (
                      <div class="text-[10px] text-gray-500">
                        <span class="font-mono text-gray-400">{event.nodeId ?? "cluster"}</span>
                        {" — "}
                        {event.message}
                      </div>
                    )}
                  </For>
                </div>
              </Show>
            </div>
          )}
        </Show>
      </div>
    </div>
  );
}

export { NodesSection };
