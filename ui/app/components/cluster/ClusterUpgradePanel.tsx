import { createSignal } from "solid-js";
import { useQuery, useQueryClient } from "@tanstack/solid-query";
import { ArrowUpCircle, Loader2 } from "lucide-solid";
import { clusterInfoQuery } from "../../lib/queries";
import { startClusterUpgrade } from "../../lib/api";

function ClusterUpgradePanel() {
  const queryClient = useQueryClient();
  const cluster = useQuery(() => clusterInfoQuery({ pollWhenUpgrading: true, live: true }));
  const [version, setVersion] = createSignal("latest");
  const [busy, setBusy] = createSignal(false);
  const [error, setError] = createSignal<string | null>(null);

  const trigger = async () => {
    if (busy()) return;
    const target = version().trim() || "latest";
    if (!confirm(`Start rolling cluster upgrade to "${target}"?`)) return;
    setBusy(true);
    setError(null);
    try {
      await startClusterUpgrade(target);
      await queryClient.invalidateQueries({ queryKey: ["cluster"] });
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setBusy(false);
    }
  };

  return (
    <section>
      <div class="flex items-center gap-2 text-sm font-semibold text-gray-900 mb-3">
        <ArrowUpCircle class="size-4 text-gray-500" />
        Cluster upgrade
      </div>
      <div class="bg-white border border-gray-200 rounded-lg p-4 space-y-3">
        <div class="text-xs text-gray-500 leading-relaxed">
          Rolling upgrade walks every worker, then transfers leadership and upgrades this node.
          Drains each node, replaces the binary, verifies health, restores. Fails the whole run if
          any node can't be upgraded so you can investigate.
        </div>
        <div class="flex flex-wrap items-center gap-2">
          <label class="text-xs text-gray-700 font-medium" for="upgrade-version">
            Target version
          </label>
          <input
            id="upgrade-version"
            type="text"
            value={version()}
            onInput={(event) => setVersion(event.currentTarget.value)}
            class="text-sm font-mono px-2 py-1 border border-gray-300 rounded w-48"
            placeholder="latest"
            disabled={busy() || cluster.data?.upgrading}
          />
          <button
            type="button"
            onClick={trigger}
            disabled={busy() || cluster.data?.upgrading}
            class={
              busy() || cluster.data?.upgrading
                ? "text-xs font-medium px-3 py-1.5 rounded bg-gray-200 text-gray-500 cursor-not-allowed inline-flex items-center gap-1.5"
                : "text-xs font-medium px-3 py-1.5 rounded bg-indigo-600 text-white hover:bg-indigo-700 inline-flex items-center gap-1.5"
            }
          >
            {(busy() || cluster.data?.upgrading) && (
              <Loader2 class="size-3 animate-spin" />
            )}
            {cluster.data?.upgrading
              ? "Upgrading…"
              : busy()
                ? "Starting…"
                : "Start upgrade"}
          </button>
        </div>
        {error() && <div class="text-xs text-red-600">{error()}</div>}
      </div>
    </section>
  );
}

export { ClusterUpgradePanel };
