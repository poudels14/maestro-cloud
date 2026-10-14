import { For, Match, Show, Switch } from "solid-js";
import { createStore } from "solid-js/store";
import { useQuery } from "@tanstack/solid-query";
import clsx from "clsx";
import type { Maintenance } from "../../lib/types";
import { clusterInfoQuery } from "../../lib/queries";
import { startMaintenance, unfreezeCluster } from "../../lib/api";
import { timeAgo } from "../../lib/ui";
import { ConfirmDialog } from "./ConfirmDialog";

const STALE_RUN_MS = 30_000;

function MaintenanceSection() {
  const cluster = useQuery(() => clusterInfoQuery({ pollForMaintenance: true }));
  const [state, setState] = createStore({
    targetVersion: "",
    confirming: null as "restart" | "upgrade" | null,
    busy: false,
    error: null as string | null
  });

  const run = () => cluster.data?.maintenance ?? null;
  const active = () => {
    const current = run();
    return current !== null && current.phase !== "succeeded" && current.phase !== "failed";
  };
  const stalled = () => {
    const current = run();
    return active() && current !== null && Date.now() - current.updatedAtMs > STALE_RUN_MS;
  };

  const perform = async (action: () => Promise<void>) => {
    setState({ busy: true, error: null });
    try {
      await action();
      await cluster.refetch();
    } catch (cause) {
      setState("error", cause instanceof Error ? cause.message : String(cause));
    } finally {
      setState({ busy: false, confirming: null });
    }
  };

  const confirm = () => {
    if (state.confirming === "upgrade") {
      void perform(() =>
        startMaintenance({ kind: "upgrade", targetVersion: state.targetVersion.trim() })
      );
    } else {
      void perform(() => startMaintenance({ kind: "restart" }));
    }
  };

  return (
    <div>
      <div class="flex flex-wrap items-end justify-between gap-3 mb-3">
        <div>
          <h2 class="text-lg font-semibold text-gray-900">Maintenance</h2>
          <p class="text-xs text-gray-500 mt-1">
            Nodes are drained and updated one at a time, the leader last. Deploys are frozen until
            the run ends.
          </p>
        </div>
        <span class="text-xs text-gray-400">Running {cluster.data?.version ?? "–"}</span>
      </div>
      <div class="rounded-xl border border-gray-200 bg-white">
        <form
          class="flex flex-wrap items-center gap-2 border-b border-gray-100 px-4 py-3"
          onSubmit={(event) => {
            event.preventDefault();
            setState("confirming", "upgrade");
          }}
        >
          <label class="sr-only" for="target-version">
            Target version
          </label>
          <input
            id="target-version"
            value={state.targetVersion}
            onInput={(event) => setState("targetVersion", event.currentTarget.value)}
            placeholder="Version, e.g. 3.1.0"
            class="h-8 w-44 rounded-md border border-gray-200 px-2.5 font-mono text-xs text-gray-800 outline-none focus:border-indigo-400"
          />
          <button
            type="submit"
            disabled={active() || state.busy || state.targetVersion.trim() === ""}
            class="h-8 rounded-md bg-indigo-600 px-3 text-xs font-medium text-white hover:bg-indigo-700 disabled:cursor-not-allowed disabled:opacity-50"
          >
            Upgrade cluster
          </button>
          <button
            type="button"
            disabled={active() || state.busy}
            onClick={() => setState("confirming", "restart")}
            class="h-8 rounded-md border border-gray-200 px-3 text-xs font-medium text-gray-700 hover:bg-gray-50 disabled:cursor-not-allowed disabled:opacity-50"
          >
            Restart every node
          </button>
          <Show when={stalled()}>
            <button
              type="button"
              disabled={state.busy}
              onClick={() => perform(unfreezeCluster)}
              class="ml-auto h-8 rounded-md border border-red-200 px-3 text-xs font-medium text-red-700 hover:bg-red-50 disabled:opacity-50"
            >
              Stop run and unfreeze
            </button>
          </Show>
        </form>
        <Show when={state.error}>
          {(message) => (
            <div class="border-b border-gray-100 bg-red-50 px-4 py-2 text-xs text-red-700">
              {message()}
            </div>
          )}
        </Show>
        <Show
          when={run()}
          fallback={
            <div class="px-4 py-6 text-xs text-gray-400">No upgrade or restart has run yet.</div>
          }
        >
          {(current) => <RunSummary run={current()} />}
        </Show>
      </div>
      <ConfirmDialog
        open={state.confirming !== null}
        title={
          state.confirming === "upgrade"
            ? `Upgrade to ${state.targetVersion.trim()}`
            : "Restart every node"
        }
        description={
          <Switch>
            <Match when={state.confirming === "upgrade"}>
              Each node updates /etc/maestro, rebuilds NixOS and reboots. This can take a while per
              node.
            </Match>
            <Match when={state.confirming === "restart"}>
              Maestro restarts on each node in turn. Containers keep running.
            </Match>
          </Switch>
        }
        confirmLabel={state.confirming === "upgrade" ? "Start upgrade" : "Start restart"}
        destructive={false}
        busy={state.busy}
        onConfirm={confirm}
        onCancel={() => setState("confirming", null)}
      />
    </div>
  );
}

function RunSummary(props: { run: Maintenance.Run }) {
  const title = () =>
    props.run.kind === "upgrade" ? `Upgrade to ${props.run.targetVersion}` : "Rolling restart";

  return (
    <div class="px-4 py-3">
      <div class="flex flex-wrap items-baseline justify-between gap-2">
        <span class="text-sm font-medium text-gray-800">{title()}</span>
        <span
          class={clsx("text-xs font-medium", {
            "text-emerald-700": props.run.phase === "succeeded",
            "text-red-700": props.run.phase === "failed",
            "text-amber-700": props.run.phase !== "succeeded" && props.run.phase !== "failed"
          })}
        >
          {phaseLabel(props.run.phase)}, updated {timeAgo(props.run.updatedAtMs)}
        </span>
      </div>
      <Show when={props.run.failure}>
        {(failure) => <p class="mt-1 text-xs text-red-700">{failure()}</p>}
      </Show>
      <ol class="mt-3 flex flex-wrap gap-2">
        <For each={props.run.nodes}>
          {(step, index) => (
            <li
              class={clsx("rounded-md border px-2.5 py-1.5 text-xs", {
                "border-emerald-200 bg-emerald-50 text-emerald-800": step.status === "succeeded",
                "border-red-200 bg-red-50 text-red-800": step.status === "failed",
                "border-amber-200 bg-amber-50 text-amber-800":
                  step.status !== "succeeded" &&
                  step.status !== "failed" &&
                  step.status !== "pending",
                "border-gray-200 text-gray-500": step.status === "pending"
              })}
              title={step.error ?? undefined}
            >
              <span class="text-gray-400 tabular-nums">{index() + 1}.</span> {step.nodeName}
              <span class="ml-1.5 opacity-70">{step.status}</span>
            </li>
          )}
        </For>
      </ol>
    </div>
  );
}

function phaseLabel(phase: Maintenance.Phase): string {
  const labels: Record<Maintenance.Phase, string> = {
    draining: "Draining",
    requested: "Requesting",
    verifying: "Waiting for node",
    restoring: "Restoring",
    succeeded: "Finished",
    failed: "Failed"
  };
  return labels[phase];
}

export { MaintenanceSection };
