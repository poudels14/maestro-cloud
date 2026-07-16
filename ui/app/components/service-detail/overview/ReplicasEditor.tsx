import { createEffect, createSignal, Show } from "solid-js";
import { useMutation, useQueryClient } from "@tanstack/solid-query";
import type { Service } from "../../../lib/types";
import { clearServiceReplicasOverride, setServiceReplicas } from "../../../lib/api";
import { queryKeys } from "../../../lib/queries";

const MAX_REPLICAS = 25;

function ReplicasEditor(props: { service: Service }) {
  const queryClient = useQueryClient();
  const configuredReplicas = () => props.service.deploy.replicas ?? 1;
  const effectiveReplicas = () => props.service.replicasOverride ?? configuredReplicas();
  const hasWritableVolume = () => (props.service.deploy.volumes ?? []).some((v) => !v.readOnly);
  const scalingLocked = () => hasWritableVolume();

  const [replicasInput, setReplicasInput] = createSignal(effectiveReplicas());
  const [replicasError, setReplicasError] = createSignal<string | null>(null);

  createEffect(() => {
    setReplicasInput(effectiveReplicas());
  });

  const invalidate = () => queryClient.invalidateQueries({ queryKey: queryKeys.services });

  const applyMutation = useMutation(() => ({
    mutationFn: (next: number) => setServiceReplicas(props.service.id, next),
    onSuccess: invalidate,
    onError: (err) => setReplicasError(err instanceof Error ? err.message : "failed to update")
  }));
  const revertMutation = useMutation(() => ({
    mutationFn: () => clearServiceReplicasOverride(props.service.id),
    onSuccess: invalidate,
    onError: (err) => setReplicasError(err instanceof Error ? err.message : "failed to revert")
  }));
  const busy = () => applyMutation.isPending || revertMutation.isPending;

  const applyReplicas = () => {
    setReplicasError(null);
    const next = Number(replicasInput());
    if (!Number.isInteger(next) || next < 1) {
      setReplicasError("replicas must be an integer >= 1");
      return;
    }
    if (next < configuredReplicas()) {
      setReplicasError(`cannot go below configured (${configuredReplicas()})`);
      return;
    }
    if (next > MAX_REPLICAS) {
      setReplicasError(`cannot exceed ${MAX_REPLICAS}`);
      return;
    }
    applyMutation.mutate(next);
  };

  return (
    <div class="px-4 py-2.5 flex items-center justify-between gap-4">
      <div class="flex items-baseline gap-2 min-w-0">
        <span class="text-xs font-medium text-gray-700 shrink-0">Replicas</span>
        <Show when={props.service.replicasOverride}>
          <span class="text-[11px] text-amber-600">override · config: {configuredReplicas()}</span>
        </Show>
        <Show when={scalingLocked() && !replicasError()}>
          <span class="text-[11px] text-gray-400 truncate">locked at 1 (writable volume)</span>
        </Show>
        <Show when={replicasError()}>
          <span class="text-[11px] text-red-600 truncate">{replicasError()}</span>
        </Show>
      </div>
      <div class="flex items-center gap-1 shrink-0">
        <Show when={replicasInput() !== effectiveReplicas()}>
          <button
            type="button"
            onClick={applyReplicas}
            disabled={busy()}
            class="mr-1 px-2 py-1 text-xs font-medium rounded-md bg-indigo-100 text-indigo-700 hover:bg-indigo-200 disabled:bg-gray-100 disabled:text-gray-400 transition-colors"
          >
            Save
          </button>
        </Show>
        <button
          type="button"
          onClick={() => setReplicasInput(Math.max(configuredReplicas(), replicasInput() - 1))}
          disabled={busy() || replicasInput() <= configuredReplicas()}
          class="size-6 flex items-center justify-center text-gray-500 hover:text-gray-800 hover:bg-gray-100 disabled:opacity-30 rounded-md"
        >
          −
        </button>
        <input
          type="number"
          min={configuredReplicas()}
          max={MAX_REPLICAS}
          value={replicasInput()}
          onInput={(e) => setReplicasInput(Number(e.currentTarget.value))}
          class="w-12 text-center text-sm font-mono text-gray-800 border border-gray-200 rounded-md py-0.5 outline-none focus:border-indigo-300 [appearance:textfield] [&::-webkit-inner-spin-button]:appearance-none [&::-webkit-outer-spin-button]:appearance-none"
          disabled={busy()}
        />
        <button
          type="button"
          onClick={() => setReplicasInput(Math.min(MAX_REPLICAS, replicasInput() + 1))}
          disabled={busy() || replicasInput() >= MAX_REPLICAS || scalingLocked()}
          title={scalingLocked() ? "writable volume — cannot scale" : undefined}
          class="size-6 flex items-center justify-center text-gray-500 hover:text-gray-800 hover:bg-gray-100 disabled:opacity-30 rounded-md"
        >
          +
        </button>
        <button
          type="button"
          onClick={() => {
            setReplicasError(null);
            revertMutation.mutate();
          }}
          disabled={busy() || !props.service.replicasOverride}
          title="Revert to configured value"
          class="ml-1 size-6 flex items-center justify-center text-gray-400 hover:text-gray-700 hover:bg-gray-100 disabled:opacity-30 rounded-md"
        >
          ↺
        </button>
      </div>
    </div>
  );
}

export { ReplicasEditor };
