import clsx from "clsx";
import { Show } from "solid-js";
import { useMutation, useQueryClient } from "@tanstack/solid-query";
import type { Service } from "../../../lib/types";
import { freezeService } from "../../../lib/api";
import { queryKeys } from "../../../lib/queries";

function FreezeToggle(props: { service: Service }) {
  const queryClient = useQueryClient();
  const frozen = () => props.service.status.rollout === "frozen";
  const mutation = useMutation(() => ({
    mutationFn: (next: boolean) => freezeService(props.service, next),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: queryKeys.services })
  }));

  return (
    <div class="px-4 py-2.5 flex items-center justify-between gap-4">
      <div class="flex min-w-0 items-baseline gap-2">
        <span class="text-xs font-medium text-gray-700 shrink-0">Deploy freeze</span>
        <Show when={frozen()}>
          <span class="text-[11px] text-amber-600 truncate">
            auto-deploys paused · manual deploys require force
          </span>
        </Show>
      </div>
      <button
        type="button"
        onClick={() => mutation.mutate(!frozen())}
        disabled={mutation.isPending}
        class={clsx(
          "shrink-0 px-3 py-1.5 text-xs font-medium rounded-lg transition-colors outline-none",
          {
            "bg-amber-100 text-amber-700 hover:bg-amber-200": frozen(),
            "bg-gray-100 text-gray-600 hover:bg-gray-200": !frozen()
          }
        )}
      >
        {frozen() ? "Unfreeze" : "Freeze"}
      </button>
    </div>
  );
}

export { FreezeToggle };
