import clsx from "clsx";
import { useMutation, useQueryClient } from "@tanstack/solid-query";
import type { Service } from "../../../lib/types";
import { freezeService } from "../../../lib/api";
import { queryKeys } from "../../../lib/queries";

function FreezeToggle(props: { service: Service }) {
  const queryClient = useQueryClient();
  const mutation = useMutation(() => ({
    mutationFn: (frozen: boolean) => freezeService(props.service.id, frozen),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: queryKeys.services })
  }));

  return (
    <div>
      <h4 class="text-xs font-medium text-gray-400 mb-2">Deploy freeze</h4>
      <div class="bg-white rounded-lg border border-gray-200 px-4 py-3 flex items-center justify-between">
        <div>
          <p class="text-sm text-gray-700">
            {props.service.deployFrozen ? "Deploys are frozen" : "Deploys are active"}
          </p>
          <p class="text-xs text-gray-400 mt-0.5">
            {props.service.deployFrozen
              ? "Auto-deploys from git watch are paused. Manual deploys require force."
              : "Services will auto-deploy when new commits are detected."}
          </p>
        </div>
        <button
          type="button"
          onClick={() => mutation.mutate(!props.service.deployFrozen)}
          disabled={mutation.isPending}
          class={clsx("px-3 py-1.5 text-xs font-medium rounded-lg transition-colors outline-none", {
            "bg-amber-100 text-amber-700 hover:bg-amber-200": props.service.deployFrozen,
            "bg-gray-100 text-gray-600 hover:bg-gray-200": !props.service.deployFrozen
          })}
        >
          {props.service.deployFrozen ? "Unfreeze" : "Freeze"}
        </button>
      </div>
    </div>
  );
}

export { FreezeToggle };
