import { For, Show } from "solid-js";
import type { Service } from "../../../lib/types";

function VolumesList(props: { service: Service }) {
  const volumes = () => props.service.deploy.volumes ?? [];

  return (
    <Show when={volumes().length > 0}>
      <div>
        <h4 class="text-xs font-medium text-gray-400 mb-2">Volumes</h4>
        <div class="bg-white rounded-lg border border-gray-200 divide-y divide-gray-100">
          <For each={volumes()}>
            {(volume) => (
              <div class="px-4 py-2.5 flex items-baseline justify-between gap-6">
                <span class="text-xs font-medium text-gray-700 shrink-0 truncate">
                  {volume.hostPath}
                </span>
                <span class="text-xs text-gray-600 text-right truncate tabular-nums">
                  {volume.mountPath}
                  <Show when={volume.readOnly}>
                    <span class="ml-1.5 text-xs text-gray-400">(ro)</span>
                  </Show>
                  <Show when={volume.owner}>
                    {(owner) => (
                      <span class="ml-1.5 text-xs text-gray-400">
                        {owner().uid}:{owner().gid ?? owner().uid}
                      </span>
                    )}
                  </Show>
                </span>
              </div>
            )}
          </For>
        </div>
      </div>
    </Show>
  );
}

export { VolumesList };
