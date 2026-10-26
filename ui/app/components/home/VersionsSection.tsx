import { For, Show } from "solid-js";
import { useQuery } from "../../lib/useQuery";
import { clusterInfoQuery } from "../../lib/queries";
import { SectionHeader } from "../../lib/ui";

function VersionsSection() {
  const info = useQuery(() => clusterInfoQuery());

  return (
    <Show when={info.data}>
      {(data) => (
        <div>
          <SectionHeader class="mb-4">Versions</SectionHeader>
          <div class="bg-white rounded-lg border border-gray-200 divide-y divide-gray-100">
            <For each={[{ name: "Maestro", version: data().version }, ...data().versions]}>
              {(component) => (
                <div class="px-4 py-2.5 flex items-baseline justify-between gap-6">
                  <span class="text-xs font-medium text-gray-700 shrink-0">{component.name}</span>
                  <span class="font-mono text-xs text-gray-600 text-right break-all min-w-0">
                    {component.version}
                  </span>
                </div>
              )}
            </For>
          </div>
        </div>
      )}
    </Show>
  );
}

export { VersionsSection };
