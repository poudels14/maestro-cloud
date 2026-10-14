import { For, Show } from "solid-js";
import { useQuery } from "@tanstack/solid-query";
import type { Service } from "../../../lib/types";
import { routesQuery } from "../../../lib/queries";

function IngressInfo(props: { service: Service }) {
  const ingress = () => props.service.ingress;
  const hosts = () => {
    const value = ingress();
    if (!value) return [];
    return [value.host, ...(value.hosts ?? [])].filter((host): host is string => !!host);
  };
  const routes = useQuery(() => ({
    ...routesQuery(),
    enabled: !!props.service.ingress
  }));
  const backends = () => routes.data?.routes[props.service.id]?.backends ?? [];

  return (
    <>
      <Show when={ingress()}>
        {(value) => (
          <div>
            <h4 class="text-xs font-medium text-gray-400 mb-2">Ingress</h4>
            <div class="bg-white rounded-lg border border-gray-200 divide-y divide-gray-100">
              <div class="px-4 py-2.5 flex items-baseline justify-between gap-6">
                <span class="text-xs font-medium text-gray-700 shrink-0">
                  {hosts().length > 1 ? "Hosts" : "Host"}
                </span>
                <Show
                  when={hosts().length > 1}
                  fallback={
                    <span class="text-xs text-gray-600 text-right truncate tabular-nums">
                      {hosts()[0] ?? "(not set)"}
                    </span>
                  }
                >
                  <div class="flex flex-col items-end gap-1 min-w-0">
                    <For each={hosts()}>
                      {(host) => (
                        <span class="text-xs text-gray-600 text-right truncate tabular-nums max-w-full">
                          {host}
                        </span>
                      )}
                    </For>
                  </div>
                </Show>
              </div>
              <div class="px-4 py-2.5 flex items-baseline justify-between gap-6">
                <span class="text-xs font-medium text-gray-700 shrink-0">Port</span>
                <span class="text-xs text-gray-600 text-right truncate tabular-nums">
                  {String(value().port)}
                </span>
              </div>
              <Show when={value().sessionAffinity}>
                {(affinity) => (
                  <div class="px-4 py-2.5 flex items-baseline justify-between gap-6">
                    <span class="text-xs font-medium text-gray-700 shrink-0">Session affinity</span>
                    <span class="text-xs text-gray-600 text-right truncate font-mono">
                      {affinity().header}
                    </span>
                  </div>
                )}
              </Show>
            </div>
          </div>
        )}
      </Show>
      <Show when={backends().length > 0}>
        <div>
          <h4 class="text-xs font-medium text-gray-400 mb-2">Routed replicas</h4>
          <div class="bg-white rounded-lg border border-gray-200 divide-y divide-gray-100">
            <For each={backends()}>
              {(backend) => (
                <div class="px-4 py-2.5 flex items-baseline justify-between gap-6">
                  <span class="text-xs font-mono text-gray-700 shrink-0">{backend.replicaId}</span>
                  <span class="text-xs font-mono text-gray-600 text-right truncate">
                    {backend.address}
                  </span>
                </div>
              )}
            </For>
          </div>
        </div>
      </Show>
    </>
  );
}

export { IngressInfo };
