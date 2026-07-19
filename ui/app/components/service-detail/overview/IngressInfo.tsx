import { For, Show } from "solid-js";
import { useQuery } from "../../../lib/useQuery";
import type { Service } from "../../../lib/types";
import { ingressRoutesQuery } from "../../../lib/queries";

function IngressInfo(props: { service: Service }) {
  const ingress = () => props.service.ingress;
  const hosts = () => {
    const value = ingress();
    if (!value) return [];
    return [value.host, ...(value.hosts ?? [])].filter((host): host is string => !!host);
  };
  const isIngressService = () => props.service.id === "maestro-ingress";

  const routes = useQuery(() => ({
    ...ingressRoutesQuery(),
    enabled: isIngressService()
  }));

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
                  {String(value().port ?? 80)}
                </span>
              </div>
            </div>
          </div>
        )}
      </Show>
      <Show when={isIngressService() && (routes.data?.length ?? 0) > 0}>
        <div>
          <h4 class="text-xs font-medium text-gray-400 mb-2">Routes</h4>
          <div class="space-y-3">
            <For each={routes.data}>
              {(route) => (
                <div class="bg-white rounded-lg border border-gray-200 divide-y divide-gray-100">
                  <div class="px-4 py-2.5 flex items-baseline justify-between gap-6">
                    <span class="text-xs font-medium text-gray-700 shrink-0">Service</span>
                    <span class="text-xs text-gray-600 text-right truncate tabular-nums">
                      {route.serviceId}
                    </span>
                  </div>
                  <div class="px-4 py-2.5 flex items-baseline justify-between gap-6">
                    <span class="text-xs font-medium text-gray-700 shrink-0">Rule</span>
                    <span class="text-xs text-gray-600 text-right truncate tabular-nums">
                      {route.rule}
                    </span>
                  </div>
                  <div class="px-4 py-2.5 flex items-baseline justify-between gap-6">
                    <span class="text-xs font-medium text-gray-700 shrink-0">Entry points</span>
                    <span class="text-xs text-gray-600 text-right truncate tabular-nums">
                      {route.entryPoints.join(", ")}
                    </span>
                  </div>
                  <For each={route.servers}>
                    {(server, idx) => (
                      <div class="px-4 py-2.5 flex items-baseline justify-between gap-6">
                        <span class="text-xs font-medium text-gray-700 shrink-0">
                          {route.servers.length > 1 ? `Server ${idx() + 1}` : "Server"}
                        </span>
                        <span class="text-xs text-gray-600 text-right truncate tabular-nums">
                          {server}
                        </span>
                      </div>
                    )}
                  </For>
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
