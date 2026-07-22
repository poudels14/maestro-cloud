import { For, Show } from "solid-js";
import { useQuery } from "@maestro/sdk";
import type { IngressApi } from "./api";
import { ingressRoutesQuery } from "./queries";

function IngressInfo(props: { api: IngressApi; serviceId: string }) {
  const routes = useQuery(() => ingressRoutesQuery(props.api));
  const serviceRoutes = () =>
    (routes.data ?? []).filter((route) => route.serviceId === props.serviceId);

  return (
    <Show when={serviceRoutes().length > 0}>
      <div>
        <h4 class="text-xs font-medium text-gray-400 mb-2">Ingress</h4>
        <div class="space-y-3">
          <For each={serviceRoutes()}>
            {(route) => (
              <div class="bg-white rounded-lg border border-gray-200 divide-y divide-gray-100">
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
                  {(server, index) => (
                    <div class="px-4 py-2.5 flex items-baseline justify-between gap-6">
                      <span class="text-xs font-medium text-gray-700 shrink-0">
                        {route.servers.length > 1 ? `Server ${index() + 1}` : "Server"}
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
  );
}

export { IngressInfo };
