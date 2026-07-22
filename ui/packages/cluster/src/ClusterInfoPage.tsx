import { Show } from "solid-js";
import { useQuery } from "@maestro/sdk";
import type { ClusterApi } from "./api";
import { ClusterConfigSection } from "./ClusterConfigSection";
import { ClusterStatsSection } from "./ClusterStatsSection";
import { clusterInfoQuery } from "./queries";
import { Webhooks } from "./Webhooks";

function ClusterInfoPage(props: { api: ClusterApi }) {
  const cluster = useQuery(() => clusterInfoQuery(props.api));

  return (
    <div class="space-y-8">
      <Show when={cluster.data}>
        {(info) => (
          <div>
            <div class="flex flex-wrap items-center gap-2">
              <h1 class="text-lg font-semibold text-gray-900 tracking-tight truncate">
                {info().clusterId}
              </h1>
            </div>
            <div class="mt-1.5 flex flex-wrap items-center gap-x-4 gap-y-1 text-xs text-gray-500">
              <span>{info().nodeCount} nodes</span>
              <span>{info().controlPlaneNodeCount} control plane</span>
              <span>{info().workloadNodeCount} workload</span>
            </div>
          </div>
        )}
      </Show>
      <ClusterStatsSection api={props.api} />
      <ClusterConfigSection api={props.api} />
      <Webhooks api={props.api} />
    </div>
  );
}

export { ClusterInfoPage };
