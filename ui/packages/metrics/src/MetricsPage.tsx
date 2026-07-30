import type { MetricsApi } from "./api";
import { DisksSection } from "./DisksSection";
import { NodeMetricsSection } from "./NodeMetricsSection";

function MetricsPage(props: { api: MetricsApi }) {
  return (
    <div class="space-y-8">
      <div>
        <h1 class="text-lg font-semibold text-gray-900">Metrics</h1>
        <p class="mt-1 text-sm text-gray-400">Resource usage across nodes and the cluster.</p>
      </div>
      <DisksSection api={props.api} />
      <NodeMetricsSection api={props.api} />
    </div>
  );
}

export { MetricsPage };
