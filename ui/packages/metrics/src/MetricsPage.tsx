import type { MetricsApi } from "./api";
import { DisksSection } from "./DisksSection";
import { ResourceMetricsSection } from "./ResourceMetricsSection";

function MetricsPage(props: { api: MetricsApi }) {
  return (
    <div class="space-y-8">
      <h1 class="text-lg font-semibold text-gray-900">Metrics</h1>
      <DisksSection api={props.api} />
      <ResourceMetricsSection api={props.api} />
    </div>
  );
}

export { MetricsPage };
