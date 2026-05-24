import { Show } from "solid-js";
import { useQuery } from "@tanstack/solid-query";
import { clusterMetricsQuery, nodeMetricsQuery } from "../../lib/queries";
import { formatBytes, formatPercent } from "../../lib/format";
import { Card, ErrorBanner, SectionHeader } from "../../lib/ui";
import { TimelineChart } from "../TimelineChart";

const RANGE_MS = 3_600_000;

function NodeMetricsSection() {
  const nodeMetrics = useQuery(() => nodeMetricsQuery(RANGE_MS));
  const clusterMetrics = useQuery(() => clusterMetricsQuery(RANGE_MS));

  const latestNode = () => {
    const data = nodeMetrics.data ?? [];
    return data.length > 0 ? data[data.length - 1] : null;
  };
  const latestCluster = () => {
    const data = clusterMetrics.data ?? [];
    return data.length > 0 ? data[data.length - 1] : null;
  };

  return (
    <div class="space-y-6">
      <Show when={nodeMetrics.isError || clusterMetrics.isError}>
        <ErrorBanner
          message="Failed to load metrics"
          onRetry={() => {
            nodeMetrics.refetch();
            clusterMetrics.refetch();
          }}
        />
      </Show>
      <div>
        <SectionHeader class="mb-4">Node</SectionHeader>
        <div class="grid grid-cols-1 gap-4 sm:grid-cols-2">
          <MetricCard
            title="CPU"
            value={latestNode() ? formatPercent(latestNode()!.cpuPercent) : null}
          >
            <TimelineChart
              data={(nodeMetrics.data ?? []).map((m) => ({ ts: m.ts, value: m.cpuPercent }))}
              label="CPU"
              color="#6366f1"
              yFormat={formatPercent}
              height={140}
            />
          </MetricCard>
          <MetricCard
            title="Memory"
            value={
              latestNode()
                ? `${formatBytes(latestNode()!.memoryBytes)} / ${formatBytes(latestNode()!.memoryLimitBytes)}`
                : null
            }
          >
            <TimelineChart
              data={(nodeMetrics.data ?? []).map((m) => ({ ts: m.ts, value: m.memoryBytes }))}
              label="Memory"
              color="#8b5cf6"
              yFormat={formatBytes}
              height={140}
            />
          </MetricCard>
        </div>
      </div>
      <div>
        <SectionHeader class="mb-4">Cluster</SectionHeader>
        <div class="grid grid-cols-1 gap-4 sm:grid-cols-2">
          <MetricCard
            title="CPU"
            value={latestCluster() ? formatPercent(latestCluster()!.cpuPercent) : null}
          >
            <TimelineChart
              data={(clusterMetrics.data ?? []).map((m) => ({ ts: m.ts, value: m.cpuPercent }))}
              label="CPU"
              color="#0ea5e9"
              yFormat={formatPercent}
              height={140}
            />
          </MetricCard>
          <MetricCard
            title="Memory"
            value={
              latestCluster()
                ? `${formatBytes(latestCluster()!.memoryBytes)} / ${formatBytes(latestCluster()!.memoryLimitBytes)}`
                : null
            }
          >
            <TimelineChart
              data={(clusterMetrics.data ?? []).map((m) => ({ ts: m.ts, value: m.memoryBytes }))}
              label="Memory"
              color="#14b8a6"
              yFormat={formatBytes}
              height={140}
            />
          </MetricCard>
        </div>
      </div>
    </div>
  );
}

function MetricCard(props: { title: string; value: string | null; children: any }) {
  return (
    <Card class="p-4">
      <div class="flex items-baseline justify-between mb-3">
        <h3 class="text-xs font-medium text-gray-500 uppercase tracking-wider">{props.title}</h3>
        <Show when={props.value}>
          <span class="text-xs text-gray-400">{props.value}</span>
        </Show>
      </div>
      {props.children}
    </Card>
  );
}

export { NodeMetricsSection };
