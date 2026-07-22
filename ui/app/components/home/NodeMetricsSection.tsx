import { createSignal, For, Show } from "solid-js";
import type { JSX } from "solid-js";
import { useQuery } from "../../lib/useQuery";
import clsx from "clsx";
import { clusterMetricsQuery, nodeMetricsQuery } from "../../lib/queries";
import { formatBytes, formatPercent } from "../../lib/format";
import { Card, ErrorBanner, SectionHeader } from "@maestro/kit";
import { TimelineChart } from "../TimelineChart";

const TIME_RANGES = [
  { label: "1h", ms: 3_600_000 },
  { label: "6h", ms: 21_600_000 },
  { label: "24h", ms: 86_400_000 },
  { label: "7d", ms: 604_800_000 }
];

function NodeMetricsSection() {
  const [rangeMs, setRangeMs] = createSignal(3_600_000);
  const nodeMetrics = useQuery(() => nodeMetricsQuery(rangeMs()));
  const clusterMetrics = useQuery(() => clusterMetricsQuery(rangeMs()));

  const latestNode = () => {
    const data = nodeMetrics.data ?? [];
    return data.length > 0 ? data[data.length - 1] : null;
  };
  const latestCluster = () => {
    const data = clusterMetrics.data ?? [];
    return data.length > 0 ? data[data.length - 1] : null;
  };

  const xMax = () => Date.now();
  const xMin = () => xMax() - rangeMs();

  return (
    <div class="space-y-8">
      <Show when={nodeMetrics.isError || clusterMetrics.isError}>
        <ErrorBanner
          message="Failed to load metrics"
          onRetry={() => {
            nodeMetrics.refetch();
            clusterMetrics.refetch();
          }}
        />
      </Show>
      <div class="flex justify-end">
        <div class="flex gap-1 bg-gray-100 rounded-md p-0.5">
          <For each={TIME_RANGES}>
            {(range) => (
              <button
                type="button"
                onClick={() => setRangeMs(range.ms)}
                class={clsx(
                  "text-xs px-3 py-1 rounded outline-none tabular-nums transition-[transform,color,background-color,box-shadow] duration-150 ease-out-strong active:scale-[0.96]",
                  {
                    "bg-white text-gray-900 shadow-sm font-medium": rangeMs() === range.ms,
                    "text-gray-500 hover:text-gray-700": rangeMs() !== range.ms
                  }
                )}
              >
                {range.label}
              </button>
            )}
          </For>
        </div>
      </div>
      <div>
        <SectionHeader class="mb-4">Node</SectionHeader>
        <div class="grid grid-cols-1 gap-4">
          <MetricCard
            title="CPU"
            value={latestNode() ? formatPercent(latestNode()!.cpuPercent) : null}
          >
            <TimelineChart
              data={(nodeMetrics.data ?? []).map((m) => ({ ts: m.ts, value: m.cpuPercent }))}
              label="CPU"
              color="#6366f1"
              yFormat={formatPercent}
              xMin={xMin()}
              xMax={xMax()}
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
              xMin={xMin()}
              xMax={xMax()}
            />
          </MetricCard>
        </div>
      </div>
      <div>
        <SectionHeader class="mb-4">Cluster</SectionHeader>
        <div class="grid grid-cols-1 gap-4">
          <MetricCard
            title="CPU"
            value={latestCluster() ? formatPercent(latestCluster()!.cpuPercent) : null}
          >
            <TimelineChart
              data={(clusterMetrics.data ?? []).map((m) => ({ ts: m.ts, value: m.cpuPercent }))}
              label="CPU"
              color="#0ea5e9"
              yFormat={formatPercent}
              xMin={xMin()}
              xMax={xMax()}
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
              xMin={xMin()}
              xMax={xMax()}
            />
          </MetricCard>
        </div>
      </div>
    </div>
  );
}

function MetricCard(props: { title: string; value: string | null; children: JSX.Element }) {
  return (
    <Card class="p-4">
      <div class="flex items-baseline justify-between mb-3">
        <h3 class="text-xs font-medium text-gray-500">{props.title}</h3>
        <Show when={props.value}>
          <span class="text-xs text-gray-400 tabular-nums">{props.value}</span>
        </Show>
      </div>
      {props.children}
    </Card>
  );
}

export { NodeMetricsSection };
