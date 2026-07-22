import { createMemo, createSignal, For, Show, type JSX } from "solid-js";
import { useQuery } from "@maestro/sdk";
import clsx from "clsx";
import { serviceMetricsQuery, serviceTrafficQuery } from "./queries";
import {
  Card,
  ErrorBanner,
  formatBytes,
  formatBytesRate,
  formatMs,
  formatPercent,
  formatRate,
  SectionHeader
} from "@maestro/kit";
import { TimelineChart } from "@maestro/charts";
import type { MetricsApi } from "./api";
import { buildServiceTrafficSeries } from "./traffic";

const TIME_RANGES = [
  { label: "1h", ms: 3_600_000 },
  { label: "6h", ms: 21_600_000 },
  { label: "24h", ms: 86_400_000 },
  { label: "7d", ms: 604_800_000 }
];
function MetricsTab(props: { api: MetricsApi; serviceId: string }) {
  const [rangeMs, setRangeMs] = createSignal(3_600_000);

  const metrics = useQuery(() => serviceMetricsQuery(props.api, props.serviceId, rangeMs()));
  const traffic = useQuery(() => serviceTrafficQuery(props.api, props.serviceId, rangeMs()));

  const metricsData = () => metrics.data ?? [];
  const xMax = () => Date.now();
  const xMin = () => xMax() - rangeMs();
  const cpuData = () => metricsData().map((m) => ({ ts: m.ts, value: m.cpuPercent }));
  const memData = () => metricsData().map((m) => ({ ts: m.ts, value: m.memoryBytes }));
  const netRxData = () => metricsData().map((m) => ({ ts: m.ts, value: m.netRxBytes }));
  const netTxData = () => metricsData().map((m) => ({ ts: m.ts, value: m.netTxBytes }));

  const trafficSeries = createMemo(() => buildServiceTrafficSeries(traffic.data ?? []));

  return (
    <div class="space-y-4">
      <Show when={metrics.isError}>
        <ErrorBanner message="Failed to load metrics" onRetry={() => metrics.refetch()} />
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

      <ChartCard title="CPU Usage">
        <TimelineChart
          data={cpuData()}
          label="CPU"
          color="#6366f1"
          yFormat={formatPercent}
          xMin={xMin()}
          xMax={xMax()}
        />
      </ChartCard>

      <ChartCard title="Memory">
        <TimelineChart
          data={memData()}
          label="Memory"
          color="#8b5cf6"
          yFormat={formatBytes}
          xMin={xMin()}
          xMax={xMax()}
        />
      </ChartCard>

      <ChartCard title="Network I/O">
        <TimelineChart
          data={netRxData()}
          label="RX"
          color="#10b981"
          yFormat={formatBytes}
          xMin={xMin()}
          xMax={xMax()}
          secondarySeries={{ data: netTxData(), color: "#f59e0b", label: "TX" }}
        />
      </ChartCard>

      <ChartCard
        title="HTTP Requests"
        legend={[
          { color: "bg-indigo-500", label: "Total" },
          { color: "bg-red-500", label: "Errors (4xx/5xx)" }
        ]}
      >
        <TimelineChart
          data={trafficSeries().totalRequestRate}
          label="req/s"
          color="#6366f1"
          yFormat={formatRate}
          xMin={xMin()}
          xMax={xMax()}
          secondarySeries={{
            data: trafficSeries().errorRequestRate,
            color: "#ef4444",
            label: "errors/s"
          }}
        />
      </ChartCard>

      <ChartCard
        title="Latency"
        legend={[
          { color: "bg-cyan-500", label: "p50" },
          { color: "bg-amber-500", label: "p95" }
        ]}
      >
        <TimelineChart
          data={trafficSeries().p50LatencyMs}
          label="p50"
          color="#06b6d4"
          yFormat={formatMs}
          xMin={xMin()}
          xMax={xMax()}
          secondarySeries={{
            data: trafficSeries().p95LatencyMs,
            color: "#f59e0b",
            label: "p95"
          }}
        />
      </ChartCard>

      <ChartCard
        title="HTTP Bandwidth"
        legend={[
          { color: "bg-emerald-500", label: "In" },
          { color: "bg-orange-500", label: "Out" }
        ]}
      >
        <TimelineChart
          data={trafficSeries().bytesInRate}
          label="in/s"
          color="#10b981"
          yFormat={formatBytesRate}
          xMin={xMin()}
          xMax={xMax()}
          secondarySeries={{
            data: trafficSeries().bytesOutRate,
            color: "#f97316",
            label: "out/s"
          }}
        />
      </ChartCard>
    </div>
  );
}

function ChartCard(props: {
  title: string;
  legend?: { color: string; label: string }[];
  children: JSX.Element;
}) {
  return (
    <Card class="p-4">
      <div class="flex items-center justify-between mb-3">
        <SectionHeader class="text-xs">{props.title}</SectionHeader>
        <Show when={props.legend}>
          <div class="flex items-center gap-3 text-[11px] text-gray-500">
            <For each={props.legend}>
              {(entry) => (
                <span class="inline-flex items-center gap-1">
                  <span class={clsx("size-2 rounded-full", entry.color)} />
                  {entry.label}
                </span>
              )}
            </For>
          </div>
        </Show>
      </div>
      {props.children}
    </Card>
  );
}

export { MetricsTab };
