import { createMemo, createSignal, For, Show } from "solid-js";
import { useQuery } from "../../lib/useQuery";
import clsx from "clsx";
import type { Service, TrafficPoint } from "../../lib/types";
import { serviceMetricsQuery, serviceTrafficQuery } from "../../lib/queries";
import {
  formatBytes,
  formatBytesRate,
  formatMs,
  formatPercent,
  formatRate
} from "../../lib/format";
import { Card, ErrorBanner, SectionHeader } from "@maestro/kit";
import { TimelineChart } from "../TimelineChart";

const TIME_RANGES = [
  { label: "1h", ms: 3_600_000 },
  { label: "6h", ms: 21_600_000 },
  { label: "24h", ms: 86_400_000 },
  { label: "7d", ms: 604_800_000 }
];
const SCRAPE_INTERVAL_S = 5;

function MetricsTab(props: { service: Service }) {
  const [rangeMs, setRangeMs] = createSignal(3_600_000);

  const metrics = useQuery(() => serviceMetricsQuery(props.service.meta.id, rangeMs()));
  const traffic = useQuery(() => serviceTrafficQuery(props.service.meta.id, rangeMs()));

  const metricsData = () => metrics.data ?? [];
  const xMax = () => Date.now();
  const xMin = () => xMax() - rangeMs();
  const cpuData = () => metricsData().map((m) => ({ ts: m.ts, value: m.cpuPercent }));
  const memData = () => metricsData().map((m) => ({ ts: m.ts, value: m.memoryBytes }));
  const netRxData = () => metricsData().map((m) => ({ ts: m.ts, value: m.netRxBytes }));
  const netTxData = () => metricsData().map((m) => ({ ts: m.ts, value: m.netTxBytes }));

  const trafficByTs = createMemo(() => groupByTimestamp(traffic.data ?? []));
  const totalReqRate = () =>
    trafficByTs().map(([ts, points]) => ({
      ts,
      value: sumRequests(points) / SCRAPE_INTERVAL_S
    }));
  const errorReqRate = () =>
    trafficByTs().map(([ts, points]) => ({
      ts,
      value: sumRequestsWhere(points, (p) => p.statusCode >= 400) / SCRAPE_INTERVAL_S
    }));
  const p95LatencyMs = () =>
    trafficByTs().map(([ts, points]) => ({
      ts,
      value: bucketPercentileSec(points, 0.95) * 1000
    }));
  const p50LatencyMs = () =>
    trafficByTs().map(([ts, points]) => ({
      ts,
      value: bucketPercentileSec(points, 0.5) * 1000
    }));
  const bytesInRate = () =>
    trafficByTs().map(([ts, points]) => ({
      ts,
      value: sumBytes(points, "in") / SCRAPE_INTERVAL_S
    }));
  const bytesOutRate = () =>
    trafficByTs().map(([ts, points]) => ({
      ts,
      value: sumBytes(points, "out") / SCRAPE_INTERVAL_S
    }));

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
          data={totalReqRate()}
          label="req/s"
          color="#6366f1"
          yFormat={formatRate}
          xMin={xMin()}
          xMax={xMax()}
          secondarySeries={{ data: errorReqRate(), color: "#ef4444", label: "errors/s" }}
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
          data={p50LatencyMs()}
          label="p50"
          color="#06b6d4"
          yFormat={formatMs}
          xMin={xMin()}
          xMax={xMax()}
          secondarySeries={{ data: p95LatencyMs(), color: "#f59e0b", label: "p95" }}
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
          data={bytesInRate()}
          label="in/s"
          color="#10b981"
          yFormat={formatBytesRate}
          xMin={xMin()}
          xMax={xMax()}
          secondarySeries={{ data: bytesOutRate(), color: "#f97316", label: "out/s" }}
        />
      </ChartCard>
    </div>
  );
}

function ChartCard(props: {
  title: string;
  legend?: { color: string; label: string }[];
  children: any;
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

function groupByTimestamp(points: TrafficPoint[]): [number, TrafficPoint[]][] {
  const buckets = new Map<number, TrafficPoint[]>();
  for (const point of points) {
    const arr = buckets.get(point.ts);
    if (arr) arr.push(point);
    else buckets.set(point.ts, [point]);
  }
  return Array.from(buckets.entries()).sort(([a], [b]) => a - b);
}

function sumRequests(points: TrafficPoint[]): number {
  let total = 0;
  for (const p of points) total += p.requests;
  return total;
}

function sumRequestsWhere(points: TrafficPoint[], predicate: (p: TrafficPoint) => boolean): number {
  let total = 0;
  for (const p of points) {
    if (predicate(p)) total += p.requests;
  }
  return total;
}

function sumBytes(points: TrafficPoint[], dir: "in" | "out"): number {
  let total = 0;
  for (const p of points) total += dir === "in" ? p.bytesIn : p.bytesOut;
  return total;
}

function bucketPercentileSec(points: TrafficPoint[], p: number): number {
  let le1 = 0;
  let le5 = 0;
  let le10 = 0;
  let total = 0;
  for (const point of points) {
    le1 += point.latLe1s;
    le5 += point.latLe5s;
    le10 += point.latLe10s;
    total += point.latTotal;
  }
  if (total === 0) return 0;
  const rank = p * total;
  if (le1 >= rank) {
    return le1 === 0 ? 0 : rank / le1;
  }
  if (le5 >= rank) {
    const span = le5 - le1;
    return span === 0 ? 1 : 1 + 4 * ((rank - le1) / span);
  }
  if (le10 >= rank) {
    const span = le10 - le5;
    return span === 0 ? 5 : 5 + 5 * ((rank - le5) / span);
  }
  return 10;
}

export { MetricsTab };
