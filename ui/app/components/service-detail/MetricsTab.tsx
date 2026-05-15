import { createMemo, createResource, createSignal, For, Show, onCleanup } from "solid-js";
import clsx from "clsx";
import type { Service, TrafficPoint } from "../../lib/types";
import { getServiceMetrics, getServiceTraffic } from "../../lib/api";
import { ErrorBanner } from "../../lib/ui";
import { TimelineChart } from "../TimelineChart";

const TIME_RANGES = [
  { label: "1h", ms: 3_600_000 },
  { label: "6h", ms: 21_600_000 },
  { label: "24h", ms: 86_400_000 },
  { label: "7d", ms: 604_800_000 }
];
const METRICS_POLL_MS = 10_000;
const SCRAPE_INTERVAL_S = 5;

function MetricsTab(props: { service: Service }) {
  const [rangeMs, setRangeMs] = createSignal(3_600_000);

  const [metrics, { refetch: refetchMetrics }] = createResource(
    () => ({ serviceId: props.service.id, range: rangeMs() }),
    ({ serviceId, range }) => {
      const now = Date.now();
      return getServiceMetrics(serviceId, now - range, now);
    }
  );

  const [traffic, { refetch: refetchTraffic }] = createResource(
    () => ({ serviceId: props.service.id, range: rangeMs() }),
    ({ serviceId, range }) => {
      const now = Date.now();
      return getServiceTraffic(serviceId, now - range, now);
    }
  );

  const pollTimer = setInterval(() => {
    refetchMetrics();
    refetchTraffic();
  }, METRICS_POLL_MS);
  onCleanup(() => clearInterval(pollTimer));

  const data = () => metrics() ?? [];
  const xMax = () => Date.now();
  const xMin = () => xMax() - rangeMs();
  const cpuData = () => data().map((m) => ({ ts: m.ts, value: m.cpuPercent }));
  const memData = () => data().map((m) => ({ ts: m.ts, value: m.memoryBytes }));
  const netRxData = () => data().map((m) => ({ ts: m.ts, value: m.netRxBytes }));
  const netTxData = () => data().map((m) => ({ ts: m.ts, value: m.netTxBytes }));

  const trafficByTs = createMemo(() => groupByTimestamp(traffic() ?? []));
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
      <Show when={metrics.error}>
        <ErrorBanner message="Failed to load metrics" onRetry={refetchMetrics} />
      </Show>
      <div class="flex justify-end">
        <div class="flex gap-1 bg-gray-100 rounded-md p-0.5">
          <For each={TIME_RANGES}>
            {(range) => (
              <button
                type="button"
                onClick={() => setRangeMs(range.ms)}
                class={clsx("text-xs px-3 py-1 rounded outline-none transition-colors", {
                  "bg-white text-gray-900 shadow-sm font-medium": rangeMs() === range.ms,
                  "text-gray-500 hover:text-gray-700": rangeMs() !== range.ms
                })}
              >
                {range.label}
              </button>
            )}
          </For>
        </div>
      </div>

      <div class="bg-white rounded-lg border border-gray-200 p-4">
        <h3 class="text-xs font-medium text-gray-500 uppercase tracking-wider mb-3">CPU Usage</h3>
        <TimelineChart
          data={cpuData()}
          label="CPU"
          color="#6366f1"
          yFormat={formatPercent}
          xMin={xMin()}
          xMax={xMax()}
        />
      </div>

      <div class="bg-white rounded-lg border border-gray-200 p-4">
        <h3 class="text-xs font-medium text-gray-500 uppercase tracking-wider mb-3">Memory</h3>
        <TimelineChart
          data={memData()}
          label="Memory"
          color="#8b5cf6"
          yFormat={formatBytes}
          xMin={xMin()}
          xMax={xMax()}
        />
      </div>

      <div class="bg-white rounded-lg border border-gray-200 p-4">
        <h3 class="text-xs font-medium text-gray-500 uppercase tracking-wider mb-3">Network I/O</h3>
        <TimelineChart
          data={netRxData()}
          label="RX"
          color="#10b981"
          yFormat={formatBytes}
          xMin={xMin()}
          xMax={xMax()}
          secondarySeries={{ data: netTxData(), color: "#f59e0b", label: "TX" }}
        />
      </div>

      <div class="bg-white rounded-lg border border-gray-200 p-4">
        <div class="flex items-center justify-between mb-3">
          <h3 class="text-xs font-medium text-gray-500 uppercase tracking-wider">HTTP Requests</h3>
          <div class="flex items-center gap-3 text-[11px] text-gray-500">
            <span class="inline-flex items-center gap-1">
              <span class="size-2 rounded-full bg-indigo-500" />
              Total
            </span>
            <span class="inline-flex items-center gap-1">
              <span class="size-2 rounded-full bg-red-500" />
              Errors (4xx/5xx)
            </span>
          </div>
        </div>
        <TimelineChart
          data={totalReqRate()}
          label="req/s"
          color="#6366f1"
          yFormat={formatRate}
          xMin={xMin()}
          xMax={xMax()}
          secondarySeries={{ data: errorReqRate(), color: "#ef4444", label: "errors/s" }}
        />
      </div>

      <div class="bg-white rounded-lg border border-gray-200 p-4">
        <div class="flex items-center justify-between mb-3">
          <h3 class="text-xs font-medium text-gray-500 uppercase tracking-wider">Latency</h3>
          <div class="flex items-center gap-3 text-[11px] text-gray-500">
            <span class="inline-flex items-center gap-1">
              <span class="size-2 rounded-full bg-cyan-500" />
              p50
            </span>
            <span class="inline-flex items-center gap-1">
              <span class="size-2 rounded-full bg-amber-500" />
              p95
            </span>
          </div>
        </div>
        <TimelineChart
          data={p50LatencyMs()}
          label="p50"
          color="#06b6d4"
          yFormat={formatMs}
          xMin={xMin()}
          xMax={xMax()}
          secondarySeries={{ data: p95LatencyMs(), color: "#f59e0b", label: "p95" }}
        />
      </div>

      <div class="bg-white rounded-lg border border-gray-200 p-4">
        <div class="flex items-center justify-between mb-3">
          <h3 class="text-xs font-medium text-gray-500 uppercase tracking-wider">HTTP Bandwidth</h3>
          <div class="flex items-center gap-3 text-[11px] text-gray-500">
            <span class="inline-flex items-center gap-1">
              <span class="size-2 rounded-full bg-emerald-500" />
              In
            </span>
            <span class="inline-flex items-center gap-1">
              <span class="size-2 rounded-full bg-orange-500" />
              Out
            </span>
          </div>
        </div>
        <TimelineChart
          data={bytesInRate()}
          label="in/s"
          color="#10b981"
          yFormat={formatBytesRate}
          xMin={xMin()}
          xMax={xMax()}
          secondarySeries={{ data: bytesOutRate(), color: "#f97316", label: "out/s" }}
        />
      </div>
    </div>
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

function formatBytes(v: number) {
  if (v >= 1_000_000_000) return `${(v / 1_000_000_000).toFixed(1)} GB`;
  if (v >= 1_000_000) return `${(v / 1_000_000).toFixed(1)} MB`;
  if (v >= 1_000) return `${(v / 1_000).toFixed(1)} KB`;
  return `${Math.round(v)} B`;
}

function formatBytesRate(v: number) {
  return `${formatBytes(v)}/s`;
}

function formatPercent(v: number) {
  return `${v.toFixed(1)}%`;
}

function formatRate(v: number) {
  if (v >= 1000) return `${(v / 1000).toFixed(1)}k/s`;
  if (v >= 10) return `${v.toFixed(0)}/s`;
  return `${v.toFixed(2)}/s`;
}

function formatMs(v: number) {
  if (v >= 1000) return `${(v / 1000).toFixed(2)}s`;
  return `${Math.round(v)}ms`;
}

export { MetricsTab };
