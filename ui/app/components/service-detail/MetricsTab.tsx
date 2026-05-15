import { createResource, createSignal, For, Show, onCleanup } from "solid-js";
import clsx from "clsx";
import type { Service } from "../../lib/types";
import { getServiceMetrics } from "../../lib/api";
import { ErrorBanner } from "../../lib/ui";
import { TimelineChart } from "../TimelineChart";

const TIME_RANGES = [
  { label: "1h", ms: 3_600_000 },
  { label: "6h", ms: 21_600_000 },
  { label: "24h", ms: 86_400_000 },
  { label: "7d", ms: 604_800_000 }
];
const METRICS_POLL_MS = 10_000;

function MetricsTab(props: { service: Service }) {
  const [rangeMs, setRangeMs] = createSignal(3_600_000);

  const [metrics, { refetch }] = createResource(
    () => ({ serviceId: props.service.id, range: rangeMs() }),
    ({ serviceId, range }) => {
      const now = Date.now();
      return getServiceMetrics(serviceId, now - range, now);
    }
  );

  const pollTimer = setInterval(refetch, METRICS_POLL_MS);
  onCleanup(() => clearInterval(pollTimer));

  const data = () => metrics() ?? [];
  const xMax = () => Date.now();
  const xMin = () => xMax() - rangeMs();
  const cpuData = () => data().map((m) => ({ ts: m.ts, value: m.cpuPercent }));
  const memData = () => data().map((m) => ({ ts: m.ts, value: m.memoryBytes }));
  const netRxData = () => data().map((m) => ({ ts: m.ts, value: m.netRxBytes }));
  const netTxData = () => data().map((m) => ({ ts: m.ts, value: m.netTxBytes }));

  return (
    <div class="space-y-4">
      <Show when={metrics.error}>
        <ErrorBanner message="Failed to load metrics" onRetry={refetch} />
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
    </div>
  );
}

function formatBytes(v: number) {
  if (v >= 1_000_000_000) return `${(v / 1_000_000_000).toFixed(1)} GB`;
  if (v >= 1_000_000) return `${(v / 1_000_000).toFixed(1)} MB`;
  if (v >= 1_000) return `${(v / 1_000).toFixed(1)} KB`;
  return `${Math.round(v)} B`;
}

function formatPercent(v: number) {
  return `${v.toFixed(1)}%`;
}

export { MetricsTab };
