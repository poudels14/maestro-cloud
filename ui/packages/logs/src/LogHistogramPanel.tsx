import { For, Show } from "solid-js";
import clsx from "clsx";
import { Loader2, X } from "lucide-solid";
import { StackedHistogramChart } from "@maestro/charts";
import type { LogHistogram, LogHistogramBucket } from "./api";
import { dateFormatter } from "./logFormat";

type SelectedLogBucket = {
  ts: number;
  from: number;
  to: number;
};

type LogTimeRange = {
  label: string;
  ms: number;
  bucketMs: number;
};

function LogHistogramPanel(props: {
  histogram: LogHistogram | null;
  loading: boolean;
  error: string | null;
  selectedBucket: SelectedLogBucket | null;
  rangeMs: number;
  ranges: readonly LogTimeRange[];
  onClearSelected: () => void;
  onSelectRange: (rangeMs: number) => void;
  onRetry: () => void;
  onSelectInterval: (bucket: LogHistogramBucket) => void;
  onSelectBucket: (bucket: LogHistogramBucket, group: string) => void;
}) {
  const histogramTotal = () =>
    props.histogram?.buckets.reduce((total, bucket) => total + bucket.count, 0) ?? 0;
  const selectedBucketCount = () => {
    const selected = props.selectedBucket;
    if (!selected) return 0;
    return props.histogram?.buckets.find((bucket) => bucket.ts === selected.ts)?.count ?? 0;
  };

  return (
    <div class="shrink-0 border-b border-gray-100 px-3 pt-3 pb-1.5">
      <div class="flex flex-wrap items-center justify-between gap-2 px-1">
        <div class="flex items-center gap-2 min-w-0">
          <Show when={props.histogram}>
            <div class="flex min-w-0 flex-wrap items-center gap-x-2 gap-y-1 text-[11px]">
              <span class="text-sm font-medium tabular-nums whitespace-nowrap text-gray-700">
                {histogramTotal().toLocaleString()} {histogramTotal() === 1 ? "log" : "logs"}
              </span>
              <Show when={props.selectedBucket}>
                {(selected) => (
                  <span class="inline-flex max-w-full items-center overflow-hidden rounded-md border border-gray-200 bg-gray-100">
                    <span class="min-w-0 truncate py-0.5 pl-2 pr-1 font-mono text-gray-700">
                      {selectedIntervalLabel(selected())}
                      <span class="text-gray-400"> · </span>
                      {selectedBucketCount().toLocaleString()}{" "}
                      {selectedBucketCount() === 1 ? "log" : "logs"}
                    </span>
                    <button
                      type="button"
                      onClick={props.onClearSelected}
                      aria-label="Clear selected log interval"
                      title="Clear selected interval"
                      class="self-stretch pl-0.5 pr-1.5 text-gray-400 outline-none transition-colors hover:bg-gray-200 hover:text-gray-700"
                    >
                      <X class="size-3" />
                    </button>
                  </span>
                )}
              </Show>
            </div>
          </Show>
          <Show when={props.loading && props.histogram}>
            <Loader2 class="size-3 animate-spin text-gray-400" />
          </Show>
        </div>
        <div class="flex gap-1 bg-gray-100 rounded-md p-0.5">
          <For each={props.ranges}>
            {(range) => (
              <button
                type="button"
                onClick={() => props.onSelectRange(range.ms)}
                class={clsx(
                  "text-[11px] px-2.5 py-1 rounded outline-none tabular-nums transition-[transform,color,background-color,box-shadow] duration-150 ease-out-strong active:scale-[0.96]",
                  {
                    "bg-white text-gray-900 shadow-sm font-medium": props.rangeMs === range.ms,
                    "text-gray-500 hover:text-gray-700": props.rangeMs !== range.ms
                  }
                )}
              >
                {range.label}
              </button>
            )}
          </For>
        </div>
      </div>
      <Show when={props.error}>
        {(error) => (
          <div
            class={clsx("flex items-center justify-center gap-2 text-xs text-red-500", {
              "h-28": !props.histogram,
              "pt-2": props.histogram
            })}
          >
            <span>{error()}</span>
            <button
              type="button"
              onClick={props.onRetry}
              class="font-medium hover:text-red-700 outline-none"
            >
              Retry
            </button>
          </div>
        )}
      </Show>
      <Show when={!props.error && props.loading && !props.histogram}>
        <div class="h-28 flex items-center justify-center text-gray-400">
          <Loader2 class="size-4 animate-spin" />
        </div>
      </Show>
      <Show when={props.histogram}>
        {(histogram) => (
          <StackedHistogramChart
            data={histogram().buckets}
            from={histogram().from}
            to={histogram().to}
            bucketMs={histogram().bucketMs}
            itemName="log"
            {...(props.selectedBucket ? { selectedTs: props.selectedBucket.ts } : {})}
            onSelectInterval={props.onSelectInterval}
            onSelect={props.onSelectBucket}
          />
        )}
      </Show>
    </div>
  );
}

const minuteFormatter = new Intl.DateTimeFormat(undefined, {
  hour: "2-digit",
  minute: "2-digit",
  hour12: false
});

function selectedIntervalLabel(selected: SelectedLogBucket) {
  const from = new Date(selected.from);
  const to = new Date(selected.to);
  const fromLabel = `${dateFormatter.format(from)} ${minuteFormatter.format(from)}`;
  if (from.toDateString() === to.toDateString()) {
    return `${fromLabel}–${minuteFormatter.format(to)}`;
  }
  return `${fromLabel} – ${dateFormatter.format(to)} ${minuteFormatter.format(to)}`;
}

export { LogHistogramPanel };
export type { LogTimeRange, SelectedLogBucket };
