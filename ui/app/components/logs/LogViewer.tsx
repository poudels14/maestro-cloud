import {
  createEffect,
  createMemo,
  createSignal,
  For,
  Show,
  Switch,
  Match,
  on,
  onCleanup
} from "solid-js";
import { ChevronDown, ChevronUp, Loader2, X } from "lucide-solid";
import clsx from "clsx";
import type { LogEntry } from "../../lib/types";
import {
  getLogHistogram,
  queryLogs,
  type LogHistogram,
  type LogHistogramBucket
} from "../../lib/api";
import { ErrorBanner } from "../../lib/ui";
import { dateFormatter, httpFields } from "../../lib/logFormat";
import {
  TimeCell,
  ExpanderCell,
  HostCell,
  LevelCell,
  MessageCell,
  MethodCell,
  StatusCell,
  PathCell
} from "./cells";
import { LogDetailPanel } from "./LogDetailPanel";
import { LogHistogramChart } from "./LogHistogram";
import { LogQueryInput, type LogQueryCatalog } from "./LogQueryInput";
import {
  combineLogQueries,
  logQueryPills,
  removeLogQueryPill,
  withLogHistogramGroupFilter
} from "./logQueryPills";

const COL = {
  time: "sm:w-[118px]",
  host: "sm:w-[112px]",
  service: "sm:w-[124px]",
  level: "sm:w-[52px]",
  method: "sm:w-[60px]",
  status: "sm:w-[56px]"
};

const minuteFormatter = new Intl.DateTimeFormat(undefined, {
  hour: "2-digit",
  minute: "2-digit",
  hour12: false
});

const PAGE_SIZE = 500;
const POLL_INTERVAL_MS = 5000;
const HISTOGRAM_POLL_INTERVAL_MS = 30_000;
const TIME_RANGES = [
  { label: "1h", ms: 3_600_000, bucketMs: 60_000 },
  { label: "6h", ms: 21_600_000, bucketMs: 300_000 },
  { label: "24h", ms: 86_400_000, bucketMs: 600_000 },
  { label: "7d", ms: 604_800_000, bucketMs: 7_200_000 }
];

type SelectedLogBucket = {
  ts: number;
  from: number;
  to: number;
};

function mergeLogEntries(current: LogEntry[], incoming: LogEntry[]) {
  const existing = new Set(current.map(logEntryKey));
  const unique = incoming.filter((entry) => {
    const key = logEntryKey(entry);
    if (existing.has(key)) return false;
    existing.add(key);
    return true;
  });
  return [...current, ...unique].sort((left, right) => left.ts - right.ts);
}

function logEntryKey(entry: LogEntry) {
  return `${entry.nodeId ?? "local"}:${entry.seq}`;
}

function LogViewer(props: {
  scope: string;
  nodeId?: number;
  showHost?: boolean;
  embedded?: boolean;
  showHistogram?: boolean;
  histogramGroupBy?: "level" | "status";
  fillHeight?: boolean;
  query?: string;
  requiredQuery?: string;
  onQueryChange?: (query: string) => void;
  range?: string;
  onRangeChange?: (range: string) => void;
}) {
  const [lines, setLines] = createSignal<LogEntry[]>([]);
  const [loading, setLoading] = createSignal(true);
  const [error, setError] = createSignal<string | null>(null);
  const [hasMore, setHasMore] = createSignal(false);
  const [queryDraft, setQueryDraft] = createSignal("");
  const [internalQuery, setInternalQuery] = createSignal(props.query ?? "");
  const query = () => (props.onQueryChange ? (props.query ?? "") : internalQuery());
  const setQuery = (value: string) => {
    if (props.onQueryChange) {
      props.onQueryChange(value);
    } else {
      setInternalQuery(value);
    }
  };
  const [loadingMore, setLoadingMore] = createSignal(false);
  const [expanded, setExpanded] = createSignal<Set<string>>(new Set());
  const [newestTs, setNewestTs] = createSignal<number | null>(null);
  const [olderCursor, setOlderCursor] = createSignal<number | null>(null);
  const [unavailableNodes, setUnavailableNodes] = createSignal<string[]>([]);
  const [internalRangeMs, setInternalRangeMs] = createSignal(TIME_RANGES[0].ms);
  const rangeMs = () => {
    if (props.onRangeChange) {
      const matched = TIME_RANGES.find((range) => range.label === props.range);
      return matched?.ms ?? TIME_RANGES[0].ms;
    }
    return internalRangeMs();
  };
  const setRangeMs = (value: number) => {
    if (props.onRangeChange) {
      const matched = TIME_RANGES.find((range) => range.ms === value);
      props.onRangeChange(matched?.label ?? TIME_RANGES[0].label);
    } else {
      setInternalRangeMs(value);
    }
  };
  const [histogram, setHistogram] = createSignal<LogHistogram | null>(null);
  const [histogramLoading, setHistogramLoading] = createSignal(false);
  const [histogramError, setHistogramError] = createSignal<string | null>(null);
  const [histogramRefresh, setHistogramRefresh] = createSignal(0);
  const [selectedBucket, setSelectedBucket] = createSignal<SelectedLogBucket | null>(null);
  let fetchGeneration = 0;
  let histogramGeneration = 0;

  const phaseLines = () => lines().filter((line) => line.text.trim().length > 0);

  const queryCatalog = createMemo<LogQueryCatalog>(() => {
    const fields = new Set<string>();
    const values = new Map<string, Set<string>>();
    const addValue = (field: string, value: string | undefined) => {
      const normalized = value?.trim();
      if (!normalized || normalized.length > 160) return;
      let candidates = values.get(field);
      if (!candidates) {
        candidates = new Set();
        values.set(field, candidates);
      }
      if (candidates.size < 25) candidates.add(normalized);
    };

    for (const line of phaseLines()) {
      addValue("service", line.serviceId);
      addValue("deployment", line.deploymentId);
      addValue("level", line.level.toLowerCase());
      addValue("status", line.level.toLowerCase());
      addValue("replica", line.source);
      addValue("stream", line.stream);
      const http = httpFields(line.attrs);
      addValue("@http.status_code", http.status);
      for (const [name, value] of line.attrs ?? []) {
        if (!/^[A-Za-z0-9._-]+$/.test(name)) continue;
        const field = `@${name}`;
        fields.add(field);
        addValue(field, value);
      }
    }

    return {
      fields: Array.from(fields)
        .sort((left, right) => left.localeCompare(right))
        .slice(0, 100),
      values: new Map(
        Array.from(values, ([field, candidates]) => [
          field,
          Array.from(candidates).sort((left, right) =>
            left.localeCompare(right, undefined, { numeric: true })
          )
        ])
      )
    };
  });

  const filteredLines = () => {
    return phaseLines();
  };

  const requestQuery = () => {
    return combineLogQueries(combineLogQueries(props.scope, props.requiredQuery ?? ""), query());
  };

  const applyQuery = () => {
    setQuery(combineLogQueries(query(), queryDraft()));
    setQueryDraft("");
  };

  const activeTimeRange = () => {
    if (!props.showHistogram) return { from: undefined, to: undefined };
    const selected = selectedBucket();
    if (selected) return { from: selected.from, to: selected.to };
    const to = Date.now() + 1;
    return { from: to - rangeMs(), to };
  };

  const rowRequestContext = createMemo(
    () =>
      `${props.scope}\0${props.nodeId ?? ""}\0${requestQuery()}\0${props.showHistogram ? rangeMs() : ""}\0${selectedBucket()?.from ?? ""}\0${selectedBucket()?.to ?? ""}`
  );

  const selectRange = (nextRangeMs: number) => {
    if (nextRangeMs === rangeMs() && selectedBucket() == null) return;
    setRangeMs(nextRangeMs);
    setSelectedBucket(null);
    setHistogram(null);
  };

  const selectHistogramInterval = (bucket: LogHistogramBucket) => {
    const value = histogram();
    if (!value) return;
    if (selectedBucket()?.ts === bucket.ts) {
      setSelectedBucket(null);
      return;
    }
    setSelectedBucket({
      ts: bucket.ts,
      from: Math.max(value.from, bucket.ts),
      to: Math.min(value.to, bucket.ts + value.bucketMs)
    });
  };

  const selectHistogramBucket = (bucket: LogHistogramBucket, group: string) => {
    const value = histogram();
    if (!value) return;
    setSelectedBucket({
      ts: bucket.ts,
      from: Math.max(value.from, bucket.ts),
      to: Math.min(value.to, bucket.ts + value.bucketMs)
    });
    setQueryDraft("");
    setQuery(withLogHistogramGroupFilter(query(), props.histogramGroupBy ?? "level", group));
  };

  const histogramTotal = () =>
    histogram()?.buckets.reduce((total, bucket) => total + bucket.count, 0) ?? 0;

  const selectedBucketCount = () => {
    const selected = selectedBucket();
    if (!selected) return 0;
    return histogram()?.buckets.find((bucket) => bucket.ts === selected.ts)?.count ?? 0;
  };

  const selectedIntervalLabel = () => {
    const selected = selectedBucket();
    if (!selected) return "";
    const from = new Date(selected.from);
    const to = new Date(selected.to);
    const fromLabel = `${dateFormatter.format(from)} ${minuteFormatter.format(from)}`;
    if (from.toDateString() === to.toDateString()) {
      return `${fromLabel}–${minuteFormatter.format(to)}`;
    }
    return `${fromLabel} – ${dateFormatter.format(to)} ${minuteFormatter.format(to)}`;
  };

  const showHost = () => {
    if (props.showHost) return true;
    const seen = new Set<string>();
    for (const line of phaseLines()) {
      const host = line.source || "";
      if (host) seen.add(host);
      if (seen.size > 1) return true;
    }
    return false;
  };

  const showHttp = () => {
    for (const line of phaseLines()) {
      const { method, status, path } = httpFields(line.attrs);
      if (method || status || path) return true;
    }
    return false;
  };

  const toggleExpanded = (key: string) => {
    const next = new Set(expanded());
    if (next.has(key)) next.delete(key);
    else next.add(key);
    setExpanded(next);
  };

  const fetchPage = (searchQuery: string, bounds: { afterTs?: number; beforeTs?: number }) =>
    queryLogs({
      query: searchQuery,
      limit: PAGE_SIZE,
      nodeId: props.nodeId,
      ...bounds
    });

  const fetchHistogram = async () => {
    if (!props.showHistogram) return;
    const generation = ++histogramGeneration;
    const requestedRangeMs = rangeMs();
    const bucketMs = TIME_RANGES.find((range) => range.ms === requestedRangeMs)?.bucketMs;
    const to = Date.now() + 1;
    const from = to - requestedRangeMs;
    const searchQuery = requestQuery();
    setHistogramLoading(true);
    setHistogramError(null);
    try {
      const result = await getLogHistogram({
        query: searchQuery,
        from,
        to,
        bucketMs,
        groupBy: props.histogramGroupBy,
        nodeId: props.nodeId
      });
      if (
        generation !== histogramGeneration ||
        searchQuery !== requestQuery() ||
        requestedRangeMs !== rangeMs()
      )
        return;
      setHistogram(result);
      setUnavailableNodes((result.unavailableNodes ?? []).map((node) => node.nodeName));
      setHistogramError(null);
    } catch (err) {
      if (generation !== histogramGeneration) return;
      setHistogramError(err instanceof Error ? err.message : "Failed to load log counts");
    } finally {
      if (generation === histogramGeneration) setHistogramLoading(false);
    }
  };

  const fetchInitialLogs = async (searchQuery: string, generation: number) => {
    try {
      const { from, to } = activeTimeRange();
      const page = await fetchPage(searchQuery, { afterTs: from, beforeTs: to });
      if (generation !== fetchGeneration) return;
      setHasMore(page.nextCursor != null);
      setOlderCursor(page.nextCursor);
      setLines(page.entries);
      setNewestTs(page.entries.at(-1)?.ts ?? null);
      setUnavailableNodes(page.unavailableNodes.map((node) => node.nodeName));
      setError(null);
    } catch (err) {
      if (generation !== fetchGeneration) return;
      setError(err instanceof Error ? err.message : "Failed to load logs");
    } finally {
      if (generation === fetchGeneration) setLoading(false);
    }
  };

  const pollLogs = async () => {
    if (selectedBucket()) return;
    const searchQuery = requestQuery();
    const requestContext = rowRequestContext();
    const { from, to } = activeTimeRange();
    try {
      const page = await fetchPage(searchQuery, {
        afterTs: Math.max(newestTs() ?? from ?? 0, from ?? 0) || undefined,
        beforeTs: to
      });
      if (requestContext !== rowRequestContext()) return;
      setNewestTs((current) => Math.max(current ?? 0, page.entries.at(-1)?.ts ?? 0) || null);
      setUnavailableNodes(page.unavailableNodes.map((node) => node.nodeName));
      keepVisibleRowInPlace(() =>
        setLines((prev) =>
          mergeLogEntries(prev, page.entries).filter(
            (entry) => (from == null || entry.ts >= from) && (to == null || entry.ts < to)
          )
        )
      );
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load logs");
    }
  };

  const loadMore = async () => {
    const cursor = olderCursor();
    if (!loadingMore() && cursor != null) {
      setLoadingMore(true);
      const searchQuery = requestQuery();
      const requestContext = rowRequestContext();
      try {
        const page = await fetchPage(searchQuery, {
          afterTs: activeTimeRange().from,
          beforeTs: cursor
        });
        if (requestContext !== rowRequestContext()) return;
        setHasMore(page.nextCursor != null);
        setOlderCursor(page.nextCursor);
        wasAtBottom = false;
        keepVisibleRowInPlace(() => setLines((prev) => mergeLogEntries(prev, page.entries)));
        updateScrollFlags();
      } catch (err) {
        if (requestContext === rowRequestContext()) {
          setError(err instanceof Error ? err.message : "Failed to load earlier logs");
        }
      } finally {
        setLoadingMore(false);
      }
    }
  };

  createEffect(
    on(rowRequestContext, () => {
      setLines([]);
      setHasMore(false);
      setNewestTs(null);
      setOlderCursor(null);
      setExpanded(new Set<string>());
      setUnavailableNodes([]);
      setLoading(true);
      const generation = ++fetchGeneration;
      fetchInitialLogs(requestQuery(), generation);
    })
  );

  createEffect(
    on(
      () =>
        `${props.scope}\0${props.nodeId ?? ""}\0${requestQuery()}\0${rangeMs()}\0${histogramRefresh()}`,
      () => {
        void fetchHistogram();
      }
    )
  );

  const pollTimer = setInterval(pollLogs, POLL_INTERVAL_MS);
  const histogramTimer = props.showHistogram
    ? setInterval(() => setHistogramRefresh((value) => value + 1), HISTOGRAM_POLL_INTERVAL_MS)
    : undefined;
  onCleanup(() => {
    clearInterval(pollTimer);
    if (histogramTimer != null) clearInterval(histogramTimer);
  });

  let scrollRef: HTMLDivElement | undefined;
  let wasAtBottom = true;
  const [nearTop, setNearTop] = createSignal(false);
  const [atBottom, setAtBottom] = createSignal(true);

  const keepVisibleRowInPlace = (update: () => void) => {
    const container = scrollRef;
    const anchor = container && !wasAtBottom ? firstVisibleRow(container) : null;
    update();
    if (container && anchor) {
      const row = container.querySelector<HTMLElement>(
        `[data-log-key="${CSS.escape(anchor.key)}"]`
      );
      if (row) {
        container.scrollTop += row.getBoundingClientRect().top - anchor.top;
      }
    }
  };

  const updateScrollFlags = () => {
    if (!scrollRef) return;
    wasAtBottom = scrollRef.scrollHeight - scrollRef.scrollTop - scrollRef.clientHeight < 50;
    setAtBottom(wasAtBottom);
    setNearTop(scrollRef.scrollTop < 80);
  };

  const jumpToLatest = () => {
    if (scrollRef) {
      scrollRef.scrollTop = scrollRef.scrollHeight;
      updateScrollFlags();
    }
  };

  createEffect(
    on(filteredLines, () => {
      if (wasAtBottom && scrollRef) {
        requestAnimationFrame(() => {
          scrollRef!.scrollTop = scrollRef!.scrollHeight;
          updateScrollFlags();
        });
      } else {
        updateScrollFlags();
      }
    })
  );

  const onScroll = () => {
    updateScrollFlags();
    if (scrollRef?.scrollTop != null && scrollRef.scrollTop < 80 && hasMore() && !loadingMore()) {
      void loadMore();
    }
  };

  return (
    <div
      class={clsx("relative overflow-hidden", {
        "h-full min-h-0 flex flex-col": props.fillHeight,
        "bg-white rounded-lg border border-gray-200": !props.embedded
      })}
    >
      <Show when={props.showHistogram}>
        <div class="shrink-0 border-b border-gray-100 px-3 pt-2 pb-1">
          <div class="flex flex-wrap items-center justify-between gap-2 px-1">
            <div class="flex items-center gap-2 min-w-0">
              <Show when={histogram()}>
                <div class="flex min-w-0 flex-wrap items-center gap-x-2 gap-y-1 text-[11px]">
                  <span class="text-sm font-medium tabular-nums whitespace-nowrap text-gray-700">
                    {histogramTotal().toLocaleString()} {histogramTotal() === 1 ? "log" : "logs"}
                  </span>
                  <Show when={selectedBucket()}>
                    <span class="inline-flex max-w-full items-center overflow-hidden rounded-md border border-gray-200 bg-gray-100">
                      <span class="min-w-0 truncate py-0.5 pl-2 pr-1 font-mono text-gray-700">
                        {selectedIntervalLabel()}
                        <span class="text-gray-400"> · </span>
                        {selectedBucketCount().toLocaleString()}{" "}
                        {selectedBucketCount() === 1 ? "log" : "logs"}
                      </span>
                      <button
                        type="button"
                        onClick={() => setSelectedBucket(null)}
                        aria-label="Clear selected log interval"
                        title="Clear selected interval"
                        class="self-stretch pl-0.5 pr-1.5 text-gray-400 outline-none transition-colors hover:bg-gray-200 hover:text-gray-700"
                      >
                        <X class="size-3" />
                      </button>
                    </span>
                  </Show>
                </div>
              </Show>
              <Show when={histogramLoading() && histogram()}>
                <Loader2 class="size-3 animate-spin text-gray-400" />
              </Show>
            </div>
            <div class="flex gap-1 bg-gray-100 rounded-md p-0.5">
              <For each={TIME_RANGES}>
                {(range) => (
                  <button
                    type="button"
                    onClick={() => selectRange(range.ms)}
                    class={clsx(
                      "text-[11px] px-2.5 py-0.5 rounded outline-none tabular-nums transition-[transform,color,background-color,box-shadow] duration-150 ease-out-strong active:scale-[0.96]",
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
          <Show when={histogramError()}>
            <div
              class={clsx("flex items-center justify-center gap-2 text-xs text-red-500", {
                "h-20": !histogram(),
                "pt-2": histogram()
              })}
            >
              <span>{histogramError()}</span>
              <button
                type="button"
                onClick={() => setHistogramRefresh((value) => value + 1)}
                class="font-medium hover:text-red-700 outline-none"
              >
                Retry
              </button>
            </div>
          </Show>
          <Show when={!histogramError() && histogramLoading() && !histogram()}>
            <div class="h-20 flex items-center justify-center text-gray-400">
              <Loader2 class="size-4 animate-spin" />
            </div>
          </Show>
          <Show when={histogram()}>
            <LogHistogramChart
              data={histogram()!.buckets}
              from={histogram()!.from}
              to={histogram()!.to}
              bucketMs={histogram()!.bucketMs}
              selectedTs={selectedBucket()?.ts}
              onSelectInterval={selectHistogramInterval}
              onSelect={selectHistogramBucket}
            />
          </Show>
        </div>
      </Show>
      <Show when={error()}>
        <div class="shrink-0 p-3">
          <ErrorBanner
            message={error()!}
            onRetry={() => {
              setLoading(true);
              const generation = ++fetchGeneration;
              fetchInitialLogs(requestQuery(), generation);
            }}
          />
        </div>
      </Show>
      <Show when={unavailableNodes().length > 0}>
        <div class="shrink-0 border-b border-amber-200 bg-amber-50 px-3 py-2 text-xs text-amber-800">
          Logs from {unavailableNodes().join(", ")} are temporarily unavailable. Results are
          partial.
        </div>
      </Show>
      <div class="shrink-0 px-3 py-2 border-b border-gray-100 flex items-center gap-3">
        <LogQueryInput
          value={queryDraft()}
          appliedQuery={query()}
          catalog={queryCatalog()}
          onInput={setQueryDraft}
          onApply={applyQuery}
          onClear={() => {
            setQueryDraft("");
            setQuery("");
          }}
          onAppliedQueryChange={setQuery}
        />
      </div>
      <div
        ref={scrollRef}
        onScroll={onScroll}
        class={clsx("overflow-y-auto [overflow-anchor:none]", {
          "min-h-0 flex-1": props.fillHeight,
          "max-h-[600px]": !props.fillHeight
        })}
      >
        <Switch>
          <Match when={loading()}>
            <div class="text-gray-400 text-center py-8 font-mono text-xs">Loading logs…</div>
          </Match>
          <Match when={!loading() && filteredLines().length === 0}>
            <div class="text-gray-400 text-center py-8 font-mono text-xs">No logs available.</div>
          </Match>
          <Match when={filteredLines().length > 0}>
            <Show when={hasMore() && nearTop()}>
              <div class="sticky top-0 z-20 h-0 flex items-start justify-center pointer-events-none">
                <button
                  type="button"
                  onClick={loadMore}
                  disabled={loadingMore()}
                  class={clsx(
                    "pointer-events-auto shrink-0 whitespace-nowrap mt-9 sm:mt-12 inline-flex items-center gap-1.5 text-xs font-medium px-2.5 py-1 rounded-md border bg-white shadow-md outline-none transition-colors",
                    loadingMore()
                      ? "text-gray-400 border-gray-200 cursor-wait"
                      : "text-gray-600 border-gray-200 hover:text-indigo-600 hover:border-indigo-200 hover:bg-indigo-50"
                  )}
                >
                  <Show when={loadingMore()} fallback={<ChevronUp class="size-3" />}>
                    <Loader2 class="size-3 animate-spin" />
                  </Show>
                  Load earlier
                  <span class="text-gray-400 font-mono">+{PAGE_SIZE.toLocaleString()}</span>
                </button>
              </div>
            </Show>
            <ul class="font-mono text-xs">
              <li class="hidden sm:flex items-stretch py-1.5 px-2 border-b border-gray-200 bg-gray-100 text-[11px] font-sans font-medium text-gray-500 sticky top-0 z-10">
                <span class="w-[18px] shrink-0" />
                <span class={clsx(COL.time, "shrink-0 pr-2 truncate")}>Time</span>
                <Show when={showHost()}>
                  <span class={clsx(COL.host, "shrink-0 px-2 truncate border-l border-gray-300")}>
                    {props.showHost ? "Node" : "Replica"}
                  </span>
                </Show>
                <Show when={props.showHost}>
                  <span
                    class={clsx(COL.service, "shrink-0 px-2 truncate border-l border-gray-300")}
                  >
                    Service
                  </span>
                </Show>
                <span class={clsx(COL.level, "shrink-0 px-2 truncate border-l border-gray-300")}>
                  Level
                </span>
                <Show when={showHttp()}>
                  <span class={clsx(COL.method, "shrink-0 px-2 truncate border-l border-gray-300")}>
                    Method
                  </span>
                  <span class={clsx(COL.status, "shrink-0 px-2 truncate border-l border-gray-300")}>
                    Status
                  </span>
                </Show>
                <span class="flex-1 pl-2 truncate border-l border-gray-300">
                  {showHttp() ? "Request" : "Message"}
                </span>
              </li>
              <For each={filteredLines()}>
                {(line, index) => {
                  const key = () => logEntryKey(line);
                  return (
                    <LogRow
                      rowKey={key()}
                      line={line}
                      index={index()}
                      showHost={showHost()}
                      showService={Boolean(props.showHost)}
                      showHttp={showHttp()}
                      expanded={expanded().has(key())}
                      onToggle={() => toggleExpanded(key())}
                    />
                  );
                }}
              </For>
            </ul>
          </Match>
        </Switch>
      </div>
      <Show when={!atBottom() && filteredLines().length > 0}>
        <button
          type="button"
          onClick={jumpToLatest}
          class="absolute bottom-3 left-1/2 z-20 inline-flex -translate-x-1/2 items-center gap-1.5 whitespace-nowrap rounded-md border border-gray-200 bg-white px-2.5 py-1 text-xs font-medium text-gray-600 shadow-md outline-none transition-colors hover:border-indigo-200 hover:bg-indigo-50 hover:text-indigo-600"
        >
          <ChevronDown class="size-3" />
          Jump to latest
        </button>
      </Show>
    </div>
  );
}

function LogRow(props: {
  rowKey: string;
  line: LogEntry;
  index: number;
  showHost: boolean;
  showService: boolean;
  showHttp: boolean;
  expanded: boolean;
  onToggle: () => void;
}) {
  const host = () =>
    (props.showService ? props.line.nodeName : props.line.source) || props.line.source || "";
  const service = () => props.line.serviceId || props.line.source || "";
  const http = () => httpFields(props.line.attrs);
  return (
    <li
      data-log-key={props.rowKey}
      class={clsx("cursor-pointer border-b border-gray-50 transition-colors", {
        "bg-indigo-50/60 hover:bg-indigo-50/80": props.expanded,
        "bg-white hover:bg-gray-50": !props.expanded && props.index % 2 === 0,
        "bg-gray-50/60 hover:bg-gray-100/60": !props.expanded && props.index % 2 === 1
      })}
      onClick={() => props.onToggle()}
    >
      <div class="flex flex-col sm:flex-row sm:items-start gap-1 sm:gap-0 pl-2 pr-2 py-1.5 sm:py-1">
        <div class="flex items-center flex-wrap gap-x-2 gap-y-1 shrink-0 sm:contents">
          <ExpanderCell
            expanded={props.expanded}
            onToggle={(ev) => {
              ev.stopPropagation();
              props.onToggle();
            }}
          />
          <div class={clsx("shrink-0 sm:pr-2 sm:pt-px", COL.time)}>
            <TimeCell ts={props.line.ts} />
          </div>
          <Show when={props.showHost}>
            <div class={clsx("min-w-0 max-w-[120px] sm:max-w-none sm:px-2 sm:pt-px", COL.host)}>
              <HostCell value={host()} />
            </div>
          </Show>
          <Show when={props.showService}>
            <div class={clsx("min-w-0 max-w-[140px] sm:max-w-none sm:px-2 sm:pt-px", COL.service)}>
              <HostCell value={service()} />
            </div>
          </Show>
          <div class={clsx("shrink-0 sm:px-2 sm:pt-px", COL.level)}>
            <LevelCell level={props.line.level} />
          </div>
          <Show when={props.showHttp}>
            <div class={clsx("shrink-0 sm:px-2 sm:pt-px", COL.method)}>
              <MethodCell method={http().method} />
            </div>
            <div class={clsx("shrink-0 sm:px-2 sm:pt-px", COL.status)}>
              <StatusCell status={http().status} />
            </div>
          </Show>
        </div>
        <div class="min-w-0 flex-1 w-full pl-6 sm:pl-2 sm:pr-4 sm:w-auto">
          <Show when={http().path} fallback={<MessageCell text={props.line.text} />}>
            <PathCell
              path={http().path!}
              durationLabel={http().durationLabel}
              requestHost={http().requestHost}
              clientIp={http().clientIp}
              router={http().router}
            />
          </Show>
        </div>
      </div>
      <Show when={props.expanded}>
        <div class="border-t border-gray-100 bg-gray-50/80 px-4 py-3">
          <LogDetailPanel entry={props.line} />
        </div>
      </Show>
    </li>
  );
}

function firstVisibleRow(container: HTMLElement) {
  const containerTop = container.getBoundingClientRect().top;
  const row = Array.from(container.querySelectorAll<HTMLElement>("[data-log-key]")).find(
    (candidate) => candidate.getBoundingClientRect().bottom > containerTop
  );
  return row?.dataset.logKey
    ? { key: row.dataset.logKey, top: row.getBoundingClientRect().top }
    : null;
}

export { LogViewer };
