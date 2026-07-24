import { createEffect, createMemo, createSignal, Show, on, onCleanup } from "solid-js";
import { ChevronDown } from "lucide-solid";
import clsx from "clsx";
import type { LogEntry, LogHistogram, LogHistogramBucket, LogPage, LogScope, LogsApi } from "./api";
import { ErrorBanner } from "@maestro/kit";
import { httpFields } from "./logFormat";
import { LogHistogramPanel, type LogTimeRange, type SelectedLogBucket } from "./LogHistogramPanel";
import { LogQueryInput, type LogQueryCatalog } from "./LogQueryInput";
import { LogTable } from "./LogTable";
import { combineLogQueries, withLogHistogramGroupFilter } from "./logQueryPills";
import { buildLogQueryCatalog, logEntryKey, mergeLogEntries } from "./logViewerModel";

const PAGE_SIZE = 500;
const POLL_INTERVAL_MS = 5000;
const HISTOGRAM_POLL_INTERVAL_MS = 30_000;
const TIME_RANGES: readonly LogTimeRange[] = [
  { label: "1h", ms: 3_600_000, bucketMs: 60_000 },
  { label: "6h", ms: 21_600_000, bucketMs: 300_000 },
  { label: "24h", ms: 86_400_000, bucketMs: 600_000 },
  { label: "7d", ms: 604_800_000, bucketMs: 7_200_000 }
];
const DEFAULT_TIME_RANGE = TIME_RANGES[0]!;

function LogViewer(props: {
  api: LogsApi;
  serviceId: string;
  deploymentId: string | null;
  buildId?: string | null;
  isSystem: boolean;
  phase?: "build" | "deploy";
  embedded?: boolean;
  showHistogram?: boolean;
  histogramGroupBy?: "level" | "status";
  fillHeight?: boolean;
  query?: string;
  requiredQuery?: string;
  onQueryChange?: (query: string) => void;
  range?: string;
  onRangeChange?: (range: string) => void;
  cluster?: { nodeId?: string };
}) {
  const [lines, setLines] = createSignal<LogEntry[]>([]);
  const [loading, setLoading] = createSignal(true);
  const [error, setError] = createSignal<string | null>(null);
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
  const [expanded, setExpanded] = createSignal<Set<string>>(new Set());
  const [pollCursor, setPollCursor] = createSignal<LogPage["cursor"]>({});
  const [internalRangeMs, setInternalRangeMs] = createSignal(DEFAULT_TIME_RANGE.ms);
  const rangeMs = () => {
    if (props.onRangeChange) {
      const matched = TIME_RANGES.find((range) => range.label === props.range);
      return matched?.ms ?? DEFAULT_TIME_RANGE.ms;
    }
    return internalRangeMs();
  };
  const setRangeMs = (value: number) => {
    if (props.onRangeChange) {
      const matched = TIME_RANGES.find((range) => range.ms === value);
      props.onRangeChange(matched?.label ?? DEFAULT_TIME_RANGE.label);
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

  const phaseLines = () => {
    const all = lines().filter((line) => line.text.trim().length > 0);
    if (!props.phase) return all;
    if (props.phase === "build") return all.filter((line) => line.source?.endsWith("/build"));
    return all.filter((line) => !line.source?.endsWith("/build"));
  };

  const queryCatalog = createMemo<LogQueryCatalog>(() =>
    buildLogQueryCatalog(props.serviceId, phaseLines())
  );

  const filteredLines = () => {
    return phaseLines();
  };

  const requestQuery = () => {
    return combineLogQueries(props.requiredQuery ?? "", query());
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
      `${props.serviceId}\0${props.cluster?.nodeId ?? ""}\0${props.deploymentId ?? ""}\0${props.buildId ?? ""}\0${props.phase ?? ""}\0${requestQuery()}\0${props.showHistogram ? rangeMs() : ""}\0${selectedBucket()?.from ?? ""}\0${selectedBucket()?.to ?? ""}`
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

  const showHost = () => {
    if (props.cluster) return true;
    const seen = new Set<string>();
    for (const line of phaseLines()) {
      const host = line.hostname || line.source || "";
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

  const logScope = (): LogScope => {
    if (props.phase === "build" && props.buildId) {
      return { type: "build", serviceId: props.serviceId, buildId: props.buildId };
    }
    if (props.isSystem) {
      return props.serviceId ? { type: "system", component: props.serviceId } : { type: "system" };
    }
    if (props.deploymentId) {
      return {
        type: "deployment",
        serviceId: props.serviceId,
        deploymentId: props.deploymentId
      };
    }
    if (props.serviceId) return { type: "service", serviceId: props.serviceId };
    return { type: "all" };
  };

  const fetchPage = async (searchQuery: string, cursor?: LogPage["cursor"]) => {
    const { from, to } = activeTimeRange();
    return props.api.getLogPage({
      scope: logScope(),
      tail: PAGE_SIZE,
      ...(cursor ? { cursor } : {}),
      ...(props.cluster?.nodeId ? { nodeId: props.cluster.nodeId } : {}),
      ...(searchQuery ? { query: searchQuery } : {}),
      ...(from != null ? { from } : {}),
      ...(to != null ? { to } : {})
    });
  };

  const fetchHistogram = async () => {
    if (!props.showHistogram || props.deploymentId) return;
    const generation = ++histogramGeneration;
    const requestedRangeMs = rangeMs();
    const bucketMs = TIME_RANGES.find((range) => range.ms === requestedRangeMs)?.bucketMs;
    const to = Date.now() + 1;
    const from = to - requestedRangeMs;
    const searchQuery = requestQuery();
    setHistogramLoading(true);
    setHistogramError(null);
    try {
      const result = await props.api.getLogHistogram({
        scope: logScope(),
        from,
        to,
        bucketMs: bucketMs ?? 60_000,
        ...(props.histogramGroupBy ? { groupBy: props.histogramGroupBy } : {}),
        ...(props.cluster?.nodeId ? { nodeId: props.cluster.nodeId } : {}),
        ...(searchQuery ? { query: searchQuery } : {})
      });
      if (
        generation !== histogramGeneration ||
        searchQuery !== requestQuery() ||
        requestedRangeMs !== rangeMs()
      )
        return;
      setHistogram(result);
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
      const page = await fetchPage(searchQuery);
      if (generation !== fetchGeneration) return;
      setLines(page.entries);
      setPollCursor(page.cursor);
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
    try {
      const page = await fetchPage(searchQuery, pollCursor());
      if (requestContext !== rowRequestContext()) return;
      setPollCursor(page.cursor);
      const { from, to } = activeTimeRange();
      setLines((prev) =>
        mergeLogEntries(prev, page.entries).filter(
          (entry) => (from == null || entry.ts >= from) && (to == null || entry.ts < to)
        )
      );
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load logs");
    }
  };

  createEffect(
    on(rowRequestContext, () => {
      setLines([]);
      setPollCursor({});
      setExpanded(new Set<string>());
      setLoading(true);
      const generation = ++fetchGeneration;
      fetchInitialLogs(requestQuery(), generation);
    })
  );

  createEffect(
    on(
      () =>
        `${props.serviceId}\0${props.cluster?.nodeId ?? ""}\0${props.phase ?? ""}\0${requestQuery()}\0${rangeMs()}\0${histogramRefresh()}`,
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
  const [atBottom, setAtBottom] = createSignal(true);

  const updateScrollFlags = () => {
    if (!scrollRef) return;
    wasAtBottom = scrollRef.scrollHeight - scrollRef.scrollTop - scrollRef.clientHeight < 50;
    setAtBottom(wasAtBottom);
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
  };

  const streamView = () => props.phase === "build";

  return (
    <div
      class={clsx("relative overflow-hidden", {
        "h-full min-h-0 flex flex-col": props.fillHeight,
        "bg-white rounded-lg border border-gray-200": !props.embedded
      })}
    >
      <Show when={props.showHistogram}>
        <LogHistogramPanel
          histogram={histogram()}
          loading={histogramLoading()}
          error={histogramError()}
          selectedBucket={selectedBucket()}
          rangeMs={rangeMs()}
          ranges={TIME_RANGES}
          onClearSelected={() => setSelectedBucket(null)}
          onSelectRange={selectRange}
          onRetry={() => setHistogramRefresh((value) => value + 1)}
          onSelectInterval={selectHistogramInterval}
          onSelectBucket={selectHistogramBucket}
        />
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
        class={clsx("overflow-y-auto", {
          "min-h-0 flex-1": props.fillHeight,
          "max-h-[600px]": !props.fillHeight
        })}
      >
        <LogTable
          lines={filteredLines()}
          loading={loading()}
          cluster={Boolean(props.cluster)}
          showHost={showHost()}
          showHttp={showHttp()}
          stream={streamView()}
          expanded={expanded()}
          onToggle={toggleExpanded}
        />
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

export { LogViewer };
