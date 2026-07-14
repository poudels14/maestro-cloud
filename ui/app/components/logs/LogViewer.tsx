import { createEffect, createSignal, For, Show, Switch, Match, on, onCleanup } from "solid-js";
import { ChevronUp, ListFilter, Search, X, Loader2 } from "lucide-solid";
import clsx from "clsx";
import type { LogEntry } from "../../lib/types";
import {
  getLogs,
  getServiceLogHistogram,
  getServiceLogs,
  getSystemLogHistogram,
  getSystemLogs,
  type LogHistogram,
  type LogHistogramBucket
} from "../../lib/api";
import { ErrorBanner } from "../../lib/ui";
import { logLevelColors, httpFields, tsFormatter } from "../../lib/logFormat";
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

const COL = {
  time: "sm:w-[118px]",
  host: "sm:w-[112px]",
  level: "sm:w-[52px]",
  method: "sm:w-[60px]",
  status: "sm:w-[56px]"
};

const PAGE_SIZE = 500;
const POLL_INTERVAL_MS = 5000;
const HISTOGRAM_POLL_INTERVAL_MS = 30_000;
const ALWAYS_SHOW_LEVELS = ["error", "warn", "info"];
const OPTIONAL_LEVELS = ["debug", "trace"];
const TIME_RANGES = [
  { label: "1h", ms: 3_600_000 },
  { label: "6h", ms: 21_600_000 },
  { label: "24h", ms: 86_400_000 },
  { label: "7d", ms: 604_800_000 }
];

type SelectedLogBucket = {
  ts: number;
  from: number;
  to: number;
};

function LogViewer(props: {
  serviceId: string;
  deploymentId: string | null;
  isSystem: boolean;
  hasBuild: boolean;
  phase?: "build" | "deploy";
  embedded?: boolean;
  showHistogram?: boolean;
}) {
  const [lines, setLines] = createSignal<LogEntry[]>([]);
  const [loading, setLoading] = createSignal(true);
  const [error, setError] = createSignal<string | null>(null);
  const [hasMore, setHasMore] = createSignal(false);
  const [queryDraft, setQueryDraft] = createSignal("");
  const [query, setQuery] = createSignal("");
  const [levelFilter, setLevelFilter] = createSignal<Set<string>>(new Set());
  const [loadingMore, setLoadingMore] = createSignal(false);
  const [expanded, setExpanded] = createSignal<Set<number>>(new Set());
  const [pollCursor, setPollCursor] = createSignal(0);
  const [rangeMs, setRangeMs] = createSignal(TIME_RANGES[0].ms);
  const [histogram, setHistogram] = createSignal<LogHistogram | null>(null);
  const [histogramLoading, setHistogramLoading] = createSignal(false);
  const [histogramError, setHistogramError] = createSignal<string | null>(null);
  const [histogramRefresh, setHistogramRefresh] = createSignal(0);
  const [selectedBucket, setSelectedBucket] = createSignal<SelectedLogBucket | null>(null);
  let fetchGeneration = 0;
  let histogramGeneration = 0;

  const hasBuildLogs = () => lines().some((line) => line.source?.endsWith("/build"));

  const phaseLines = () => {
    const all = lines().filter((line) => line.text.trim().length > 0);
    if (!props.phase) return all;
    if (props.phase === "build") return all.filter((line) => line.source?.endsWith("/build"));
    return all.filter((line) => !line.source?.endsWith("/build"));
  };

  const filteredLines = () => {
    return phaseLines();
  };

  const requestQuery = () => {
    const terms: string[] = [];
    const entered = query().trim();
    if (entered) terms.push(`(${entered})`);
    const levels = Array.from(levelFilter()).sort();
    if (levels.length === 1) terms.push(`level:${levels[0]}`);
    if (levels.length > 1) terms.push(`(${levels.map((level) => `level:${level}`).join(" OR ")})`);
    return terms.join(" AND ");
  };

  const applyQuery = () => setQuery(queryDraft().trim());

  const rowRequestContext = () =>
    `${props.serviceId}\0${props.deploymentId ?? ""}\0${props.phase ?? ""}\0${requestQuery()}\0${props.showHistogram ? rangeMs() : ""}\0${selectedBucket()?.from ?? ""}\0${selectedBucket()?.to ?? ""}`;

  const activeTimeRange = () => {
    if (!props.showHistogram) return { from: undefined, to: undefined };
    const selected = selectedBucket();
    if (selected) return { from: selected.from, to: selected.to };
    const to = Date.now() + 1;
    return { from: to - rangeMs(), to };
  };

  const selectRange = (nextRangeMs: number) => {
    if (nextRangeMs === rangeMs() && selectedBucket() == null) return;
    setRangeMs(nextRangeMs);
    setSelectedBucket(null);
    setHistogram(null);
  };

  const selectHistogramBucket = (bucket: LogHistogramBucket) => {
    const current = selectedBucket();
    if (current?.ts === bucket.ts) {
      setSelectedBucket(null);
      return;
    }
    const value = histogram();
    if (!value) return;
    setSelectedBucket({
      ts: bucket.ts,
      from: Math.max(value.from, bucket.ts),
      to: Math.min(value.to, bucket.ts + value.bucketMs)
    });
  };

  const histogramTotal = () =>
    histogram()?.buckets.reduce((total, bucket) => total + bucket.count, 0) ?? 0;

  const selectedBucketCount = () => {
    const selected = selectedBucket();
    if (!selected) return 0;
    return histogram()?.buckets.find((bucket) => bucket.ts === selected.ts)?.count ?? 0;
  };

  const histogramBucketLabel = () => {
    const bucketMs = histogram()?.bucketMs;
    if (bucketMs === 60_000) return "1 minute buckets";
    if (bucketMs === 300_000) return "5 minute buckets";
    if (bucketMs === 1_800_000) return "30 minute buckets";
    return "time buckets";
  };

  const selectedIntervalLabel = () => {
    const selected = selectedBucket();
    if (!selected) return "";
    return `${tsFormatter.format(new Date(selected.from))} – ${tsFormatter.format(new Date(selected.to))}`;
  };

  const showHost = () => {
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

  const availableLevels = () => {
    const present = new Set<string>();
    for (const line of phaseLines()) present.add(line.level.toLowerCase());
    const ordered: string[] = [...ALWAYS_SHOW_LEVELS];
    for (const level of OPTIONAL_LEVELS) {
      if (present.has(level)) ordered.push(level);
    }
    for (const level of Array.from(present).sort()) {
      if (!ordered.includes(level)) ordered.push(level);
    }
    return ordered;
  };

  const levelCounts = () => {
    const counts = new Map<string, number>();
    for (const line of phaseLines()) {
      const key = line.level.toLowerCase();
      counts.set(key, (counts.get(key) ?? 0) + 1);
    }
    return counts;
  };

  const toggleLevel = (level: string) => {
    const next = new Set(levelFilter());
    if (next.has(level)) next.delete(level);
    else next.add(level);
    setLevelFilter(next);
  };

  const toggleExpanded = (seq: number) => {
    const next = new Set(expanded());
    if (next.has(seq)) next.delete(seq);
    else next.add(seq);
    setExpanded(next);
  };

  const fetchTail = async (searchQuery: string) => {
    const { from, to } = activeTimeRange();
    if (props.isSystem)
      return getSystemLogs(
        props.serviceId,
        PAGE_SIZE,
        undefined,
        undefined,
        searchQuery || undefined,
        from,
        to
      );
    if (props.deploymentId)
      return getLogs(
        props.serviceId,
        props.deploymentId,
        PAGE_SIZE,
        undefined,
        undefined,
        props.phase,
        searchQuery || undefined,
        from,
        to
      );
    return getServiceLogs(
      props.serviceId,
      PAGE_SIZE,
      undefined,
      undefined,
      props.phase,
      searchQuery || undefined,
      from,
      to
    );
  };

  const fetchAfter = async (after: number, searchQuery: string) => {
    const { from, to } = activeTimeRange();
    if (props.isSystem)
      return getSystemLogs(
        props.serviceId,
        PAGE_SIZE,
        after,
        undefined,
        searchQuery || undefined,
        from,
        to
      );
    if (props.deploymentId)
      return getLogs(
        props.serviceId,
        props.deploymentId,
        PAGE_SIZE,
        after,
        undefined,
        props.phase,
        searchQuery || undefined,
        from,
        to
      );
    return getServiceLogs(
      props.serviceId,
      PAGE_SIZE,
      after,
      undefined,
      props.phase,
      searchQuery || undefined,
      from,
      to
    );
  };

  const fetchBefore = async (before: number, searchQuery: string) => {
    const { from, to } = activeTimeRange();
    if (props.isSystem)
      return getSystemLogs(
        props.serviceId,
        PAGE_SIZE,
        undefined,
        before,
        searchQuery || undefined,
        from,
        to
      );
    if (props.deploymentId)
      return getLogs(
        props.serviceId,
        props.deploymentId,
        PAGE_SIZE,
        undefined,
        before,
        props.phase,
        searchQuery || undefined,
        from,
        to
      );
    return getServiceLogs(
      props.serviceId,
      PAGE_SIZE,
      undefined,
      before,
      props.phase,
      searchQuery || undefined,
      from,
      to
    );
  };

  const fetchHistogram = async () => {
    if (!props.showHistogram || props.deploymentId) return;
    const generation = ++histogramGeneration;
    const requestedRangeMs = rangeMs();
    const to = Date.now() + 1;
    const from = to - requestedRangeMs;
    const searchQuery = requestQuery();
    setHistogramLoading(true);
    setHistogramError(null);
    try {
      const result = props.isSystem
        ? await getSystemLogHistogram(props.serviceId, from, to, searchQuery || undefined)
        : await getServiceLogHistogram(
            props.serviceId,
            from,
            to,
            props.phase,
            searchQuery || undefined
          );
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
      const page = await fetchTail(searchQuery);
      if (generation !== fetchGeneration) return;
      setHasMore(page.entries.length >= PAGE_SIZE);
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
      const page = await fetchAfter(pollCursor(), searchQuery);
      if (requestContext !== rowRequestContext()) return;
      setPollCursor(page.cursor);
      const { from, to } = activeTimeRange();
      setLines((prev) =>
        [...prev, ...page.entries].filter(
          (entry) => (from == null || entry.ts >= from) && (to == null || entry.ts < to)
        )
      );
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load logs");
    }
  };

  const loadMore = async () => {
    if (loadingMore()) return;
    const oldestSeq = lines()[0]?.seq;
    if (oldestSeq != null) {
      setLoadingMore(true);
      const prevHeight = scrollRef?.scrollHeight ?? 0;
      const prevTop = scrollRef?.scrollTop ?? 0;
      const searchQuery = requestQuery();
      const requestContext = rowRequestContext();
      try {
        const page = await fetchBefore(oldestSeq, searchQuery);
        if (requestContext !== rowRequestContext()) return;
        setHasMore(page.entries.length >= PAGE_SIZE);
        wasAtBottom = false;
        setLines((prev) => [...page.entries, ...prev]);
        requestAnimationFrame(() => {
          if (!scrollRef) return;
          scrollRef.scrollTop = scrollRef.scrollHeight - prevHeight + prevTop;
          updateScrollFlags();
        });
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
      setPollCursor(0);
      setExpanded(new Set<number>());
      setLoading(true);
      const generation = ++fetchGeneration;
      fetchInitialLogs(requestQuery(), generation);
    })
  );

  createEffect(
    on(
      () =>
        `${props.serviceId}\0${props.phase ?? ""}\0${requestQuery()}\0${rangeMs()}\0${histogramRefresh()}`,
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

  const updateScrollFlags = () => {
    if (!scrollRef) return;
    wasAtBottom = scrollRef.scrollHeight - scrollRef.scrollTop - scrollRef.clientHeight < 50;
    setNearTop(scrollRef.scrollTop < 80);
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

  createEffect(() => {
    if (props.phase !== "build") return;
    if (loading() || loadingMore()) return;
    if (!hasMore()) return;
    if (hasBuildLogs()) return;
    loadMore();
  });

  return (
    <div
      class={clsx("overflow-hidden", {
        "bg-white rounded-lg border border-gray-200": !props.embedded
      })}
    >
      <Show when={props.showHistogram}>
        <div class="border-b border-gray-100 px-3 pt-3 pb-1.5">
          <div class="flex flex-wrap items-center justify-between gap-2 px-1">
            <div class="flex items-center gap-2 min-w-0">
              <span class="text-xs font-semibold text-gray-700">Logs over time</span>
              <Show when={histogram()}>
                <span class="text-[10px] text-gray-400 whitespace-nowrap">
                  {histogramTotal().toLocaleString()} logs · {histogramBucketLabel()}
                </span>
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
                      "text-[11px] px-2.5 py-1 rounded outline-none tabular-nums transition-[transform,color,background-color,box-shadow] duration-150 ease-out-strong active:scale-[0.96]",
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
          <Show when={selectedBucket()}>
            <div class="mt-2 mx-1 flex items-center justify-between gap-3 rounded-md bg-indigo-50 px-2.5 py-1.5 text-[11px] text-indigo-700">
              <span class="truncate">
                {selectedBucketCount().toLocaleString()} matching logs from{" "}
                {selectedIntervalLabel()}
                {" · "}rows load {PAGE_SIZE.toLocaleString()} at a time
              </span>
              <button
                type="button"
                onClick={() => setSelectedBucket(null)}
                class="shrink-0 font-medium hover:text-indigo-900 outline-none"
              >
                Clear interval
              </button>
            </div>
          </Show>
          <Show when={histogramError()}>
            <div
              class={clsx("flex items-center justify-center gap-2 text-xs text-red-500", {
                "h-28": !histogram(),
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
            <div class="h-28 flex items-center justify-center text-gray-400">
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
              onSelect={selectHistogramBucket}
            />
          </Show>
        </div>
      </Show>
      <Show when={error()}>
        <div class="p-3">
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
      <div class="px-3 py-2 border-b border-gray-100 flex items-center gap-3">
        <div class="relative flex-1 min-w-0">
          <button
            type="button"
            onClick={applyQuery}
            title="Apply log query"
            class="absolute left-1.5 top-1/2 -translate-y-1/2 p-1 text-gray-400 hover:text-indigo-600 outline-none rounded hover:bg-indigo-50"
          >
            <Search class="size-3.5" />
          </button>
          <input
            type="text"
            value={queryDraft()}
            onInput={(ev) => setQueryDraft(ev.currentTarget.value)}
            onKeyDown={(ev) => {
              if (ev.key === "Enter") applyQuery();
            }}
            placeholder="Filter logs… e.g. @http.status_code:[500 TO 599]"
            title="Datadog-style query; press Enter to apply"
            class="w-full text-sm pl-8 pr-8 py-1.5 bg-gray-50 border border-gray-200 rounded-md outline-none focus:border-indigo-300 focus:bg-white focus:ring-2 focus:ring-indigo-100 transition-colors placeholder:text-gray-400"
          />
          <Show when={queryDraft().length > 0}>
            <button
              type="button"
              onClick={() => {
                setQueryDraft("");
                setQuery("");
              }}
              title="Clear"
              class="absolute right-1.5 top-1/2 -translate-y-1/2 p-1 text-gray-400 hover:text-gray-600 outline-none rounded hover:bg-gray-100"
            >
              <X class="size-3" />
            </button>
          </Show>
        </div>
        <Show when={availableLevels().length > 0}>
          <div class="flex items-center gap-1.5 shrink-0">
            <ListFilter class="size-3.5 text-gray-400 mr-0.5 shrink-0" />
            <For each={availableLevels()}>
              {(level) => {
                const active = () => levelFilter().has(level);
                const count = () => levelCounts().get(level) ?? 0;
                const empty = () => count() === 0;
                const colors = logLevelColors(level);
                return (
                  <button
                    type="button"
                    onClick={() => toggleLevel(level)}
                    aria-pressed={active()}
                    class={clsx(
                      "inline-flex items-center gap-1.5 text-[11px] pl-1.5 pr-2 py-0.5 rounded-md border transition-[background-color,border-color,color,transform] duration-150 ease-out-strong active:scale-[0.96] outline-none",
                      active() && `${colors.pillActive} font-medium`,
                      !active() &&
                        "border-gray-200 bg-white text-gray-600 hover:bg-gray-50 hover:text-gray-800",
                      !active() && empty() && "opacity-40"
                    )}
                  >
                    <span class={clsx("size-1.5 rounded-full shrink-0", colors.dot)} />
                    <span class="capitalize">{level}</span>
                  </button>
                );
              }}
            </For>
            <Show when={levelFilter().size > 0}>
              <button
                type="button"
                onClick={() => setLevelFilter(new Set())}
                class="text-[11px] text-gray-400 hover:text-gray-600 ml-0.5 outline-none"
              >
                Clear
              </button>
            </Show>
          </div>
        </Show>
      </div>
      <div ref={scrollRef} onScroll={onScroll} class="max-h-[600px] overflow-y-auto">
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
                    "pointer-events-auto shrink-0 whitespace-nowrap mt-9 sm:mt-12 inline-flex items-center gap-1.5 text-xs font-medium px-2.5 py-1 rounded-full border bg-white shadow-md outline-none transition-colors",
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
              <li class="hidden sm:flex items-stretch px-2 py-1.5 border-b border-gray-200 bg-gray-100 text-[10px] font-sans font-semibold uppercase tracking-normal text-gray-500 sticky top-0 z-10">
                <span class="w-[18px] shrink-0" />
                <span class={clsx(COL.time, "shrink-0 pr-2 truncate")}>Time</span>
                <Show when={showHost()}>
                  <span class={clsx(COL.host, "shrink-0 px-2 truncate border-l border-gray-300")}>
                    Host
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
                {(line, index) => (
                  <LogRow
                    line={line}
                    index={index()}
                    showHost={showHost()}
                    showHttp={showHttp()}
                    expanded={expanded().has(line.seq)}
                    onToggle={() => toggleExpanded(line.seq)}
                  />
                )}
              </For>
            </ul>
          </Match>
        </Switch>
      </div>
    </div>
  );
}

function LogRow(props: {
  line: LogEntry;
  index: number;
  showHost: boolean;
  showHttp: boolean;
  expanded: boolean;
  onToggle: () => void;
}) {
  const host = () => props.line.hostname || props.line.source || "";
  const http = () => httpFields(props.line.attrs);
  return (
    <li
      class={clsx("border-b border-gray-50 cursor-pointer transition-colors", {
        "bg-indigo-50/60 hover:bg-indigo-50/80": props.expanded,
        "bg-white hover:bg-gray-50": !props.expanded && props.index % 2 === 0,
        "bg-gray-50/60 hover:bg-gray-100/60": !props.expanded && props.index % 2 === 1
      })}
      onClick={props.onToggle}
    >
      <div class="flex flex-col sm:flex-row sm:items-start gap-1 sm:gap-0 px-2 py-1.5 sm:py-1">
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

export { LogViewer };
