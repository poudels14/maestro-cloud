import { createEffect, createSignal, For, Show, Switch, Match, on, onCleanup } from "solid-js";
import { ChevronUp, Search, X, Loader2 } from "lucide-solid";
import clsx from "clsx";
import type { LogEntry } from "../../lib/types";
import { getLogs, getServiceLogs, getSystemLogs } from "../../lib/api";
import { ErrorBanner } from "../../lib/ui";
import { logLevelColors, httpFields } from "../../lib/logFormat";
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

const COL = {
  time: "sm:w-[118px]",
  host: "sm:w-[112px]",
  level: "sm:w-[52px]",
  method: "sm:w-[60px]",
  status: "sm:w-[56px]"
};

const DEFAULT_LOG_TAIL = 1000;
const LOAD_MORE_STEP = 5000;
const POLL_INTERVAL_MS = 5000;
const ALWAYS_SHOW_LEVELS = ["error", "warn", "info"];
const OPTIONAL_LEVELS = ["debug", "trace"];

function LogViewer(props: {
  serviceId: string;
  deploymentId: string | null;
  isSystem: boolean;
  hasBuild: boolean;
  phase?: "build" | "deploy";
  embedded?: boolean;
}) {
  const [lines, setLines] = createSignal<LogEntry[]>([]);
  const [loading, setLoading] = createSignal(true);
  const [error, setError] = createSignal<string | null>(null);
  const [hasMore, setHasMore] = createSignal(false);
  const [tail, setTail] = createSignal(DEFAULT_LOG_TAIL);
  const [search, setSearch] = createSignal("");
  const [levelFilter, setLevelFilter] = createSignal<Set<string>>(new Set());
  const [loadingMore, setLoadingMore] = createSignal(false);
  const [expanded, setExpanded] = createSignal<Set<number>>(new Set());
  const lastSeq = () => lines().at(-1)?.seq ?? 0;

  const hasBuildLogs = () => lines().some((line) => line.source?.endsWith("/build"));

  const phaseLines = () => {
    const all = lines().filter((line) => line.text.trim().length > 0);
    if (!props.phase) return all;
    if (props.phase === "build") return all.filter((line) => line.source?.endsWith("/build"));
    return all.filter((line) => !line.source?.endsWith("/build"));
  };

  const filteredLines = () => {
    const query = search().trim().toLowerCase();
    const levels = levelFilter();
    return phaseLines().filter((line) => {
      if (levels.size > 0 && !levels.has(line.level.toLowerCase())) return false;
      if (query.length > 0 && !line.text.toLowerCase().includes(query)) return false;
      return true;
    });
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

  const fetchTail = async (tailSize: number) => {
    if (props.isSystem) return getSystemLogs(props.serviceId, tailSize);
    if (props.deploymentId)
      return getLogs(props.serviceId, props.deploymentId, tailSize, undefined, props.phase);
    return getServiceLogs(props.serviceId, tailSize, undefined, props.phase);
  };

  const fetchAfter = async (tailSize: number, after: number) => {
    if (props.isSystem) return getSystemLogs(props.serviceId, tailSize, after);
    if (props.deploymentId)
      return getLogs(props.serviceId, props.deploymentId, tailSize, after, props.phase);
    return getServiceLogs(props.serviceId, tailSize, after, props.phase);
  };

  const fetchInitialLogs = async () => {
    try {
      const t = tail();
      const fetched = await fetchTail(t);
      setHasMore(fetched.length >= t);
      setLines(fetched);
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load logs");
    } finally {
      setLoading(false);
    }
  };

  const pollLogs = async () => {
    try {
      const fetched = await fetchAfter(DEFAULT_LOG_TAIL, lastSeq());
      if (fetched.length === 0) return;
      setLines((prev) => [...prev, ...fetched]);
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load logs");
    }
  };

  const loadMore = async () => {
    if (loadingMore()) return;
    setLoadingMore(true);
    const prevHeight = scrollRef?.scrollHeight ?? 0;
    const prevTop = scrollRef?.scrollTop ?? 0;
    try {
      const newTail = tail() + LOAD_MORE_STEP;
      setTail(newTail);
      const fetched = await fetchTail(newTail);
      setHasMore(fetched.length >= newTail);
      wasAtBottom = false;
      setLines(fetched);
      requestAnimationFrame(() => {
        if (!scrollRef) return;
        scrollRef.scrollTop = scrollRef.scrollHeight - prevHeight + prevTop;
        updateScrollFlags();
      });
    } finally {
      setLoadingMore(false);
    }
  };

  createEffect(
    on(
      () => props.deploymentId,
      () => {
        setLines([]);
        setExpanded(new Set<number>());
        setLoading(true);
        setTail(DEFAULT_LOG_TAIL);
        fetchInitialLogs();
      }
    )
  );

  const pollTimer = setInterval(pollLogs, POLL_INTERVAL_MS);
  onCleanup(() => clearInterval(pollTimer));

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

  const onScroll = () => updateScrollFlags();

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
      <Show when={error()}>
        <div class="p-3">
          <ErrorBanner message={error()!} onRetry={fetchInitialLogs} />
        </div>
      </Show>
      <div class="px-3 py-2 border-b border-gray-100 space-y-2">
        <div class="relative">
          <Search class="size-3.5 text-gray-400 absolute left-2.5 top-1/2 -translate-y-1/2 pointer-events-none" />
          <input
            type="text"
            value={search()}
            onInput={(ev) => setSearch(ev.currentTarget.value)}
            placeholder="Search logs…"
            class="w-full text-sm pl-8 pr-16 py-1.5 bg-gray-50 border border-gray-200 rounded-md outline-none focus:border-indigo-300 focus:bg-white focus:ring-2 focus:ring-indigo-100 transition-colors placeholder:text-gray-400"
          />
          <div class="absolute right-1.5 top-1/2 -translate-y-1/2 flex items-center gap-1">
            <Show when={search().length > 0}>
              <button
                type="button"
                onClick={() => setSearch("")}
                title="Clear"
                class="p-1 text-gray-400 hover:text-gray-600 outline-none rounded hover:bg-gray-100"
              >
                <X class="size-3" />
              </button>
            </Show>
            <span class="text-[10px] font-mono text-gray-400 tabular-nums px-1">
              {filteredLines().length}
              <Show when={filteredLines().length !== phaseLines().length}>
                <span class="text-gray-300"> / {phaseLines().length}</span>
              </Show>
            </span>
          </div>
        </div>
        <Show when={availableLevels().length > 0}>
          <div class="flex items-center gap-1.5 flex-wrap">
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
                    class={clsx(
                      "inline-flex items-center gap-1.5 text-[11px] px-1.5 py-0.5 rounded transition-colors outline-none",
                      active() && `${colors.pillActive} font-medium`,
                      !active() && "text-gray-500 hover:bg-gray-100 hover:text-gray-700",
                      !active() && empty() && "opacity-40"
                    )}
                  >
                    <span class={clsx("size-1.5 rounded-full shrink-0", colors.dot)} />
                    <span class="uppercase tracking-wide">{level}</span>
                    <span class="font-mono tabular-nums text-gray-400">{count()}</span>
                  </button>
                );
              }}
            </For>
            <Show when={levelFilter().size > 0}>
              <button
                type="button"
                onClick={() => setLevelFilter(new Set())}
                class="text-[10px] text-gray-400 hover:text-gray-600 ml-1 outline-none uppercase tracking-wider"
              >
                clear
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
                  <span class="text-gray-400 font-mono">+{LOAD_MORE_STEP.toLocaleString()}</span>
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
                  {showHttp() ? "Path" : "Message"}
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
            <PathCell path={http().path!} durationLabel={http().durationLabel} />
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
