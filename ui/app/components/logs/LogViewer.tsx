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
import {
  createSolidTable,
  getCoreRowModel,
  getExpandedRowModel,
  flexRender,
  type ColumnDef
} from "@tanstack/solid-table";
import { ChevronUp, Search, X, Loader2 } from "lucide-solid";
import clsx from "clsx";
import type { LogEntry } from "../../lib/types";
import { getLogs, getServiceLogs, getSystemLogs } from "../../lib/api";
import { ErrorBanner } from "../../lib/ui";
import { logLevelColors } from "../../lib/logFormat";
import { TimeCell, ExpanderCell, HostCell, LevelCell, MessageCell } from "./cells";
import { LogDetailPanel } from "./LogDetailPanel";

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
  const lastSeq = () => lines().at(-1)?.seq ?? 0;

  const hasBuildLogs = () => lines().some((l) => l.source?.endsWith("/build"));
  const phaseLines = () => {
    const all = lines().filter((l) => l.text.trim().length > 0);
    const phase = props.phase;
    if (!phase) return all;
    return all.filter((l) => {
      if (phase === "build") return l.source?.endsWith("/build");
      return !l.source?.endsWith("/build");
    });
  };
  const filteredLines = () => {
    const query = search().trim().toLowerCase();
    const levels = levelFilter();
    return phaseLines().filter((l) => {
      if (levels.size > 0 && !levels.has(l.level.toLowerCase())) return false;
      if (query.length > 0 && !l.text.toLowerCase().includes(query)) return false;
      return true;
    });
  };
  const uniqueHosts = () => {
    const set = new Set<string>();
    for (const line of phaseLines()) {
      const host = line.hostname || line.source || "";
      if (host) set.add(host);
    }
    return set;
  };
  const showHostColumn = () => uniqueHosts().size > 1;
  const availableLevels = () => {
    const present = new Set<string>();
    for (const line of phaseLines()) present.add(line.level.toLowerCase());
    const ordered: string[] = [];
    for (const lvl of ALWAYS_SHOW_LEVELS) ordered.push(lvl);
    for (const lvl of OPTIONAL_LEVELS) {
      if (present.has(lvl)) ordered.push(lvl);
    }
    for (const lvl of Array.from(present).sort()) {
      if (!ordered.includes(lvl)) ordered.push(lvl);
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

  const columns = createMemo<ColumnDef<LogEntry>[]>(() => {
    const cols: ColumnDef<LogEntry>[] = [
      { id: "expander", size: 22, header: () => null, cell: ExpanderCell },
      { accessorKey: "ts", header: "Time", size: 140, cell: TimeCell }
    ];
    if (showHostColumn()) {
      cols.push({
        id: "host",
        header: "Host",
        size: 140,
        accessorFn: (row) => row.hostname || row.source || "",
        cell: HostCell
      });
    }
    cols.push({
      accessorKey: "level",
      header: "Level",
      size: 50,
      cell: LevelCell
    });
    cols.push({
      accessorKey: "text",
      header: "Message",
      cell: MessageCell
    });
    return cols;
  });

  const table = createSolidTable({
    get data() {
      return filteredLines();
    },
    get columns() {
      return columns();
    },
    getRowId: (row) => String(row.seq),
    getCoreRowModel: getCoreRowModel(),
    getExpandedRowModel: getExpandedRowModel(),
    getRowCanExpand: () => true
  });

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
    try {
      const newTail = tail() + LOAD_MORE_STEP;
      setTail(newTail);
      const fetched = await fetchTail(newTail);
      setHasMore(fetched.length >= newTail);
      setLines(fetched);
    } finally {
      setLoadingMore(false);
    }
  };

  createEffect(
    on(
      () => props.deploymentId,
      () => {
        setLines([]);
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

  createEffect(
    on(filteredLines, () => {
      if (wasAtBottom && scrollRef) {
        requestAnimationFrame(() => {
          scrollRef!.scrollTop = scrollRef!.scrollHeight;
        });
      }
    })
  );

  const onScroll = () => {
    if (!scrollRef) return;
    wasAtBottom = scrollRef.scrollHeight - scrollRef.scrollTop - scrollRef.clientHeight < 50;
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
      <Show when={error()}>
        <div class="p-3">
          <ErrorBanner message={error()!} onRetry={fetchInitialLogs} />
        </div>
      </Show>
      <div class="px-3 py-2 border-b border-gray-100 space-y-2">
        <div class="flex items-center gap-2">
          <div class="relative flex-1 min-w-0">
            <Search class="size-3.5 text-gray-400 absolute left-2.5 top-1/2 -translate-y-1/2 pointer-events-none" />
            <input
              type="text"
              value={search()}
              onInput={(e) => setSearch(e.currentTarget.value)}
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
            <Show when={hasMore()}>
              <div class="flex justify-center py-2">
                <button
                  type="button"
                  onClick={loadMore}
                  disabled={loadingMore()}
                  class={clsx(
                    "inline-flex items-center gap-1.5 text-xs font-medium px-2.5 py-1 rounded-full border bg-white shadow-sm outline-none transition-colors",
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
            <table class="w-full font-mono text-xs border-collapse" style="table-layout: fixed">
              <colgroup>
                <For each={table.getVisibleLeafColumns()}>
                  {(col) => (
                    <col style={col.id === "text" ? undefined : { width: `${col.getSize()}px` }} />
                  )}
                </For>
              </colgroup>
              <tbody>
                <For each={table.getRowModel().rows}>
                  {(row) => (
                    <>
                      <tr
                        class={clsx(
                          "align-top border-b border-gray-50 cursor-pointer transition-colors",
                          {
                            "bg-indigo-50/60 hover:bg-indigo-50/80": row.getIsExpanded(),
                            "bg-white hover:bg-gray-50":
                              !row.getIsExpanded() && row.index % 2 === 0,
                            "bg-gray-50/60 hover:bg-gray-100/60":
                              !row.getIsExpanded() && row.index % 2 === 1
                          }
                        )}
                        onClick={() => row.toggleExpanded()}
                      >
                        <For each={row.getVisibleCells()}>
                          {(cell) => (
                            <td
                              class={clsx("py-1 overflow-hidden", {
                                "pl-2": cell.column.id === "expander",
                                "pr-2": cell.column.id === "ts",
                                "px-2": cell.column.id !== "expander" && cell.column.id !== "text",
                                "pl-2 pr-4": cell.column.id === "text"
                              })}
                            >
                              {flexRender(cell.column.columnDef.cell, cell.getContext())}
                            </td>
                          )}
                        </For>
                      </tr>
                      <Show when={row.getIsExpanded()}>
                        <tr class="border-b border-gray-100 bg-gray-50/80">
                          <td colSpan={table.getVisibleLeafColumns().length} class="px-4 py-3">
                            <LogDetailPanel entry={row.original} />
                          </td>
                        </tr>
                      </Show>
                    </>
                  )}
                </For>
              </tbody>
            </table>
          </Match>
        </Switch>
      </div>
    </div>
  );
}

export { LogViewer };
