import { createSignal, For, Show } from "solid-js";
import { Check, ChevronLeft, ChevronRight, Copy } from "lucide-solid";
import clsx from "clsx";
import { Card, SectionHeader, timeAgo } from "@maestro/kit";
import { statusClassSummary, statusCodeSummary, statusColor, type TrafficGroup } from "./traffic";

const TRAFFIC_PAGE_SIZE = 10;

function TrafficTable(props: {
  title: string;
  groups: TrafficGroup[];
  loading: boolean;
  empty: string;
  valueClass?: string | undefined;
  onSelect?: ((group: TrafficGroup) => void) | undefined;
}) {
  const [page, setPage] = createSignal(0);
  const [expandedValues, setExpandedValues] = createSignal<Set<string>>(new Set());
  const toggleExpanded = (value: string) => {
    const next = new Set(expandedValues());
    if (next.has(value)) {
      next.delete(value);
    } else {
      next.add(value);
    }
    setExpandedValues(next);
  };
  const pageCount = () => Math.max(1, Math.ceil(props.groups.length / TRAFFIC_PAGE_SIZE));
  const currentPage = () => Math.min(page(), pageCount() - 1);
  const visibleGroups = () =>
    props.groups.slice(currentPage() * TRAFFIC_PAGE_SIZE, (currentPage() + 1) * TRAFFIC_PAGE_SIZE);
  const fillerRowCount = () =>
    props.groups.length > 0 ? TRAFFIC_PAGE_SIZE - visibleGroups().length : 0;

  return (
    <Card class="overflow-hidden">
      <div class="flex items-center justify-between px-4 py-3 border-b border-gray-100">
        <SectionHeader class="text-xs">{props.title}</SectionHeader>
        <Show when={props.groups.length > 0}>
          <span class="text-[11px] text-gray-400 tabular-nums">{props.groups.length}</span>
        </Show>
      </div>
      <Show
        when={!props.loading}
        fallback={<div class="py-12 text-center text-xs text-gray-400">Loading traffic…</div>}
      >
        <Show
          when={props.groups.length > 0}
          fallback={<div class="py-12 text-center text-xs text-gray-400">{props.empty}</div>}
        >
          <div class="overflow-x-auto">
            <table class="w-full text-left">
              <thead class="bg-gray-50/70 text-[11px] text-gray-500">
                <tr>
                  <th class={clsx("px-4 py-2 font-medium", props.valueClass)}>Value</th>
                  <th class="px-4 py-2 font-medium text-right">Requests</th>
                  <th class="px-4 py-2 font-medium">Status codes</th>
                  <th class="px-4 py-2 font-medium whitespace-nowrap">Last seen</th>
                </tr>
              </thead>
              <tbody class="divide-y divide-gray-100">
                <For each={visibleGroups()}>
                  {(group) => (
                    <>
                      <tr
                        class="group cursor-pointer text-xs text-gray-600 hover:bg-gray-50/50"
                        onClick={() => {
                          if (props.onSelect) {
                            props.onSelect(group);
                          } else {
                            toggleExpanded(group.value);
                          }
                        }}
                      >
                        <td class={clsx("px-4 py-2.5", props.valueClass ?? "max-w-[26rem]")}>
                          <div class="flex items-center gap-1.5 min-w-0">
                            <span
                              class="min-w-0 truncate font-mono text-gray-800"
                              title={group.value}
                            >
                              {group.value}
                            </span>
                            <CopyValueButton value={group.value} />
                          </div>
                        </td>
                        <td class="px-4 py-2.5 text-right tabular-nums font-medium text-gray-800">
                          {group.requests.toLocaleString()}
                        </td>
                        <td class="px-4 py-2.5">
                          <div class="flex flex-wrap gap-1">
                            <For each={statusClassSummary(group.statuses)}>
                              {(statusClass) => (
                                <span
                                  class={clsx(
                                    "inline-flex gap-1 rounded px-1.5 py-0.5 font-mono tabular-nums",
                                    statusColor(statusClass.statusCode)
                                  )}
                                >
                                  {statusClass.label}
                                  <span class="opacity-60">
                                    ×{statusClass.requests.toLocaleString()}
                                  </span>
                                </span>
                              )}
                            </For>
                          </div>
                        </td>
                        <td class="px-4 py-2.5 text-gray-400 whitespace-nowrap">
                          {timeAgo(group.lastSeenAtMs)}
                        </td>
                      </tr>
                      <Show when={expandedValues().has(group.value)}>
                        <tr class="bg-gray-50/60 text-xs">
                          <td colspan={4} class="px-4 py-2.5">
                            <div class="flex flex-wrap items-center gap-1">
                              <For each={statusCodeSummary(group.statuses)}>
                                {(status) => (
                                  <span
                                    class={clsx(
                                      "inline-flex gap-1 rounded px-1.5 py-0.5 font-mono tabular-nums",
                                      statusColor(status.statusCode)
                                    )}
                                  >
                                    {status.statusCode}
                                    <span class="opacity-60">
                                      ×{status.requests.toLocaleString()}
                                    </span>
                                  </span>
                                )}
                              </For>
                            </div>
                          </td>
                        </tr>
                      </Show>
                    </>
                  )}
                </For>
                <For each={Array.from({ length: fillerRowCount() })}>
                  {() => (
                    <tr aria-hidden="true" class="text-xs">
                      <td class={clsx("px-4 py-2.5", props.valueClass ?? "max-w-[26rem]")}>
                        <span class="invisible font-mono">·</span>
                      </td>
                      <td class="px-4 py-2.5" />
                      <td class="px-4 py-2.5">
                        <span class="invisible inline-flex gap-1 rounded px-1.5 py-0.5 font-mono">
                          ·
                        </span>
                      </td>
                      <td class="px-4 py-2.5" />
                    </tr>
                  )}
                </For>
              </tbody>
            </table>
          </div>
          <Show when={props.groups.length > 0}>
            <div class="flex items-center justify-between border-t border-gray-100 px-4 py-2">
              <span class="text-[11px] text-gray-400 tabular-nums">
                {currentPage() * TRAFFIC_PAGE_SIZE + 1}–
                {Math.min((currentPage() + 1) * TRAFFIC_PAGE_SIZE, props.groups.length)} of{" "}
                {props.groups.length}
              </span>
              <div class="flex items-center gap-1.5">
                <button
                  type="button"
                  disabled={currentPage() === 0}
                  onClick={() => setPage(Math.max(0, currentPage() - 1))}
                  class="inline-flex items-center gap-1 rounded-md border border-gray-200 px-2 py-1 text-[11px] font-medium text-gray-600 outline-none transition-colors hover:bg-gray-50 disabled:cursor-not-allowed disabled:opacity-40"
                >
                  <ChevronLeft class="size-3" />
                  Prev
                </button>
                <button
                  type="button"
                  disabled={currentPage() >= pageCount() - 1}
                  onClick={() => setPage(Math.min(pageCount() - 1, currentPage() + 1))}
                  class="inline-flex items-center gap-1 rounded-md border border-gray-200 px-2 py-1 text-[11px] font-medium text-gray-600 outline-none transition-colors hover:bg-gray-50 disabled:cursor-not-allowed disabled:opacity-40"
                >
                  Next
                  <ChevronRight class="size-3" />
                </button>
              </div>
            </div>
          </Show>
        </Show>
      </Show>
    </Card>
  );
}

function CopyValueButton(props: { value: string }) {
  const [copied, setCopied] = createSignal(false);
  const copy = async (event: MouseEvent) => {
    event.stopPropagation();
    await navigator.clipboard.writeText(props.value);
    setCopied(true);
    setTimeout(() => setCopied(false), 1_500);
  };

  return (
    <button
      type="button"
      onClick={copy}
      title="Copy value"
      aria-label={`Copy ${props.value}`}
      class="shrink-0 rounded p-0.5 text-gray-300 opacity-0 outline-none transition-opacity hover:bg-gray-100 hover:text-gray-600 group-hover:opacity-100"
    >
      <Show when={copied()} fallback={<Copy class="size-3" />}>
        <Check class="size-3 text-emerald-500" />
      </Show>
    </button>
  );
}

export { TrafficTable };
