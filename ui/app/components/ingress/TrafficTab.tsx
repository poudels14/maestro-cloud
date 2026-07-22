import { createMemo, createSignal, For, Show, type JSX } from "solid-js";
import { useMutation, useQueryClient } from "@tanstack/solid-query";
import { useQuery } from "../../lib/useQuery";
import { useLocation, useNavigate } from "@tanstack/solid-router";
import { Ban, Check, ChevronLeft, ChevronRight, Copy, ShieldCheck, Trash2, X } from "lucide-solid";
import { Dialog } from "@kobalte/core/dialog";
import clsx from "clsx";
import { getLogHistogram, setBlockedIngressIp, type LogHistogram } from "../../lib/api";
import {
  blockedIngressTrafficQuery,
  ingressTrafficQuery,
  queryKeys,
  ingressBlocklistQuery
} from "../../lib/queries";
import type { TrafficBreakdownEntry } from "../../lib/types";
import { Card, ErrorBanner, SectionHeader, timeAgo } from "@maestro/kit";
import { LogHistogramChart } from "../logs/LogHistogram";
import { LogViewer } from "../logs/LogViewer";

const TIME_RANGES = [
  { label: "1h", ms: 3_600_000, bucketMs: 60_000 },
  { label: "6h", ms: 21_600_000, bucketMs: 300_000 },
  { label: "24h", ms: 86_400_000, bucketMs: 600_000 },
  { label: "7d", ms: 604_800_000, bucketMs: 7_200_000 }
];

interface TrafficGroup {
  value: string;
  requests: number;
  lastSeenAtMs: number;
  statuses: { statusCode: number; requests: number }[];
}

function IngressTrafficTab() {
  const queryClient = useQueryClient();
  const location = useLocation();
  const search = () => location().search as { range?: string };
  const navigate = useNavigate();
  const rangeMs = () =>
    TIME_RANGES.find((range) => range.label === (search().range ?? "1h"))?.ms ?? TIME_RANGES[0].ms;
  const setRange = (label: string) =>
    navigate({
      to: "/traffic",
      search: label === "1h" ? {} : { range: label },
      replace: true
    });
  const [pendingIp, setPendingIp] = createSignal<string | null>(null);
  const [ipSheetGroup, setIpSheetGroup] = createSignal<TrafficGroup | null>(null);
  const traffic = useQuery(() => ingressTrafficQuery(rangeMs()));
  const statusHistogram = useQuery(() => ({
    queryKey: ["ingress", "status-histogram", rangeMs()],
    queryFn: () => {
      const to = Date.now() + 1;
      const bucketMs = TIME_RANGES.find((range) => range.ms === rangeMs())?.bucketMs;
      return getLogHistogram({
        scope: { type: "system", component: "maestro-ingress" },
        from: to - rangeMs(),
        to,
        bucketMs: bucketMs ?? 60_000,
        groupBy: "status"
      });
    },
    refetchInterval: 30_000
  }));
  const blockedTraffic = useQuery(() => blockedIngressTrafficQuery(rangeMs()));
  const blocklist = useQuery(ingressBlocklistQuery);

  const blockMutation = useMutation(() => ({
    mutationFn: (request: { ip: string; blocked: boolean }) =>
      setBlockedIngressIp(request.ip, request.blocked),
    onSuccess: (response) => {
      queryClient.setQueryData(queryKeys.ingressBlocklist, response);
      queryClient.invalidateQueries({
        queryKey: queryKeys.ingressTraffic(rangeMs())
      });
      queryClient.invalidateQueries({
        queryKey: queryKeys.blockedIngressTraffic(rangeMs())
      });
    },
    onSettled: () => setPendingIp(null)
  }));

  const byIp = createMemo(() => groupEntries(traffic.data?.byIp ?? []));
  const byPath = createMemo(() => groupEntries(traffic.data?.byPath ?? []));
  const blockedByIp = createMemo(() => groupEntries(blockedTraffic.data?.byIp ?? []));
  const blockedByPath = createMemo(() => groupEntries(blockedTraffic.data?.byPath ?? []));
  const blockedIps = () => blocklist.data?.blockedIps ?? [];
  const blocked = (ip: string) => blockedIps().includes(ip);
  const updateBlock = (ip: string, shouldBlock: boolean) => {
    setPendingIp(ip);
    blockMutation.mutate({ ip, blocked: shouldBlock });
  };
  const blockAction = (group: TrafficGroup) => (
    <button
      type="button"
      disabled={pendingIp() !== null}
      onClick={(event) => {
        event.stopPropagation();
        updateBlock(group.value, !blocked(group.value));
      }}
      class={clsx(
        "inline-flex items-center gap-1.5 rounded-md border px-2 py-1 text-[11px] font-medium outline-none transition-colors disabled:opacity-50",
        blocked(group.value)
          ? "border-emerald-200 bg-emerald-50 text-emerald-700 hover:bg-emerald-100"
          : "border-red-200 bg-red-50 text-red-700 hover:bg-red-100"
      )}
    >
      {blocked(group.value) ? <ShieldCheck class="size-3" /> : <Ban class="size-3" />}
      {blocked(group.value) ? "Unblock IP" : "Block IP"}
    </button>
  );

  return (
    <div class="space-y-4">
      <Show when={traffic.isError}>
        <ErrorBanner message="Failed to load ingress traffic" onRetry={() => traffic.refetch()} />
      </Show>
      <Show when={blockMutation.isError}>
        <ErrorBanner
          message={
            blockMutation.error instanceof Error
              ? blockMutation.error.message
              : "Failed to update the IP blocklist"
          }
        />
      </Show>
      <Show when={blocklist.isError}>
        <ErrorBanner
          message="Failed to load the ingress blocklist"
          onRetry={() => blocklist.refetch()}
        />
      </Show>
      <Show when={blockedTraffic.isError}>
        <ErrorBanner
          message="Failed to load cluster-wide denied traffic"
          onRetry={() => blockedTraffic.refetch()}
        />
      </Show>
      <Card class="px-3 pt-3 pb-1.5">
        <div class="flex flex-wrap items-center justify-between gap-2 px-1">
          <div class="text-sm font-medium tabular-nums text-gray-700">
            <Show when={statusHistogram.data} fallback={<span class="text-gray-400">–</span>}>
              {(histogram) => (
                <>
                  {histogramRequestTotal(histogram()).toLocaleString()}{" "}
                  {histogramRequestTotal(histogram()) === 1 ? "request" : "requests"}
                </>
              )}
            </Show>
          </div>
          <div class="flex gap-1 bg-gray-100 rounded-md p-0.5">
            <For each={TIME_RANGES}>
              {(range) => (
                <button
                  type="button"
                  onClick={() => setRange(range.label)}
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
        <Show when={statusHistogram.data}>
          {(histogram) => (
            <LogHistogramChart
              data={histogram().buckets}
              from={histogram().from}
              to={histogram().to}
              bucketMs={histogram().bucketMs}
            />
          )}
        </Show>
      </Card>

      <Show when={blockedIps().length > 0}>
        <Card class="p-4">
          <div class="flex items-center justify-between mb-3">
            <SectionHeader class="text-xs">Cluster-wide blocked IPs</SectionHeader>
            <span class="text-[11px] text-gray-400 tabular-nums">{blockedIps().length}</span>
          </div>
          <div class="flex flex-wrap gap-2">
            <For each={blockedIps()}>
              {(ip) => (
                <span class="inline-flex items-center gap-2 rounded-md border border-red-200 bg-red-50 px-2.5 py-1 text-xs font-mono text-red-700">
                  {ip}
                  <button
                    type="button"
                    disabled={pendingIp() !== null}
                    onClick={() => updateBlock(ip, false)}
                    class="text-red-500 hover:text-red-800 disabled:opacity-50"
                    aria-label={`Unblock ${ip}`}
                    title="Unblock IP cluster-wide"
                  >
                    <Trash2 class="size-3" />
                  </button>
                </span>
              )}
            </For>
          </div>
        </Card>
      </Show>

      <TrafficTable
        title="Ingress traffic by IP — cluster-wide"
        groups={byIp()}
        loading={traffic.isLoading}
        empty="No ingress requests recorded for this period."
        valueClass="w-[22rem]"
        onSelect={setIpSheetGroup}
      />

      <TrafficTable
        title="Ingress traffic by path — cluster-wide"
        groups={byPath()}
        loading={traffic.isLoading}
        empty="No ingress paths recorded for this period."
      />

      <TrafficTable
        title="Denied traffic by IP — cluster-wide"
        groups={blockedByIp()}
        loading={blockedTraffic.isLoading}
        empty="No blocked ingress requests recorded for this period."
        valueClass="w-[22rem]"
        onSelect={setIpSheetGroup}
      />

      <TrafficTable
        title="Denied traffic by path — cluster-wide"
        groups={blockedByPath()}
        loading={blockedTraffic.isLoading}
        empty="No blocked ingress paths recorded for this period."
      />

      <TrafficIpSheet
        group={ipSheetGroup()}
        action={blockAction}
        range={search().range ?? "1h"}
        onRangeChange={setRange}
        onClose={() => setIpSheetGroup(null)}
      />
    </div>
  );
}

const TRAFFIC_PAGE_SIZE = 10;

function TrafficTable(props: {
  title: string;
  groups: TrafficGroup[];
  loading: boolean;
  empty: string;
  valueClass?: string;
  onSelect?: (group: TrafficGroup) => void;
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

function TrafficIpSheet(props: {
  group: TrafficGroup | null;
  action: (group: TrafficGroup) => JSX.Element;
  range: string;
  onRangeChange: (range: string) => void;
  onClose: () => void;
}) {
  return (
    <Dialog
      open={props.group !== null}
      onOpenChange={(open) => {
        if (!open) props.onClose();
      }}
    >
      <Dialog.Portal>
        <Dialog.Overlay class="fixed inset-0 bg-black/20 z-40 backdrop-blur-[1px]" />
        <Dialog.Content
          class="fixed top-0 right-0 bottom-0 w-full max-w-4xl bg-white border-l border-gray-200 shadow-2xl z-50 flex flex-col outline-none"
          onOpenAutoFocus={(event) => event.preventDefault()}
        >
          <Show when={props.group} keyed>
            {(group) => (
              <>
                <div class="px-4 sm:px-5 py-3 sm:py-4 border-b border-gray-200 shrink-0">
                  <div class="flex items-start justify-between gap-3">
                    <div class="min-w-0 flex-1">
                      <div
                        class="font-mono text-xl font-semibold text-gray-900 leading-snug tracking-tight truncate"
                        title={group.value}
                      >
                        {group.value}
                      </div>
                      <div class="flex items-center gap-2 flex-wrap text-xs mt-1.5 tabular-nums">
                        <span class="font-medium text-gray-600">
                          {group.requests.toLocaleString()}{" "}
                          {group.requests === 1 ? "request" : "requests"}
                        </span>
                        <span class="text-gray-300">·</span>
                        <span class="text-gray-400">last seen {timeAgo(group.lastSeenAtMs)}</span>
                      </div>
                    </div>
                    <div class="flex items-center gap-2">
                      {props.action(group)}
                      <Dialog.CloseButton class="p-1 text-gray-400 hover:text-gray-700 hover:bg-gray-100 rounded-md transition-colors outline-none">
                        <X class="size-4" />
                      </Dialog.CloseButton>
                    </div>
                  </div>
                </div>
                <div class="px-4 sm:px-5 py-3 border-b border-gray-100 shrink-0">
                  <div class="flex flex-wrap items-center gap-1">
                    <For each={statusCodeSummary(group.statuses)}>
                      {(status) => (
                        <span
                          class={clsx(
                            "inline-flex gap-1 rounded px-1.5 py-0.5 font-mono text-xs tabular-nums",
                            statusColor(status.statusCode)
                          )}
                        >
                          {status.statusCode}
                          <span class="opacity-60">×{status.requests.toLocaleString()}</span>
                        </span>
                      )}
                    </For>
                  </div>
                </div>
                <div class="flex-1 min-h-0 overflow-hidden">
                  <LogViewer
                    serviceId="maestro-ingress"
                    deploymentId={null}
                    isSystem
                    phase="deploy"
                    embedded={true}
                    fillHeight
                    query={ipLogQuery(group.value)}
                    range={props.range}
                    onRangeChange={props.onRangeChange}
                  />
                </div>
              </>
            )}
          </Show>
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog>
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

function histogramRequestTotal(histogram: LogHistogram) {
  return histogram.buckets.reduce((total, bucket) => total + bucket.count, 0);
}

function statusClassSummary(statuses: { statusCode: number; requests: number }[]) {
  const classes = new Map<number, number>();
  for (const status of statuses) {
    const statusClass = Math.floor(status.statusCode / 100);
    classes.set(statusClass, (classes.get(statusClass) ?? 0) + status.requests);
  }
  return Array.from(classes.entries())
    .sort(([left], [right]) => left - right)
    .map(([statusClass, requests]) => ({
      label: `${statusClass}xx`,
      statusCode: statusClass * 100,
      requests
    }));
}

function statusCodeSummary(statuses: { statusCode: number; requests: number }[]) {
  const codes = new Map<number, number>();
  for (const status of statuses) {
    codes.set(status.statusCode, (codes.get(status.statusCode) ?? 0) + status.requests);
  }
  return Array.from(codes.entries())
    .sort(([left], [right]) => left - right)
    .map(([statusCode, requests]) => ({ statusCode, requests }));
}

function groupEntries(entries: TrafficBreakdownEntry[]): TrafficGroup[] {
  const groups = new Map<string, TrafficGroup>();
  for (const entry of entries) {
    const group = groups.get(entry.value) ?? {
      value: entry.value,
      requests: 0,
      lastSeenAtMs: 0,
      statuses: []
    };
    group.requests += entry.requests;
    group.lastSeenAtMs = Math.max(group.lastSeenAtMs, entry.lastSeenAtMs);
    group.statuses.push({ statusCode: entry.statusCode, requests: entry.requests });
    groups.set(entry.value, group);
  }
  return Array.from(groups.values()).sort(
    (left, right) =>
      right.requests - left.requests ||
      right.lastSeenAtMs - left.lastSeenAtMs ||
      left.value.localeCompare(right.value)
  );
}

function ipLogQuery(ip: string): string {
  if (/^[^\s:()[\]"]+$/.test(ip)) return `@maestro.client_ip:${ip}`;
  return `@maestro.client_ip:"${ip.replaceAll("\\", "\\\\").replaceAll('"', '\\"')}"`;
}

function statusColor(status: number): string {
  if (status >= 500) return "bg-red-50 text-red-700";
  if (status >= 400) return "bg-amber-50 text-amber-700";
  if (status >= 300) return "bg-blue-50 text-blue-700";
  if (status >= 200) return "bg-emerald-50 text-emerald-700";
  return "bg-gray-100 text-gray-600";
}

export { IngressTrafficTab };
