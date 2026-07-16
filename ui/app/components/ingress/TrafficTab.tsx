import { createMemo, createSignal, For, Show, type JSX } from "solid-js";
import { useMutation, useQuery, useQueryClient } from "@tanstack/solid-query";
import { Ban, ShieldCheck, Undo2 } from "lucide-solid";
import clsx from "clsx";
import { setBlockedIngressIp } from "../../lib/api";
import {
  blockedIngressTrafficQuery,
  ingressTrafficQuery,
  queryKeys,
  ingressBlocklistQuery
} from "../../lib/queries";
import type { TrafficBreakdownEntry } from "../../lib/types";
import { Card, ErrorBanner, SectionHeader, timeAgo } from "../../lib/ui";

const TIME_RANGES = [
  { label: "1h", ms: 3_600_000 },
  { label: "6h", ms: 21_600_000 },
  { label: "24h", ms: 86_400_000 },
  { label: "7d", ms: 604_800_000 }
];

interface TrafficGroup {
  value: string;
  requests: number;
  lastSeenAtMs: number;
  statuses: { statusCode: number; requests: number }[];
}

function IngressTrafficTab() {
  const queryClient = useQueryClient();
  const [rangeMs, setRangeMs] = createSignal(3_600_000);
  const [pendingIp, setPendingIp] = createSignal<string | null>(null);
  const traffic = useQuery(() => ingressTrafficQuery(rangeMs()));
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
      onClick={() => updateBlock(group.value, !blocked(group.value))}
      class={clsx(
        "inline-flex items-center gap-1.5 rounded-md border px-2 py-1 text-[11px] font-medium transition-colors disabled:opacity-50",
        blocked(group.value)
          ? "border-emerald-200 bg-emerald-50 text-emerald-700 hover:bg-emerald-100"
          : "border-red-200 bg-red-50 text-red-700 hover:bg-red-100"
      )}
    >
      {blocked(group.value) ? <ShieldCheck class="size-3" /> : <Ban class="size-3" />}
      {blocked(group.value) ? "Unblock globally" : "Block globally"}
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
      <Show when={traffic.data?.partial}>
        <div class="rounded-md border border-amber-200 bg-amber-50 px-3 py-2 text-xs text-amber-700">
          Traffic from {traffic.data?.unavailableNodes} live node
          {traffic.data?.unavailableNodes === 1 ? " is" : "s are"} temporarily unavailable. The
          totals below are partial.
        </div>
      </Show>
      <Show when={blockedTraffic.data?.partial}>
        <div class="rounded-md border border-amber-200 bg-amber-50 px-3 py-2 text-xs text-amber-700">
          Blocked traffic from {blockedTraffic.data?.unavailableNodes} live node
          {blockedTraffic.data?.unavailableNodes === 1 ? " is" : "s are"} temporarily unavailable.
          The denied totals below are partial.
        </div>
      </Show>

      <div class="flex flex-col sm:flex-row sm:items-center justify-between gap-3">
        <p class="text-xs text-gray-500">
          Counts combine available node-local summaries. Query strings are excluded from path
          groups.
        </p>
        <div class="flex gap-1 bg-gray-100 rounded-md p-0.5 self-end sm:self-auto">
          <For each={TIME_RANGES}>
            {(range) => (
              <button
                type="button"
                onClick={() => setRangeMs(range.ms)}
                class={clsx(
                  "text-xs px-3 py-1 rounded outline-none tabular-nums transition-[transform,color,background-color,box-shadow] duration-150 ease-out-strong active:scale-[0.96]",
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
                    <Undo2 class="size-3" />
                  </button>
                </span>
              )}
            </For>
          </div>
        </Card>
      </Show>

      <div class="rounded-md border border-gray-200 bg-gray-50 px-3 py-2 text-xs text-gray-600">
        Cluster-wide denied traffic contains requests rejected by the shared Traefik block policy
        before they reached a workload.
      </div>

      <TrafficTable
        title="Denied traffic by IP — cluster-wide"
        groups={blockedByIp()}
        loading={blockedTraffic.isLoading}
        empty="No blocked ingress requests recorded for this period."
        action={blockAction}
      />

      <TrafficTable
        title="Denied traffic by path — cluster-wide"
        groups={blockedByPath()}
        loading={blockedTraffic.isLoading}
        empty="No blocked ingress paths recorded for this period."
      />

      <TrafficTable
        title="Ingress traffic by IP — cluster-wide"
        groups={byIp()}
        loading={traffic.isLoading}
        empty="No ingress requests recorded for this period."
        action={blockAction}
      />

      <TrafficTable
        title="Ingress traffic by path — cluster-wide"
        groups={byPath()}
        loading={traffic.isLoading}
        empty="No ingress paths recorded for this period."
      />
    </div>
  );
}

function TrafficTable(props: {
  title: string;
  groups: TrafficGroup[];
  loading: boolean;
  empty: string;
  action?: (group: TrafficGroup) => JSX.Element;
}) {
  return (
    <Card class="overflow-hidden">
      <div class="flex items-center justify-between px-4 py-3 border-b border-gray-100">
        <SectionHeader class="text-xs">{props.title}</SectionHeader>
        <span class="text-[11px] text-gray-400 tabular-nums">{props.groups.length} values</span>
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
                  <th class="px-4 py-2 font-medium">Value</th>
                  <th class="px-4 py-2 font-medium text-right">Requests</th>
                  <th class="px-4 py-2 font-medium">Status codes</th>
                  <th class="px-4 py-2 font-medium whitespace-nowrap">Last seen</th>
                  <Show when={props.action}>
                    <th class="px-4 py-2 font-medium text-right">Action</th>
                  </Show>
                </tr>
              </thead>
              <tbody class="divide-y divide-gray-100">
                <For each={props.groups}>
                  {(group) => (
                    <tr class="text-xs text-gray-600 hover:bg-gray-50/50">
                      <td
                        class="px-4 py-2.5 font-mono text-gray-800 max-w-[26rem] break-all"
                        title={group.value}
                      >
                        {group.value}
                      </td>
                      <td class="px-4 py-2.5 text-right tabular-nums font-medium text-gray-800">
                        {group.requests.toLocaleString()}
                      </td>
                      <td class="px-4 py-2.5">
                        <div class="flex flex-wrap gap-1">
                          <For each={group.statuses}>
                            {(status) => (
                              <span
                                class={clsx(
                                  "inline-flex gap-1 rounded px-1.5 py-0.5 font-mono tabular-nums",
                                  statusColor(status.statusCode)
                                )}
                              >
                                {status.statusCode}
                                <span class="opacity-60">×{status.requests.toLocaleString()}</span>
                              </span>
                            )}
                          </For>
                        </div>
                      </td>
                      <td class="px-4 py-2.5 text-gray-400 whitespace-nowrap">
                        {timeAgo(group.lastSeenAtMs)}
                      </td>
                      <Show when={props.action}>
                        <td class="px-4 py-2.5 text-right">{props.action?.(group)}</td>
                      </Show>
                    </tr>
                  )}
                </For>
              </tbody>
            </table>
          </div>
        </Show>
      </Show>
    </Card>
  );
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

function statusColor(status: number): string {
  if (status >= 500) return "bg-red-50 text-red-700";
  if (status >= 400) return "bg-amber-50 text-amber-700";
  if (status >= 300) return "bg-blue-50 text-blue-700";
  if (status >= 200) return "bg-emerald-50 text-emerald-700";
  return "bg-gray-100 text-gray-600";
}

export { IngressTrafficTab };
