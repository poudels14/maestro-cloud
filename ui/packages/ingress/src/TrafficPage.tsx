import { createMemo, createSignal, For, Show } from "solid-js";
import { useMutation, useQueryClient } from "@tanstack/solid-query";
import { useQuery } from "@maestro/sdk";
import { useLocation, useNavigate } from "@tanstack/solid-router";
import { Ban, ShieldCheck, Trash2 } from "lucide-solid";
import clsx from "clsx";
import { Card, ErrorBanner, SectionHeader } from "@maestro/kit";
import { StackedHistogramChart } from "@maestro/charts";
import type { LogsApi } from "@maestro/logs";
import type { IngressApi } from "./api";
import {
  blockedIngressTrafficQuery,
  ingressBlocklistQuery,
  ingressQueryKeys,
  ingressTrafficQuery
} from "./queries";
import { groupEntries, histogramRequestTotal, type TrafficGroup } from "./traffic";
import { TrafficIpSheet } from "./TrafficIpSheet";
import { TrafficTable } from "./TrafficTable";

const TIME_RANGES = [
  { label: "1h", ms: 3_600_000, bucketMs: 60_000 },
  { label: "6h", ms: 21_600_000, bucketMs: 300_000 },
  { label: "24h", ms: 86_400_000, bucketMs: 600_000 },
  { label: "7d", ms: 604_800_000, bucketMs: 7_200_000 }
] as const;
const DEFAULT_TIME_RANGE = TIME_RANGES[0];

function TrafficPage(props: { api: IngressApi; logsApi: LogsApi }) {
  const queryClient = useQueryClient();
  const location = useLocation();
  const search = () => location().search as { range?: string };
  const navigate = useNavigate();
  const rangeMs = () =>
    TIME_RANGES.find((range) => range.label === (search().range ?? DEFAULT_TIME_RANGE.label))?.ms ??
    DEFAULT_TIME_RANGE.ms;
  const setRange = (label: string) =>
    navigate({
      to: "/traffic",
      search: label === DEFAULT_TIME_RANGE.label ? {} : { range: label },
      replace: true
    });
  const [pendingIp, setPendingIp] = createSignal<string | null>(null);
  const [ipSheetGroup, setIpSheetGroup] = createSignal<TrafficGroup | null>(null);
  const traffic = useQuery(() => ingressTrafficQuery(props.api, rangeMs()));
  const statusHistogram = useQuery(() => ({
    queryKey: ["ingress", "status-histogram", rangeMs()],
    queryFn: () => {
      const to = Date.now() + 1;
      const bucketMs = TIME_RANGES.find((range) => range.ms === rangeMs())?.bucketMs;
      return props.logsApi.getLogHistogram({
        scope: { type: "system", component: "maestro-ingress" },
        from: to - rangeMs(),
        to,
        bucketMs: bucketMs ?? DEFAULT_TIME_RANGE.bucketMs,
        groupBy: "status"
      });
    },
    refetchInterval: 30_000
  }));
  const blockedTraffic = useQuery(() => blockedIngressTrafficQuery(props.api, rangeMs()));
  const blocklist = useQuery(() => ingressBlocklistQuery(props.api));

  const blockMutation = useMutation(() => ({
    mutationFn: (request: { ip: string; blocked: boolean }) =>
      props.api.setBlockedIp(request.ip, request.blocked),
    onSuccess: (response) => {
      queryClient.setQueryData(ingressQueryKeys.blocklist, response);
      queryClient.invalidateQueries({
        queryKey: ingressQueryKeys.traffic(rangeMs())
      });
      queryClient.invalidateQueries({
        queryKey: ingressQueryKeys.blockedTraffic(rangeMs())
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
            <StackedHistogramChart
              data={histogram().buckets}
              from={histogram().from}
              to={histogram().to}
              bucketMs={histogram().bucketMs}
              itemName="request"
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
        logsApi={props.logsApi}
        group={ipSheetGroup()}
        action={blockAction}
        range={search().range ?? DEFAULT_TIME_RANGE.label}
        onRangeChange={setRange}
        onClose={() => setIpSheetGroup(null)}
      />
    </div>
  );
}

export { TrafficPage };
