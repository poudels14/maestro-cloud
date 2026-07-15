import { For, Show, createMemo } from "solid-js";
import { useQuery } from "@tanstack/solid-query";
import clsx from "clsx";
import { AlertTriangle } from "lucide-solid";
import type { BackupStats, ControllerStats, SinkStats } from "../../lib/types";
import { formatBytes } from "../../lib/format";
import { ErrorBanner, SectionHeader, timeAgo } from "../../lib/ui";
import { clusterConfigQuery, clusterStatsQuery } from "../../lib/queries";

type HealthLevel = "healthy" | "catching-up" | "warning" | "error" | "disabled";

function ClusterStatsSection() {
  const stats = useQuery(() => clusterStatsQuery());
  const config = useQuery(() => clusterConfigQuery());
  const errors = createMemo(() =>
    (stats.data?.warnings ?? []).filter((item) => item.severity === "error")
  );
  const warnings = createMemo(() =>
    (stats.data?.warnings ?? []).filter((item) => item.severity === "warning")
  );

  return (
    <div>
      <div class="mb-4 flex items-center justify-between gap-3">
        <SectionHeader>Cluster stats</SectionHeader>
        <Show when={stats.data}>
          {(data) => (
            <HealthPill
              level={errors().length > 0 ? "error" : warnings().length > 0 ? "warning" : "healthy"}
              label={
                errors().length > 0
                  ? `${errors().length} issue${errors().length === 1 ? "" : "s"}`
                  : warnings().length > 0
                    ? `${warnings().length} warning${warnings().length === 1 ? "" : "s"}`
                    : "healthy"
              }
            />
          )}
        </Show>
      </div>

      <Show when={stats.isError}>
        <ErrorBanner message="Failed to load cluster stats" onRetry={() => stats.refetch()} />
      </Show>

      <Show when={stats.data}>
        {(data) => {
          const controller = () => data().controller;
          const probeSink = () => controller()?.sinks.find((sink) => sink.id === "controller");
          const datadogSink = () => controller()?.sinks.find((sink) => sink.id === "datadog");
          return (
            <div class="space-y-3">
              <div class="bg-white rounded-lg border border-gray-200 divide-y divide-gray-100">
                <HealthRow
                  label="Controller"
                  level={controllerLevel(controller(), data().controllerHeartbeatAgeMs)}
                  value={
                    controller()
                      ? `v${controller()!.version} · uptime ${formatDuration(controller()!.uptimeMs)}`
                      : "No heartbeat received"
                  }
                  detail={
                    controller()
                      ? `reported ${timeAgo(controller()!.reportedAtMs)}`
                      : "The probe has not received a controller stats report"
                  }
                />
                <HealthRow
                  label="Probe"
                  level="healthy"
                  value={`v${data().probe.version} · ${data().probe.storageMode}`}
                  detail={`uptime ${formatDuration(data().probe.uptimeMs)}`}
                />
                <SinkRow
                  label="Probe log sync"
                  sink={probeSink()}
                  controllerPresent={!!controller()}
                />
                <SinkRow
                  label="Datadog logs"
                  sink={datadogSink()}
                  controllerPresent={!!controller()}
                  successfulHealthchecksFiltered={
                    config.data?.datadog
                      ? !config.data.datadog.logs["include-healthcheck"]
                      : undefined
                  }
                />
                <SpoolRow controller={controller()} />
                <DeadLetterRow controller={controller()} />
                <BackupRow backup={data().backup} />
              </div>

              <Show when={data().warnings.length > 0}>
                <div class="rounded-lg border border-amber-200 bg-amber-50 divide-y divide-amber-100">
                  <For each={data().warnings}>
                    {(warning) => (
                      <div class="flex items-start gap-2.5 px-4 py-2.5">
                        <AlertTriangle
                          class={clsx("mt-0.5 size-3.5 shrink-0", {
                            "text-red-500": warning.severity === "error",
                            "text-amber-500": warning.severity === "warning"
                          })}
                        />
                        <span
                          class={clsx("text-xs", {
                            "text-red-700": warning.severity === "error",
                            "text-amber-700": warning.severity === "warning"
                          })}
                        >
                          {warning.message}
                        </span>
                      </div>
                    )}
                  </For>
                </div>
              </Show>
            </div>
          );
        }}
      </Show>
    </div>
  );
}

function SinkRow(props: {
  label: string;
  sink?: SinkStats;
  controllerPresent: boolean;
  successfulHealthchecksFiltered?: boolean;
}) {
  return (
    <Show
      when={props.controllerPresent}
      fallback={
        <HealthRow label={props.label} level="error" value="Unknown" detail="Controller offline" />
      }
    >
      <Show
        when={props.sink}
        fallback={<HealthRow label={props.label} level="disabled" value="Disabled" />}
      >
        {(sink) => {
          const pending = () => `${sink().pendingEntries.toLocaleString()} pending`;
          const oldest = () =>
            sink().oldestPendingAtMs ? ` · oldest ${timeAgo(sink().oldestPendingAtMs!)}` : "";
          const deliveryDetail = () =>
            sink().consecutiveFailures > 0
              ? `${sink().consecutiveFailures} consecutive failures${sink().lastError ? ` · ${sink().lastError}` : ""}`
              : sink().lastSuccessAtMs
                ? `last delivered ${timeAgo(sink().lastSuccessAtMs!)}`
                : sink().pendingEntries === 0
                  ? "No delivery required yet"
                  : "Waiting for first delivery";
          const filterDetail = () => {
            if (props.successfulHealthchecksFiltered === undefined) return undefined;
            if (!props.successfulHealthchecksFiltered) {
              return "successful health checks included";
            }
            return sink().filteredEntries > 0
              ? `health-check filter active · ${sink().filteredEntries.toLocaleString()} filtered since restart`
              : "health-check filter active · none filtered since restart";
          };
          const detail = () =>
            [deliveryDetail(), filterDetail()].filter((item): item is string => !!item).join(" · ");
          return (
            <HealthRow
              label={props.label}
              level={sinkLevel(sink())}
              value={sink().pendingEntries === 0 ? "Caught up" : `${pending()}${oldest()}`}
              detail={detail()}
            />
          );
        }}
      </Show>
    </Show>
  );
}

function SpoolRow(props: { controller: ControllerStats | null }) {
  const spool = () => props.controller?.spool;
  return (
    <HealthRow
      label="Controller log spool"
      level={props.controller ? "healthy" : "error"}
      value={
        spool()
          ? `${spool()!.rowCount.toLocaleString()} rows · ${formatBytes(spool()!.databaseBytes)}`
          : "Unknown"
      }
      detail={
        spool()?.oldestEntryAtMs
          ? `oldest retained ${timeAgo(spool()!.oldestEntryAtMs!)}`
          : undefined
      }
    />
  );
}

function DeadLetterRow(props: { controller: ControllerStats | null }) {
  const dead = () => props.controller?.deadLetters;
  const level = () => {
    if (!dead()) return "error" as const;
    if (dead()!.count === 0) return "healthy" as const;
    return dead()!.count * 10 >= dead()!.capacity * 9 ? ("error" as const) : ("warning" as const);
  };
  return (
    <HealthRow
      label="Datadog dead letters"
      level={level()}
      value={
        dead()
          ? `${dead()!.count.toLocaleString()} / ${dead()!.capacity.toLocaleString()} · ${formatBytes(dead()!.payloadBytes)}`
          : "Unknown"
      }
      detail={
        dead()?.latestAtMs
          ? `latest ${timeAgo(dead()!.latestAtMs!)}${dead()!.latestStatus ? ` · HTTP ${dead()!.latestStatus}` : ""}`
          : undefined
      }
    />
  );
}

function BackupRow(props: { backup: BackupStats }) {
  const level = () => {
    if (!props.backup.configured) return "disabled" as const;
    if (
      props.backup.lastErrorAtMs &&
      (!props.backup.lastSuccessAtMs || props.backup.lastErrorAtMs > props.backup.lastSuccessAtMs)
    ) {
      return "error" as const;
    }
    return props.backup.pendingPartitions > 0 ? ("catching-up" as const) : ("healthy" as const);
  };
  const value = () => {
    if (!props.backup.configured) return "Disabled";
    if (props.backup.pendingPartitions > 0) {
      return `${props.backup.pendingPartitions} pending · ${formatBytes(props.backup.pendingBytes)}`;
    }
    return "Up to date";
  };
  const detail = () => {
    if (props.backup.lastError) return props.backup.lastError;
    if (props.backup.lastSuccessAtMs)
      return `last successful ${timeAgo(props.backup.lastSuccessAtMs)}`;
    return props.backup.configured ? "Waiting for first backup run" : undefined;
  };
  return <HealthRow label="S3 log backup" level={level()} value={value()} detail={detail()} />;
}

function HealthRow(props: { label: string; level: HealthLevel; value: string; detail?: string }) {
  return (
    <div class="px-4 py-3 flex items-start justify-between gap-5">
      <div class="min-w-0">
        <div class="text-xs text-gray-500">{props.label}</div>
        <Show when={props.detail}>
          <div class="mt-0.5 text-[11px] text-gray-400 break-words">{props.detail}</div>
        </Show>
      </div>
      <div class="min-w-0 flex items-center justify-end gap-2.5 text-right">
        <span class="text-xs font-mono text-gray-700 tabular-nums break-words">{props.value}</span>
        <HealthPill level={props.level} />
      </div>
    </div>
  );
}

function HealthPill(props: { level: HealthLevel; label?: string }) {
  const label = () => props.label ?? props.level.replace("-", " ");
  return (
    <span
      class={clsx(
        "shrink-0 inline-flex items-center gap-1.5 rounded-md border px-2 py-0.5 text-[11px] font-medium",
        {
          "border-emerald-200 bg-emerald-50 text-emerald-700": props.level === "healthy",
          "border-blue-200 bg-blue-50 text-blue-700": props.level === "catching-up",
          "border-amber-200 bg-amber-50 text-amber-700": props.level === "warning",
          "border-red-200 bg-red-50 text-red-700": props.level === "error",
          "border-gray-200 bg-gray-50 text-gray-500": props.level === "disabled"
        }
      )}
    >
      <span
        class={clsx("size-1.5 rounded-full", {
          "bg-emerald-400": props.level === "healthy",
          "bg-blue-400": props.level === "catching-up",
          "bg-amber-400": props.level === "warning",
          "bg-red-400": props.level === "error",
          "bg-gray-400": props.level === "disabled"
        })}
      />
      {label()}
    </span>
  );
}

function controllerLevel(
  controller: ControllerStats | null,
  heartbeatAgeMs: number | null
): HealthLevel {
  if (!controller || heartbeatAgeMs == null || heartbeatAgeMs > 30_000) return "error";
  return "healthy";
}

function sinkLevel(sink: SinkStats): HealthLevel {
  if (sink.consecutiveFailures > 0) return "error";
  if (sink.pendingEntries === 0) return "healthy";
  if (sink.lastCursorAdvanceAtMs && Date.now() - sink.lastCursorAdvanceAtMs <= 30_000) {
    return "catching-up";
  }
  if (sink.oldestPendingAtMs && Date.now() - sink.oldestPendingAtMs > 60_000) return "warning";
  return "catching-up";
}

function formatDuration(ms: number): string {
  const minutes = Math.floor(ms / 60_000);
  if (minutes < 1) return "<1m";
  if (minutes < 60) return `${minutes}m`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours}h ${minutes % 60}m`;
  return `${Math.floor(hours / 24)}d ${hours % 24}h`;
}

export { ClusterStatsSection };
