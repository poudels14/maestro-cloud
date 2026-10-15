import { For, Show } from "solid-js";
import { useQuery } from "../../lib/useQuery";
import clsx from "clsx";
import { AlertTriangle } from "lucide-solid";
import type { Stats } from "../../lib/types";
import { formatBytes, formatUptime } from "../../lib/format";
import { ErrorBanner, SectionHeader, timeAgo } from "../../lib/ui";
import { clusterConfigQuery, clusterStatsQuery } from "../../lib/queries";

type HealthLevel = "healthy" | "catching-up" | "warning" | "error" | "disabled";

function ClusterStatsSection() {
  const stats = useQuery(() => clusterStatsQuery());
  const config = useQuery(() => clusterConfigQuery());
  const healthchecksFiltered = () =>
    config.data?.datadog ? !config.data.datadog.logs?.includeHealthcheck : undefined;

  return (
    <div>
      <div class="mb-4">
        <SectionHeader>Cluster stats</SectionHeader>
      </div>

      <Show when={stats.isError}>
        <ErrorBanner message="Failed to load cluster stats" onRetry={() => stats.refetch()} />
      </Show>

      <Show when={stats.data}>
        {(data) => (
          <div class="space-y-3">
            <For each={data().nodes}>
              {(node) => (
                <NodeStatsCard
                  node={node}
                  isLeader={data().leader === node.nodeId}
                  healthchecksFiltered={healthchecksFiltered()}
                />
              )}
            </For>
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
        )}
      </Show>
    </div>
  );
}

function NodeStatsCard(props: {
  node: Stats.Node;
  isLeader: boolean;
  healthchecksFiltered: boolean | undefined;
}) {
  const datadog = () => props.node.sinks.find((sink) => sink.id === "datadog");
  const summary = () =>
    [
      props.node.version ? `v${props.node.version}` : null,
      props.node.uptimeMs != null ? `up ${formatUptime(props.node.uptimeMs)}` : null,
      props.isLeader ? "leader" : null
    ]
      .filter((part): part is string => part !== null)
      .join(", ");

  return (
    <div class="bg-white rounded-lg border border-gray-200 divide-y divide-gray-100">
      <HealthRow
        label={props.node.nodeName}
        level={props.node.available ? "healthy" : "error"}
        value={props.node.available ? summary() : "Unreachable"}
      />
      <Show when={props.node.available}>
        <HealthRow
          label="Telemetry"
          level={props.node.telemetry ? "healthy" : "error"}
          value={props.node.telemetry ? "Collecting logs and metrics" : "Not responding"}
        />
        <Show
          when={datadog()}
          fallback={<HealthRow label="Datadog logs" level="disabled" value="Disabled" />}
        >
          {(sink) => <SinkRow sink={sink()} healthchecksFiltered={props.healthchecksFiltered} />}
        </Show>
        <BackupRow backup={props.node.backup} />
      </Show>
    </div>
  );
}

function SinkRow(props: { sink: Stats.Sink; healthchecksFiltered: boolean | undefined }) {
  const delivery = () => {
    if (props.sink.consecutiveFailures > 0) {
      return [`${props.sink.consecutiveFailures} consecutive failures`, props.sink.lastError]
        .filter(Boolean)
        .join(": ");
    } else if (props.sink.lastSuccessAtMs) {
      return `last delivered ${timeAgo(props.sink.lastSuccessAtMs)}`;
    } else {
      return "Waiting for first delivery";
    }
  };
  const detail = () =>
    [
      delivery(),
      props.sink.deadLetters > 0
        ? `${props.sink.deadLetters.toLocaleString()} of ${props.sink.deadLetterCapacity.toLocaleString()} dead letters`
        : null,
      props.healthchecksFiltered === true ? "successful health checks filtered" : null
    ]
      .filter((part): part is string => !!part)
      .join(", ");
  return (
    <HealthRow
      label="Datadog logs"
      level={sinkLevel(props.sink)}
      value={
        props.sink.pendingEntries === 0
          ? "Caught up"
          : `${props.sink.pendingEntries.toLocaleString()} pending`
      }
      detail={detail()}
    />
  );
}

function BackupRow(props: { backup: Stats.Backup | null }) {
  const level = (): HealthLevel => {
    const backup = props.backup;
    if (!backup?.configured) {
      return "disabled";
    } else if (
      backup.lastErrorAtMs &&
      (!backup.lastSuccessAtMs || backup.lastErrorAtMs > backup.lastSuccessAtMs)
    ) {
      return "error";
    } else {
      return backup.pendingPartitions > 0 ? "catching-up" : "healthy";
    }
  };
  const value = () => {
    const backup = props.backup;
    if (!backup?.configured) {
      return "Disabled";
    } else if (backup.pendingPartitions > 0) {
      return `${backup.pendingPartitions} partitions pending`;
    } else {
      return "Up to date";
    }
  };
  const detail = () => {
    const backup = props.backup;
    if (backup?.lastError) {
      return backup.lastError;
    } else if (backup?.lastSuccessAtMs) {
      return `last run ${timeAgo(backup.lastSuccessAtMs)}, ${formatBytes(backup.uploadedBytesLastRun)} uploaded`;
    } else {
      return backup?.configured ? "Waiting for first backup run" : undefined;
    }
  };
  return <HealthRow label="S3 log backup" level={level()} value={value()} detail={detail()} />;
}

function HealthRow(props: { label: string; level: HealthLevel; value: string; detail?: string }) {
  return (
    <div class="px-4 py-3 flex items-start justify-between gap-5">
      <div class="min-w-0">
        <div class="text-xs font-medium text-gray-700">{props.label}</div>
        <Show when={props.detail}>
          <div class="mt-0.5 text-[11px] text-gray-400 break-words">{props.detail}</div>
        </Show>
      </div>
      <div class="min-w-0 flex items-center justify-end gap-2.5 text-right">
        <span class="text-xs text-gray-600 tabular-nums break-words">{props.value}</span>
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
          "border-emerald-100 bg-emerald-50 text-emerald-700": props.level === "healthy",
          "border-blue-100 bg-blue-50 text-blue-700": props.level === "catching-up",
          "border-amber-100 bg-amber-50 text-amber-700": props.level === "warning",
          "border-red-100 bg-red-50 text-red-700": props.level === "error",
          "border-gray-100 bg-gray-50 text-gray-500": props.level === "disabled"
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

function sinkLevel(sink: Stats.Sink): HealthLevel {
  if (sink.consecutiveFailures > 0) {
    return "error";
  } else if (sink.deadLetters * 10 >= sink.deadLetterCapacity * 9 && sink.deadLetters > 0) {
    return "warning";
  } else {
    return sink.pendingEntries === 0 ? "healthy" : "catching-up";
  }
}

export { ClusterStatsSection };
