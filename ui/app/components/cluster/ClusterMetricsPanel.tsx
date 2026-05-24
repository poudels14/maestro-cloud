import { useQuery } from "@tanstack/solid-query";
import { For, Show } from "solid-js";
import { Activity } from "lucide-solid";
import { clusterControlMetricsQuery } from "../../lib/queries";
import type { ClusterControlMetrics } from "../../lib/api";

function ClusterMetricsPanel() {
  const metrics = useQuery(() => clusterControlMetricsQuery());
  const entries = () => Object.entries(metrics.data ?? {});

  return (
    <section>
      <div class="flex items-center gap-2 text-sm font-semibold text-gray-900 mb-3">
        <Activity class="size-4 text-gray-500" />
        Control-plane metrics
      </div>
      <Show
        when={entries().length > 0}
        fallback={
          <div class="text-sm text-gray-400 py-8 text-center bg-white border border-gray-200 rounded-lg">
            No control-plane metrics published yet — single-node mode, or cluster scheduling not yet enabled.
          </div>
        }
      >
        <div class="space-y-3">
          <For each={entries()}>
            {([nodeId, snapshot]) => (
              <NodeMetricsCard nodeId={nodeId} snapshot={snapshot} />
            )}
          </For>
        </div>
      </Show>
    </section>
  );
}

function NodeMetricsCard(props: { nodeId: string; snapshot: ClusterControlMetrics }) {
  return (
    <div class="bg-white border border-gray-200 rounded-lg p-4">
      <div class="text-xs font-mono text-gray-500 mb-3">{props.nodeId}</div>
      <div class="grid grid-cols-2 sm:grid-cols-3 gap-x-4 gap-y-3 text-xs">
        <Metric label="Leader campaigns won" value={props.snapshot.leaderCampaignsWon} />
        <Metric label="Leader resigns" value={props.snapshot.leaderResigns} />
        <Metric label="Scheduling ticks" value={props.snapshot.schedulingTicks} />
        <Metric
          label="Scheduling tick avg"
          value={`${props.snapshot.schedulingTickAvgDurationMs.toFixed(1)} ms`}
        />
        <Metric label="Scheduling failures" value={props.snapshot.schedulingTickFailures} />
        <Metric label="Assignments started" value={props.snapshot.assignmentsStarted} />
        <Metric label="Assignments stopped" value={props.snapshot.assignmentsStopped} />
        <Metric label="Start failures" value={props.snapshot.assignmentsStartFailures} />
        <Metric label="Stale swept" value={props.snapshot.staleAssignmentsSwept} />
        <Metric
          label="Unhealthy replicas (now)"
          value={props.snapshot.currentUnhealthyReplicas}
          highlight={props.snapshot.currentUnhealthyReplicas > 0}
        />
      </div>
    </div>
  );
}

function Metric(props: { label: string; value: number | string; highlight?: boolean }) {
  return (
    <div>
      <div class="text-gray-500">{props.label}</div>
      <div
        class={
          props.highlight
            ? "text-base font-semibold text-red-600 tabular-nums"
            : "text-base font-semibold text-gray-900 tabular-nums"
        }
      >
        {props.value}
      </div>
    </div>
  );
}

export { ClusterMetricsPanel };
