import { For, Show } from "solid-js";
import { ErrorBanner, StatusDot } from "@maestro/kit";
import { useQuery } from "@maestro/sdk";
import type { ServicesApi } from "../api";
import { replicaDisplayName } from "../deploymentView";
import { deploymentAssignmentsQuery, deploymentReplicasQuery, dnsRecordsQuery } from "../queries";
import { assignmentWorkloadAddress, replicaEndpoint } from "../replicaEndpoint";
import type { Assignment, Deployment, ReplicaState } from "../types";

function ReplicaOverview(props: { api: ServicesApi; deployment: Deployment }) {
  const replicas = useQuery(() => deploymentReplicasQuery(props.api, props.deployment));
  const assignments = useQuery(() => deploymentAssignmentsQuery(props.api, props.deployment));
  const dnsRecords = useQuery(() => dnsRecordsQuery(props.api));
  const orderedReplicas = () =>
    [...(replicas.data ?? [])].sort(
      (left, right) => left.spec.replicaIndex - right.spec.replicaIndex
    );
  const retry = () => {
    void Promise.all([replicas.refetch(), assignments.refetch(), dnsRecords.refetch()]);
  };

  return (
    <div>
      <h4 class="mb-2 text-xs font-medium text-gray-400">Replicas</h4>
      <Show when={replicas.isError || assignments.isError || dnsRecords.isError}>
        <ErrorBanner message="Failed to load replica details" onRetry={retry} />
      </Show>
      <Show
        when={!replicas.isLoading}
        fallback={
          <div class="rounded-lg border border-gray-200 bg-white px-4 py-4 text-xs text-gray-400">
            Loading replicas…
          </div>
        }
      >
        <Show
          when={orderedReplicas().length > 0}
          fallback={
            <div class="rounded-lg border border-gray-200 bg-white px-4 py-4 text-xs text-gray-400">
              No replicas reported for this deployment.
            </div>
          }
        >
          <div class="divide-y divide-gray-100 overflow-hidden rounded-lg border border-gray-200 bg-white">
            <For each={orderedReplicas()}>
              {(replica) => (
                <ReplicaOverviewRow
                  deployment={props.deployment}
                  replica={replica}
                  assignment={assignments.data?.find(
                    (assignment) => assignment.meta.id === replica.spec.assignmentId
                  )}
                  endpoint={replicaEndpoint(
                    props.deployment.spec.serviceId,
                    replica.spec.replicaIndex,
                    dnsRecords.data ?? []
                  )}
                />
              )}
            </For>
          </div>
        </Show>
      </Show>
    </div>
  );
}

function ReplicaOverviewRow(props: {
  deployment: Deployment;
  replica: ReplicaState;
  assignment: Assignment | undefined;
  endpoint: ReturnType<typeof replicaEndpoint>;
}) {
  const workloadAddress = () => assignmentWorkloadAddress(props.assignment) ?? "Pending";

  return (
    <div class="grid grid-cols-1 gap-3 px-4 py-3 sm:grid-cols-[minmax(0,1fr)_minmax(0,1fr)_minmax(0,1.6fr)_minmax(0,1fr)] sm:items-center">
      <div class="min-w-0">
        <div class="mb-1 text-[10px] font-medium uppercase tracking-wide text-gray-400">
          Replica
        </div>
        <div class="flex min-w-0 items-center gap-2">
          <StatusDot status={props.replica.status.phase} />
          <span class="truncate font-mono text-xs text-gray-700">
            {replicaDisplayName(props.deployment, props.replica)}
          </span>
        </div>
      </div>
      <ReplicaValue label="Node" value={props.replica.status.nodeId ?? "Pending"} />
      <ReplicaValue label="Host" value={props.endpoint?.hostname ?? "Pending"} />
      <ReplicaValue label="IP" value={workloadAddress()} />
    </div>
  );
}

function ReplicaValue(props: { label: string; value: string }) {
  return (
    <div class="min-w-0">
      <div class="mb-1 text-[10px] font-medium uppercase tracking-wide text-gray-400">
        {props.label}
      </div>
      <div class="truncate font-mono text-xs text-gray-600" title={props.value}>
        {props.value}
      </div>
    </div>
  );
}

export { ReplicaOverview };
