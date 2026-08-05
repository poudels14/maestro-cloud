import { createSignal, For, onCleanup, Show } from "solid-js";
import { useMutation, useQueryClient } from "@tanstack/solid-query";
import { useQuery } from "@maestro/sdk";
import { useLocation } from "@tanstack/solid-router";
import { Rocket } from "lucide-solid";
import type { LogsApi } from "@maestro/logs";
import type { ServicesApi } from "./api";
import { deploymentsQuery, dnsRecordsQuery, serviceQueryKeys } from "./queries";
import { ErrorBanner } from "@maestro/kit";
import { ConfirmDialog } from "@maestro/kit";
import { DeploymentSheet, type SheetTabId } from "./DeploymentSheet";
import { DeploymentRow } from "./DeploymentRow";
import type { Deployment, Service, ServiceDetailSearchUpdate } from "./types";
import { isSystemService } from "./serviceView";

const INITIAL_VISIBLE = 10;
const LOAD_MORE_STEP = 10;

function DeploymentsTab(props: {
  api: ServicesApi;
  logsApi: LogsApi;
  service: Service;
  onSearchChange: (updates: ServiceDetailSearchUpdate) => void;
  onError: (title: string, cause: unknown) => void;
}) {
  const queryClient = useQueryClient();
  const serviceId = () => props.service.meta.id;
  const deployFrozen = () => props.service.status.rollout === "frozen";
  const deployments = useQuery(() => deploymentsQuery(props.api, serviceId()));
  const showReplicas = () => !isSystemService(props.service);
  const dnsRecords = useQuery(() => ({
    ...dnsRecordsQuery(props.api),
    enabled: showReplicas()
  }));

  const replicaUrl = (replicaIndex: number) => {
    const prefix = `${serviceId()}-${replicaIndex}.`;
    const record = (dnsRecords.data ?? []).find((candidate) =>
      candidate.spec.name.startsWith(prefix)
    );
    if (!record) return null;
    const hostname = record.spec.name.replace(/\.$/, "");
    const port = props.service.spec.exposedPorts?.[0];
    if (port === undefined || port === 80) return `http://${hostname}`;
    if (port === 443) return `https://${hostname}`;
    return `http://${hostname}:${port}`;
  };
  const location = useLocation();
  const search = () => location().search as { deployment?: string; tab?: SheetTabId };

  const [confirmFrozenRedeploy, setConfirmFrozenRedeploy] = createSignal(false);
  const [visibleCount, setVisibleCount] = createSignal(INITIAL_VISIBLE);

  const setUrlSheetState = (updates: {
    deployment?: string | undefined;
    tab?: SheetTabId | undefined;
  }) => props.onSearchChange(updates);

  const selectedId = () => search().deployment ?? null;
  const sheetTab = () => search().tab ?? "logs";

  const invalidateDeployments = () =>
    queryClient.invalidateQueries({ queryKey: serviceQueryKeys.deployments(serviceId()) });
  const invalidateService = () =>
    Promise.all([
      queryClient.invalidateQueries({ queryKey: serviceQueryKeys.all }),
      queryClient.invalidateQueries({ queryKey: serviceQueryKeys.deployments(serviceId()) })
    ]);

  const cancelMutation = useMutation(() => ({
    mutationFn: (deployment: Deployment) => props.api.cancelDeployment(deployment),
    onSuccess: invalidateDeployments,
    onError: (error) => props.onError("Cancel failed", error)
  }));
  const removeMutation = useMutation(() => ({
    mutationFn: (deployment: Deployment) => props.api.removeDeployment(deployment),
    onSuccess: invalidateDeployments,
    onError: (error) => props.onError("Remove failed", error)
  }));
  const redeployMutation = useMutation(() => ({
    mutationFn: () => props.api.redeployService(props.service),
    onSuccess: invalidateService,
    onError: (error) => props.onError("Redeploy failed", error)
  }));
  const restartMutation = useMutation(() => ({
    mutationFn: (deployment: Deployment) => props.api.restartDeployment(deployment),
    onSuccess: invalidateDeployments,
    onError: (error) => props.onError("Restart failed", error)
  }));

  const selectedDeployment = () =>
    deployments.data?.find((deployment) => deployment.meta.id === selectedId()) ?? null;
  const visibleDeployments = () => deployments.data?.slice(0, visibleCount()) ?? [];
  const hasMore = () => (deployments.data?.length ?? 0) > visibleCount();

  let sentinelRef: HTMLDivElement | undefined;
  const attachObserver = (el: HTMLDivElement | null) => {
    sentinelRef = el ?? undefined;
    if (!sentinelRef || typeof IntersectionObserver === "undefined") return;
    const observer = new IntersectionObserver(
      (entries) => {
        for (const entry of entries) {
          if (entry.isIntersecting && hasMore()) {
            setVisibleCount((c) => c + LOAD_MORE_STEP);
          }
        }
      },
      { rootMargin: "200px" }
    );
    observer.observe(sentinelRef);
    onCleanup(() => observer.disconnect());
  };

  const handleRedeploy = () => {
    if (deployFrozen()) {
      setConfirmFrozenRedeploy(true);
    } else {
      redeployMutation.mutate();
    }
  };
  return (
    <>
      <Show when={deployments.isError}>
        <div class="mb-3">
          <ErrorBanner message="Failed to load deployments" onRetry={() => deployments.refetch()} />
        </div>
      </Show>
      <Show when={deployFrozen()}>
        <div class="mb-3 px-4 py-2.5 bg-amber-50 border border-amber-200 rounded-lg flex items-center justify-between">
          <span class="text-xs text-amber-700 font-medium">
            Deploy is frozen — auto-deploys from git watch are paused
          </span>
        </div>
      </Show>
      <ConfirmDialog
        open={confirmFrozenRedeploy()}
        title="Deploy is frozen"
        description={
          <>
            This service is frozen. The command will be accepted, but new rollout work remains
            queued until you unfreeze it.
          </>
        }
        confirmLabel="Redeploy"
        onConfirm={() => {
          setConfirmFrozenRedeploy(false);
          redeployMutation.mutate();
        }}
        onCancel={() => setConfirmFrozenRedeploy(false)}
      />

      <Show
        when={!deployments.isLoading}
        fallback={<div class="text-xs text-gray-400 py-8 text-center">Loading deployments…</div>}
      >
        <Show
          when={(deployments.data?.length ?? 0) > 0}
          fallback={
            <div class="text-center py-16 bg-white rounded-xl border border-dashed border-gray-200">
              <Rocket class="size-9 text-gray-300 mx-auto mb-3" />
              <p class="text-sm font-medium text-gray-500">No deployments yet</p>
              <p class="text-xs text-gray-400 mt-1">
                Deployments will appear here once this service ships.
              </p>
            </div>
          }
        >
          <div>
            <div class="bg-white rounded-xl border border-gray-200 divide-y divide-gray-100 overflow-hidden">
              <For each={visibleDeployments()}>
                {(deployment, index) => (
                  <DeploymentRow
                    api={props.api}
                    deployment={deployment}
                    actionsEnabled={!isSystemService(props.service)}
                    showReplicas={showReplicas()}
                    replicaUrl={replicaUrl}
                    isLatest={index() === 0}
                    isSelected={selectedId() === deployment.meta.id}
                    onOpen={() =>
                      setUrlSheetState({
                        deployment: deployment.meta.id,
                        tab:
                          deployment.spec.service.artifact.type === "build" &&
                          ["QUEUED", "BUILDING"].includes(deployment.status.phase)
                            ? "build"
                            : "logs"
                      })
                    }
                    onCancel={() => cancelMutation.mutate(deployment)}
                    onRemove={() => removeMutation.mutate(deployment)}
                    onRedeploy={handleRedeploy}
                    onRestart={() => restartMutation.mutate(deployment)}
                  />
                )}
              </For>
            </div>
            <Show when={hasMore()}>
              <div ref={attachObserver} class="h-4" />
              <div class="text-center text-xs text-gray-400 py-2">
                Showing {visibleCount()} of {deployments.data?.length ?? 0} deployments…
              </div>
            </Show>
          </div>
        </Show>
      </Show>
      <DeploymentSheet
        api={props.api}
        logsApi={props.logsApi}
        deployment={selectedDeployment()}
        tab={sheetTab()}
        onTabChange={(tab) => setUrlSheetState({ tab })}
        onClose={() => setUrlSheetState({ deployment: undefined, tab: undefined })}
      />
    </>
  );
}

export { DeploymentsTab };
