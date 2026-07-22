import { createSignal, For, onCleanup, Show } from "solid-js";
import { useMutation, useQueryClient } from "@tanstack/solid-query";
import { useQuery } from "../../lib/useQuery";
import { useLocation, useNavigate } from "@tanstack/solid-router";
import { Rocket } from "lucide-solid";
import { cancelDeployment, redeployService, restartService, stopDeployment } from "../../lib/api";
import { deploymentsQuery, queryKeys } from "../../lib/queries";
import { ErrorBanner } from "../../lib/ui";
import { ConfirmDialog } from "../home/ConfirmDialog";
import { DeploymentSheet, type SheetTabId } from "./DeploymentSheet";
import { DeploymentRow } from "./DeploymentRow";
import { showErrorToast } from "../AppToasts";
import type { Service } from "../../lib/types";

const INITIAL_VISIBLE = 10;
const LOAD_MORE_STEP = 10;

function DeploymentsTab(props: { service: Service; hasBuild: boolean }) {
  const queryClient = useQueryClient();
  const serviceId = () => props.service.meta.id;
  const deployFrozen = () => props.service.status.rollout === "frozen";
  const deployments = useQuery(() => deploymentsQuery(serviceId()));
  const location = useLocation();
  const search = () => location().search as { deployment?: string; tab?: SheetTabId };
  const navigate = useNavigate();

  const [freezeConfirmAction, setFreezeConfirmAction] = createSignal<"redeploy" | "restart" | null>(
    null
  );
  const [visibleCount, setVisibleCount] = createSignal(INITIAL_VISIBLE);

  const setUrlSheetState = (updates: { deployment?: string; tab?: SheetTabId }) =>
    navigate({
      to: "/services/$serviceId/$tab",
      params: { serviceId: serviceId(), tab: "deployments" },
      search: { ...search(), ...updates },
      replace: true
    });

  const selectedId = () => search().deployment ?? null;
  const sheetTab = () => search().tab ?? "logs";

  const invalidateDeployments = () =>
    queryClient.invalidateQueries({ queryKey: queryKeys.deployments(serviceId()) });
  const invalidateService = () =>
    Promise.all([
      queryClient.invalidateQueries({ queryKey: queryKeys.services }),
      queryClient.invalidateQueries({ queryKey: queryKeys.deployments(serviceId()) })
    ]);

  const cancelMutation = useMutation(() => ({
    mutationFn: (deploymentId: string) => cancelDeployment(serviceId(), deploymentId),
    onSuccess: invalidateDeployments
  }));
  const stopMutation = useMutation(() => ({
    mutationFn: (deploymentId: string) => stopDeployment(serviceId(), deploymentId),
    onSuccess: invalidateDeployments
  }));
  const redeployMutation = useMutation(() => ({
    mutationFn: () => redeployService(props.service),
    onSuccess: invalidateService,
    onError: (error) => showErrorToast("Redeploy failed", error)
  }));
  const restartMutation = useMutation(() => ({
    mutationFn: () => restartService(serviceId()),
    onSuccess: invalidateDeployments,
    onError: (error) => showErrorToast("Restart failed", error)
  }));

  const selectedDeployment = () => deployments.data?.find((d) => d.id === selectedId()) ?? null;
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
      setFreezeConfirmAction("redeploy");
    } else {
      redeployMutation.mutate();
    }
  };
  const handleRestart = () => {
    if (deployFrozen()) {
      setFreezeConfirmAction("restart");
    } else {
      restartMutation.mutate();
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
        open={freezeConfirmAction() !== null}
        title="Deploy is frozen"
        description={
          <>
            This service is frozen. The command will be accepted, but new rollout work remains
            queued until you unfreeze it.
          </>
        }
        confirmLabel={freezeConfirmAction() === "restart" ? "Restart" : "Redeploy"}
        onConfirm={() => {
          const action = freezeConfirmAction();
          setFreezeConfirmAction(null);
          if (action === "restart") {
            restartMutation.mutate();
          } else if (action === "redeploy") {
            redeployMutation.mutate();
          }
        }}
        onCancel={() => setFreezeConfirmAction(null)}
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
                    deployment={deployment}
                    isLatest={index() === 0}
                    isSelected={selectedId() === deployment.id}
                    onOpen={() =>
                      setUrlSheetState({
                        deployment: deployment.id,
                        tab: ["QUEUED", "BUILDING"].includes(deployment.status) ? "build" : "logs"
                      })
                    }
                    onCancel={() => cancelMutation.mutate(deployment.id)}
                    onStop={() => stopMutation.mutate(deployment.id)}
                    onRedeploy={handleRedeploy}
                    onRestart={handleRestart}
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
        deployment={selectedDeployment()}
        serviceId={serviceId()}
        hasBuild={props.hasBuild}
        tab={sheetTab()}
        onTabChange={(tab) => setUrlSheetState({ tab })}
        onClose={() => setUrlSheetState({ deployment: undefined, tab: undefined })}
      />
    </>
  );
}

export { DeploymentsTab };
