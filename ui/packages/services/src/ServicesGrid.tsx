import { createSignal, For, Show } from "solid-js";
import { useMutation, useQueryClient } from "@tanstack/solid-query";
import { useQuery } from "@maestro/sdk";
import { useNavigate } from "@tanstack/solid-router";
import { Rocket } from "lucide-solid";
import type { ServicesApi } from "./api";
import type { Service } from "./types";
import { serviceQueryKeys, servicesQuery } from "./queries";
import {
  isSystemService,
  systemServices as visibleSystemServices,
  userServices as visibleUserServices
} from "./serviceView";
import { ErrorBanner, SectionHeader } from "@maestro/kit";
import { ServiceCard } from "./ServiceCard";
import { ConfirmDialog } from "@maestro/kit";

function ServicesGrid(props: { api: ServicesApi }) {
  const services = useQuery(() => servicesQuery(props.api));
  const queryClient = useQueryClient();
  const navigate = useNavigate();
  const [deleteTarget, setDeleteTarget] = createSignal<Service | null>(null);

  const deleteMutation = useMutation(() => ({
    mutationFn: (service: Service) => props.api.deleteService(service),
    onSuccess: () => {
      setDeleteTarget(null);
      queryClient.invalidateQueries({ queryKey: serviceQueryKeys.all });
    }
  }));

  const userServices = () => visibleUserServices(services.data ?? []);
  const systemServices = () => visibleSystemServices(services.data ?? []);
  const deleteError = () => {
    const error = deleteMutation.error;
    if (!error) return null;
    return error instanceof Error ? error.message : "Failed to remove service";
  };

  const openService = (service: Service) =>
    navigate({
      to: "/services/$serviceId/$tab",
      params: { serviceId: service.meta.id, tab: "overview" }
    });

  return (
    <>
      <Show when={services.isError}>
        <div class="mb-6">
          <ErrorBanner message="Failed to load services" onRetry={() => services.refetch()} />
        </div>
      </Show>
      <Show
        when={services.data}
        fallback={<div class="text-sm text-gray-400 py-20 text-center">Loading services…</div>}
      >
        <div class="mb-4 flex items-end justify-between gap-3">
          <div>
            <h1 class="text-lg font-semibold text-gray-900">Services</h1>
            <p class="mt-1 text-sm text-gray-400">Workloads you deploy and operate.</p>
          </div>
          <Show when={userServices().length > 0}>
            <span class="text-xs text-gray-400 tabular-nums">
              {userServices().length} {userServices().length === 1 ? "service" : "services"}
            </span>
          </Show>
        </div>
        <Show
          when={userServices().length > 0}
          fallback={
            <div class="text-center py-16 bg-white rounded-xl border border-dashed border-gray-200">
              <Rocket class="size-9 text-gray-300 mx-auto mb-3" />
              <p class="text-sm font-medium text-gray-500">No services configured yet</p>
              <p class="text-xs text-gray-400 mt-1">
                Services you deploy will show up here alongside their status.
              </p>
            </div>
          }
        >
          <div class="grid grid-cols-1 gap-3 sm:grid-cols-2">
            <For each={userServices()}>
              {(service) => (
                <ServiceCard
                  service={service}
                  onClick={() => openService(service)}
                  onDelete={() => {
                    deleteMutation.reset();
                    setDeleteTarget(service);
                  }}
                />
              )}
            </For>
          </div>
        </Show>
        <Show when={systemServices().length > 0}>
          <div class="mt-10">
            <div class="mb-4 flex items-baseline justify-between gap-3">
              <div>
                <SectionHeader>System</SectionHeader>
                <p class="mt-0.5 text-xs text-gray-400">
                  Managed by Maestro to run the cluster — always on, not removable.
                </p>
              </div>
              <span class="text-xs text-gray-400 tabular-nums">{systemServices().length}</span>
            </div>
            <div class="grid grid-cols-1 gap-3 sm:grid-cols-2">
              <For each={systemServices()}>
                {(service) => (
                  <ServiceCard
                    service={service}
                    onClick={() => openService(service)}
                    onDelete={() => {
                      deleteMutation.reset();
                      setDeleteTarget(service);
                    }}
                  />
                )}
              </For>
            </div>
          </div>
        </Show>
      </Show>

      <ConfirmDialog
        open={deleteTarget() !== null}
        title="Remove service"
        description={
          <>
            Are you sure you want to remove{" "}
            <span class="font-medium text-gray-700">{deleteTarget()?.spec.name}</span>? This will
            delete all deployments and cannot be undone.
            <Show when={deleteError()}>
              {(error) => <span class="mt-3 block text-red-600">{error()}</span>}
            </Show>
          </>
        }
        confirmLabel="Remove"
        confirmBusyLabel="Removing…"
        busy={deleteMutation.isPending}
        onConfirm={() => {
          const target = deleteTarget();
          if (target && !isSystemService(target)) {
            deleteMutation.mutate(target);
          }
        }}
        onCancel={() => setDeleteTarget(null)}
      />
    </>
  );
}

export { ServicesGrid };
