import { createSignal, For, Show } from "solid-js";
import { useMutation, useQuery, useQueryClient } from "@tanstack/solid-query";
import { useNavigate } from "@tanstack/solid-router";
import { Rocket } from "lucide-solid";
import type { Service } from "../../lib/types";
import { deleteService } from "../../lib/api";
import { ErrorBanner, SectionHeader } from "../../lib/ui";
import { queryKeys, servicesQuery } from "../../lib/queries";
import { ServiceCard } from "./ServiceCard";
import { ConfirmDialog } from "./ConfirmDialog";

function ServicesGrid() {
  const services = useQuery(() => servicesQuery());
  const queryClient = useQueryClient();
  const navigate = useNavigate();
  const [deleteTarget, setDeleteTarget] = createSignal<Service | null>(null);

  const deleteMutation = useMutation(() => ({
    mutationFn: (serviceId: string) => deleteService(serviceId),
    onSuccess: () => {
      setDeleteTarget(null);
      queryClient.invalidateQueries({ queryKey: queryKeys.services });
    }
  }));

  const userServices = () => (services.data ?? []).filter((s) => !s.system);
  const systemServices = () => (services.data ?? []).filter((s) => s.system);

  const openService = (service: Service) =>
    navigate({
      to: "/services/$serviceId/$tab",
      params: { serviceId: service.id, tab: "overview" }
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
        <div class="flex items-baseline gap-2 mb-6">
          <h1 class="text-xl font-semibold text-gray-900">Services</h1>
          <span class="text-sm text-gray-400">{userServices().length}</span>
        </div>
        <Show
          when={userServices().length > 0}
          fallback={
            <div class="text-center py-20">
              <Rocket class="size-10 text-gray-300 mx-auto mb-3" />
              <p class="text-sm text-gray-400">No services configured yet.</p>
            </div>
          }
        >
          <div class="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
            <For each={userServices()}>
              {(service) => (
                <ServiceCard
                  service={service}
                  onClick={() => openService(service)}
                  onDelete={() => setDeleteTarget(service)}
                />
              )}
            </For>
          </div>
        </Show>

        <Show when={systemServices().length > 0}>
          <div class="flex items-baseline gap-2 mb-4 mt-10">
            <SectionHeader>System</SectionHeader>
          </div>
          <div class="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
            <For each={systemServices()}>
              {(service) => (
                <ServiceCard
                  service={service}
                  onClick={() => openService(service)}
                  onDelete={() => {}}
                />
              )}
            </For>
          </div>
        </Show>
      </Show>

      <ConfirmDialog
        open={deleteTarget() !== null}
        title="Remove service"
        description={
          <>
            Are you sure you want to remove{" "}
            <span class="font-medium text-gray-700">{deleteTarget()?.name}</span>? This will delete
            all deployments and cannot be undone.
          </>
        }
        confirmLabel="Remove"
        confirmBusyLabel="Removing…"
        busy={deleteMutation.isPending}
        onConfirm={() => {
          const target = deleteTarget();
          if (target) deleteMutation.mutate(target.id);
        }}
        onCancel={() => setDeleteTarget(null)}
      />
    </>
  );
}

export { ServicesGrid };
