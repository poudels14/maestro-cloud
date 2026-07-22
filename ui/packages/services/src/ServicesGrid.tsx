import { createSignal, For, Show } from "solid-js";
import { useMutation, useQueryClient } from "@tanstack/solid-query";
import { useQuery } from "@maestro/sdk";
import { useNavigate } from "@tanstack/solid-router";
import { Rocket } from "lucide-solid";
import type { ServicesApi } from "./api";
import type { Service } from "./types";
import { serviceQueryKeys, servicesQuery } from "./queries";
import { userServices as visibleUserServices } from "./serviceView";
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
        <div class="mb-4">
          <SectionHeader>Services</SectionHeader>
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
          <div class="grid grid-cols-1 gap-3 sm:grid-cols-2">
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
      </Show>

      <ConfirmDialog
        open={deleteTarget() !== null}
        title="Remove service"
        description={
          <>
            Are you sure you want to remove{" "}
            <span class="font-medium text-gray-700">{deleteTarget()?.spec.name}</span>? This will
            delete all deployments and cannot be undone.
          </>
        }
        confirmLabel="Remove"
        confirmBusyLabel="Removing…"
        busy={deleteMutation.isPending}
        onConfirm={() => {
          const target = deleteTarget();
          if (target) deleteMutation.mutate(target);
        }}
        onCancel={() => setDeleteTarget(null)}
      />
    </>
  );
}

export { ServicesGrid };
