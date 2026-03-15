import { createFileRoute, useNavigate } from "@tanstack/solid-router";
import { createResource, createSignal, For, Show, Suspense } from "solid-js";
import { EllipsisVertical, Monitor, Rocket, Trash2 } from "lucide-solid";
import { DropdownMenu } from "@kobalte/core/dropdown-menu";
import { Dialog } from "@kobalte/core/dialog";
import type { Service } from "../lib/types";
import { deleteService, getServices } from "../lib/api";
import { StatusBadge } from "../lib/ui";

export const Route = createFileRoute("/")({
  component: ServicesPage
});

function ServiceCard(props: { service: Service; onClick: () => void; onDelete: () => void }) {
  const s = props.service;
  const isSystem = s.system === true;
  const status = isSystem ? "SYSTEM" : (s.status ?? "IDLE");
  const sourceName =
    s.build != null
      ? s.build.repo
          .replace(/\.git$/, "")
          .split("/")
          .slice(-2)
          .join("/")
      : (s.image ?? "(no source)");

  return (
    <div
      onClick={props.onClick}
      class="bg-white border border-gray-200 rounded-lg p-5 hover:border-indigo-200 hover:shadow-sm transition-all text-left w-full cursor-pointer outline-none group relative"
    >
      <div class="flex items-start justify-between gap-4">
        <div class="min-w-0">
          <div class="flex items-center gap-2.5 mb-1">
            <span class="text-base font-semibold text-gray-900 truncate group-hover:text-indigo-600 transition-colors">
              {s.name}
            </span>
          </div>
          <p class="text-sm text-gray-500 truncate">{sourceName}</p>
        </div>
        <div class="flex items-center gap-2 shrink-0">
          <StatusBadge status={status} />
          <Show when={!isSystem}>
            <div onClick={(e: MouseEvent) => e.stopPropagation()}>
              <DropdownMenu>
                <DropdownMenu.Trigger class="text-gray-400 hover:text-gray-600 p-1 rounded hover:bg-gray-100 transition-colors outline-none">
                  <EllipsisVertical class="size-4" />
                </DropdownMenu.Trigger>
                <DropdownMenu.Portal>
                  <DropdownMenu.Content class="bg-white border border-gray-200 rounded-lg shadow-lg z-50 py-1 min-w-[160px]">
                    <DropdownMenu.Item
                      class="flex items-center gap-2 px-3 py-2 text-sm text-red-600 hover:bg-red-50 cursor-pointer outline-none"
                      onSelect={() => props.onDelete()}
                    >
                      <Trash2 class="size-3.5" />
                      Remove service
                    </DropdownMenu.Item>
                  </DropdownMenu.Content>
                </DropdownMenu.Portal>
              </DropdownMenu>
            </div>
          </Show>
        </div>
      </div>
    </div>
  );
}

function DeleteConfirmDialog(props: {
  open: boolean;
  serviceName: string;
  onConfirm: () => void;
  onCancel: () => void;
}) {
  return (
    <Dialog open={props.open} onOpenChange={(open) => { if (!open) props.onCancel(); }}>
      <Dialog.Portal>
        <Dialog.Overlay class="fixed inset-0 bg-black/30 z-50" />
        <Dialog.Content class="fixed top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 bg-white rounded-xl shadow-xl z-50 p-6 w-full max-w-sm outline-none">
          <Dialog.Title class="text-base font-semibold text-gray-900 mb-2">
            Remove service
          </Dialog.Title>
          <Dialog.Description class="text-sm text-gray-500 mb-5">
            Are you sure you want to remove <span class="font-medium text-gray-700">{props.serviceName}</span>? This will delete all deployments and cannot be undone.
          </Dialog.Description>
          <div class="flex justify-end gap-2">
            <button
              type="button"
              onClick={props.onCancel}
              class="px-3 py-1.5 text-sm text-gray-600 hover:bg-gray-100 rounded-lg transition-colors outline-none"
            >
              Cancel
            </button>
            <button
              type="button"
              onClick={props.onConfirm}
              class="px-3 py-1.5 text-sm text-white bg-red-600 hover:bg-red-700 rounded-lg transition-colors outline-none"
            >
              Remove
            </button>
          </div>
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog>
  );
}

function ServicesPage() {
  const [services, { refetch }] = createResource(() => (import.meta.env.SSR ? null : true), getServices);
  const navigate = useNavigate();
  const [deleteTarget, setDeleteTarget] = createSignal<Service | null>(null);

  const confirmDelete = async () => {
    const target = deleteTarget();
    if (target) {
      await deleteService(target.id);
      setDeleteTarget(null);
      refetch();
    }
  };

  return (
    <div class="min-h-screen bg-[#fafafa]">
      <header class="bg-white border-b border-gray-200">
        <div class="max-w-5xl mx-auto px-6 h-14 flex items-center">
          <div class="flex items-center gap-2.5">
            <div class="size-7 rounded-lg bg-indigo-500 flex items-center justify-center">
              <Monitor class="size-4 text-white" />
            </div>
            <span class="text-sm font-semibold text-gray-900 tracking-tight">Maestro</span>
          </div>
        </div>
      </header>

      <main class="max-w-5xl mx-auto px-6 py-8">
        <Show
          when={!import.meta.env.SSR}
          fallback={<div class="text-sm text-gray-400 py-20 text-center">Loading services…</div>}
        >
          <Suspense
            fallback={<div class="text-sm text-gray-400 py-20 text-center">Loading services…</div>}
          >
            <Show when={services()}>
              {(list) => {
                const userServices = () => list().filter((s) => !s.system);
                const systemServices = () => list().filter((s) => s.system);

                return (
                  <>
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
                              onClick={() =>
                                navigate({
                                  to: "/services/$serviceId/$tab",
                                  params: { serviceId: service.id, tab: "overview" }
                                })
                              }
                              onDelete={() => setDeleteTarget(service)}
                            />
                          )}
                        </For>
                      </div>
                    </Show>

                    <Show when={systemServices().length > 0}>
                      <div class="flex items-baseline gap-2 mb-4 mt-10">
                        <h2 class="text-sm font-medium text-gray-400 uppercase tracking-wider">System</h2>
                      </div>
                      <div class="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
                        <For each={systemServices()}>
                          {(service) => (
                            <ServiceCard
                              service={service}
                              onClick={() =>
                                navigate({
                                  to: "/services/$serviceId/$tab",
                                  params: { serviceId: service.id, tab: "overview" }
                                })
                              }
                              onDelete={() => {}}
                            />
                          )}
                        </For>
                      </div>
                    </Show>
                  </>
                );
              }}
            </Show>
          </Suspense>
        </Show>
      </main>

      <DeleteConfirmDialog
        open={deleteTarget() !== null}
        serviceName={deleteTarget()?.name ?? ""}
        onConfirm={confirmDelete}
        onCancel={() => setDeleteTarget(null)}
      />
    </div>
  );
}
