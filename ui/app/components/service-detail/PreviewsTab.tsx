import { createSignal, For, Show } from "solid-js";
import { useMutation, useQueryClient } from "@tanstack/solid-query";
import { DropdownMenu } from "@kobalte/core/dropdown-menu";
import { EllipsisVertical, ExternalLink, GitPullRequest, Github, Trash2 } from "lucide-solid";
import type { Service } from "../../lib/types";
import { deleteService } from "../../lib/api";
import { servicePreviews } from "../../lib/previews";
import { clusterNodesQuery, queryKeys } from "../../lib/queries";
import { useQuery } from "../../lib/useQuery";
import { StatusBadge, timeAgo } from "../../lib/ui";
import { ConfirmDialog } from "../home/ConfirmDialog";
import { ReplicaRow } from "./DeploymentRow";

function PreviewsTab(props: { service: Service; services: Service[] }) {
  const previews = () => servicePreviews(props.services, props.service.id);
  const nodes = useQuery(() => clusterNodesQuery());
  const queryClient = useQueryClient();
  const [deleteTarget, setDeleteTarget] = createSignal<Service | null>(null);
  const deleteMutation = useMutation(() => ({
    mutationFn: (serviceId: string) => deleteService(serviceId),
    onSuccess: () => {
      setDeleteTarget(null);
      queryClient.invalidateQueries({ queryKey: queryKeys.services });
    }
  }));

  return (
    <Show
      when={previews().length > 0}
      fallback={
        <div class="rounded-lg border border-dashed border-gray-200 bg-white px-4 py-10 text-center text-sm text-gray-400">
          No open pull requests have a preview right now.
        </div>
      }
    >
      <div class="divide-y divide-gray-100 rounded-lg border border-gray-200 bg-white">
        <For each={previews()}>
          {(preview) => {
            const source = () => preview.previewSource!;
            const previewUrl = () =>
              preview.ingress?.host ? `https://${preview.ingress.host}` : undefined;
            return (
              <div class="flex flex-col gap-3 px-4 py-3 sm:flex-row sm:items-center">
                <div class="flex min-w-0 flex-1 items-start gap-2.5">
                  <GitPullRequest class="mt-0.5 size-4 shrink-0 text-gray-400" />
                  <div class="min-w-0">
                    <a
                      href={`/services/${encodeURIComponent(preview.id)}/overview`}
                      class="block min-w-0 outline-none hover:text-indigo-600"
                    >
                      <span class="block truncate text-sm font-medium text-gray-800">
                        PR #{source().prNumber} · {source().title}
                      </span>
                      <span class="block truncate text-xs text-gray-400">
                        {source().headRef} · opened {timeAgo(source().createdAtMs)}
                      </span>
                    </a>
                    <Show when={preview.latestDeployment?.replicas.length}>
                      <div class="mt-1.5 space-y-0.5">
                        <For each={preview.latestDeployment!.replicas}>
                          {(replica) => <ReplicaRow replica={replica} nodes={nodes.data ?? []} />}
                        </For>
                      </div>
                    </Show>
                  </div>
                </div>
                <div class="flex items-center gap-2 pl-6 sm:pl-0">
                  <Show when={source().closedAtMs}>
                    <span class="text-xs font-medium text-amber-600">closing</span>
                  </Show>
                  <StatusBadge status={preview.status} />
                  <Show when={preview.pullRequestUrl}>
                    {(url) => (
                      <a
                        href={url()}
                        target="_blank"
                        rel="noreferrer"
                        class="rounded p-1 text-gray-400 outline-none hover:bg-gray-50 hover:text-indigo-600"
                        title={`Open PR #${source().prNumber} on GitHub`}
                        aria-label={`Open PR #${source().prNumber} on GitHub`}
                      >
                        <Github class="size-4" />
                      </a>
                    )}
                  </Show>
                  <Show when={previewUrl()}>
                    {(url) => (
                      <a
                        href={url()}
                        target="_blank"
                        rel="noreferrer"
                        class="rounded p-1 text-gray-400 outline-none hover:bg-gray-50 hover:text-indigo-600"
                        title="Open preview"
                        aria-label={`Open preview for PR ${source().prNumber}`}
                      >
                        <ExternalLink class="size-4" />
                      </a>
                    )}
                  </Show>
                  <DropdownMenu>
                    <DropdownMenu.Trigger
                      class="rounded p-1 text-gray-400 outline-none hover:bg-gray-100 hover:text-gray-600"
                      aria-label={`More actions for PR ${source().prNumber}`}
                    >
                      <EllipsisVertical class="size-4" />
                    </DropdownMenu.Trigger>
                    <DropdownMenu.Portal>
                      <DropdownMenu.Content class="bg-white border border-gray-200 rounded-lg shadow-lg z-50 py-1 min-w-[160px]">
                        <DropdownMenu.Item
                          class="flex items-center gap-2 px-3 py-2 text-sm text-red-600 hover:bg-red-50 cursor-pointer outline-none"
                          onSelect={() => setDeleteTarget(preview)}
                        >
                          <Trash2 class="size-3.5" />
                          Delete preview
                        </DropdownMenu.Item>
                      </DropdownMenu.Content>
                    </DropdownMenu.Portal>
                  </DropdownMenu>
                </div>
              </div>
            );
          }}
        </For>
      </div>
      <ConfirmDialog
        open={deleteTarget() !== null}
        title="Delete preview"
        description={
          <>
            Delete the preview for{" "}
            <span class="font-medium text-gray-700">
              PR #{deleteTarget()?.previewSource?.prNumber}
            </span>
            ? Its replicas and deployments are removed.
            <Show when={!deleteTarget()?.previewSource?.closedAtMs}>
              {" "}
              The pull request is still open, so Maestro creates the preview again on its next
              check.
            </Show>
          </>
        }
        confirmLabel="Delete"
        confirmBusyLabel="Deleting…"
        destructive
        busy={deleteMutation.isPending}
        onConfirm={() => {
          const target = deleteTarget();
          if (target) deleteMutation.mutate(target.id);
        }}
        onCancel={() => setDeleteTarget(null)}
      />
    </Show>
  );
}

export { PreviewsTab };
