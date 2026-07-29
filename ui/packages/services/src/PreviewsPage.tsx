import { For, Show } from "solid-js";
import { useNavigate } from "@tanstack/solid-router";
import { GitPullRequest } from "lucide-solid";
import { ErrorBanner, formatDateTime, SectionHeader, StatusBadge } from "@maestro/kit";
import { useQuery } from "@maestro/sdk";
import type { ServicesApi } from "./api";
import { servicesQuery } from "./queries";
import { previewEnabledServices, previewServices, serviceDisplayStatus } from "./serviceView";
import type { Service } from "./types";

function PreviewsPage(props: { api: ServicesApi }) {
  const services = useQuery(() => servicesQuery(props.api));
  const navigate = useNavigate();
  const previews = () => previewServices(services.data ?? []);
  const enabledServices = () => previewEnabledServices(services.data ?? []);
  const openService = (service: Service) =>
    navigate({
      to: "/services/$serviceId/$tab",
      params: { serviceId: service.meta.id, tab: "overview" }
    });

  return (
    <>
      <Show when={services.isError}>
        <div class="mb-6">
          <ErrorBanner
            message="Failed to load pull request previews"
            onRetry={() => services.refetch()}
          />
        </div>
      </Show>
      <Show
        when={services.data}
        fallback={<div class="py-20 text-center text-sm text-gray-400">Loading previews…</div>}
      >
        <div class="mb-5">
          <SectionHeader>PR Previews</SectionHeader>
          <p class="mt-1 text-sm text-gray-400">
            Ephemeral services created for open pull requests.
          </p>
        </div>

        <Show
          when={previews().length > 0}
          fallback={
            <div class="mb-7 rounded-lg border border-dashed border-gray-200 bg-white py-12 text-center">
              <GitPullRequest class="mx-auto mb-3 size-9 text-gray-300" />
              <p class="text-sm font-medium text-gray-600">No pull request previews found.</p>
              <p class="mt-1 text-xs text-gray-400">
                Open, non-draft pull requests appear here after GitHub discovery.
              </p>
            </div>
          }
        >
          <div class="mb-7 divide-y divide-gray-100 rounded-lg border border-gray-200 bg-white">
            <For each={previews()}>
              {(service) => <PreviewRow service={service} onOpen={() => openService(service)} />}
            </For>
          </div>
        </Show>

        <Show when={enabledServices().length > 0}>
          <div>
            <h3 class="mb-2 text-xs font-medium text-gray-400">Preview-enabled services</h3>
            <div class="divide-y divide-gray-100 rounded-lg border border-gray-200 bg-white">
              <For each={enabledServices()}>
                {(service) => (
                  <button
                    type="button"
                    onClick={() => openService(service)}
                    class="flex w-full items-center justify-between gap-4 px-4 py-3 text-left outline-none hover:bg-gray-50"
                  >
                    <span class="min-w-0">
                      <span class="block truncate text-sm font-medium text-gray-800">
                        {service.spec.name}
                      </span>
                      <span class="block text-xs text-gray-400">
                        {service.spec.preview!.replicas}{" "}
                        {service.spec.preview!.replicas === 1 ? "replica" : "replicas"} per preview
                      </span>
                    </span>
                    <span class="shrink-0 text-xs text-gray-400">Enabled</span>
                  </button>
                )}
              </For>
            </div>
          </div>
        </Show>

        <Show when={enabledServices().length === 0 && previews().length === 0}>
          <p class="text-center text-xs text-gray-400">
            No services have pull request previews enabled.
          </p>
        </Show>
      </Show>
    </>
  );
}

function PreviewRow(props: { service: Service; onOpen: () => void }) {
  const preview = () => props.service.previewResource!;
  const pullRequestUrl = () =>
    `https://github.com/${preview().spec.repository}/pull/${preview().spec.pullRequestNumber}`;

  return (
    <div class="flex flex-col gap-3 px-4 py-3 sm:flex-row sm:items-center">
      <div class="flex min-w-0 flex-1 items-start gap-2.5">
        <GitPullRequest class="mt-0.5 size-4 shrink-0 text-gray-400" />
        <span class="min-w-0">
          <span class="flex min-w-0 items-baseline gap-1.5 text-sm">
            <a
              href={pullRequestUrl()}
              target="_blank"
              rel="noreferrer"
              class="shrink-0 font-medium text-gray-800 outline-none hover:text-indigo-600"
            >
              PR #{preview().spec.pullRequestNumber}
            </a>
            <span class="truncate text-gray-500">· {preview().spec.repository}</span>
          </span>
          <span class="block truncate text-xs text-gray-400">
            {preview().spec.headRevision.slice(0, 12)} · expires{" "}
            {formatDateTime(preview().spec.expiresAt, true)}
          </span>
        </span>
      </div>
      <div class="flex items-center gap-2 pl-6 sm:pl-0">
        <span class="text-xs font-medium text-gray-500">{preview().status.phase}</span>
        <StatusBadge status={serviceDisplayStatus(props.service)} />
        <button
          type="button"
          onClick={props.onOpen}
          class="rounded px-2 py-1 text-xs font-medium text-indigo-600 outline-none hover:bg-indigo-50"
        >
          Open service
        </button>
      </div>
    </div>
  );
}

export { PreviewsPage };
