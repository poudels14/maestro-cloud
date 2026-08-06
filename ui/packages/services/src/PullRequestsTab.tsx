import { createSignal, For, Show } from "solid-js";
import { ArrowUpRight, GitPullRequest } from "lucide-solid";
import { formatDateTime, StatusBadge, TabButton } from "@maestro/kit";
import type { ApiSchemas } from "@maestro/api-client";
import type { Service } from "./types";
import { previewPullRequestState, serviceDisplayStatus, servicePreviews } from "./serviceView";

function PullRequestsTab(props: {
  service: Service;
  services: Service[];
  previewUrl: (serviceId: string) => string | null;
}) {
  const [state, setState] = createSignal<ApiSchemas["PullRequestState"]>("open");
  const allPreviews = () => servicePreviews(props.services, props.service.meta.id);
  const openPreviews = () =>
    allPreviews().filter((preview) => previewPullRequestState(preview) === "open");
  const closedPreviews = () =>
    allPreviews().filter((preview) => previewPullRequestState(preview) === "closed");
  const previews = () => (state() === "open" ? openPreviews() : closedPreviews());

  return (
    <div class="space-y-4">
      <div class="flex gap-4 border-b border-gray-200">
        <TabButton
          label="Open"
          count={openPreviews().length}
          active={state() === "open"}
          onClick={() => setState("open")}
        />
        <TabButton
          label="Closed"
          count={closedPreviews().length}
          active={state() === "closed"}
          onClick={() => setState("closed")}
        />
      </div>
      <Show
        when={previews().length > 0}
        fallback={
          <div class="rounded-xl border border-dashed border-gray-200 bg-white py-16 text-center">
            <GitPullRequest class="mx-auto mb-3 size-9 text-gray-300" />
            <p class="text-sm font-medium text-gray-500">No {state()} pull requests</p>
            <p class="mt-1 text-xs text-gray-400">
              {state() === "open"
                ? "A preview deployment is created for every open, non-draft pull request."
                : "Closed and merged pull requests with retained previews appear here."}
            </p>
          </div>
        }
      >
        <div class="divide-y divide-gray-100 overflow-hidden rounded-xl border border-gray-200 bg-white">
          <For each={previews()}>
            {(preview) => (
              <PullRequestRow preview={preview} url={props.previewUrl(preview.meta.id)} />
            )}
          </For>
        </div>
      </Show>
    </div>
  );
}

function PullRequestRow(props: { preview: Service; url: string | null }) {
  const resource = () => props.preview.previewResource!;
  const pullRequestUrl = () =>
    `https://github.com/${resource().spec.repository}/pull/${resource().spec.pullRequestNumber}`;

  return (
    <div class="flex flex-col gap-3 px-4 py-3.5 sm:flex-row sm:items-center sm:px-5">
      <a
        href={`/services/${encodeURIComponent(resource().spec.baseServiceId)}/prs/${resource().spec.pullRequestNumber}/overview`}
        class="group flex min-w-0 flex-1 items-start gap-3 outline-none"
      >
        <span class="mt-0.5 flex size-7 shrink-0 items-center justify-center rounded-md border border-gray-200 bg-gray-50">
          <GitPullRequest class="size-3.5 text-gray-500" />
        </span>
        <span class="min-w-0">
          <span class="flex min-w-0 items-baseline gap-1.5 text-sm">
            <span class="shrink-0 font-semibold text-gray-900 group-hover:text-brand">
              #{resource().spec.pullRequestNumber}
            </span>
            <span class="truncate font-medium text-gray-800 group-hover:text-brand">
              {resource().spec.title || resource().spec.repository}
            </span>
          </span>
          <span class="mt-0.5 block truncate text-xs text-gray-400">
            {resource().spec.repository}
            {" · "}
            <span class="font-mono">{resource().spec.headRevision.slice(0, 12)}</span>
            {" · expires "}
            {formatDateTime(resource().spec.expiresAt, true)}
          </span>
        </span>
      </a>
      <div class="flex items-center gap-2 pl-10 sm:pl-0">
        <Show
          when={
            previewPullRequestState(props.preview) === "open" ||
            props.preview.status.activeDeploymentId != null
          }
        >
          <StatusBadge status={serviceDisplayStatus(props.preview)} />
        </Show>
        <a
          href={pullRequestUrl()}
          target="_blank"
          rel="noreferrer"
          class="rounded-md px-2 py-1 text-xs font-medium text-gray-500 outline-none hover:bg-gray-100 hover:text-gray-700"
        >
          View PR
        </a>
        <Show when={props.url}>
          {(url) => (
            <a
              href={url()}
              target="_blank"
              rel="noreferrer"
              class="inline-flex items-center gap-1 rounded-md border border-brand-border px-2 py-1 text-xs font-medium text-brand outline-none hover:bg-brand-light"
            >
              Open app
              <ArrowUpRight class="size-3" />
            </a>
          )}
        </Show>
      </div>
    </div>
  );
}

export { PullRequestsTab };
