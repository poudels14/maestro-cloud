import { For, Show } from "solid-js";
import { ExternalLink, GitPullRequest } from "lucide-solid";
import type { Service } from "../../lib/types";
import { servicePreviews } from "../../lib/previews";
import { StatusBadge, timeAgo } from "../../lib/ui";

function PreviewsTab(props: { service: Service; services: Service[] }) {
  const previews = () => servicePreviews(props.services, props.service.id);

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
                <a
                  href={`/services/${encodeURIComponent(preview.id)}/overview`}
                  class="flex min-w-0 flex-1 items-start gap-2.5 outline-none hover:text-indigo-600"
                >
                  <GitPullRequest class="mt-0.5 size-4 shrink-0 text-gray-400" />
                  <span class="min-w-0">
                    <span class="block truncate text-sm font-medium text-gray-800">
                      PR #{source().prNumber} · {source().title}
                    </span>
                    <span class="block truncate text-xs text-gray-400">
                      {source().headRef} · opened {timeAgo(source().createdAtMs)}
                    </span>
                  </span>
                </a>
                <div class="flex items-center gap-2 pl-6 sm:pl-0">
                  <Show when={source().closedAtMs}>
                    <span class="text-xs font-medium text-amber-600">closing</span>
                  </Show>
                  <StatusBadge status={preview.status} />
                  <Show when={previewUrl()}>
                    {(url) => (
                      <a
                        href={url()}
                        target="_blank"
                        rel="noreferrer"
                        class="rounded p-1 text-gray-400 outline-none hover:bg-gray-50 hover:text-indigo-600"
                        aria-label={`Open preview for PR ${source().prNumber}`}
                      >
                        <ExternalLink class="size-4" />
                      </a>
                    )}
                  </Show>
                </div>
              </div>
            );
          }}
        </For>
      </div>
    </Show>
  );
}

export { PreviewsTab };
