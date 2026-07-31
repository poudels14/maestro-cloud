import { Show, type JSX } from "solid-js";
import { ArrowUpRight, GitPullRequest, Menu } from "lucide-solid";
import clsx from "clsx";
import { StatusDot, TabButton } from "@maestro/kit";
import type { LogsApi } from "@maestro/logs";
import type { ServicesApi } from "./api";
import { DeploymentsTab } from "./DeploymentsTab";
import { LogsTab } from "./LogsTab";
import { OverviewTab } from "./OverviewTab";
import { PullRequestsTab } from "./PullRequestsTab";
import { serviceDisplayStatus, servicePreviews } from "./serviceView";
import type { Service } from "./types";

type DetailTab = "overview" | "deployments" | "metrics" | "logs" | "pull-requests";

function ServiceDetailPanel(props: {
  api: ServicesApi;
  logsApi: LogsApi;
  service: Service;
  services: Service[];
  tab: DetailTab;
  previewUrl: (serviceId: string) => string | null;
  renderIngress: () => JSX.Element;
  renderMetrics: () => JSX.Element;
  navigateTab: (tab: DetailTab) => void;
  onOpenDrawer: () => void;
  onError: (title: string, cause: unknown) => void;
}) {
  const previews = () => servicePreviews(props.services, props.service.meta.id);
  const showPullRequests = () =>
    props.service.previewResource == null &&
    (props.service.spec.preview != null || previews().length > 0);
  const tab = () => (props.tab === "pull-requests" && !showPullRequests() ? "overview" : props.tab);
  const contentMaxWidth = () => (tab() === "logs" ? "max-w-6xl" : "max-w-4xl");

  return (
    <div class="flex-1 flex flex-col min-w-0 h-full">
      <div class="shrink-0 bg-white border-b border-gray-200">
        <div class="md:hidden h-12 px-3 flex items-center gap-2">
          <button
            type="button"
            onClick={props.onOpenDrawer}
            class="p-2 -ml-2 text-gray-500 hover:text-gray-700 rounded-md outline-none"
            aria-label="Open menu"
          >
            <Menu class="size-5" />
          </button>
          <span class="text-sm font-semibold text-gray-900 truncate">
            {props.service.spec.name}
          </span>
        </div>
        <div class="relative px-3 sm:px-6 pt-1.5 sm:pt-2.5 overflow-x-auto">
          <div class="absolute left-4 top-1/2 hidden -translate-y-1/2 items-center gap-2 lg:flex">
            <StatusDot status={serviceDisplayStatus(props.service)} />
            <span class="max-w-[13rem] truncate text-sm font-semibold text-gray-900">
              {props.service.spec.name}
            </span>
          </div>
          <div
            class={clsx(
              "mx-auto flex justify-start sm:justify-center gap-4 -mb-px whitespace-nowrap",
              contentMaxWidth()
            )}
          >
            <TabButton
              label="Overview"
              active={tab() === "overview"}
              onClick={() => props.navigateTab("overview")}
            />
            <TabButton
              label="Metrics"
              active={tab() === "metrics"}
              onClick={() => props.navigateTab("metrics")}
            />
            <TabButton
              label="Deployments"
              active={tab() === "deployments"}
              onClick={() => props.navigateTab("deployments")}
            />
            <TabButton
              label="Logs"
              active={tab() === "logs"}
              onClick={() => props.navigateTab("logs")}
            />
            <Show when={showPullRequests()}>
              <TabButton
                label="Pull requests"
                active={tab() === "pull-requests"}
                onClick={() => props.navigateTab("pull-requests")}
              />
            </Show>
          </div>
        </div>
      </div>
      <div
        class={clsx("flex-1 py-3 sm:py-4 bg-[#fafafa]", {
          "min-h-0 overflow-hidden": tab() === "logs",
          "overflow-y-auto": tab() !== "logs"
        })}
      >
        <div
          class={clsx("mx-auto px-3 sm:px-6", contentMaxWidth(), {
            "h-full min-h-0": tab() === "logs"
          })}
        >
          <Show when={props.service.previewResource}>
            {(resource) => (
              <PreviewOriginBanner
                service={props.service}
                services={props.services}
                resource={resource()}
                url={props.previewUrl(props.service.meta.id)}
              />
            )}
          </Show>
          <Show when={tab() === "overview"}>
            <OverviewTab api={props.api} service={props.service} ingress={props.renderIngress()} />
          </Show>
          <Show when={tab() === "deployments"}>
            <DeploymentsTab
              api={props.api}
              logsApi={props.logsApi}
              service={props.service}
              onError={props.onError}
            />
          </Show>
          <Show when={tab() === "pull-requests"}>
            <PullRequestsTab
              service={props.service}
              services={props.services}
              previewUrl={props.previewUrl}
            />
          </Show>
          <Show when={tab() === "metrics"}>{props.renderMetrics()}</Show>
          <Show when={tab() === "logs"}>
            <LogsTab api={props.api} logsApi={props.logsApi} service={props.service} />
          </Show>
        </div>
      </div>
    </div>
  );
}

function PreviewOriginBanner(props: {
  service: Service;
  services: Service[];
  resource: NonNullable<Service["previewResource"]>;
  url: string | null;
}) {
  const baseService = () =>
    props.services.find((candidate) => candidate.meta.id === props.resource.spec.baseServiceId);
  const pullRequestUrl = () =>
    `https://github.com/${props.resource.spec.repository}/pull/${props.resource.spec.pullRequestNumber}`;

  return (
    <div class="mb-4 flex flex-col gap-2.5 rounded-lg border border-brand-ring bg-brand-light px-4 py-3 sm:flex-row sm:items-center">
      <div class="flex min-w-0 flex-1 items-center gap-2.5">
        <GitPullRequest class="size-4 shrink-0 text-brand/60" />
        <span class="min-w-0 truncate text-xs text-brand-hover">
          Preview of{" "}
          <a
            href={`/services/${encodeURIComponent(props.resource.spec.baseServiceId)}/overview`}
            class="font-semibold outline-none hover:underline"
          >
            {baseService()?.spec.name ?? props.resource.spec.baseServiceId}
          </a>{" "}
          for{" "}
          <a
            href={pullRequestUrl()}
            target="_blank"
            rel="noreferrer"
            class="font-semibold outline-none hover:underline"
          >
            PR #{props.resource.spec.pullRequestNumber}
          </a>
          <span class="text-brand/60">
            {" · "}
            {props.resource.spec.title || props.resource.spec.repository}
          </span>
        </span>
      </div>
      <Show when={props.url}>
        {(url) => (
          <a
            href={url()}
            target="_blank"
            rel="noreferrer"
            class="inline-flex shrink-0 items-center gap-1 self-start rounded-md border border-brand-border bg-white px-2 py-1 text-xs font-medium text-brand outline-none hover:bg-brand-light sm:self-auto"
          >
            Open app
            <ArrowUpRight class="size-3" />
          </a>
        )}
      </Show>
    </div>
  );
}

export { ServiceDetailPanel };
export type { DetailTab };
