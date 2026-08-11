import { Show, type JSX } from "solid-js";
import { ArrowUpRight, ChevronRight, GitPullRequest, Menu } from "lucide-solid";
import clsx from "clsx";
import { StatusDot, TabButton } from "@maestro/kit";
import type { LogsApi } from "@maestro/logs";
import type { ServicesApi } from "./api";
import { DeploymentsTab } from "./DeploymentsTab";
import { LogsTab } from "./LogsTab";
import { OverviewTab } from "./OverviewTab";
import { PullRequestsTab } from "./PullRequestsTab";
import { serviceDisplayStatus, servicePreviews } from "./serviceView";
import type { Service, ServiceDetailSearchUpdate } from "./types";

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
  onSearchChange: (updates: ServiceDetailSearchUpdate) => void;
  onNavigateBaseService: () => void;
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
        <div class="lg:hidden h-12 px-3 flex items-center gap-2">
          <button
            type="button"
            onClick={props.onOpenDrawer}
            class="p-2 -ml-2 text-gray-500 hover:text-gray-700 rounded-md outline-none md:hidden"
            aria-label="Open menu"
          >
            <Menu class="size-5" />
          </button>
          <ServiceIdentity
            service={props.service}
            services={props.services}
            onNavigateBaseService={props.onNavigateBaseService}
          />
        </div>
        <div class="relative px-3 sm:px-6 pt-1.5 sm:pt-2.5 overflow-x-auto">
          <div class="absolute left-4 top-1/2 hidden max-w-[15rem] -translate-y-1/2 lg:flex">
            <ServiceIdentity
              service={props.service}
              services={props.services}
              onNavigateBaseService={props.onNavigateBaseService}
            />
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
            "h-full min-h-0 flex flex-col": tab() === "logs"
          })}
        >
          <Show when={props.service.previewResource}>
            {(resource) => (
              <PreviewOriginBanner
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
              onSearchChange={props.onSearchChange}
              onError={props.onError}
            />
          </Show>
          <Show when={tab() === "pull-requests"}>
            <PullRequestsTab
              api={props.api}
              service={props.service}
              services={props.services}
              previewUrl={props.previewUrl}
            />
          </Show>
          <Show when={tab() === "metrics"}>{props.renderMetrics()}</Show>
          <Show when={tab() === "logs"}>
            <div class="min-h-0 flex-1">
              <LogsTab
                api={props.api}
                logsApi={props.logsApi}
                service={props.service}
                onSearchChange={props.onSearchChange}
              />
            </div>
          </Show>
        </div>
      </div>
    </div>
  );
}

function ServiceIdentity(props: {
  service: Service;
  services: Service[];
  onNavigateBaseService: () => void;
}) {
  const preview = () => props.service.previewResource;
  const baseService = () =>
    props.services.find((service) => service.meta.id === preview()?.spec.baseServiceId);

  return (
    <div class="flex min-w-0 items-center gap-2 text-sm">
      <StatusDot status={serviceDisplayStatus(props.service)} />
      <Show
        when={preview()}
        fallback={
          <span class="truncate font-semibold text-gray-900">{props.service.spec.name}</span>
        }
      >
        <button
          type="button"
          onClick={props.onNavigateBaseService}
          class="min-w-0 truncate font-semibold text-gray-700 outline-none hover:text-brand hover:underline"
          title={baseService()?.spec.name ?? preview()!.spec.baseServiceId}
        >
          {baseService()?.spec.name ?? preview()!.spec.baseServiceId}
        </button>
        <ChevronRight class="size-3.5 shrink-0 text-gray-300" aria-hidden="true" />
        <span
          class="min-w-0 truncate font-semibold text-gray-900"
          title={`PR #${preview()!.spec.pullRequestNumber}`}
        >
          PR #{preview()!.spec.pullRequestNumber}
        </span>
      </Show>
    </div>
  );
}

function PreviewOriginBanner(props: {
  resource: NonNullable<Service["previewResource"]>;
  url: string | null;
}) {
  const pullRequestUrl = () =>
    `https://github.com/${props.resource.spec.repository}/pull/${props.resource.spec.pullRequestNumber}`;

  return (
    <div class="mb-4 flex flex-col gap-2.5 rounded-lg border border-brand-ring bg-brand-light px-4 py-3 sm:flex-row sm:items-center">
      <div class="flex min-w-0 flex-1 items-center gap-2.5">
        <GitPullRequest class="size-4 shrink-0 text-brand/60" />
        <div class="min-w-0 flex-1 truncate text-sm font-semibold text-brand-hover">
          {props.resource.spec.title || `PR #${props.resource.spec.pullRequestNumber}`}
        </div>
        <a
          href={pullRequestUrl()}
          target="_blank"
          rel="noreferrer"
          class="inline-flex shrink-0 items-center gap-1 rounded-md px-2 py-1 text-xs font-medium text-brand outline-none hover:bg-white/70"
        >
          View PR
          <ArrowUpRight class="size-3" />
        </a>
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
