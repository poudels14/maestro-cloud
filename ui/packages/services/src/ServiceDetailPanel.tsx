import { Show, type JSX } from "solid-js";
import { Menu } from "lucide-solid";
import clsx from "clsx";
import { TabButton } from "@maestro/kit";
import type { LogsApi } from "@maestro/logs";
import type { ServicesApi } from "./api";
import { DeploymentsTab } from "./DeploymentsTab";
import { LogsTab } from "./LogsTab";
import { OverviewTab } from "./OverviewTab";
import type { Service } from "./types";

type DetailTab = "overview" | "deployments" | "metrics" | "logs";

function ServiceDetailPanel(props: {
  api: ServicesApi;
  logsApi: LogsApi;
  service: Service;
  services: Service[];
  tab: DetailTab;
  renderIngress: () => JSX.Element;
  renderMetrics: () => JSX.Element;
  navigateTab: (tab: DetailTab) => void;
  onOpenDrawer: () => void;
  onError: (title: string, cause: unknown) => void;
}) {
  const contentMaxWidth = () => (props.tab === "logs" ? "max-w-6xl" : "max-w-4xl");

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
        <div class="px-3 sm:px-6 pt-1.5 sm:pt-2.5 overflow-x-auto">
          <div
            class={clsx(
              "mx-auto flex justify-start sm:justify-center gap-4 -mb-px whitespace-nowrap",
              contentMaxWidth()
            )}
          >
            <TabButton
              label="Overview"
              active={props.tab === "overview"}
              onClick={() => props.navigateTab("overview")}
            />
            <TabButton
              label="Metrics"
              active={props.tab === "metrics"}
              onClick={() => props.navigateTab("metrics")}
            />
            <TabButton
              label="Deployments"
              active={props.tab === "deployments"}
              onClick={() => props.navigateTab("deployments")}
            />
            <TabButton
              label="Logs"
              active={props.tab === "logs"}
              onClick={() => props.navigateTab("logs")}
            />
          </div>
        </div>
      </div>
      <div
        class={clsx("flex-1 py-3 sm:py-4 bg-[#fafafa]", {
          "min-h-0 overflow-hidden": props.tab === "logs",
          "overflow-y-auto": props.tab !== "logs"
        })}
      >
        <div
          class={clsx("mx-auto px-3 sm:px-6", contentMaxWidth(), {
            "h-full min-h-0": props.tab === "logs"
          })}
        >
          <Show when={props.tab === "overview"}>
            <OverviewTab
              api={props.api}
              service={props.service}
              services={props.services}
              ingress={props.renderIngress()}
            />
          </Show>
          <Show when={props.tab === "deployments"}>
            <DeploymentsTab
              api={props.api}
              logsApi={props.logsApi}
              service={props.service}
              onError={props.onError}
            />
          </Show>
          <Show when={props.tab === "metrics"}>{props.renderMetrics()}</Show>
          <Show when={props.tab === "logs"}>
            <LogsTab api={props.api} logsApi={props.logsApi} service={props.service} />
          </Show>
        </div>
      </div>
    </div>
  );
}

export { ServiceDetailPanel };
export type { DetailTab };
