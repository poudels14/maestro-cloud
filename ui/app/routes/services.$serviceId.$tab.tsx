import { createFileRoute, useNavigate } from "@tanstack/solid-router";
import { useQuery } from "@tanstack/solid-query";
import { createEffect, createSignal, Show } from "solid-js";
import { Menu } from "lucide-solid";
import clsx from "clsx";
import type { Service } from "../lib/types";
import { servicesQuery } from "../lib/queries";
import { TabButton } from "../lib/ui";
import { ServiceSidebar } from "../components/service-detail/Sidebar";
import { NodeNavSection } from "../components/home/NodeNavSection";
import { OverviewTab } from "../components/service-detail/OverviewTab";
import { DeploymentsTab } from "../components/service-detail/DeploymentsTab";
import { MetricsTab } from "../components/service-detail/MetricsTab";
import { LogsTab } from "../components/service-detail/LogsTab";
import { IngressTrafficTab } from "../components/ingress/TrafficTab";

const VALID_TABS = new Set(["overview", "deployments", "metrics", "traffic", "logs"]);
type DetailTab = "overview" | "deployments" | "metrics" | "traffic" | "logs";

export const Route = createFileRoute("/services/$serviceId/$tab")({
  validateSearch: (
    search: Record<string, unknown>
  ): {
    query?: string;
    range?: string;
    deployment?: string;
    tab?: "logs" | "build" | "details";
  } => ({
    ...(typeof search.query === "string" && search.query ? { query: search.query } : {}),
    ...(typeof search.range === "string" && search.range ? { range: search.range } : {}),
    ...(typeof search.deployment === "string" && search.deployment
      ? { deployment: search.deployment }
      : {}),
    ...(search.tab === "logs" || search.tab === "build" || search.tab === "details"
      ? { tab: search.tab }
      : {})
  }),
  component: ServiceDetailPage
});

function ServiceDetailPage() {
  const params = Route.useParams();
  const navigate = useNavigate();
  const services = useQuery(() => servicesQuery());
  const [drawerOpen, setDrawerOpen] = createSignal(false);

  const tab = () => {
    const raw = params().tab;
    return VALID_TABS.has(raw) ? (raw as DetailTab) : "overview";
  };

  const selected = () => services.data?.find((s) => s.id === params().serviceId);

  const navigateTab = (next: DetailTab) =>
    navigate({
      to: "/services/$serviceId/$tab",
      params: { serviceId: params().serviceId, tab: next }
    });

  const navigateService = (service: Service) => {
    setDrawerOpen(false);
    navigate({ to: "/services/$serviceId/$tab", params: { serviceId: service.id, tab: tab() } });
  };

  const loading = () => services.isLoading;

  return (
    <div class="h-screen flex bg-[#fafafa]">
      <ServiceSidebar
        services={services.data ?? []}
        selected={selected() ?? null}
        onSelect={navigateService}
        onBack={() => navigate({ to: "/" })}
        topSection={<NodeNavSection onNavigate={() => setDrawerOpen(false)} />}
        mobileOpen={drawerOpen()}
        onCloseMobile={() => setDrawerOpen(false)}
      />
      <Show when={loading()}>
        <div class="flex-1 flex items-center justify-center">
          <span class="text-sm text-gray-400">Loading…</span>
        </div>
      </Show>
      <Show when={!loading() && !selected()}>
        <div class="flex-1 flex items-center justify-center">
          <div class="text-center">
            <p class="text-sm text-gray-500">Service not found.</p>
            <button
              type="button"
              onClick={() => navigate({ to: "/" })}
              class="mt-3 text-sm text-indigo-600 hover:text-indigo-700"
            >
              Back to services
            </button>
          </div>
        </div>
      </Show>
      <Show when={!loading() && selected()}>
        {(service) => (
          <ServiceDetailPanel
            service={service()}
            tab={tab()}
            navigateTab={navigateTab}
            onOpenDrawer={() => setDrawerOpen(true)}
          />
        )}
      </Show>
    </div>
  );
}

function ServiceDetailPanel(props: {
  service: Service;
  tab: DetailTab;
  navigateTab: (t: DetailTab) => void;
  onOpenDrawer: () => void;
}) {
  createEffect(() => {
    if (props.tab === "traffic" && props.service.id !== "maestro-ingress") {
      props.navigateTab("overview");
    } else if (props.service.system && props.tab === "deployments") {
      props.navigateTab("logs");
    }
  });

  const contentMaxWidth = () =>
    props.tab === "logs" || props.tab === "traffic" ? "max-w-6xl" : "max-w-4xl";

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
          <span class="text-sm font-semibold text-gray-900 truncate">{props.service.name}</span>
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
            <Show when={props.service.id === "maestro-ingress"}>
              <TabButton
                label="Traffic"
                active={props.tab === "traffic"}
                onClick={() => props.navigateTab("traffic")}
              />
            </Show>
            <Show when={!props.service.system}>
              <TabButton
                label="Deployments"
                active={props.tab === "deployments"}
                onClick={() => props.navigateTab("deployments")}
              />
            </Show>
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
            <OverviewTab service={props.service} />
          </Show>
          <Show when={props.tab === "deployments"}>
            <DeploymentsTab
              serviceId={props.service.id}
              hasBuild={!!props.service.build}
              deployFrozen={!!props.service.deployFrozen}
            />
          </Show>
          <Show when={props.tab === "metrics"}>
            <MetricsTab service={props.service} />
          </Show>
          <Show when={props.tab === "traffic" && props.service.id === "maestro-ingress"}>
            <IngressTrafficTab />
          </Show>
          <Show when={props.tab === "logs"}>
            <LogsTab service={props.service} />
          </Show>
        </div>
      </div>
    </div>
  );
}
