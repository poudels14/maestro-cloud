import { createFileRoute, useNavigate } from "@tanstack/solid-router";
import { createEffect, createResource, Show } from "solid-js";
import type { Service } from "../lib/types";
import { getServices } from "../lib/api";
import { TabButton } from "../lib/ui";
import { ServiceSidebar } from "../components/service-detail/Sidebar";
import { OverviewTab } from "../components/service-detail/OverviewTab";
import { DeploymentsTab } from "../components/service-detail/DeploymentsTab";
import { MetricsTab } from "../components/service-detail/MetricsTab";
import { LogsTab } from "../components/service-detail/LogsTab";

const VALID_TABS = new Set(["overview", "deployments", "metrics", "logs"]);
type DetailTab = "overview" | "deployments" | "metrics" | "logs";

export const Route = createFileRoute("/services/$serviceId/$tab")({
  component: ServiceDetailPage
});

function ServiceDetailPage() {
  const params = Route.useParams();
  const navigate = useNavigate();
  const [services, { refetch: refetchServices }] = createResource(
    () => (import.meta.env.SSR ? null : true),
    getServices
  );
  const tab = useTab();

  const selected = () => services()?.find((s) => s.id === params().serviceId);

  const navigateTab = (t: DetailTab) =>
    navigate({
      to: "/services/$serviceId/$tab",
      params: { serviceId: params().serviceId, tab: t }
    });

  const navigateService = (s: Service) =>
    navigate({ to: "/services/$serviceId/$tab", params: { serviceId: s.id, tab } });

  const serviceList = () => services() ?? [];
  const loading = () => services.loading || services() === undefined;

  return (
    <div class="h-screen flex bg-[#fafafa]">
      <ServiceSidebar
        services={serviceList()}
        selected={selected() ?? null}
        onSelect={navigateService}
        onBack={() => navigate({ to: "/" })}
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
            tab={tab}
            navigateTab={navigateTab}
            onServiceUpdate={refetchServices}
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
  onServiceUpdate: () => void;
}) {
  const s = props.service;

  createEffect(() => {
    if (s.system && props.tab === "deployments") {
      props.navigateTab("logs");
    }
  });

  return (
    <div class="flex-1 flex flex-col min-w-0 h-full">
      <div class="pt-5 pb-0 shrink-0 bg-white border-b border-gray-200">
        <div class="max-w-4xl mx-auto px-6 flex justify-center gap-4 -mb-px">
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
          <Show when={!s.system}>
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
      <div class="flex-1 overflow-y-auto py-5 bg-[#fafafa]">
        <div class="max-w-4xl mx-auto px-6">
          <Show when={props.tab === "overview"}>
            <OverviewTab service={s} onServiceUpdate={props.onServiceUpdate} />
          </Show>
          <Show when={props.tab === "deployments"}>
            <DeploymentsTab serviceId={s.id} hasBuild={!!s.build} deployFrozen={!!s.deployFrozen} />
          </Show>
          <Show when={props.tab === "metrics"}>
            <MetricsTab service={s} />
          </Show>
          <Show when={props.tab === "logs"}>
            <LogsTab service={s} />
          </Show>
        </div>
      </div>
    </div>
  );
}

function useTab(): DetailTab {
  const params = Route.useParams();
  const raw = () => params().tab;
  return VALID_TABS.has(raw()) ? (raw() as DetailTab) : "overview";
}
