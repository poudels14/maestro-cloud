import { createFileRoute, useNavigate } from "@tanstack/solid-router";
import { createSignal, Show } from "solid-js";
import { IngressInfo, ingressRoutesQuery, routePublicUrl } from "@maestro/ingress";
import { MetricsTab } from "@maestro/metrics";
import { useQuery } from "@maestro/sdk";
import {
  ServiceDetailPanel,
  ServiceSidebar,
  servicesQuery,
  type DetailTab,
  type Service
} from "@maestro/services";
import { NodeNavSection } from "../components/home/NodeNavSection";
import { SessionControls } from "../components/SessionControls";
import { showErrorToast } from "../components/AppToasts";
import { ingressApi, logsApi, metricsApi, servicesApi } from "../features";

const VALID_TABS = new Set(["overview", "deployments", "metrics", "logs", "pull-requests"]);

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
  const services = useQuery(() => servicesQuery(servicesApi));
  const ingressRoutes = useQuery(() => ingressRoutesQuery(ingressApi));
  const [drawerOpen, setDrawerOpen] = createSignal(false);

  const previewUrl = (serviceId: string) => {
    const route = (ingressRoutes.data ?? []).find(
      (candidate) => candidate.serviceId === serviceId
    );
    return route ? routePublicUrl(route) : null;
  };

  const tab = () => {
    const raw = params().tab;
    return VALID_TABS.has(raw) ? (raw as DetailTab) : "overview";
  };
  const selected = () => services.data?.find((service) => service.meta.id === params().serviceId);
  const navigateTab = (next: DetailTab) =>
    navigate({
      to: "/services/$serviceId/$tab",
      params: { serviceId: params().serviceId, tab: next }
    });
  const navigateService = (service: Service) => {
    setDrawerOpen(false);
    navigate({
      to: "/services/$serviceId/$tab",
      params: { serviceId: service.meta.id, tab: tab() }
    });
  };

  return (
    <div class="h-screen flex bg-[#fafafa]">
      <ServiceSidebar
        services={services.data ?? []}
        selected={selected() ?? null}
        onSelect={navigateService}
        onBack={() => navigate({ to: "/" })}
        topSection={<NodeNavSection onNavigate={() => setDrawerOpen(false)} />}
        footer={<SessionControls />}
        mobileOpen={drawerOpen()}
        onCloseMobile={() => setDrawerOpen(false)}
      />
      <Show when={services.isLoading}>
        <div class="flex-1 flex items-center justify-center">
          <span class="text-sm text-gray-400">Loading…</span>
        </div>
      </Show>
      <Show when={!services.isLoading && !selected()}>
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
      <Show when={!services.isLoading && selected()}>
        {(service) => (
          <ServiceDetailPanel
            api={servicesApi}
            logsApi={logsApi}
            service={service()}
            services={services.data ?? []}
            tab={tab()}
            previewUrl={previewUrl}
            navigateTab={navigateTab}
            onOpenDrawer={() => setDrawerOpen(true)}
            onError={showErrorToast}
            renderIngress={() => <IngressInfo api={ingressApi} serviceId={service().meta.id} />}
            renderMetrics={() => <MetricsTab api={metricsApi} serviceId={service().meta.id} />}
          />
        )}
      </Show>
    </div>
  );
}
