import { createSignal, Show } from "solid-js";
import { useLocation, useNavigate } from "@tanstack/solid-router";
import { IngressInfo, ingressRoutesQuery, routePublicUrl } from "@maestro/ingress";
import { MetricsTab } from "@maestro/metrics";
import { mergeDefinedProperties, useQuery } from "@maestro/sdk";
import {
  ServiceDetailPanel,
  ServiceSidebar,
  servicesQuery,
  type DetailTab,
  type ServiceDetailSearchUpdate,
  type Service
} from "@maestro/services";
import { showErrorToast } from "../AppToasts";
import { SessionControls } from "../SessionControls";
import { NodeNavSection } from "../home/NodeNavSection";
import { ingressApi, logsApi, metricsApi, servicesApi } from "../../features";

const VALID_TABS = new Set(["overview", "deployments", "metrics", "logs", "pull-requests"]);

type ServiceDetailSearch = {
  query?: string;
  range?: string;
  deployment?: string;
  tab?: "logs" | "build" | "details";
};

function validateServiceDetailSearch(search: Record<string, unknown>): ServiceDetailSearch {
  return {
    ...(typeof search.query === "string" && search.query ? { query: search.query } : {}),
    ...(typeof search.range === "string" && search.range ? { range: search.range } : {}),
    ...(typeof search.deployment === "string" && search.deployment
      ? { deployment: search.deployment }
      : {}),
    ...(search.tab === "logs" || search.tab === "build" || search.tab === "details"
      ? { tab: search.tab }
      : {})
  };
}

function ServiceDetailRoute(props: { serviceId: string; pullRequestId?: string; tab: string }) {
  const navigate = useNavigate();
  const location = useLocation();
  const services = useQuery(() => servicesQuery(servicesApi));
  const ingressRoutes = useQuery(() => ingressRoutesQuery(ingressApi));
  const [drawerOpen, setDrawerOpen] = createSignal(false);

  const previewUrl = (serviceId: string) => {
    const route = (ingressRoutes.data ?? []).find((candidate) => candidate.serviceId === serviceId);
    return route ? routePublicUrl(route) : null;
  };
  const tab = () => (VALID_TABS.has(props.tab) ? (props.tab as DetailTab) : "overview");
  const baseService = () =>
    services.data?.find((service) => service.meta.id === props.serviceId) ?? null;
  const selected = () => {
    if (props.pullRequestId === undefined) {
      return baseService();
    }
    return (
      services.data?.find((service) => {
        const preview = service.previewResource;
        return (
          preview?.spec.baseServiceId === props.serviceId &&
          String(preview.spec.pullRequestNumber) === props.pullRequestId
        );
      }) ?? null
    );
  };
  const navigateTab = (next: DetailTab) => {
    if (props.pullRequestId === undefined) {
      return navigate({
        to: "/services/$serviceId/$tab",
        params: { serviceId: props.serviceId, tab: next }
      });
    }
    return navigate({
      to: "/services/$serviceId/prs/$prId/$tab",
      params: { serviceId: props.serviceId, prId: props.pullRequestId, tab: next }
    });
  };
  const navigateService = (service: Service) => {
    setDrawerOpen(false);
    navigate({
      to: "/services/$serviceId/$tab",
      params: { serviceId: service.meta.id, tab: tab() }
    });
  };
  const navigateBaseService = () =>
    navigate({
      to: "/services/$serviceId/$tab",
      params: { serviceId: props.serviceId, tab: "pull-requests" }
    });
  const updateSearch = (updates: ServiceDetailSearchUpdate) => {
    const search = mergeDefinedProperties(location().search as ServiceDetailSearch, updates);
    if (props.pullRequestId === undefined) {
      return navigate({
        to: "/services/$serviceId/$tab",
        params: { serviceId: props.serviceId, tab: tab() },
        search,
        replace: true
      });
    }
    return navigate({
      to: "/services/$serviceId/prs/$prId/$tab",
      params: { serviceId: props.serviceId, prId: props.pullRequestId, tab: tab() },
      search,
      replace: true
    });
  };

  return (
    <div class="h-screen flex bg-[#fafafa]">
      <ServiceSidebar
        services={services.data ?? []}
        selected={selected()}
        onSelect={navigateService}
        onBack={() => navigate({ to: "/" })}
        onSelectControllerLogs={() =>
          navigate({ to: "/cluster/logs", search: { component: "controller" } })
        }
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
            <p class="text-sm text-gray-500">
              {props.pullRequestId === undefined
                ? "Service not found."
                : `Pull request #${props.pullRequestId} preview not found.`}
            </p>
            <button
              type="button"
              onClick={
                props.pullRequestId === undefined
                  ? () => navigate({ to: "/" })
                  : navigateBaseService
              }
              class="mt-3 text-sm text-brand hover:text-brand-hover"
            >
              {props.pullRequestId === undefined
                ? "Back to services"
                : `Back to ${baseService()?.spec.name ?? props.serviceId}`}
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
            onSearchChange={updateSearch}
            onNavigateBaseService={navigateBaseService}
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

export { ServiceDetailRoute, validateServiceDetailSearch };
export type { ServiceDetailSearch };
