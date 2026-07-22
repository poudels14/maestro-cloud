import { createSignal, Match, Show, Switch } from "solid-js";
import { Dynamic } from "solid-js/web";
import clsx from "clsx";
import { useQuery } from "../../lib/useQuery";
import { useNavigate } from "@tanstack/solid-router";
import { Menu } from "lucide-solid";
import type { Service } from "@maestro/services";
import { servicesQuery } from "@maestro/services";
import { clusterInfoQuery } from "../../lib/queries";
import { ServiceSidebar } from "../service-detail/Sidebar";
import { NodeNavSection } from "./NodeNavSection";
import type { HomePath } from "./NodeNavSection";
import { ClientOnly } from "../ClientOnly";
import { ServicesGrid } from "./ServicesGrid";
import { ClusterConfigSection } from "./ClusterConfigSection";
import { Webhooks } from "../Webhooks";
import { ClusterStatsSection } from "./ClusterStatsSection";
import { NodesSection } from "./NodesSection";
import { ClusterLogsSection } from "./ClusterLogsSection";
import { panelFeatureRegistry, servicesApi } from "../../features";

function HomeShell(props: { path: HomePath }) {
  const navigate = useNavigate();
  const services = useQuery(() => servicesQuery(servicesApi));
  const cluster = useQuery(() => clusterInfoQuery());
  const [drawerOpen, setDrawerOpen] = createSignal(false);
  const featureRoute = () => panelFeatureRegistry.routes.find((route) => route.path === props.path);
  const fullPage = () =>
    props.path === "/http-logs" ||
    props.path === "/cluster/logs" ||
    featureRoute()?.layout === "full";
  const widePage = () => fullPage() || featureRoute()?.layout === "wide";

  const navigateService = (service: Service) => {
    setDrawerOpen(false);
    navigate({
      to: "/services/$serviceId/$tab",
      params: { serviceId: service.meta.id, tab: "overview" }
    });
  };

  return (
    <div class="h-screen flex bg-[#fafafa]">
      <ServiceSidebar
        services={services.data ?? []}
        selected={null}
        onSelect={navigateService}
        onBack={() => {
          setDrawerOpen(false);
          navigate({ to: "/" });
        }}
        topSection={<NodeNavSection active={props.path} onNavigate={() => setDrawerOpen(false)} />}
        mobileOpen={drawerOpen()}
        onCloseMobile={() => setDrawerOpen(false)}
      />
      <div class="flex-1 flex flex-col min-w-0 h-full">
        <div class="md:hidden shrink-0 bg-white border-b border-gray-200 h-12 px-3 flex items-center gap-2">
          <button
            type="button"
            onClick={() => setDrawerOpen(true)}
            class="p-2 -ml-2 text-gray-500 hover:text-gray-700 rounded-md outline-none"
            aria-label="Open menu"
          >
            <Menu class="size-5" />
          </button>
          <Show when={cluster.data}>
            {(info) => (
              <span class="text-sm font-semibold text-gray-900 truncate">{info().clusterId}</span>
            )}
          </Show>
        </div>
        <div
          class={clsx("flex-1 py-5 sm:py-6", {
            "min-h-0 overflow-hidden": fullPage(),
            "overflow-y-auto": !fullPage()
          })}
        >
          <div
            class={clsx("mx-auto px-4 sm:px-6", {
              "max-w-6xl": widePage(),
              "max-w-4xl": !widePage(),
              "h-full min-h-0": fullPage()
            })}
          >
            <ClientOnly
              fallback={<div class="text-sm text-gray-400 py-20 text-center">Loading…</div>}
            >
              <Switch>
                <Match when={props.path === "/"}>
                  <div class="space-y-8">
                    <ClusterHero />
                    <ClusterStatsSection />
                    <ClusterConfigSection />
                    <Webhooks />
                  </div>
                </Match>
                <Match when={props.path === "/services"}>
                  <ServicesGrid />
                </Match>
                <Match when={props.path === "/cluster"}>
                  <NodesSection />
                </Match>
                <Match when={props.path === "/cluster/logs"}>
                  <ClusterLogsSection />
                </Match>
                <Match when={featureRoute()}>
                  {(route) => <Dynamic component={route().component} />}
                </Match>
              </Switch>
            </ClientOnly>
          </div>
        </div>
      </div>
    </div>
  );
}

function ClusterHero() {
  const cluster = useQuery(() => clusterInfoQuery());

  return (
    <Show when={cluster.data}>
      {(info) => (
        <div>
          <div class="flex flex-wrap items-center gap-2">
            <h1 class="text-lg font-semibold text-gray-900 tracking-tight truncate">
              {info().clusterId}
            </h1>
          </div>
          <div class="mt-1.5 flex flex-wrap items-center gap-x-4 gap-y-1 text-xs text-gray-500">
            <span>{info().nodeCount} nodes</span>
            <span>{info().controlPlaneNodeCount} control plane</span>
            <span>{info().workloadNodeCount} workload</span>
          </div>
        </div>
      )}
    </Show>
  );
}

export { HomeShell };
