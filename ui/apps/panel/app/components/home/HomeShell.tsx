import { createSignal, Show } from "solid-js";
import { Dynamic } from "solid-js/web";
import clsx from "clsx";
import { useQuery } from "../../lib/useQuery";
import { useNavigate } from "@tanstack/solid-router";
import { Menu } from "lucide-solid";
import { ServiceSidebar, servicesQuery, type Service } from "@maestro/services";
import { clusterConfigQuery, clusterInfoQuery } from "@maestro/cluster";
import { NodeNavSection } from "./NodeNavSection";
import type { HomePath } from "./NodeNavSection";
import { ClientOnly } from "../ClientOnly";
import { SessionControls } from "../SessionControls";
import { clusterApi, panelFeatureRegistry, servicesApi } from "../../features";

function HomeShell(props: { path: HomePath }) {
  const navigate = useNavigate();
  const services = useQuery(() => servicesQuery(servicesApi));
  const cluster = useQuery(() => clusterInfoQuery(clusterApi));
  const config = useQuery(() => clusterConfigQuery(clusterApi));
  const [drawerOpen, setDrawerOpen] = createSignal(false);
  const featureRoute = () => panelFeatureRegistry.routes.find((route) => route.path === props.path);
  const featureLayout = () => {
    const route = featureRoute();
    return route && "layout" in route ? route.layout : "standard";
  };
  const fullPage = () => featureLayout() === "full";
  const widePage = () => fullPage() || featureLayout() === "wide";

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
        footer={<SessionControls />}
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
              <span class="text-sm font-semibold text-gray-900 truncate">
                {config.data?.name ?? info().clusterId}
              </span>
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
              <Show when={featureRoute()}>
                {(route) => <Dynamic component={route().component} />}
              </Show>
            </ClientOnly>
          </div>
        </div>
      </div>
    </div>
  );
}

export { HomeShell };
