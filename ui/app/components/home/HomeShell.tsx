import { createSignal, Match, Show, Switch } from "solid-js";
import clsx from "clsx";
import { useQuery } from "@tanstack/solid-query";
import { useNavigate } from "@tanstack/solid-router";
import { Menu } from "lucide-solid";
import type { Service } from "../../lib/types";
import { clusterInfoQuery, servicesQuery } from "../../lib/queries";
import { ServiceSidebar } from "../service-detail/Sidebar";
import { NodeNavSection } from "./NodeNavSection";
import type { HomeTab } from "./NodeNavSection";
import { ClientOnly } from "../ClientOnly";
import { NodeMetricsSection } from "./NodeMetricsSection";
import { DisksSection } from "./DisksSection";
import { ServicesGrid } from "./ServicesGrid";
import { ClusterConfigSection } from "./ClusterConfigSection";
import { SlackWebhooks } from "../SlackWebhooks";
import { ClusterStatsSection } from "./ClusterStatsSection";
import { NodesSection } from "./NodesSection";
import { HttpLogsSection } from "./HttpLogsSection";
import { ClusterLogsSection } from "./ClusterLogsSection";
import { IngressTrafficTab } from "../ingress/TrafficTab";

function HomeShell(props: { tab: HomeTab }) {
  const navigate = useNavigate();
  const services = useQuery(() => servicesQuery());
  const cluster = useQuery(() => clusterInfoQuery());
  const [drawerOpen, setDrawerOpen] = createSignal(false);

  const navigateService = (service: Service) => {
    setDrawerOpen(false);
    navigate({
      to: "/services/$serviceId/$tab",
      params: { serviceId: service.id, tab: "overview" }
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
        topSection={<NodeNavSection active={props.tab} onNavigate={() => setDrawerOpen(false)} />}
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
              <span class="text-sm font-semibold text-gray-900 truncate">{info().clusterName}</span>
            )}
          </Show>
        </div>
        <div
          class={clsx("flex-1 py-5 sm:py-6", {
            "min-h-0 overflow-hidden": props.tab === "http-logs" || props.tab === "cluster-logs",
            "overflow-y-auto": props.tab !== "http-logs" && props.tab !== "cluster-logs"
          })}
        >
          <div
            class={clsx("mx-auto px-4 sm:px-6", {
              "max-w-6xl":
                props.tab === "traffic" ||
                props.tab === "http-logs" ||
                props.tab === "cluster-logs",
              "max-w-4xl":
                props.tab !== "traffic" &&
                props.tab !== "http-logs" &&
                props.tab !== "cluster-logs",
              "h-full min-h-0": props.tab === "http-logs" || props.tab === "cluster-logs"
            })}
          >
            <ClientOnly
              fallback={<div class="text-sm text-gray-400 py-20 text-center">Loading…</div>}
            >
              <Switch>
                <Match when={props.tab === "info"}>
                  <div class="space-y-8">
                    <ClusterHero />
                    <ClusterStatsSection />
                    <ClusterConfigSection />
                    <SlackWebhooks />
                  </div>
                </Match>
                <Match when={props.tab === "metrics"}>
                  <div class="space-y-8">
                    <DisksSection />
                    <NodeMetricsSection />
                  </div>
                </Match>
                <Match when={props.tab === "traffic"}>
                  <IngressTrafficTab />
                </Match>
                <Match when={props.tab === "http-logs"}>
                  <HttpLogsSection />
                </Match>
                <Match when={props.tab === "services"}>
                  <ServicesGrid />
                </Match>
                <Match when={props.tab === "cluster"}>
                  <NodesSection />
                </Match>
                <Match when={props.tab === "cluster-logs"}>
                  <ClusterLogsSection />
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
              {info().clusterName}
            </h1>
            <Show when={info().version}>
              {(version) => (
                <span
                  class="rounded-md border border-gray-200 bg-white px-1.5 py-0.5 text-[11px] font-mono text-gray-500"
                  title="Maestro version"
                >
                  v{version()}
                </span>
              )}
            </Show>
          </div>
          <div class="mt-1.5 flex flex-wrap items-center gap-x-4 gap-y-1 text-xs">
            <a
              href={`http://${info().canonicalDomain}`}
              title="Canonical domain"
              class="text-gray-500 hover:text-gray-700 no-underline truncate"
            >
              {info().canonicalDomain}
            </a>
            <a
              href={`http://${info().aliasDomain}`}
              title="Alias domain"
              class="text-gray-400 hover:text-gray-600 no-underline truncate"
            >
              {info().aliasDomain}
            </a>
          </div>
        </div>
      )}
    </Show>
  );
}

export { HomeShell };
