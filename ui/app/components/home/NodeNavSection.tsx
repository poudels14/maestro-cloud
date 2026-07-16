import { Show } from "solid-js";
import { useNavigate } from "@tanstack/solid-router";
import { useQuery } from "@tanstack/solid-query";
import { Activity, ArrowLeftRight, Info, LayoutGrid, Network, ScrollText } from "lucide-solid";
import { SidebarNavItem, SidebarSection } from "../service-detail/Sidebar";
import { clusterInfoQuery } from "../../lib/queries";
import { isPartOfCluster } from "../../lib/systemServices";

type HomeTab = "info" | "metrics" | "services" | "cluster" | "traffic" | "http-logs";

function NodeNavSection(props: { active?: HomeTab; onNavigate?: () => void }) {
  const navigate = useNavigate();
  const cluster = useQuery(() => clusterInfoQuery());

  const go = (to: "/" | "/metrics" | "/services" | "/cluster" | "/traffic" | "/http-logs") => {
    props.onNavigate?.();
    if (to === "/traffic" || to === "/http-logs") {
      navigate({
        to,
        search: (previous: { range?: string }) =>
          typeof previous.range === "string" && previous.range !== "1h"
            ? { range: previous.range }
            : {}
      });
    } else {
      navigate({ to });
    }
  };

  return (
    <SidebarSection title="Node">
      <SidebarNavItem
        label="Info"
        icon={Info}
        selected={props.active === "info"}
        onClick={() => go("/")}
      />
      <SidebarNavItem
        label="Metrics"
        icon={Activity}
        selected={props.active === "metrics"}
        onClick={() => go("/metrics")}
      />
      <SidebarNavItem
        label="Services"
        icon={LayoutGrid}
        selected={props.active === "services"}
        onClick={() => go("/services")}
      />
      <SidebarNavItem
        label="Traffic"
        icon={ArrowLeftRight}
        selected={props.active === "traffic"}
        onClick={() => go("/traffic")}
      />
      <SidebarNavItem
        label="HTTP logs"
        icon={ScrollText}
        selected={props.active === "http-logs"}
        onClick={() => go("/http-logs")}
      />
      <Show when={isPartOfCluster(cluster.data)}>
        <SidebarNavItem
          label="Cluster"
          icon={Network}
          selected={props.active === "cluster"}
          onClick={() => go("/cluster")}
        />
      </Show>
    </SidebarSection>
  );
}

export { NodeNavSection };
export type { HomeTab };
