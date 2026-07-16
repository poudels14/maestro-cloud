import { Show } from "solid-js";
import { useNavigate } from "@tanstack/solid-router";
import { useQuery } from "@tanstack/solid-query";
import { Activity, Info, LayoutGrid, Network } from "lucide-solid";
import { SidebarNavItem, SidebarSection } from "../service-detail/Sidebar";
import { clusterInfoQuery } from "../../lib/queries";
import { isPartOfCluster } from "../../lib/systemServices";

type HomeTab = "info" | "metrics" | "services" | "cluster";

function NodeNavSection(props: { active?: HomeTab; onNavigate?: () => void }) {
  const navigate = useNavigate();
  const cluster = useQuery(() => clusterInfoQuery());

  const go = (to: "/" | "/metrics" | "/services" | "/cluster") => {
    props.onNavigate?.();
    navigate({ to });
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
      <Show when={isPartOfCluster(cluster.data)}>
        <SidebarNavItem
          label="Cluster"
          icon={Network}
          selected={props.active === "cluster"}
          onClick={() => go("/cluster")}
        />
      </Show>
      <SidebarNavItem
        label="Services"
        icon={LayoutGrid}
        selected={props.active === "services"}
        onClick={() => go("/services")}
      />
    </SidebarSection>
  );
}

export { NodeNavSection };
export type { HomeTab };
