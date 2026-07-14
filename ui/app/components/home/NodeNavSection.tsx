import { useNavigate } from "@tanstack/solid-router";
import { Activity, Info, LayoutGrid, Network } from "lucide-solid";
import { SidebarNavItem, SidebarSection } from "../service-detail/Sidebar";

type HomeTab = "info" | "metrics" | "services" | "nodes";

function NodeNavSection(props: { active?: HomeTab; onNavigate?: () => void }) {
  const navigate = useNavigate();

  const go = (to: "/" | "/metrics" | "/services" | "/nodes") => {
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
      <SidebarNavItem
        label="Nodes"
        icon={Network}
        selected={props.active === "nodes"}
        onClick={() => go("/nodes")}
      />
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
