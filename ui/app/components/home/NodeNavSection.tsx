import { useNavigate } from "@tanstack/solid-router";
import { Activity, ArrowLeftRight, Info, LayoutGrid, Network, ScrollText } from "lucide-solid";
import { SidebarNavItem, SidebarSection } from "../service-detail/Sidebar";

type HomeTab =
  | "info"
  | "metrics"
  | "services"
  | "cluster"
  | "cluster-logs"
  | "traffic"
  | "http-logs";

function NodeNavSection(props: { active?: HomeTab; onNavigate?: () => void }) {
  const navigate = useNavigate();

  const go = (
    to: "/" | "/metrics" | "/services" | "/cluster" | "/cluster/logs" | "/traffic" | "/http-logs"
  ) => {
    props.onNavigate?.();
    if (to === "/traffic" || to === "/http-logs" || to === "/cluster/logs") {
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
    <>
      <SidebarSection title="Cluster">
        <SidebarNavItem
          label="Overview"
          icon={Info}
          selected={props.active === "info"}
          onClick={() => go("/")}
        />
        <SidebarNavItem
          label="Services"
          icon={LayoutGrid}
          selected={props.active === "services"}
          onClick={() => go("/services")}
        />
        <SidebarNavItem
          label="Nodes"
          icon={Network}
          selected={props.active === "cluster"}
          onClick={() => go("/cluster")}
        />
        <SidebarNavItem
          label="Metrics"
          icon={Activity}
          selected={props.active === "metrics"}
          onClick={() => go("/metrics")}
        />
        <SidebarNavItem
          label="Logs"
          icon={ScrollText}
          selected={props.active === "cluster-logs"}
          onClick={() => go("/cluster/logs")}
        />
      </SidebarSection>
      <SidebarSection title="Ingress">
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
      </SidebarSection>
    </>
  );
}

export { NodeNavSection };
export type { HomeTab };
