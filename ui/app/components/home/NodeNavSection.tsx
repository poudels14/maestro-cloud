import { Show } from "solid-js";
import { useNavigate } from "@tanstack/solid-router";
import { useQuery } from "../../lib/useQuery";
import {
  Activity,
  ArrowLeftRight,
  Info,
  LayoutGrid,
  Network,
  ScrollText,
  Shield
} from "lucide-solid";
import { SidebarNavItem, SidebarSection } from "../service-detail/Sidebar";
import { clusterInfoQuery } from "../../lib/queries";
import { isPartOfCluster } from "../../lib/systemServices";

type HomeTab =
  | "info"
  | "metrics"
  | "services"
  | "cluster"
  | "cluster-logs"
  | "traffic"
  | "http-logs"
  | "firewall";

function NodeNavSection(props: { active?: HomeTab; onNavigate?: () => void }) {
  const navigate = useNavigate();
  const cluster = useQuery(() => clusterInfoQuery());

  const go = (
    to:
      | "/"
      | "/metrics"
      | "/services"
      | "/cluster"
      | "/cluster/logs"
      | "/traffic"
      | "/http-logs"
      | "/firewall"
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
        <SidebarNavItem
          label="Firewall"
          icon={Shield}
          selected={props.active === "firewall"}
          onClick={() => go("/firewall")}
        />
      </SidebarSection>
      <Show when={isPartOfCluster(cluster.data)}>
        <SidebarSection title="Cluster">
          <SidebarNavItem
            label="Nodes"
            icon={Network}
            selected={props.active === "cluster"}
            onClick={() => go("/cluster")}
          />
          <SidebarNavItem
            label="Logs"
            icon={ScrollText}
            selected={props.active === "cluster-logs"}
            onClick={() => go("/cluster/logs")}
          />
        </SidebarSection>
      </Show>
    </>
  );
}

export { NodeNavSection };
export type { HomeTab };
