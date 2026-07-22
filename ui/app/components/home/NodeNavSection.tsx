import { For, Show } from "solid-js";
import { useNavigate } from "@tanstack/solid-router";
import { useQuery } from "../../lib/useQuery";
import { ArrowLeftRight, Info, LayoutGrid, Network, ScrollText } from "lucide-solid";
import { SidebarNavItem, SidebarSection } from "../service-detail/Sidebar";
import { clusterInfoQuery } from "../../lib/queries";
import { isPartOfCluster } from "../../lib/systemServices";
import { panelFeatureRegistry } from "../../features";
import type { PanelFeaturePath } from "../../features";
import type { NavEntry } from "@maestro/sdk";

type CoreHomePath = "/" | "/services" | "/cluster" | "/cluster/logs" | "/traffic";
type HomePath = CoreHomePath | PanelFeaturePath;

const CORE_NODE_NAV = [
  { path: "/", label: "Info", icon: Info, section: "node", order: 10 },
  { path: "/services", label: "Services", icon: LayoutGrid, section: "node", order: 30 },
  { path: "/traffic", label: "Traffic", icon: ArrowLeftRight, section: "node", order: 40 }
] as const satisfies readonly NavEntry[];

function NodeNavSection(props: { active?: HomePath; onNavigate?: () => void }) {
  const navigate = useNavigate();
  const cluster = useQuery(() => clusterInfoQuery());
  const nodeNavigation = () =>
    [
      ...CORE_NODE_NAV,
      ...panelFeatureRegistry.nav.filter((entry) => entry.section === "node")
    ].sort((left, right) => left.order - right.order || left.label.localeCompare(right.label));

  const go = (to: HomePath) => {
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
        <For each={nodeNavigation()}>
          {(entry) => (
            <SidebarNavItem
              label={entry.label}
              icon={entry.icon}
              selected={props.active === entry.path}
              onClick={() => go(entry.path)}
            />
          )}
        </For>
      </SidebarSection>
      <Show when={isPartOfCluster(cluster.data)}>
        <SidebarSection title="Cluster">
          <SidebarNavItem
            label="Nodes"
            icon={Network}
            selected={props.active === "/cluster"}
            onClick={() => go("/cluster")}
          />
          <SidebarNavItem
            label="Logs"
            icon={ScrollText}
            selected={props.active === "/cluster/logs"}
            onClick={() => go("/cluster/logs")}
          />
        </SidebarSection>
      </Show>
    </>
  );
}

export { NodeNavSection };
export type { HomePath };
