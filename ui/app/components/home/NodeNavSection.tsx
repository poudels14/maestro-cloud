import { For, Show } from "solid-js";
import { useNavigate } from "@tanstack/solid-router";
import { useQuery } from "../../lib/useQuery";
import { Network, ScrollText } from "lucide-solid";
import { SidebarNavItem, SidebarSection } from "@maestro/kit";
import { clusterInfoQuery, isPartOfCluster } from "@maestro/cluster";
import { clusterApi, panelFeatureRegistry } from "../../features";
import type { PanelFeaturePath } from "../../features";

type CoreHomePath = "/cluster/logs";
type HomePath = CoreHomePath | PanelFeaturePath;

function NodeNavSection(props: { active?: HomePath; onNavigate?: () => void }) {
  const navigate = useNavigate();
  const cluster = useQuery(() => clusterInfoQuery(clusterApi));
  const nodeNavigation = () =>
    panelFeatureRegistry.nav
      .filter((entry) => entry.section === "node")
      .sort((left, right) => left.order - right.order || left.label.localeCompare(right.label));

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
