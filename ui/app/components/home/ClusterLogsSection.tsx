import { createMemo } from "solid-js";
import { useLocation, useNavigate } from "@tanstack/solid-router";
import { clusterNodesQuery, servicesQuery } from "../../lib/queries";
import { useQuery } from "../../lib/useQuery";
import { LogViewer } from "../logs/LogViewer";
import { LogFilters, filterParam, parseFilterParam } from "../logs/LogFilters";
import { anyOfLogQuery } from "../logs/logQueryPills";

type ClusterLogsSearch = {
  node?: string;
  service?: string;
  query?: string;
  range?: string;
};

function ClusterLogsSection() {
  const nodes = useQuery(() => clusterNodesQuery());
  const services = useQuery(() => servicesQuery());
  const location = useLocation();
  const navigate = useNavigate();
  const search = () => location().search as ClusterLogsSearch;
  const sortedServices = createMemo(() =>
    [...(services.data ?? [])].sort((left, right) => left.name.localeCompare(right.name))
  );
  const selection = () => ({
    nodes: parseFilterParam(search().node),
    services: parseFilterParam(search().service)
  });
  const setUrlSearch = (updates: ClusterLogsSearch) =>
    navigate({
      to: "/cluster/logs",
      search: { ...search(), ...updates },
      replace: true
    });

  return (
    <div class="h-full min-h-0">
      <LogViewer
        scope={anyOfLogQuery("service", selection().services)}
        nodeIds={selection().nodes.map(Number)}
        showHost
        showHistogram
        fillHeight
        query={search().query ?? ""}
        onQueryChange={(value) => setUrlSearch({ query: value || undefined })}
        range={search().range}
        onRangeChange={(value) => setUrlSearch({ range: value === "1h" ? undefined : value })}
        filters={
          <LogFilters
            nodes={nodes.data ?? []}
            services={sortedServices()}
            selection={selection()}
            onChange={(next) =>
              setUrlSearch({ node: filterParam(next.nodes), service: filterParam(next.services) })
            }
          />
        }
      />
    </div>
  );
}

export { ClusterLogsSection };
