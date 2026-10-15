import { createMemo } from "solid-js";
import { useLocation, useNavigate } from "@tanstack/solid-router";
import { clusterNodesQuery, servicesQuery } from "../../lib/queries";
import { useQuery } from "../../lib/useQuery";
import { LogViewer } from "../logs/LogViewer";
import { LogFilters, filterParam, parseFilterParam } from "../logs/LogFilters";
import { anyOfLogQuery, combineLogQueries } from "../logs/logQueryPills";

const INGRESS_ACCESS_LOG_QUERY = "@maestro.log_type:ingress_access";
const ROUTER_FIELD = "@RouterName";

type HttpLogsSearch = {
  node?: string;
  service?: string;
  query?: string;
  range?: string;
};

function HttpLogsSection() {
  const nodes = useQuery(() => clusterNodesQuery());
  const services = useQuery(() => servicesQuery());
  const location = useLocation();
  const search = () => location().search as HttpLogsSearch;
  const navigate = useNavigate();
  const ingressServices = createMemo(() =>
    [...(services.data ?? [])]
      .filter((service) => service.ingress)
      .sort((left, right) => left.name.localeCompare(right.name))
  );
  const selection = () => ({
    nodes: parseFilterParam(search().node),
    services: parseFilterParam(search().service)
  });
  const setUrlSearch = (updates: HttpLogsSearch) =>
    navigate({ to: "/http-logs", search: { ...search(), ...updates }, replace: true });

  return (
    <LogViewer
      scope="replica:maestro-traefik"
      nodeIds={selection().nodes.map(Number)}
      showHistogram
      histogramGroupBy="status"
      fillHeight
      query={search().query ?? ""}
      requiredQuery={combineLogQueries(
        INGRESS_ACCESS_LOG_QUERY,
        anyOfLogQuery(
          ROUTER_FIELD,
          selection().services.map((service) => `svc-${service}@file`)
        )
      )}
      onQueryChange={(value) => setUrlSearch({ query: value || undefined })}
      range={search().range}
      onRangeChange={(value) => setUrlSearch({ range: value === "1h" ? undefined : value })}
      filters={
        <LogFilters
          nodes={nodes.data ?? []}
          services={ingressServices()}
          selection={selection()}
          onChange={(next) =>
            setUrlSearch({ node: filterParam(next.nodes), service: filterParam(next.services) })
          }
        />
      }
    />
  );
}

export { HttpLogsSection };
