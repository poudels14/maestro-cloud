import { useLocation, useNavigate } from "@tanstack/solid-router";
import { LogViewer } from "../logs/LogViewer";

const INGRESS_ACCESS_LOG_QUERY = "@maestro.log_type:ingress_access OR @RequestMethod:*";

function HttpLogsSection() {
  const location = useLocation();
  const search = () => location().search as { query?: string; range?: string };
  const navigate = useNavigate();

  const setUrlSearch = (updates: { query?: string; range?: string }) =>
    navigate({ to: "/http-logs", search: { ...search(), ...updates }, replace: true });

  return (
    <LogViewer
      serviceId="maestro-ingress"
      deploymentId={null}
      isSystem
      phase="deploy"
      showHistogram
      histogramGroupBy="status"
      fillHeight
      query={search().query ?? ""}
      requiredQuery={INGRESS_ACCESS_LOG_QUERY}
      onQueryChange={(value) => setUrlSearch({ query: value || undefined })}
      range={search().range}
      onRangeChange={(value) => setUrlSearch({ range: value === "1h" ? undefined : value })}
    />
  );
}

export { HttpLogsSection };
