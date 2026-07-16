import { useLocation, useNavigate } from "@tanstack/solid-router";
import { LogViewer } from "../logs/LogViewer";

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
      hasBuild={false}
      phase="deploy"
      showHistogram
      fillHeight
      query={search().query ?? ""}
      onQueryChange={(value) => setUrlSearch({ query: value || undefined })}
      range={search().range}
      onRangeChange={(value) => setUrlSearch({ range: value === "1h" ? undefined : value })}
    />
  );
}

export { HttpLogsSection };
