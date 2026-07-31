import { useLocation, useNavigate } from "@tanstack/solid-router";
import { mergeDefinedProperties } from "@maestro/sdk";
import type { LogsApi } from "./api";
import { LogViewer } from "./LogViewer";

const INGRESS_ACCESS_LOG_QUERY = "@maestro.log_type:ingress_access OR @RequestMethod:*";

function HttpLogsPage(props: { api: LogsApi }) {
  const location = useLocation();
  const search = () => location().search as { query?: string; range?: string };
  const navigate = useNavigate();

  const setUrlSearch = (updates: { query?: string | undefined; range?: string | undefined }) =>
    navigate({
      to: "/http-logs",
      search: mergeDefinedProperties(search(), updates),
      replace: true
    });

  return (
    <div class="h-full min-h-0 flex flex-col gap-3">
      <div class="shrink-0">
        <h1 class="text-lg font-semibold text-gray-900">HTTP logs</h1>
      </div>
      <div class="min-h-0 flex-1">
        <LogViewer
          api={props.api}
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
          {...(search().range ? { range: search().range } : {})}
          onRangeChange={(value) => setUrlSearch({ range: value === "1h" ? undefined : value })}
        />
      </div>
    </div>
  );
}

export { HttpLogsPage };
