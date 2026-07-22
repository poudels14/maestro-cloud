import { Show } from "solid-js";
import { mergeDefinedProperties, useQuery } from "@maestro/sdk";
import { useLocation, useNavigate } from "@tanstack/solid-router";
import { LogViewer, type LogsApi } from "@maestro/logs";
import type { ServicesApi } from "./api";
import { deploymentsQuery } from "./queries";
import type { Service } from "./types";

function LogsTab(props: { api: ServicesApi; logsApi: LogsApi; service: Service }) {
  const location = useLocation();
  const search = () => location().search as { query?: string; range?: string };
  const navigate = useNavigate();

  const deployments = useQuery(() => ({
    ...deploymentsQuery(props.api, props.service.meta.id)
  }));
  const hasAnyDeployment = () => (deployments.data?.length ?? 0) > 0;

  const setUrlSearch = (updates: { query?: string | undefined; range?: string | undefined }) =>
    navigate({
      to: "/services/$serviceId/$tab",
      params: { serviceId: props.service.meta.id, tab: "logs" },
      search: mergeDefinedProperties(search(), updates),
      replace: true
    });

  return (
    <Show
      when={hasAnyDeployment()}
      fallback={
        <div class="bg-white rounded-lg border border-gray-200 p-8 text-center">
          <p class="text-sm text-gray-400">No deployments yet. Deploy this service to see logs.</p>
        </div>
      }
    >
      <LogViewer
        api={props.logsApi}
        serviceId={props.service.meta.id}
        deploymentId={null}
        isSystem={false}
        phase="deploy"
        showHistogram
        fillHeight
        query={search().query ?? ""}
        onQueryChange={(value) => setUrlSearch({ query: value || undefined })}
        {...(search().range ? { range: search().range } : {})}
        onRangeChange={(value) => setUrlSearch({ range: value === "1h" ? undefined : value })}
      />
    </Show>
  );
}

export { LogsTab };
