import { Show } from "solid-js";
import { useQuery } from "../../lib/useQuery";
import { useLocation, useNavigate } from "@tanstack/solid-router";
import type { Service } from "../../lib/types";
import { deploymentsQuery } from "../../lib/queries";
import { LogViewer } from "../logs/LogViewer";

function LogsTab(props: { service: Service }) {
  const isSystem = () => props.service.system === true;
  const location = useLocation();
  const search = () => location().search as { query?: string; range?: string };
  const navigate = useNavigate();

  const deployments = useQuery(() => ({
    ...deploymentsQuery(props.service.id),
    enabled: !isSystem()
  }));
  const hasAnyDeployment = () => (deployments.data?.length ?? 0) > 0;

  const setUrlSearch = (updates: { query?: string; range?: string }) =>
    navigate({
      to: "/services/$serviceId/$tab",
      params: { serviceId: props.service.id, tab: "logs" },
      search: { ...search(), ...updates },
      replace: true
    });

  return (
    <Show
      when={isSystem() || hasAnyDeployment()}
      fallback={
        <div class="bg-white rounded-lg border border-gray-200 p-8 text-center">
          <p class="text-sm text-gray-400">No deployments yet. Deploy this service to see logs.</p>
        </div>
      }
    >
      <LogViewer
        serviceId={props.service.id}
        deploymentId={null}
        isSystem={isSystem()}
        hasBuild={!!props.service.build}
        phase="deploy"
        showHistogram
        fillHeight
        query={search().query ?? ""}
        onQueryChange={(value) => setUrlSearch({ query: value || undefined })}
        range={search().range}
        onRangeChange={(value) => setUrlSearch({ range: value === "1h" ? undefined : value })}
      />
    </Show>
  );
}

export { LogsTab };
