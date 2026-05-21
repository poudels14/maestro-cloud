import { Show } from "solid-js";
import { useQuery } from "@tanstack/solid-query";
import type { Service } from "../../lib/types";
import { deploymentsQuery } from "../../lib/queries";
import { LogViewer } from "../logs/LogViewer";

function LogsTab(props: { service: Service }) {
  const isSystem = () => props.service.system === true;

  const deployments = useQuery(() => ({
    ...deploymentsQuery(props.service.id),
    enabled: !isSystem()
  }));
  const hasAnyDeployment = () => (deployments.data?.length ?? 0) > 0;

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
      />
    </Show>
  );
}

export { LogsTab };
