import { createResource, Show } from "solid-js";
import type { Service } from "../../lib/types";
import { getDeployments } from "../../lib/api";
import { LogViewer } from "../logs/LogViewer";

function LogsTab(props: { service: Service }) {
  const isSystem = props.service.system === true;

  const [deployments] = createResource(
    () => (isSystem ? null : props.service.id),
    (id) => getDeployments(id)
  );
  const hasAnyDeployment = () => (deployments()?.length ?? 0) > 0;

  return (
    <Show
      when={isSystem || hasAnyDeployment()}
      fallback={
        <div class="bg-white rounded-lg border border-gray-200 p-8 text-center">
          <p class="text-sm text-gray-400">
            No deployments yet. Deploy this service to see logs.
          </p>
        </div>
      }
    >
      <LogViewer
        serviceId={props.service.id}
        deploymentId={null}
        isSystem={isSystem}
        hasBuild={!!props.service.build}
        phase="deploy"
      />
    </Show>
  );
}

export { LogsTab };
