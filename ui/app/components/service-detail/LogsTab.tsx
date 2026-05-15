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
  const latestDeployment = () => deployments()?.[0] ?? null;

  return (
    <Show
      when={!isSystem && latestDeployment()}
      fallback={
        isSystem ? (
          <LogViewer
            serviceId={props.service.id}
            deploymentId={null}
            isSystem={true}
            hasBuild={false}
          />
        ) : (
          <div class="bg-white rounded-lg border border-gray-200 p-8 text-center">
            <p class="text-sm text-gray-400">
              No deployments yet. Deploy this service to see logs.
            </p>
          </div>
        )
      }
    >
      {(dep) => (
        <LogViewer
          serviceId={props.service.id}
          deploymentId={dep().id}
          isSystem={false}
          hasBuild={!!props.service.build}
          phase="deploy"
        />
      )}
    </Show>
  );
}

export { LogsTab };
