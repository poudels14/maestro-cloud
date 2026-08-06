import { StatusBadge } from "@maestro/kit";
import { useQuery } from "@maestro/sdk";
import type { ServicesApi } from "./api";
import { deploymentsQuery } from "./queries";
import { previewDeploymentStatus } from "./serviceView";
import type { Service } from "./types";

function PreviewStatusBadge(props: { api: ServicesApi; service: Service }) {
  const deployments = useQuery(() => deploymentsQuery(props.api, props.service.meta.id));
  const status = () => previewDeploymentStatus(props.service, deployments.data ?? []);

  return <StatusBadge status={status()} />;
}

export { PreviewStatusBadge };
