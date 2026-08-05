import { createFileRoute } from "@tanstack/solid-router";
import {
  ServiceDetailRoute,
  validateServiceDetailSearch
} from "../components/services/ServiceDetailRoute";

export const Route = createFileRoute("/services/$serviceId/prs/$prId/$tab")({
  validateSearch: validateServiceDetailSearch,
  component: PullRequestServiceDetailPage
});

function PullRequestServiceDetailPage() {
  const params = Route.useParams();
  return (
    <ServiceDetailRoute
      serviceId={params().serviceId}
      pullRequestId={params().prId}
      tab={params().tab}
    />
  );
}
