import { createFileRoute } from "@tanstack/solid-router";
import {
  ServiceDetailRoute,
  validateServiceDetailSearch
} from "../components/services/ServiceDetailRoute";

export const Route = createFileRoute("/services/$serviceId/$tab")({
  validateSearch: validateServiceDetailSearch,
  component: ServiceDetailPage
});

function ServiceDetailPage() {
  const params = Route.useParams();
  return <ServiceDetailRoute serviceId={params().serviceId} tab={params().tab} />;
}
