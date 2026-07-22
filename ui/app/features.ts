import { createFirewallApi, createFirewallFeature } from "@maestro/firewall";
import { createIngressApi, createIngressFeature } from "@maestro/ingress";
import { createLogsApi, createLogsFeature } from "@maestro/logs";
import { createMetricsApi, createMetricsFeature } from "@maestro/metrics";
import { createServicesApi, createServicesFeature } from "@maestro/services";
import { composeFeatureManifests } from "@maestro/sdk";
import { apiClient, apiRequestError } from "./lib/client";

const firewallApi = createFirewallApi(apiClient, apiRequestError);
const ingressApi = createIngressApi(apiClient, apiRequestError);
const logsApi = createLogsApi(apiClient, apiRequestError);
const metricsApi = createMetricsApi(apiClient, apiRequestError);
const servicesApi = createServicesApi(apiClient, apiRequestError);

const firewallFeature = createFirewallFeature(firewallApi);
const metricsFeature = createMetricsFeature(metricsApi);
const servicesFeature = createServicesFeature(servicesApi);
const ingressFeature = createIngressFeature(ingressApi, logsApi);
const logsFeature = createLogsFeature(logsApi);
const panelFeatureRegistry = composeFeatureManifests([
  metricsFeature,
  servicesFeature,
  ingressFeature,
  logsFeature,
  firewallFeature
]);

type PanelFeaturePath = (typeof panelFeatureRegistry.routes)[number]["path"];

export { ingressApi, logsApi, metricsApi, panelFeatureRegistry, servicesApi };
export type { PanelFeaturePath };
