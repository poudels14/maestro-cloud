import { createFirewallApi, createFirewallFeature } from "@maestro/firewall";
import { createIngressApi, createIngressFeature } from "@maestro/ingress";
import { createLogsApi, createLogsFeature } from "@maestro/logs";
import { createMetricsApi, createMetricsFeature } from "@maestro/metrics";
import { composeFeatureManifests } from "@maestro/sdk";
import { apiClient, apiRequestError } from "./lib/client";

const firewallApi = createFirewallApi(apiClient, apiRequestError);
const ingressApi = createIngressApi(apiClient, apiRequestError);
const logsApi = createLogsApi(apiClient, apiRequestError);
const metricsApi = createMetricsApi(apiClient, apiRequestError);

const firewallFeature = createFirewallFeature(firewallApi);
const metricsFeature = createMetricsFeature(metricsApi);
const ingressFeature = createIngressFeature(ingressApi, logsApi);
const logsFeature = createLogsFeature(logsApi);
const panelFeatureRegistry = composeFeatureManifests([
  metricsFeature,
  ingressFeature,
  logsFeature,
  firewallFeature
]);

type PanelFeaturePath = (typeof panelFeatureRegistry.routes)[number]["path"];

export { ingressApi, logsApi, metricsApi, panelFeatureRegistry };
export type { PanelFeaturePath };
