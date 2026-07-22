import { For, Show, type JSX } from "solid-js";
import { GitPullRequest } from "lucide-solid";
import type { ServicesApi } from "./api";
import type { Service } from "./types";
import { serviceDisplayStatus, servicePreviews } from "./serviceView";
import { StatusBadge } from "@maestro/kit";
import { ConfigSection } from "./overview/ConfigSection";
import { ReplicasEditor } from "./overview/ReplicasEditor";
import { VolumesList } from "./overview/VolumesList";
import { FreezeToggle } from "./overview/FreezeToggle";

function OverviewTab(props: {
  api: ServicesApi;
  service: Service;
  services: Service[];
  ingress: JSX.Element;
}) {
  const artifact = () => props.service.spec.artifact;
  const sourceItems = () => {
    const value = artifact();
    if (value.type === "image") return [{ label: "Image", value: value.reference }];
    const source = value.source;
    return [
      ...(source.type === "git"
        ? [
            { label: "Git repository", value: source.repository },
            { label: "Git revision", value: source.revision }
          ]
        : [{ label: "Upload archive", value: source.archiveId }]),
      { label: "Dockerfile", value: value.dockerfile },
      ...(value.watch ? [{ label: "Watch", value: "enabled" }] : [])
    ];
  };

  const buildEnvItems = () => {
    const value = artifact();
    return value.type === "build"
      ? Object.entries(value.environment ?? {}).map(([label, entryValue]) => ({
          label,
          value: entryValue
        }))
      : [];
  };
  const buildSecretKeys = () => {
    const value = artifact();
    return value.type === "build" ? Object.keys(value.secrets ?? {}).sort() : [];
  };

  const deployItems = () => {
    const spec = props.service.spec;
    const items: { label: string; value: string }[] = [];
    if (spec.command) {
      items.push({
        label: "Command",
        value: `${spec.command.executable} ${(spec.command.arguments ?? []).join(" ")}`.trim()
      });
    }
    if ((spec.exposedPorts?.length ?? 0) > 0) {
      items.push({ label: "Exposed ports", value: spec.exposedPorts!.join(", ") });
    }
    if (spec.healthCheck) {
      const probe = spec.healthCheck.probe;
      items.push({
        label: "Health check",
        value: probe.protocol === "http" ? `HTTP ${probe.port}${probe.path}` : `TCP ${probe.port}`
      });
      items.push({
        label: "Health interval",
        value: `${spec.healthCheck.intervalSecs}s · ${spec.healthCheck.unhealthyThreshold} failures`
      });
    }
    if (spec.maxRestarts != null) {
      items.push({ label: "Maximum restarts", value: String(spec.maxRestarts) });
    }
    items.push({ label: "Interactive exec", value: spec.exec });
    items.push({ label: "Node API", value: spec.nodeApi });
    if (spec.placement.nodeId) {
      items.push({ label: "Pinned node", value: spec.placement.nodeId });
    }
    for (const [label, value] of Object.entries(spec.placement.labels ?? {})) {
      items.push({ label: `Placement: ${label}`, value });
    }
    return items;
  };

  const envItems = () =>
    Object.entries(props.service.spec.environment ?? {}).map(([label, value]) => ({
      label,
      value
    }));
  const secretKeys = () => Object.keys(props.service.spec.secrets?.items ?? {}).sort();
  const previews = () => servicePreviews(props.services, props.service.meta.id);
  const isPreview = () => props.service.previewResource != null;

  return (
    <div class="space-y-6">
      <ConfigSection title="Deploy" items={[...sourceItems(), ...deployItems()]} />

      <Show when={!isPreview() && previews().length > 0}>
        <PreviewsList previews={previews()} />
      </Show>

      <Show when={buildEnvItems().length > 0}>
        <ConfigSection title="Build environment variables" items={buildEnvItems()} />
      </Show>

      <Show when={buildSecretKeys().length > 0}>
        <div>
          <h4 class="text-xs font-medium text-gray-400 mb-2">Build secrets</h4>
          <SecretsList keys={buildSecretKeys()} />
        </div>
      </Show>

      {props.ingress}

      <Show when={!isPreview()}>
        <div class="bg-white rounded-lg border border-gray-200 divide-y divide-gray-100">
          <ReplicasEditor api={props.api} service={props.service} />
          <FreezeToggle api={props.api} service={props.service} />
        </div>
      </Show>

      <Show when={envItems().length > 0}>
        <ConfigSection title="Environment variables" items={envItems()} />
      </Show>

      <Show when={secretKeys().length > 0}>
        <div>
          <h4 class="text-xs font-medium text-gray-400 mb-2">
            Secrets
            <span class="ml-1.5 text-gray-300 normal-case">
              (mounted at {props.service.spec.secrets?.mountPath})
            </span>
          </h4>
          <SecretsList keys={secretKeys()} />
        </div>
      </Show>

      <VolumesList service={props.service} />
    </div>
  );
}

function PreviewsList(props: { previews: Service[] }) {
  return (
    <div>
      <h4 class="mb-2 text-xs font-medium text-gray-400">Previews</h4>
      <div class="divide-y divide-gray-100 rounded-lg border border-gray-200 bg-white">
        <For each={props.previews}>
          {(preview) => {
            const resource = () => preview.previewResource!;
            return (
              <div class="flex flex-col gap-3 px-4 py-3 sm:flex-row sm:items-center">
                <a
                  href={`/services/${encodeURIComponent(preview.meta.id)}/overview`}
                  class="flex min-w-0 flex-1 items-start gap-2.5 outline-none hover:text-indigo-600"
                >
                  <GitPullRequest class="mt-0.5 size-4 shrink-0 text-gray-400" />
                  <span class="min-w-0">
                    <span class="block truncate text-sm font-medium text-gray-800">
                      PR #{resource().spec.pullRequestNumber} · {resource().spec.repository}
                    </span>
                    <span class="block truncate text-xs text-gray-400">
                      {resource().spec.headRevision.slice(0, 12)} · expires{" "}
                      {formatDate(resource().spec.expiresAt)}
                    </span>
                  </span>
                </a>
                <div class="flex items-center gap-2 pl-6 sm:pl-0">
                  <span class="text-xs font-medium text-gray-500">{resource().status.phase}</span>
                  <StatusBadge status={serviceDisplayStatus(preview)} />
                </div>
              </div>
            );
          }}
        </For>
      </div>
    </div>
  );
}

function SecretsList(props: { keys: string[] }) {
  return (
    <div class="bg-white rounded-lg border border-gray-200 divide-y divide-gray-100">
      <For each={props.keys}>
        {(key) => (
          <div class="px-4 py-2.5 flex items-baseline justify-between gap-6">
            <span class="text-xs font-medium text-gray-700 shrink-0">{key}</span>
            <span class="text-xs text-gray-400">••••••••</span>
          </div>
        )}
      </For>
    </div>
  );
}

function formatDate(timestamp: number): string {
  return new Date(timestamp).toLocaleDateString();
}

export { OverviewTab };
