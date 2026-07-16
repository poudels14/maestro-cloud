import { For, Show } from "solid-js";
import type { Service } from "../../lib/types";
import { ConfigSection } from "./overview/ConfigSection";
import { ReplicasEditor } from "./overview/ReplicasEditor";
import { IngressInfo } from "./overview/IngressInfo";
import { VolumesList } from "./overview/VolumesList";
import { FreezeToggle } from "./overview/FreezeToggle";

function OverviewTab(props: { service: Service }) {
  const sourceItems = () => {
    const build = props.service.build;
    if (!build) {
      return [{ label: "Image", value: props.service.image ?? "(not set)" }];
    }
    return [
      { label: "Git repository", value: build.repo ?? "uploaded context" },
      ...(build.branch ? [{ label: "Branch", value: build.branch }] : []),
      { label: "Dockerfile", value: build.dockerfile },
      ...(build.registry ? [{ label: "Registry", value: build.registry }] : []),
      ...(build.watch ? [{ label: "Watch", value: "enabled" }] : [])
    ];
  };

  const buildEnvItems = () =>
    Object.entries(props.service.build?.env?.items ?? {}).map(([key, value]) => ({
      label: key,
      value
    }));
  const buildEnvSource = () => props.service.build?.env?.source ?? null;
  const buildSecretKeys = () => Object.keys(props.service.build?.secrets?.items ?? {}).sort();
  const buildSecretSource = () => props.service.build?.secrets?.source ?? null;

  const deployItems = () => {
    const items: { label: string; value: string }[] = [];
    const command = props.service.deploy.command;
    if (command) {
      items.push({
        label: "Deploy command",
        value: `${command.command} ${command.args.join(" ")}`.trim()
      });
    }
    const secretsSource = props.service.deploy.secrets?.source;
    if (secretsSource) {
      items.push({ label: "Secrets source", value: secretsSource });
    }
    if (props.service.deploy.healthcheckPath) {
      items.push({ label: "Healthcheck path", value: props.service.deploy.healthcheckPath });
    }
    items.push({
      label: "Healthcheck interval",
      value: `${props.service.deploy.healthcheckInterval}s`
    });
    return items;
  };

  const envItems = () =>
    Object.entries(props.service.deploy.env?.items ?? {}).map(([key, value]) => ({
      label: key,
      value
    }));
  const envSource = () => props.service.deploy.env?.source ?? null;
  const secretKeys = () => Object.keys(props.service.deploy.secrets?.keys ?? {}).sort();
  const secretMountPath = () => props.service.deploy.secrets?.mountPath ?? null;

  return (
    <div class="space-y-6">
      <ConfigSection title="Deploy" items={[...sourceItems(), ...deployItems()]} />

      <Show when={buildEnvItems().length > 0 || buildEnvSource()}>
        <div>
          <h4 class="text-xs font-medium text-gray-400 mb-2">
            Build environment variables
            <Show when={buildEnvSource()}>
              <span class="ml-1.5 text-gray-300 normal-case font-mono">{buildEnvSource()}</span>
            </Show>
          </h4>
          <Show when={buildEnvItems().length > 0}>
            <ConfigSection items={buildEnvItems()} maskValues />
          </Show>
        </div>
      </Show>

      <Show when={buildSecretKeys().length > 0 || buildSecretSource()}>
        <div>
          <h4 class="text-xs font-medium text-gray-400 mb-2">
            Build Secrets
            <Show when={buildSecretSource()}>
              <span class="ml-1.5 text-gray-300 normal-case font-mono">{buildSecretSource()}</span>
            </Show>
          </h4>
          <Show when={buildSecretKeys().length > 0}>
            <SecretsList keys={buildSecretKeys()} />
          </Show>
        </div>
      </Show>

      <IngressInfo service={props.service} />

      <Show when={!props.service.system}>
        <div class="bg-white rounded-lg border border-gray-200 divide-y divide-gray-100">
          <ReplicasEditor service={props.service} />
          <FreezeToggle service={props.service} />
        </div>
      </Show>

      <Show when={envItems().length > 0 || envSource()}>
        <div>
          <h4 class="text-xs font-medium text-gray-400 mb-2">
            Deploy environment variables
            <Show when={envSource()}>
              <span class="ml-1.5 text-gray-300 normal-case font-mono">{envSource()}</span>
            </Show>
          </h4>
          <Show when={envItems().length > 0}>
            <ConfigSection items={envItems()} maskValues />
          </Show>
        </div>
      </Show>

      <Show when={secretKeys().length > 0}>
        <div>
          <h4 class="text-xs font-medium text-gray-400 mb-2">
            Deploy Secrets
            <Show when={secretMountPath()}>
              <span class="ml-1.5 text-gray-300 normal-case">(mounted at {secretMountPath()})</span>
            </Show>
          </h4>
          <SecretsList keys={secretKeys()} />
        </div>
      </Show>

      <VolumesList service={props.service} />
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

export { OverviewTab };
