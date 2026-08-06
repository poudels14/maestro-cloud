import { For, Show } from "solid-js";
import type { ApiSchemas } from "@maestro/api-client";
import { secretEntries, secretSourcePath } from "./secretView";

type SecretMountSpec = ApiSchemas["SecretMountSpec"];
type ResolvedSecrets = ApiSchemas["DeploymentStatus"]["resolvedSecrets"];

function SecretConfig(props: {
  secrets: SecretMountSpec | null | undefined;
  resolvedSecrets?: ResolvedSecrets;
}) {
  const entries = () => secretEntries(props.secrets, props.resolvedSecrets);
  const sourcePath = () => secretSourcePath(props.secrets);

  return (
    <Show when={props.secrets}>
      {(secrets) => (
        <div>
          <h4 class="text-xs font-medium text-gray-400 mb-2">Deploy secrets</h4>
          <div class="bg-white rounded-lg border border-gray-200 divide-y divide-gray-100">
            <Show when={sourcePath()}>
              {(path) => (
                <>
                  <SecretRow label="Source" value="AWS Secrets Manager" />
                  <SecretRow label="Path" value={path()} />
                </>
              )}
            </Show>
            <SecretRow label="Mount path" value={secrets().mountPath} />
            <For each={entries()}>
              {([key, maskedValue]) => <SecretRow label={key} value={maskedValue} />}
            </For>
          </div>
        </div>
      )}
    </Show>
  );
}

function SecretRow(props: { label: string; value: string }) {
  return (
    <div class="px-4 py-2.5 flex items-baseline justify-between gap-6">
      <span class="text-xs font-medium text-gray-700 shrink-0">{props.label}</span>
      <span class="text-xs tabular-nums text-gray-600 text-right truncate" title={props.value}>
        {props.value}
      </span>
    </div>
  );
}

export { SecretConfig };
