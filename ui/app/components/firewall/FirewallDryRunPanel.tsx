import { For, Show } from "solid-js";
import type { FirewallDryRun } from "../../lib/types";

function FirewallDryRunPanel(props: { result: FirewallDryRun | null }) {
  return (
    <Show when={props.result}>
      {(result) => (
        <div class="mt-5 rounded-lg border border-indigo-200 bg-indigo-50/40 p-3">
          <div class="flex flex-wrap items-baseline justify-between gap-2">
            <h3 class="text-xs font-semibold text-indigo-900">Dry-run output</h3>
            <span class="font-mono text-[10px] text-indigo-500" title={result().bundleDigest}>
              bundle {result().bundleDigest.slice(0, 12)}
            </span>
          </div>
          <div class="mt-2 space-y-2">
            <For each={result().rulesets}>
              {(ruleset) => (
                <details class="rounded-md border border-indigo-100 bg-white">
                  <summary class="cursor-pointer px-3 py-2 text-[11px] font-medium text-gray-700">
                    <span class="font-mono">{ruleset.nodeId}</span>
                    <span class="ml-2 text-gray-400">{ruleset.tableName}</span>
                  </summary>
                  <pre class="max-h-72 overflow-auto border-t border-indigo-100 p-3 text-[10px] leading-5 text-gray-700">
                    {ruleset.script}
                  </pre>
                </details>
              )}
            </For>
          </div>
        </div>
      )}
    </Show>
  );
}

export { FirewallDryRunPanel };
