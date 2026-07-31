import { For, Show } from "solid-js";
import { ChevronRight, FlaskConical } from "lucide-solid";
import type { FirewallDryRun } from "./api";

function FirewallDryRunPanel(props: { result: FirewallDryRun | null }) {
  return (
    <Show when={props.result}>
      {(result) => (
        <div class="mt-5 overflow-hidden rounded-md border border-neutral-200">
          <div class="flex flex-wrap items-center justify-between gap-2 border-b border-neutral-100 bg-neutral-50 px-3 py-2">
            <span class="inline-flex items-center gap-1.5 text-xs font-semibold text-neutral-700">
              <FlaskConical class="size-3.5 text-neutral-400" />
              Dry run — compiled rulesets
            </span>
            <span class="font-mono text-[10px] text-neutral-400" title={result().bundleDigest}>
              bundle {result().bundleDigest.slice(0, 12)}
            </span>
          </div>
          <div class="divide-y divide-neutral-100 bg-white">
            <For each={result().rulesets}>
              {(ruleset) => (
                <details class="group">
                  <summary class="flex cursor-pointer items-center gap-2 px-3 py-2 text-xs hover:bg-neutral-50 [&::-webkit-details-marker]:hidden">
                    <ChevronRight class="size-3.5 shrink-0 text-neutral-300 transition-transform duration-150 group-open:rotate-90" />
                    <span class="font-mono font-medium text-neutral-700">{ruleset.nodeId}</span>
                    <span class="truncate font-mono text-[10px] text-neutral-400">
                      {ruleset.tableName}
                    </span>
                  </summary>
                  <pre class="max-h-72 overflow-auto bg-neutral-900 px-4 py-3 font-mono text-[11px] leading-5 text-neutral-200 selection:bg-brand selection:text-white">
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
