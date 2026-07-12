import { For, Show } from "solid-js";
import type { LogEntry } from "../../lib/types";
import { tsFormatter } from "../../lib/logFormat";

function LogDetailPanel(props: { entry: LogEntry }) {
  const baseAttrs = () => {
    const entry = props.entry;
    const attrs: { label: string; value: string }[] = [
      { label: "Timestamp", value: tsFormatter.format(new Date(entry.ts)) },
      { label: "Sequence", value: String(entry.seq) },
      { label: "Level", value: entry.level.toUpperCase() },
      { label: "Stream", value: entry.stream }
    ];
    if (entry.hostname) {
      attrs.push({ label: "Hostname", value: entry.hostname });
    }
    if (entry.source) {
      attrs.push({ label: "Source", value: entry.source });
    }
    entry.attrs?.forEach(([key, value]) => {
      attrs.push({ label: key, value });
    });
    return attrs;
  };

  const tags = () =>
    props.entry.tags?.filter(
      (tag) => !tag.startsWith("hostname:") && !tag.startsWith("maestro.internal.")
    ) ?? [];

  return (
    <div class="flex flex-col gap-2.5">
      <pre class="text-xs text-gray-800 font-mono whitespace-pre-wrap break-all">
        {props.entry.text}
      </pre>
      <div class="grid grid-cols-[auto_1fr] gap-x-6 gap-y-1.5">
        <For each={baseAttrs()}>
          {(attr) => (
            <>
              <span class="text-[11px] text-gray-400 font-medium whitespace-nowrap">
                {attr.label}
              </span>
              <span class="text-[11px] font-mono text-gray-600">{attr.value}</span>
            </>
          )}
        </For>
      </div>
      <Show when={tags().length > 0}>
        <div class="flex items-center gap-1.5 flex-wrap">
          <span class="text-[11px] text-gray-400 font-medium">Tags</span>
          <For each={tags()}>
            {(tag) => (
              <span class="text-[11px] font-mono text-gray-600 bg-gray-200/70 rounded px-1.5 py-0.5">
                {tag}
              </span>
            )}
          </For>
        </div>
      </Show>
    </div>
  );
}

export { LogDetailPanel };
