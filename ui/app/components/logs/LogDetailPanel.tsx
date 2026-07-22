import { For, Show } from "solid-js";
import clsx from "clsx";
import type { LogEntry } from "../../lib/logView";
import { httpFields, httpMethodColor, httpStatusPill, type HttpFields } from "../../lib/logFormat";

function AccessLogSummary(props: { fields: HttpFields }) {
  const metadata = () => {
    const fields = props.fields;
    const values: { label: string; value: string }[] = [];
    if (fields.clientIp) values.push({ label: "Client IP", value: fields.clientIp });
    if (fields.router) values.push({ label: "Router", value: fields.router });
    if (fields.service) values.push({ label: "Service", value: fields.service });
    if (fields.entryPoint) values.push({ label: "Entry point", value: fields.entryPoint });
    return values;
  };

  return (
    <div class="rounded-md border border-gray-200 bg-white p-3 flex flex-col gap-2.5">
      <div class="flex items-baseline gap-2 min-w-0 font-mono text-xs">
        <Show when={props.fields.method}>
          <span class={clsx("font-semibold shrink-0", httpMethodColor(props.fields.method!))}>
            {props.fields.method}
          </span>
        </Show>
        <Show when={props.fields.status}>
          <span
            class={clsx(
              "inline-block rounded px-1 tabular-nums font-medium shrink-0",
              httpStatusPill(props.fields.status!)
            )}
          >
            {props.fields.status}
          </span>
        </Show>
        <span class="truncate text-gray-700" title={props.fields.path}>
          <Show when={props.fields.requestHost}>
            <span class="text-gray-400">
              {props.fields.scheme ? `${props.fields.scheme}://` : ""}
              {props.fields.requestHost}
            </span>
          </Show>
          {props.fields.path}
        </span>
      </div>
      <Show when={metadata().length > 0}>
        <div class="grid grid-cols-[repeat(auto-fit,minmax(140px,1fr))] gap-x-5 gap-y-1.5">
          <For each={metadata()}>
            {(item) => (
              <span class="min-w-0">
                <span class="block text-[11px] font-medium text-gray-400">{item.label}</span>
                <span class="block text-[11px] font-mono text-gray-600 truncate" title={item.value}>
                  {item.value}
                </span>
              </span>
            )}
          </For>
        </div>
      </Show>
    </div>
  );
}

function prettyJson(value: string): string {
  try {
    return JSON.stringify(JSON.parse(value), null, 2);
  } catch {
    return value;
  }
}

function LogDetailPanel(props: { entry: LogEntry }) {
  const http = () => httpFields(props.entry.attrs);
  const baseAttrs = () => {
    const entry = props.entry;
    const attrs: { label: string; value: string }[] = [];
    if (entry.hostname) {
      attrs.push({ label: "Hostname", value: entry.hostname });
    }
    entry.attrs?.forEach(([key, value]) => {
      attrs.push({ label: key, value });
    });
    return attrs;
  };

  const tags = () =>
    props.entry.tags?.filter(
      (tag) =>
        !tag.startsWith("hostname:") &&
        !tag.startsWith("maestro.internal.") &&
        !tag.startsWith("service:") &&
        !tag.startsWith("deployment_id:") &&
        !tag.startsWith("cluster:")
    ) ?? [];

  return (
    <div class="flex flex-col gap-2.5" onClick={(event) => event.stopPropagation()}>
      <Show
        when={http().isTraefikAccessLog}
        fallback={
          <pre class="text-xs text-gray-800 font-mono whitespace-pre-wrap break-all">
            {props.entry.text}
          </pre>
        }
      >
        <AccessLogSummary fields={http()} />
        <details class="group rounded border border-gray-200 bg-white">
          <summary class="cursor-pointer select-none px-2.5 py-1.5 text-[11px] font-medium text-gray-500 hover:text-gray-700">
            Raw access log
          </summary>
          <pre class="max-h-72 overflow-auto border-t border-gray-100 px-2.5 py-2 text-[11px] text-gray-700 font-mono whitespace-pre-wrap break-all">
            {prettyJson(props.entry.text)}
          </pre>
        </details>
      </Show>
      <Show when={baseAttrs().length > 0}>
        <div>
          <div class="mb-1.5 text-[11px] font-semibold text-gray-500">Fields</div>
          <div class="grid grid-cols-[auto_1fr] gap-x-6 gap-y-1.5">
            <For each={baseAttrs()}>
              {(attr) => (
                <>
                  <span class="text-[11px] text-gray-400 font-medium whitespace-nowrap">
                    {attr.label}
                  </span>
                  <span class="text-[11px] font-mono text-gray-600 break-all">{attr.value}</span>
                </>
              )}
            </For>
          </div>
        </div>
      </Show>
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
