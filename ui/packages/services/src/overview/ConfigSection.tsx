import { createSignal, For, Show } from "solid-js";
import { Eye, EyeOff } from "lucide-solid";

function ConfigSection(props: {
  title?: string;
  items: { label: string; value: string }[];
  maskValues?: boolean;
}) {
  const [revealed, setRevealed] = createSignal(false);
  const masked = () => props.maskValues && !revealed();

  return (
    <div>
      <Show when={props.title}>
        <div class="flex items-center justify-between mb-2">
          <h4 class="text-xs font-medium text-gray-400">{props.title}</h4>
          <Show when={props.maskValues}>
            <button
              type="button"
              onClick={() => setRevealed(!revealed())}
              class="text-gray-400 hover:text-gray-600 transition-colors"
            >
              <Show when={revealed()} fallback={<Eye class="size-3.5" />}>
                <EyeOff class="size-3.5" />
              </Show>
            </button>
          </Show>
        </div>
      </Show>
      <div class="bg-white rounded-lg border border-gray-200 divide-y divide-gray-100">
        <For each={props.items}>
          {(item) => (
            <div class="px-4 py-2.5 flex items-baseline justify-between gap-6">
              <span class="text-xs font-medium text-gray-700 shrink-0">{item.label}</span>
              <span
                class="text-xs tabular-nums text-gray-600 text-right truncate"
                title={masked() ? undefined : item.value}
              >
                {masked() ? "••••••••" : item.value}
              </span>
            </div>
          )}
        </For>
      </div>
    </div>
  );
}

export { ConfigSection };
