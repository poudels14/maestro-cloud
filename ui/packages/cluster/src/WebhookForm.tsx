import { createSignal, For, Show } from "solid-js";
import { useMutation, useQueryClient } from "@tanstack/solid-query";
import type { ClusterApi } from "./api";
import { clusterQueryKeys } from "./queries";
import type { Webhook, WebhookCategory, WebhookEvent, WebhookFormat } from "./types";

const EVENT_OPTIONS: ReadonlyArray<{ value: WebhookEvent; label: string }> = [
  { value: "deploymentTransition", label: "Deployments" },
  { value: "nodeAvailability", label: "Nodes" },
  { value: "previewTransition", label: "Previews" },
  { value: "upgradeTransition", label: "Upgrades" }
];

const CATEGORY_OPTIONS: ReadonlyArray<{ value: WebhookCategory; label: string }> = [
  { value: "info", label: "Info" },
  { value: "error", label: "Errors" }
];

function WebhookForm(props: {
  api: ClusterApi;
  webhook?: Webhook | undefined;
  onCancel: () => void;
  onSaved: () => void;
  onError: (message: string) => void;
}) {
  const queryClient = useQueryClient();
  const current = props.webhook;
  const [id, setId] = createSignal(current?.meta.id ?? "");
  const [name, setName] = createSignal(current?.spec.name ?? "");
  const [endpoint, setEndpoint] = createSignal("");
  const [signingSecret, setSigningSecret] = createSignal("");
  const [format, setFormat] = createSignal<WebhookFormat>(current?.spec.format ?? "slack");
  const [events, setEvents] = createSignal<WebhookEvent[]>(
    current?.spec.events ?? ["deploymentTransition", "nodeAvailability"]
  );
  const [categories, setCategories] = createSignal<WebhookCategory[]>(
    current?.spec.categories ?? ["info", "error"]
  );
  const [enabled, setEnabled] = createSignal(current?.spec.enabled ?? true);

  const mutation = useMutation(() => ({
    mutationFn: async () => {
      const common = {
        name: name().trim(),
        events: events(),
        categories: categories(),
        enabled: enabled(),
        format: format(),
        ...(endpoint().trim() ? { endpoint: endpoint().trim() } : {}),
        ...(format() === "maestro" && signingSecret() ? { signingSecret: signingSecret() } : {})
      };
      if (current) {
        await props.api.updateWebhook(current, common);
      } else {
        await props.api.createWebhook({
          ...common,
          id: id().trim(),
          endpoint: endpoint().trim()
        });
      }
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: clusterQueryKeys.webhooks });
      props.onSaved();
    },
    onError: (error) =>
      props.onError(error instanceof Error ? error.message : "webhook save failed")
  }));

  const toggle = <Value,>(
    value: Value,
    values: () => Value[],
    setValues: (update: (currentValues: Value[]) => Value[]) => void
  ) => {
    setValues((currentValues) =>
      currentValues.includes(value)
        ? currentValues.filter((candidate) => candidate !== value)
        : [...currentValues, value]
    );
  };

  const save = () => {
    if (!current && !id().trim()) {
      props.onError("Webhook ID is required");
      return;
    }
    if (!name().trim()) {
      props.onError("Webhook name is required");
      return;
    }
    if (!current && !endpoint().trim()) {
      props.onError("Endpoint is required");
      return;
    }
    if (events().length === 0) {
      props.onError("Select at least one event");
      return;
    }
    if (categories().length === 0) {
      props.onError("Select at least one category");
      return;
    }
    const existingNativeSecret =
      current?.spec.format === "maestro" && Boolean(current.spec.signingSecret);
    if (format() === "maestro" && !signingSecret() && !existingNativeSecret) {
      props.onError("A native Maestro webhook requires a signing secret");
      return;
    }
    if (signingSecret() && signingSecret().length < 32) {
      props.onError("Signing secret must contain at least 32 characters");
      return;
    }
    mutation.mutate();
  };

  return (
    <div class="px-4 py-3 bg-gray-50">
      <div class="grid gap-2.5">
        <div class="grid grid-cols-1 sm:grid-cols-2 gap-2.5">
          <input
            type="text"
            placeholder="Webhook ID (e.g. operations)"
            value={id()}
            onInput={(event) => setId(event.currentTarget.value)}
            disabled={mutation.isPending || Boolean(current)}
            class="w-full px-2.5 py-1.5 text-sm text-gray-800 border border-gray-200 bg-white rounded-md outline-none focus:border-indigo-300 disabled:bg-gray-100"
          />
          <input
            type="text"
            placeholder="Display name"
            value={name()}
            onInput={(event) => setName(event.currentTarget.value)}
            disabled={mutation.isPending}
            class="w-full px-2.5 py-1.5 text-sm text-gray-800 border border-gray-200 bg-white rounded-md outline-none focus:border-indigo-300"
          />
        </div>
        <select
          value={format()}
          onChange={(event) => setFormat(event.currentTarget.value as WebhookFormat)}
          disabled={mutation.isPending}
          class="w-full px-2.5 py-1.5 text-sm text-gray-800 border border-gray-200 bg-white rounded-md outline-none focus:border-indigo-300"
        >
          <option value="slack">Slack incoming webhook</option>
          <option value="maestro">Native Maestro (signed JSON)</option>
        </select>
        <input
          type="url"
          placeholder={
            current
              ? `Leave blank to keep ${current.spec.endpoint}`
              : "https://hooks.slack.com/services/…"
          }
          value={endpoint()}
          onInput={(event) => setEndpoint(event.currentTarget.value)}
          disabled={mutation.isPending}
          autocomplete="off"
          class="w-full px-2.5 py-1.5 text-sm font-mono text-gray-800 border border-gray-200 bg-white rounded-md outline-none focus:border-indigo-300"
        />
        <Show when={format() === "maestro"}>
          <input
            type="password"
            placeholder={
              current
                ? "Leave blank to keep the current signing secret"
                : "Signing secret (at least 32 characters)"
            }
            value={signingSecret()}
            onInput={(event) => setSigningSecret(event.currentTarget.value)}
            disabled={mutation.isPending}
            autocomplete="new-password"
            class="w-full px-2.5 py-1.5 text-sm font-mono text-gray-800 border border-gray-200 bg-white rounded-md outline-none focus:border-indigo-300"
          />
        </Show>
        <OptionGroup
          label="Events"
          options={EVENT_OPTIONS}
          selected={events}
          toggle={(value) => toggle(value, events, setEvents)}
          disabled={mutation.isPending}
        />
        <OptionGroup
          label="Categories"
          options={CATEGORY_OPTIONS}
          selected={categories}
          toggle={(value) => toggle(value, categories, setCategories)}
          disabled={mutation.isPending}
        />
        <label class="inline-flex items-center gap-1.5 text-xs text-gray-700 cursor-pointer w-fit">
          <input
            type="checkbox"
            checked={enabled()}
            onChange={(event) => setEnabled(event.currentTarget.checked)}
            disabled={mutation.isPending}
            class="size-3.5"
          />
          Delivery enabled
        </label>
        <div class="flex items-center gap-2 mt-1">
          <button
            type="button"
            onClick={save}
            disabled={mutation.isPending}
            class="px-3 py-1.5 text-xs font-medium rounded-md bg-indigo-600 text-white hover:bg-indigo-700 disabled:bg-gray-300"
          >
            {current ? "Update" : "Save"}
          </button>
          <button
            type="button"
            onClick={props.onCancel}
            disabled={mutation.isPending}
            class="px-3 py-1.5 text-xs font-medium rounded-md text-gray-600 hover:bg-gray-100"
          >
            Cancel
          </button>
        </div>
      </div>
    </div>
  );
}

function OptionGroup<Value extends string>(props: {
  label: string;
  options: ReadonlyArray<{ value: Value; label: string }>;
  selected: () => Value[];
  toggle: (value: Value) => void;
  disabled: boolean;
}) {
  return (
    <div class="flex flex-wrap items-center gap-2">
      <span class="text-xs text-gray-500">{props.label}:</span>
      <For each={props.options}>
        {(option) => (
          <label class="inline-flex items-center gap-1.5 text-xs text-gray-700 cursor-pointer">
            <input
              type="checkbox"
              checked={props.selected().includes(option.value)}
              onChange={() => props.toggle(option.value)}
              disabled={props.disabled}
              class="size-3.5"
            />
            {option.label}
          </label>
        )}
      </For>
    </div>
  );
}

export { EVENT_OPTIONS, WebhookForm };
