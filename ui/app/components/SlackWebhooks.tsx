import { createResource, createSignal, For, Show } from "solid-js";
import clsx from "clsx";
import { Plus, Send, Trash2 } from "lucide-solid";
import type { SlackCategory, SlackWebhook } from "../lib/types";
import {
  createSlackWebhook,
  deleteSlackWebhook,
  listSlackWebhooks,
  testSlackWebhook,
  updateSlackWebhook
} from "../lib/api";

const ALL_CATEGORIES: SlackCategory[] = ["info", "error"];

function SlackWebhooks() {
  const [webhooks, { refetch }] = createResource(listSlackWebhooks);
  const [showForm, setShowForm] = createSignal(false);
  const [actionError, setActionError] = createSignal<string | null>(null);

  return (
    <div>
      <div class="flex items-baseline gap-2 mb-4 mt-10">
        <h2 class="text-sm font-medium text-gray-400 uppercase tracking-wider">Slack webhooks</h2>
        <span class="text-sm text-gray-400">{webhooks()?.length ?? 0}</span>
      </div>
      <Show when={actionError()}>
        <div class="mb-3 px-4 py-2 bg-red-50 border border-red-200 rounded-lg text-xs text-red-700">
          {actionError()}
        </div>
      </Show>
      <div class="bg-white border border-gray-200 rounded-lg divide-y divide-gray-100">
        <Show
          when={(webhooks()?.length ?? 0) > 0}
          fallback={<div class="px-4 py-3 text-xs text-gray-400">No webhooks configured.</div>}
        >
          <For each={webhooks()}>
            {(webhook) => (
              <WebhookRow
                webhook={webhook}
                onChange={() => refetch()}
                onError={(msg) => setActionError(msg)}
              />
            )}
          </For>
        </Show>
        <Show
          when={showForm()}
          fallback={
            <button
              type="button"
              onClick={() => {
                setActionError(null);
                setShowForm(true);
              }}
              class="w-full px-4 py-2.5 flex items-center gap-2 text-xs font-medium text-indigo-600 hover:bg-indigo-50 transition-colors outline-none"
            >
              <Plus class="size-3.5" />
              Add webhook
            </button>
          }
        >
          <WebhookForm
            onCancel={() => setShowForm(false)}
            onSaved={() => {
              setShowForm(false);
              setActionError(null);
              refetch();
            }}
            onError={(msg) => setActionError(msg)}
          />
        </Show>
      </div>
    </div>
  );
}

function WebhookRow(props: {
  webhook: SlackWebhook;
  onChange: () => void;
  onError: (msg: string) => void;
}) {
  const [busy, setBusy] = createSignal<"toggle" | "test" | "delete" | null>(null);

  const toggleEnabled = async () => {
    setBusy("toggle");
    try {
      await updateSlackWebhook(props.webhook.id, { enabled: !props.webhook.enabled });
      props.onChange();
    } catch (err) {
      props.onError(err instanceof Error ? err.message : "toggle failed");
    } finally {
      setBusy(null);
    }
  };

  const sendTest = async () => {
    setBusy("test");
    try {
      await testSlackWebhook(props.webhook.id);
    } catch (err) {
      props.onError(err instanceof Error ? err.message : "test failed");
    } finally {
      setBusy(null);
    }
  };

  const remove = async () => {
    if (!confirm(`Delete webhook "${props.webhook.name}"?`)) return;
    setBusy("delete");
    try {
      await deleteSlackWebhook(props.webhook.id);
      props.onChange();
    } catch (err) {
      props.onError(err instanceof Error ? err.message : "delete failed");
    } finally {
      setBusy(null);
    }
  };

  return (
    <div class="px-4 py-3 flex items-center justify-between gap-4">
      <div class="min-w-0 flex-1">
        <div class="flex items-center gap-2">
          <span class="text-sm font-medium text-gray-800 truncate">{props.webhook.name}</span>
          <For each={props.webhook.categories}>
            {(category) => (
              <span
                class={clsx(
                  "text-[10px] font-medium uppercase tracking-wider px-1.5 py-0.5 rounded",
                  {
                    "bg-sky-50 text-sky-700 border border-sky-200": category === "info",
                    "bg-red-50 text-red-700 border border-red-200": category === "error"
                  }
                )}
              >
                {category}
              </span>
            )}
          </For>
          <Show when={!props.webhook.enabled}>
            <span class="text-[10px] font-medium uppercase tracking-wider text-gray-400">
              disabled
            </span>
          </Show>
        </div>
        <p class="text-xs font-mono text-gray-400 truncate mt-0.5">{props.webhook.url}</p>
      </div>
      <div class="flex items-center gap-1 shrink-0">
        <button
          type="button"
          onClick={toggleEnabled}
          disabled={busy() !== null}
          class={clsx("text-xs px-2 py-1 rounded-md transition-colors", {
            "bg-gray-100 text-gray-600 hover:bg-gray-200": props.webhook.enabled,
            "bg-gray-50 text-gray-400 hover:bg-gray-100": !props.webhook.enabled
          })}
        >
          {props.webhook.enabled ? "disable" : "enable"}
        </button>
        <button
          type="button"
          onClick={sendTest}
          disabled={busy() !== null}
          title="Send test message"
          class="size-7 flex items-center justify-center text-gray-400 hover:text-indigo-600 hover:bg-indigo-50 rounded-md disabled:opacity-50"
        >
          <Send class="size-3.5" />
        </button>
        <button
          type="button"
          onClick={remove}
          disabled={busy() !== null}
          title="Delete webhook"
          class="size-7 flex items-center justify-center text-gray-400 hover:text-red-600 hover:bg-red-50 rounded-md disabled:opacity-50"
        >
          <Trash2 class="size-3.5" />
        </button>
      </div>
    </div>
  );
}

function WebhookForm(props: {
  onCancel: () => void;
  onSaved: () => void;
  onError: (msg: string) => void;
}) {
  const [name, setName] = createSignal("");
  const [url, setUrl] = createSignal("");
  const [categories, setCategories] = createSignal<SlackCategory[]>(["info", "error"]);
  const [saving, setSaving] = createSignal(false);

  const toggleCategory = (category: SlackCategory) => {
    setCategories((current) =>
      current.includes(category) ? current.filter((c) => c !== category) : [...current, category]
    );
  };

  const save = async () => {
    if (!name().trim()) {
      props.onError("Name is required");
      return;
    }
    if (!url().trim()) {
      props.onError("URL is required");
      return;
    }
    if (categories().length === 0) {
      props.onError("Select at least one category");
      return;
    }
    setSaving(true);
    try {
      await createSlackWebhook({
        name: name().trim(),
        url: url().trim(),
        categories: categories(),
        enabled: true
      });
      props.onSaved();
    } catch (err) {
      props.onError(err instanceof Error ? err.message : "create failed");
    } finally {
      setSaving(false);
    }
  };

  return (
    <div class="px-4 py-3 bg-gray-50">
      <div class="grid gap-2.5">
        <input
          type="text"
          placeholder="Name (e.g. #deploys)"
          value={name()}
          onInput={(e) => setName(e.currentTarget.value)}
          disabled={saving()}
          class="w-full px-2.5 py-1.5 text-sm text-gray-800 border border-gray-200 bg-white rounded-md outline-none focus:border-indigo-300"
        />
        <input
          type="url"
          placeholder="https://hooks.slack.com/services/..."
          value={url()}
          onInput={(e) => setUrl(e.currentTarget.value)}
          disabled={saving()}
          class="w-full px-2.5 py-1.5 text-sm font-mono text-gray-800 border border-gray-200 bg-white rounded-md outline-none focus:border-indigo-300"
        />
        <div class="flex items-center gap-2">
          <span class="text-xs text-gray-500">Categories:</span>
          <For each={ALL_CATEGORIES}>
            {(category) => (
              <label class="inline-flex items-center gap-1.5 text-xs text-gray-700 cursor-pointer">
                <input
                  type="checkbox"
                  checked={categories().includes(category)}
                  onChange={() => toggleCategory(category)}
                  disabled={saving()}
                  class="size-3.5"
                />
                {category}
              </label>
            )}
          </For>
        </div>
        <div class="flex items-center gap-2 mt-1">
          <button
            type="button"
            onClick={save}
            disabled={saving()}
            class="px-3 py-1.5 text-xs font-medium rounded-md bg-indigo-600 text-white hover:bg-indigo-700 disabled:bg-gray-300"
          >
            Save
          </button>
          <button
            type="button"
            onClick={props.onCancel}
            disabled={saving()}
            class="px-3 py-1.5 text-xs font-medium rounded-md text-gray-600 hover:bg-gray-100"
          >
            Cancel
          </button>
        </div>
      </div>
    </div>
  );
}

export { SlackWebhooks };
