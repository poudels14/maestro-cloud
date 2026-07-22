import { createSignal, For, Show } from "solid-js";
import { useMutation, useQueryClient } from "@tanstack/solid-query";
import { useQuery } from "../lib/useQuery";
import clsx from "clsx";
import { Plus, Send, Trash2 } from "lucide-solid";
import type { Webhook, WebhookEvent } from "../lib/types";
import { createWebhook, deleteWebhook, testWebhook } from "../lib/api";
import { queryKeys, webhooksQuery } from "../lib/queries";
import { SectionHeader } from "@maestro/kit";
import { ConfirmDialog } from "./home/ConfirmDialog";

const EVENT_OPTIONS: ReadonlyArray<{ value: WebhookEvent; label: string }> = [
  { value: "deploymentTransition", label: "Deployments" },
  { value: "nodeAvailability", label: "Nodes" },
  { value: "previewTransition", label: "Previews" },
  { value: "upgradeTransition", label: "Upgrades" }
];

function Webhooks() {
  const queryClient = useQueryClient();
  const webhooks = useQuery(() => webhooksQuery());
  const [showForm, setShowForm] = createSignal(false);
  const [actionError, setActionError] = createSignal<string | null>(null);
  const [pendingDelete, setPendingDelete] = createSignal<Webhook | null>(null);

  const invalidate = () => queryClient.invalidateQueries({ queryKey: queryKeys.webhooks });

  const deleteMutation = useMutation(() => ({
    mutationFn: (webhook: Webhook) => deleteWebhook(webhook),
    onSuccess: () => {
      setPendingDelete(null);
      invalidate();
    },
    onError: (err) => {
      setActionError(err instanceof Error ? err.message : "delete failed");
      setPendingDelete(null);
    }
  }));

  return (
    <div>
      <div class="mb-4">
        <SectionHeader>Webhooks</SectionHeader>
        <p class="mt-1 text-xs text-gray-400">
          Deliver signed deployment, node, preview, and upgrade transitions to an HTTPS endpoint.
        </p>
      </div>
      <Show when={actionError()}>
        <div class="mb-3 px-4 py-2 bg-red-50 border border-red-200 rounded-lg text-xs text-red-700">
          {actionError()}
        </div>
      </Show>
      <div class="bg-white border border-gray-200 rounded-lg divide-y divide-gray-100">
        <Show
          when={(webhooks.data?.length ?? 0) > 0}
          fallback={<div class="px-4 py-3 text-xs text-gray-400">No webhooks configured.</div>}
        >
          <For each={webhooks.data}>
            {(webhook) => (
              <WebhookRow
                webhook={webhook}
                onError={(msg) => setActionError(msg)}
                onRequestDelete={() => {
                  setActionError(null);
                  setPendingDelete(webhook);
                }}
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
            }}
            onError={(msg) => setActionError(msg)}
          />
        </Show>
      </div>
      <ConfirmDialog
        open={pendingDelete() !== null}
        title="Delete webhook"
        description={
          <>
            Are you sure you want to delete{" "}
            <span class="font-medium text-gray-700">{pendingDelete()?.meta.id}</span>? No more
            events will be sent to this endpoint.
          </>
        }
        confirmLabel="Delete"
        confirmBusyLabel="Deleting…"
        busy={deleteMutation.isPending}
        onConfirm={() => {
          const target = pendingDelete();
          if (target) deleteMutation.mutate(target);
        }}
        onCancel={() => setPendingDelete(null)}
      />
    </div>
  );
}

function WebhookRow(props: {
  webhook: Webhook;
  onError: (msg: string) => void;
  onRequestDelete: () => void;
}) {
  const testMutation = useMutation(() => ({
    mutationFn: () => testWebhook(props.webhook.meta.id),
    onError: (err) => props.onError(err instanceof Error ? err.message : "test failed")
  }));
  const busy = () => testMutation.isPending;

  return (
    <div class="px-4 py-3 flex items-center justify-between gap-4">
      <div class="min-w-0 flex-1">
        <div class="flex items-center gap-2">
          <span class="text-sm font-medium text-gray-800 truncate">{props.webhook.meta.id}</span>
          <For each={props.webhook.spec.events}>
            {(event) => (
              <span
                class={clsx("text-[11px] font-medium px-1.5 py-0.5 rounded", {
                  "bg-sky-50 text-sky-700 border border-sky-200": event === "deploymentTransition",
                  "bg-emerald-50 text-emerald-700 border border-emerald-200":
                    event === "nodeAvailability",
                  "bg-violet-50 text-violet-700 border border-violet-200":
                    event === "previewTransition",
                  "bg-amber-50 text-amber-700 border border-amber-200":
                    event === "upgradeTransition"
                })}
              >
                {EVENT_OPTIONS.find((option) => option.value === event)?.label ?? event}
              </span>
            )}
          </For>
          <Show when={props.webhook.status.consecutiveFailures > 0}>
            <span class="text-[11px] font-medium text-red-600">
              {props.webhook.status.consecutiveFailures} failed
            </span>
          </Show>
        </div>
        <p class="text-xs font-mono text-gray-400 truncate mt-0.5">{props.webhook.spec.endpoint}</p>
      </div>
      <div class="flex items-center gap-1 shrink-0">
        <button
          type="button"
          onClick={() => testMutation.mutate()}
          disabled={busy()}
          title="Send test message"
          class="size-7 flex items-center justify-center text-gray-400 hover:text-indigo-600 hover:bg-indigo-50 rounded-md disabled:opacity-50"
        >
          <Send class="size-3.5" />
        </button>
        <button
          type="button"
          onClick={() => props.onRequestDelete()}
          disabled={busy()}
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
  const queryClient = useQueryClient();
  const [id, setId] = createSignal("");
  const [endpoint, setEndpoint] = createSignal("");
  const [signingSecret, setSigningSecret] = createSignal("");
  const [events, setEvents] = createSignal<WebhookEvent[]>(
    EVENT_OPTIONS.map((option) => option.value)
  );

  const createMutation = useMutation(() => ({
    mutationFn: () =>
      createWebhook({
        id: id().trim(),
        endpoint: endpoint().trim(),
        events: events(),
        signingSecret: signingSecret()
      }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: queryKeys.webhooks });
      props.onSaved();
    },
    onError: (err) => props.onError(err instanceof Error ? err.message : "create failed")
  }));

  const toggleEvent = (event: WebhookEvent) => {
    setEvents((current) =>
      current.includes(event)
        ? current.filter((candidate) => candidate !== event)
        : [...current, event]
    );
  };

  const save = () => {
    if (!id().trim()) {
      props.onError("Webhook ID is required");
      return;
    }
    if (!endpoint().trim()) {
      props.onError("Endpoint is required");
      return;
    }
    if (events().length === 0) {
      props.onError("Select at least one event");
      return;
    }
    if (signingSecret().length < 32) {
      props.onError("Signing secret must contain at least 32 characters");
      return;
    }
    createMutation.mutate();
  };

  return (
    <div class="px-4 py-3 bg-gray-50">
      <div class="grid gap-2.5">
        <input
          type="text"
          placeholder="Webhook ID (e.g. deployments)"
          value={id()}
          onInput={(e) => setId(e.currentTarget.value)}
          disabled={createMutation.isPending}
          class="w-full px-2.5 py-1.5 text-sm text-gray-800 border border-gray-200 bg-white rounded-md outline-none focus:border-indigo-300"
        />
        <input
          type="url"
          placeholder="https://events.example.com/maestro"
          value={endpoint()}
          onInput={(e) => setEndpoint(e.currentTarget.value)}
          disabled={createMutation.isPending}
          class="w-full px-2.5 py-1.5 text-sm font-mono text-gray-800 border border-gray-200 bg-white rounded-md outline-none focus:border-indigo-300"
        />
        <input
          type="password"
          placeholder="Signing secret (at least 32 characters)"
          value={signingSecret()}
          onInput={(e) => setSigningSecret(e.currentTarget.value)}
          disabled={createMutation.isPending}
          autocomplete="new-password"
          class="w-full px-2.5 py-1.5 text-sm font-mono text-gray-800 border border-gray-200 bg-white rounded-md outline-none focus:border-indigo-300"
        />
        <div class="flex items-center gap-2">
          <span class="text-xs text-gray-500">Events:</span>
          <For each={EVENT_OPTIONS}>
            {(option) => (
              <label class="inline-flex items-center gap-1.5 text-xs text-gray-700 cursor-pointer">
                <input
                  type="checkbox"
                  checked={events().includes(option.value)}
                  onChange={() => toggleEvent(option.value)}
                  disabled={createMutation.isPending}
                  class="size-3.5"
                />
                {option.label}
              </label>
            )}
          </For>
        </div>
        <div class="flex items-center gap-2 mt-1">
          <button
            type="button"
            onClick={save}
            disabled={createMutation.isPending}
            class="px-3 py-1.5 text-xs font-medium rounded-md bg-indigo-600 text-white hover:bg-indigo-700 disabled:bg-gray-300"
          >
            Save
          </button>
          <button
            type="button"
            onClick={props.onCancel}
            disabled={createMutation.isPending}
            class="px-3 py-1.5 text-xs font-medium rounded-md text-gray-600 hover:bg-gray-100"
          >
            Cancel
          </button>
        </div>
      </div>
    </div>
  );
}

export { Webhooks };
