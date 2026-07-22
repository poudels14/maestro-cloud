import { For, Show } from "solid-js";
import { createStore } from "solid-js/store";
import { FlaskConical, Loader2, Plus, ShieldCheck, Trash2 } from "lucide-solid";
import clsx from "clsx";
import { useQuery } from "../../lib/useQuery";
import { firewallPoliciesQuery } from "../../lib/queries";
import { deleteFirewallPolicy, dryRunFirewallPolicy, saveFirewallPolicy } from "../../lib/api";
import {
  emptyFirewallPolicy,
  emptyFirewallRule,
  firewallPolicyDraft,
  firewallPolicySpec
} from "../../lib/firewallPolicyEditor";
import type { FirewallPolicyDraft, FirewallRuleDraft } from "../../lib/firewallPolicyEditor";
import type { FirewallDryRun, FirewallPolicy } from "../../lib/types";
import { ConfirmDialog } from "../home/ConfirmDialog";
import { FirewallDryRunPanel } from "./FirewallDryRunPanel";
import { FirewallPolicyForm } from "./FirewallPolicyForm";

type Operation = "save" | "delete" | "dry-run";

interface EditorState {
  selectedId: string | null;
  expectedRevision: number | null;
  creating: boolean;
  draft: FirewallPolicyDraft;
  operation: Operation | null;
  error: string | null;
  dryRun: FirewallDryRun | null;
  confirmDelete: boolean;
}

function FirewallSection() {
  const policies = useQuery(() => firewallPoliciesQuery());
  const [editor, setEditor] = createStore<EditorState>({
    selectedId: null,
    expectedRevision: null,
    creating: false,
    draft: emptyFirewallPolicy(),
    operation: null,
    error: null,
    dryRun: null,
    confirmDelete: false
  });

  const selectedPolicy = () =>
    policies.data?.find((policy) => policy.meta.id === editor.selectedId) ?? null;
  const isEditing = () => editor.creating || selectedPolicy() != null;
  const isManaged = () => (selectedPolicy()?.meta.ownerRefs?.length ?? 0) > 0;
  const isDeleting = () => selectedPolicy()?.meta.deletionTimestamp != null;
  const readOnly = () => isManaged() || isDeleting();

  const selectPolicy = (policy: FirewallPolicy) => {
    setEditor({
      selectedId: policy.meta.id,
      expectedRevision: policy.meta.revision,
      creating: false,
      draft: firewallPolicyDraft(policy),
      operation: null,
      error: null,
      dryRun: null,
      confirmDelete: false
    });
  };

  const createPolicy = () => {
    setEditor({
      selectedId: null,
      expectedRevision: null,
      creating: true,
      draft: emptyFirewallPolicy(),
      operation: null,
      error: null,
      dryRun: null,
      confirmDelete: false
    });
  };

  const updateField = <Key extends keyof FirewallPolicyDraft>(
    key: Key,
    value: FirewallPolicyDraft[Key]
  ) => {
    setEditor("draft", (draft) => ({ ...draft, [key]: value }));
    if (key === "direction") {
      const allowed = value === "egress" ? ["global", "service"] : ["global", "node"];
      if (!allowed.includes(editor.draft.subjectType)) {
        setEditor("draft", "subjectType", "global");
        setEditor("draft", "subjectId", "");
      }
    }
    setEditor("dryRun", null);
    setEditor("error", null);
  };

  const updateRule = (index: number, update: Partial<FirewallRuleDraft>) => {
    setEditor("draft", "rules", (rules) =>
      rules.map((rule, ruleIndex) => (ruleIndex === index ? { ...rule, ...update } : rule))
    );
    setEditor("dryRun", null);
    setEditor("error", null);
  };

  const runOperation = async (operation: "save" | "dry-run") => {
    setEditor("operation", operation);
    setEditor("error", null);
    try {
      const spec = firewallPolicySpec(editor.draft);
      const policyId = editor.draft.id.trim();
      if (operation === "dry-run") {
        setEditor("dryRun", await dryRunFirewallPolicy(policyId, spec));
      } else {
        await saveFirewallPolicy(policyId, spec, editor.expectedRevision ?? undefined);
        const refreshed = await policies.refetch();
        setEditor("selectedId", policyId);
        setEditor(
          "expectedRevision",
          refreshed.data?.find((policy) => policy.meta.id === policyId)?.meta.revision ?? null
        );
        setEditor("creating", false);
        setEditor("dryRun", null);
      }
    } catch (cause) {
      setEditor("error", cause instanceof Error ? cause.message : String(cause));
    } finally {
      setEditor("operation", null);
    }
  };

  const confirmDelete = async () => {
    const policy = selectedPolicy();
    if (!policy || editor.expectedRevision == null) return;
    setEditor("operation", "delete");
    setEditor("error", null);
    try {
      await deleteFirewallPolicy(policy.meta.id, editor.expectedRevision);
      await policies.refetch();
      setEditor({
        selectedId: null,
        expectedRevision: null,
        creating: false,
        operation: null,
        dryRun: null,
        confirmDelete: false
      });
    } catch (cause) {
      setEditor("error", cause instanceof Error ? cause.message : String(cause));
      setEditor("operation", null);
      setEditor("confirmDelete", false);
    }
  };

  return (
    <section>
      <div class="mb-4 flex items-end justify-between gap-3">
        <div>
          <h1 class="text-lg font-semibold text-gray-900">Firewall policies</h1>
          <p class="mt-1 text-xs text-gray-500">
            Ordered workload-egress and host-input rules compiled for every affected node.
          </p>
        </div>
        <button
          type="button"
          onClick={createPolicy}
          class="inline-flex items-center gap-1.5 rounded-md bg-indigo-600 px-2.5 py-1.5 text-xs font-medium text-white hover:bg-indigo-700"
        >
          <Plus class="size-3.5" /> New policy
        </button>
      </div>

      <Show when={policies.isError}>
        <div class="mb-3 rounded-lg border border-red-200 bg-red-50 px-3 py-2 text-xs text-red-700">
          Failed to load firewall policies.
        </div>
      </Show>

      <div class="grid items-start gap-4 lg:grid-cols-[16rem_minmax(0,1fr)]">
        <div class="overflow-hidden rounded-xl border border-gray-200 bg-white">
          <div class="border-b border-gray-200 bg-gray-50 px-3 py-2 text-[11px] font-medium text-gray-500">
            {policies.data?.length ?? 0} policies
          </div>
          <For each={policies.data ?? []}>
            {(policy) => (
              <button
                type="button"
                onClick={() => selectPolicy(policy)}
                class={clsx(
                  "block w-full border-b border-gray-100 px-3 py-3 text-left last:border-b-0 hover:bg-gray-50",
                  editor.selectedId === policy.meta.id && !editor.creating && "bg-indigo-50"
                )}
              >
                <div class="flex items-center justify-between gap-2">
                  <span class="truncate font-mono text-xs font-medium text-gray-800">
                    {policy.meta.id}
                  </span>
                  <PolicyStatus policy={policy} />
                </div>
                <div class="mt-1 truncate text-[10px] text-gray-500">
                  {directionLabel(policy.spec.direction)} · {subjectLabel(policy)}
                </div>
              </button>
            )}
          </For>
          <Show when={!policies.isLoading && (policies.data?.length ?? 0) === 0}>
            <div class="px-3 py-10 text-center text-xs text-gray-400">No policies yet.</div>
          </Show>
        </div>

        <Show
          when={isEditing()}
          fallback={
            <div class="rounded-xl border border-dashed border-gray-200 bg-white px-6 py-20 text-center">
              <ShieldCheck class="mx-auto size-8 text-gray-300" />
              <p class="mt-3 text-sm font-medium text-gray-500">
                Select a policy or create a new one.
              </p>
            </div>
          }
        >
          <div class="rounded-xl border border-gray-200 bg-white p-4 sm:p-5">
            <div class="mb-5 flex flex-wrap items-start justify-between gap-3">
              <div>
                <h2 class="text-sm font-semibold text-gray-900">
                  {editor.creating ? "New firewall policy" : editor.draft.id}
                </h2>
                <Show when={isManaged()}>
                  <p class="mt-1 text-[11px] text-amber-600">
                    This policy is managed by another resource. It can be inspected and dry-run, but
                    edited only through its owner.
                  </p>
                </Show>
                <Show when={isDeleting()}>
                  <p class="mt-1 text-[11px] text-amber-600">Deletion is in progress.</p>
                </Show>
              </div>
              <Show when={selectedPolicy() && !readOnly()}>
                <button
                  type="button"
                  onClick={() => setEditor("confirmDelete", true)}
                  class="inline-flex items-center gap-1 rounded-md border border-red-200 px-2 py-1 text-[11px] font-medium text-red-600 hover:bg-red-50"
                >
                  <Trash2 class="size-3" /> Delete
                </button>
              </Show>
            </div>

            <Show when={selectedPolicy()}>{(policy) => <PolicyEvidence policy={policy()} />}</Show>

            <FirewallPolicyForm
              draft={editor.draft}
              existing={!editor.creating}
              readOnly={readOnly()}
              onField={updateField}
              onRule={updateRule}
              onAddRule={() => {
                setEditor("draft", "rules", (rules) => [...rules, emptyFirewallRule()]);
                setEditor("dryRun", null);
              }}
              onRemoveRule={(index) => {
                setEditor("draft", "rules", (rules) =>
                  rules.filter((_, ruleIndex) => ruleIndex !== index)
                );
                setEditor("dryRun", null);
              }}
            />

            <Show when={editor.error}>
              {(error) => (
                <div class="mt-4 rounded-lg border border-red-200 bg-red-50 px-3 py-2 text-xs text-red-700">
                  {error()}
                </div>
              )}
            </Show>

            <div class="mt-5 flex justify-end gap-2 border-t border-gray-100 pt-4">
              <button
                type="button"
                disabled={editor.operation != null || isDeleting()}
                onClick={() => runOperation("dry-run")}
                class="inline-flex items-center gap-1.5 rounded-md border border-indigo-200 px-3 py-1.5 text-xs font-medium text-indigo-700 hover:bg-indigo-50 disabled:opacity-50"
              >
                <Show
                  when={editor.operation === "dry-run"}
                  fallback={<FlaskConical class="size-3.5" />}
                >
                  <Loader2 class="size-3.5 animate-spin" />
                </Show>
                Dry run
              </button>
              <Show when={!readOnly()}>
                <button
                  type="button"
                  disabled={editor.operation != null}
                  onClick={() => runOperation("save")}
                  class="inline-flex items-center gap-1.5 rounded-md bg-indigo-600 px-3 py-1.5 text-xs font-medium text-white hover:bg-indigo-700 disabled:bg-indigo-300"
                >
                  <Show when={editor.operation === "save"}>
                    <Loader2 class="size-3.5 animate-spin" />
                  </Show>
                  {editor.creating ? "Create policy" : "Save changes"}
                </button>
              </Show>
            </div>

            <FirewallDryRunPanel result={editor.dryRun} />
          </div>
        </Show>
      </div>

      <ConfirmDialog
        open={editor.confirmDelete}
        title="Delete firewall policy?"
        description={
          <>
            Policy <span class="font-mono">{selectedPolicy()?.meta.id}</span> will stop governing
            traffic after the controller applies the next ruleset generation.
          </>
        }
        confirmLabel="Delete policy"
        confirmBusyLabel="Deleting…"
        busy={editor.operation === "delete"}
        onConfirm={() => void confirmDelete()}
        onCancel={() => setEditor("confirmDelete", false)}
      />
    </section>
  );
}

function PolicyStatus(props: { policy: FirewallPolicy }) {
  const state = () => {
    if (props.policy.meta.deletionTimestamp != null) return "deleting";
    const failure = props.policy.status.conditions?.find(
      (condition) => condition.status === "false" || condition.status === "unknown"
    );
    if (failure) return "error";
    return props.policy.status.appliedGeneration === props.policy.meta.generation
      ? "applied"
      : "pending";
  };
  return (
    <span
      class={clsx("rounded-full px-1.5 py-0.5 text-[9px] font-medium", {
        "bg-emerald-50 text-emerald-700": state() === "applied",
        "bg-amber-50 text-amber-700": state() === "pending" || state() === "deleting",
        "bg-red-50 text-red-700": state() === "error"
      })}
    >
      {state()}
    </span>
  );
}

function PolicyEvidence(props: { policy: FirewallPolicy }) {
  return (
    <div class="mb-5 rounded-lg border border-gray-100 bg-gray-50 px-3 py-2 text-[10px] text-gray-500">
      <div class="flex flex-wrap gap-x-4 gap-y-1">
        <span>
          generation <span class="font-mono text-gray-700">{props.policy.meta.generation}</span>
        </span>
        <span>
          applied{" "}
          <span class="font-mono text-gray-700">{props.policy.status.appliedGeneration}</span>
        </span>
        <Show when={props.policy.status.rulesetDigest}>
          {(digest) => (
            <span title={digest()}>
              ruleset <span class="font-mono text-gray-700">{digest().slice(0, 12)}</span>
            </span>
          )}
        </Show>
      </div>
      <For each={props.policy.status.conditions ?? []}>
        {(condition) => (
          <div class="mt-1">
            <span class="font-medium text-gray-600">{condition.type}</span>: {condition.message}
          </div>
        )}
      </For>
    </div>
  );
}

function directionLabel(direction: FirewallPolicy["spec"]["direction"]) {
  return direction === "egress" ? "workload egress" : "host input";
}

function subjectLabel(policy: FirewallPolicy) {
  const subject = policy.spec.subject;
  return subject.type === "global" ? "cluster-wide" : `${subject.type} ${subject.id}`;
}

export { FirewallSection };
