import { For, Show } from "solid-js";
import { Plus, Trash2 } from "lucide-solid";
import type { FirewallPolicyDraft, FirewallRuleDraft, FirewallSubjectType } from "./editor";

function FirewallPolicyForm(props: {
  draft: FirewallPolicyDraft;
  existing: boolean;
  readOnly: boolean;
  onField: <Key extends keyof FirewallPolicyDraft>(
    key: Key,
    value: FirewallPolicyDraft[Key]
  ) => void;
  onRule: (index: number, update: Partial<FirewallRuleDraft>) => void;
  onAddRule: () => void;
  onRemoveRule: (index: number) => void;
}) {
  const subjectOptions = () =>
    props.draft.direction === "egress"
      ? (["global", "service"] as FirewallSubjectType[])
      : (["global", "node"] as FirewallSubjectType[]);

  return (
    <fieldset disabled={props.readOnly} class="space-y-5 disabled:opacity-70">
      <div class="grid gap-4 sm:grid-cols-2">
        <Field label="Policy ID">
          <input
            value={props.draft.id}
            disabled={props.existing || props.readOnly}
            onInput={(event) => props.onField("id", event.currentTarget.value)}
            placeholder="api-egress"
            class={inputClass}
          />
        </Field>
        <Field label="Direction">
          <select
            value={props.draft.direction}
            onChange={(event) => props.onField("direction", event.currentTarget.value as never)}
            class={inputClass}
          >
            <option value="egress">Workload egress</option>
            <option value="hostInput">Host input</option>
          </select>
        </Field>
        <Field label="Subject">
          <select
            value={props.draft.subjectType}
            onChange={(event) =>
              props.onField("subjectType", event.currentTarget.value as FirewallSubjectType)
            }
            class={inputClass}
          >
            <For each={subjectOptions()}>
              {(subject) => <option value={subject}>{subjectLabel(subject)}</option>}
            </For>
          </select>
        </Field>
        <Show when={props.draft.subjectType !== "global"}>
          <Field label={props.draft.subjectType === "service" ? "Service ID" : "Node ID"}>
            <input
              value={props.draft.subjectId}
              onInput={(event) => props.onField("subjectId", event.currentTarget.value)}
              placeholder={props.draft.subjectType === "service" ? "api" : "worker-a"}
              class={inputClass}
            />
          </Field>
        </Show>
        <Field label="Default verdict">
          <select
            value={props.draft.defaultVerdict}
            onChange={(event) =>
              props.onField("defaultVerdict", event.currentTarget.value as "allow" | "deny")
            }
            class={inputClass}
          >
            <option value="deny">Deny</option>
            <option value="allow">Allow</option>
          </select>
        </Field>
      </div>

      <div>
        <div class="mb-2 flex items-center justify-between gap-3">
          <div>
            <h3 class="text-sm font-semibold text-gray-800">Ordered rules</h3>
            <p class="mt-0.5 text-[11px] text-gray-500">
              Rules are evaluated top to bottom before the default verdict.
            </p>
          </div>
          <button
            type="button"
            onClick={props.onAddRule}
            class="inline-flex items-center gap-1 rounded-md border border-gray-200 px-2 py-1 text-[11px] font-medium text-gray-600 hover:bg-gray-50"
          >
            <Plus class="size-3" /> Add rule
          </button>
        </div>
        <div class="space-y-2">
          <For each={props.draft.rules}>
            {(rule, index) => (
              <div class="grid gap-2 rounded-lg border border-gray-200 bg-gray-50 p-3 sm:grid-cols-[minmax(10rem,1fr)_6rem_minmax(8rem,0.8fr)_6rem_2rem]">
                <input
                  value={rule.cidr}
                  onInput={(event) => props.onRule(index(), { cidr: event.currentTarget.value })}
                  placeholder="10.0.0.0/8"
                  aria-label={`Rule ${index() + 1} CIDR`}
                  class={inputClass}
                />
                <select
                  value={rule.protocol}
                  onChange={(event) =>
                    props.onRule(index(), {
                      protocol: event.currentTarget.value as FirewallRuleDraft["protocol"]
                    })
                  }
                  aria-label={`Rule ${index() + 1} protocol`}
                  class={inputClass}
                >
                  <option value="tcp">TCP</option>
                  <option value="udp">UDP</option>
                  <option value="any">Any</option>
                </select>
                <input
                  value={rule.ports}
                  onInput={(event) => props.onRule(index(), { ports: event.currentTarget.value })}
                  placeholder="443, 8000-8080"
                  aria-label={`Rule ${index() + 1} ports`}
                  class={inputClass}
                />
                <select
                  value={rule.verdict}
                  onChange={(event) =>
                    props.onRule(index(), {
                      verdict: event.currentTarget.value as FirewallRuleDraft["verdict"]
                    })
                  }
                  aria-label={`Rule ${index() + 1} verdict`}
                  class={inputClass}
                >
                  <option value="allow">Allow</option>
                  <option value="deny">Deny</option>
                </select>
                <button
                  type="button"
                  onClick={() => props.onRemoveRule(index())}
                  aria-label={`Remove rule ${index() + 1}`}
                  class="flex size-8 items-center justify-center rounded-md text-gray-400 hover:bg-red-50 hover:text-red-600"
                >
                  <Trash2 class="size-3.5" />
                </button>
              </div>
            )}
          </For>
          <Show when={props.draft.rules.length === 0}>
            <div class="rounded-lg border border-dashed border-gray-200 px-3 py-8 text-center text-xs text-gray-400">
              No explicit rules. The default verdict applies to all traffic.
            </div>
          </Show>
        </div>
      </div>
    </fieldset>
  );
}

function Field(props: { label: string; children: import("solid-js").JSX.Element }) {
  return (
    <label class="block">
      <span class="mb-1 block text-[11px] font-medium text-gray-600">{props.label}</span>
      {props.children}
    </label>
  );
}

function subjectLabel(subject: FirewallSubjectType) {
  if (subject === "global") return "Cluster-wide";
  return subject === "service" ? "Service" : "Node";
}

const inputClass =
  "h-8 w-full rounded-md border border-gray-200 bg-white px-2 text-xs text-gray-700 outline-none focus:border-indigo-400 focus:ring-2 focus:ring-indigo-100 disabled:bg-gray-100";

export { FirewallPolicyForm };
