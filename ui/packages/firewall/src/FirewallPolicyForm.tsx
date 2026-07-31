import { For, Show } from "solid-js";
import { CornerDownRight, Plus, Trash2 } from "lucide-solid";
import clsx from "clsx";
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
    <fieldset disabled={props.readOnly} class="space-y-6 disabled:opacity-70">
      <div class="grid gap-x-5 gap-y-4 sm:grid-cols-2">
        <Field label="Policy ID">
          <input
            value={props.draft.id}
            disabled={props.existing || props.readOnly}
            onInput={(event) => props.onField("id", event.currentTarget.value)}
            placeholder="api-egress"
            class={clsx(inputClass, "font-mono")}
          />
        </Field>
        <Field label="Direction">
          <Segmented
            value={props.draft.direction}
            options={[
              { value: "egress", label: "Workload egress" },
              { value: "hostInput", label: "Host input" }
            ]}
            onChange={(value) => props.onField("direction", value as never)}
          />
        </Field>
        <Field label="Applies to">
          <Segmented
            value={props.draft.subjectType}
            options={subjectOptions().map((subject) => ({
              value: subject,
              label: subjectLabel(subject)
            }))}
            onChange={(value) => props.onField("subjectType", value as FirewallSubjectType)}
          />
        </Field>
        <Show when={props.draft.subjectType !== "global"}>
          <Field label={props.draft.subjectType === "service" ? "Service ID" : "Node ID"}>
            <input
              value={props.draft.subjectId}
              onInput={(event) => props.onField("subjectId", event.currentTarget.value)}
              placeholder={props.draft.subjectType === "service" ? "api" : "worker-a"}
              class={clsx(inputClass, "font-mono")}
            />
          </Field>
        </Show>
      </div>

      <div>
        <div class="mb-2 flex items-center justify-between gap-3">
          <div>
            <h3 class="text-sm font-semibold text-neutral-900">Rules</h3>
            <p class="mt-0.5 text-xs text-neutral-400">
              Evaluated top to bottom — the first match wins.
            </p>
          </div>
          <Show when={!props.readOnly}>
            <button
              type="button"
              onClick={props.onAddRule}
              class="inline-flex items-center gap-1.5 rounded border border-neutral-200 bg-white px-2.5 py-1.5 text-xs font-medium text-neutral-600 hover:bg-neutral-50 hover:text-neutral-900"
            >
              <Plus class="size-3.5" /> Add rule
            </button>
          </Show>
        </div>

        <div class="overflow-hidden rounded-md border border-neutral-200">
          <div class="hidden gap-2 border-b border-neutral-100 bg-neutral-50 px-3 py-2 text-[10px] font-medium uppercase tracking-wide text-neutral-400 sm:grid sm:grid-cols-[1.5rem_minmax(10rem,1fr)_5.5rem_minmax(8rem,0.9fr)_6rem_2rem]">
            <span />
            <span>{props.draft.direction === "egress" ? "Destination CIDR" : "Source CIDR"}</span>
            <span>Protocol</span>
            <span>Ports</span>
            <span>Verdict</span>
            <span />
          </div>
          <div class="divide-y divide-neutral-100 bg-white">
            <For each={props.draft.rules}>
              {(rule, index) => (
                <div class="grid items-center gap-2 px-3 py-2 sm:grid-cols-[1.5rem_minmax(10rem,1fr)_5.5rem_minmax(8rem,0.9fr)_6rem_2rem]">
                  <span class="hidden text-center text-[11px] tabular-nums text-neutral-300 sm:block">
                    {index() + 1}
                  </span>
                  <input
                    value={rule.cidr}
                    onInput={(event) => props.onRule(index(), { cidr: event.currentTarget.value })}
                    placeholder="10.0.0.0/8"
                    aria-label={`Rule ${index() + 1} CIDR`}
                    class={clsx(inputClass, "font-mono")}
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
                    class={clsx(inputClass, "font-mono")}
                  />
                  <select
                    value={rule.verdict}
                    onChange={(event) =>
                      props.onRule(index(), {
                        verdict: event.currentTarget.value as FirewallRuleDraft["verdict"]
                      })
                    }
                    aria-label={`Rule ${index() + 1} verdict`}
                    class={clsx(inputClass, "font-medium", {
                      "text-emerald-700": rule.verdict === "allow",
                      "text-red-700": rule.verdict === "deny"
                    })}
                  >
                    <option value="allow">Allow</option>
                    <option value="deny">Deny</option>
                  </select>
                  <Show when={!props.readOnly} fallback={<span />}>
                    <button
                      type="button"
                      onClick={() => props.onRemoveRule(index())}
                      aria-label={`Remove rule ${index() + 1}`}
                      class="flex size-7 items-center justify-center justify-self-center rounded text-neutral-300 hover:bg-red-50 hover:text-red-600"
                    >
                      <Trash2 class="size-3.5" />
                    </button>
                  </Show>
                </div>
              )}
            </For>
            <Show when={props.draft.rules.length === 0}>
              <div class="px-3 py-6 text-center text-xs text-neutral-400">
                No explicit rules — every packet falls through to the default verdict.
              </div>
            </Show>
            <div class="flex flex-wrap items-center justify-between gap-2 bg-neutral-50/70 px-3 py-2">
              <span class="inline-flex items-center gap-1.5 text-xs text-neutral-500">
                <CornerDownRight class="size-3.5 text-neutral-300" />
                All other traffic
              </span>
              <Segmented
                value={props.draft.defaultVerdict}
                options={[
                  { value: "allow", label: "Allow" },
                  { value: "deny", label: "Deny" }
                ]}
                verdict
                onChange={(value) => props.onField("defaultVerdict", value as "allow" | "deny")}
              />
            </div>
          </div>
        </div>
      </div>
    </fieldset>
  );
}

function Segmented(props: {
  value: string;
  options: { value: string; label: string }[];
  onChange: (value: string) => void;
  verdict?: boolean;
}) {
  return (
    <div class="inline-flex h-8 items-center gap-0.5 rounded border border-neutral-200 bg-neutral-100/80 p-0.5">
      <For each={props.options}>
        {(option) => (
          <button
            type="button"
            onClick={() => props.onChange(option.value)}
            class={clsx(
              "h-full rounded-[3px] px-2.5 text-xs font-medium transition-colors duration-100",
              {
                "bg-white text-neutral-900 shadow-[0_1px_2px_rgb(0_0_0/0.08)]":
                  props.value === option.value && !props.verdict,
                "bg-white text-emerald-700 shadow-[0_1px_2px_rgb(0_0_0/0.08)]":
                  props.value === option.value && props.verdict && option.value === "allow",
                "bg-white text-red-700 shadow-[0_1px_2px_rgb(0_0_0/0.08)]":
                  props.value === option.value && props.verdict && option.value === "deny",
                "text-neutral-500 hover:text-neutral-800": props.value !== option.value
              }
            )}
          >
            {option.label}
          </button>
        )}
      </For>
    </div>
  );
}

function Field(props: { label: string; children: import("solid-js").JSX.Element }) {
  return (
    <label class="block">
      <span class="mb-1.5 block text-xs font-medium text-neutral-600">{props.label}</span>
      {props.children}
    </label>
  );
}

function subjectLabel(subject: FirewallSubjectType) {
  if (subject === "global") return "Whole cluster";
  return subject === "service" ? "One service" : "One node";
}

const inputClass =
  "h-8 w-full rounded border border-neutral-200 bg-white px-2.5 text-xs text-neutral-700 outline-none focus:border-brand focus:ring-2 focus:ring-brand-ring disabled:bg-neutral-100";

export { FirewallPolicyForm };
