// navigator.clipboard only exists in secure contexts (https or localhost), so the dashboard
// opened over plain http on a node or tailnet address falls back to execCommand("copy").
async function copyText(text: string, trigger: HTMLElement): Promise<void> {
  if (navigator.clipboard) {
    await navigator.clipboard.writeText(text);
  } else {
    const textarea = document.createElement("textarea");
    textarea.value = text;
    textarea.readOnly = true;
    textarea.style.position = "fixed";
    textarea.style.opacity = "0";
    (trigger.parentElement ?? document.body).append(textarea);
    textarea.select();
    const copied = document.execCommand("copy");
    textarea.remove();
    trigger.focus();
    if (!copied) {
      throw new Error("copy is not available in this browser");
    }
  }
}

export { copyText };
