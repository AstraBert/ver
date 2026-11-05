import { mount } from "@cloudflare/nimbus-docs/client";

const RESET_MS = 1500;

function initCopyCode(root: HTMLElement): () => void {
  const button = root.querySelector<HTMLButtonElement>("[data-nb-copy-code-copy]");
  const label = root.querySelector<HTMLElement>("[data-nb-copy-code-label]");
  const status = root.querySelector<HTMLElement>("[data-nb-copy-code-status]");
  const text = button?.dataset.copy;
  if (!button || !label || !text) return () => {};

  const idle = label.textContent;
  let timer: number | undefined;
  const onCopy = async () => {
    let copied = true;
    try {
      await navigator.clipboard.writeText(text);
    } catch {
      copied = false;
    }
    if (!root.isConnected) return;
    label.textContent = copied ? "Copied" : "Couldn't copy";
    if (status) status.textContent = copied ? "Copied to clipboard" : "Couldn't copy to clipboard";
    if (timer !== undefined) window.clearTimeout(timer);
    timer = window.setTimeout(() => {
      label.textContent = idle;
      if (status) status.textContent = "";
    }, RESET_MS);
  };
  button.addEventListener("click", onCopy);

  return () => {
    button.removeEventListener("click", onCopy);
    if (timer !== undefined) window.clearTimeout(timer);
  };
}

mount("[data-nb-copy-code]", initCopyCode);
