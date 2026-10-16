// chan auto-pull overlay — deterministically injects pending channel messages
// into the session's LLM context before reasoning, so delivery does not depend
// on the model remembering to call chan_await / chan_recv.
//
// Install: copy/symlink this file to ~/.config/opencode/plugins/chan-auto-pull.ts

import type { Plugin } from "@opencode-ai/plugin";

export const ChanAutoPull: Plugin = async ({ $ }) => {
  const participant = (): string => {
    if (process.env.CHAN_PARTICIPANT) return process.env.CHAN_PARTICIPANT;
    if (process.env.OPENCODE_SESSION_ID) return `oc:${process.env.OPENCODE_SESSION_ID}`;
    return "orchestrator";
  };

  return {
    "chat.message": async (_input, output) => {
      const who = participant();
      try {
        const raw = (await $`chan recv --as ${who}`.text()) || "[]";
        let msgs: any[] = [];
        try {
          const parsed = JSON.parse(raw);
          if (Array.isArray(parsed)) msgs = parsed;
        } catch {
          return;
        }
        if (!msgs.length) return;

        const block = [
          "[chan inbound messages]",
          ...msgs.map((m) =>
            `- from=${m.from} kind=${m.kind} id=${m.id}: ${JSON.stringify(m.payload ?? null)}`
          ),
          "[end chan inbound]",
        ].join("\n");

        const parts = output.parts as any[];
        const textPart = parts.find((p) => p && p.type === "text");
        if (textPart) {
          textPart.text = `${textPart.text ?? ""}\n\n${block}`;
        } else if (output.message) {
          const m = output.message as any;
          parts.unshift({
            id: crypto.randomUUID(),
            sessionID: m.sessionID,
            messageID: m.id,
            type: "text",
            text: block,
            synthetic: true,
          });
        }
      } catch {
        // chan missing / no channel: stay out of the way.
      }
    },
  };
};