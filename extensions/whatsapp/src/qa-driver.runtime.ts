import type { WAMessage } from "baileys";
import { extractContextInfo, extractText } from "./inbound/extract.js";
import { createWebSendApi } from "./inbound/send-api.js";
import type { ActiveWebSendOptions } from "./inbound/types.js";
import { createWaSocket, waitForWaConnection } from "./session.js";
import { jidToE164 } from "./text-runtime.js";

export type WhatsAppQaDriverObservedMessageKind =
  | "media"
  | "poll"
  | "reaction"
  | "text"
  | "unknown";

export type WhatsAppQaDriverQuotedMessage = {
  messageId?: string;
  participant?: string;
  text?: string;
};

export type WhatsAppQaDriverObservedReaction = {
  emoji: string;
  fromMe?: boolean;
  messageId?: string;
  participant?: string;
};

export type WhatsAppQaDriverObservedPoll = {
  options: string[];
  question?: string;
};

export type WhatsAppQaDriverObservedMessage = {
  fromJid?: string;
  fromPhoneE164?: string | null;
  hasMedia?: boolean;
  kind: WhatsAppQaDriverObservedMessageKind;
  mediaFileName?: string;
  mediaType?: string;
  messageId?: string;
  observedAt: string;
  poll?: WhatsAppQaDriverObservedPoll;
  quoted?: WhatsAppQaDriverQuotedMessage;
  reaction?: WhatsAppQaDriverObservedReaction;
  text: string;
};

export type WhatsAppQaDriverSendTextOptions = Pick<ActiveWebSendOptions, "quotedMessageKey">;

export type WhatsAppQaDriverSendMediaOptions = Pick<
  ActiveWebSendOptions,
  "asDocument" | "fileName" | "gifPlayback" | "quotedMessageKey"
>;

export type WhatsAppQaDriverSendReactionOptions = {
  fromMe: boolean;
  participant?: string;
};

export type WhatsAppQaDriverSession = {
  close: () => Promise<void>;
  getObservedMessages: () => WhatsAppQaDriverObservedMessage[];
  sendMedia: (
    to: string,
    text: string,
    mediaBuffer: Buffer,
    mediaType: string,
    options?: WhatsAppQaDriverSendMediaOptions,
  ) => Promise<{ messageId?: string }>;
  sendPoll: (
    to: string,
    poll: { maxSelections?: number; options: string[]; question: string },
  ) => Promise<{ messageId?: string }>;
  sendReaction: (
    chatJid: string,
    messageId: string,
    emoji: string,
    options: WhatsAppQaDriverSendReactionOptions,
  ) => Promise<{ messageId?: string }>;
  sendText: (
    to: string,
    text: string,
    options?: WhatsAppQaDriverSendTextOptions,
  ) => Promise<{ messageId?: string }>;
  waitForMessage: (params: {
    match: (message: WhatsAppQaDriverObservedMessage) => boolean;
    timeoutMs: number;
  }) => Promise<WhatsAppQaDriverObservedMessage>;
};

type MessageUpsertEvent = {
  messages?: WAMessage[];
};

type Waiter = {
  predicate: (message: WhatsAppQaDriverObservedMessage) => boolean;
  reject: (error: Error) => void;
  resolve: (message: WhatsAppQaDriverObservedMessage) => void;
  timeout: NodeJS.Timeout;
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value && typeof value === "object");
}

function readString(value: unknown): string | undefined {
  return typeof value === "string" && value.trim() ? value.trim() : undefined;
}

function readBoolean(value: unknown): boolean | undefined {
  return typeof value === "boolean" ? value : undefined;
}

function findMessageSection(
  message: unknown,
  sectionNames: readonly string[],
): Record<string, unknown> | undefined {
  if (!isRecord(message)) {
    return undefined;
  }
  const queue: Array<{ depth: number; value: Record<string, unknown> }> = [
    { depth: 0, value: message },
  ];
  const seen = new Set<Record<string, unknown>>();
  while (queue.length > 0) {
    const current = queue.shift();
    if (!current || seen.has(current.value)) {
      continue;
    }
    seen.add(current.value);
    for (const sectionName of sectionNames) {
      const section = current.value[sectionName];
      if (isRecord(section)) {
        return section;
      }
    }
    if (current.depth >= 4) {
      continue;
    }
    for (const wrapperName of [
      "botInvokeMessage",
      "documentWithCaptionMessage",
      "ephemeralMessage",
      "groupMentionedMessage",
      "viewOnceMessage",
      "viewOnceMessageV2",
      "viewOnceMessageV2Extension",
    ]) {
      const wrapper = current.value[wrapperName];
      if (isRecord(wrapper) && isRecord(wrapper.message)) {
        queue.push({ depth: current.depth + 1, value: wrapper.message });
      }
    }
  }
  return undefined;
}

function readReaction(message: unknown): WhatsAppQaDriverObservedReaction | undefined {
  const reaction = findMessageSection(message, ["reactionMessage"]);
  if (!reaction) {
    return undefined;
  }
  const emoji = readString(reaction.text) ?? "";
  const key = isRecord(reaction.key) ? reaction.key : undefined;
  return {
    emoji,
    fromMe: readBoolean(key?.fromMe),
    messageId: readString(key?.id),
    participant: readString(key?.participant),
  };
}

function readPoll(message: unknown): WhatsAppQaDriverObservedPoll | undefined {
  const poll = findMessageSection(message, [
    "pollCreationMessage",
    "pollCreationMessageV2",
    "pollCreationMessageV3",
  ]);
  if (!poll) {
    return undefined;
  }
  const rawOptions = Array.isArray(poll.options) ? poll.options : [];
  const options = rawOptions
    .map((option) => (isRecord(option) ? readString(option.optionName) : undefined))
    .filter((option): option is string => Boolean(option));
  return {
    options,
    question: readString(poll.name),
  };
}

function readMedia(message: unknown):
  | {
      fileName?: string;
      mediaType?: string;
    }
  | undefined {
  const mediaSections = [
    ["imageMessage", "image"] as const,
    ["videoMessage", "video"] as const,
    ["audioMessage", "audio"] as const,
    ["documentMessage", "document"] as const,
    ["stickerMessage", "sticker"] as const,
  ];
  for (const [sectionName, fallbackType] of mediaSections) {
    const section = findMessageSection(message, [sectionName]);
    if (!section) {
      continue;
    }
    return {
      fileName: readString(section.fileName),
      mediaType: readString(section.mimetype) ?? fallbackType,
    };
  }
  return undefined;
}

function readQuotedMessage(message: WAMessage): WhatsAppQaDriverQuotedMessage | undefined {
  const contextInfo = extractContextInfo(message.message ?? undefined);
  if (!contextInfo) {
    return undefined;
  }
  const quotedText = extractText(contextInfo.quotedMessage ?? undefined);
  if (!contextInfo.stanzaId && !contextInfo.participant && !quotedText) {
    return undefined;
  }
  return {
    messageId: contextInfo.stanzaId ?? undefined,
    participant: contextInfo.participant ?? undefined,
    text: quotedText,
  };
}

function normalizeObservedMessage(
  message: WAMessage,
  authDir: string,
): WhatsAppQaDriverObservedMessage | null {
  if (message.key.fromMe) {
    return null;
  }
  const text = extractText(message.message ?? undefined);
  const reaction = readReaction(message.message);
  const poll = readPoll(message.message);
  const media = readMedia(message.message);
  const quoted = readQuotedMessage(message);
  const kind: WhatsAppQaDriverObservedMessageKind = reaction
    ? "reaction"
    : poll
      ? "poll"
      : media
        ? "media"
        : text
          ? "text"
          : "unknown";
  if (!text && kind === "unknown") {
    return null;
  }
  const fromJid = message.key.remoteJid ?? undefined;
  return {
    fromJid,
    fromPhoneE164: fromJid ? jidToE164(fromJid, { authDir }) : null,
    hasMedia: media ? true : undefined,
    kind,
    mediaFileName: media?.fileName,
    mediaType: media?.mediaType,
    messageId: message.key.id ?? undefined,
    observedAt: new Date().toISOString(),
    poll,
    quoted,
    reaction,
    text: text ?? "",
  };
}

function closeSocket(sock: Awaited<ReturnType<typeof createWaSocket>>) {
  const maybeEnd = (sock as unknown as { end?: (error?: Error) => void }).end;
  if (typeof maybeEnd === "function") {
    maybeEnd.call(sock);
    return;
  }
  const maybeClose = (sock.ws as unknown as { close?: () => void } | undefined)?.close;
  if (typeof maybeClose === "function") {
    maybeClose.call(sock.ws);
  }
}

export async function startWhatsAppQaDriverSession(params: {
  authDir: string;
  connectionTimeoutMs?: number;
}): Promise<WhatsAppQaDriverSession> {
  const sock = await createWaSocket(false, false, { authDir: params.authDir });
  const observedMessages: WhatsAppQaDriverObservedMessage[] = [];
  const waiters: Waiter[] = [];
  let closed = false;

  const removeWaiter = (waiter: Waiter) => {
    const index = waiters.indexOf(waiter);
    if (index >= 0) {
      waiters.splice(index, 1);
    }
    clearTimeout(waiter.timeout);
  };

  const observe = (message: WhatsAppQaDriverObservedMessage) => {
    observedMessages.push(message);
    for (const waiter of waiters.slice()) {
      if (!waiter.predicate(message)) {
        continue;
      }
      removeWaiter(waiter);
      waiter.resolve(message);
    }
  };

  const onMessagesUpsert = (event: MessageUpsertEvent) => {
    for (const rawMessage of event.messages ?? []) {
      const observed = normalizeObservedMessage(rawMessage, params.authDir);
      if (observed) {
        observe(observed);
      }
    }
  };

  const removeMessageListener = () => {
    const evWithOff = sock.ev as unknown as {
      off?: (event: string, listener: (event: MessageUpsertEvent) => void) => void;
    };
    evWithOff.off?.("messages.upsert", onMessagesUpsert);
  };

  const closeSessionResources = (waiterError?: Error) => {
    if (closed) {
      return;
    }
    closed = true;
    for (const waiter of waiters.slice()) {
      removeWaiter(waiter);
      if (waiterError) {
        waiter.reject(waiterError);
      }
    }
    removeMessageListener();
    closeSocket(sock);
  };

  sock.ev.on("messages.upsert", onMessagesUpsert);
  try {
    await waitForWaConnection(sock, { timeoutMs: params.connectionTimeoutMs ?? 45_000 });
  } catch (error) {
    closeSessionResources(
      error instanceof Error ? error : new Error("failed starting WhatsApp QA driver session"),
    );
    throw error;
  }

  const sendApi = createWebSendApi({
    sock,
    defaultAccountId: "qa-driver",
    authDir: params.authDir,
  });

  return {
    async close() {
      closeSessionResources(new Error("WhatsApp QA driver session closed"));
    },
    getObservedMessages() {
      return [...observedMessages];
    },
    async sendMedia(to, text, mediaBuffer, mediaType, options) {
      const result = await sendApi.sendMessage(to, text, mediaBuffer, mediaType, options);
      return {
        messageId: result.messageId,
      };
    },
    async sendPoll(to, poll) {
      const result = await sendApi.sendPoll(to, poll);
      return {
        messageId: result.messageId,
      };
    },
    async sendReaction(chatJid, messageId, emoji, options) {
      const result = await sendApi.sendReaction(
        chatJid,
        messageId,
        emoji,
        options.fromMe,
        options.participant,
      );
      return {
        messageId: result.messageId,
      };
    },
    async sendText(to, text, options) {
      const result = await sendApi.sendMessage(to, text, undefined, undefined, options);
      return {
        messageId: result.messageId,
      };
    },
    async waitForMessage(paramsLocal) {
      const existing = observedMessages.find(paramsLocal.match);
      if (existing) {
        return existing;
      }
      return await new Promise<WhatsAppQaDriverObservedMessage>((resolve, reject) => {
        const waiter: Waiter = {
          predicate: paramsLocal.match,
          resolve,
          reject,
          timeout: setTimeout(() => {
            removeWaiter(waiter);
            reject(new Error("timed out waiting for WhatsApp QA driver message"));
          }, paramsLocal.timeoutMs),
        };
        waiters.push(waiter);
      });
    },
  };
}
