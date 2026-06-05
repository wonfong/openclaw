import { EventEmitter } from "node:events";
import type { WAMessage } from "baileys";
import { afterEach, describe, expect, it, vi } from "vitest";
import { startWhatsAppQaDriverSession } from "./qa-driver.runtime.js";

const mocks = vi.hoisted(() => ({
  createWaSocket: vi.fn(),
  jidToE164: vi.fn(),
  sendContact: vi.fn(),
  sendLocation: vi.fn(),
  sendPoll: vi.fn(),
  sendReaction: vi.fn(),
  sendMessage: vi.fn(),
  sendSticker: vi.fn(),
  waitForWaConnection: vi.fn(),
}));

vi.mock("./session.js", () => ({
  createWaSocket: mocks.createWaSocket,
  waitForWaConnection: mocks.waitForWaConnection,
}));

vi.mock("./text-runtime.js", () => ({
  jidToE164: mocks.jidToE164,
}));

vi.mock("./inbound/send-api.js", () => ({
  createWebSendApi: () => ({
    sendContact: mocks.sendContact,
    sendLocation: mocks.sendLocation,
    sendMessage: mocks.sendMessage,
    sendPoll: mocks.sendPoll,
    sendReaction: mocks.sendReaction,
    sendSticker: mocks.sendSticker,
  }),
}));

function createMockSocket() {
  return {
    end: vi.fn(),
    ev: new EventEmitter(),
    ws: {
      close: vi.fn(),
    },
  };
}

function incomingMessage(remoteJid: string, text: string, id = "message-1"): WAMessage {
  return {
    key: {
      fromMe: false,
      id,
      remoteJid,
    },
    message: {
      conversation: text,
    },
  } as WAMessage;
}

function incomingImageMessage(remoteJid: string, text: string): WAMessage {
  return {
    key: {
      fromMe: false,
      id: "image-1",
      remoteJid,
    },
    message: {
      imageMessage: {
        caption: text,
        mimetype: "image/png",
      },
    },
  } as WAMessage;
}

function incomingAudioMessage(remoteJid: string): WAMessage {
  return {
    key: {
      fromMe: false,
      id: "audio-1",
      remoteJid,
    },
    message: {
      audioMessage: {
        mimetype: "audio/ogg; codecs=opus",
      },
    },
  } as WAMessage;
}

function incomingReactionMessage(remoteJid: string): WAMessage {
  return {
    key: {
      fromMe: false,
      id: "reaction-1",
      remoteJid,
    },
    message: {
      reactionMessage: {
        text: "👍",
        key: {
          fromMe: true,
          id: "driver-message-1",
          participant: "15551234567@s.whatsapp.net",
        },
      },
    },
  } as WAMessage;
}

function incomingQuotedMessage(remoteJid: string): WAMessage {
  return {
    key: {
      fromMe: false,
      id: "quoted-reply-1",
      remoteJid,
    },
    message: {
      extendedTextMessage: {
        text: "reply body",
        contextInfo: {
          participant: "15551234567@s.whatsapp.net",
          quotedMessage: {
            conversation: "original body",
          },
          stanzaId: "driver-message-1",
        },
      },
    },
  } as WAMessage;
}

describe("startWhatsAppQaDriverSession", () => {
  afterEach(() => {
    vi.useRealTimers();
    vi.clearAllMocks();
  });

  it("normalizes LID-backed senders using the QA auth directory", async () => {
    const sock = createMockSocket();
    mocks.createWaSocket.mockResolvedValue(sock);
    mocks.waitForWaConnection.mockResolvedValue(undefined);
    mocks.jidToE164.mockReturnValue("+15551234567");

    const session = await startWhatsAppQaDriverSession({
      authDir: "/tmp/openclaw-whatsapp-auth",
    });

    sock.ev.emit("messages.upsert", {
      messages: [incomingMessage("12345@lid", "hello")],
    });

    expect(mocks.jidToE164).toHaveBeenCalledWith("12345@lid", {
      authDir: "/tmp/openclaw-whatsapp-auth",
    });
    const observedMessages = session.getObservedMessages();
    const observedAt = observedMessages[0]?.observedAt;
    expect(observedAt).toBe(new Date(observedAt ?? "").toISOString());
    expect(observedMessages).toEqual([
      {
        fromJid: "12345@lid",
        fromPhoneE164: "+15551234567",
        kind: "text",
        messageId: "message-1",
        observedAt,
        text: "hello",
      },
    ]);

    await session.close();
  });

  it("does not satisfy a wait with messages observed before the lower bound", async () => {
    vi.useFakeTimers();
    const sock = createMockSocket();
    mocks.createWaSocket.mockResolvedValue(sock);
    mocks.waitForWaConnection.mockResolvedValue(undefined);
    mocks.jidToE164.mockReturnValue("+15551234567");

    vi.setSystemTime(new Date("2026-06-04T23:42:32.036Z"));
    const session = await startWhatsAppQaDriverSession({
      authDir: "/tmp/openclaw-whatsapp-auth",
    });

    sock.ev.emit("messages.upsert", {
      messages: [incomingMessage("12345@lid", "OpenClaw status stale", "stale-message")],
    });

    const observedAfter = new Date("2026-06-04T23:46:59.166Z");
    vi.setSystemTime(observedAfter);
    const waited = session.waitForMessage({
      observedAfter,
      timeoutMs: 1_000,
      match: (message) => message.text.includes("OpenClaw status"),
    });

    vi.setSystemTime(new Date("2026-06-04T23:47:00.000Z"));
    sock.ev.emit("messages.upsert", {
      messages: [incomingMessage("12345@lid", "OpenClaw status fresh", "fresh-message")],
    });

    await expect(waited).resolves.toMatchObject({
      messageId: "fresh-message",
      text: "OpenClaw status fresh",
    });

    await session.close();
  });

  it("observes media messages without dropping their caption text", async () => {
    const sock = createMockSocket();
    mocks.createWaSocket.mockResolvedValue(sock);
    mocks.waitForWaConnection.mockResolvedValue(undefined);
    mocks.jidToE164.mockReturnValue("+15551234567");

    const session = await startWhatsAppQaDriverSession({
      authDir: "/tmp/openclaw-whatsapp-auth",
    });

    sock.ev.emit("messages.upsert", {
      messages: [incomingImageMessage("12345@lid", "image caption")],
    });

    expect(session.getObservedMessages()[0]).toMatchObject({
      hasMedia: true,
      kind: "media",
      mediaType: "image/png",
      text: "image caption",
    });

    await session.close();
  });

  it("observes audio media messages without requiring a text body", async () => {
    const sock = createMockSocket();
    mocks.createWaSocket.mockResolvedValue(sock);
    mocks.waitForWaConnection.mockResolvedValue(undefined);
    mocks.jidToE164.mockReturnValue("+15551234567");

    const session = await startWhatsAppQaDriverSession({
      authDir: "/tmp/openclaw-whatsapp-auth",
    });

    sock.ev.emit("messages.upsert", {
      messages: [incomingAudioMessage("12345@lid")],
    });

    expect(session.getObservedMessages()[0]).toMatchObject({
      hasMedia: true,
      kind: "media",
      mediaType: "audio/ogg; codecs=opus",
      text: "",
    });

    await session.close();
  });

  it("observes reaction messages that have no text body", async () => {
    const sock = createMockSocket();
    mocks.createWaSocket.mockResolvedValue(sock);
    mocks.waitForWaConnection.mockResolvedValue(undefined);
    mocks.jidToE164.mockReturnValue("+15551234567");

    const session = await startWhatsAppQaDriverSession({
      authDir: "/tmp/openclaw-whatsapp-auth",
    });

    sock.ev.emit("messages.upsert", {
      messages: [incomingReactionMessage("12345@lid")],
    });

    expect(session.getObservedMessages()[0]).toMatchObject({
      kind: "reaction",
      reaction: {
        emoji: "👍",
        fromMe: true,
        messageId: "driver-message-1",
        participant: "15551234567@s.whatsapp.net",
      },
      text: "",
    });

    await session.close();
  });

  it("observes quoted reply context", async () => {
    const sock = createMockSocket();
    mocks.createWaSocket.mockResolvedValue(sock);
    mocks.waitForWaConnection.mockResolvedValue(undefined);
    mocks.jidToE164.mockReturnValue("+15551234567");

    const session = await startWhatsAppQaDriverSession({
      authDir: "/tmp/openclaw-whatsapp-auth",
    });

    sock.ev.emit("messages.upsert", {
      messages: [incomingQuotedMessage("12345@lid")],
    });

    expect(session.getObservedMessages()[0]).toMatchObject({
      kind: "text",
      quoted: {
        messageId: "driver-message-1",
        participant: "15551234567@s.whatsapp.net",
        text: "original body",
      },
      text: "reply body",
    });

    await session.close();
  });

  it("exposes structured send helpers over the web send API", async () => {
    const sock = createMockSocket();
    mocks.createWaSocket.mockResolvedValue(sock);
    mocks.waitForWaConnection.mockResolvedValue(undefined);
    mocks.sendMessage.mockResolvedValue({ messageId: "send-1" });
    mocks.sendPoll.mockResolvedValue({ messageId: "poll-1" });
    mocks.sendReaction.mockResolvedValue({ messageId: "reaction-send-1" });
    mocks.sendContact.mockResolvedValue({ messageId: "contact-1" });
    mocks.sendLocation.mockResolvedValue({ messageId: "location-1" });
    mocks.sendSticker.mockResolvedValue({ messageId: "sticker-1" });

    const session = await startWhatsAppQaDriverSession({
      authDir: "/tmp/openclaw-whatsapp-auth",
    });

    await expect(
      session.sendMedia("15551234567", "caption", Buffer.from("png"), "image/png", {
        fileName: "qa.png",
      }),
    ).resolves.toEqual({ messageId: "send-1" });
    await expect(
      session.sendPoll("15551234567", {
        question: "Pick one",
        options: ["A", "B"],
      }),
    ).resolves.toEqual({ messageId: "poll-1" });
    await expect(
      session.sendReaction("15551234567@s.whatsapp.net", "driver-message-1", "👍", {
        fromMe: true,
      }),
    ).resolves.toEqual({ messageId: "reaction-send-1" });
    await expect(
      session.sendContact("15551234567", {
        displayName: "QA Contact",
        vcard: "BEGIN:VCARD\nFN:QA Contact\nEND:VCARD",
      }),
    ).resolves.toEqual({ messageId: "contact-1" });
    await expect(
      session.sendLocation("15551234567", {
        degreesLatitude: 37.7749,
        degreesLongitude: -122.4194,
        name: "QA Location",
      }),
    ).resolves.toEqual({ messageId: "location-1" });
    await expect(
      session.sendSticker("15551234567", Buffer.from("webp"), { mimetype: "image/webp" }),
    ).resolves.toEqual({ messageId: "sticker-1" });

    expect(mocks.sendMessage).toHaveBeenCalledWith(
      "15551234567",
      "caption",
      Buffer.from("png"),
      "image/png",
      { fileName: "qa.png" },
    );
    expect(mocks.sendPoll).toHaveBeenCalledWith("15551234567", {
      question: "Pick one",
      options: ["A", "B"],
    });
    expect(mocks.sendReaction).toHaveBeenCalledWith(
      "15551234567@s.whatsapp.net",
      "driver-message-1",
      "👍",
      true,
      undefined,
    );
    expect(mocks.sendContact).toHaveBeenCalledWith("15551234567", {
      displayName: "QA Contact",
      vcard: "BEGIN:VCARD\nFN:QA Contact\nEND:VCARD",
    });
    expect(mocks.sendLocation).toHaveBeenCalledWith("15551234567", {
      degreesLatitude: 37.7749,
      degreesLongitude: -122.4194,
      name: "QA Location",
    });
    expect(mocks.sendSticker).toHaveBeenCalledWith("15551234567", Buffer.from("webp"), {
      mimetype: "image/webp",
    });

    await session.close();
  });

  it("passes the connection timeout to the shared connection waiter", async () => {
    const sock = createMockSocket();
    mocks.createWaSocket.mockResolvedValue(sock);
    mocks.waitForWaConnection.mockResolvedValue(undefined);

    const session = await startWhatsAppQaDriverSession({
      authDir: "/tmp/openclaw-whatsapp-auth",
      connectionTimeoutMs: 45_000,
    });

    expect(mocks.waitForWaConnection).toHaveBeenCalledWith(sock, { timeoutMs: 45_000 });

    await session.close();
  });

  it("closes the socket and removes listeners when connection setup times out", async () => {
    const sock = createMockSocket();
    const timeoutError = new Error("timed out waiting for WhatsApp QA driver session");
    mocks.createWaSocket.mockResolvedValue(sock);
    mocks.waitForWaConnection.mockRejectedValue(timeoutError);

    await expect(
      startWhatsAppQaDriverSession({
        authDir: "/tmp/openclaw-whatsapp-auth",
        connectionTimeoutMs: 10,
      }),
    ).rejects.toThrow("timed out waiting for WhatsApp QA driver session");

    expect(mocks.waitForWaConnection).toHaveBeenCalledWith(sock, { timeoutMs: 10 });
    expect(sock.ev.listenerCount("messages.upsert")).toBe(0);
    expect(sock.end).toHaveBeenCalledOnce();
  });
});
