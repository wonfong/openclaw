import { execFile } from "node:child_process";
import { randomUUID } from "node:crypto";
import fs from "node:fs/promises";
import path from "node:path";
import { promisify } from "node:util";
import {
  startWhatsAppQaDriverSession,
  type WhatsAppQaDriverObservedMessage,
  type WhatsAppQaDriverSession,
} from "@openclaw/whatsapp/api.js";
import { normalizeE164 } from "openclaw/plugin-sdk/account-resolution";
import type { OpenClawConfig } from "openclaw/plugin-sdk/config-contracts";
import { formatErrorMessage } from "openclaw/plugin-sdk/error-runtime";
import { normalizeStringEntries, uniqueStrings } from "openclaw/plugin-sdk/string-coerce-runtime";
import { resolvePreferredOpenClawTmpDir } from "openclaw/plugin-sdk/temp-path";
import { z } from "zod";
import { startQaGatewayChild } from "../../gateway-child.js";
import { DEFAULT_QA_LIVE_PROVIDER_MODE } from "../../providers/index.js";
import { fingerprintQaCredentialId } from "../../qa-credentials-fingerprint.runtime.js";
import {
  defaultQaModelForMode,
  normalizeQaProviderMode,
  type QaProviderMode,
  type QaProviderModeInput,
} from "../../run-config.js";
import {
  acquireQaCredentialLease,
  startQaCredentialLeaseHeartbeat,
  type QaCredentialRole,
} from "../shared/credential-lease.runtime.js";
import {
  appendQaLiveLaneIssue as appendLiveLaneIssue,
  buildQaLiveLaneArtifactsError as buildLiveLaneArtifactsError,
} from "../shared/live-artifacts.js";
import { startQaLiveLaneGateway } from "../shared/live-gateway.runtime.js";
import {
  collectLiveTransportStandardScenarioCoverage,
  selectLiveTransportScenarios,
  type LiveTransportScenarioDefinition,
} from "../shared/live-transport-scenarios.js";

const execFileAsync = promisify(execFile);

export type WhatsAppQaRuntimeEnv = {
  driverAuthArchiveBase64: string;
  driverPhoneE164: string;
  sutAuthArchiveBase64: string;
  sutPhoneE164: string;
  groupJid?: string;
};

type WhatsAppQaScenarioId =
  | "whatsapp-access-control-dm-disabled"
  | "whatsapp-access-control-dm-open"
  | "whatsapp-access-control-group-disabled"
  | "whatsapp-access-control-group-open"
  | "whatsapp-approval-exec-deny-native"
  | "whatsapp-approval-exec-reaction-native"
  | "whatsapp-audio-preflight"
  | "whatsapp-canary"
  | "whatsapp-group-allowlist-block"
  | "whatsapp-group-audio-gating"
  | "whatsapp-help-command"
  | "whatsapp-inbound-image-caption"
  | "whatsapp-inbound-structured-messages"
  | "whatsapp-message-actions"
  | "whatsapp-native-new-command"
  | "whatsapp-outbound-media-matrix"
  | "whatsapp-outbound-poll"
  | "whatsapp-pairing-block"
  | "whatsapp-mention-gating"
  | "whatsapp-reply-delivery-shape"
  | "whatsapp-reply-to-message"
  | "whatsapp-restart-resume"
  | "whatsapp-status-command"
  | "whatsapp-status-reactions"
  | "whatsapp-top-level-reply-shape"
  | "whatsapp-approval-exec-native"
  | "whatsapp-approval-plugin-native";

type WhatsAppQaApprovalKind = "exec" | "plugin";
type WhatsAppQaApprovalDecision = "allow-once" | "deny";
type WhatsAppQaApprovalDecisionMode = "reaction" | "rpc";

type WhatsAppQaMessageSendMode =
  | {
      kind?: "text";
    }
  | {
      fileName?: string;
      kind: "media";
      mediaBuffer: Buffer;
      mediaType: string;
    };

type WhatsAppQaMessageScenarioContext = {
  driver: WhatsAppQaDriverSession;
  gateway: WhatsAppQaGateway;
  gatewayWorkspaceDir: string;
  recordObservedMessage: (message: WhatsAppQaDriverObservedMessage) => void;
  scenarioId: WhatsAppQaScenarioId;
  scenarioTitle: string;
  sent: { messageId?: string };
  sutAccountId: string;
  sutPhoneE164: string;
  target: string;
  waitForReady: () => Promise<void>;
};

type WhatsAppQaMessageScenarioRun = {
  afterReply?: (
    reply: WhatsAppQaDriverObservedMessage,
    context: WhatsAppQaMessageScenarioContext,
  ) => Promise<string | undefined> | string | undefined;
  afterSend?: (context: WhatsAppQaMessageScenarioContext) => Promise<string | undefined>;
  configMode: "allowlist" | "disabled" | "open" | "pairing";
  expectReply: boolean;
  input: string;
  kind?: "message";
  matchText: string | RegExp;
  quietInput?: string;
  quietMatchText?: string | RegExp;
  quietWindowMs?: number;
  sendMode?: WhatsAppQaMessageSendMode;
  target: "dm" | "group";
  verify?: (
    reply: WhatsAppQaDriverObservedMessage,
    context: WhatsAppQaMessageScenarioContext,
  ) => void;
};

type WhatsAppQaApprovalScenarioRun = {
  approvalKind: WhatsAppQaApprovalKind;
  decision: WhatsAppQaApprovalDecision;
  decisionMode?: WhatsAppQaApprovalDecisionMode;
  kind: "approval";
  token: string;
};

type WhatsAppQaScenarioRun = WhatsAppQaApprovalScenarioRun | WhatsAppQaMessageScenarioRun;

type WhatsAppQaConfigOverrides = {
  actions?: boolean;
  audioPreflight?: boolean;
  approvals?: {
    exec?: boolean;
    plugin?: boolean;
  };
  groupPolicy?: "allowlist" | "disabled" | "open";
  replyToMode?: "all" | "first" | "off";
  statusReactions?: boolean;
};

type WhatsAppQaScenarioDefinition = LiveTransportScenarioDefinition<WhatsAppQaScenarioId> & {
  buildRun: () => WhatsAppQaScenarioRun;
  configOverrides?: WhatsAppQaConfigOverrides;
  defaultEnabled?: boolean;
  defaultProviderModes?: readonly QaProviderMode[];
  requiresGroupJid?: boolean;
  requiredPluginIds?: readonly string[];
};

type WhatsAppQaGateway = Awaited<ReturnType<typeof startQaGatewayChild>>;

type WhatsAppObservedMessage = WhatsAppQaDriverObservedMessage & {
  approvalState?: "pending" | "resolved";
  matchedScenario?: boolean;
  scenarioId?: string;
  scenarioTitle?: string;
};

type WhatsAppObservedMessageArtifact = {
  approvalState?: "pending" | "resolved";
  fromPhoneE164?: string | null;
  hasMedia?: boolean;
  kind?: WhatsAppQaDriverObservedMessage["kind"];
  matchedScenario?: boolean;
  mediaFileName?: string;
  mediaType?: string;
  messageId?: string;
  observedAt: string;
  poll?: WhatsAppQaDriverObservedMessage["poll"];
  quoted?: WhatsAppQaDriverObservedMessage["quoted"];
  reaction?: WhatsAppQaDriverObservedMessage["reaction"];
  scenarioId?: string;
  scenarioTitle?: string;
  text?: string;
};

type WhatsAppQaScenarioResult = {
  details: string;
  id: string;
  requestStartedAt?: string;
  responseObservedAt?: string;
  rttMs?: number;
  rttMeasurement?: {
    finalMatchedReplyRttMs: number;
    requestStartedAt: string;
    responseObservedAt: string;
    source: "approval-request-to-resolution" | "request-to-observed-message";
  };
  status: "fail" | "pass" | "skip";
  title: string;
};

export type WhatsAppQaRunResult = {
  gatewayDebugDirPath?: string;
  observedMessagesPath: string;
  outputDir: string;
  reportPath: string;
  scenarios: WhatsAppQaScenarioResult[];
  summaryPath: string;
};

type WhatsAppQaSummary = {
  cleanupIssues: string[];
  counts: {
    failed: number;
    passed: number;
    skipped: number;
    total: number;
  };
  credentials: {
    credentialFingerprint?: string;
    credentialId?: string;
    kind: string;
    ownerId?: string;
    role?: QaCredentialRole;
    source: "convex" | "env";
  };
  finishedAt: string;
  scenarios: WhatsAppQaScenarioResult[];
  startedAt: string;
  sutAccountId: string;
  sutPhoneE164: string;
};

type WhatsAppCredentialLease = Awaited<
  ReturnType<typeof acquireQaCredentialLease<WhatsAppQaRuntimeEnv>>
>;
type WhatsAppCredentialHeartbeat = ReturnType<typeof startQaCredentialLeaseHeartbeat>;

const WHATSAPP_QA_CAPTURE_CONTENT_ENV = "OPENCLAW_QA_WHATSAPP_CAPTURE_CONTENT";
const QA_REDACT_PUBLIC_METADATA_ENV = "OPENCLAW_QA_REDACT_PUBLIC_METADATA";
const WHATSAPP_QA_TRANSIENT_DRIVER_ATTEMPTS = 5;
const WHATSAPP_QA_READY_TIMEOUT_MS = 150_000;
const WHATSAPP_QA_READY_STABILITY_MS = 20_000;
const WHATSAPP_QA_DRIVER_RECONNECT_DELAY_MS = 10_000;
const WHATSAPP_QA_APPROVAL_DECISION_TIMEOUT_MS = 60_000;
const WHATSAPP_QA_ENV_KEYS = [
  "OPENCLAW_QA_WHATSAPP_DRIVER_PHONE_E164",
  "OPENCLAW_QA_WHATSAPP_SUT_PHONE_E164",
  "OPENCLAW_QA_WHATSAPP_DRIVER_AUTH_ARCHIVE_BASE64",
  "OPENCLAW_QA_WHATSAPP_SUT_AUTH_ARCHIVE_BASE64",
] as const;
const WHATSAPP_QA_ONE_PIXEL_PNG = Buffer.from(
  "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAFgwJ/lzK4ZQAAAABJRU5ErkJggg==",
  "base64",
);
const WHATSAPP_QA_ONE_PIXEL_WEBP = Buffer.from(
  "UklGRiIAAABXRUJQVlA4IBYAAAAwAQCdASoBAAEADsD+JaQAA3AA/vuUAAA=",
  "base64",
);
const WHATSAPP_QA_AUDIO_TRANSCRIPT_MARKER = "WHATSAPP_QA_AUDIO_TRANSCRIPT_OK";

function createWhatsAppQaPdfBuffer() {
  return Buffer.from(
    [
      "%PDF-1.4",
      "1 0 obj",
      "<< /Type /Catalog /Pages 2 0 R >>",
      "endobj",
      "2 0 obj",
      "<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
      "endobj",
      "3 0 obj",
      "<< /Type /Page /Parent 2 0 R /MediaBox [0 0 200 200] >>",
      "endobj",
      "trailer",
      "<< /Root 1 0 R >>",
      "%%EOF",
      "",
    ].join("\n"),
    "utf8",
  );
}

function createWhatsAppQaAudioWavBuffer() {
  const sampleRate = 16_000;
  const channelCount = 1;
  const bitsPerSample = 16;
  const durationSeconds = 1;
  const bytesPerSample = bitsPerSample / 8;
  const dataBytes = sampleRate * durationSeconds * channelCount * bytesPerSample;
  const buffer = Buffer.alloc(44 + dataBytes);
  buffer.write("RIFF", 0, "ascii");
  buffer.writeUInt32LE(36 + dataBytes, 4);
  buffer.write("WAVE", 8, "ascii");
  buffer.write("fmt ", 12, "ascii");
  buffer.writeUInt32LE(16, 16);
  buffer.writeUInt16LE(1, 20);
  buffer.writeUInt16LE(channelCount, 22);
  buffer.writeUInt32LE(sampleRate, 24);
  buffer.writeUInt32LE(sampleRate * channelCount * bytesPerSample, 28);
  buffer.writeUInt16LE(channelCount * bytesPerSample, 32);
  buffer.writeUInt16LE(bitsPerSample, 34);
  buffer.write("data", 36, "ascii");
  buffer.writeUInt32LE(dataBytes, 40);
  return buffer;
}

const whatsappQaCredentialPayloadSchema = z.object({
  driverPhoneE164: z.string().trim().min(1),
  sutPhoneE164: z.string().trim().min(1),
  driverAuthArchiveBase64: z.string().trim().min(1),
  sutAuthArchiveBase64: z.string().trim().min(1),
  groupJid: z.string().trim().min(1).optional(),
});

const WHATSAPP_QA_SCENARIOS: WhatsAppQaScenarioDefinition[] = [
  {
    id: "whatsapp-canary",
    standardId: "canary",
    title: "WhatsApp DM canary",
    timeoutMs: 60_000,
    buildRun: () => {
      const token = `WHATSAPP_QA_ECHO_${randomUUID().slice(0, 8).toUpperCase()}`;
      return {
        configMode: "allowlist",
        expectReply: true,
        input: `Reply with only this exact marker: ${token}`,
        matchText: token,
        target: "dm",
      };
    },
  },
  {
    id: "whatsapp-pairing-block",
    standardId: "allowlist-block",
    title: "WhatsApp non-allowlisted DM gets pairing gate",
    timeoutMs: 20_000,
    buildRun: () => ({
      configMode: "pairing",
      expectReply: true,
      input: `Do not run the agent for this pairing QA marker ${randomUUID().slice(0, 8)}`,
      matchText: /OpenClaw: access not configured|Pairing code:/iu,
      target: "dm",
    }),
  },
  {
    id: "whatsapp-mention-gating",
    standardId: "mention-gating",
    title: "WhatsApp group mention gating",
    timeoutMs: 60_000,
    requiresGroupJid: true,
    buildRun: () => {
      const quietToken = `WHATSAPP_QA_GROUP_QUIET_${randomUUID().slice(0, 8).toUpperCase()}`;
      const replyToken = `WHATSAPP_QA_GROUP_MENTION_${randomUUID().slice(0, 8).toUpperCase()}`;
      return {
        configMode: "allowlist",
        expectReply: true,
        input: `openclawqa reply with only this exact marker: ${replyToken}`,
        matchText: replyToken,
        quietInput: `This group message is intentionally unmentioned. If you respond, include ${quietToken}.`,
        quietMatchText: quietToken,
        quietWindowMs: 5_000,
        target: "group",
      };
    },
  },
  {
    id: "whatsapp-top-level-reply-shape",
    standardId: "top-level-reply-shape",
    title: "WhatsApp DM top-level reply shape",
    timeoutMs: 60_000,
    configOverrides: {
      replyToMode: "off",
    },
    buildRun: () => {
      const token = `WHATSAPP_QA_TOP_LEVEL_${randomUUID().slice(0, 8).toUpperCase()}`;
      return {
        configMode: "allowlist",
        expectReply: true,
        input: `Reply with only this exact marker: ${token}`,
        matchText: token,
        target: "dm",
        verify: (reply) => {
          if (reply.quoted?.messageId) {
            throw new Error(
              `expected top-level WhatsApp reply without quote metadata, got quoted message ${reply.quoted.messageId}`,
            );
          }
        },
      };
    },
  },
  {
    id: "whatsapp-restart-resume",
    standardId: "restart-resume",
    title: "WhatsApp DM resumes after gateway restart",
    timeoutMs: 120_000,
    buildRun: () => {
      const firstToken = `WHATSAPP_QA_RESTART_BEFORE_${randomUUID().slice(0, 8).toUpperCase()}`;
      const secondToken = `WHATSAPP_QA_RESTART_AFTER_${randomUUID().slice(0, 8).toUpperCase()}`;
      return {
        afterReply: async (_reply, context) => {
          await context.gateway.restart();
          await context.waitForReady();
          await context.driver.sendText(
            context.target,
            `After the restart, reply with only this exact marker: ${secondToken}`,
          );
          await context.driver.waitForMessage({
            timeoutMs: 60_000,
            match: (message) =>
              message.fromPhoneE164 === context.sutPhoneE164 && message.text.includes(secondToken),
          });
          return "gateway restarted and post-restart reply matched";
        },
        configMode: "allowlist",
        expectReply: true,
        input: `Before the restart, reply with only this exact marker: ${firstToken}`,
        matchText: firstToken,
        target: "dm",
      };
    },
  },
  {
    id: "whatsapp-help-command",
    standardId: "help-command",
    title: "WhatsApp help command replies",
    timeoutMs: 60_000,
    buildRun: () => ({
      configMode: "allowlist",
      expectReply: true,
      input: "/help",
      matchText: /OpenClaw|commands|status|\/new/iu,
      target: "dm",
    }),
  },
  {
    id: "whatsapp-status-command",
    title: "WhatsApp status command replies",
    timeoutMs: 60_000,
    buildRun: () => ({
      configMode: "allowlist",
      expectReply: true,
      input: "/status",
      matchText: /OpenClaw|status|session|agent/iu,
      target: "dm",
    }),
  },
  {
    id: "whatsapp-reply-to-message",
    title: "WhatsApp DM reply-to mode quotes the triggering message",
    timeoutMs: 60_000,
    configOverrides: {
      replyToMode: "all",
    },
    buildRun: () => {
      const token = `WHATSAPP_QA_REPLY_TO_${randomUUID().slice(0, 8).toUpperCase()}`;
      return {
        configMode: "allowlist",
        expectReply: true,
        input: `Reply with only this exact marker: ${token}`,
        matchText: token,
        target: "dm",
        verify: (reply, context) => {
          if (!context.sent.messageId) {
            throw new Error("WhatsApp driver did not return a triggering message id.");
          }
          if (reply.quoted?.messageId !== context.sent.messageId) {
            throw new Error(
              `expected reply quote ${context.sent.messageId}, got ${reply.quoted?.messageId ?? "<missing>"}`,
            );
          }
        },
      };
    },
  },
  {
    id: "whatsapp-inbound-image-caption",
    title: "WhatsApp inbound image caption reaches the agent",
    defaultProviderModes: ["mock-openai"],
    timeoutMs: 60_000,
    buildRun: () => {
      const token = `WHATSAPP_QA_IMAGE_${randomUUID().slice(0, 8).toUpperCase()}`;
      return {
        configMode: "allowlist",
        expectReply: true,
        input: `This image caption asks you to reply with only this exact marker: ${token}`,
        matchText: token,
        sendMode: {
          fileName: "whatsapp-qa.png",
          kind: "media",
          mediaBuffer: WHATSAPP_QA_ONE_PIXEL_PNG,
          mediaType: "image/png",
        },
        target: "dm",
      };
    },
  },
  {
    id: "whatsapp-audio-preflight",
    title: "WhatsApp inbound audio preflight transcript reaches the agent",
    defaultProviderModes: ["mock-openai"],
    timeoutMs: 90_000,
    configOverrides: {
      audioPreflight: true,
    },
    requiredPluginIds: ["openai"],
    buildRun: () => ({
      configMode: "allowlist",
      expectReply: true,
      input: "",
      matchText: WHATSAPP_QA_AUDIO_TRANSCRIPT_MARKER,
      sendMode: {
        fileName: "whatsapp-qa-audio.wav",
        kind: "media",
        mediaBuffer: createWhatsAppQaAudioWavBuffer(),
        mediaType: "audio/wav",
      },
      target: "dm",
    }),
  },
  {
    id: "whatsapp-outbound-media-matrix",
    title: "WhatsApp gateway send delivers outbound media variants",
    defaultProviderModes: ["mock-openai"],
    timeoutMs: 120_000,
    buildRun: () => {
      const token = `WHATSAPP_QA_OUTBOUND_MEDIA_${randomUUID().slice(0, 8).toUpperCase()}`;
      return {
        afterReply: async (_reply, context) => {
          const mediaRootToken = randomUUID().slice(0, 8);
          const imagePath = await writeWhatsAppQaWorkspaceFixture(context, {
            buffer: WHATSAPP_QA_ONE_PIXEL_PNG,
            fileName: `whatsapp-qa-${mediaRootToken}.png`,
          });
          const documentPath = await writeWhatsAppQaWorkspaceFixture(context, {
            buffer: createWhatsAppQaPdfBuffer(),
            fileName: `whatsapp-qa-${mediaRootToken}.pdf`,
          });
          const audioPath = await writeWhatsAppQaWorkspaceFixture(context, {
            buffer: createWhatsAppQaAudioWavBuffer(),
            fileName: `whatsapp-qa-${mediaRootToken}.wav`,
          });

          const imageStartedAt = new Date();
          await callWhatsAppGatewaySend(context, {
            label: "image",
            mediaUrl: imagePath,
            message: `${token}_IMAGE`,
          });
          await waitForScenarioObservedMessage(context, {
            observedAfter: imageStartedAt,
            match: (message) =>
              message.kind === "media" &&
              message.hasMedia === true &&
              message.mediaType?.startsWith("image/") === true &&
              message.text.includes(`${token}_IMAGE`),
          });

          const documentStartedAt = new Date();
          await callWhatsAppGatewaySend(context, {
            forceDocument: true,
            label: "document",
            mediaUrl: documentPath,
            message: `${token}_DOCUMENT`,
          });
          await waitForScenarioObservedMessage(context, {
            observedAfter: documentStartedAt,
            match: (message) =>
              message.kind === "media" &&
              message.hasMedia === true &&
              (message.mediaType === "application/pdf" ||
                message.mediaFileName?.endsWith(".pdf") === true) &&
              message.text.includes(`${token}_DOCUMENT`),
          });

          const audioStartedAt = new Date();
          await callWhatsAppGatewaySend(context, {
            asVoice: true,
            label: "audio",
            mediaUrl: audioPath,
            message: `${token}_AUDIO`,
          });
          await waitForScenarioObservedMessage(context, {
            observedAfter: audioStartedAt,
            match: (message) =>
              message.kind === "media" &&
              message.hasMedia === true &&
              message.mediaType?.startsWith("audio/") === true,
          });
          await waitForScenarioObservedMessage(context, {
            observedAfter: audioStartedAt,
            match: (message) => message.text.includes(`${token}_AUDIO`),
          });

          const multiStartedAt = new Date();
          await callWhatsAppGatewaySend(context, {
            label: "multi",
            mediaUrls: [imagePath, documentPath],
            message: `${token}_MULTI`,
          });
          await waitForScenarioObservedMessage(context, {
            observedAfter: multiStartedAt,
            match: (message) =>
              message.kind === "media" && message.mediaType?.startsWith("image/") === true,
          });
          await waitForScenarioObservedMessage(context, {
            observedAfter: multiStartedAt,
            match: (message) =>
              message.kind === "media" &&
              (message.mediaType === "application/pdf" ||
                message.mediaFileName?.endsWith(".pdf") === true),
          });
          return "gateway send delivered image, document, audio, and multi-media";
        },
        configMode: "allowlist",
        expectReply: true,
        input: `Reply with only this exact marker before outbound media checks: ${token}`,
        matchText: token,
        target: "dm",
      };
    },
  },
  {
    id: "whatsapp-outbound-poll",
    title: "WhatsApp gateway poll delivers outbound native poll",
    defaultProviderModes: ["mock-openai"],
    timeoutMs: 90_000,
    buildRun: () => {
      const token = `WHATSAPP_QA_OUTBOUND_POLL_${randomUUID().slice(0, 8).toUpperCase()}`;
      const question = `${token} choose one`;
      return {
        afterReply: async (_reply, context) => {
          const pollStartedAt = new Date();
          await callWhatsAppGatewayPoll(context, {
            label: "poll",
            options: ["alpha", "beta"],
            question,
          });
          const poll = await waitForScenarioObservedMessage(context, {
            observedAfter: pollStartedAt,
            match: (message) =>
              message.kind === "poll" &&
              message.poll?.question === question &&
              message.poll.options.includes("alpha") &&
              message.poll.options.includes("beta"),
          });
          return `poll observed with ${poll.poll?.options.length ?? 0} options`;
        },
        configMode: "allowlist",
        expectReply: true,
        input: `Reply with only this exact marker before outbound poll check: ${token}`,
        matchText: token,
        target: "dm",
      };
    },
  },
  {
    id: "whatsapp-message-actions",
    title: "WhatsApp message.action react and upload-file execute",
    defaultProviderModes: ["mock-openai"],
    timeoutMs: 120_000,
    configOverrides: {
      actions: true,
    },
    buildRun: () => {
      const token = `WHATSAPP_QA_ACTIONS_${randomUUID().slice(0, 8).toUpperCase()}`;
      return {
        afterReply: async (_reply, context) => {
          if (!context.sent.messageId) {
            throw new Error("WhatsApp driver did not return a triggering message id.");
          }
          const reactionStartedAt = new Date();
          await callWhatsAppGatewayMessageAction(context, {
            action: "react",
            label: "react",
            params: {
              emoji: "👍",
              messageId: context.sent.messageId,
              to: context.target,
            },
          });
          await waitForScenarioObservedMessage(context, {
            observedAfter: reactionStartedAt,
            match: (message) =>
              message.kind === "reaction" &&
              message.reaction?.messageId === context.sent.messageId &&
              message.reaction?.emoji === "👍",
          });

          const uploadStartedAt = new Date();
          await callWhatsAppGatewayMessageAction(context, {
            action: "upload-file",
            label: "upload-file",
            params: {
              buffer: WHATSAPP_QA_ONE_PIXEL_PNG.toString("base64"),
              caption: `${token}_UPLOAD`,
              contentType: "image/png",
              filename: "whatsapp-qa-upload.png",
              to: context.target,
            },
          });
          await waitForScenarioObservedMessage(context, {
            observedAfter: uploadStartedAt,
            match: (message) =>
              message.kind === "media" &&
              message.mediaType?.startsWith("image/") === true &&
              message.text.includes(`${token}_UPLOAD`),
          });
          return "message.action react and upload-file observed";
        },
        configMode: "allowlist",
        expectReply: true,
        input: `Reply with only this exact marker before action checks: ${token}`,
        matchText: token,
        target: "dm",
      };
    },
  },
  {
    id: "whatsapp-inbound-structured-messages",
    title: "WhatsApp inbound structured messages reach the agent",
    defaultProviderModes: ["mock-openai"],
    timeoutMs: 150_000,
    buildRun: () => {
      const token = `WHATSAPP_QA_STRUCTURED_${randomUUID().slice(0, 8).toUpperCase()}`;
      return {
        afterReply: async (_reply, context) => {
          const documentToken = `${token}_DOCUMENT`;
          await context.driver.sendMedia(
            context.target,
            `Reply with only this exact marker after reading the document caption: ${documentToken}`,
            createWhatsAppQaPdfBuffer(),
            "application/pdf",
            { fileName: "whatsapp-qa-document.pdf" },
          );
          const documentReply = await context.driver.waitForMessage({
            timeoutMs: 60_000,
            match: (message) =>
              message.fromPhoneE164 === context.sutPhoneE164 &&
              message.text.includes(documentToken),
          });
          context.recordObservedMessage(documentReply);

          const pollToken = `${token}_POLL`;
          await context.driver.sendPoll(context.target, {
            question: `Reply with only this exact marker after seeing this poll: ${pollToken}`,
            options: ["yes", "no"],
          });
          const pollReply = await context.driver.waitForMessage({
            timeoutMs: 60_000,
            match: (message) =>
              message.fromPhoneE164 === context.sutPhoneE164 && message.text.includes(pollToken),
          });
          context.recordObservedMessage(pollReply);

          await context.driver.sendContact(context.target, {
            displayName: "WhatsApp QA Contact",
            vcard: "BEGIN:VCARD\nVERSION:3.0\nFN:WhatsApp QA Contact\nTEL:+15550101010\nEND:VCARD",
          });
          const locationToken = `${token}_LOCATION`;
          await context.driver.sendLocation(context.target, {
            degreesLatitude: 37.7749,
            degreesLongitude: -122.4194,
            name: `Reply with only this exact marker after seeing this location: ${locationToken}`,
          });
          const locationReply = await context.driver.waitForMessage({
            timeoutMs: 60_000,
            match: (message) =>
              message.fromPhoneE164 === context.sutPhoneE164 &&
              message.text.includes(locationToken),
          });
          context.recordObservedMessage(locationReply);
          await context.driver.sendSticker(context.target, WHATSAPP_QA_ONE_PIXEL_WEBP, {
            mimetype: "image/webp",
          });
          return "document, poll, and location elicited replies; contact and sticker were sent";
        },
        configMode: "allowlist",
        expectReply: true,
        input: `Reply with only this exact marker before structured inbound checks: ${token}`,
        matchText: token,
        target: "dm",
      };
    },
  },
  {
    id: "whatsapp-group-audio-gating",
    title: "WhatsApp group audio mention gating",
    defaultProviderModes: ["mock-openai"],
    timeoutMs: 120_000,
    configOverrides: {
      audioPreflight: true,
    },
    requiredPluginIds: ["openai"],
    requiresGroupJid: true,
    buildRun: () => {
      const quietToken = `WHATSAPP_QA_GROUP_AUDIO_QUIET_${randomUUID().slice(0, 8).toUpperCase()}`;
      const replyToken = `WHATSAPP_QA_GROUP_AUDIO_${randomUUID().slice(0, 8).toUpperCase()}`;
      return {
        configMode: "allowlist",
        expectReply: true,
        input: `openclawqa reply with only this exact marker after group audio preflight: ${replyToken}`,
        matchText: replyToken,
        quietInput: `This unmentioned group audio caption must stay quiet: ${quietToken}`,
        quietMatchText: quietToken,
        quietWindowMs: 5_000,
        sendMode: {
          fileName: "whatsapp-qa-group-audio.wav",
          kind: "media",
          mediaBuffer: createWhatsAppQaAudioWavBuffer(),
          mediaType: "audio/wav",
        },
        target: "group",
      };
    },
  },
  {
    id: "whatsapp-access-control-dm-open",
    title: "WhatsApp dmPolicy open allows direct messages",
    defaultProviderModes: ["mock-openai"],
    timeoutMs: 60_000,
    buildRun: () => {
      const token = `WHATSAPP_QA_DM_OPEN_${randomUUID().slice(0, 8).toUpperCase()}`;
      return {
        configMode: "open",
        expectReply: true,
        input: `Reply with only this exact marker under dmPolicy open: ${token}`,
        matchText: token,
        target: "dm",
      };
    },
  },
  {
    id: "whatsapp-access-control-dm-disabled",
    title: "WhatsApp dmPolicy disabled stays quiet",
    defaultProviderModes: ["mock-openai"],
    timeoutMs: 8_000,
    buildRun: () => {
      const token = `WHATSAPP_QA_DM_DISABLED_${randomUUID().slice(0, 8).toUpperCase()}`;
      return {
        configMode: "disabled",
        expectReply: false,
        input: `Do not reply under dmPolicy disabled. Forbidden marker: ${token}`,
        matchText: token,
        target: "dm",
      };
    },
  },
  {
    id: "whatsapp-access-control-group-open",
    title: "WhatsApp groupPolicy open allows mention-gated groups",
    defaultProviderModes: ["mock-openai"],
    requiresGroupJid: true,
    timeoutMs: 60_000,
    configOverrides: {
      groupPolicy: "open",
    },
    buildRun: () => {
      const token = `WHATSAPP_QA_GROUP_OPEN_${randomUUID().slice(0, 8).toUpperCase()}`;
      return {
        configMode: "allowlist",
        expectReply: true,
        input: `openclawqa reply with only this exact marker under groupPolicy open: ${token}`,
        matchText: token,
        target: "group",
      };
    },
  },
  {
    id: "whatsapp-access-control-group-disabled",
    title: "WhatsApp groupPolicy disabled stays quiet",
    defaultProviderModes: ["mock-openai"],
    requiresGroupJid: true,
    timeoutMs: 8_000,
    configOverrides: {
      groupPolicy: "disabled",
    },
    buildRun: () => {
      const token = `WHATSAPP_QA_GROUP_DISABLED_${randomUUID().slice(0, 8).toUpperCase()}`;
      return {
        configMode: "allowlist",
        expectReply: false,
        input: `openclawqa groupPolicy disabled must not reply with ${token}`,
        matchText: token,
        target: "group",
      };
    },
  },
  {
    id: "whatsapp-reply-delivery-shape",
    title: "WhatsApp gateway send chunks long quoted replies",
    defaultProviderModes: ["mock-openai"],
    timeoutMs: 120_000,
    buildRun: () => {
      const token = `WHATSAPP_QA_REPLY_SHAPE_${randomUUID().slice(0, 8).toUpperCase()}`;
      return {
        afterReply: async (_reply, context) => {
          if (!context.sent.messageId) {
            throw new Error("WhatsApp driver did not return a triggering message id.");
          }
          const chunkStartedAt = new Date();
          const longText = `${token}_LONG_BEGIN\n${"A".repeat(4_500)}\n${token}_LONG_END`;
          await callWhatsAppGatewaySend(context, {
            label: "long-reply",
            message: longText,
            replyToId: context.sent.messageId,
          });
          const firstChunk = await waitForScenarioObservedMessage(context, {
            observedAfter: chunkStartedAt,
            match: (message) =>
              message.text.includes(`${token}_LONG_BEGIN`) &&
              message.quoted?.messageId === context.sent.messageId,
          });
          const secondChunk = await waitForScenarioObservedMessage(context, {
            observedAfter: chunkStartedAt,
            match: (message) =>
              message.messageId !== firstChunk.messageId && message.text.length > 0,
          });
          return `long reply chunked across ${firstChunk.messageId ?? "<first>"} and ${secondChunk.messageId ?? "<second>"}`;
        },
        configMode: "allowlist",
        expectReply: true,
        input: `Reply with only this exact marker before reply-shape checks: ${token}`,
        matchText: token,
        target: "dm",
      };
    },
  },
  {
    id: "whatsapp-native-new-command",
    title: "WhatsApp /new command starts a new session",
    defaultProviderModes: ["mock-openai"],
    timeoutMs: 60_000,
    buildRun: () => ({
      configMode: "allowlist",
      expectReply: true,
      input: "/new",
      matchText: /new session|session/i,
      target: "dm",
    }),
  },
  {
    id: "whatsapp-approval-exec-deny-native",
    title: "WhatsApp native exec approval prompt denies",
    timeoutMs: 60_000,
    configOverrides: {
      approvals: {
        exec: true,
      },
    },
    buildRun: () => ({
      approvalKind: "exec",
      decision: "deny",
      kind: "approval",
      token: `WHATSAPP_QA_EXEC_DENY_${randomUUID().slice(0, 8).toUpperCase()}`,
    }),
  },
  {
    id: "whatsapp-status-reactions",
    standardId: "reaction-observation",
    title: "WhatsApp status reactions are observable",
    timeoutMs: 60_000,
    configOverrides: {
      statusReactions: true,
    },
    buildRun: () => {
      const token = `WHATSAPP_QA_STATUS_REACTION_${randomUUID().slice(0, 8).toUpperCase()}`;
      return {
        afterSend: async (context) => {
          if (!context.sent.messageId) {
            throw new Error("WhatsApp driver did not return a triggering message id.");
          }
          const reaction = await context.driver.waitForMessage({
            timeoutMs: 30_000,
            match: (message) => {
              const observedReaction = message.reaction;
              if (!observedReaction) {
                return false;
              }
              return (
                message.kind === "reaction" &&
                message.fromPhoneE164 === context.sutPhoneE164 &&
                observedReaction.messageId === context.sent.messageId &&
                Boolean(observedReaction.emoji)
              );
            },
          });
          return `status reaction ${reaction.reaction?.emoji ?? "<unknown>"} observed`;
        },
        configMode: "allowlist",
        expectReply: true,
        input: `Reply with only this exact marker after normal processing: ${token}`,
        matchText: token,
        target: "dm",
      };
    },
  },
  {
    id: "whatsapp-group-allowlist-block",
    title: "WhatsApp group outside allowlist stays quiet",
    timeoutMs: 8_000,
    configOverrides: {
      groupPolicy: "allowlist",
    },
    requiresGroupJid: true,
    buildRun: () => {
      const quietToken = `WHATSAPP_QA_GROUP_BLOCK_${randomUUID().slice(0, 8).toUpperCase()}`;
      return {
        configMode: "allowlist",
        expectReply: false,
        input: `openclawqa blocked group should not reply with ${quietToken}`,
        matchText: quietToken,
        target: "group",
      };
    },
  },
  {
    id: "whatsapp-approval-exec-native",
    title: "WhatsApp native exec approval prompt resolves",
    timeoutMs: 60_000,
    configOverrides: {
      approvals: {
        exec: true,
      },
    },
    buildRun: () => ({
      approvalKind: "exec",
      decision: "allow-once",
      kind: "approval",
      token: `WHATSAPP_QA_EXEC_APPROVAL_${randomUUID().slice(0, 8).toUpperCase()}`,
    }),
  },
  {
    id: "whatsapp-approval-exec-reaction-native",
    title: "WhatsApp native exec approval resolves from reaction",
    timeoutMs: 60_000,
    configOverrides: {
      approvals: {
        exec: true,
      },
    },
    buildRun: () => ({
      approvalKind: "exec",
      decision: "allow-once",
      decisionMode: "reaction",
      kind: "approval",
      token: `WHATSAPP_QA_EXEC_REACTION_APPROVAL_${randomUUID().slice(0, 8).toUpperCase()}`,
    }),
  },
  {
    id: "whatsapp-approval-plugin-native",
    title: "WhatsApp native plugin approval prompt resolves with exec approvals enabled",
    timeoutMs: 60_000,
    configOverrides: {
      approvals: {
        exec: true,
        plugin: true,
      },
    },
    buildRun: () => ({
      approvalKind: "plugin",
      decision: "allow-once",
      kind: "approval",
      token: `WHATSAPP_QA_PLUGIN_APPROVAL_${randomUUID().slice(0, 8).toUpperCase()}`,
    }),
  },
];

export const WHATSAPP_QA_STANDARD_SCENARIO_IDS = collectLiveTransportStandardScenarioCoverage({
  scenarios: WHATSAPP_QA_SCENARIOS,
});

function isTruthyOptIn(value: string | undefined) {
  const normalized = value?.trim().toLowerCase();
  return normalized === "1" || normalized === "true" || normalized === "yes";
}

function resolveEnvValue(env: NodeJS.ProcessEnv, key: (typeof WHATSAPP_QA_ENV_KEYS)[number]) {
  const value = env[key]?.trim();
  if (!value) {
    throw new Error(`Missing ${key}.`);
  }
  return value;
}

function inferWhatsAppCredentialSource(
  value: string | undefined,
  env: NodeJS.ProcessEnv = process.env,
): "convex" | "env" {
  const normalized =
    value?.trim().toLowerCase() || env.OPENCLAW_QA_CREDENTIAL_SOURCE?.trim().toLowerCase();
  return normalized === "convex" ? "convex" : "env";
}

function inferWhatsAppCredentialRole(value: string | undefined): QaCredentialRole | undefined {
  const normalized = value?.trim().toLowerCase();
  if (normalized === "ci" || normalized === "maintainer") {
    return normalized;
  }
  return undefined;
}

function resolveWhatsAppMetadataRedaction(env: NodeJS.ProcessEnv = process.env) {
  const raw = env[QA_REDACT_PUBLIC_METADATA_ENV];
  return raw === undefined ? true : isTruthyOptIn(raw);
}

function normalizePhone(value: string, label: string) {
  const normalized = normalizeE164(value);
  if (!/^\+[1-9]\d{6,14}$/u.test(normalized)) {
    throw new Error(`${label} must be an E.164 phone number.`);
  }
  return normalized;
}

function validateWhatsAppQaRuntimeEnv(
  runtimeEnv: WhatsAppQaRuntimeEnv,
  label: string,
): WhatsAppQaRuntimeEnv {
  const driverPhoneE164 = normalizePhone(runtimeEnv.driverPhoneE164, `${label} driverPhoneE164`);
  const sutPhoneE164 = normalizePhone(runtimeEnv.sutPhoneE164, `${label} sutPhoneE164`);
  if (driverPhoneE164 === sutPhoneE164) {
    throw new Error(`${label} requires two distinct WhatsApp phone numbers.`);
  }
  return {
    ...runtimeEnv,
    driverPhoneE164,
    sutPhoneE164,
  };
}

function resolveWhatsAppQaRuntimeEnv(env: NodeJS.ProcessEnv = process.env): WhatsAppQaRuntimeEnv {
  return validateWhatsAppQaRuntimeEnv(
    {
      driverPhoneE164: resolveEnvValue(env, "OPENCLAW_QA_WHATSAPP_DRIVER_PHONE_E164"),
      sutPhoneE164: resolveEnvValue(env, "OPENCLAW_QA_WHATSAPP_SUT_PHONE_E164"),
      driverAuthArchiveBase64: resolveEnvValue(
        env,
        "OPENCLAW_QA_WHATSAPP_DRIVER_AUTH_ARCHIVE_BASE64",
      ),
      sutAuthArchiveBase64: resolveEnvValue(env, "OPENCLAW_QA_WHATSAPP_SUT_AUTH_ARCHIVE_BASE64"),
      groupJid: env.OPENCLAW_QA_WHATSAPP_GROUP_JID?.trim() || undefined,
    },
    "OPENCLAW_QA_WHATSAPP",
  );
}

function parseWhatsAppQaCredentialPayload(payload: unknown): WhatsAppQaRuntimeEnv {
  const parsed = whatsappQaCredentialPayloadSchema.parse(payload);
  return validateWhatsAppQaRuntimeEnv(parsed, "WhatsApp credential payload");
}

function shouldRunWhatsAppScenarioByDefault(
  scenario: WhatsAppQaScenarioDefinition,
  providerMode: QaProviderMode,
) {
  if (scenario.defaultEnabled === false) {
    return false;
  }
  if (scenario.standardId) {
    return true;
  }
  return Boolean(scenario.defaultProviderModes?.includes(providerMode));
}

function findScenarios(
  ids?: string[],
  providerMode: QaProviderMode = DEFAULT_QA_LIVE_PROVIDER_MODE,
) {
  const scenarios =
    ids && ids.length > 0
      ? WHATSAPP_QA_SCENARIOS
      : WHATSAPP_QA_SCENARIOS.filter((scenario) =>
          shouldRunWhatsAppScenarioByDefault(scenario, providerMode),
        );
  return selectLiveTransportScenarios({
    ids,
    laneLabel: "WhatsApp",
    scenarios,
  });
}

function buildWhatsAppQaConfig(
  baseCfg: OpenClawConfig,
  params: {
    allowFrom: string[];
    authDir: string;
    dmPolicy: "allowlist" | "disabled" | "open" | "pairing";
    groupJid?: string;
    overrides?: WhatsAppQaConfigOverrides;
    sutAccountId: string;
  },
): OpenClawConfig {
  const pluginAllow = uniqueStrings([...(baseCfg.plugins?.allow ?? []), "whatsapp"]);
  const approvalOverrides = params.overrides?.approvals;
  const groupPolicy = params.overrides?.groupPolicy ?? "open";
  const audioPreflightConfig = params.overrides?.audioPreflight
    ? {
        tools: {
          ...baseCfg.tools,
          media: {
            ...baseCfg.tools?.media,
            audio: {
              ...baseCfg.tools?.media?.audio,
              enabled: true,
              models: [
                {
                  provider: "openai",
                  model: "gpt-4o-transcribe",
                },
              ],
            },
          },
        },
      }
    : {};
  const approvalForwardingConfig =
    approvalOverrides?.exec || approvalOverrides?.plugin
      ? {
          approvals: {
            ...baseCfg.approvals,
            ...(approvalOverrides.exec
              ? {
                  exec: {
                    ...baseCfg.approvals?.exec,
                    enabled: true,
                    mode: "session" as const,
                  },
                }
              : {}),
            ...(approvalOverrides.plugin
              ? {
                  plugin: {
                    ...baseCfg.approvals?.plugin,
                    enabled: true,
                    mode: "session" as const,
                  },
                }
              : {}),
          },
        }
      : {};
  return {
    ...baseCfg,
    ...approvalForwardingConfig,
    ...audioPreflightConfig,
    plugins: {
      ...baseCfg.plugins,
      allow: pluginAllow,
      entries: {
        ...baseCfg.plugins?.entries,
        whatsapp: { enabled: true },
      },
    },
    channels: {
      ...baseCfg.channels,
      whatsapp: {
        enabled: true,
        defaultAccount: params.sutAccountId,
        ...(params.overrides?.actions
          ? {
              actions: {
                reactions: true,
                polls: true,
              },
              reactionLevel: "minimal" as const,
            }
          : {}),
        accounts: {
          [params.sutAccountId]: {
            enabled: true,
            authDir: params.authDir,
            dmPolicy: params.dmPolicy,
            allowFrom: params.allowFrom,
            ...(params.overrides?.replyToMode
              ? {
                  replyToMode: params.overrides.replyToMode,
                }
              : {}),
            ...(params.groupJid
              ? {
                  groupPolicy,
                  ...(groupPolicy === "open"
                    ? {
                        groups: {
                          [params.groupJid]: { requireMention: true },
                        },
                      }
                    : {}),
                }
              : {}),
          },
        },
      },
    },
    ...(params.groupJid || params.overrides?.statusReactions
      ? {
          messages: {
            ...baseCfg.messages,
            ...(params.groupJid
              ? {
                  groupChat: {
                    ...baseCfg.messages?.groupChat,
                    visibleReplies: "automatic",
                    mentionPatterns: [
                      ...new Set([
                        ...(baseCfg.messages?.groupChat?.mentionPatterns ?? []),
                        "\\bopenclawqa\\b",
                      ]),
                    ],
                  },
                }
              : {}),
            ...(params.overrides?.statusReactions
              ? {
                  statusReactions: {
                    ...baseCfg.messages?.statusReactions,
                    enabled: true,
                  },
                }
              : {}),
          },
        }
      : {}),
  };
}

type WhatsAppChannelStatus = {
  connected?: boolean;
  lastConnectedAt?: number;
  lastDisconnect?: unknown;
  lastError?: string;
  restartPending?: boolean;
  running?: boolean;
};

async function waitForWhatsAppChannelRunning(
  gateway: WhatsAppQaGateway,
  accountId: string,
): Promise<WhatsAppChannelStatus> {
  const startedAt = Date.now();
  let lastStatus: WhatsAppChannelStatus | undefined;
  while (Date.now() - startedAt < WHATSAPP_QA_READY_TIMEOUT_MS) {
    try {
      const payload = (await gateway.call(
        "channels.status",
        { probe: false, timeoutMs: 2_000 },
        { timeoutMs: 5_000 },
      )) as {
        channelAccounts?: Record<
          string,
          Array<{
            accountId?: string;
            connected?: boolean;
            lastConnectedAt?: number;
            lastDisconnect?: unknown;
            lastError?: string;
            restartPending?: boolean;
            running?: boolean;
          }>
        >;
      };
      const accounts = payload.channelAccounts?.whatsapp ?? [];
      const match = accounts.find((entry) => entry.accountId === accountId);
      lastStatus = match
        ? {
            connected: match.connected,
            lastConnectedAt: match.lastConnectedAt,
            lastDisconnect: match.lastDisconnect,
            lastError: match.lastError,
            restartPending: match.restartPending,
            running: match.running,
          }
        : undefined;
      if (match?.running && match.connected === true && match.restartPending !== true) {
        if (!lastStatus) {
          throw new Error(
            `whatsapp account "${accountId}" status disappeared after readiness check`,
          );
        }
        return lastStatus;
      }
    } catch {
      // retry
    }
    await new Promise((resolve) => {
      setTimeout(resolve, 750);
    });
  }
  throw new Error(
    `whatsapp account "${accountId}" did not become ready` +
      (lastStatus ? `; last status: ${JSON.stringify(lastStatus)}` : ""),
  );
}

async function waitForWhatsAppChannelStable(gateway: WhatsAppQaGateway, accountId: string) {
  const startedAt = Date.now();
  while (Date.now() - startedAt < WHATSAPP_QA_READY_TIMEOUT_MS) {
    const status = await waitForWhatsAppChannelRunning(gateway, accountId);
    const connectedAt =
      typeof status.lastConnectedAt === "number" && status.lastConnectedAt > 0
        ? status.lastConnectedAt
        : Date.now();
    const connectedForMs = Date.now() - connectedAt;
    if (connectedForMs >= WHATSAPP_QA_READY_STABILITY_MS) {
      return;
    }
    await new Promise((resolve) => {
      setTimeout(resolve, Math.max(750, WHATSAPP_QA_READY_STABILITY_MS - connectedForMs));
    });
  }
  throw new Error(
    `whatsapp account "${accountId}" did not remain ready for ${WHATSAPP_QA_READY_STABILITY_MS}ms`,
  );
}

async function listTarEntries(archivePath: string): Promise<string[]> {
  const { stdout } = await execFileAsync("tar", ["-tzf", archivePath], {
    maxBuffer: 1024 * 1024,
  });
  return normalizeStringEntries(stdout.split("\n"));
}

function assertSafeArchiveEntries(entries: string[]) {
  if (entries.length === 0) {
    throw new Error("WhatsApp auth archive is empty.");
  }
  for (const entry of entries) {
    if (path.isAbsolute(entry) || entry.split(/[\\/]/u).includes("..")) {
      throw new Error(`WhatsApp auth archive contains unsafe entry "${entry}".`);
    }
  }
}

export async function unpackWhatsAppAuthArchive(params: {
  archiveBase64: string;
  label: string;
  parentDir: string;
}): Promise<string> {
  const authDir = path.join(params.parentDir, params.label);
  await fs.mkdir(authDir, { recursive: true, mode: 0o700 });
  const archivePath = path.join(params.parentDir, `${params.label}.tgz`);
  await fs.writeFile(archivePath, Buffer.from(params.archiveBase64, "base64"), { mode: 0o600 });
  const entries = await listTarEntries(archivePath);
  assertSafeArchiveEntries(entries);
  await execFileAsync("tar", ["-xzf", archivePath, "-C", authDir], { maxBuffer: 1024 * 1024 });
  await fs.rm(archivePath, { force: true });
  return authDir;
}

function messageMatches(message: WhatsAppObservedMessage, matchText: string | RegExp) {
  return typeof matchText === "string"
    ? message.text.includes(matchText)
    : matchText.test(message.text);
}

function buildWhatsAppQaIdempotencyKey(scenarioId: WhatsAppQaScenarioId, label: string) {
  return `${scenarioId}:${label}:${randomUUID()}`;
}

async function writeWhatsAppQaWorkspaceFixture(
  context: WhatsAppQaMessageScenarioContext,
  params: {
    buffer: Buffer;
    fileName: string;
  },
) {
  const fixtureDir = path.join(context.gatewayWorkspaceDir, ".openclaw", "qa-whatsapp-media");
  await fs.mkdir(fixtureDir, { recursive: true });
  const filePath = path.join(fixtureDir, params.fileName);
  await fs.writeFile(filePath, params.buffer);
  return filePath;
}

async function callWhatsAppGatewaySend(
  context: WhatsAppQaMessageScenarioContext,
  params: {
    asVoice?: boolean;
    forceDocument?: boolean;
    label: string;
    mediaUrl?: string;
    mediaUrls?: string[];
    message?: string;
    replyToId?: string;
  },
) {
  return await context.gateway.call(
    "send",
    {
      accountId: context.sutAccountId,
      agentId: "main",
      channel: "whatsapp",
      idempotencyKey: buildWhatsAppQaIdempotencyKey(context.scenarioId, params.label),
      to: context.target,
      ...(params.message !== undefined ? { message: params.message } : {}),
      ...(params.mediaUrl ? { mediaUrl: params.mediaUrl } : {}),
      ...(params.mediaUrls ? { mediaUrls: params.mediaUrls } : {}),
      ...(params.asVoice !== undefined ? { asVoice: params.asVoice } : {}),
      ...(params.forceDocument !== undefined ? { forceDocument: params.forceDocument } : {}),
      ...(params.replyToId ? { replyToId: params.replyToId } : {}),
    },
    { timeoutMs: 60_000 },
  );
}

async function callWhatsAppGatewayPoll(
  context: WhatsAppQaMessageScenarioContext,
  params: {
    label: string;
    maxSelections?: number;
    options: string[];
    question: string;
  },
) {
  return await context.gateway.call(
    "poll",
    {
      accountId: context.sutAccountId,
      channel: "whatsapp",
      idempotencyKey: buildWhatsAppQaIdempotencyKey(context.scenarioId, params.label),
      maxSelections: params.maxSelections,
      options: params.options,
      question: params.question,
      to: context.target,
    },
    { timeoutMs: 60_000 },
  );
}

async function callWhatsAppGatewayMessageAction(
  context: WhatsAppQaMessageScenarioContext,
  params: {
    action: "react" | "upload-file";
    label: string;
    params: Record<string, unknown>;
  },
) {
  return await context.gateway.call(
    "message.action",
    {
      accountId: context.sutAccountId,
      action: params.action,
      channel: "whatsapp",
      idempotencyKey: buildWhatsAppQaIdempotencyKey(context.scenarioId, params.label),
      params: params.params,
    },
    { timeoutMs: 60_000 },
  );
}

async function waitForScenarioObservedMessage(
  context: WhatsAppQaMessageScenarioContext,
  params: {
    match: (message: WhatsAppQaDriverObservedMessage) => boolean;
    observedAfter?: Date;
    timeoutMs?: number;
  },
) {
  const message = await context.driver.waitForMessage({
    timeoutMs: params.timeoutMs ?? 45_000,
    match: (candidate) =>
      candidate.fromPhoneE164 === context.sutPhoneE164 &&
      (!params.observedAfter ||
        new Date(candidate.observedAt).getTime() >= params.observedAfter.getTime()) &&
      params.match(candidate),
  });
  context.recordObservedMessage(message);
  return message;
}

function isTransientWhatsAppQaDriverError(error: unknown) {
  const message = formatErrorMessage(error);
  return (
    /\bConnection Closed\b/iu.test(message) ||
    /\bconflict\b/iu.test(message) ||
    /\bsession conflict\b/iu.test(message) ||
    /\btimed out waiting for WhatsApp QA driver message\b/iu.test(message)
  );
}

async function restartWhatsAppQaDriverSession(params: {
  authDir: string;
  current: WhatsAppQaDriverSession;
}) {
  await params.current.close().catch(() => {});
  return await startWhatsAppQaDriverSession({ authDir: params.authDir });
}

async function startWhatsAppQaDriverSessionWithRetry(params: { authDir: string }) {
  for (const attempt of Array.from(
    { length: WHATSAPP_QA_TRANSIENT_DRIVER_ATTEMPTS },
    (_, index) => index + 1,
  )) {
    try {
      return await startWhatsAppQaDriverSession({ authDir: params.authDir });
    } catch (error) {
      if (
        attempt >= WHATSAPP_QA_TRANSIENT_DRIVER_ATTEMPTS ||
        !isTransientWhatsAppQaDriverError(error)
      ) {
        throw error;
      }
      await new Promise((resolve) => {
        setTimeout(resolve, WHATSAPP_QA_DRIVER_RECONNECT_DELAY_MS);
      });
    }
  }
  throw new Error("unreachable WhatsApp QA driver retry loop exit");
}

function formatApprovalResultValue(value: unknown) {
  if (typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
    return String(value);
  }
  if (value == null) {
    return "<missing>";
  }
  return JSON.stringify(value) ?? "<unserializable>";
}

function readAcceptedApprovalRequest(result: unknown) {
  const accepted =
    typeof result === "object" && result !== null
      ? (result as { id?: unknown; status?: unknown })
      : null;
  if (accepted?.status !== "accepted") {
    throw new Error(
      `approval request status was ${formatApprovalResultValue(
        accepted?.status,
      )} instead of accepted`,
    );
  }
  return accepted;
}

function readAcceptedApprovalRequestId(result: unknown) {
  const id = readAcceptedApprovalRequest(result).id;
  if (typeof id !== "string" || id.trim().length === 0) {
    throw new Error(`approval request id was ${formatApprovalResultValue(id)}`);
  }
  return id;
}

async function requestWhatsAppApproval(params: {
  approvalId: string;
  driverPhoneE164: string;
  gateway: WhatsAppQaGateway;
  run: WhatsAppQaApprovalScenarioRun;
  sutAccountId: string;
}) {
  const commonParams = {
    timeoutMs: WHATSAPP_QA_APPROVAL_DECISION_TIMEOUT_MS,
    turnSourceAccountId: params.sutAccountId,
    turnSourceChannel: "whatsapp",
    turnSourceTo: params.driverPhoneE164,
    twoPhase: true,
  };
  if (params.run.approvalKind === "exec") {
    const result = await params.gateway.call(
      "exec.approval.request",
      {
        ...commonParams,
        ask: "always",
        command: `printf '%s\\n' '${params.run.token}'`,
        host: "gateway",
        id: params.approvalId,
        security: "full",
      },
      {
        expectFinal: false,
        timeoutMs: WHATSAPP_QA_APPROVAL_DECISION_TIMEOUT_MS + 5_000,
      },
    );
    const acceptedId = readAcceptedApprovalRequestId(result);
    if (acceptedId !== params.approvalId) {
      throw new Error(
        `accepted exec approval id was ${formatApprovalResultValue(
          acceptedId,
        )} instead of ${params.approvalId}`,
      );
    }
    return acceptedId;
  }
  const result = await params.gateway.call(
    "plugin.approval.request",
    {
      ...commonParams,
      agentId: "qa",
      description: `WhatsApp plugin approval QA request ${params.run.token}`,
      pluginId: "qa-whatsapp-plugin",
      severity: "warning",
      title: `WhatsApp plugin approval QA ${params.run.token}`,
      toolName: "whatsapp_qa_tool",
    },
    {
      expectFinal: false,
      timeoutMs: WHATSAPP_QA_APPROVAL_DECISION_TIMEOUT_MS + 5_000,
    },
  );
  return readAcceptedApprovalRequestId(result);
}

async function waitForApprovalDecision(params: {
  approvalId: string;
  gateway: WhatsAppQaGateway;
  kind: WhatsAppQaApprovalKind;
}) {
  const method =
    params.kind === "exec" ? "exec.approval.waitDecision" : "plugin.approval.waitDecision";
  return await params.gateway.call(
    method,
    { id: params.approvalId },
    {
      expectFinal: true,
      timeoutMs: WHATSAPP_QA_APPROVAL_DECISION_TIMEOUT_MS + 5_000,
    },
  );
}

async function resolveApprovalDecision(params: {
  approvalId: string;
  decision: WhatsAppQaApprovalDecision;
  gateway: WhatsAppQaGateway;
  kind: WhatsAppQaApprovalKind;
}) {
  const method = params.kind === "exec" ? "exec.approval.resolve" : "plugin.approval.resolve";
  return await params.gateway.call(
    method,
    { decision: params.decision, id: params.approvalId },
    {
      expectFinal: false,
      timeoutMs: WHATSAPP_QA_APPROVAL_DECISION_TIMEOUT_MS + 5_000,
    },
  );
}

function assertApprovalDecisionResult(params: {
  decision: WhatsAppQaApprovalDecision;
  result: unknown;
}) {
  const resultDecision =
    typeof params.result === "object" && params.result !== null
      ? (params.result as { decision?: unknown }).decision
      : undefined;
  if (resultDecision !== params.decision) {
    throw new Error(
      `approval decision was ${formatApprovalResultValue(resultDecision)} instead of ${params.decision}`,
    );
  }
}

function matchesWhatsAppApprovalPendingText(params: {
  approvalId: string;
  approvalKind: WhatsAppQaApprovalKind;
  text: string;
  token: string;
}) {
  const heading =
    params.approvalKind === "exec" ? "Exec approval required" : "Plugin approval required";
  return (
    params.text.includes(heading) &&
    params.text.includes(params.approvalId) &&
    params.text.includes(params.token) &&
    params.text.includes("React with:") &&
    params.text.includes("👍")
  );
}

function matchesWhatsAppApprovalResolvedText(params: {
  approvalId: string;
  approvalKind: WhatsAppQaApprovalKind;
  decision?: WhatsAppQaApprovalDecision;
  text: string;
}) {
  const heading =
    params.decision === "deny"
      ? params.approvalKind === "exec"
        ? "Exec approval denied"
        : "Plugin approval denied"
      : params.approvalKind === "exec"
        ? "Exec approval allow-once"
        : "Plugin approval allowed once";
  return params.text.includes(params.approvalId) && params.text.includes(heading);
}

async function waitForWhatsAppApprovalMessage(params: {
  approvalId: string;
  approvalKind: WhatsAppQaApprovalKind;
  decision?: WhatsAppQaApprovalDecision;
  driver: WhatsAppQaDriverSession;
  observedMessages: WhatsAppObservedMessage[];
  scenario: WhatsAppQaScenarioDefinition;
  state: "pending" | "resolved";
  sutPhoneE164: string;
  timeoutMs: number;
  token: string;
}) {
  const reply = await params.driver.waitForMessage({
    timeoutMs: params.timeoutMs,
    match: (message) => {
      const fromExpectedSender =
        !message.fromPhoneE164 || message.fromPhoneE164 === params.sutPhoneE164;
      return (
        fromExpectedSender &&
        (params.state === "pending"
          ? matchesWhatsAppApprovalPendingText({
              approvalId: params.approvalId,
              approvalKind: params.approvalKind,
              text: message.text,
              token: params.token,
            })
          : matchesWhatsAppApprovalResolvedText({
              approvalId: params.approvalId,
              approvalKind: params.approvalKind,
              decision: params.decision,
              text: message.text,
            }))
      );
    },
  });
  const observed: WhatsAppObservedMessage = {
    ...reply,
    approvalState: params.state,
    matchedScenario: true,
    scenarioId: params.scenario.id,
    scenarioTitle: params.scenario.title,
  };
  params.observedMessages.push(observed);
  return observed;
}

async function runWhatsAppApprovalScenario(params: {
  driver: WhatsAppQaDriverSession;
  driverPhoneE164: string;
  gateway: WhatsAppQaGateway;
  observedMessages: WhatsAppObservedMessage[];
  run: WhatsAppQaApprovalScenarioRun;
  scenario: WhatsAppQaScenarioDefinition;
  sutAccountId: string;
  sutPhoneE164: string;
}) {
  const requestStartedAt = new Date();
  const requestedApprovalId =
    params.run.approvalKind === "exec"
      ? `whatsapp-qa-exec-${randomUUID()}`
      : `whatsapp-qa-plugin-${randomUUID()}`;
  const approvalId = await requestWhatsAppApproval({
    approvalId: requestedApprovalId,
    driverPhoneE164: params.driverPhoneE164,
    gateway: params.gateway,
    run: params.run,
    sutAccountId: params.sutAccountId,
  });
  const pending = await waitForWhatsAppApprovalMessage({
    approvalId,
    approvalKind: params.run.approvalKind,
    decision: params.run.decision,
    driver: params.driver,
    observedMessages: params.observedMessages,
    scenario: params.scenario,
    state: "pending",
    sutPhoneE164: params.sutPhoneE164,
    timeoutMs: params.scenario.timeoutMs,
    token: params.run.token,
  });
  const resolvedPromise = waitForWhatsAppApprovalMessage({
    approvalId,
    approvalKind: params.run.approvalKind,
    decision: params.run.decision,
    driver: params.driver,
    observedMessages: params.observedMessages,
    scenario: params.scenario,
    state: "resolved",
    sutPhoneE164: params.sutPhoneE164,
    timeoutMs: params.scenario.timeoutMs,
    token: params.run.token,
  });
  try {
    if (params.run.decisionMode === "reaction") {
      if (!pending.fromJid || !pending.messageId) {
        throw new Error("WhatsApp approval prompt did not expose message coordinates.");
      }
      await params.driver.sendReaction(pending.fromJid, pending.messageId, "👍", {
        fromMe: false,
      });
    } else {
      await resolveApprovalDecision({
        approvalId,
        decision: params.run.decision,
        gateway: params.gateway,
        kind: params.run.approvalKind,
      });
    }
    assertApprovalDecisionResult({
      decision: params.run.decision,
      result: await waitForApprovalDecision({
        approvalId,
        gateway: params.gateway,
        kind: params.run.approvalKind,
      }),
    });
  } catch (error) {
    resolvedPromise.catch(() => {});
    throw error;
  }
  const resolved = await resolvedPromise;
  const responseObservedAt = new Date(resolved.observedAt);
  return {
    approvalId,
    requestStartedAt,
    responseObservedAt,
    rttMs: responseObservedAt.getTime() - requestStartedAt.getTime(),
  };
}

async function runWhatsAppScenario(params: {
  driver: WhatsAppQaDriverSession;
  driverPhoneE164: string;
  gatewayDebugDirPath: string;
  observedMessages: WhatsAppObservedMessage[];
  providerMode: ReturnType<typeof normalizeQaProviderMode>;
  primaryModel: string;
  alternateModel: string;
  fastMode?: boolean;
  repoRoot: string;
  scenario: WhatsAppQaScenarioDefinition;
  sutAccountId: string;
  sutAuthDir: string;
  sutPhoneE164: string;
  groupJid?: string;
}): Promise<WhatsAppQaScenarioResult> {
  const scenarioRun = params.scenario.buildRun();
  if (scenarioRun.kind !== "approval" && scenarioRun.target === "group" && !params.groupJid) {
    throw new Error(`WhatsApp scenario ${params.scenario.id} requires groupJid.`);
  }
  const target =
    scenarioRun.kind !== "approval" && scenarioRun.target === "group"
      ? params.groupJid!
      : params.sutPhoneE164;
  const allowFrom =
    scenarioRun.kind === "approval"
      ? [params.driverPhoneE164]
      : scenarioRun.configMode === "open"
        ? ["*"]
        : scenarioRun.configMode === "pairing"
          ? ["+15550000000"]
          : [params.driverPhoneE164];
  const dmPolicy =
    scenarioRun.kind === "approval"
      ? "allowlist"
      : scenarioRun.configMode === "open" || scenarioRun.configMode === "disabled"
        ? scenarioRun.configMode
        : scenarioRun.configMode === "allowlist"
          ? "allowlist"
          : "pairing";
  const gatewayHarness = await startQaLiveLaneGateway({
    repoRoot: params.repoRoot,
    transport: {
      requiredPluginIds: params.scenario.requiredPluginIds ?? [],
      createGatewayConfig: () => ({}),
    },
    transportBaseUrl: "http://127.0.0.1:0",
    command: {
      executablePath: process.execPath,
      argsPrefix: [path.join(params.repoRoot, "dist", "index.js")],
      argsSuffix: ["--verbose"],
    },
    providerMode: params.providerMode,
    primaryModel: params.primaryModel,
    alternateModel: params.alternateModel,
    fastMode: params.fastMode,
    controlUiEnabled: false,
    mutateConfig: (cfg) =>
      buildWhatsAppQaConfig(cfg, {
        allowFrom,
        authDir: params.sutAuthDir,
        dmPolicy,
        groupJid:
          scenarioRun.kind !== "approval" && scenarioRun.target === "group"
            ? params.groupJid
            : undefined,
        overrides: params.scenario.configOverrides,
        sutAccountId: params.sutAccountId,
      }),
  });
  let preservedGatewayDebug = false;
  try {
    await waitForWhatsAppChannelStable(gatewayHarness.gateway, params.sutAccountId);
    if (scenarioRun.kind === "approval") {
      const approval = await runWhatsAppApprovalScenario({
        driver: params.driver,
        driverPhoneE164: params.driverPhoneE164,
        gateway: gatewayHarness.gateway,
        observedMessages: params.observedMessages,
        run: scenarioRun,
        scenario: params.scenario,
        sutAccountId: params.sutAccountId,
        sutPhoneE164: params.sutPhoneE164,
      });
      return {
        id: params.scenario.id,
        title: params.scenario.title,
        status: "pass" as const,
        details: `${scenarioRun.approvalKind} approval ${approval.approvalId} resolved ${scenarioRun.decision} in ${approval.rttMs}ms`,
        rttMs: approval.rttMs,
        requestStartedAt: approval.requestStartedAt.toISOString(),
        responseObservedAt: approval.responseObservedAt.toISOString(),
        rttMeasurement: {
          finalMatchedReplyRttMs: approval.rttMs,
          requestStartedAt: approval.requestStartedAt.toISOString(),
          responseObservedAt: approval.responseObservedAt.toISOString(),
          source: "approval-request-to-resolution" as const,
        },
      };
    }
    if (scenarioRun.quietInput) {
      const quietStartedAt = new Date();
      if (scenarioRun.sendMode?.kind === "media") {
        await params.driver.sendMedia(
          target,
          scenarioRun.quietInput,
          scenarioRun.sendMode.mediaBuffer,
          scenarioRun.sendMode.mediaType,
          {
            fileName: scenarioRun.sendMode.fileName,
          },
        );
      } else {
        await params.driver.sendText(target, scenarioRun.quietInput);
      }
      await new Promise((resolve) => {
        setTimeout(resolve, scenarioRun.quietWindowMs ?? 5_000);
      });
      const unexpectedReply = params.driver.getObservedMessages().find((message) => {
        if (new Date(message.observedAt).getTime() < quietStartedAt.getTime()) {
          return false;
        }
        if (scenarioRun.target === "group" && message.fromJid !== params.groupJid) {
          return false;
        }
        return scenarioRun.quietMatchText
          ? messageMatches(message as WhatsAppObservedMessage, scenarioRun.quietMatchText)
          : true;
      });
      if (unexpectedReply) {
        throw new Error("unexpected WhatsApp group reply before mention gate was triggered");
      }
    }
    const requestStartedAt = new Date();
    const sent =
      scenarioRun.sendMode?.kind === "media"
        ? await params.driver.sendMedia(
            target,
            scenarioRun.input,
            scenarioRun.sendMode.mediaBuffer,
            scenarioRun.sendMode.mediaType,
            {
              fileName: scenarioRun.sendMode.fileName,
            },
          )
        : await params.driver.sendText(target, scenarioRun.input);
    const scenarioContext: WhatsAppQaMessageScenarioContext = {
      driver: params.driver,
      gateway: gatewayHarness.gateway,
      gatewayWorkspaceDir: gatewayHarness.gateway.workspaceDir,
      recordObservedMessage: (message) => {
        params.observedMessages.push({
          ...message,
          matchedScenario: true,
          scenarioId: params.scenario.id,
          scenarioTitle: params.scenario.title,
        });
      },
      scenarioId: params.scenario.id,
      scenarioTitle: params.scenario.title,
      sent,
      sutAccountId: params.sutAccountId,
      sutPhoneE164: params.sutPhoneE164,
      target,
      waitForReady: async () => {
        await waitForWhatsAppChannelStable(gatewayHarness.gateway, params.sutAccountId);
      },
    };
    const afterSendDetails = await scenarioRun.afterSend?.(scenarioContext);
    if (!scenarioRun.expectReply) {
      await new Promise((resolve) => {
        setTimeout(resolve, params.scenario.timeoutMs);
      });
      const unexpectedReply = params.driver.getObservedMessages().find((message) => {
        if (new Date(message.observedAt).getTime() < requestStartedAt.getTime()) {
          return false;
        }
        if (scenarioRun.target === "group" && message.fromJid !== params.groupJid) {
          return false;
        }
        if (scenarioRun.target === "dm" && message.fromPhoneE164 !== params.sutPhoneE164) {
          return false;
        }
        return messageMatches(message as WhatsAppObservedMessage, scenarioRun.matchText);
      });
      if (unexpectedReply) {
        throw new Error("unexpected WhatsApp reply matched a quiet scenario marker");
      }
      return {
        id: params.scenario.id,
        title: params.scenario.title,
        status: "pass" as const,
        details: "no reply",
      };
    }
    const reply = await params.driver.waitForMessage({
      timeoutMs: params.scenario.timeoutMs,
      match: (message) =>
        (scenarioRun.target === "group"
          ? message.fromJid === params.groupJid
          : message.fromPhoneE164 === params.sutPhoneE164) &&
        messageMatches(message as WhatsAppObservedMessage, scenarioRun.matchText),
    });
    const observed: WhatsAppObservedMessage = {
      ...reply,
      matchedScenario: true,
      scenarioId: params.scenario.id,
      scenarioTitle: params.scenario.title,
    };
    scenarioRun.verify?.(reply, scenarioContext);
    params.observedMessages.push(observed);
    const afterReplyDetails = await scenarioRun.afterReply?.(reply, scenarioContext);
    const responseObservedAt = new Date(reply.observedAt);
    const rttMs = responseObservedAt.getTime() - requestStartedAt.getTime();
    return {
      id: params.scenario.id,
      title: params.scenario.title,
      status: "pass" as const,
      details: [`reply matched in ${rttMs}ms`, afterSendDetails, afterReplyDetails]
        .filter(Boolean)
        .join("; "),
      rttMs,
      requestStartedAt: requestStartedAt.toISOString(),
      responseObservedAt: responseObservedAt.toISOString(),
      rttMeasurement: {
        finalMatchedReplyRttMs: rttMs,
        requestStartedAt: requestStartedAt.toISOString(),
        responseObservedAt: responseObservedAt.toISOString(),
        source: "request-to-observed-message" as const,
      },
    };
  } catch (error) {
    preservedGatewayDebug = true;
    await gatewayHarness.gateway
      .stop({ preserveToDir: params.gatewayDebugDirPath })
      .catch(() => {});
    throw error;
  } finally {
    if (!preservedGatewayDebug) {
      await gatewayHarness.stop().catch(() => {});
    }
  }
}

function toObservedWhatsAppArtifacts(params: {
  includeContent: boolean;
  messages: WhatsAppObservedMessage[];
  redactMetadata: boolean;
}): WhatsAppObservedMessageArtifact[] {
  return params.messages.map((message) => ({
    approvalState: message.approvalState,
    fromPhoneE164: params.redactMetadata ? undefined : message.fromPhoneE164,
    hasMedia: message.hasMedia,
    kind: message.kind,
    matchedScenario: message.matchedScenario,
    mediaFileName: params.redactMetadata ? undefined : message.mediaFileName,
    mediaType: message.mediaType,
    messageId: params.redactMetadata ? undefined : message.messageId,
    observedAt: message.observedAt,
    poll: params.includeContent ? message.poll : undefined,
    quoted:
      message.quoted && !params.redactMetadata
        ? message.quoted
        : message.quoted
          ? { text: params.includeContent ? message.quoted.text : undefined }
          : undefined,
    reaction: message.reaction
      ? {
          emoji: message.reaction.emoji,
          fromMe: message.reaction.fromMe,
          messageId: params.redactMetadata ? undefined : message.reaction.messageId,
        }
      : undefined,
    scenarioId: message.scenarioId,
    scenarioTitle: message.scenarioTitle,
    text: params.includeContent ? message.text : undefined,
  }));
}

function renderWhatsAppQaMarkdown(params: {
  cleanupIssues: string[];
  credentialFingerprint?: string;
  credentialSource: "convex" | "env";
  finishedAt: string;
  gatewayDebugDirPath?: string;
  redactMetadata: boolean;
  scenarios: WhatsAppQaScenarioResult[];
  startedAt: string;
  sutPhoneE164?: string;
}) {
  const lines = [
    "# WhatsApp QA Report",
    "",
    `- Credential source: \`${params.credentialSource}\``,
    ...(params.credentialFingerprint
      ? [`- Credential fingerprint: \`${params.credentialFingerprint}\``]
      : []),
    `- SUT phone: \`${params.redactMetadata ? "<redacted>" : (params.sutPhoneE164 ?? "<unavailable>")}\``,
    `- Metadata redaction: \`${params.redactMetadata ? "enabled" : "disabled"}\``,
    `- Started: ${params.startedAt}`,
    `- Finished: ${params.finishedAt}`,
  ];
  if (params.gatewayDebugDirPath) {
    lines.push(`- Gateway debug artifacts: \`${params.gatewayDebugDirPath}\``);
  }
  if (params.cleanupIssues.length > 0) {
    lines.push("", "## Cleanup issues", "");
    for (const issue of params.cleanupIssues) {
      lines.push(`- ${issue}`);
    }
  }
  lines.push("", "## Scenarios", "");
  for (const scenario of params.scenarios) {
    lines.push(`### ${scenario.title}`, "");
    lines.push(`- Status: ${scenario.status}`);
    lines.push(`- Details: ${scenario.details}`);
    if (scenario.rttMs !== undefined) {
      lines.push(`- RTT: ${scenario.rttMs}ms`);
    }
    lines.push("");
  }
  return lines.join("\n");
}

function createMissingGroupJidScenarioResult(params: {
  explicitScenarioSelection: boolean;
  scenario: WhatsAppQaScenarioDefinition;
}): WhatsAppQaScenarioResult {
  return {
    id: params.scenario.id,
    title: params.scenario.title,
    status: params.explicitScenarioSelection ? "fail" : "skip",
    details: params.explicitScenarioSelection
      ? "requested scenario requires groupJid in the WhatsApp QA credential payload"
      : "requires groupJid in the WhatsApp QA credential payload",
  };
}

function appendPreScenarioFailureResults(params: {
  details: string;
  scenarioResults: WhatsAppQaScenarioResult[];
  scenarios: WhatsAppQaScenarioDefinition[];
}) {
  const recordedScenarioIds = new Set(params.scenarioResults.map((result) => result.id));
  const pendingScenarios = params.scenarios.filter(
    (scenario) => !recordedScenarioIds.has(scenario.id),
  );
  const failedScenarios =
    pendingScenarios.length > 0 ? pendingScenarios : params.scenarios.slice(0, 1);
  for (const scenario of failedScenarios) {
    params.scenarioResults.push({
      id: scenario.id,
      title: scenario.title,
      status: "fail",
      details: params.details,
    });
  }
}

export async function runWhatsAppQaLive(params: {
  alternateModel?: string;
  credentialRole?: string;
  credentialSource?: string;
  fastMode?: boolean;
  outputDir?: string;
  primaryModel?: string;
  providerMode?: QaProviderModeInput;
  repoRoot?: string;
  scenarioIds?: string[];
  sutAccountId?: string;
}): Promise<WhatsAppQaRunResult> {
  const repoRoot = path.resolve(params.repoRoot ?? process.cwd());
  const outputDir =
    params.outputDir ??
    path.join(repoRoot, ".artifacts", "qa-e2e", `whatsapp-${Date.now().toString(36)}`);
  await fs.mkdir(outputDir, { recursive: true });

  const providerMode = normalizeQaProviderMode(
    params.providerMode ?? DEFAULT_QA_LIVE_PROVIDER_MODE,
  );
  const primaryModel = params.primaryModel?.trim() || defaultQaModelForMode(providerMode);
  const alternateModel = params.alternateModel?.trim() || defaultQaModelForMode(providerMode, true);
  const sutAccountId = params.sutAccountId?.trim() || "sut";
  const scenarios = findScenarios(params.scenarioIds, providerMode);
  const explicitScenarioSelection = (params.scenarioIds?.length ?? 0) > 0;
  const requestedCredentialSource = inferWhatsAppCredentialSource(params.credentialSource);
  const requestedCredentialRole = inferWhatsAppCredentialRole(params.credentialRole);
  const redactPublicMetadata = resolveWhatsAppMetadataRedaction();
  const includeObservedMessageContent = isTruthyOptIn(process.env[WHATSAPP_QA_CAPTURE_CONTENT_ENV]);
  const startedAt = new Date().toISOString();
  const observedMessages: WhatsAppObservedMessage[] = [];
  const scenarioResults: WhatsAppQaScenarioResult[] = [];
  const cleanupIssues: string[] = [];
  const gatewayDebugDirPath = path.join(outputDir, "gateway-debug");
  let preservedGatewayDebugArtifacts = false;
  let credentialLease: WhatsAppCredentialLease | undefined;
  let leaseHeartbeat: WhatsAppCredentialHeartbeat | undefined;
  let runtimeEnv: WhatsAppQaRuntimeEnv | undefined;
  let tempAuthRoot: string | undefined;
  let driver: WhatsAppQaDriverSession | undefined;

  try {
    credentialLease = await acquireQaCredentialLease({
      kind: "whatsapp",
      source: params.credentialSource,
      role: params.credentialRole,
      resolveEnvPayload: () => resolveWhatsAppQaRuntimeEnv(),
      parsePayload: parseWhatsAppQaCredentialPayload,
    });
    leaseHeartbeat = startQaCredentialLeaseHeartbeat(credentialLease);
    const assertLeaseHealthy = () => {
      leaseHeartbeat?.throwIfFailed();
    };
    runtimeEnv = credentialLease.payload;
    tempAuthRoot = await fs.mkdtemp(
      path.join(resolvePreferredOpenClawTmpDir(), "openclaw-whatsapp-qa-"),
    );
    const [driverAuthDir, sutAuthDir] = await Promise.all([
      unpackWhatsAppAuthArchive({
        archiveBase64: runtimeEnv.driverAuthArchiveBase64,
        label: "driver-auth",
        parentDir: tempAuthRoot,
      }),
      unpackWhatsAppAuthArchive({
        archiveBase64: runtimeEnv.sutAuthArchiveBase64,
        label: "sut-auth",
        parentDir: tempAuthRoot,
      }),
    ]);
    let activeDriver = await startWhatsAppQaDriverSessionWithRetry({ authDir: driverAuthDir });
    driver = activeDriver;

    for (const scenario of scenarios) {
      assertLeaseHealthy();
      if (scenario.requiresGroupJid && !runtimeEnv.groupJid) {
        scenarioResults.push(
          createMissingGroupJidScenarioResult({
            explicitScenarioSelection,
            scenario,
          }),
        );
        continue;
      }
      let driverAttempt = 1;
      while (true) {
        try {
          const result = await runWhatsAppScenario({
            driver: activeDriver,
            driverPhoneE164: runtimeEnv.driverPhoneE164,
            gatewayDebugDirPath,
            observedMessages,
            providerMode,
            primaryModel,
            alternateModel,
            fastMode: params.fastMode,
            groupJid: runtimeEnv.groupJid,
            repoRoot,
            scenario,
            sutAccountId,
            sutAuthDir,
            sutPhoneE164: runtimeEnv.sutPhoneE164,
          });
          scenarioResults.push(
            driverAttempt > 1
              ? {
                  ...result,
                  details: `${result.details}; driver reconnected ${driverAttempt - 1}x`,
                }
              : result,
          );
          break;
        } catch (error) {
          if (
            driverAttempt < WHATSAPP_QA_TRANSIENT_DRIVER_ATTEMPTS &&
            isTransientWhatsAppQaDriverError(error)
          ) {
            driverAttempt += 1;
            await new Promise((resolve) => {
              setTimeout(resolve, WHATSAPP_QA_DRIVER_RECONNECT_DELAY_MS);
            });
            try {
              activeDriver = await restartWhatsAppQaDriverSession({
                authDir: driverAuthDir,
                current: activeDriver,
              });
              driver = activeDriver;
            } catch (restartError) {
              if (!isTransientWhatsAppQaDriverError(restartError)) {
                throw restartError;
              }
            }
            continue;
          }
          preservedGatewayDebugArtifacts = true;
          scenarioResults.push({
            id: scenario.id,
            title: scenario.title,
            status: "fail",
            details:
              driverAttempt > 1
                ? `${formatErrorMessage(error)}; driver reconnected ${driverAttempt - 1}x`
                : formatErrorMessage(error),
          });
          break;
        }
      }
      if (scenarioResults.at(-1)?.status === "fail") {
        break;
      }
    }
  } catch (error) {
    cleanupIssues.push(
      buildLiveLaneArtifactsError({
        heading: "WhatsApp QA failed before scenario completion.",
        details: [formatErrorMessage(error)],
        artifacts: {
          gatewayDebug: gatewayDebugDirPath,
        },
      }),
    );
    preservedGatewayDebugArtifacts = true;
    await fs.mkdir(gatewayDebugDirPath, { recursive: true }).catch(() => {});
    appendPreScenarioFailureResults({
      details: formatErrorMessage(error),
      scenarioResults,
      scenarios,
    });
  } finally {
    if (driver) {
      try {
        await driver.close();
      } catch (error) {
        appendLiveLaneIssue(cleanupIssues, "driver session stop failed", error);
      }
    }
    if (leaseHeartbeat) {
      try {
        await leaseHeartbeat.stop();
      } catch (error) {
        appendLiveLaneIssue(cleanupIssues, "credential heartbeat stop failed", error);
      }
    }
    if (credentialLease) {
      try {
        await credentialLease.release();
      } catch (error) {
        appendLiveLaneIssue(cleanupIssues, "credential release failed", error);
      }
    }
    if (tempAuthRoot) {
      await fs.rm(tempAuthRoot, { recursive: true, force: true }).catch((error: unknown) => {
        appendLiveLaneIssue(cleanupIssues, "temporary auth cleanup failed", error);
      });
    }
  }

  const finishedAt = new Date().toISOString();
  const reportPath = path.join(outputDir, "whatsapp-qa-report.md");
  const summaryPath = path.join(outputDir, "whatsapp-qa-summary.json");
  const observedMessagesPath = path.join(outputDir, "whatsapp-qa-observed-messages.json");
  const passed = scenarioResults.filter((entry) => entry.status === "pass").length;
  const failed = scenarioResults.filter((entry) => entry.status === "fail").length;
  const skipped = scenarioResults.filter((entry) => entry.status === "skip").length;
  const credentialFingerprint = fingerprintQaCredentialId(credentialLease?.credentialId);
  const summary: WhatsAppQaSummary = {
    credentials: credentialLease
      ? {
          source: credentialLease.source,
          kind: credentialLease.kind,
          role: credentialLease.role,
          credentialFingerprint,
          credentialId: redactPublicMetadata ? undefined : credentialLease.credentialId,
          ownerId: redactPublicMetadata ? undefined : credentialLease.ownerId,
        }
      : {
          source: requestedCredentialSource,
          kind: "whatsapp",
          role: requestedCredentialRole,
        },
    sutAccountId,
    sutPhoneE164: redactPublicMetadata
      ? "<redacted>"
      : (runtimeEnv?.sutPhoneE164 ?? "<unavailable>"),
    startedAt,
    finishedAt,
    cleanupIssues,
    counts: {
      total: scenarioResults.length,
      passed,
      failed,
      skipped,
    },
    scenarios: scenarioResults,
  };
  await fs.writeFile(
    observedMessagesPath,
    `${JSON.stringify(
      toObservedWhatsAppArtifacts({
        messages: observedMessages,
        includeContent: includeObservedMessageContent,
        redactMetadata: redactPublicMetadata,
      }),
      null,
      2,
    )}\n`,
  );
  await fs.writeFile(summaryPath, `${JSON.stringify(summary, null, 2)}\n`);
  await fs.writeFile(
    reportPath,
    `${renderWhatsAppQaMarkdown({
      cleanupIssues,
      credentialFingerprint,
      credentialSource: credentialLease?.source ?? requestedCredentialSource,
      finishedAt,
      gatewayDebugDirPath: preservedGatewayDebugArtifacts ? gatewayDebugDirPath : undefined,
      redactMetadata: redactPublicMetadata,
      scenarios: scenarioResults,
      startedAt,
      sutPhoneE164: runtimeEnv?.sutPhoneE164,
    })}\n`,
  );
  return {
    outputDir,
    reportPath,
    summaryPath,
    observedMessagesPath,
    gatewayDebugDirPath: preservedGatewayDebugArtifacts ? gatewayDebugDirPath : undefined,
    scenarios: scenarioResults,
  };
}

export const testing = {
  assertSafeArchiveEntries,
  appendPreScenarioFailureResults,
  buildWhatsAppQaConfig,
  createMissingGroupJidScenarioResult,
  findScenarios,
  fingerprintWhatsAppCredentialId: fingerprintQaCredentialId,
  isTransientWhatsAppQaDriverError,
  matchesWhatsAppApprovalResolvedText,
  parseWhatsAppQaCredentialPayload,
  renderWhatsAppQaMarkdown,
  resolveWhatsAppQaRuntimeEnv,
  resolveWhatsAppMetadataRedaction,
  toObservedWhatsAppArtifacts,
  unpackWhatsAppAuthArchive,
  WHATSAPP_QA_STANDARD_SCENARIO_IDS,
};
export { testing as __testing };
