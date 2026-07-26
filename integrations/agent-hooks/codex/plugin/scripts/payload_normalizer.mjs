import { resolveSessionId, resolveWorkspace } from "./session_resolver.mjs";

function textFromPayload(payload) {
  for (const key of [
    "prompt",
    "user_prompt",
    "userPrompt",
    "text",
    "input",
    "message",
    "assistant_message",
    "assistantMessage",
    "last_agent_message",
    "lastAgentMessage",
    "last_assistant_message",
    "lastAssistantMessage",
    "response",
    "output",
    "tool_output",
    "toolOutput",
    "terminal_output",
    "terminalOutput"
  ]) {
    if (typeof payload[key] === "string" && payload[key].trim()) {
      return payload[key].trim();
    }
  }
  for (const path of [
    ["params", "prompt"],
    ["params", "text"],
    ["params", "output"],
    ["params", "tool_output"],
    ["params", "assistant_message"],
    ["turn", "text"],
    ["turn", "assistant_message"],
    ["tool", "output"],
    ["tool", "result"],
    ["metadata", "prompt"],
    ["metadata", "text"],
    ["metadata", "output"],
    ["metadata", "assistant_message"]
  ]) {
    let value = payload;
    for (const part of path) {
      if (!value || typeof value !== "object" || !(part in value)) {
        value = undefined;
        break;
      }
      value = value[part];
    }
    if (typeof value === "string" && value.trim()) {
      return value.trim();
    }
  }
  if (Array.isArray(payload.output)) {
    const parts = [];
    for (const item of payload.output) {
      if (typeof item === "string" && item.trim()) {
        parts.push(item.trim());
      } else if (item && typeof item === "object" && typeof item.text === "string" && item.text.trim()) {
        parts.push(item.text.trim());
      }
    }
    if (parts.length) return parts.join("");
  }
  if (payload.message && typeof payload.message === "object") {
    const text = textFromPayload(payload.message);
    if (text) return text;
  }
  return "";
}

function roleForEvent(event) {
  if (event === "UserPromptSubmit") return "user";
  if (event === "PostToolUse") return "tool";
  if (event === "AssistantResponse" || event === "Stop" || event === "PreCompact") return "assistant";
  return "unknown";
}

function splitList(value) {
  if (!value) return [];
  return String(value)
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}

export function normalizePayload({ event, payload = {}, env = process.env }) {
  const session = resolveSessionId(payload, env);
  const workspaceRoot = resolveWorkspace(payload);
  return {
    agent: env.TEMPORALSTORE_AGENT_NAME || "codex",
    event,
    conversation_id: session.conversationId,
    session_id: session.sessionId,
    session_id_source: session.source,
    workspace_root: workspaceRoot,
    repo_remote: env.TEMPORALSTORE_REPO_REMOTE || "",
    repo_branch: env.TEMPORALSTORE_REPO_BRANCH || "",
    project: env.TEMPORALSTORE_AGENT_PROJECT || env.MATRIXARK_PROJECT || "TemporalStore",
    products: splitList(env.MATRIXARK_PRODUCTS || "TemporalStore,MatrixArk"),
    topics: splitList(env.MATRIXARK_TOPICS || "agent-hooks,context-memory"),
    role: roleForEvent(event),
    text: textFromPayload(payload),
    timestamp_ms: Date.now(),
    raw: payload
  };
}
