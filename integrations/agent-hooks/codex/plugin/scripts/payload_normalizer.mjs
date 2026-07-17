import { resolveSessionId, resolveWorkspace } from "./session_resolver.mjs";

function textFromPayload(payload) {
  for (const key of ["prompt", "user_prompt", "userPrompt", "text", "input", "message"]) {
    if (typeof payload[key] === "string" && payload[key].trim()) {
      return payload[key].trim();
    }
  }
  for (const path of [["params", "prompt"], ["params", "text"], ["turn", "text"], ["metadata", "prompt"]]) {
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
  return "";
}

function roleForEvent(event) {
  if (event === "UserPromptSubmit") return "user";
  if (event === "PostToolUse") return "tool";
  if (event === "Stop" || event === "PreCompact") return "assistant";
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
