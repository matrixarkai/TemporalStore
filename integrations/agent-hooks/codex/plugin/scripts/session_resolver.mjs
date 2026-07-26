import crypto from "node:crypto";
import os from "node:os";

function firstStringAt(payload, paths) {
  for (const path of paths) {
    let value = payload;
    for (const part of path) {
      if (!value || typeof value !== "object" || !(part in value)) {
        value = undefined;
        break;
      }
      value = value[part];
    }
    if (typeof value === "string" && value.trim()) {
      return { value: value.trim(), source: path.join(".") };
    }
  }
  return { value: "", source: "" };
}

function shortHash(value) {
  return crypto.createHash("sha256").update(String(value)).digest("hex").slice(0, 16);
}

function firstStringEnv(env, keys) {
  for (const key of keys) {
    const value = env[key];
    if (typeof value === "string" && value.trim()) {
      return { value: value.trim(), source: key };
    }
  }
  return { value: "", source: "" };
}

export function resolveSessionId(payload = {}, env = process.env) {
  const direct = firstStringAt(payload, [
    ["session_id"],
    ["sessionId"],
    ["codex_session_id"],
    ["conversation_id"],
    ["conversationId"],
    ["thread_id"],
    ["threadId"],
    ["transcript_id"],
    ["transcriptId"],
    ["run", "session_id"],
    ["run", "thread_id"],
    ["params", "session_id"],
    ["params", "thread_id"],
    ["turn", "session_id"],
    ["turn", "thread_id"],
    ["metadata", "session_id"],
    ["metadata", "thread_id"]
  ]);
  if (direct.value) {
    return {
      sessionId: `${env.TEMPORALSTORE_AGENT_NAME || "codex"}:${direct.value}`,
      conversationId: direct.value,
      source: `payload.${direct.source}`
    };
  }

  const pathValue = firstStringAt(payload, [
    ["transcript_path"],
    ["transcriptPath"],
    ["conversation_path"],
    ["conversationPath"],
    ["thread_path"],
    ["threadPath"],
    ["log_path"],
    ["logPath"]
  ]);
  if (pathValue.value) {
    const value = `path:${shortHash(pathValue.value)}`;
    return {
      sessionId: `${env.TEMPORALSTORE_AGENT_NAME || "codex"}:${value}`,
      conversationId: value,
      source: `payload.${pathValue.source}`
    };
  }

  const envValue = firstStringEnv(env, [
    "CODEX_SESSION_ID",
    "CODEX_THREAD_ID",
    "CODEX_CONVERSATION_ID",
    "CODEX_TRANSCRIPT_ID",
    "MATRIXARK_CODEX_SESSION_ID",
    "MATRIXARK_CODEX_THREAD_ID",
    "TEMPORALSTORE_CODEX_SESSION_ID",
    "TEMPORALSTORE_AGENT_SESSION_ID"
  ]);
  if (envValue.value) {
    return {
      sessionId: `${env.TEMPORALSTORE_AGENT_NAME || "codex"}:${envValue.value}`,
      conversationId: envValue.value,
      source: `env.${envValue.source}`
    };
  }

  const workspace = firstStringAt(payload, [
    ["workspace_root"],
    ["workspaceRoot"],
    ["cwd"],
    ["params", "cwd"],
    ["metadata", "cwd"]
  ]).value || process.cwd();
  const seed = [
    env.MATRIXARK_ACCOUNT_ID || "acct_local",
    env.MATRIXARK_TENANT_ID || "tenant_local",
    env.MATRIXARK_USER_ID || os.userInfo().username,
    workspace
  ].join("|");
  const fallback = `local:${shortHash(seed)}`;
  return {
    sessionId: `${env.TEMPORALSTORE_AGENT_NAME || "codex"}:${fallback}`,
    conversationId: fallback,
    source: "workspace_hash"
  };
}

export function resolveWorkspace(payload = {}) {
  return firstStringAt(payload, [
    ["workspace_root"],
    ["workspaceRoot"],
    ["cwd"],
    ["params", "cwd"],
    ["metadata", "cwd"]
  ]).value || process.cwd();
}
