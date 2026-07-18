import http from "node:http";
import https from "node:https";
import { spawnSync } from "node:child_process";

function readJsonStdin() {
  return new Promise((resolve, reject) => {
    let input = "";
    process.stdin.setEncoding("utf8");
    process.stdin.on("data", (chunk) => {
      input += chunk;
    });
    process.stdin.on("end", () => {
      if (!input.trim()) {
        resolve({});
        return;
      }
      try {
        resolve(JSON.parse(input));
      } catch (error) {
        reject(new Error(`failed to parse hook stdin as JSON: ${error.message}`));
      }
    });
  });
}

export async function loadPayloadFromStdin() {
  return readJsonStdin();
}

function hookArgs(event, env) {
  return [
    "tools/matrixark_codex_hook.py",
    "--event",
    event,
    "--backend",
    env.TEMPORALSTORE_HOOK_BACKEND || "temporalstore-direct",
    "--metaserver",
    env.TEMPORALSTORE_METASERVER || "127.0.0.1:18000",
    "--namespace",
    env.TEMPORALSTORE_NAMESPACE || "deploy_ns",
    "--table",
    env.TEMPORALSTORE_TABLE || "deploy_table",
    "--temporalstore-lib",
    env.TEMPORALSTORE_LIBRARY || "output-ubuntu22/release/sdk/lib/libbcache2.so",
    "--storage-prefix",
    env.MATRIXARK_STORAGE_PREFIX || "matrixark:agent-hook",
    "--account-id",
    env.MATRIXARK_ACCOUNT_ID || "acct_local",
    "--tenant-id",
    env.MATRIXARK_TENANT_ID || "tenant_codex",
    "--user-id",
    env.MATRIXARK_USER_ID || "local_user",
    "--team",
    env.MATRIXARK_TEAM || "agent",
    "--project",
    env.TEMPORALSTORE_AGENT_PROJECT || "TemporalStore",
    "--max-context-tokens",
    env.MATRIXARK_MAX_CONTEXT_TOKENS || "10000",
    "--request-timeout-ms",
    env.MATRIXARK_REQUEST_TIMEOUT_MS || "90000",
    "--io-timeout-ms",
    env.MATRIXARK_IO_TIMEOUT_MS || "90000",
    "--session-commit-threshold",
    env.MATRIXARK_SESSION_COMMIT_THRESHOLD || "20",
    "--idle-commit-timeout-ms",
    env.MATRIXARK_IDLE_COMMIT_TIMEOUT_MS || "300000",
    "--codex-strict-output"
  ];
}

export function callLocalPythonHook({ event, payload, env, mode }) {
  const stdin = JSON.stringify(payload);
  let command;
  let args;

  if (mode === "wsl") {
    command = "wsl.exe";
    args = [];
    if (env.TEMPORALSTORE_WSL_DISTRO) {
      args.push("-d", env.TEMPORALSTORE_WSL_DISTRO);
    }
    args.push(
      "--cd",
      env.TEMPORALSTORE_WSL_REPO || env.TEMPORALSTORE_REPO || "/root/src/github-services/TemporalStore",
      "-e",
      env.TEMPORALSTORE_PYTHON || "python3",
      ...hookArgs(event, env)
    );
  } else {
    command = env.TEMPORALSTORE_PYTHON || "python3";
    args = hookArgs(event, env);
  }

  const result = spawnSync(command, args, {
    cwd: mode === "wsl" ? undefined : env.TEMPORALSTORE_REPO || process.cwd(),
    input: stdin,
    encoding: "utf8",
    timeout: Number(env.TEMPORALSTORE_HOOK_TIMEOUT_MS || 120000),
    env: process.env
  });

  if (result.error) {
    throw result.error;
  }
  if (result.status !== 0) {
    throw new Error(`TemporalStore hook exited ${result.status}: ${result.stderr || result.stdout}`);
  }
  return result.stdout || "{}";
}

export async function callRemoteHook({ event, normalizedEvent, payload, env }) {
  const endpoint = env.TEMPORALSTORE_AGENT_ENDPOINT;
  if (!endpoint) {
    throw new Error("TEMPORALSTORE_AGENT_ENDPOINT is required for remote mode");
  }
  const url = new URL("/api/agent/hook", endpoint);
  const body = JSON.stringify({ event, normalized_event: normalizedEvent, raw_payload: payload });
  const client = url.protocol === "https:" ? https : http;
  return new Promise((resolve, reject) => {
    const request = client.request(
      url,
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "content-length": Buffer.byteLength(body)
        },
        timeout: Number(env.TEMPORALSTORE_HOOK_TIMEOUT_MS || 120000)
      },
      (response) => {
        let output = "";
        response.setEncoding("utf8");
        response.on("data", (chunk) => {
          output += chunk;
        });
        response.on("end", () => {
          if (response.statusCode && response.statusCode >= 400) {
            reject(new Error(`remote hook HTTP ${response.statusCode}: ${output}`));
          } else {
            resolve(output || "{}");
          }
        });
      }
    );
    request.on("error", reject);
    request.write(body);
    request.end();
  });
}
