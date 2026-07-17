#!/usr/bin/env node
import { loadPayloadFromStdin, callLocalPythonHook, callRemoteHook } from "./temporalstore_client.mjs";
import { normalizePayload } from "./payload_normalizer.mjs";

function usage() {
  return `Usage: temporalstore_hook_launcher.mjs <event> [--dry-run] [--print-config]\n`;
}

function parseArgs(argv) {
  const args = {
    event: "UserPromptSubmit",
    dryRun: false,
    printConfig: false
  };
  for (const arg of argv) {
    if (arg === "--dry-run") args.dryRun = true;
    else if (arg === "--print-config") args.printConfig = true;
    else if (arg === "-h" || arg === "--help") {
      process.stdout.write(usage());
      process.exit(0);
    } else if (!arg.startsWith("--")) {
      args.event = arg;
    }
  }
  return args;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const env = process.env;
  const mode = args.dryRun ? "dry-run" : (env.TEMPORALSTORE_AGENT_MODE || "dry-run").toLowerCase();
  const payload = await loadPayloadFromStdin();
  const normalizedEvent = normalizePayload({ event: args.event, payload, env });

  if (args.printConfig) {
    process.stdout.write(JSON.stringify({
      mode,
      event: args.event,
      repo: env.TEMPORALSTORE_REPO || "",
      endpoint: env.TEMPORALSTORE_AGENT_ENDPOINT || "",
      normalized_event: normalizedEvent
    }, null, 2) + "\n");
    return;
  }

  if (mode === "dry-run") {
    process.stdout.write(JSON.stringify({ status: "dry_run", event: normalizedEvent }, null, 2) + "\n");
    return;
  }

  if (mode === "remote" || mode === "docker") {
    process.stdout.write(await callRemoteHook({ event: args.event, normalizedEvent, payload, env }));
    return;
  }

  if (mode === "wsl" || mode === "native") {
    process.stdout.write(callLocalPythonHook({ event: args.event, payload, env, mode }));
    return;
  }

  throw new Error(`unsupported TEMPORALSTORE_AGENT_MODE: ${mode}`);
}

main().catch((error) => {
  process.stderr.write(`temporalstore agent hook failed: ${error.message}\n`);
  process.exit(1);
});
