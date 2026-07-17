#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODE="${TEMPORALSTORE_AGENT_MODE:-dry-run}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode)
      MODE="$2"
      shift 2
      ;;
    *)
      echo "unknown argument: $1" >&2
      exit 2
      ;;
  esac
done

NODE_BIN="${CODEX_NODE_PATH:-${NODE_BIN:-node}}"
MARKER="TEMPORALSTORE_AGENT_HOOK_SMOKE_$(date +%s)"
PAYLOAD="{\"prompt\":\"$MARKER\",\"session_id\":\"smoke-session-$MARKER\",\"cwd\":\"$ROOT\"}"

TEMPORALSTORE_AGENT_MODE="$MODE" "$NODE_BIN" \
  "$ROOT/codex/plugin/scripts/temporalstore_hook_launcher.mjs" \
  UserPromptSubmit < <(printf '%s' "$PAYLOAD")
