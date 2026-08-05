#!/usr/bin/env bash
#
# MatrixArk Claude Code context hook: ingestion / extraction / retrieval.
#
# This is the Claude Code counterpart to the Codex hook family
# (tools/matrixark_codex_cpp_hook.sh, tools/matrixark_codex_rust_hook.sh). Claude
# Code invokes it as a `command` hook, one invocation per lifecycle event, e.g.:
#
#   "command": "/abs/path/tools/matrixark_claude_hook.sh --event UserPromptSubmit"
#
# It reads the Claude Code hook JSON on stdin, drives the shared MatrixArk context
# engine as agent "claude", and on UserPromptSubmit / SessionStart prints
#   {"hookSpecificOutput": {"hookEventName": "<event>", "additionalContext": "..."}}
# so retrieved context is injected into Claude. All other events print `{}`.
#
# Backends (MATRIXARK_CLAUDE_HOOK_BACKEND):
#   rust   (default) -> self-contained, offline crate bin `codex_context_hook`
#   python           -> tools/matrixark_agent_hook.py --agent claude (full MCP pipeline)
#
# Fail-open: on any internal failure it prints `{}` and exits 0 so a hook error
# never blocks the Claude Code turn (override with MATRIXARK_HOOK_FAIL_OPEN=0).
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"
PYTHON="${TEMPORALSTORE_PYTHON:-python3}"
# Backend selection:
#   auto   (default) -> python parity pipeline when its runtime (rust proxy) is
#                       present, else the self-contained offline rust engine.
#   python           -> tools/matrixark_agent_hook.py --agent claude (full MCP
#                       pipeline; byte-for-byte the same code as the Codex hook).
#   rust             -> crate bin codex_context_hook (offline, self-contained).
# Default backend is `auto`: prefer the shared multi-agent pipeline
# (tools/matrixark_agent_hook.py --agent claude, the SAME ingest/extract/retrieve
# engine the Codex hook uses) whenever its runtime (the rust proxy) is available,
# and fall back to the self-contained offline rust engine otherwise so the hook
# never hard-fails. Override with MATRIXARK_CLAUDE_HOOK_BACKEND=python|rust|auto.
BACKEND="${MATRIXARK_CLAUDE_HOOK_BACKEND:-auto}"
# Locate the shared-pipeline runtime: an explicit override, else any prebuilt
# release/debug proxy, else the conventional release path (SessionStart may build it).
RUST_PROXY="${MATRIXARK_TEST_RUST_PROXY:-}"
if [[ -z "$RUST_PROXY" ]]; then
  for _cand in "$REPO_ROOT/target/release/matrixark_rust_proxy" "$REPO_ROOT/target/debug/matrixark_rust_proxy"; do
    [[ -x "$_cand" ]] && { RUST_PROXY="$_cand"; break; }
  done
  RUST_PROXY="${RUST_PROXY:-$REPO_ROOT/target/release/matrixark_rust_proxy}"
fi
FAIL_OPEN="${MATRIXARK_HOOK_FAIL_OPEN:-1}"

EVENT=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --event) EVENT="${2:-}"; shift 2 ;;
    --event=*) EVENT="${1#--event=}"; shift ;;
    --backend) BACKEND="${2:-}"; shift 2 ;;
    --backend=*) BACKEND="${1#--backend=}"; shift ;;
    --*) shift ;;
    *) [[ -z "$EVENT" ]] && EVENT="$1"; shift ;;
  esac
done
EVENT="${EVENT:-${CLAUDE_CODE_HOOK_EVENT:-}}"

# Persist stdin once; both the resolver and the hook engine need to read it.
PAYLOAD_FILE="$(mktemp 2>/dev/null || echo /tmp/matrixark-claude-hook-$$.json)"
trap 'rm -f "$PAYLOAD_FILE"' EXIT
cat > "$PAYLOAD_FILE" 2>/dev/null || true
# Strip a leading UTF-8 BOM (Windows tooling can prepend one) so JSON parses cleanly
# in both the resolver and the engine instead of degrading to a raw-text blob.
if [[ -s "$PAYLOAD_FILE" ]]; then
  "$PYTHON" -c 'import sys
p=sys.argv[1]
b=open(p,"rb").read()
if b[:3]==b"\xef\xbb\xbf":
    open(p,"wb").write(b[3:])' "$PAYLOAD_FILE" 2>/dev/null || true
fi

fail_open() {
  if [[ "$FAIL_OPEN" == "1" ]]; then printf '{}\n'; exit 0; fi
  "$PYTHON" -c 'import json,sys; print(json.dumps({"status":"error","reason":sys.argv[1]}))' "${1:-error}" 2>/dev/null \
    || printf '{"status":"error"}\n'
  exit 0
}

# Agent identity + per-claude storage scope (kept distinct from codex state).
export MATRIXARK_AGENT_NAME="claude"
export TEMPORALSTORE_AGENT_NAME="claude"
export MATRIXARK_ACCOUNT_ID="${MATRIXARK_ACCOUNT_ID:-acct_claude}"
export MATRIXARK_TENANT_ID="${MATRIXARK_TENANT_ID:-tenant_claude}"
export MATRIXARK_USER_ID="${MATRIXARK_USER_ID:-${USER:-claude_user}}"
export TEMPORALSTORE_RUST_CODEX_HOOK_ROOT="${TEMPORALSTORE_RUST_CLAUDE_HOOK_ROOT:-${TEMPORALSTORE_RUST_CODEX_HOOK_ROOT:-/tmp/temporalstore-rust-claude-hook}}"
export TEMPORALSTORE_RUST_CODEX_EVENT_LOG="${TEMPORALSTORE_RUST_CLAUDE_EVENT_LOG:-${TEMPORALSTORE_RUST_CODEX_EVENT_LOG:-/tmp/temporalstore-rust-claude-hook.jsonl}}"

# Resolve the event + session from the payload when not passed as an argument.
# Data arrives on stdin; the program is passed via -c (no stdin conflict).
RESOLVED="$("$PYTHON" -c '
import sys, json
try:
    d = json.loads(sys.stdin.read() or "{}")
    d = d if isinstance(d, dict) else {}
except Exception:
    d = {}
ev = d.get("hook_event_name") or ""
ss = (d.get("session_id") or d.get("sessionId") or d.get("conversation_id")
      or d.get("transcript_path") or "")
sys.stdout.write(ev + "\t" + str(ss))
' < "$PAYLOAD_FILE" 2>/dev/null || true)"
PAYLOAD_EVENT="${RESOLVED%%$'\t'*}"
PAYLOAD_SESSION="${RESOLVED#*$'\t'}"
EVENT="${EVENT:-$PAYLOAD_EVENT}"
EVENT="${EVENT:-UserPromptSubmit}"
SESSION="${PAYLOAD_SESSION:-claude_session}"

# Resolve `auto` to the shared pipeline when its runtime is present. On the
# long-budget SessionStart, kick a detached build of the shared proxy so it never
# blocks the turn; once present, later turns auto-upgrade from the offline engine
# to the same ingest/extract/retrieve pipeline Codex uses. Until then, and whenever
# the proxy is unavailable, fall back to the self-contained offline rust engine.
if [[ "$BACKEND" == "auto" ]]; then
  if [[ ! -x "$RUST_PROXY" && "$EVENT" == "SessionStart" && "${MATRIXARK_CLAUDE_HOOK_ALLOW_BUILD:-1}" == "1" ]]; then
    setsid bash -c "CARGO_TARGET_DIR='$REPO_ROOT/target' cargo build -q -p temporalstore-rust --bin matrixark_rust_proxy" \
      >/dev/null 2>&1 </dev/null &
    disown 2>/dev/null || true
  fi
  if [[ -x "$RUST_PROXY" ]]; then BACKEND="python"; else BACKEND="rust"; fi
fi

if [[ "$BACKEND" == "python" ]]; then
  # Parity pipeline: matrixark_agent_hook.py runs the identical ingest/extract/
  # retrieve engine as the Codex hook and already emits the Claude Code contract
  # (hookSpecificOutput.additionalContext on UserPromptSubmit). Uses the local
  # rust proxy so it works without a running metaserver.
  export MATRIXARK_MCP_BACKEND="${MATRIXARK_MCP_BACKEND:-temporalstore-rust}"
  export MATRIXARK_LOCAL_MODE="${MATRIXARK_LOCAL_MODE:-no-metaserver}"
  export MATRIXARK_TEMPORALSTORE_METASERVER="${MATRIXARK_TEMPORALSTORE_METASERVER:-local}"
  export MATRIXARK_TEMPORALSTORE_RUST_ROOT="${MATRIXARK_TEMPORALSTORE_RUST_ROOT:-/tmp/temporalstore-claude-hook-store}"
  export MATRIXARK_RUST_PROXY_ASYNC_STORAGE="${MATRIXARK_RUST_PROXY_ASYNC_STORAGE:-true}"
  export MATRIXARK_HOOK_STORAGE_ROUTE="${MATRIXARK_HOOK_STORAGE_ROUTE:-shared_store_async}"
  PY_REPORT="$("$PYTHON" tools/matrixark_agent_hook.py \
    --agent claude \
    --event "$EVENT" \
    --backend "${MATRIXARK_MCP_BACKEND}" \
    --metaserver "${MATRIXARK_TEMPORALSTORE_METASERVER}" \
    --namespace "${TEMPORALSTORE_NAMESPACE:-deploy_ns}" \
    --table "${TEMPORALSTORE_TABLE:-deploy_table}" \
    --rust-proxy "${RUST_PROXY}" \
    --storage-prefix "${MATRIXARK_STORAGE_PREFIX:-matrixark:claude-hook}" \
    --account-id "${MATRIXARK_ACCOUNT_ID}" \
    --tenant-id "${MATRIXARK_TENANT_ID}" \
    --user-id "${MATRIXARK_USER_ID}" \
    --team "${MATRIXARK_TEAM:-agent}" \
    --project "${TEMPORALSTORE_AGENT_PROJECT:-TemporalStore}" \
    --max-context-tokens "${MATRIXARK_MAX_CONTEXT_TOKENS:-10000}" \
    < "$PAYLOAD_FILE")" || fail_open "python agent hook failed"
  # Emit only the Claude Code hook contract: pass through hookSpecificOutput when the
  # adapter produced one (before-LLM events with retrieved context), else {} .
  printf '%s' "$PY_REPORT" | "$PYTHON" -c '
import sys, json
try:
    rep = json.loads(sys.stdin.read() or "{}")
    rep = rep if isinstance(rep, dict) else {}
except Exception:
    rep = {}
hso = rep.get("hookSpecificOutput")
if isinstance(hso, dict) and (hso.get("additionalContext") or "").strip():
    print(json.dumps({"hookSpecificOutput": hso}))
else:
    print("{}")
' || fail_open "translate failed"
  exit 0
fi

# --- rust backend (default, offline, self-contained) ---
TARGET_DIR="${CARGO_TARGET_DIR:-/tmp/temporalstore-context-workflow-target}"
BIN="$TARGET_DIR/debug/codex_context_hook"
SRC="crates/temporalstore-rust/src/bin/codex_context_hook.rs"
# Build only on the long-budget SessionStart event (or when explicitly allowed) so a
# cold cargo build never lands inside the 30s UserPromptSubmit budget. A stale but
# present binary is still used (better a slightly old engine than no context); only a
# fully missing binary fails the hot path open.
if [[ "$EVENT" == "SessionStart" || "${MATRIXARK_CLAUDE_HOOK_ALLOW_BUILD:-0}" == "1" ]]; then
  if [[ ! -x "$BIN" || "$SRC" -nt "$BIN" ]]; then
    CARGO_TARGET_DIR="$TARGET_DIR" cargo build -q -p temporalstore-rust --bin codex_context_hook \
      >/dev/null 2>&1 || fail_open "hook binary build failed"
  fi
elif [[ ! -x "$BIN" ]]; then
  fail_open "hook binary not built yet; run the SessionStart hook or set MATRIXARK_CLAUDE_HOOK_ALLOW_BUILD=1"
fi

# Optional async first-start backfill (opt-in via MATRIXARK_BACKFILL_ON_START=1).
# Detached so it never blocks the turn; the daemon self-locks and resumes from
# per-agent offsets, warming the store with local Claude/Codex context + durable
# global memory. Fail-open: any error here must not affect the hook output.
if [[ "$EVENT" == "SessionStart" && "${MATRIXARK_BACKFILL_ON_START:-0}" == "1" ]]; then
  setsid bash "$REPO_ROOT/tools/matrixark_backfill_daemon.sh" >/dev/null 2>&1 </dev/null &
  disown 2>/dev/null || true
fi

REPORT="$("$BIN" --agent-name claude --event "$EVENT" --session-id "$SESSION" < "$PAYLOAD_FILE" 2>/dev/null)" \
  || fail_open "hook engine failed"
[[ -z "$REPORT" ]] && fail_open "empty hook engine report"

# Translate the engine report into the Claude Code hook output contract.
# The report arrives on stdin; the program is passed via -c, event via argv.
printf '%s' "$REPORT" | "$PYTHON" -c '
import sys, json
event = sys.argv[1] if len(sys.argv) > 1 else "UserPromptSubmit"
try:
    rep = json.loads(sys.stdin.read() or "{}")
    rep = rep if isinstance(rep, dict) else {}
except Exception:
    rep = {}
ctx = (rep.get("retrieve") or {}).get("additional_context") or ""
if event in ("UserPromptSubmit", "SessionStart") and isinstance(ctx, str) and ctx.strip():
    print(json.dumps({"hookSpecificOutput": {"hookEventName": event, "additionalContext": ctx}}))
else:
    print("{}")
' "$EVENT" || fail_open "translate failed"
