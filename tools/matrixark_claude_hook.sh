#!/usr/bin/env bash
#
# MatrixArk Claude Code context hook: ingestion / extraction / retrieval.
#
# This is the Claude Code counterpart to the Codex hook family
# (tools/matrixark_codex_hook.sh, tools/matrixark_codex_rust_hook.sh). Claude
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
#   auto   (default) -> python conformance pipeline when its runtime (rust proxy) is
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
# Persistent shared store base. /tmp is wiped on reboot and per-agent roots split
# local memory; anchor Claude and Codex Rust hooks in one durable MatrixArk root.
# Override with MATRIXARK_SHARED_HOOK_STORE_BASE or MATRIXARK_CLAUDE_HOOK_STORE_BASE.
CLAUDE_STORE_BASE="${MATRIXARK_CLAUDE_HOOK_STORE_BASE:-${MATRIXARK_SHARED_HOOK_STORE_BASE:-/root/.matrixark/temporalstore-hooks/shared}}"
mkdir -p "$CLAUDE_STORE_BASE" 2>/dev/null || true
export TEMPORALSTORE_RUST_CODEX_HOOK_ROOT="${TEMPORALSTORE_RUST_CLAUDE_HOOK_ROOT:-${TEMPORALSTORE_RUST_CODEX_HOOK_ROOT:-$CLAUDE_STORE_BASE/rust}}"
export TEMPORALSTORE_RUST_CODEX_EVENT_LOG="${TEMPORALSTORE_RUST_CLAUDE_EVENT_LOG:-${TEMPORALSTORE_RUST_CODEX_EVENT_LOG:-$CLAUDE_STORE_BASE/rust.jsonl}}"
export MATRIXARK_CLAUDE_RUST_HOOK_ROOT="${MATRIXARK_CLAUDE_RUST_HOOK_ROOT:-$TEMPORALSTORE_RUST_CODEX_HOOK_ROOT}"

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

_matrixark_backfill_enabled() {
  case "${MATRIXARK_BACKFILL_ON_START:-auto}" in
    0|false|False|FALSE|no|No|NO|off|Off|OFF) return 1 ;;
    *) return 0 ;;
  esac
}

_matrixark_start_backfill_daemon() {
  _matrixark_backfill_enabled || return 0
  [[ "$EVENT" == "SessionStart" || "${MATRIXARK_BACKFILL_ON_EVERY_HOOK:-0}" == "1" ]] || return 0
  setsid bash "$REPO_ROOT/tools/matrixark_backfill_daemon.sh" >/dev/null 2>&1 </dev/null &
  disown 2>/dev/null || true
}

# Async first-start local context load. Start before backend selection so Claude's
# shared Python pipeline and self-contained Rust path both get the same
# nonblocking backfill behavior.
_matrixark_start_backfill_daemon

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
  # Conformance pipeline: matrixark_agent_hook.py runs the identical ingest/extract/
  # retrieve engine as the Codex hook and already emits the Claude Code contract
  # (hookSpecificOutput.additionalContext on UserPromptSubmit). Uses the local
  # rust proxy so it works without a running metaserver.
  export MATRIXARK_MCP_BACKEND="${MATRIXARK_MCP_BACKEND:-temporalstore-rust}"
  export MATRIXARK_LOCAL_MODE="${MATRIXARK_LOCAL_MODE:-no-metaserver}"
  export MATRIXARK_TEMPORALSTORE_METASERVER="${MATRIXARK_TEMPORALSTORE_METASERVER:-local}"
  export MATRIXARK_TEMPORALSTORE_RUST_ROOT="${MATRIXARK_TEMPORALSTORE_RUST_ROOT:-$CLAUDE_STORE_BASE/store}"
  export MATRIXARK_RUST_PROXY_ASYNC_STORAGE="${MATRIXARK_RUST_PROXY_ASYNC_STORAGE:-true}"
  export MATRIXARK_HOOK_STORAGE_ROUTE="${MATRIXARK_HOOK_STORAGE_ROUTE:-shared_store_async}"
  # Durability contract for this single-node, no-metaserver deployment: serving
  # records extracted on ingest/session-commit must be written through the durable
  # TemporalStore backend writer (_append_many_materialized) instead of the
  # backend adapter's disabled local-JSONL mirror, which would silently drop every
  # record (records_written == 0) and leave retrieval permanently empty. This is
  # orthogonal to MATRIXARK_HOOK_STORAGE_ROUTE above (the route does not control
  # durability here); both stay overridable via env. Set to 0 to opt out.
  # Warm resident proxy daemon (kills the cold-start re-scan). Each hook invocation
  # is a fresh short-lived process; spawning a brand-new matrixark_rust_proxy every
  # call forces it to reload the shard and re-scan the full serving-record set
  # (~thousands of records) before every retrieve -- slow and nondeterministic
  # (coverage swung run-to-run, and worse under synchronous storage). Instead route
  # the shared pipeline through a long-lived proxy kept warm behind a Unix socket,
  # exactly like the Codex hook (tools/matrixark_codex_rust_hook.sh). The daemon
  # inherits MATRIXARK_TEMPORALSTORE_RUST_ROOT so its resident proxy is pinned to
  # THIS claude store (one daemon per store root); the adapter's _call_socket_json
  # dispatches to it when MATRIXARK_RUST_PROXY_SOCKET is set. Gated by
  # MATRIXARK_CLAUDE_HOOK_PROXY_DAEMON (default on). Fail-safe: if the daemon cannot
  # be made reachable we UNSET the socket so the adapter falls back to the prior
  # ephemeral-spawn path (default behavior fully preserved).
  if [[ "${MATRIXARK_CLAUDE_HOOK_PROXY_DAEMON:-1}" == "1" && -x "$RUST_PROXY" ]]; then
    # Codex and Claude now share the same durable Rust hook root, so they also
    # share the warm proxy socket. Override MATRIXARK_RUST_PROXY_SOCKET for
    # isolated test roots.
    export MATRIXARK_RUST_PROXY_SOCKET="${MATRIXARK_RUST_PROXY_SOCKET:-/tmp/matrixark-rust-proxy-shared-live.sock}"
    _MATRIXARK_CLAUDE_DAEMON_LOG="${MATRIXARK_RUST_PROXY_DAEMON_LOG:-/dev/null}"
    _matrixark_claude_daemon_ping() {
      "$PYTHON" "$REPO_ROOT/tools/matrixark_rust_proxy_daemon.py" \
        --proxy "$RUST_PROXY" --socket "$MATRIXARK_RUST_PROXY_SOCKET" \
        --log "$_MATRIXARK_CLAUDE_DAEMON_LOG" --ping >/dev/null 2>&1
    }
    if ! _matrixark_claude_daemon_ping; then
      if [[ "${MATRIXARK_CLAUDE_HOOK_PROXY_DAEMON_AUTOSTART:-1}" == "1" ]]; then
        (
          flock -n 8 || exit 0
          if _matrixark_claude_daemon_ping; then exit 0; fi
          setsid "$PYTHON" "$REPO_ROOT/tools/matrixark_rust_proxy_daemon.py" \
            --proxy "$RUST_PROXY" --socket "$MATRIXARK_RUST_PROXY_SOCKET" \
            --log "$_MATRIXARK_CLAUDE_DAEMON_LOG" >/dev/null 2>&1 </dev/null &
          disown 2>/dev/null || true
        ) 8>"$MATRIXARK_RUST_PROXY_SOCKET.start.lock"
        for _ in $(seq 1 40); do _matrixark_claude_daemon_ping && break; sleep 0.05; done
      fi
    fi
    if ! _matrixark_claude_daemon_ping; then unset MATRIXARK_RUST_PROXY_SOCKET; fi
  fi
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
