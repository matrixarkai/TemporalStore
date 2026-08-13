#!/usr/bin/env bash
#
# Async, resumable first-start backfill of local Claude/Codex context into the
# live TemporalStore hook stores. Launched detached from the SessionStart hook
# (opt-in: MATRIXARK_BACKFILL_ON_START=1) so it never blocks a turn.
#
# It only ingests external local context when the on-disk store is EMPTY (a first
# start or a fresh/wiped data dir). A populated store recovers context/memory from
# its own on-disk persistence on restart, so the daemon skips it (see the
# recover-from-persistence guard below) — restarts never re-ingest from logs.
#
# Override: MATRIXARK_BACKFILL_FORCE=1 forces a re-ingest from the agents' OWN
# logs (Claude/Codex transcripts, rollouts, resources) even when the store is
# already populated — for re-importing agent history on demand. It is dedup-safe.
#
# Safe to run repeatedly: a lockfile prevents overlap, a per-agent offset marker
# makes it resume where it left off after a teardown, and the engine dedups by
# content hash so re-processed records never duplicate.
#
# Flow: build batch bin (offline) -> emit per-agent JSONL once -> ingest in
# bounded chunks (MATRIXARK_BULK_INGEST keeps disk O(1)/record) advancing an
# offset per chunk -> mark done. High-value durable memory (resources/skills/
# memory/openviking) is emitted under the `_global` scope by the ingester, so it
# becomes cross-session-retrievable as soon as the first chunks land.
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"
PYTHON="${TEMPORALSTORE_PYTHON:-python3}"
TARGET_DIR="${CARGO_TARGET_DIR:-$REPO_ROOT/target}"
BATCH="$TARGET_DIR/debug/context_batch_ingest"
# Low-priority execution so real-time (live-hook) ingestion is never starved by
# the backfill: nice lowers CPU priority, ionice (if present) lowers IO priority,
# and a short yield between chunks releases the store lock for any live write.
NICE_PREFIX="nice -n ${MATRIXARK_BACKFILL_NICE:-19}"
command -v ionice >/dev/null 2>&1 && NICE_PREFIX="ionice -c3 $NICE_PREFIX"
YIELD_MS="${MATRIXARK_BACKFILL_YIELD_MS:-200}"
INGESTER="$REPO_ROOT/tools/matrixark_local_backfill_ingester.py"
WORK="${MATRIXARK_BACKFILL_WORK:-/root/.matrixark/temporalstore-backfill}"
CHUNK="${MATRIXARK_BACKFILL_CHUNK:-400}"
AGENTS="${MATRIXARK_BACKFILL_AGENTS:-claude codex}"
LOCK="$WORK/.lock"
DONE="$WORK/.done"
LOG="$WORK/daemon.log"

FORCE="${MATRIXARK_BACKFILL_FORCE:-0}"     # =1: force re-ingest from agent logs, overriding the guard
mkdir -p "$WORK"
exec 9>"$LOCK" 2>/dev/null || exit 0
flock -n 9 || exit 0                      # another daemon already running
[[ -f "$DONE" && "$FORCE" != "1" ]] && exit 0   # already backfilled (a forced run ignores this)

log() { echo "[$(date -u +%FT%TZ)] $*" >>"$LOG"; }
log "daemon start (chunk=$CHUNK agents='$AGENTS')"

# Recover-from-persistence guard.
#
# TemporalStore is the system of record: on restart the engine loads its own
# persisted records from the on-disk data dir, so context/memory is recovered
# from persistence, NOT by re-ingesting external logs. We therefore only ingest
# local context on a *first start* or when the on-disk store is *empty* (e.g. a
# fresh or wiped data dir). If every agent's store already holds records, there
# is nothing to backfill.
agent_root() {
  case "$1" in
    claude)
      if [[ -n "${MATRIXARK_CLAUDE_RUST_HOOK_ROOT:-}" ]]; then echo "$MATRIXARK_CLAUDE_RUST_HOOK_ROOT"; return; fi
      if [[ -n "${TEMPORALSTORE_RUST_CLAUDE_HOOK_ROOT:-}" ]]; then echo "$TEMPORALSTORE_RUST_CLAUDE_HOOK_ROOT"; return; fi
      echo "${MATRIXARK_CLAUDE_HOOK_STORE_BASE:-/root/.matrixark/temporalstore-hooks/claude}/rust"
      ;;
    codex)
      if [[ -n "${MATRIXARK_CODEX_RUST_HOOK_ROOT:-}" ]]; then echo "$MATRIXARK_CODEX_RUST_HOOK_ROOT"; return; fi
      echo "${MATRIXARK_CODEX_HOOK_STORE_BASE:-/root/.matrixark/temporalstore-hooks/codex}/rust"
      ;;
    *)
      echo "/root/.matrixark/temporalstore-hooks/$1/rust"
      ;;
  esac
}
store_has_memory() {
  local root="$1"
  [[ -d "$root" ]] || return 1
  [[ -n "$(find "$root/indexes" "$root/cache" -type f -size +0c -print -quit 2>/dev/null)" ]]
}
if [[ "$FORCE" == "1" ]]; then
  # User forced a re-ingest from the AGENTS' own logs (Claude/Codex transcripts,
  # rollouts, resources — not TemporalStore's own logs). Bypass the
  # recover-from-persistence guard and re-read from the start; the engine dedups
  # by content hash, so records already present are not duplicated.
  log "MATRIXARK_BACKFILL_FORCE=1: forcing backfill from agent logs (bypassing recover-from-persistence guard)"
  rm -f "$DONE" "$WORK/.emitted" "$WORK"/.offset.*
else
  need_backfill=0
  for AG in $AGENTS; do store_has_memory "$(agent_root "$AG")" || need_backfill=1; done
  if (( ! need_backfill )); then
    log "all agent stores already populated on disk; recovering from persistence, skipping local-context backfill"
    touch "$DONE"
    exit 0
  fi
fi

# 1) Ensure the load-once batch bin exists (offline build; deps are cached).
if [[ ! -x "$BATCH" ]]; then
  log "building context_batch_ingest"
  CARGO_TARGET_DIR="$TARGET_DIR" cargo build --offline -q -p temporalstore-rust \
    --bin context_batch_ingest >>"$LOG" 2>&1 || { log "build failed"; exit 0; }
fi

# 2) Emit per-agent JSONL once (fast enumerator; durable memory -> _global scope).
if [[ ! -f "$WORK/.emitted" ]]; then
  log "emitting per-agent JSONL"
  "$PYTHON" "$INGESTER" --emit-jsonl "$WORK" >>"$LOG" 2>&1 || { log "emit failed"; exit 0; }
  touch "$WORK/.emitted"
fi

# 3) Ingest each agent in resumable chunks.
agent_account() { case "$1" in claude) echo acct_claude;; codex) echo acct_codex;; *) echo "acct_$1";; esac; }
agent_tenant()  { case "$1" in claude) echo tenant_claude;; codex) echo tenant_codex;; *) echo "tenant_$1";; esac; }
# Bounded parallel pool. Each agent has its own store root, so agents are
# independent and run concurrently; chunks WITHIN an agent stay sequential (one
# per-store write lock + a resumable offset that must advance in order). Parallel
# writers into a SINGLE store would only contend on that lock, so we never split
# one agent across workers. JOBS defaults to min(cores-1, #agents).
num_agents=$(echo $AGENTS | wc -w)
JOBS="${MATRIXARK_BACKFILL_JOBS:-0}"
if (( JOBS <= 0 )); then
  ncpu=$(nproc 2>/dev/null || echo 2)
  JOBS=$(( ncpu > 1 ? ncpu - 1 : 1 ))
  (( JOBS > num_agents )) && JOBS=$num_agents
  (( JOBS < 1 )) && JOBS=1
fi

# Ingest one agent to completion (or until a chunk fails). Records outcome in a
# per-agent status file so the parent can aggregate after the pool drains.
backfill_agent() {
  local AG="$1"
  local SRC="$WORK/backfill.$AG.jsonl"
  local ROOT; ROOT="$(agent_root "$AG")"
  [[ -f "$SRC" ]] || { echo skipped >"$WORK/.status.$AG"; return 0; }
  if [[ "$FORCE" != "1" ]] && store_has_memory "$ROOT"; then
    log "$AG: store already populated ($ROOT); recovering from persistence, skipping local-context backfill"
    echo skipped >"$WORK/.status.$AG"; return 0
  fi
  local total off end off_file
  total=$(wc -l <"$SRC")
  off_file="$WORK/.offset.$AG"
  off=$(cat "$off_file" 2>/dev/null || echo 0)
  while (( off < total )); do
    end=$(( off + CHUNK ))
    log "$AG: ingesting rows $((off+1))..$([[ $end -gt $total ]] && echo $total || echo $end) / $total"
    if sed -n "$((off+1)),${end}p" "$SRC" | \
        MATRIXARK_AGENT_NAME="$AG" MATRIXARK_ACCOUNT_ID="$(agent_account "$AG")" \
        MATRIXARK_TENANT_ID="$(agent_tenant "$AG")" MATRIXARK_USER_ID="${USER:-root}" \
        TEMPORALSTORE_RUST_CODEX_HOOK_ROOT="$ROOT" \
        $NICE_PREFIX "$BATCH" --agent-name "$AG" >>"$LOG" 2>&1; then
      off=$end
      echo "$off" >"$off_file"
      [[ "${YIELD_MS:-0}" -gt 0 ]] && sleep "$(awk "BEGIN{print $YIELD_MS/1000}")"
    else
      log "$AG: chunk failed at offset $off; will retry next launch"
      echo paused >"$WORK/.status.$AG"; return 0
    fi
  done
  echo done >"$WORK/.status.$AG"
  return 0
}

log "parallel backfill: JOBS=$JOBS agents='$AGENTS'"
running=0
for AG in $AGENTS; do
  rm -f "$WORK/.status.$AG"
  backfill_agent "$AG" &
  running=$(( running + 1 ))
  if (( running >= JOBS )); then
    wait -n 2>/dev/null || wait
    running=$(( running - 1 ))
  fi
done
wait

all_done=1
for AG in $AGENTS; do
  st=$(cat "$WORK/.status.$AG" 2>/dev/null || echo paused)
  [[ "$st" == "paused" ]] && all_done=0
done

if (( all_done )); then
  touch "$DONE"
  log "daemon complete: all agents fully backfilled"
else
  log "daemon paused; relaunch (next SessionStart) resumes from offsets"
fi
