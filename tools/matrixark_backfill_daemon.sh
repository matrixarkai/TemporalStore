#!/usr/bin/env bash
#
# Async, resumable first-start backfill of local Claude/Codex context into the
# live TemporalStore hook stores. Launched detached from the SessionStart hook
# (opt-in: MATRIXARK_BACKFILL_ON_START=1) so it never blocks a turn. Safe to run
# repeatedly: a lockfile prevents overlap, a per-agent offset marker makes it
# resume where it left off after a teardown, and the engine dedups by content
# hash so re-processed records never duplicate.
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
TARGET_DIR="${CARGO_TARGET_DIR:-/tmp/temporalstore-context-workflow-target}"
BATCH="$TARGET_DIR/debug/context_batch_ingest"
# Low-priority execution so real-time (live-hook) ingestion is never starved by
# the backfill: nice lowers CPU priority, ionice (if present) lowers IO priority,
# and a short yield between chunks releases the store lock for any live write.
NICE_PREFIX="nice -n ${MATRIXARK_BACKFILL_NICE:-19}"
command -v ionice >/dev/null 2>&1 && NICE_PREFIX="ionice -c3 $NICE_PREFIX"
YIELD_MS="${MATRIXARK_BACKFILL_YIELD_MS:-200}"
INGESTER="$REPO_ROOT/tools/matrixark_local_backfill_ingester.py"
WORK="${MATRIXARK_BACKFILL_WORK:-/tmp/matrixark-backfill}"
CHUNK="${MATRIXARK_BACKFILL_CHUNK:-400}"
AGENTS="${MATRIXARK_BACKFILL_AGENTS:-claude codex}"
LOCK="$WORK/.lock"
DONE="$WORK/.done"
LOG="$WORK/daemon.log"

mkdir -p "$WORK"
exec 9>"$LOCK" 2>/dev/null || exit 0
flock -n 9 || exit 0                      # another daemon already running
[[ -f "$DONE" ]] && exit 0                # already fully backfilled

log() { echo "[$(date -u +%FT%TZ)] $*" >>"$LOG"; }
log "daemon start (chunk=$CHUNK agents='$AGENTS')"

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
agent_root()    { echo "/tmp/temporalstore-rust-$1-hook"; }

all_done=1
for AG in $AGENTS; do
  SRC="$WORK/backfill.$AG.jsonl"
  [[ -f "$SRC" ]] || continue
  total=$(wc -l <"$SRC")
  off_file="$WORK/.offset.$AG"
  off=$(cat "$off_file" 2>/dev/null || echo 0)
  ROOT="$(agent_root "$AG")"
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
      all_done=0
      break
    fi
  done
  (( off < total )) && all_done=0
done

if (( all_done )); then
  touch "$DONE"
  log "daemon complete: all agents fully backfilled"
else
  log "daemon paused; relaunch (next SessionStart) resumes from offsets"
fi
