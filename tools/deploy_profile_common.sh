#!/usr/bin/env bash
# =============================================================================
# deploy_profile_common.sh - the shared body of the three profile launchers.
#
# TemporalStore ships three deployment profiles, and they differ in exactly two
# things: how many nodes there are, and whether a node has to reach across a
# distance to the authoritative copy.
#
#   one-box         1 node    memory + local disk (EBS)
#   raft            N nodes   memory + local disk (EBS), per node
#   shared-storage  N nodes   memory + local disk + one shared store
#
# One box and raft are the SAME storage shape - both keep the durable copy on
# the node's own disk, so neither wants the on-disk cache tier, which exists to
# span the distance to a shared store. They differ in node count, and that is
# the whole difference. Scaling out defaults to raft; shared storage is opt-in.
#
# Everything below this line is identical across the three profiles on purpose.
# The performance flags are set EXPLICITLY rather than left to their defaults:
# several of them still default off in the engine, and a deployment that is
# "fast when the defaults happen to be right" is not reproducible.
#
# Sourced, never run directly. A profile script sets TS_PROFILE_EXPECT and the
# handful of vars its shape requires, then calls ts_profile_launch.
# =============================================================================
set -euo pipefail

TS_PROFILE_BIN="${TS_PROFILE_BIN:-/opt/temporalstore/bin/matrixark_rust_datanode}"
TS_PROFILE_DATA="${TS_PROFILE_DATA:-/var/lib/temporalstore}"
TS_PROFILE_LOG="${TS_PROFILE_LOG:-${TS_PROFILE_DATA}/node.log}"
TS_PROFILE_WAIT_S="${TS_PROFILE_WAIT_S:-20}"

# --- performance flags: live on every profile --------------------------------
ts_profile_perf_flags() {
  # WAL: binary framing and binary records. The WAL carries outcomes, not
  # commands, and encodes them as protobuf rather than JSON.
  export TS_WAL_BINARY_FRAME="${TS_WAL_BINARY_FRAME:-1}"
  export TS_WAL_BINARY_RECORDS="${TS_WAL_BINARY_RECORDS:-1}"
  export TS_WAL_OUTCOME_ITEMS="${TS_WAL_OUTCOME_ITEMS:-1}"
  export TS_WAL_DATA_ONLY="${TS_WAL_DATA_ONLY:-1}"
  # One fsync barrier per commit instead of the historical three.
  export TS_WAL_SINGLE_BARRIER="${TS_WAL_SINGLE_BARRIER:-1}"
  # Grow the WAL in chunks rather than a syscall per append.
  export TS_WAL_PREALLOCATE="${TS_WAL_PREALLOCATE:-1}"

  # Index log: binary codec rather than JSON lines.
  export TS_INDEX_BINARY="${TS_INDEX_BINARY:-1}"

  # Embeddings: uniform scale=1e4 quantization, 1024 B for a 384-dim vector.
  #
  # This is the encoding that SHIPS, and it is not int8. Measured over 713 real
  # document chunks with 8 bilingual queries, uniform scaling held every query's
  # top-1 and the exact order of all 10 hits; int8 kept a 0.99983 reconstruction
  # cosine and still lost one top-1 and reordered 5 of 8 result lists. Uniform
  # scaling is rank-preserving and per-vector peak scaling is not, and rank is
  # what retrieval serves. Set TS_VECTOR_INT8=1 to trade that for half the bytes.
  export TS_VECTOR_SCALED="${TS_VECTOR_SCALED:-1}"

  # Drain dirty rows into embeddings/extraction twice a second, so a write is
  # searchable without waiting for a batch job.
  export MATRIXARK_EMBED_DRAINER="${MATRIXARK_EMBED_DRAINER:-1}"
  export MATRIXARK_EMBED_DRAINER_INTERVAL_MS="${MATRIXARK_EMBED_DRAINER_INTERVAL_MS:-500}"

  # Page store compression.
  export TS_PAGE_STORE_COMPRESSION_ENABLED="${TS_PAGE_STORE_COMPRESSION_ENABLED:-1}"
}

# --- data directories --------------------------------------------------------
ts_profile_dirs() {
  export TS_PAGE_STORE_DIR="${TS_PAGE_STORE_DIR:-${TS_PROFILE_DATA}/pages}"
  export TS_CACHE_DIR="${TS_CACHE_DIR:-${TS_PROFILE_DATA}/cache}"
  export TS_INDEX_DIR="${TS_INDEX_DIR:-${TS_PROFILE_DATA}/index}"
  export TS_BLOB_STORE_DIR="${TS_BLOB_STORE_DIR:-${TS_PROFILE_DATA}/pages/blobs}"
  mkdir -p "$TS_PAGE_STORE_DIR" "$TS_CACHE_DIR" "$TS_INDEX_DIR" "$TS_BLOB_STORE_DIR" \
           "$(dirname "$TS_PROFILE_LOG")"
}

# --- launch, then make the node prove which profile it resolved --------------
#
# The node derives its profile from the topology it is actually running, so
# reading it back is the only way to know the script's intent survived. A
# launcher that announces "one-box" while the node resolved shared-storage is
# precisely the confusion these three scripts exist to end - so it is checked,
# and a mismatch is a failed deploy rather than a surprise weeks later.
ts_profile_launch() {
  : "${TS_PROFILE_EXPECT:?profile script must set TS_PROFILE_EXPECT}"
  [[ -x "$TS_PROFILE_BIN" ]] || { echo "[deploy] no datanode at ${TS_PROFILE_BIN}" >&2; exit 1; }
  ts_profile_perf_flags
  ts_profile_dirs

  echo "[deploy] profile=${TS_PROFILE_EXPECT} data=${TS_PROFILE_DATA} bin=${TS_PROFILE_BIN}"
  setsid nohup "$TS_PROFILE_BIN" >>"$TS_PROFILE_LOG" 2>&1 </dev/null &
  local pid=$!

  local waited=0 line=""
  while (( waited < TS_PROFILE_WAIT_S )); do
    line="$(grep -a 'deployment profile' "$TS_PROFILE_LOG" | tail -1 || true)"
    [[ -n "$line" ]] && break
    kill -0 "$pid" 2>/dev/null || { echo "[deploy] datanode exited during startup" >&2
                                    tail -20 "$TS_PROFILE_LOG" >&2; exit 1; }
    sleep 1; waited=$((waited + 1))
  done
  [[ -n "$line" ]] || { echo "[deploy] node never announced a profile in ${TS_PROFILE_WAIT_S}s" >&2
                        tail -20 "$TS_PROFILE_LOG" >&2; exit 1; }

  local got
  got="$(sed -n 's/.*profile=\([a-z-]*\).*/\1/p' <<<"$line")"
  if [[ "$got" != "$TS_PROFILE_EXPECT" ]]; then
    echo "[deploy] FAILED: asked for '${TS_PROFILE_EXPECT}', node resolved '${got}'" >&2
    echo "  $line" >&2
    kill "$pid" 2>/dev/null || true
    exit 1
  fi
  echo "[deploy] ok pid=${pid}"
  echo "  $line"
}
