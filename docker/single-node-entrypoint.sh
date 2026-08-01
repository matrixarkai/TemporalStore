#!/usr/bin/env bash
# Single-node TemporalStore supervisor.
#
# Runs the Rust metaserver + datanode together in one container and keeps the
# container alive for as long as both processes stay up. This is the simplest
# way to get a working local TemporalStore for development; it is not a
# production HA topology.
set -euo pipefail

DATA_DIR="${TS_DATA_DIR:-/var/lib/temporalstore}"
META_PORT="${TS_META_PORT:-17101}"
DATA_PORT="${TS_DATA_PORT:-17102}"

# Bind on all interfaces so the ports are reachable from the host and from other
# containers. The two processes talk to each other over localhost *inside* the
# container, so the advertised/meta addresses stay on 127.0.0.1.
export TS_META_BIND_ADDR="0.0.0.0:${META_PORT}"
export TS_META_ADDR="127.0.0.1:${META_PORT}"
export TS_SERVER_BIND_ADDR="0.0.0.0:${DATA_PORT}"
export TS_SERVER_ADVERTISE_ADDR="127.0.0.1:${DATA_PORT}"
export TS_SHARD_ID="${TS_SHARD_ID:-1}"
export TS_CACHE_DIR="${DATA_DIR}/cache"
export TS_PAGE_STORE_DIR="${DATA_DIR}/pages"
export TS_INDEX_DIR="${DATA_DIR}/indexes"
export TS_REPLICA_REPLAY_CURSOR_DIR="${DATA_DIR}/replica-replay-cursors"
export TS_CACHE_MEMORY_BYTES="${TS_CACHE_MEMORY_BYTES:-67108864}"

mkdir -p "$TS_CACHE_DIR" "$TS_PAGE_STORE_DIR" "$TS_INDEX_DIR" \
  "$TS_REPLICA_REPLAY_CURSOR_DIR"

pids=()
cleanup() { kill "${pids[@]}" 2>/dev/null || true; }
trap cleanup EXIT INT TERM

echo "[temporalstore] starting metaserver on 0.0.0.0:${META_PORT}"
matrixark_rust_metaserver &
pids+=("$!")
sleep 1

echo "[temporalstore] starting datanode on 0.0.0.0:${DATA_PORT}"
matrixark_rust_datanode &
pids+=("$!")

echo "[temporalstore] single node up:"
echo "[temporalstore]   metaserver  http://0.0.0.0:${META_PORT}/health"
echo "[temporalstore]   datanode    http://0.0.0.0:${DATA_PORT}/health  (writes/reads via POST /execute)"

# Take the container down as soon as either service exits, so orchestrators can
# restart a broken node instead of leaving a half-dead one running.
wait -n
echo "[temporalstore] a service exited; shutting the node down" >&2
exit 1
