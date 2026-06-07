#!/usr/bin/env bash
set -euo pipefail
ROOT=/home/vj/tslink
OUT_DIR=${OUT_DIR:-/home/vj/tslink/output-ubuntu22/release-mtcache-ssd}
BUILD_DIR=${BUILD_DIR:-/home/vj/tslink/build-ubuntu22/release-mtcache-ssd}
SMOKE_DIR=${SMOKE_DIR:-/tmp/temporalstore-proxy-ssd-smoke}
SSD_PATH=${SSD_PATH:-/tmp/temporalstore-proxy-ssd-cache}
CLUSTER_NAME=${CLUSTER_NAME:-proxyssdsmoke}
NAMESPACE_NAME=${NAMESPACE_NAME:-ns_proxy_ssd}
TABLE_NAME=${TABLE_NAME:-tbl_proxy_ssd}
MS_PORT=${MS_PORT:-18200}
MS_RAFT_PORT=${MS_RAFT_PORT:-18210}
MS_SNAPSHOT_PORT=${MS_SNAPSHOT_PORT:-18220}
SERVER_PORT=${SERVER_PORT:-18201}
PROXY_PORT=${PROXY_PORT:-18290}
LAUNCHER_LOG=${SMOKE_DIR}.launcher.log
PROXY_LOG=${SMOKE_DIR}/proxy.log
cleanup() {
  local status=$?
  if [[ -f ${SMOKE_DIR}/proxy.pid ]]; then kill "$(cat ${SMOKE_DIR}/proxy.pid)" >/dev/null 2>&1 || true; fi
  if [[ -f ${SMOKE_DIR}.launcher.pid ]]; then kill "$(cat ${SMOKE_DIR}.launcher.pid)" >/dev/null 2>&1 || true; fi
  pkill -f "bcache2-proxy.*proxy_cluster_name=${CLUSTER_NAME}" >/dev/null 2>&1 || true
  pkill -f "bcache2-server.*cluster_name=${CLUSTER_NAME}" >/dev/null 2>&1 || true
  pkill -f "bcache2-metaserver.*metaserver_cluster_name=${CLUSTER_NAME}" >/dev/null 2>&1 || true
  return "$status"
}
trap cleanup EXIT
pkill -f "bcache2-proxy.*proxy_cluster_name=${CLUSTER_NAME}" >/dev/null 2>&1 || true
pkill -f "bcache2-server.*cluster_name=${CLUSTER_NAME}" >/dev/null 2>&1 || true
pkill -f "bcache2-metaserver.*metaserver_cluster_name=${CLUSTER_NAME}" >/dev/null 2>&1 || true
rm -rf "$SMOKE_DIR" "$SSD_PATH" "$LAUNCHER_LOG" "${SMOKE_DIR}.launcher.pid"
mkdir -p "$SMOKE_DIR"
SERVER_EXTRA_FLAGS="--enable_blockcache=true --ssd_engine_type=0 --blockcache_dram_capacity=8388608 --blockcache_ssd_capacity=67108864 --blockcache_ssd_path=${SSD_PATH} --blockcache_ssd_instance_only=true --blockcache_clear_ssd_folder=false"
(
  cd "$ROOT"
  SMOKE_DIR="$SMOKE_DIR" CLUSTER_NAME="$CLUSTER_NAME" NAMESPACE_NAME="$NAMESPACE_NAME" TABLE_NAME="$TABLE_NAME" \
  MS_PORT="$MS_PORT" MS_RAFT_PORT="$MS_RAFT_PORT" MS_SNAPSHOT_PORT="$MS_SNAPSHOT_PORT" SERVER_PORT="$SERVER_PORT" \
  OUT_DIR="$OUT_DIR" KEEP_RUNNING=1 SERVER_EXTRA_FLAGS="$SERVER_EXTRA_FLAGS" bash tools/smoke_ubuntu22.sh
) > "$LAUNCHER_LOG" 2>&1 &
echo $! > "${SMOKE_DIR}.launcher.pid"
for _ in $(seq 1 160); do
  if grep -q "TemporalStore Ubuntu smoke test passed" "$LAUNCHER_LOG"; then break; fi
  if ! kill -0 "$(cat ${SMOKE_DIR}.launcher.pid)" >/dev/null 2>&1; then
    echo launcher exited early >&2; tail -n 160 "$LAUNCHER_LOG" >&2 || true; exit 1
  fi
  sleep 0.5
done
grep -q "TemporalStore Ubuntu smoke test passed" "$LAUNCHER_LOG" || { echo timed out waiting for cluster >&2; tail -n 160 "$LAUNCHER_LOG" >&2 || true; exit 1; }
leader=$(awk '/metaserver leader:/ {print $3}' "$LAUNCHER_LOG")
leader=${leader:-127.0.0.1:${MS_PORT}}
mkdir -p "$SMOKE_DIR/proxy-log"
BYTED_HOST_IP=127.0.0.1 BYTED_HOST_IPV6= "$OUT_DIR/bcache2-proxy" \
  --proxy_cluster_name="$CLUSTER_NAME" \
  --metaserver_uri="$leader" \
  --master_endpoint="$leader" \
  --proxy_log_dir="$SMOKE_DIR/proxy-log" \
  --proxy_log_level=2 \
  --proxy_vregion=vregion \
  --proxy_vdc=vdc1 \
  --proxy_vau=vau1 \
  --port="$PROXY_PORT" \
  > "$SMOKE_DIR/proxy.stdout" 2> "$SMOKE_DIR/proxy.stderr" &
echo $! > "$SMOKE_DIR/proxy.pid"
for _ in $(seq 1 80); do
  if grep -q "Server.*Start" "$SMOKE_DIR/proxy.stderr" "$SMOKE_DIR/proxy.stdout" 2>/dev/null || kill -0 "$(cat $SMOKE_DIR/proxy.pid)" >/dev/null 2>&1; then
    sleep 1
    break
  fi
  sleep 0.25
done
"$BUILD_DIR/src/client/example/proxy_smoke_example" "127.0.0.1:${PROXY_PORT}" "$NAMESPACE_NAME" "$TABLE_NAME" proxy_smoke > "$SMOKE_DIR/proxy_smoke.log" 2>&1
cat "$SMOKE_DIR/proxy_smoke.log"
echo "PASS local proxy smoke"
echo "leader=${leader} proxy=127.0.0.1:${PROXY_PORT} smoke_dir=${SMOKE_DIR} ssd_path=${SSD_PATH}"
