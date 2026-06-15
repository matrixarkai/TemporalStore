#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${ROOT}/tools/temporalstore_runtime_env.sh"

BUILD_TYPE="${BUILD_TYPE:-Release}"
BUILD_FLAVOR="$(printf '%s' "${BUILD_TYPE}" | tr '[:upper:]' '[:lower:]')"
OUT_DIR="${OUT_DIR:-${ROOT}/output-ubuntu22/${BUILD_FLAVOR}}"
BIN_DIR="${BIN_DIR:-${ROOT}/build-ubuntu22/${BUILD_FLAVOR}/src/client/example}"
RESULT_DIR="${RESULT_DIR:-/tmp/temporalstore-data-raft-mixed-rw-$(date +%Y%m%d_%H%M%S)}"
SMOKE_DIR="${SMOKE_DIR:-${RESULT_DIR}/cluster}"
CLUSTER_NAME="${CLUSTER_NAME:-raft_mixed_rw}"
NAMESPACE_NAME="${NAMESPACE_NAME:-ns1}"
TABLE_NAME="${TABLE_NAME:-table1}"
IDC="${IDC:-vdc1}"
MS_PORT="${MS_PORT:-28100}"
SERVER_PORT="${SERVER_PORT:-14101}"
SERVER_COUNT="${SERVER_COUNT:-3}"
REPLICA_COUNT="${REPLICA_COUNT:-3}"
DATA_RAFT_RAFT_PORT_DELTA="${DATA_RAFT_RAFT_PORT_DELTA:-1000}"
DATA_RAFT_SNAPSHOT_PORT_DELTA="${DATA_RAFT_SNAPSHOT_PORT_DELTA:-2000}"
DATA_RAFT_BOUNDED_STALE_MAX_INDEX_LAG="${DATA_RAFT_BOUNDED_STALE_MAX_INDEX_LAG:-16}"
DATA_RAFT_PROPOSE_TIMEOUT_MS="${DATA_RAFT_PROPOSE_TIMEOUT_MS:-5000}"
SERVER_HEARTBEAT_INTERVAL_MS="${SERVER_HEARTBEAT_INTERVAL_MS:-500}"
SERVER_HEARTBEAT_TIMEOUT_MS="${SERVER_HEARTBEAT_TIMEOUT_MS:-1000}"
SERVER_META_TINKER_INTERVAL_MS="${SERVER_META_TINKER_INTERVAL_MS:-500}"
PROBE_OPS="${PROBE_OPS:-300}"
PROBE_THREADS="${PROBE_THREADS:-4}"
VALUE_BYTES="${VALUE_BYTES:-128}"
MAX_WAIT_MS="${MAX_WAIT_MS:-30000}"
BACKGROUND_WRITER_THREADS="${BACKGROUND_WRITER_THREADS:-2}"
BACKGROUND_READER_THREADS="${BACKGROUND_READER_THREADS:-4}"
BENCH_TIMEOUT_S="${BENCH_TIMEOUT_S:-300}"
REPLICATION_SMOKE_ATTEMPTS="${REPLICATION_SMOKE_ATTEMPTS:-30}"
REPLICATION_SMOKE_WAIT_MS="${REPLICATION_SMOKE_WAIT_MS:-30000}"
REPLICATION_SMOKE_POLL_MS="${REPLICATION_SMOKE_POLL_MS:-5}"
RAFT_STABILITY_SMOKE_COUNT="${RAFT_STABILITY_SMOKE_COUNT:-3}"

need_file() {
  if [[ ! -x "$1" ]]; then
    echo "missing executable: $1" >&2
    exit 1
  fi
}

need_file "${OUT_DIR}/bcache2-server"
need_file "${OUT_DIR}/bcache2-metaserver"
need_file "${BIN_DIR}/replication_smoke_example"
need_file "${BIN_DIR}/secondary_visibility_lag_benchmark"

preflight_port() {
  local port="$1"
  python3 - "${port}" <<'PY'
import socket
import sys

port = int(sys.argv[1])
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        sock.bind(("127.0.0.1", port))
    except OSError as exc:
        raise SystemExit(f"port {port} is not free: {exc}")
PY
}

csv_has_zero_errors() {
  local file="$1"
  awk -F, '
    $1 == "phase" {
      for (i = 1; i <= NF; ++i) {
        if ($i == "errors") err_col = i
      }
      next
    }
    $1 ~ /^secondary_/ {
      rows += 1
      if (err_col == 0 || $err_col != 0) bad += 1
    }
    $1 == "background" && $2 == "writes" {
      next
    }
    $1 == "background" && NF >= 5 {
      if ($4 != 0 || $5 != 0) bad += 1
    }
    END {
      exit (rows >= 2 && bad == 0) ? 0 : 1
    }
  ' "${file}"
}

run_replication_smoke_with_retries() {
  local out="$1"
  local err="$2"
  local code=1

  for attempt in $(seq 1 "${REPLICATION_SMOKE_ATTEMPTS}"); do
    set +e
    timeout 120 \
      "${BIN_DIR}/replication_smoke_example" \
      "${leader}" "${IDC}" "${NAMESPACE_NAME}" "${TABLE_NAME}" \
      "${REPLICATION_SMOKE_WAIT_MS}" "${REPLICATION_SMOKE_POLL_MS}" \
      > "${out}" 2> "${err}"
    code=$?
    set -e
    if [[ "${code}" == "0" ]]; then
      return 0
    fi
    if ! grep -q "Slot not found" "${err}"; then
      return "${code}"
    fi
    echo "replication smoke attempt ${attempt} hit Slot not found; waiting for data-node slot install" \
      | tee -a "${RESULT_DIR}/summary.txt"
    sleep 1
  done

  return "${code}"
}

cleanup() {
  local status=$?
  if [[ -f "${RESULT_DIR}/bootstrap.pid" ]]; then
    kill "$(cat "${RESULT_DIR}/bootstrap.pid")" >/dev/null 2>&1 || true
  fi
  for pid_file in "${SMOKE_DIR}"/server*.pid "${SMOKE_DIR}"/metaserver*.pid; do
    [[ -f "${pid_file}" ]] || continue
    kill "$(cat "${pid_file}")" >/dev/null 2>&1 || true
  done
  wait >/dev/null 2>&1 || true
  return "${status}"
}
trap cleanup EXIT

if (( SERVER_PORT + SERVER_COUNT - 1 + DATA_RAFT_RAFT_PORT_DELTA > 65535 ||
      SERVER_PORT + SERVER_COUNT - 1 + DATA_RAFT_SNAPSHOT_PORT_DELTA > 65535 )); then
  echo "data raft transport ports exceed 65535; lower SERVER_PORT or deltas" >&2
  exit 2
fi

preflight_port "${MS_PORT}"
preflight_port "$((MS_PORT + 10))"
preflight_port "$((MS_PORT + 20))"
for i in $(seq 0 "$((SERVER_COUNT - 1))"); do
  preflight_port "$((SERVER_PORT + i))"
  preflight_port "$((SERVER_PORT + i + DATA_RAFT_RAFT_PORT_DELTA))"
  preflight_port "$((SERVER_PORT + i + DATA_RAFT_SNAPSHOT_PORT_DELTA))"
done

mkdir -p "${RESULT_DIR}"
rm -rf "${SMOKE_DIR}"

(
  cd "${ROOT}"
  env \
    BUILD_TYPE="${BUILD_TYPE}" \
    OUT_DIR="${OUT_DIR}" \
    SMOKE_DIR="${SMOKE_DIR}" \
    CLUSTER_NAME="${CLUSTER_NAME}" \
    NAMESPACE_NAME="${NAMESPACE_NAME}" \
    TABLE_NAME="${TABLE_NAME}" \
    META_COUNT=1 \
    SERVER_COUNT="${SERVER_COUNT}" \
    REPLICA_COUNT="${REPLICA_COUNT}" \
    MS_PORT="${MS_PORT}" \
    MS_RAFT_PORT="$((MS_PORT + 10))" \
    MS_SNAPSHOT_PORT="$((MS_PORT + 20))" \
    SERVER_PORT="${SERVER_PORT}" \
    TABLE_ELECTION_POLICY=PROMOTE_SECONDARY \
    TABLE_PARTITION_UNIT_RELATION=ANTI_ENTROPY \
    SERVER_EXTRA_FLAGS="--data_replication_mode=raft_consensus --data_raft_work_dir=${SMOKE_DIR}/data-raft --data_raft_raft_port_delta=${DATA_RAFT_RAFT_PORT_DELTA} --data_raft_snapshot_port_delta=${DATA_RAFT_SNAPSHOT_PORT_DELTA} --data_raft_enable_empty_snapshot_for_tests=false --data_raft_read_mode=bounded_stale --data_raft_bounded_stale_max_index_lag=${DATA_RAFT_BOUNDED_STALE_MAX_INDEX_LAG} --data_raft_propose_timeout_ms=${DATA_RAFT_PROPOSE_TIMEOUT_MS} --server_heartbeat_interval_ms=${SERVER_HEARTBEAT_INTERVAL_MS} --server_heartbeat_timeout_ms=${SERVER_HEARTBEAT_TIMEOUT_MS} --server_meta_tinker_interval_ms=${SERVER_META_TINKER_INTERVAL_MS} --storage_async=true --storage_enable_evict=false --storage_enable_expire=false --storage_enable_page_gc=false --storage_enable_page_compaction=false --storage_enable_index_gc=false --storage_enable_oplog_rolling=false" \
    KEEP_RUNNING=1 \
    bash tools/smoke_ubuntu22.sh
) > "${RESULT_DIR}/bootstrap.log" 2>&1 &
echo "$!" > "${RESULT_DIR}/bootstrap.pid"

for _ in $(seq 1 180); do
  if grep -q "KEEP_RUNNING=1" "${RESULT_DIR}/bootstrap.log" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$(cat "${RESULT_DIR}/bootstrap.pid")" >/dev/null 2>&1; then
    echo "bootstrap exited early" >&2
    cat "${RESULT_DIR}/bootstrap.log" >&2 || true
    exit 1
  fi
  sleep 1
done

if ! grep -q "KEEP_RUNNING=1" "${RESULT_DIR}/bootstrap.log"; then
  echo "bootstrap timed out" >&2
  tail -120 "${RESULT_DIR}/bootstrap.log" >&2 || true
  exit 1
fi

leader="$(awk '/metaserver leader:/ {print $3}' "${RESULT_DIR}/bootstrap.log")"
if [[ -z "${leader}" ]]; then
  echo "could not parse metaserver leader" >&2
  cat "${RESULT_DIR}/bootstrap.log" >&2
  exit 1
fi

echo "result_dir=${RESULT_DIR}" | tee "${RESULT_DIR}/summary.txt"
echo "leader=${leader}" | tee -a "${RESULT_DIR}/summary.txt"
echo "read_policy=secondary_preferred" | tee -a "${RESULT_DIR}/summary.txt"
echo "write_policy=primary_leader" | tee -a "${RESULT_DIR}/summary.txt"
echo "data_raft_bounded_stale_max_index_lag=${DATA_RAFT_BOUNDED_STALE_MAX_INDEX_LAG}" \
  | tee -a "${RESULT_DIR}/summary.txt"
echo "server_heartbeat_interval_ms=${SERVER_HEARTBEAT_INTERVAL_MS}" \
  | tee -a "${RESULT_DIR}/summary.txt"
echo "server_heartbeat_timeout_ms=${SERVER_HEARTBEAT_TIMEOUT_MS}" \
  | tee -a "${RESULT_DIR}/summary.txt"
echo "server_meta_tinker_interval_ms=${SERVER_META_TINKER_INTERVAL_MS}" \
  | tee -a "${RESULT_DIR}/summary.txt"

for smoke_idx in $(seq 1 "${RAFT_STABILITY_SMOKE_COUNT}"); do
  run_replication_smoke_with_retries \
    "${RESULT_DIR}/replication_smoke_${smoke_idx}.out" \
    "${RESULT_DIR}/replication_smoke_${smoke_idx}.err"
  cat "${RESULT_DIR}/replication_smoke_${smoke_idx}.out" | tee -a "${RESULT_DIR}/summary.txt"
done

timeout "${BENCH_TIMEOUT_S}" \
  "${BIN_DIR}/secondary_visibility_lag_benchmark" \
  "${leader}" "${IDC}" "${NAMESPACE_NAME}" "${TABLE_NAME}" \
  "${PROBE_OPS}" "${PROBE_THREADS}" "${VALUE_BYTES}" "${MAX_WAIT_MS}" \
  "${BACKGROUND_WRITER_THREADS}" "${BACKGROUND_READER_THREADS}" \
  > "${RESULT_DIR}/mixed_visibility.out" 2> "${RESULT_DIR}/mixed_visibility.err"
csv_has_zero_errors "${RESULT_DIR}/mixed_visibility.out"
cat "${RESULT_DIR}/mixed_visibility.out" | tee -a "${RESULT_DIR}/summary.txt"

echo "PASS data-raft mixed read/write follower-read"
echo "${RESULT_DIR}"
