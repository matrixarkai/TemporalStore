#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${ROOT}/tools/temporalstore_runtime_env.sh"

BUILD_TYPE="${BUILD_TYPE:-Release}"
BUILD_FLAVOR="$(printf '%s' "${BUILD_TYPE}" | tr '[:upper:]' '[:lower:]')"
OUT_DIR="${OUT_DIR:-${ROOT}/output-ubuntu22/${BUILD_FLAVOR}}"
BIN_DIR="${BIN_DIR:-${ROOT}/build-ubuntu22/${BUILD_FLAVOR}/src/client/example}"
RESULT_DIR="${RESULT_DIR:-/tmp/temporalstore-data-raft-2node-scale-$(date +%Y%m%d_%H%M%S)}"
SMOKE_DIR="${SMOKE_DIR:-${RESULT_DIR}/cluster}"
CLUSTER_NAME="${CLUSTER_NAME:-raft_2node_scale}"
NAMESPACE_NAME="${NAMESPACE_NAME:-ns1}"
TABLE_NAME="${TABLE_NAME:-table1}"
IDC="${IDC:-vdc1}"
MS_PORT="${MS_PORT:-34100}"
SERVER_PORT="${SERVER_PORT:-17101}"
DATA_RAFT_RAFT_PORT_DELTA="${DATA_RAFT_RAFT_PORT_DELTA:-1000}"
DATA_RAFT_SNAPSHOT_PORT_DELTA="${DATA_RAFT_SNAPSHOT_PORT_DELTA:-2000}"
OPS="${OPS:-4000}"
THREAD_LIST="${THREAD_LIST:-1 2}"
THREAD_LIST="${THREAD_LIST//,/ }"
VALUE_BYTES="${VALUE_BYTES:-128}"
BENCH_TIMEOUT_S="${BENCH_TIMEOUT_S:-120}"

need_file() {
  if [[ ! -x "$1" ]]; then
    echo "missing executable: $1" >&2
    exit 1
  fi
}

need_file "${OUT_DIR}/bcache2-server"
need_file "${OUT_DIR}/bcache2-metaserver"
need_file "${BIN_DIR}/replication_smoke_example"
need_file "${BIN_DIR}/string_scale_benchmark"

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

if (( SERVER_PORT + 1 + DATA_RAFT_RAFT_PORT_DELTA > 65535 ||
      SERVER_PORT + 1 + DATA_RAFT_SNAPSHOT_PORT_DELTA > 65535 )); then
  echo "data raft transport ports exceed 65535; lower SERVER_PORT or deltas" >&2
  echo "server2 raft=$((SERVER_PORT + 1 + DATA_RAFT_RAFT_PORT_DELTA)) snapshot=$((SERVER_PORT + 1 + DATA_RAFT_SNAPSHOT_PORT_DELTA))" >&2
  exit 2
fi

for i in 0 1; do
  preflight_port "$((SERVER_PORT + i))"
  preflight_port "$((SERVER_PORT + i + DATA_RAFT_RAFT_PORT_DELTA))"
  preflight_port "$((SERVER_PORT + i + DATA_RAFT_SNAPSHOT_PORT_DELTA))"
done
preflight_port "${MS_PORT}"
preflight_port "$((MS_PORT + 10))"
preflight_port "$((MS_PORT + 20))"

cleanup() {
  local status=$?
  if [[ -f "${RESULT_DIR}/bootstrap.pid" ]]; then
    kill "$(cat "${RESULT_DIR}/bootstrap.pid")" >/dev/null 2>&1 || true
  fi
  for pid_file in "${SMOKE_DIR}"/server*.pid "${SMOKE_DIR}"/metaserver*.pid; do
    [[ -f "${pid_file}" ]] || continue
    kill "$(cat "${pid_file}")" >/dev/null 2>&1 || true
  done
  sleep 0.2
  return "${status}"
}
trap cleanup EXIT

run_replication_smoke_with_retries() {
  local out="$1"
  local err="$2"
  local code=1

  for attempt in $(seq 1 60); do
    set +e
    timeout 120 \
      "${BIN_DIR}/replication_smoke_example" "${leader}" "${IDC}" "${NAMESPACE_NAME}" \
      "${TABLE_NAME}" \
      > "${out}" 2> "${err}"
    code=$?
    set -e
    if [[ "${code}" == "0" ]]; then
      return 0
    fi
    if ! grep -q "Slot not found" "${err}"; then
      return "${code}"
    fi
    echo "replication smoke attempt ${attempt} hit Slot not found; waiting for slot install" \
      | tee -a "${RESULT_DIR}/summary.txt"
    sleep 1
  done

  return "${code}"
}

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
    SERVER_COUNT=2 \
    REPLICA_COUNT=2 \
    MS_PORT="${MS_PORT}" \
    MS_RAFT_PORT="$((MS_PORT + 10))" \
    MS_SNAPSHOT_PORT="$((MS_PORT + 20))" \
    SERVER_PORT="${SERVER_PORT}" \
    TABLE_ELECTION_POLICY=PROMOTE_SECONDARY \
    TABLE_PARTITION_UNIT_RELATION=ANTI_ENTROPY \
    SERVER_EXTRA_FLAGS="--data_replication_mode=raft_consensus --data_raft_work_dir=${SMOKE_DIR}/data-raft --data_raft_raft_port_delta=${DATA_RAFT_RAFT_PORT_DELTA} --data_raft_snapshot_port_delta=${DATA_RAFT_SNAPSHOT_PORT_DELTA} --data_raft_enable_empty_snapshot_for_tests=false --data_raft_read_mode=bounded_stale --data_raft_bounded_stale_max_index_lag=16 --data_raft_propose_timeout_ms=5000 --storage_async=true --storage_enable_evict=false --storage_enable_expire=false --storage_enable_page_gc=false --storage_enable_page_compaction=false --storage_enable_index_gc=false --storage_enable_oplog_rolling=false" \
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
echo "== replication smoke ==" | tee -a "${RESULT_DIR}/summary.txt"
set +e
run_replication_smoke_with_retries \
  "${RESULT_DIR}/replication_smoke.out" \
  "${RESULT_DIR}/replication_smoke.err"
replication_code=$?
set -e
cat "${RESULT_DIR}/replication_smoke.out" "${RESULT_DIR}/replication_smoke.err" \
  | tee -a "${RESULT_DIR}/summary.txt"
if [[ "${replication_code}" != "0" ]]; then
  echo "REPLICATION_SMOKE_FAILED code=${replication_code}" | tee -a "${RESULT_DIR}/summary.txt"
  exit 4
fi

echo "threads,set_qps,set_p50_us,set_p95_us,set_p99_us,get_qps,get_p50_us,get_p95_us,get_p99_us,errors,exit_code" \
  | tee "${RESULT_DIR}/results.csv"

failed=0
for threads in ${THREAD_LIST}; do
  out="${RESULT_DIR}/string_t${threads}.out"
  err="${RESULT_DIR}/string_t${threads}.err"
  echo "RUN string threads=${threads}" | tee -a "${RESULT_DIR}/summary.txt"
  set +e
  timeout "${BENCH_TIMEOUT_S}" \
    "${BIN_DIR}/string_scale_benchmark" "${leader}" "${IDC}" "${NAMESPACE_NAME}" "${TABLE_NAME}" \
    "${OPS}" "${threads}" "${VALUE_BYTES}" 1 1000 > "${out}" 2> "${err}"
  code=$?
  set -e
  echo "${code}" > "${RESULT_DIR}/string_t${threads}.exit_code"
  if [[ "${code}" != "0" ]]; then
    failed=1
    echo "STRING_SCALE_FAILED threads=${threads} code=${code}" | tee -a "${RESULT_DIR}/summary.txt"
    cat "${err}" | tee -a "${RESULT_DIR}/summary.txt"
  fi
  python3 - "${out}" "${threads}" "${code}" <<'PY' | tee -a "${RESULT_DIR}/results.csv"
import csv
import sys

path, threads, code = sys.argv[1], sys.argv[2], sys.argv[3]
setrow = getrow = None
try:
    rows = list(csv.reader(open(path, encoding="utf-8")))
except FileNotFoundError:
    rows = []
for row in rows:
    if row and row[0] == "TemporalStore" and row[1] == "set":
        setrow = row
    if row and row[0] == "TemporalStore" and row[1] in ("get", "get_raw_success_attempt"):
        getrow = row
if not setrow or not getrow:
    print(",".join([threads, "", "", "", "", "", "", "", "", "1", code]))
else:
    errors = int(setrow[5]) + int(getrow[5])
    print(",".join([threads, setrow[6], setrow[8], setrow[9], setrow[10],
                    getrow[6], getrow[8], getrow[9], getrow[10], str(errors), code]))
PY
done

echo "process_snapshot" | tee -a "${RESULT_DIR}/summary.txt"
ps -o pid,pcpu,pmem,rss,vsz,cmd -p "$(tr '\n' ',' < <(cat "${SMOKE_DIR}"/metaserver*.pid "${SMOKE_DIR}"/server*.pid) | sed 's/,$//')" \
  | tee -a "${RESULT_DIR}/summary.txt" || true

if [[ "${failed}" != "0" ]]; then
  echo "FAIL data-raft 2-node scale"
  echo "${RESULT_DIR}"
  exit 1
fi

echo "PASS data-raft 2-node scale"
echo "${RESULT_DIR}"
