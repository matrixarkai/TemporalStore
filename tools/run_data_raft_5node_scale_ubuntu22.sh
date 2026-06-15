#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${ROOT}/tools/temporalstore_runtime_env.sh"

BUILD_TYPE="${BUILD_TYPE:-Release}"
BUILD_FLAVOR="$(printf '%s' "${BUILD_TYPE}" | tr '[:upper:]' '[:lower:]')"
OUT_DIR="${OUT_DIR:-${ROOT}/output-ubuntu22/${BUILD_FLAVOR}}"
BIN_DIR="${BIN_DIR:-${ROOT}/build-ubuntu22/${BUILD_FLAVOR}/src/client/example}"
RESULT_DIR="${RESULT_DIR:-/tmp/temporalstore-data-raft-5node-scale-$(date +%Y%m%d_%H%M%S)}"
SMOKE_DIR="${SMOKE_DIR:-${RESULT_DIR}/cluster}"
CLUSTER_NAME="${CLUSTER_NAME:-raft_5node_scale}"
NAMESPACE_NAME="${NAMESPACE_NAME:-ns1}"
TABLE_NAME="${TABLE_NAME:-table1}"
IDC="${IDC:-vdc1}"
MS_PORT="${MS_PORT:-38100}"
SERVER_PORT="${SERVER_PORT:-13100}"
DATA_RAFT_RAFT_PORT_DELTA="${DATA_RAFT_RAFT_PORT_DELTA:-1000}"
DATA_RAFT_SNAPSHOT_PORT_DELTA="${DATA_RAFT_SNAPSHOT_PORT_DELTA:-2000}"
OPS="${OPS:-1000}"
THREAD_LIST="${THREAD_LIST:-1 2}"
THREAD_LIST="${THREAD_LIST//,/ }"
VALUE_BYTES="${VALUE_BYTES:-128}"
BENCH_TIMEOUT_S="${BENCH_TIMEOUT_S:-180}"

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
  for pid_file in "${SMOKE_DIR}"/server*.pid "${SMOKE_DIR}"/metaserver*.pid; do
    [[ -f "${pid_file}" ]] || continue
    kill -9 "$(cat "${pid_file}")" >/dev/null 2>&1 || true
  done
  return "${status}"
}
trap cleanup EXIT

run_replication_smoke_with_retries() {
  local out="$1"
  local err="$2"
  local code=1
  for attempt in $(seq 1 90); do
    set +e
    timeout 120 "${BIN_DIR}/replication_smoke_example" "${leader}" "${IDC}" \
      "${NAMESPACE_NAME}" "${TABLE_NAME}" > "${out}" 2> "${err}"
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

parse_partition_distribution() {
  local input_file="$1"
  local output_file="$2"
  python3 - "${input_file}" "${output_file}" <<'PY'
import collections
import json
import sys

with open(sys.argv[1], encoding="utf-8") as fh:
    data = json.load(fh)
counts = collections.Counter()
roles = collections.Counter()
for table in data.get("info", []):
    for partition in table.get("partition_info", []):
        port = partition.get("placement_actual", {}).get("server", {}).get("port")
        if port:
            counts[int(port)] += 1
            roles[(int(port), partition.get("role", ""))] += 1
with open(sys.argv[2], "w", encoding="utf-8") as out:
    out.write("server_port,partition_count,primary_count,secondary_count\n")
    for port in sorted(counts):
        out.write(f"{port},{counts[port]},{roles[(port, 'PARTITION_ROLE_PRIMARY')]},{roles[(port, 'PARTITION_ROLE_SECONDARY')]}\n")
PY
}

mkdir -p "${RESULT_DIR}"
rm -rf "${SMOKE_DIR}"

if (( SERVER_PORT + 4 + DATA_RAFT_RAFT_PORT_DELTA > 65535 ||
      SERVER_PORT + 4 + DATA_RAFT_SNAPSHOT_PORT_DELTA > 65535 )); then
  echo "data raft transport ports exceed 65535; lower SERVER_PORT or deltas" >&2
  exit 2
fi

preflight_port "${MS_PORT}"
preflight_port "$((MS_PORT + 10))"
preflight_port "$((MS_PORT + 20))"
for i in 0 1 2 3 4; do
  preflight_port "$((SERVER_PORT + i))"
  preflight_port "$((SERVER_PORT + i + DATA_RAFT_RAFT_PORT_DELTA))"
  preflight_port "$((SERVER_PORT + i + DATA_RAFT_SNAPSHOT_PORT_DELTA))"
done

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
    SERVER_COUNT=5 \
    REPLICA_COUNT=3 \
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
  tail -120 "${RESULT_DIR}/bootstrap.log" >&2 || true
  exit 1
fi

request_id="{\"cluster_name\":\"${CLUSTER_NAME}\",\"operator_name\":\"raft_5node_scale\"}"
list_server_body="{\"id\":${request_id},\"read_stale\":false,\"list_all_tag\":true}"
list_partition_body="{\"id\":${request_id},\"read_stale\":false,\"namespace_name\":\"${NAMESPACE_NAME}\",\"table_name\":\"${TABLE_NAME}\"}"

echo "result_dir=${RESULT_DIR}" | tee "${RESULT_DIR}/summary.txt"
echo "leader=${leader}" | tee -a "${RESULT_DIR}/summary.txt"

curl -fsS -m 5 -H "Content-Type: application/json" -d "${list_server_body}" \
  "http://127.0.0.1:${MS_PORT}/QueryService/ListServer" > "${RESULT_DIR}/list_server.json"
python3 - "${RESULT_DIR}/list_server.json" <<'PY'
import json
import sys

data = json.load(open(sys.argv[1], encoding="utf-8"))
normal = [
    s for s in data.get("servers", [])
    if s.get("server_info", {}).get("state") == "SERVER_NORMAL"
]
if len(normal) < 5:
    raise SystemExit(f"expected 5 normal servers, got {len(normal)}: {data}")
PY
echo "normal_servers=5" | tee -a "${RESULT_DIR}/summary.txt"

curl -fsS -m 5 -H "Content-Type: application/json" -d "${list_partition_body}" \
  "http://127.0.0.1:${MS_PORT}/QueryService/ListPartition" > "${RESULT_DIR}/list_partition.json"
parse_partition_distribution "${RESULT_DIR}/list_partition.json" "${RESULT_DIR}/partition_distribution.csv"
cat "${RESULT_DIR}/partition_distribution.csv" | tee -a "${RESULT_DIR}/summary.txt"
python3 - "${RESULT_DIR}/partition_distribution.csv" <<'PY'
import csv
import sys

rows = list(csv.DictReader(open(sys.argv[1], encoding="utf-8")))
if len(rows) < 3:
    raise SystemExit(f"expected table replicas on at least 3 servers, got {len(rows)}")
if sum(int(r["partition_count"]) for r in rows) < 3:
    raise SystemExit(f"expected at least 3 partitions, got {rows}")
PY

run_replication_smoke_with_retries \
  "${RESULT_DIR}/replication_smoke.out" \
  "${RESULT_DIR}/replication_smoke.err"
cat "${RESULT_DIR}/replication_smoke.out" "${RESULT_DIR}/replication_smoke.err" \
  | tee -a "${RESULT_DIR}/summary.txt"

echo "threads,set_qps,set_p50_us,set_p95_us,set_p99_us,get_qps,get_p50_us,get_p95_us,get_p99_us,errors,exit_code" \
  | tee "${RESULT_DIR}/results.csv"
failed=0
for threads in ${THREAD_LIST}; do
  out="${RESULT_DIR}/string_t${threads}.out"
  err="${RESULT_DIR}/string_t${threads}.err"
  set +e
  timeout "${BENCH_TIMEOUT_S}" \
    "${BIN_DIR}/string_scale_benchmark" "${leader}" "${IDC}" "${NAMESPACE_NAME}" "${TABLE_NAME}" \
    "${OPS}" "${threads}" "${VALUE_BYTES}" 1 1000 > "${out}" 2> "${err}"
  code=$?
  set -e
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
  if [[ "${code}" != "0" ]] || awk -F, 'NR > 1 && $10 != "0" {bad=1} END {exit bad ? 0 : 1}' "${RESULT_DIR}/results.csv"; then
    failed=1
    cat "${err}" | tee -a "${RESULT_DIR}/summary.txt" || true
  fi
done

if [[ "${failed}" != "0" ]]; then
  echo "FAIL data-raft 5-node scale"
  echo "${RESULT_DIR}"
  exit 1
fi

echo "PASS data-raft 5-node scale"
echo "${RESULT_DIR}"
