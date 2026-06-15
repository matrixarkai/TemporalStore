#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${ROOT}/tools/temporalstore_runtime_env.sh"

BUILD_TYPE="${BUILD_TYPE:-Release}"
BUILD_FLAVOR="$(printf '%s' "${BUILD_TYPE}" | tr '[:upper:]' '[:lower:]')"
OUT_DIR="${OUT_DIR:-${ROOT}/output-ubuntu22/${BUILD_FLAVOR}}"
BIN_DIR="${BIN_DIR:-${ROOT}/build-ubuntu22/${BUILD_FLAVOR}/src/client/example}"
RESULT_DIR="${RESULT_DIR:-/tmp/temporalstore-data-raft-snapshot-restore-$(date +%Y%m%d_%H%M%S)}"
SMOKE_DIR="${SMOKE_DIR:-${RESULT_DIR}/cluster}"
CLUSTER_NAME="${CLUSTER_NAME:-raft_snapshot_restore}"
NAMESPACE_NAME="${NAMESPACE_NAME:-ns1}"
TABLE_NAME="${TABLE_NAME:-table1}"
IDC="${IDC:-vdc1}"
MS_PORT="${MS_PORT:-37100}"
SERVER_PORT="${SERVER_PORT:-18101}"
DATA_RAFT_RAFT_PORT_DELTA="${DATA_RAFT_RAFT_PORT_DELTA:-1000}"
DATA_RAFT_SNAPSHOT_PORT_DELTA="${DATA_RAFT_SNAPSHOT_PORT_DELTA:-2000}"
OPS="${OPS:-240}"
THREADS="${THREADS:-2}"
VALUE_BYTES="${VALUE_BYTES:-128}"
BENCH_TIMEOUT_S="${BENCH_TIMEOUT_S:-120}"
SNAPSHOT_PRESSURE_OPS="${SNAPSHOT_PRESSURE_OPS:-100}"
SNAPSHOT_PRESSURE_THREADS="${SNAPSHOT_PRESSURE_THREADS:-1}"
SNAPSHOT_WAIT_S="${SNAPSHOT_WAIT_S:-60}"
RESTART_WAIT_S="${RESTART_WAIT_S:-60}"
SNAPSHOT_MAX_APPLIED_LOG_BYTES="${SNAPSHOT_MAX_APPLIED_LOG_BYTES:-4096}"
RAFT_STATUS_WAIT_S="${RAFT_STATUS_WAIT_S:-30}"
ADMIN_RPC_TIMEOUT_S="${ADMIN_RPC_TIMEOUT_S:-60}"

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

post_json() {
  local port="$1"
  local path="$2"
  local body="$3"
  curl -fsS -m "${ADMIN_RPC_TIMEOUT_S}" \
    -H "Content-Type: application/json" \
    -d "${body}" \
    "http://127.0.0.1:${port}/${path}"
}

check_status_ok() {
  local path="$1"
  python3 - "${path}" <<'PY'
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as fh:
    data = json.load(fh)
status = data.get("status", {})
if status.get("code", 0) != 0:
    raise SystemExit(f"request failed: {data}")
PY
}

if (( SERVER_PORT + 1 + DATA_RAFT_RAFT_PORT_DELTA > 65535 ||
      SERVER_PORT + 1 + DATA_RAFT_SNAPSHOT_PORT_DELTA > 65535 )); then
  echo "data raft transport ports exceed 65535; lower SERVER_PORT or deltas" >&2
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
    timeout 120 \
      "${BIN_DIR}/replication_smoke_example" "${leader}" "${IDC}" "${NAMESPACE_NAME}" \
      "${TABLE_NAME}" \
      > "${out}" 2> "${err}"
    code=$?
    set -e
    if [[ "${code}" == "0" ]]; then
      return 0
    fi
    echo "replication smoke attempt ${attempt} failed; waiting for snapshot/replay catch-up" \
      | tee -a "${RESULT_DIR}/summary.txt"
    cat "${err}" >> "${RESULT_DIR}/summary.txt" || true
    sleep 1
  done

  return "${code}"
}

csv_has_zero_errors() {
  local file="$1"
  awk -F, '
    $1 == "system" {
      for (i = 1; i <= NF; ++i) {
        if ($i == "errors") {
          err_col = i
        }
      }
      next
    }
    NF > 0 {
      rows += 1
      if (!err_col || $err_col != 0) {
        bad += 1
      }
    }
    END {
      exit (rows > 0 && bad == 0) ? 0 : 1
    }
  ' "${file}"
}

snapshot_file_count() {
  find "${SMOKE_DIR}/data-raft/snapshot" -type f 2>/dev/null | wc -l
}

wait_for_snapshot_files() {
  local deadline=$((SECONDS + SNAPSHOT_WAIT_S))
  local count=0
  while (( SECONDS < deadline )); do
    count="$(snapshot_file_count)"
    if (( count > 0 )); then
      echo "${count}"
      return 0
    fi
    sleep 1
  done
  echo "timed out waiting for data raft snapshot files" >&2
  find "${SMOKE_DIR}/data-raft" -maxdepth 4 -type f 2>/dev/null | sort >&2 || true
  return 1
}

primary_partition_id_from_smoke() {
  python3 - "${SMOKE_DIR}/list_partition.json" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as fh:
    data = json.load(fh)
infos = data.get("info", [])
if not infos:
    raise SystemExit("missing partition info")
units = infos[0].get("set_info", {}).get("membership", {}).get("units", [])
if not units:
    raise SystemExit("missing partition membership")
print(int(units[0]["primary_id"]))
PY
}

secondary_partition_id_from_smoke() {
  python3 - "${SMOKE_DIR}/list_partition.json" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as fh:
    data = json.load(fh)
infos = data.get("info", [])
if not infos:
    raise SystemExit("missing partition info")
primary = infos[0].get("set_info", {}).get("membership", {}).get("units", [{}])[0].get("primary_id")
for partition in infos[0].get("partition_info", []):
    if int(partition["id"]) != int(primary):
        print(int(partition["id"]))
        raise SystemExit(0)
raise SystemExit("missing secondary partition")
PY
}

wait_for_data_raft_status() {
  local port="$1"
  local partition_id="$2"
  local output="$3"
  local deadline=$((SECONDS + RAFT_STATUS_WAIT_S))

  while (( SECONDS < deadline )); do
    if post_json "${port}" "ServerService/GetDataRaftStatus" \
      "{\"partition_id\":${partition_id}}" > "${output}"; then
      if python3 - "${output}" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as fh:
    data = json.load(fh)
status = data.get("status", {})
ok = (
    status.get("code", 0) == 0
    and data.get("running") is True
    and data.get("leader") is True
    and data.get("learner", False) is False
    and int(data.get("fatal_event_count", 0)) == 0
    and int(data.get("voter_count", 0)) >= 2
    and int(data.get("applied_index", 0)) >= int(data.get("committed_index", 0))
    and int(data.get("applied_index", 0)) >= int(data.get("pending_config_change_index", 0))
)
sys.exit(0 if ok else 1)
PY
      then
        return 0
      fi
    fi
    sleep 1
  done

  echo "timed out waiting for stable data raft status" >&2
  [[ -f "${output}" ]] && cat "${output}" >&2 || true
  return 1
}

wait_for_loaded_data_raft_status() {
  local port="$1"
  local partition_id="$2"
  local output="$3"
  local deadline=$((SECONDS + RAFT_STATUS_WAIT_S))

  while (( SECONDS < deadline )); do
    if post_json "${port}" "ServerService/GetDataRaftStatus" \
      "{\"partition_id\":${partition_id}}" > "${output}"; then
      if python3 - "${output}" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as fh:
    data = json.load(fh)
status = data.get("status", {})
ok = (
    status.get("code", 0) == 0
    and data.get("running") is True
    and int(data.get("fatal_event_count", 0)) == 0
    and int(data.get("voter_count", 0)) >= 2
)
sys.exit(0 if ok else 1)
PY
      then
        return 0
      fi
    fi
    sleep 1
  done

  echo "timed out waiting for loaded data raft partition" >&2
  [[ -f "${output}" ]] && cat "${output}" >&2 || true
  return 1
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
    SERVER_EXTRA_FLAGS="--data_replication_mode=raft_consensus --data_raft_work_dir=${SMOKE_DIR}/data-raft --data_raft_raft_port_delta=${DATA_RAFT_RAFT_PORT_DELTA} --data_raft_snapshot_port_delta=${DATA_RAFT_SNAPSHOT_PORT_DELTA} --data_raft_enable_empty_snapshot_for_tests=false --data_raft_max_applied_log_bytes=${SNAPSHOT_MAX_APPLIED_LOG_BYTES} --data_raft_read_mode=bounded_stale --data_raft_bounded_stale_max_index_lag=16 --data_raft_propose_timeout_ms=5000 --storage_async=true --storage_enable_evict=false --storage_enable_expire=false --storage_enable_page_gc=false --storage_enable_page_compaction=false --storage_enable_index_gc=false --storage_enable_oplog_rolling=false" \
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
echo "snapshot_max_applied_log_bytes=${SNAPSHOT_MAX_APPLIED_LOG_BYTES}" | tee -a "${RESULT_DIR}/summary.txt"
echo "snapshot_pressure_ops=${SNAPSHOT_PRESSURE_OPS}" | tee -a "${RESULT_DIR}/summary.txt"
echo "snapshot_pressure_threads=${SNAPSHOT_PRESSURE_THREADS}" | tee -a "${RESULT_DIR}/summary.txt"

echo "== pre-write replication smoke ==" | tee -a "${RESULT_DIR}/summary.txt"
set +e
run_replication_smoke_with_retries \
  "${RESULT_DIR}/pre_write_replication_smoke.out" \
  "${RESULT_DIR}/pre_write_replication_smoke.err"
prewrite_code=$?
set -e
cat "${RESULT_DIR}/pre_write_replication_smoke.out" \
  "${RESULT_DIR}/pre_write_replication_smoke.err" \
  | tee -a "${RESULT_DIR}/summary.txt"
if [[ "${prewrite_code}" != "0" ]]; then
  echo "REPLICATION_SMOKE_FAILED_BEFORE_WRITES code=${prewrite_code}" | tee -a "${RESULT_DIR}/summary.txt"
  exit 3
fi

echo "== initial writes ==" | tee -a "${RESULT_DIR}/summary.txt"
timeout "${BENCH_TIMEOUT_S}" \
  "${BIN_DIR}/string_scale_benchmark" "${leader}" "${IDC}" "${NAMESPACE_NAME}" "${TABLE_NAME}" \
  "${OPS}" "${THREADS}" "${VALUE_BYTES}" 1 1000 set \
  > "${RESULT_DIR}/initial_writes.out" 2> "${RESULT_DIR}/initial_writes.err"
if ! csv_has_zero_errors "${RESULT_DIR}/initial_writes.out"; then
  echo "INITIAL_WRITES_FAILED" | tee -a "${RESULT_DIR}/summary.txt"
  cat "${RESULT_DIR}/initial_writes.out" "${RESULT_DIR}/initial_writes.err" \
    | tee -a "${RESULT_DIR}/summary.txt"
  exit 5
fi

primary_partition_id="$(primary_partition_id_from_smoke)"
secondary_partition_id="$(secondary_partition_id_from_smoke)"
echo "primary_partition_id=${primary_partition_id}" | tee -a "${RESULT_DIR}/summary.txt"
echo "secondary_partition_id=${secondary_partition_id}" | tee -a "${RESULT_DIR}/summary.txt"
wait_for_data_raft_status "${SERVER_PORT}" "${primary_partition_id}" \
  "${RESULT_DIR}/data_raft_status_before_snapshot.json"
python3 - "${RESULT_DIR}/data_raft_status_before_snapshot.json" >> "${RESULT_DIR}/summary.txt" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as fh:
    data = json.load(fh)
for key in (
    "term",
    "leader_replica_id",
    "committed_index",
    "applied_index",
    "pending_config_change_index",
    "voter_count",
    "learner_count",
    "fatal_event_count",
):
    print(f"data_raft_status_before_snapshot_{key}={int(data.get(key, 0))}")
PY

echo "== snapshot write pressure ==" | tee -a "${RESULT_DIR}/summary.txt"
set +e
timeout "${BENCH_TIMEOUT_S}" \
  "${BIN_DIR}/string_scale_benchmark" "${leader}" "${IDC}" "${NAMESPACE_NAME}" "${TABLE_NAME}" \
  "${SNAPSHOT_PRESSURE_OPS}" "${SNAPSHOT_PRESSURE_THREADS}" "${VALUE_BYTES}" 1 1000 set \
  > "${RESULT_DIR}/snapshot_pressure_writes.out" \
  2> "${RESULT_DIR}/snapshot_pressure_writes.err" &
pressure_pid="$!"
sleep 0.2
post_json "${SERVER_PORT}" "ServerService/TriggerDataRaftSnapshot" \
  "{\"partition_id\":${primary_partition_id}}" \
  > "${RESULT_DIR}/trigger_snapshot.json"
trigger_code=$?
wait "${pressure_pid}"
pressure_code=$?
set -e
if [[ "${trigger_code}" != "0" ]]; then
  echo "TRIGGER_SNAPSHOT_FAILED_UNDER_WRITE_PRESSURE code=${trigger_code}" \
    | tee -a "${RESULT_DIR}/summary.txt"
  cat "${RESULT_DIR}/trigger_snapshot.json" 2>/dev/null | tee -a "${RESULT_DIR}/summary.txt" || true
  exit 7
fi
check_status_ok "${RESULT_DIR}/trigger_snapshot.json"
if [[ "${pressure_code}" != "0" ]] || ! csv_has_zero_errors "${RESULT_DIR}/snapshot_pressure_writes.out"; then
  echo "SNAPSHOT_PRESSURE_WRITES_FAILED code=${pressure_code}" | tee -a "${RESULT_DIR}/summary.txt"
  cat "${RESULT_DIR}/snapshot_pressure_writes.out" \
      "${RESULT_DIR}/snapshot_pressure_writes.err" \
    | tee -a "${RESULT_DIR}/summary.txt"
  exit 8
fi
cat "${RESULT_DIR}/snapshot_pressure_writes.out" | tee -a "${RESULT_DIR}/summary.txt"
snapshot_index="$(python3 - "${RESULT_DIR}/trigger_snapshot.json" <<'PY'
import json
import sys
with open(sys.argv[1], encoding="utf-8") as fh:
    data = json.load(fh)
print(int(data.get("snapshot_index", 0)))
PY
)"
echo "triggered_snapshot_index=${snapshot_index}" | tee -a "${RESULT_DIR}/summary.txt"

snapshot_count="$(wait_for_snapshot_files)"
echo "snapshot_file_count_before_restart=${snapshot_count}" | tee -a "${RESULT_DIR}/summary.txt"

follower_pid="$(cat "${SMOKE_DIR}/server2.pid")"
echo "restarting follower pid=${follower_pid}" | tee -a "${RESULT_DIR}/summary.txt"
kill "${follower_pid}" >/dev/null 2>&1 || true
sleep 0.5
kill -9 "${follower_pid}" >/dev/null 2>&1 || true

server_dir="${SMOKE_DIR}/server2"
"${OUT_DIR}/bcache2-server" \
  --cluster_name="${CLUSTER_NAME}" \
  --metaserver_uri="${leader}" \
  --host_spec_path="${server_dir}/host_spec.json" \
  --host=127.0.0.1 \
  --port="$((SERVER_PORT + 1))" \
  --server_log_dir="${server_dir}/log" \
  --server_log_level=2 \
  --server_meta_tinker_interval_ms=500 \
  --server_heartbeat_interval_ms=500 \
  --storage_zone_size=10485760 \
  --stream_max_blob_size=10485760 \
  --storage_async=true \
  --storage_oplog_delay_dump_length=0 \
  --replicator_out_of_sync_s=10 \
  --data_replication_mode=raft_consensus \
  --data_raft_work_dir="${SMOKE_DIR}/data-raft" \
  --data_raft_raft_port_delta="${DATA_RAFT_RAFT_PORT_DELTA}" \
  --data_raft_snapshot_port_delta="${DATA_RAFT_SNAPSHOT_PORT_DELTA}" \
  --data_raft_enable_empty_snapshot_for_tests=false \
  --data_raft_max_applied_log_bytes="${SNAPSHOT_MAX_APPLIED_LOG_BYTES}" \
  --data_raft_read_mode=bounded_stale \
  --data_raft_bounded_stale_max_index_lag=16 \
  --data_raft_propose_timeout_ms=5000 \
  --storage_enable_evict=false \
  --storage_enable_expire=false \
  --storage_enable_page_gc=false \
  --storage_enable_page_compaction=false \
  --storage_enable_index_gc=false \
  --storage_enable_oplog_rolling=false \
  > "${server_dir}/restart.stdout" 2> "${server_dir}/restart.stderr" &
echo "$!" > "${SMOKE_DIR}/server2.pid"

restart_deadline=$((SECONDS + RESTART_WAIT_S))
while (( SECONDS < restart_deadline )); do
  if curl -fsS -m 1 "http://127.0.0.1:$((SERVER_PORT + 1))/vars" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
curl -fsS -m 3 "http://127.0.0.1:$((SERVER_PORT + 1))/vars" >/dev/null

wait_for_loaded_data_raft_status "$((SERVER_PORT + 1))" "${secondary_partition_id}" \
  "${RESULT_DIR}/data_raft_status_after_auto_reload.json"
echo "auto_reloaded_secondary_partition_id=${secondary_partition_id}" | tee -a "${RESULT_DIR}/summary.txt"

echo "== post-restart replication smoke ==" | tee -a "${RESULT_DIR}/summary.txt"
set +e
run_replication_smoke_with_retries \
  "${RESULT_DIR}/post_restart_replication_smoke.out" \
  "${RESULT_DIR}/post_restart_replication_smoke.err"
replication_code=$?
set -e
cat "${RESULT_DIR}/post_restart_replication_smoke.out" \
  "${RESULT_DIR}/post_restart_replication_smoke.err" \
  | tee -a "${RESULT_DIR}/summary.txt"
if [[ "${replication_code}" != "0" ]]; then
  echo "REPLICATION_SMOKE_FAILED_AFTER_RESTART code=${replication_code}" | tee -a "${RESULT_DIR}/summary.txt"
  exit 4
fi

echo "== post-restart writes/reads ==" | tee -a "${RESULT_DIR}/summary.txt"
timeout "${BENCH_TIMEOUT_S}" \
  "${BIN_DIR}/string_scale_benchmark" "${leader}" "${IDC}" "${NAMESPACE_NAME}" "${TABLE_NAME}" \
  120 "${THREADS}" 64 1 1000 \
  > "${RESULT_DIR}/post_restart_scale.out" 2> "${RESULT_DIR}/post_restart_scale.err"
if ! csv_has_zero_errors "${RESULT_DIR}/post_restart_scale.out"; then
  echo "POST_RESTART_SCALE_FAILED" | tee -a "${RESULT_DIR}/summary.txt"
  cat "${RESULT_DIR}/post_restart_scale.out" "${RESULT_DIR}/post_restart_scale.err" \
    | tee -a "${RESULT_DIR}/summary.txt"
  exit 6
fi

snapshot_count_after="$(snapshot_file_count)"
applied_count="$(find "${SMOKE_DIR}/data-raft/applied" -type f 2>/dev/null | wc -l)"
wal_count="$(find "${SMOKE_DIR}/data-raft/wal" -type f 2>/dev/null | wc -l)"
echo "snapshot_file_count_after_restart=${snapshot_count_after}" | tee -a "${RESULT_DIR}/summary.txt"
echo "applied_index_file_count=${applied_count}" | tee -a "${RESULT_DIR}/summary.txt"
echo "wal_file_count=${wal_count}" | tee -a "${RESULT_DIR}/summary.txt"
echo "PASS data-raft snapshot restart guard" | tee -a "${RESULT_DIR}/summary.txt"
echo "${RESULT_DIR}"
