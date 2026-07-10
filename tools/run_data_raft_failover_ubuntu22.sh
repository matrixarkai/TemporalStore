#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${ROOT}/tools/temporalstore_runtime_env.sh"

BUILD_TYPE="${BUILD_TYPE:-Release}"
BUILD_FLAVOR="$(printf '%s' "${BUILD_TYPE}" | tr '[:upper:]' '[:lower:]')"
OUT_DIR="${OUT_DIR:-${ROOT}/output-ubuntu22/${BUILD_FLAVOR}}"
BIN_DIR="${BIN_DIR:-${ROOT}/build-ubuntu22/${BUILD_FLAVOR}/src/client/example}"
SMOKE_DIR="${SMOKE_DIR:-/tmp/temporalstore-data-raft-failover}"
RUN_LOG_DIR="${RUN_LOG_DIR:-${SMOKE_DIR}-runner}"
CLUSTER_NAME="${CLUSTER_NAME:-raft_failover}"
NAMESPACE_NAME="${NAMESPACE_NAME:-ns1}"
TABLE_NAME="${TABLE_NAME:-table1}"
MS_PORT="${MS_PORT:-18100}"
SERVER_PORT="${SERVER_PORT:-18101}"
SERVER_COUNT="${SERVER_COUNT:-3}"
REPLICA_COUNT="${REPLICA_COUNT:-3}"
DATA_RAFT_RAFT_PORT_DELTA="${DATA_RAFT_RAFT_PORT_DELTA:-1000}"
DATA_RAFT_SNAPSHOT_PORT_DELTA="${DATA_RAFT_SNAPSHOT_PORT_DELTA:-2000}"
DATA_RAFT_HEARTBEAT_CYCLE_MS="${DATA_RAFT_HEARTBEAT_CYCLE_MS:-10}"
FAILOVER_VISIBILITY_ATTEMPTS="${FAILOVER_VISIBILITY_ATTEMPTS:-5}"
FAILOVER_VISIBILITY_WAIT_S="${FAILOVER_VISIBILITY_WAIT_S:-2}"
ADMIN_RPC_TIMEOUT_S="${ADMIN_RPC_TIMEOUT_S:-10}"
RAFT_STATUS_WAIT_S="${RAFT_STATUS_WAIT_S:-60}"
FAILOVER_BACKGROUND_TRAFFIC="${FAILOVER_BACKGROUND_TRAFFIC:-1}"
FAILOVER_BACKGROUND_OPS="${FAILOVER_BACKGROUND_OPS:-600}"
FAILOVER_BACKGROUND_THREADS="${FAILOVER_BACKGROUND_THREADS:-2}"
FAILOVER_BACKGROUND_VALUE_BYTES="${FAILOVER_BACKGROUND_VALUE_BYTES:-64}"
FAILOVER_BACKGROUND_TIMEOUT_S="${FAILOVER_BACKGROUND_TIMEOUT_S:-180}"
FAILOVER_BACKGROUND_WARMUP_S="${FAILOVER_BACKGROUND_WARMUP_S:-1}"
FAILOVER_BACKGROUND_SET_RETRY_MS="${FAILOVER_BACKGROUND_SET_RETRY_MS:-30000}"

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

if (( SERVER_PORT + SERVER_COUNT - 1 + DATA_RAFT_RAFT_PORT_DELTA > 65535 ||
      SERVER_PORT + SERVER_COUNT - 1 + DATA_RAFT_SNAPSHOT_PORT_DELTA > 65535 )); then
  echo "data raft transport ports exceed 65535; lower SERVER_PORT or deltas" >&2
  exit 2
fi

for i in $(seq 0 "$((SERVER_COUNT - 1))"); do
  preflight_port "$((SERVER_PORT + i))"
  preflight_port "$((SERVER_PORT + i + DATA_RAFT_RAFT_PORT_DELTA))"
  preflight_port "$((SERVER_PORT + i + DATA_RAFT_SNAPSHOT_PORT_DELTA))"
done
preflight_port "${MS_PORT}"
preflight_port "$((MS_PORT + 10))"
preflight_port "$((MS_PORT + 20))"

csv_has_zero_errors() {
  local file="$1"
  awk -F, '
    BEGIN {
      err_col = 6
    }
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
      if (err_col == 0 || $err_col != 0) {
        bad += 1
      }
    }
    END {
      exit (rows > 0 && bad == 0) ? 0 : 1
    }
  ' "${file}"
}

csv_error_count() {
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
    NF > 0 && err_col > 0 {
      errors += $err_col
    }
    END {
      print errors + 0
    }
  ' "${file}"
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

process_gone_or_zombie() {
  local pid="$1"
  local state
  state="$(ps -o stat= -p "${pid}" 2>/dev/null | awk '{print $1}')"
  [[ -z "${state}" || "${state}" == Z* ]]
}

partition_topology() {
  local input_file="$1"
  python3 - "${input_file}" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as fh:
    data = json.load(fh)
infos = data.get("info", [])
if not infos:
    raise SystemExit("missing partition info")
unit = infos[0].get("set_info", {}).get("membership", {}).get("units", [{}])[0]
primary = int(unit.get("primary_id", 0))
if primary == 0:
    raise SystemExit("missing primary id")
for partition in infos[0].get("partition_info", []):
    pid = int(partition.get("id", 0))
    placement = partition.get("placement_actual", {})
    server = placement.get("server", {})
    port = int(server.get("port", 0))
    role = partition.get("role", "")
    state = partition.get("state", "")
    primary_tag = "primary" if pid == primary else "secondary"
    print(f"{pid},{port},{role},{state},{primary_tag}")
PY
}

primary_id_and_port() {
  local input_file="$1"
  partition_topology "${input_file}" | awk -F, '$5 == "primary" {print $1 " " $2; exit}'
}

server_pid_for_port() {
  local port="$1"
  local index=$((port - SERVER_PORT + 1))
  if (( index < 1 || index > SERVER_COUNT )); then
    echo "invalid primary server port ${port}" >&2
    return 1
  fi
  cat "${SMOKE_DIR}/server${index}.pid"
}

wait_for_promoted_primary() {
  local old_primary="$1"
  local output_file="$2"
  for _ in $(seq 1 360); do
    if post_json "${MS_PORT}" "QueryService/ListPartition" \
        "{\"id\":{\"cluster_name\":\"${CLUSTER_NAME}\",\"operator_name\":\"raft_failover\"},\"read_stale\":false,\"namespace_name\":\"${NAMESPACE_NAME}\",\"table_name\":\"${TABLE_NAME}\"}" \
        > "${output_file}" 2>"${output_file}.err"; then
      if python3 - "${output_file}" "${old_primary}" <<'PY'
import json
import sys

path, old_primary = sys.argv[1], int(sys.argv[2])
with open(path, "r", encoding="utf-8") as fh:
    data = json.load(fh)
infos = data.get("info", [])
if not infos:
    sys.exit(1)
membership = infos[0].get("set_info", {}).get("membership", {})
units = membership.get("units", [])
if not units:
    sys.exit(1)
primary = int(units[0].get("primary_id", 0))
active = {int(x) for x in units[0].get("active_id_list", [])}
frozen = {int(x) for x in units[0].get("frozen_id_list", [])}
if primary != 0 and primary != old_primary and primary in active and old_primary not in active:
    print(primary)
    sys.exit(0)
sys.exit(1)
PY
      then
        return 0
      fi
    fi
    sleep 0.5
  done
  echo "timed out waiting for promoted primary" >&2
  [[ -f "${output_file}" ]] && cat "${output_file}" >&2 || true
  [[ -f "${output_file}.err" ]] && cat "${output_file}.err" >&2 || true
  return 1
}

wait_for_data_raft_status_on_port() {
  local port="$1"
  local partition_id="$2"
  local output_file="$3"
  local require_leader="${4:-0}"
  local deadline=$((SECONDS + RAFT_STATUS_WAIT_S))

  while (( SECONDS < deadline )); do
    if post_json "${port}" "ServerService/GetDataRaftStatus" \
        "{\"partition_id\":${partition_id}}" > "${output_file}" 2>"${output_file}.err"; then
      if python3 - "${output_file}" "${require_leader}" <<'PY'
import json
import sys

path, require_leader = sys.argv[1], int(sys.argv[2])
with open(path, encoding="utf-8") as fh:
    data = json.load(fh)
status = data.get("status", {})
ok = (
    status.get("code", 0) == 0
    and data.get("running") is True
    and int(data.get("fatal_event_count", 0)) == 0
    and int(data.get("voter_count", 0)) >= 2
)
if require_leader:
    ok = ok and data.get("leader") is True
sys.exit(0 if ok else 1)
PY
      then
        return 0
      fi
    fi
    sleep 1
  done

  echo "timed out waiting for data raft status on ${port} partition ${partition_id}" >&2
  [[ -f "${output_file}" ]] && cat "${output_file}" >&2 || true
  [[ -f "${output_file}.err" ]] && cat "${output_file}.err" >&2 || true
  return 1
}

collect_data_raft_status_for_topology() {
  local phase="$1"
  local topology_file="$2"
  local output_csv="${RUN_LOG_DIR}/${phase}_raft_lag.csv"
  echo "phase,partition_id,server_port,role,state,primary_tag,running,leader,voter_count,learner_count,committed_index,applied_index,apply_lag,fatal_event_count" \
    > "${output_csv}"

  while IFS=, read -r partition_id server_port role state primary_tag; do
    [[ -n "${partition_id}" && -n "${server_port}" && "${server_port}" != "0" ]] || continue
    local status_file="${RUN_LOG_DIR}/${phase}_raft_status_${partition_id}.json"
    if post_json "${server_port}" "ServerService/GetDataRaftStatus" \
        "{\"partition_id\":${partition_id}}" > "${status_file}" 2>"${status_file}.err"; then
      python3 - "${phase}" "${partition_id}" "${server_port}" "${role}" "${state}" \
        "${primary_tag}" "${status_file}" >> "${output_csv}" <<'PY'
import json
import sys

phase, partition_id, server_port, role, state, primary_tag, path = sys.argv[1:]
with open(path, encoding="utf-8") as fh:
    data = json.load(fh)
status = data.get("status", {})
if status.get("code", 0) != 0:
    raise SystemExit(0)
committed = int(data.get("committed_index", 0))
applied = int(data.get("applied_index", 0))
lag = max(0, committed - applied)
fields = [
    phase,
    partition_id,
    server_port,
    role,
    state,
    primary_tag,
    "1" if data.get("running") is True else "0",
    "1" if data.get("leader") is True else "0",
    str(int(data.get("voter_count", 0))),
    str(int(data.get("learner_count", 0))),
    str(committed),
    str(applied),
    str(lag),
    str(int(data.get("fatal_event_count", 0))),
]
print(",".join(fields))
PY
    fi
  done < "${topology_file}"

  cat "${output_csv}"
}

wait_for_data_raft_membership_ready() {
  local out="${RUN_LOG_DIR}/membership_ready_replication_smoke.out"
  local err="${RUN_LOG_DIR}/membership_ready_replication_smoke.err"

  for attempt in $(seq 1 120); do
    if run_replication_smoke_with_retries "${out}" "${err}" >/dev/null; then
      if (( attempt > 1 )); then
        echo "data raft membership became functionally ready after ${attempt} checks"
      fi
      return 0
    fi
    sleep 1
  done

  echo "timed out waiting for data raft functional follower-read readiness before failover" >&2
  cat "${out}" "${err}" >&2 || true
  return 1
}

start_background_failover_traffic() {
  if [[ "${FAILOVER_BACKGROUND_TRAFFIC}" != "1" ]]; then
    echo "background_failover_enabled=0"
    return 0
  fi

  background_failover_start_ms="$(date +%s%3N)"
  timeout "${FAILOVER_BACKGROUND_TIMEOUT_S}" \
    "${BIN_DIR}/string_scale_benchmark" "127.0.0.1:${MS_PORT}" vdc1 \
    "${NAMESPACE_NAME}" "${TABLE_NAME}" \
    "${FAILOVER_BACKGROUND_OPS}" "${FAILOVER_BACKGROUND_THREADS}" \
    "${FAILOVER_BACKGROUND_VALUE_BYTES}" 0 30000 both "${FAILOVER_BACKGROUND_SET_RETRY_MS}" \
    > "${RUN_LOG_DIR}/background_failover_traffic.out" \
    2> "${RUN_LOG_DIR}/background_failover_traffic.err" &
  background_failover_pid="$!"
  echo "background_failover_enabled=1"
  echo "background_failover_pid=${background_failover_pid}"
}

assert_background_failover_active_at_kill() {
  if [[ "${FAILOVER_BACKGROUND_TRAFFIC}" != "1" ]]; then
    return 0
  fi
  sleep "${FAILOVER_BACKGROUND_WARMUP_S}"
  if ! kill -0 "${background_failover_pid}" >/dev/null 2>&1; then
    echo "background failover traffic finished before primary kill" >&2
    cat "${RUN_LOG_DIR}/background_failover_traffic.out" >&2 || true
    cat "${RUN_LOG_DIR}/background_failover_traffic.err" >&2 || true
    return 1
  fi
  echo "background_failover_active_at_kill=1"
}

wait_background_failover_traffic() {
  if [[ "${FAILOVER_BACKGROUND_TRAFFIC}" != "1" ]]; then
    return 0
  fi

  local code=0
  set +e
  wait "${background_failover_pid}"
  code=$?
  set -e
  local end_ms
  end_ms="$(date +%s%3N)"
  local errors=0
  if [[ -f "${RUN_LOG_DIR}/background_failover_traffic.out" ]]; then
    errors="$(csv_error_count "${RUN_LOG_DIR}/background_failover_traffic.out")"
  fi
  local zero_errors=0
  if [[ "${code}" == "0" ]] && csv_has_zero_errors "${RUN_LOG_DIR}/background_failover_traffic.out"; then
    zero_errors=1
  fi
  echo "background_failover_exit_code=${code}"
  echo "background_failover_errors=${errors}"
  echo "background_failover_zero_errors=${zero_errors}"
  echo "background_failover_elapsed_ms=$((end_ms - background_failover_start_ms))"
  if [[ "${code}" != "0" || "${zero_errors}" != "1" ]]; then
    echo "background failover traffic failed or reported errors" >&2
    cat "${RUN_LOG_DIR}/background_failover_traffic.out" >&2 || true
    cat "${RUN_LOG_DIR}/background_failover_traffic.err" >&2 || true
    return 1
  fi
}

run_visibility_benchmark_with_retries() {
  local name="$1"
  local out="${RUN_LOG_DIR}/${name}.out"
  local err="${RUN_LOG_DIR}/${name}.err"
  local code=1

  for attempt in $(seq 1 "${FAILOVER_VISIBILITY_ATTEMPTS}"); do
    set +e
    "${BIN_DIR}/secondary_visibility_lag_benchmark" \
      "127.0.0.1:${MS_PORT}" vdc1 "${NAMESPACE_NAME}" "${TABLE_NAME}" \
      100 1 128 30000 1 1 \
      > "${out}" 2> "${err}"
    code=$?
    set -e
    if [[ "${code}" == "0" ]]; then
      cat "${out}"
      return 0
    fi
    echo "${name} attempt ${attempt}/${FAILOVER_VISIBILITY_ATTEMPTS} failed; waiting for secondary visibility" >&2
    cat "${err}" >&2 || true
    sleep "${FAILOVER_VISIBILITY_WAIT_S}"
  done

  cat "${out}" "${err}" >&2 || true
  return "${code}"
}

run_replication_smoke_with_retries() {
  local out="$1"
  local err="$2"
  local code=1

  for attempt in $(seq 1 60); do
    set +e
    timeout 120 \
      "${BIN_DIR}/replication_smoke_example" "127.0.0.1:${MS_PORT}" vdc1 \
      "${NAMESPACE_NAME}" "${TABLE_NAME}" \
      > "${out}" 2> "${err}"
    code=$?
    set -e
    if [[ "${code}" == "0" ]]; then
      cat "${out}"
      return 0
    fi
    if ! grep -q "Slot not found" "${err}"; then
      cat "${out}" "${err}" >&2
      return "${code}"
    fi
    echo "replication smoke attempt ${attempt} hit Slot not found; waiting for slot install"
    sleep 1
  done

  cat "${out}" "${err}" >&2
  return "${code}"
}

cleanup() {
  local status=$?
  if [[ -n "${background_failover_pid:-}" ]]; then
    kill "${background_failover_pid}" >/dev/null 2>&1 || true
    wait "${background_failover_pid}" >/dev/null 2>&1 || true
  fi
  if [[ -n "${smoke_pid:-}" ]]; then
    kill "${smoke_pid}" >/dev/null 2>&1 || true
    sleep 0.2
    kill -9 "${smoke_pid}" >/dev/null 2>&1 || true
    wait "${smoke_pid}" >/dev/null 2>&1 || true
  fi
  for pid_file in "${SMOKE_DIR}"/server*.pid "${SMOKE_DIR}"/metaserver*.pid; do
    [[ -f "${pid_file}" ]] || continue
    kill "$(cat "${pid_file}")" >/dev/null 2>&1 || true
    sleep 0.1
    kill -9 "$(cat "${pid_file}")" >/dev/null 2>&1 || true
  done
  return "${status}"
}
trap cleanup EXIT

rm -rf "${SMOKE_DIR}" "${RUN_LOG_DIR}"
mkdir -p "${SMOKE_DIR}" "${RUN_LOG_DIR}"

TABLE_ELECTION_POLICY=PROMOTE_SECONDARY \
TABLE_PARTITION_UNIT_RELATION=INDEPENDENT \
BUILD_TYPE="${BUILD_TYPE}" \
OUT_DIR="${OUT_DIR}" \
SMOKE_DIR="${SMOKE_DIR}" \
CLUSTER_NAME="${CLUSTER_NAME}" \
NAMESPACE_NAME="${NAMESPACE_NAME}" \
TABLE_NAME="${TABLE_NAME}" \
MS_PORT="${MS_PORT}" \
MS_RAFT_PORT="$((MS_PORT + 10))" \
MS_SNAPSHOT_PORT="$((MS_PORT + 20))" \
SERVER_PORT="${SERVER_PORT}" \
SERVER_COUNT="${SERVER_COUNT}" \
REPLICA_COUNT="${REPLICA_COUNT}" \
KEEP_RUNNING=1 \
METASERVER_EXTRA_FLAGS="--metaserver_convict_routine_interval_ms=500 --metaserver_convict_safe_mode_enabled=false --metaserver_convict_safe_mode_warning_ratio=100 --metaserver_convict_safe_mode_critical_ratio=100 --metaserver_meta_check_routine_interval_sec=1 --metaserver_meta_check_max_freeze_partition_per_min=100" \
SERVER_EXTRA_FLAGS="--data_replication_mode=raft_consensus --data_raft_work_dir=${SMOKE_DIR}/data-raft --data_raft_raft_port_delta=${DATA_RAFT_RAFT_PORT_DELTA} --data_raft_snapshot_port_delta=${DATA_RAFT_SNAPSHOT_PORT_DELTA} --data_raft_heartbeat_cycle_ms=${DATA_RAFT_HEARTBEAT_CYCLE_MS} --data_raft_enable_empty_snapshot_for_tests=false --data_raft_read_mode=bounded_stale --data_raft_bounded_stale_max_index_lag=0 --data_raft_propose_timeout_ms=5000 --server_heartbeat_interval_ms=500 --server_heartbeat_timeout_ms=1000 --server_meta_tinker_interval_ms=500 --storage_enable_evict=false --storage_enable_expire=false --storage_enable_page_gc=false --storage_enable_page_compaction=false --storage_enable_index_gc=false --storage_enable_oplog_rolling=false" \
  bash "${ROOT}/tools/smoke_ubuntu22.sh" > "${RUN_LOG_DIR}/cluster.stdout" 2> "${RUN_LOG_DIR}/cluster.stderr" &
smoke_pid=$!

for _ in $(seq 1 120); do
  if grep -q "KEEP_RUNNING=1" "${RUN_LOG_DIR}/cluster.stdout" 2>/dev/null; then
    break
  fi
  if ! kill -0 "${smoke_pid}" >/dev/null 2>&1; then
    echo "cluster failed to start" >&2
    cat "${RUN_LOG_DIR}/cluster.stdout" >&2 || true
    cat "${RUN_LOG_DIR}/cluster.stderr" >&2 || true
    exit 1
  fi
  sleep 0.5
done

if ! grep -q "KEEP_RUNNING=1" "${RUN_LOG_DIR}/cluster.stdout" 2>/dev/null; then
  echo "cluster did not become ready" >&2
  cat "${RUN_LOG_DIR}/cluster.stdout" >&2 || true
  cat "${RUN_LOG_DIR}/cluster.stderr" >&2 || true
  exit 1
fi

echo "baseline replica-read smoke"
run_replication_smoke_with_retries \
  "${RUN_LOG_DIR}/baseline_replication_smoke.out" \
  "${RUN_LOG_DIR}/baseline_replication_smoke.err"
wait_for_data_raft_membership_ready

echo "baseline tight secondary visibility benchmark"
run_visibility_benchmark_with_retries baseline_secondary_visibility

post_json "${MS_PORT}" "QueryService/ListPartition" \
  "{\"id\":{\"cluster_name\":\"${CLUSTER_NAME}\",\"operator_name\":\"raft_failover\"},\"read_stale\":false,\"namespace_name\":\"${NAMESPACE_NAME}\",\"table_name\":\"${TABLE_NAME}\"}" \
  > "${SMOKE_DIR}/pre_failover_partition.json"
echo "pre-failover topology:"
partition_topology "${SMOKE_DIR}/pre_failover_partition.json" | tee "${RUN_LOG_DIR}/pre_failover_topology.csv"
collect_data_raft_status_for_topology pre_failover "${RUN_LOG_DIR}/pre_failover_topology.csv" \
  | tee "${RUN_LOG_DIR}/pre_failover_raft_lag.out"
read -r old_primary_id old_primary_port < <(primary_id_and_port "${SMOKE_DIR}/pre_failover_partition.json")
primary_pid="$(server_pid_for_port "${old_primary_port}")"
wait_for_data_raft_status_on_port "${old_primary_port}" "${old_primary_id}" \
  "${RUN_LOG_DIR}/pre_failover_primary_status.json" 1

start_background_failover_traffic
assert_background_failover_active_at_kill

echo "killing primary server pid=${primary_pid} port=${old_primary_port} partition=${old_primary_id}"
kill "${primary_pid}" >/dev/null 2>&1 || true
sleep 0.5
if kill -0 "${primary_pid}" >/dev/null 2>&1; then
  kill -9 "${primary_pid}" >/dev/null 2>&1 || true
fi
for _ in $(seq 1 40); do
  if process_gone_or_zombie "${primary_pid}"; then
    break
  fi
  sleep 0.25
done
if ! process_gone_or_zombie "${primary_pid}"; then
  echo "primary server still alive after SIGKILL: ${primary_pid}" >&2
  exit 1
fi
wait_for_promoted_primary "${old_primary_id}" "${SMOKE_DIR}/post_failover_partition.json"
echo "metaserver promoted primary:"
cat "${SMOKE_DIR}/post_failover_partition.json"
echo "post-failover topology:"
partition_topology "${SMOKE_DIR}/post_failover_partition.json" | tee "${RUN_LOG_DIR}/post_failover_topology.csv"
collect_data_raft_status_for_topology post_failover_before_write "${RUN_LOG_DIR}/post_failover_topology.csv" \
  | tee "${RUN_LOG_DIR}/post_failover_before_write_raft_lag.out"
read -r new_primary_id new_primary_port < <(primary_id_and_port "${SMOKE_DIR}/post_failover_partition.json")
wait_for_data_raft_status_on_port "${new_primary_port}" "${new_primary_id}" \
  "${RUN_LOG_DIR}/post_failover_primary_status_before_write.json" 0

failover_start_ms="$(date +%s%3N)"
success=0
for attempt in $(seq 1 80); do
  if "${BIN_DIR}/string_scale_benchmark" "127.0.0.1:${MS_PORT}" vdc1 "${NAMESPACE_NAME}" "${TABLE_NAME}" 200 2 64 1 0 \
      > "${SMOKE_DIR}/post_failover_attempt_${attempt}.stdout" \
      2> "${SMOKE_DIR}/post_failover_attempt_${attempt}.stderr" &&
      csv_has_zero_errors "${SMOKE_DIR}/post_failover_attempt_${attempt}.stdout"; then
    failover_end_ms="$(date +%s%3N)"
    echo "PASS primary-down failover write/read succeeded after ${attempt} attempts, $((failover_end_ms - failover_start_ms)) ms"
    cat "${SMOKE_DIR}/post_failover_attempt_${attempt}.stdout"
    wait_for_data_raft_status_on_port "${new_primary_port}" "${new_primary_id}" \
      "${RUN_LOG_DIR}/post_failover_primary_status_after_write.json" 1
    collect_data_raft_status_for_topology post_failover_after_write "${RUN_LOG_DIR}/post_failover_topology.csv" \
      | tee "${RUN_LOG_DIR}/post_failover_after_write_raft_lag.out"
    success=1
    break
  fi
  sleep 0.5
done

if [[ "${success}" != "1" ]]; then
  echo "FAIL primary-down failover did not route writes to promoted secondary" >&2
  tail -80 "${RUN_LOG_DIR}/cluster.stdout" >&2 || true
  tail -120 "${RUN_LOG_DIR}/cluster.stderr" >&2 || true
  tail -120 "${SMOKE_DIR}/metaserver1/log/"* >&2 || true
  tail -120 "${SMOKE_DIR}/server2/log/"* >&2 || true
  exit 1
fi

wait_background_failover_traffic

echo "logs: ${SMOKE_DIR}"
echo "runner logs: ${RUN_LOG_DIR}"
