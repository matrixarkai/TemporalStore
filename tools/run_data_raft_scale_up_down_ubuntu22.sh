#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${ROOT}/tools/temporalstore_runtime_env.sh"

BUILD_TYPE="${BUILD_TYPE:-Release}"
BUILD_FLAVOR="$(printf '%s' "${BUILD_TYPE}" | tr '[:upper:]' '[:lower:]')"
OUT_DIR="${OUT_DIR:-${ROOT}/output-ubuntu22/${BUILD_FLAVOR}}"
BIN_DIR="${BIN_DIR:-${ROOT}/build-ubuntu22/${BUILD_FLAVOR}/src/client/example}"
RESULT_DIR="${RESULT_DIR:-/tmp/temporalstore-data-raft-scale-up-down-$(date +%Y%m%d_%H%M%S)}"
SMOKE_DIR="${SMOKE_DIR:-${RESULT_DIR}/cluster}"
CLUSTER_NAME="${CLUSTER_NAME:-raft_scale_up_down}"
NAMESPACE_NAME="${NAMESPACE_NAME:-ns1}"
TABLE_NAME="${TABLE_NAME:-table1}"
IDC="${IDC:-vdc1}"
MS_PORT="${MS_PORT:-35100}"
SERVER_PORT="${SERVER_PORT:-19101}"
DATA_RAFT_RAFT_PORT_DELTA="${DATA_RAFT_RAFT_PORT_DELTA:-1000}"
DATA_RAFT_SNAPSHOT_PORT_DELTA="${DATA_RAFT_SNAPSHOT_PORT_DELTA:-2000}"
OPS="${OPS:-800}"
THREADS="${THREADS:-2}"
VALUE_BYTES="${VALUE_BYTES:-128}"
BENCH_TIMEOUT_S="${BENCH_TIMEOUT_S:-180}"
ADMIN_RPC_TIMEOUT_S="${ADMIN_RPC_TIMEOUT_S:-10}"
RAFT_STATUS_WAIT_S="${RAFT_STATUS_WAIT_S:-60}"
KILL_PRIMARY_DURING_SCALE_UP="${KILL_PRIMARY_DURING_SCALE_UP:-0}"
SCALE_UP_BACKGROUND_TRAFFIC="${SCALE_UP_BACKGROUND_TRAFFIC:-${KILL_PRIMARY_DURING_SCALE_UP}}"
SCALE_UP_BACKGROUND_OPS="${SCALE_UP_BACKGROUND_OPS:-800}"
SCALE_UP_BACKGROUND_THREADS="${SCALE_UP_BACKGROUND_THREADS:-2}"
SCALE_UP_BACKGROUND_VALUE_BYTES="${SCALE_UP_BACKGROUND_VALUE_BYTES:-64}"
SCALE_UP_BACKGROUND_TIMEOUT_S="${SCALE_UP_BACKGROUND_TIMEOUT_S:-180}"
SCALE_UP_BACKGROUND_SET_RETRY_MS="${SCALE_UP_BACKGROUND_SET_RETRY_MS:-30000}"
SCALE_UP_BACKGROUND_WARMUP_S="${SCALE_UP_BACKGROUND_WARMUP_S:-1}"
MAX_SCALE_UP_BACKGROUND_ERRORS="${MAX_SCALE_UP_BACKGROUND_ERRORS:-2}"
TEXTFILE_DIR="${TEXTFILE_DIR:-${RESULT_DIR}/metrics}"
METRICS_FILE="${METRICS_FILE:-${TEXTFILE_DIR}/temporalstore-data-raft-scale-up-down.prom}"
scale_up_primary_kill_executed=0
scale_up_background_zero_errors=1
scale_up_background_within_error_budget=1
scale_up_background_errors=0
scale_up_background_elapsed_ms=0
scale_up_failover_elapsed_ms=0

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

post_json() {
  local port="$1"
  local path="$2"
  local body="$3"
  curl -fsS -m "${ADMIN_RPC_TIMEOUT_S}" \
    -H "Content-Type: application/json" \
    -d "${body}" \
    "http://127.0.0.1:${port}/${path}"
}

post_json_retry_to_file() {
  local port="$1"
  local path="$2"
  local body="$3"
  local output_file="$4"
  local attempts="${5:-12}"
  local code=1

  for attempt in $(seq 1 "${attempts}"); do
    set +e
    post_json "${port}" "${path}" "${body}" > "${output_file}" 2>"${output_file}.err"
    code=$?
    set -e
    if [[ "${code}" == "0" ]]; then
      if (( attempt > 1 )); then
        echo "${path} succeeded after ${attempt} attempts" | tee -a "${RESULT_DIR}/summary.txt"
      fi
      return 0
    fi
    echo "${path} attempt ${attempt} failed with code=${code}; retrying" | tee -a "${RESULT_DIR}/summary.txt"
    sleep 2
  done

  echo "${path} failed after ${attempts} attempts" >&2
  cat "${output_file}.err" >&2 || true
  return "${code}"
}

check_status_ok() {
  local path="$1"
  python3 - "${path}" <<'PY'
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as fh:
    data = json.load(fh)
status = data.get("status", {})
if status.get("code", 0) not in (0, 6):
    raise SystemExit(f"request failed: {data}")
PY
}

check_status_ok_or_not_found() {
  local path="$1"
  python3 - "${path}" <<'PY'
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as fh:
    data = json.load(fh)
status = data.get("status", {})
if status.get("code", 0) not in (0, 5, 6):
    raise SystemExit(f"request failed: {data}")
PY
}

wait_for_json_field() {
  local path="$1"
  local body="$2"
  local expr="$3"
  local output_file="$4"
  local attempts="${5:-180}"

  for _ in $(seq 1 "${attempts}"); do
    if post_json "${MS_PORT}" "${path}" "${body}" > "${output_file}" 2>"${output_file}.err"; then
      if python3 - "${output_file}" "${expr}" <<'PY'
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as fh:
    data = json.load(fh)
expr = sys.argv[2]
safe_builtins = {"all": all, "any": any, "int": int, "len": len, "sum": sum}
if eval(expr, {"__builtins__": safe_builtins}, {"data": data}):
    sys.exit(0)
sys.exit(1)
PY
      then
        return 0
      fi
    fi
    sleep 0.5
  done

  echo "timed out waiting for ${path}: ${expr}" >&2
  [[ -f "${output_file}" ]] && cat "${output_file}" >&2 || true
  [[ -f "${output_file}.err" ]] && cat "${output_file}.err" >&2 || true
  return 1
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
active = {int(x) for x in unit.get("active_id_list", [])}
frozen = {int(x) for x in unit.get("frozen_id_list", [])}
for partition in infos[0].get("partition_info", []):
    pid = int(partition.get("id", 0))
    placement = partition.get("placement_actual", {})
    server = placement.get("server", {})
    port = int(server.get("port", 0))
    role = partition.get("role", "")
    state = partition.get("state", "")
    membership_state = "active" if pid in active else ("frozen" if pid in frozen else "inactive")
    primary_state = "primary" if pid == primary else "secondary"
    print(f"{pid},{port},{role},{state},{membership_state},{primary_state}")
PY
}

active_partition_for_port() {
  local input_file="$1"
  local port="$2"
  partition_topology "${input_file}" | awk -F, -v port="${port}" '$2 == port && $5 == "active" {print $1; exit}'
}

active_partition_count_for_port() {
  local input_file="$1"
  local port="$2"
  partition_topology "${input_file}" | awk -F, -v port="${port}" '$2 == port && $5 == "active" {count += 1} END {print count + 0}'
}

primary_id_and_port_from_topology() {
  local topology_file="$1"
  awk -F, '$6 == "primary" {print $1 " " $2; exit}' "${topology_file}"
}

server_pid_for_port() {
  local port="$1"
  local index=$((port - SERVER_PORT + 1))
  if (( index < 1 )); then
    echo "invalid server port ${port}" >&2
    return 1
  fi
  cat "${SMOKE_DIR}/server${index}.pid"
}

process_gone_or_zombie() {
  local pid="$1"
  local state
  state="$(ps -o stat= -p "${pid}" 2>/dev/null | awk '{print $1}')"
  [[ -z "${state}" || "${state}" == Z* ]]
}

wait_for_promoted_primary() {
  local old_primary="$1"
  local output_file="$2"
  local deadline=$((SECONDS + RAFT_STATUS_WAIT_S + 120))

  while (( SECONDS < deadline )); do
    if post_json "${MS_PORT}" "QueryService/ListPartition" "${list_partition_body}" \
        > "${output_file}" 2>"${output_file}.err"; then
      if python3 - "${output_file}" "${old_primary}" <<'PY'
import json
import sys

path, old_primary = sys.argv[1], int(sys.argv[2])
with open(path, encoding="utf-8") as fh:
    data = json.load(fh)
infos = data.get("info", [])
if not infos:
    sys.exit(1)
unit = infos[0].get("set_info", {}).get("membership", {}).get("units", [{}])[0]
primary = int(unit.get("primary_id", 0))
active = {int(x) for x in unit.get("active_id_list", [])}
if primary != 0 and primary != old_primary and primary in active:
    sys.exit(0)
sys.exit(1)
PY
      then
        return 0
      fi
    fi
    sleep 0.5
  done

  echo "timed out waiting for promoted primary after scale-up primary kill" >&2
  [[ -f "${output_file}" ]] && cat "${output_file}" >&2 || true
  [[ -f "${output_file}.err" ]] && cat "${output_file}.err" >&2 || true
  return 1
}

wait_for_data_raft_status_on_port() {
  local port="$1"
  local partition_id="$2"
  local output_file="$3"
  local min_voters="${4:-2}"
  local deadline=$((SECONDS + RAFT_STATUS_WAIT_S))

  while (( SECONDS < deadline )); do
    if post_json "${port}" "ServerService/GetDataRaftStatus" \
        "{\"partition_id\":${partition_id}}" > "${output_file}" 2>"${output_file}.err"; then
      if python3 - "${output_file}" "${min_voters}" <<'PY'
import json
import sys

path, min_voters = sys.argv[1], int(sys.argv[2])
with open(path, encoding="utf-8") as fh:
    data = json.load(fh)
status = data.get("status", {})
ok = (
    status.get("code", 0) == 0
    and data.get("running") is True
    and int(data.get("fatal_event_count", 0)) == 0
    and int(data.get("voter_count", 0)) >= min_voters
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

  echo "timed out waiting for data raft status on ${port} partition ${partition_id}" >&2
  [[ -f "${output_file}" ]] && cat "${output_file}" >&2 || true
  [[ -f "${output_file}.err" ]] && cat "${output_file}.err" >&2 || true
  return 1
}

collect_data_raft_status_for_topology() {
  local phase="$1"
  local topology_file="$2"
  local output_csv="${RESULT_DIR}/${phase}_raft_lag.csv"
  echo "phase,partition_id,server_port,role,state,membership_state,primary_state,running,leader,voter_count,learner_count,committed_index,applied_index,apply_lag,fatal_event_count" \
    > "${output_csv}"

  while IFS=, read -r partition_id server_port role state membership_state primary_state; do
    [[ -n "${partition_id}" && -n "${server_port}" && "${server_port}" != "0" ]] || continue
    [[ "${membership_state}" == "active" ]] || continue
    local status_file="${RESULT_DIR}/${phase}_raft_status_${partition_id}.json"
    if post_json "${server_port}" "ServerService/GetDataRaftStatus" \
        "{\"partition_id\":${partition_id}}" > "${status_file}" 2>"${status_file}.err"; then
      python3 - "${phase}" "${partition_id}" "${server_port}" "${role}" "${state}" \
        "${membership_state}" "${primary_state}" "${status_file}" >> "${output_csv}" <<'PY'
import json
import sys

phase, partition_id, server_port, role, state, membership_state, primary_state, path = sys.argv[1:]
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
    membership_state,
    primary_state,
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

run_string_bench() {
  local name="$1"
  local out="${RESULT_DIR}/${name}.out"
  local err="${RESULT_DIR}/${name}.err"
  local code=1

  for attempt in $(seq 1 20); do
    set +e
    timeout "${BENCH_TIMEOUT_S}" \
      "${BIN_DIR}/string_scale_benchmark" "127.0.0.1:${MS_PORT}" "${IDC}" \
      "${NAMESPACE_NAME}" "${TABLE_NAME}" "${OPS}" "${THREADS}" "${VALUE_BYTES}" 1 1000 \
      > "${out}" 2> "${err}"
    code=$?
    set -e
    if [[ "${code}" == "0" ]] && csv_has_zero_errors "${out}"; then
      if (( attempt > 1 )); then
        echo "${name} passed after ${attempt} attempts" | tee -a "${RESULT_DIR}/summary.txt"
      fi
      cat "${out}" >> "${RESULT_DIR}/summary.txt"
      return 0
    fi
    echo "${name} attempt ${attempt} failed or reported errors; retrying" | tee -a "${RESULT_DIR}/summary.txt"
    sleep 1
  done

  cat "${out}" "${err}" >&2 || true
  return "${code}"
}

start_scale_up_background_traffic() {
  if [[ "${SCALE_UP_BACKGROUND_TRAFFIC}" != "1" ]]; then
    return 0
  fi

  scale_up_background_start_ms="$(date +%s%3N)"
  timeout "${SCALE_UP_BACKGROUND_TIMEOUT_S}" \
    "${BIN_DIR}/string_scale_benchmark" "127.0.0.1:${MS_PORT}" "${IDC}" \
    "${NAMESPACE_NAME}" "${TABLE_NAME}" \
    "${SCALE_UP_BACKGROUND_OPS}" "${SCALE_UP_BACKGROUND_THREADS}" \
    "${SCALE_UP_BACKGROUND_VALUE_BYTES}" 0 30000 both "${SCALE_UP_BACKGROUND_SET_RETRY_MS}" \
    > "${RESULT_DIR}/scale_up_background_traffic.out" \
    2> "${RESULT_DIR}/scale_up_background_traffic.err" &
  scale_up_background_pid="$!"
  echo "scale_up_background_pid=${scale_up_background_pid}" | tee -a "${RESULT_DIR}/summary.txt"
}

assert_scale_up_background_active() {
  if [[ "${SCALE_UP_BACKGROUND_TRAFFIC}" != "1" ]]; then
    return 0
  fi
  sleep "${SCALE_UP_BACKGROUND_WARMUP_S}"
  if ! kill -0 "${scale_up_background_pid}" >/dev/null 2>&1; then
    echo "scale-up background traffic finished before primary kill" >&2
    cat "${RESULT_DIR}/scale_up_background_traffic.out" >&2 || true
    cat "${RESULT_DIR}/scale_up_background_traffic.err" >&2 || true
    return 1
  fi
  echo "scale_up_background_active_at_primary_kill=1" | tee -a "${RESULT_DIR}/summary.txt"
}

wait_scale_up_background_traffic() {
  if [[ "${SCALE_UP_BACKGROUND_TRAFFIC}" != "1" ]]; then
    return 0
  fi

  local code=0
  set +e
  wait "${scale_up_background_pid}"
  code=$?
  set -e
  local end_ms
  end_ms="$(date +%s%3N)"
  scale_up_background_elapsed_ms=$((end_ms - scale_up_background_start_ms))
  scale_up_background_errors="$(csv_error_count "${RESULT_DIR}/scale_up_background_traffic.out")"
  scale_up_background_zero_errors=0
  scale_up_background_within_error_budget=0
  if [[ "${code}" == "0" ]] && csv_has_zero_errors "${RESULT_DIR}/scale_up_background_traffic.out"; then
    scale_up_background_zero_errors=1
  fi
  if (( scale_up_background_errors <= MAX_SCALE_UP_BACKGROUND_ERRORS )); then
    scale_up_background_within_error_budget=1
  fi
  {
    echo "scale_up_background_exit_code=${code}"
    echo "scale_up_background_errors=${scale_up_background_errors}"
    echo "scale_up_background_zero_errors=${scale_up_background_zero_errors}"
    echo "scale_up_background_within_error_budget=${scale_up_background_within_error_budget}"
    echo "scale_up_background_elapsed_ms=${scale_up_background_elapsed_ms}"
  } | tee -a "${RESULT_DIR}/summary.txt"
  if [[ ! -s "${RESULT_DIR}/scale_up_background_traffic.out" || "${scale_up_background_within_error_budget}" != "1" ]]; then
    echo "scale-up background traffic failed, hung, or exceeded error budget" >&2
    cat "${RESULT_DIR}/scale_up_background_traffic.out" >&2 || true
    cat "${RESULT_DIR}/scale_up_background_traffic.err" >&2 || true
    return 1
  fi
}

restart_server_for_port() {
  local port="$1"
  local index=$((port - SERVER_PORT + 1))
  local server_dir="${SMOKE_DIR}/server${index}"
  if (( index < 1 || index > 2 )); then
    echo "can only restart original scale-up/down servers, got port ${port}" >&2
    return 1
  fi

  local -a storage_flags=()
  while IFS= read -r flag; do
    storage_flags+=("${flag}")
  done < <(temporalstore_storage_flags)

  "${OUT_DIR}/bcache2-server" \
    --cluster_name="${CLUSTER_NAME}" \
    --metaserver_uri="127.0.0.1:${MS_PORT}" \
    --host_spec_path="${server_dir}/host_spec.json" \
    --host="127.0.0.1" \
    --port="${port}" \
    --server_log_dir="${server_dir}/log" \
    --server_log_level=2 \
    --server_meta_tinker_interval_ms=1000 \
    --server_heartbeat_interval_ms=1000 \
    "${storage_flags[@]}" \
    --replicator_out_of_sync_s="${TEMPORALSTORE_REPLICATOR_OUT_OF_SYNC_S}" \
    --data_replication_mode=raft_consensus \
    --data_raft_work_dir="${SMOKE_DIR}/data-raft" \
    --data_raft_raft_port_delta="${DATA_RAFT_RAFT_PORT_DELTA}" \
    --data_raft_snapshot_port_delta="${DATA_RAFT_SNAPSHOT_PORT_DELTA}" \
    --data_raft_enable_empty_snapshot_for_tests=false \
    --data_raft_read_mode=bounded_stale \
    --data_raft_bounded_stale_max_index_lag=16 \
    --data_raft_propose_timeout_ms=5000 \
    --storage_async=true \
    --storage_enable_evict=false \
    --storage_enable_expire=false \
    --storage_enable_page_gc=false \
    --storage_enable_page_compaction=false \
    --storage_enable_index_gc=false \
    --storage_enable_oplog_rolling=false \
    > "${server_dir}/stdout.restart" 2> "${server_dir}/stderr.restart" &
  echo "$!" > "${SMOKE_DIR}/server${index}.pid"
  echo "restarted_server_port=${port}" | tee -a "${RESULT_DIR}/summary.txt"
}

write_metrics() {
  local pass="$1"
  mkdir -p "${TEXTFILE_DIR}"
  cat > "${METRICS_FILE}" <<METRICS
# HELP temporalstore_data_raft_scale_up_down_pass Whether the data-raft scale up/down gate passed.
# TYPE temporalstore_data_raft_scale_up_down_pass gauge
temporalstore_data_raft_scale_up_down_pass ${pass}
# HELP temporalstore_data_raft_scale_up_primary_kill_executed Whether the gate killed the current primary during scale-up convergence.
# TYPE temporalstore_data_raft_scale_up_primary_kill_executed gauge
temporalstore_data_raft_scale_up_primary_kill_executed ${scale_up_primary_kill_executed}
# HELP temporalstore_data_raft_scale_up_background_zero_errors Whether background client/proxy traffic had zero errors during scale-up primary kill.
# TYPE temporalstore_data_raft_scale_up_background_zero_errors gauge
temporalstore_data_raft_scale_up_background_zero_errors ${scale_up_background_zero_errors}
# HELP temporalstore_data_raft_scale_up_background_within_error_budget Whether background client/proxy errors stayed within the configured destructive-fault budget.
# TYPE temporalstore_data_raft_scale_up_background_within_error_budget gauge
temporalstore_data_raft_scale_up_background_within_error_budget ${scale_up_background_within_error_budget}
# HELP temporalstore_data_raft_scale_up_background_errors_total Background client/proxy errors during scale-up primary kill.
# TYPE temporalstore_data_raft_scale_up_background_errors_total counter
temporalstore_data_raft_scale_up_background_errors_total ${scale_up_background_errors}
# HELP temporalstore_data_raft_scale_up_background_elapsed_ms Background traffic elapsed time during scale-up primary kill.
# TYPE temporalstore_data_raft_scale_up_background_elapsed_ms gauge
temporalstore_data_raft_scale_up_background_elapsed_ms ${scale_up_background_elapsed_ms}
# HELP temporalstore_data_raft_scale_up_failover_elapsed_ms Time to observe a promoted primary during scale-up primary kill.
# TYPE temporalstore_data_raft_scale_up_failover_elapsed_ms gauge
temporalstore_data_raft_scale_up_failover_elapsed_ms ${scale_up_failover_elapsed_ms}
METRICS
}

run_replication_smoke_with_retries() {
  local name="$1"
  local out="${RESULT_DIR}/${name}.out"
  local err="${RESULT_DIR}/${name}.err"
  local code=1

  for attempt in $(seq 1 60); do
    set +e
    timeout 120 \
      "${BIN_DIR}/replication_smoke_example" "127.0.0.1:${MS_PORT}" "${IDC}" \
      "${NAMESPACE_NAME}" "${TABLE_NAME}" \
      > "${out}" 2> "${err}"
    code=$?
    set -e
    if [[ "${code}" == "0" ]]; then
      cat "${out}" | tee -a "${RESULT_DIR}/summary.txt"
      return 0
    fi
    if ! grep -q "Slot not found" "${err}"; then
      cat "${out}" "${err}" | tee -a "${RESULT_DIR}/summary.txt"
      return "${code}"
    fi
    echo "${name} attempt ${attempt} hit Slot not found; waiting for slot install" \
      | tee -a "${RESULT_DIR}/summary.txt"
    sleep 1
  done

  cat "${out}" "${err}" | tee -a "${RESULT_DIR}/summary.txt"
  return "${code}"
}

cleanup() {
  local status=$?
  if [[ "${status}" != "0" ]]; then
    write_metrics 0
  fi
  if [[ -n "${scale_up_background_pid:-}" ]]; then
    kill "${scale_up_background_pid}" >/dev/null 2>&1 || true
    wait "${scale_up_background_pid}" >/dev/null 2>&1 || true
  fi
  if [[ -f "${RESULT_DIR}/server3.pid" ]]; then
    kill "$(cat "${RESULT_DIR}/server3.pid")" >/dev/null 2>&1 || true
  fi
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

mkdir -p "${RESULT_DIR}" "${TEXTFILE_DIR}"
rm -rf "${SMOKE_DIR}"

if (( SERVER_PORT + 2 + DATA_RAFT_RAFT_PORT_DELTA > 65535 ||
      SERVER_PORT + 2 + DATA_RAFT_SNAPSHOT_PORT_DELTA > 65535 )); then
  echo "data raft transport ports exceed 65535; lower SERVER_PORT or deltas" >&2
  exit 2
fi

preflight_port "${MS_PORT}"
preflight_port "$((MS_PORT + 10))"
preflight_port "$((MS_PORT + 20))"
for i in 0 1 2; do
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
    SERVER_COUNT=2 \
    REPLICA_COUNT=2 \
    MS_PORT="${MS_PORT}" \
    MS_RAFT_PORT="$((MS_PORT + 10))" \
    MS_SNAPSHOT_PORT="$((MS_PORT + 20))" \
    SERVER_PORT="${SERVER_PORT}" \
    TABLE_ELECTION_POLICY=PROMOTE_SECONDARY \
    TABLE_PARTITION_UNIT_RELATION=ANTI_ENTROPY \
    METASERVER_EXTRA_FLAGS="--metaserver_convict_routine_interval_ms=500 --metaserver_convict_safe_mode_warning_ratio=100 --metaserver_convict_safe_mode_critical_ratio=100 --metaserver_meta_check_routine_interval_sec=1 --metaserver_meta_check_max_freeze_partition_per_min=100" \
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

request_id="{\"cluster_name\":\"${CLUSTER_NAME}\",\"operator_name\":\"raft_scale_up_down\"}"
list_table_body="{\"id\":${request_id},\"read_stale\":false,\"namespace_name\":\"${NAMESPACE_NAME}\",\"table_name\":\"${TABLE_NAME}\"}"
list_partition_body="{\"id\":${request_id},\"read_stale\":false,\"namespace_name\":\"${NAMESPACE_NAME}\",\"table_name\":\"${TABLE_NAME}\"}"
list_server_body="{\"id\":${request_id},\"read_stale\":false,\"list_all_tag\":true}"

echo "result_dir=${RESULT_DIR}" | tee "${RESULT_DIR}/summary.txt"
echo "== baseline ==" | tee -a "${RESULT_DIR}/summary.txt"
run_replication_smoke_with_retries baseline_replication_smoke
run_string_bench baseline_string
post_json "${MS_PORT}" "QueryService/ListPartition" "${list_partition_body}" \
  > "${RESULT_DIR}/partition_baseline.json"
partition_topology "${RESULT_DIR}/partition_baseline.json" \
  > "${RESULT_DIR}/topology_baseline.csv"
cat "${RESULT_DIR}/topology_baseline.csv" >> "${RESULT_DIR}/summary.txt"
collect_data_raft_status_for_topology baseline "${RESULT_DIR}/topology_baseline.csv" \
  | tee -a "${RESULT_DIR}/summary.txt"
if [[ "${KILL_PRIMARY_DURING_SCALE_UP}" == "1" ]]; then
  read -r scale_up_old_primary_id scale_up_old_primary_port \
    < <(primary_id_and_port_from_topology "${RESULT_DIR}/topology_baseline.csv")
  scale_up_old_primary_pid="$(server_pid_for_port "${scale_up_old_primary_port}")"
  echo "scale_up_old_primary_id=${scale_up_old_primary_id}" | tee -a "${RESULT_DIR}/summary.txt"
  echo "scale_up_old_primary_port=${scale_up_old_primary_port}" | tee -a "${RESULT_DIR}/summary.txt"
  echo "scale_up_old_primary_pid=${scale_up_old_primary_pid}" | tee -a "${RESULT_DIR}/summary.txt"
  start_scale_up_background_traffic
fi

server3_port="$((SERVER_PORT + 2))"
server3_dir="${SMOKE_DIR}/server3"
mkdir -p "${server3_dir}/data" "${server3_dir}/log"
cat > "${server3_dir}/host_spec.json" <<JSON
{
  "endpoint": {"addr_family": "ADDR_V4", "ip4": "127.0.0.1", "port": ${server3_port}},
  "location": {"vregion": "vregion", "vdc": "vdc1", "vau": "vau3"},
  "numa_nodes": [{"id": 0, "cpu_list": "-", "memory_size_mb": 1}]
}
JSON

storage_flags=()
while IFS= read -r flag; do
  storage_flags+=("${flag}")
done < <(temporalstore_storage_flags)

"${OUT_DIR}/bcache2-server" \
  --cluster_name="${CLUSTER_NAME}" \
  --metaserver_uri="127.0.0.1:${MS_PORT}" \
  --host_spec_path="${server3_dir}/host_spec.json" \
  --host="127.0.0.1" \
  --port="${server3_port}" \
  --server_log_dir="${server3_dir}/log" \
  --server_log_level=2 \
  --server_meta_tinker_interval_ms=1000 \
  --server_heartbeat_interval_ms=1000 \
  "${storage_flags[@]}" \
  --replicator_out_of_sync_s="${TEMPORALSTORE_REPLICATOR_OUT_OF_SYNC_S}" \
  --data_replication_mode=raft_consensus \
  --data_raft_work_dir="${SMOKE_DIR}/data-raft" \
  --data_raft_raft_port_delta="${DATA_RAFT_RAFT_PORT_DELTA}" \
  --data_raft_snapshot_port_delta="${DATA_RAFT_SNAPSHOT_PORT_DELTA}" \
  --data_raft_enable_empty_snapshot_for_tests=false \
  --data_raft_read_mode=bounded_stale \
  --data_raft_bounded_stale_max_index_lag=16 \
  --data_raft_propose_timeout_ms=5000 \
  --storage_async=true \
  --storage_enable_evict=false \
  --storage_enable_expire=false \
  --storage_enable_page_gc=false \
  --storage_enable_page_compaction=false \
  --storage_enable_index_gc=false \
  --storage_enable_oplog_rolling=false \
  > "${server3_dir}/stdout" 2> "${server3_dir}/stderr" &
echo "$!" > "${RESULT_DIR}/server3.pid"
echo "$!" > "${SMOKE_DIR}/server3.pid"

post_json "${MS_PORT}" "ManageService/AddServer" \
  "{
    \"id\": ${request_id},
    \"endpoint\": {\"addr_family\": \"ADDR_V4\", \"ip4\": \"127.0.0.1\", \"port\": ${server3_port}},
    \"location\": {\"vregion\": \"vregion\", \"vdc\": \"vdc1\", \"vau\": \"vau3\"},
    \"numa_nodes\": [{\"id\": 0, \"cpu_list\": \"-\", \"memory_size_mb\": 1}]
  }" > "${RESULT_DIR}/add_server3.json"
check_status_ok "${RESULT_DIR}/add_server3.json"

wait_for_json_field "QueryService/ListServer" "${list_server_body}" \
  "any(s.get('server_info', {}).get('endpoint', {}).get('port') == ${server3_port} and s.get('server_info', {}).get('state') == 'SERVER_NORMAL' for s in data.get('servers', []))" \
  "${RESULT_DIR}/list_server_after_add.json"

post_json "${MS_PORT}" "QueryService/ListTable" "${list_table_body}" > "${RESULT_DIR}/table_before_scale.json"
table_id="$(python3 - "${RESULT_DIR}/table_before_scale.json" <<'PY'
import json
import sys
data = json.load(open(sys.argv[1], encoding="utf-8"))
print(data["tables"][0]["id"])
PY
)"
unit_id="$(python3 - "${RESULT_DIR}/table_before_scale.json" <<'PY'
import json
import sys
data = json.load(open(sys.argv[1], encoding="utf-8"))
print(data["tables"][0]["partition_units"][0].get("id", 0))
PY
)"

echo "== scale up 2 -> 3 replicas ==" | tee -a "${RESULT_DIR}/summary.txt"
post_json_retry_to_file "${MS_PORT}" "ManageService/UpdateTable" \
  "{
    \"id\": ${request_id},
    \"namespace_name\": \"${NAMESPACE_NAME}\",
    \"name\": \"${TABLE_NAME}\",
    \"table_id\": ${table_id},
    \"force\": true,
    \"update_partition_unit\": {
      \"id\": ${unit_id},
      \"partition_num\": 3,
      \"placement_set\": [
        {\"vregion\": \"vregion\", \"vdc\": \"vdc1\", \"vau\": \"vau1\"},
        {\"vregion\": \"vregion\", \"vdc\": \"vdc1\", \"vau\": \"vau2\"},
        {\"vregion\": \"vregion\", \"vdc\": \"vdc1\", \"vau\": \"vau3\"}
      ]
    }
  }" "${RESULT_DIR}/update_table_scale_up.json"
check_status_ok "${RESULT_DIR}/update_table_scale_up.json"

if [[ "${KILL_PRIMARY_DURING_SCALE_UP}" == "1" ]]; then
  assert_scale_up_background_active
  scale_up_failover_start_ms="$(date +%s%3N)"
  echo "killing primary during scale up pid=${scale_up_old_primary_pid} port=${scale_up_old_primary_port} partition=${scale_up_old_primary_id}" \
    | tee -a "${RESULT_DIR}/summary.txt"
  kill "${scale_up_old_primary_pid}" >/dev/null 2>&1 || true
  sleep 0.5
  if kill -0 "${scale_up_old_primary_pid}" >/dev/null 2>&1; then
    kill -9 "${scale_up_old_primary_pid}" >/dev/null 2>&1 || true
  fi
  for _ in $(seq 1 40); do
    if process_gone_or_zombie "${scale_up_old_primary_pid}"; then
      break
    fi
    sleep 0.25
  done
  if ! process_gone_or_zombie "${scale_up_old_primary_pid}"; then
    echo "primary server still alive after SIGKILL: ${scale_up_old_primary_pid}" >&2
    exit 1
  fi
  scale_up_primary_kill_executed=1
  wait_for_promoted_primary "${scale_up_old_primary_id}" "${RESULT_DIR}/partition_after_scale_up_primary_kill.json"
  scale_up_failover_end_ms="$(date +%s%3N)"
  scale_up_failover_elapsed_ms=$((scale_up_failover_end_ms - scale_up_failover_start_ms))
  echo "scale_up_failover_elapsed_ms=${scale_up_failover_elapsed_ms}" | tee -a "${RESULT_DIR}/summary.txt"
  restart_server_for_port "${scale_up_old_primary_port}"
  wait_for_json_field "QueryService/ListServer" "${list_server_body}" \
    "any(s.get('server_info', {}).get('endpoint', {}).get('port') == ${scale_up_old_primary_port} and s.get('server_info', {}).get('state') == 'SERVER_NORMAL' for s in data.get('servers', []))" \
    "${RESULT_DIR}/list_server_after_primary_restart.json" 120
fi

if [[ "${KILL_PRIMARY_DURING_SCALE_UP}" == "1" ]]; then
  scale_up_ready_expr="len(data.get('info', [{}])[0].get('set_info', {}).get('membership', {}).get('units', [{}])[0].get('active_id_list', [])) >= 2 and data.get('info', [{}])[0].get('set_info', {}).get('membership', {}).get('units', [{}])[0].get('primary_id', 0) != ${scale_up_old_primary_id} and any(p.get('placement_actual', {}).get('server', {}).get('port') == ${server3_port} and p.get('state', 'P_NORMAL') == 'P_NORMAL' for p in data.get('info', [{}])[0].get('partition_info', []))"
else
  scale_up_ready_expr="len([p for p in data.get('info', [{}])[0].get('partition_info', []) if p.get('state', 'P_NORMAL') == 'P_NORMAL']) >= 3 and len(data.get('info', [{}])[0].get('set_info', {}).get('membership', {}).get('units', [{}])[0].get('active_id_list', [])) >= 3 and any(p.get('placement_actual', {}).get('server', {}).get('port') == ${server3_port} for p in data.get('info', [{}])[0].get('partition_info', []))"
fi
wait_for_json_field "QueryService/ListPartition" "${list_partition_body}" \
  "${scale_up_ready_expr}" \
  "${RESULT_DIR}/partition_after_scale_up.json" 240
partition_topology "${RESULT_DIR}/partition_after_scale_up.json" \
  > "${RESULT_DIR}/topology_after_scale_up.csv"
cat "${RESULT_DIR}/topology_after_scale_up.csv" >> "${RESULT_DIR}/summary.txt"
server3_partition_id="$(active_partition_for_port "${RESULT_DIR}/partition_after_scale_up.json" "${server3_port}")"
if [[ -z "${server3_partition_id}" ]]; then
  echo "server3 is not active after scale up" >&2
  cat "${RESULT_DIR}/topology_after_scale_up.csv" >&2 || true
  exit 1
fi
server3_min_voters=3
if [[ "${KILL_PRIMARY_DURING_SCALE_UP}" == "1" ]]; then
  server3_min_voters=2
fi
wait_for_data_raft_status_on_port \
  "${server3_port}" \
  "${server3_partition_id}" \
  "${RESULT_DIR}/server3_raft_status_after_scale_up.json" \
  "${server3_min_voters}"
collect_data_raft_status_for_topology after_scale_up "${RESULT_DIR}/topology_after_scale_up.csv" \
  | tee -a "${RESULT_DIR}/summary.txt"
echo "scale_up_server3_partition_id=${server3_partition_id}" | tee -a "${RESULT_DIR}/summary.txt"
run_replication_smoke_with_retries after_scale_up_replication_smoke
run_string_bench after_scale_up_string
wait_scale_up_background_traffic

echo "== scale down 3 -> 2 replicas ==" | tee -a "${RESULT_DIR}/summary.txt"
scale_down_placement_set='[
        {"vregion": "vregion", "vdc": "vdc1", "vau": "vau1"},
        {"vregion": "vregion", "vdc": "vdc1", "vau": "vau2"}
      ]'
expected_server3_active_after_scale_down=0
if [[ "${KILL_PRIMARY_DURING_SCALE_UP}" == "1" ]]; then
  expected_server3_active_after_scale_down=1
  if [[ "${scale_up_old_primary_port}" == "${SERVER_PORT}" ]]; then
    scale_down_placement_set='[
        {"vregion": "vregion", "vdc": "vdc1", "vau": "vau2"},
        {"vregion": "vregion", "vdc": "vdc1", "vau": "vau3"}
      ]'
  else
    scale_down_placement_set='[
        {"vregion": "vregion", "vdc": "vdc1", "vau": "vau1"},
        {"vregion": "vregion", "vdc": "vdc1", "vau": "vau3"}
      ]'
  fi
fi
post_json_retry_to_file "${MS_PORT}" "ManageService/UpdateTable" \
  "{
    \"id\": ${request_id},
    \"namespace_name\": \"${NAMESPACE_NAME}\",
    \"name\": \"${TABLE_NAME}\",
    \"table_id\": ${table_id},
    \"force\": true,
    \"update_partition_unit\": {
      \"id\": ${unit_id},
      \"partition_num\": 2,
      \"placement_set\": ${scale_down_placement_set}
    }
  }" "${RESULT_DIR}/update_table_scale_down.json"
check_status_ok "${RESULT_DIR}/update_table_scale_down.json"

wait_for_json_field "QueryService/ListPartition" "${list_partition_body}" \
  "len(data.get('info', [{}])[0].get('set_info', {}).get('membership', {}).get('units', [{}])[0].get('active_id_list', [])) == 2 and sum(1 for p in data.get('info', [{}])[0].get('partition_info', []) if p.get('state', 'P_NORMAL') == 'P_NORMAL') >= 2" \
  "${RESULT_DIR}/partition_after_scale_down.json" 240
partition_topology "${RESULT_DIR}/partition_after_scale_down.json" \
  > "${RESULT_DIR}/topology_after_scale_down.csv"
cat "${RESULT_DIR}/topology_after_scale_down.csv" >> "${RESULT_DIR}/summary.txt"
collect_data_raft_status_for_topology after_scale_down "${RESULT_DIR}/topology_after_scale_down.csv" \
  | tee -a "${RESULT_DIR}/summary.txt"
server3_active_count="$(active_partition_count_for_port "${RESULT_DIR}/partition_after_scale_down.json" "${server3_port}")"
if [[ "${server3_active_count}" != "${expected_server3_active_after_scale_down}" ]]; then
  echo "server3 active partition count after scale down was ${server3_active_count}, expected ${expected_server3_active_after_scale_down}" >&2
  cat "${RESULT_DIR}/topology_after_scale_down.csv" >&2 || true
  exit 1
fi
echo "scale_down_server3_active_partitions=${server3_active_count}" | tee -a "${RESULT_DIR}/summary.txt"
run_replication_smoke_with_retries after_scale_down_replication_smoke
run_string_bench after_scale_down_string

if [[ "${KILL_PRIMARY_DURING_SCALE_UP}" == "1" ]]; then
  write_metrics 1
  echo "PASS data-raft scale up/down with primary kill during scale up" | tee -a "${RESULT_DIR}/summary.txt"
  echo "${RESULT_DIR}"
  exit 0
fi

server3_id="$(python3 - "${RESULT_DIR}/list_server_after_add.json" "${server3_port}" <<'PY'
import json
import sys
data = json.load(open(sys.argv[1], encoding="utf-8"))
port = int(sys.argv[2])
for server in data.get("servers", []):
    info = server.get("server_info", {})
    if info.get("endpoint", {}).get("port") == port:
        print(info["id"])
        break
else:
    raise SystemExit("server3 not found")
PY
)"

post_json "${MS_PORT}" "ManageService/FreezeServer" \
  "{
    \"id\": ${request_id},
    \"server_id\": ${server3_id},
    \"force\": true,
    \"reason\": \"MAINTAIN\"
  }" > "${RESULT_DIR}/freeze_server3.json"
check_status_ok "${RESULT_DIR}/freeze_server3.json"

wait_for_json_field "QueryService/ListServer" "${list_server_body}" \
  "any(s.get('server_info', {}).get('endpoint', {}).get('port') == ${server3_port} and s.get('server_info', {}).get('state') == 'SERVER_FROZEN' for s in data.get('servers', []))" \
  "${RESULT_DIR}/list_server_after_freeze.json" 120

post_json "${MS_PORT}" "ManageService/DropServer" \
  "{
    \"id\": ${request_id},
    \"server_id\": ${server3_id}
  }" > "${RESULT_DIR}/drop_server3.json"
check_status_ok_or_not_found "${RESULT_DIR}/drop_server3.json"

wait_for_json_field "QueryService/ListServer" "${list_server_body}" \
  "all(len(node.get('partition_ids', [])) == 0 for s in data.get('servers', []) if s.get('server_info', {}).get('endpoint', {}).get('port') == ${server3_port} for node in s.get('node_info', []))" \
  "${RESULT_DIR}/list_server_after_drop.json" 120
post_json "${MS_PORT}" "QueryService/ListPartition" "${list_partition_body}" \
  > "${RESULT_DIR}/partition_after_drop.json"
partition_topology "${RESULT_DIR}/partition_after_drop.json" \
  > "${RESULT_DIR}/topology_after_drop.csv"
collect_data_raft_status_for_topology after_drop "${RESULT_DIR}/topology_after_drop.csv" \
  | tee -a "${RESULT_DIR}/summary.txt"
server3_active_count_after_drop="$(active_partition_count_for_port "${RESULT_DIR}/partition_after_drop.json" "${server3_port}")"
if [[ "${server3_active_count_after_drop}" != "0" ]]; then
  echo "server3 still has active partitions after drop" >&2
  cat "${RESULT_DIR}/topology_after_drop.csv" >&2 || true
  exit 1
fi
echo "drop_server3_active_partitions=${server3_active_count_after_drop}" | tee -a "${RESULT_DIR}/summary.txt"
run_replication_smoke_with_retries after_drop_replication_smoke
run_string_bench after_drop_string

write_metrics 1
echo "PASS data-raft scale up/down" | tee -a "${RESULT_DIR}/summary.txt"
echo "${RESULT_DIR}"
