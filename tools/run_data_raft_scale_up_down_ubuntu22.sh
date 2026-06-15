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

run_string_bench() {
  local name="$1"
  local out="${RESULT_DIR}/${name}.out"
  local err="${RESULT_DIR}/${name}.err"
  timeout "${BENCH_TIMEOUT_S}" \
    "${BIN_DIR}/string_scale_benchmark" "127.0.0.1:${MS_PORT}" "${IDC}" \
    "${NAMESPACE_NAME}" "${TABLE_NAME}" "${OPS}" "${THREADS}" "${VALUE_BYTES}" 1 1000 \
    > "${out}" 2> "${err}"
  csv_has_zero_errors "${out}"
  cat "${out}" >> "${RESULT_DIR}/summary.txt"
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

mkdir -p "${RESULT_DIR}"
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
post_json "${MS_PORT}" "ManageService/UpdateTable" \
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
  }" > "${RESULT_DIR}/update_table_scale_up.json"
check_status_ok "${RESULT_DIR}/update_table_scale_up.json"

wait_for_json_field "QueryService/ListPartition" "${list_partition_body}" \
  "len([p for p in data.get('info', [{}])[0].get('partition_info', []) if p.get('state', 'P_NORMAL') == 'P_NORMAL']) >= 3 and len(data.get('info', [{}])[0].get('set_info', {}).get('membership', {}).get('units', [{}])[0].get('active_id_list', [])) >= 3 and any(p.get('placement_actual', {}).get('server', {}).get('port') == ${server3_port} for p in data.get('info', [{}])[0].get('partition_info', []))" \
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
wait_for_data_raft_status_on_port \
  "${server3_port}" \
  "${server3_partition_id}" \
  "${RESULT_DIR}/server3_raft_status_after_scale_up.json" \
  3
collect_data_raft_status_for_topology after_scale_up "${RESULT_DIR}/topology_after_scale_up.csv" \
  | tee -a "${RESULT_DIR}/summary.txt"
echo "scale_up_server3_partition_id=${server3_partition_id}" | tee -a "${RESULT_DIR}/summary.txt"
run_replication_smoke_with_retries after_scale_up_replication_smoke
run_string_bench after_scale_up_string

echo "== scale down 3 -> 2 replicas ==" | tee -a "${RESULT_DIR}/summary.txt"
post_json "${MS_PORT}" "ManageService/UpdateTable" \
  "{
    \"id\": ${request_id},
    \"namespace_name\": \"${NAMESPACE_NAME}\",
    \"name\": \"${TABLE_NAME}\",
    \"table_id\": ${table_id},
    \"force\": true,
    \"update_partition_unit\": {
      \"id\": ${unit_id},
      \"partition_num\": 2,
      \"placement_set\": [
        {\"vregion\": \"vregion\", \"vdc\": \"vdc1\", \"vau\": \"vau1\"},
        {\"vregion\": \"vregion\", \"vdc\": \"vdc1\", \"vau\": \"vau2\"}
      ]
    }
  }" > "${RESULT_DIR}/update_table_scale_down.json"
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
if [[ "${server3_active_count}" != "0" ]]; then
  echo "server3 still has active partitions after scale down" >&2
  cat "${RESULT_DIR}/topology_after_scale_down.csv" >&2 || true
  exit 1
fi
echo "scale_down_server3_active_partitions=${server3_active_count}" | tee -a "${RESULT_DIR}/summary.txt"
run_replication_smoke_with_retries after_scale_down_replication_smoke
run_string_bench after_scale_down_string

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

echo "PASS data-raft scale up/down" | tee -a "${RESULT_DIR}/summary.txt"
echo "${RESULT_DIR}"
