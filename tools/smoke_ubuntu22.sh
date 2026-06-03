#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_TYPE="${BUILD_TYPE:-Debug}"
BUILD_FLAVOR="$(printf '%s' "${BUILD_TYPE}" | tr '[:upper:]' '[:lower:]')"
OUT_DIR="${OUT_DIR:-${ROOT}/output-ubuntu22/${BUILD_FLAVOR}}"
SMOKE_DIR="${SMOKE_DIR:-/tmp/temporalstore-smoke}"
CLUSTER_NAME="${CLUSTER_NAME:-smoke}"
NAMESPACE_NAME="${NAMESPACE_NAME:-ns1}"
TABLE_NAME="${TABLE_NAME:-table1}"
MS_PORT="${MS_PORT:-17000}"
MS_RAFT_PORT="${MS_RAFT_PORT:-17010}"
MS_SNAPSHOT_PORT="${MS_SNAPSHOT_PORT:-17020}"
META_COUNT="${META_COUNT:-1}"
MS_PORT_STEP="${MS_PORT_STEP:-30}"
SERVER_PORT="${SERVER_PORT:-17001}"
SERVER_COUNT="${SERVER_COUNT:-1}"
REPLICA_COUNT="${REPLICA_COUNT:-${SERVER_COUNT}}"
KEEP_RUNNING="${KEEP_RUNNING:-0}"
METASERVER_LOG_LEVEL="${METASERVER_LOG_LEVEL:-2}"
SERVER_LOG_LEVEL="${SERVER_LOG_LEVEL:-2}"
SERVER_EXTRA_FLAGS="${SERVER_EXTRA_FLAGS:-}"
STORAGE_POOL_URI="${STORAGE_POOL_URI:-file://${SMOKE_DIR}/storage/}"
REPLICATOR_OUT_OF_SYNC_S="${REPLICATOR_OUT_OF_SYNC_S:-10}"

if (( REPLICA_COUNT > SERVER_COUNT )); then
  echo "REPLICA_COUNT (${REPLICA_COUNT}) cannot exceed SERVER_COUNT (${SERVER_COUNT})" >&2
  exit 2
fi

need_file() {
  if [[ ! -x "$1" ]]; then
    echo "missing executable: $1" >&2
    exit 1
  fi
}

need_file "${OUT_DIR}/bcache2-metaserver"
need_file "${OUT_DIR}/bcache2-server"

if ! command -v curl >/dev/null 2>&1; then
  echo "missing required tool: curl" >&2
  exit 1
fi
if ! command -v python3 >/dev/null 2>&1; then
  echo "missing required tool: python3" >&2
  exit 1
fi

mkdir -p "${SMOKE_DIR}"

cleanup() {
  local status=$?
  for pid_file in "${SMOKE_DIR}"/server*.pid; do
    [[ -f "${pid_file}" ]] || continue
    kill "$(cat "${pid_file}")" >/dev/null 2>&1 || true
  done
  for pid_file in "${SMOKE_DIR}"/metaserver*.pid; do
    [[ -f "${pid_file}" ]] || continue
    kill "$(cat "${pid_file}")" >/dev/null 2>&1 || true
  done
  wait >/dev/null 2>&1 || true
  return "${status}"
}

if [[ "${KEEP_RUNNING}" != "1" ]]; then
  trap cleanup EXIT
fi

pkill -f "bcache2-metaserver.*metaserver_cluster_name=${CLUSTER_NAME}" >/dev/null 2>&1 || true
pkill -f "bcache2-server.*cluster_name=${CLUSTER_NAME}" >/dev/null 2>&1 || true

rm -rf "${SMOKE_DIR}"
mkdir -p "${SMOKE_DIR}/storage"
for i in $(seq 1 "${META_COUNT}"); do
  mkdir -p \
    "${SMOKE_DIR}/metaserver${i}/data" \
    "${SMOKE_DIR}/metaserver${i}/log"
done
for i in $(seq 1 "${SERVER_COUNT}"); do
  mkdir -p \
    "${SMOKE_DIR}/server${i}/data" \
    "${SMOKE_DIR}/server${i}/log"
done

post_json() {
  local port="$1"
  local path="$2"
  local body="$3"
  curl -fsS -m 3 \
    -H "Content-Type: application/json" \
    -d "${body}" \
    "http://127.0.0.1:${port}/${path}"
}

wait_for_json_field() {
  local port="$1"
  local path="$2"
  local body="$3"
  local python_expr="$4"
  local output_file="$5"
  local attempts="${6:-60}"

  for _ in $(seq 1 "${attempts}"); do
    if post_json "${port}" "${path}" "${body}" > "${output_file}" 2>"${output_file}.err"; then
      if python3 - "${output_file}" "${python_expr}" <<'PY'
import json
import sys

path, expr = sys.argv[1], sys.argv[2]
with open(path, "r", encoding="utf-8") as fh:
    data = json.load(fh)
if eval(expr, {"__builtins__": {"len": len}}, {"data": data}):
    sys.exit(0)
sys.exit(1)
PY
      then
        return 0
      fi
    fi
    sleep 0.5
  done

  echo "timed out waiting for ${path}" >&2
  [[ -f "${output_file}" ]] && cat "${output_file}" >&2 || true
  [[ -f "${output_file}.err" ]] && cat "${output_file}.err" >&2 || true
  return 1
}

request_id="{\"cluster_name\":\"${CLUSTER_NAME}\",\"operator_name\":\"smoke\"}"
id_body="{\"id\":${request_id}}"
raft_peers=""
for i in $(seq 1 "${META_COUNT}"); do
  if [[ -n "${raft_peers}" ]]; then
    raft_peers="${raft_peers},"
  fi
  raft_peers="${raft_peers}${i},127.0.0.1:$((MS_RAFT_PORT + i - 1)),127.0.0.1:$((MS_SNAPSHOT_PORT + i - 1)),0"
done

for i in $(seq 1 "${META_COUNT}"); do
  ms_port=$((MS_PORT + (i - 1) * MS_PORT_STEP))
  ms_dir="${SMOKE_DIR}/metaserver${i}"
  "${OUT_DIR}/bcache2-metaserver" \
    --metaserver_cluster_name="${CLUSTER_NAME}" \
    --metaserver_server_port="${ms_port}" \
    --metaserver_work_dir="${ms_dir}/data" \
    --metaserver_log_dir="${ms_dir}/log" \
    --metaserver_raft_id="${i}" \
    --metaserver_raft_peers="${raft_peers}" \
    --metaserver_raft_heartbeat_cycle_ms=500 \
    --metaserver_raft_election_cycle_ms=1500 \
    --metaserver_raft_segment_size=16384 \
    --metaserver_snapshot_trigger_interval_sec=0 \
    --metaserver_meta_check_routine_interval_sec=1 \
    --metaserver_balance_routine_interval_ms=3000 \
    --metaserver_placement_host_deduplicate=false \
    --metaserver_forbid_auto_register_for_convict_server=false \
    --metaserver_log_level="${METASERVER_LOG_LEVEL}" \
    > "${ms_dir}/stdout" \
    2> "${ms_dir}/stderr" &
  echo "$!" > "${SMOKE_DIR}/metaserver${i}.pid"
  if [[ "${i}" == "1" ]]; then
    cp "${SMOKE_DIR}/metaserver${i}.pid" "${SMOKE_DIR}/metaserver.pid"
  fi
done

leader_port=""
for _ in $(seq 1 120); do
  for i in $(seq 1 "${META_COUNT}"); do
    ms_port=$((MS_PORT + (i - 1) * MS_PORT_STEP))
    leader_json="${SMOKE_DIR}/query_leader${i}.json"
    if post_json "${ms_port}" "QueryService/QueryLeader" "${id_body}" \
      > "${leader_json}" 2>"${leader_json}.err"; then
      if python3 - "${leader_json}" <<'PY'
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as fh:
    data = json.load(fh)
sys.exit(0 if data.get("is_leader") is True else 1)
PY
      then
        leader_port="${ms_port}"
        break 2
      fi
    fi
  done
  sleep 0.5
done

if [[ -z "${leader_port}" ]]; then
  echo "timed out waiting for metaserver raft leader" >&2
  for i in $(seq 1 "${META_COUNT}"); do
    leader_json="${SMOKE_DIR}/query_leader${i}.json"
    [[ -f "${leader_json}" ]] && cat "${leader_json}" >&2 || true
    [[ -f "${leader_json}.err" ]] && cat "${leader_json}.err" >&2 || true
  done
  exit 1
fi

for i in $(seq 1 "${SERVER_COUNT}"); do
  server_port=$((SERVER_PORT + i - 1))
  server_dir="${SMOKE_DIR}/server${i}"
  vau="vau${i}"

  cat > "${server_dir}/host_spec.json" <<JSON
{
  "endpoint": {
    "addr_family": "ADDR_V4",
    "ip4": "127.0.0.1",
    "port": ${server_port}
  },
  "location": {
    "vregion": "vregion",
    "vdc": "vdc1",
    "vau": "${vau}"
  },
  "numa_nodes": [
    {
      "id": 0,
      "cpu_list": "-",
      "memory_size_mb": 1
    }
  ]
}
JSON

  "${OUT_DIR}/bcache2-server" \
    --cluster_name="${CLUSTER_NAME}" \
    --metaserver_uri="127.0.0.1:${leader_port}" \
    --host_spec_path="${server_dir}/host_spec.json" \
    --host="127.0.0.1" \
    --port="${server_port}" \
    --server_log_dir="${server_dir}/log" \
    --server_log_level="${SERVER_LOG_LEVEL}" \
    --server_meta_tinker_interval_ms=1000 \
    --server_heartbeat_interval_ms=1000 \
    --storage_zone_size=10485760 \
    --stream_max_blob_size=10485760 \
    --storage_async=false \
    --storage_oplog_delay_dump_length=0 \
    --replicator_out_of_sync_s="${REPLICATOR_OUT_OF_SYNC_S}" \
    ${SERVER_EXTRA_FLAGS} \
    > "${server_dir}/stdout" \
    2> "${server_dir}/stderr" &
  echo "$!" > "${SMOKE_DIR}/server${i}.pid"
  if [[ "${i}" == "1" ]]; then
    cp "${SMOKE_DIR}/server${i}.pid" "${SMOKE_DIR}/server.pid"
  fi

  add_server_json="${SMOKE_DIR}/add_server${i}.json"
  post_json \
    "${leader_port}" \
    "ManageService/AddServer" \
    "{
      \"id\": ${request_id},
      \"endpoint\": {
        \"addr_family\": \"ADDR_V4\",
        \"ip4\": \"127.0.0.1\",
        \"port\": ${server_port}
      },
      \"location\": {
        \"vregion\": \"vregion\",
        \"vdc\": \"vdc1\",
        \"vau\": \"${vau}\"
      },
      \"numa_nodes\": [
        {
          \"id\": 0,
          \"cpu_list\": \"-\",
          \"memory_size_mb\": 1
        }
      ]
    }" \
    > "${add_server_json}"

  python3 - "${add_server_json}" <<'PY'
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as fh:
    data = json.load(fh)
status = data.get("status", {})
if status.get("code", 0) not in (0, 6):
    raise SystemExit(f"AddServer failed: {data}")
PY
done

server_json="${SMOKE_DIR}/list_server.json"
wait_for_json_field \
  "${leader_port}" \
  "QueryService/ListServer" \
  "{\"id\":${request_id},\"read_stale\":false,\"list_all_tag\":true}" \
  "len(data.get('servers', [])) >= ${SERVER_COUNT}" \
  "${server_json}" \
  90

add_ns_json="${SMOKE_DIR}/add_namespace.json"
post_json \
  "${leader_port}" \
  "ManageService/AddNamespace" \
  "{\"id\":${request_id},\"name\":\"${NAMESPACE_NAME}\"}" \
  > "${add_ns_json}"

python3 - "${add_ns_json}" <<'PY'
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as fh:
    data = json.load(fh)
status = data.get("status", {})
if status.get("code", 0) not in (0,):
    raise SystemExit(f"AddNamespace failed: {data}")
PY

add_table_json="${SMOKE_DIR}/add_table.json"
placement_set_json=""
for i in $(seq 1 "${REPLICA_COUNT}"); do
  if [[ -n "${placement_set_json}" ]]; then
    placement_set_json="${placement_set_json},"
  fi
  placement_set_json="${placement_set_json}{\"vregion\":\"vregion\",\"vdc\":\"vdc1\",\"vau\":\"vau${i}\"}"
done

post_json \
  "${leader_port}" \
  "ManageService/AddTable" \
  "{
    \"id\": ${request_id},
    \"namespace_name\": \"${NAMESPACE_NAME}\",
    \"name\": \"${TABLE_NAME}\",
    \"partition_set_num\": 1,
    \"partition_units\": [
      {
        \"partition_num\": ${REPLICA_COUNT},
        \"placement_set\": [${placement_set_json}],
        \"storage_pool_uri\": \"${STORAGE_POOL_URI}\",
        \"primary_prefer\": {\"vregion\": \"vregion\", \"vdc\": \"vdc1\", \"vau\": \"vau1\"}
      }
    ],
    \"partition_unit_relation\": \"ANTI_ENTROPY\",
    \"quota\": {\"ops_read\": 1000},
    \"config\": {}
  }" \
  > "${add_table_json}"

python3 - "${add_table_json}" <<'PY'
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as fh:
    data = json.load(fh)
status = data.get("status", {})
if status.get("code", 0) not in (0,):
    raise SystemExit(f"AddTable failed: {data}")
PY

table_json="${SMOKE_DIR}/list_table.json"
wait_for_json_field \
  "${leader_port}" \
  "QueryService/ListTable" \
  "{\"id\":${request_id},\"read_stale\":false,\"namespace_name\":\"${NAMESPACE_NAME}\",\"table_name\":\"${TABLE_NAME}\"}" \
  "len(data.get('tables', [])) >= 1" \
  "${table_json}" \
  60

partition_json="${SMOKE_DIR}/list_partition.json"
wait_for_json_field \
  "${leader_port}" \
  "QueryService/ListPartition" \
  "{\"id\":${request_id},\"read_stale\":false,\"namespace_name\":\"${NAMESPACE_NAME}\",\"table_name\":\"${TABLE_NAME}\"}" \
  "len(data.get('info', [])) >= 1 and len(data.get('info', [{}])[0].get('partition_info', [])) >= ${REPLICA_COUNT}" \
  "${partition_json}" \
  90

echo "TemporalStore Ubuntu smoke test passed"
echo "metaserver leader: 127.0.0.1:${leader_port}"
for i in $(seq 1 "${META_COUNT}"); do
  echo "metaserver${i}: 127.0.0.1:$((MS_PORT + (i - 1) * MS_PORT_STEP)) pid=$(cat "${SMOKE_DIR}/metaserver${i}.pid")"
done
for i in $(seq 1 "${SERVER_COUNT}"); do
  echo "server${i} pid: $(cat "${SMOKE_DIR}/server${i}.pid")"
done
echo "logs: ${SMOKE_DIR}"

if [[ "${KEEP_RUNNING}" == "1" ]]; then
  echo "KEEP_RUNNING=1, cluster is still running. Press Ctrl+C to stop."
  trap cleanup INT TERM
  while true; do
    sleep 3600
  done
fi
