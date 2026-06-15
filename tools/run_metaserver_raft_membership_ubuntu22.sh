#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${ROOT}/tools/temporalstore_runtime_env.sh"

BUILD_TYPE="${BUILD_TYPE:-Release}"
BUILD_FLAVOR="$(printf '%s' "${BUILD_TYPE}" | tr '[:upper:]' '[:lower:]')"
OUT_DIR="${OUT_DIR:-${ROOT}/output-ubuntu22/${BUILD_FLAVOR}}"
RESULT_DIR="${RESULT_DIR:-/tmp/temporalstore-metaserver-raft-membership-$(date +%Y%m%d_%H%M%S)}"
CLUSTER_NAME="${CLUSTER_NAME:-meta_raft_membership}"
MS_PORT="${MS_PORT:-36100}"
MS_PORT_STEP="${MS_PORT_STEP:-30}"
MS_RAFT_PORT="${MS_RAFT_PORT:-36110}"
MS_SNAPSHOT_PORT="${MS_SNAPSHOT_PORT:-36120}"
REQUEST_OPERATOR="${REQUEST_OPERATOR:-meta_raft_membership}"
ADMIN_RPC_TIMEOUT_S="${ADMIN_RPC_TIMEOUT_S:-8}"
MEMBERSHIP_WAIT_ATTEMPTS="${MEMBERSHIP_WAIT_ATTEMPTS:-120}"

need_file() {
  if [[ ! -x "$1" ]]; then
    echo "missing executable: $1" >&2
    exit 1
  fi
}

need_file "${OUT_DIR}/bcache2-metaserver"

post_json() {
  local port="$1"
  local path="$2"
  local body="$3"
  curl -fsS -m "${ADMIN_RPC_TIMEOUT_S}" \
    -H "Content-Type: application/json" \
    -d "${body}" \
    "http://127.0.0.1:${port}/${path}"
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

wait_for_json_field() {
  local port="$1"
  local path="$2"
  local body="$3"
  local expr="$4"
  local output_file="$5"
  local attempts="${6:-120}"

  for _ in $(seq 1 "${attempts}"); do
    if post_json "${port}" "${path}" "${body}" > "${output_file}" 2>"${output_file}.err"; then
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

find_leader_port() {
  local attempts="${1:-120}"
  local id_body="{\"id\":{\"cluster_name\":\"${CLUSTER_NAME}\",\"operator_name\":\"${REQUEST_OPERATOR}\"}}"

  for _ in $(seq 1 "${attempts}"); do
    for i in 1 2 3; do
      local pid_file="${RESULT_DIR}/metaserver${i}.pid"
      [[ -f "${pid_file}" ]] || continue
      local port=$((MS_PORT + (i - 1) * MS_PORT_STEP))
      local out="${RESULT_DIR}/query_leader${i}.json"
      if post_json "${port}" "QueryService/QueryLeader" "${id_body}" > "${out}" 2>"${out}.err"; then
        if python3 - "${out}" <<'PY'
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as fh:
    data = json.load(fh)
sys.exit(0 if data.get("is_leader") is True else 1)
PY
        then
          printf '%s\n' "${port}"
          return 0
        fi
      fi
    done
    sleep 0.5
  done
  return 1
}

id_to_port() {
  local id="$1"
  echo $((MS_PORT + (id - 1) * MS_PORT_STEP))
}

assert_port_down() {
  local port="$1"
  local attempts="${2:-20}"
  for _ in $(seq 1 "${attempts}"); do
    if python3 - "${port}" <<'PY'
import socket
import sys

port = int(sys.argv[1])
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    sock.settimeout(0.5)
    rc = sock.connect_ex(("127.0.0.1", port))
if rc == 0:
    raise SystemExit(1)
PY
    then
      return 0
    fi
    sleep 0.25
  done
  echo "port ${port} is still accepting connections" >&2
  return 1
}

cleanup() {
  local status=$?
  for pid_file in "${RESULT_DIR}"/metaserver*.pid; do
    [[ -f "${pid_file}" ]] || continue
    kill "$(cat "${pid_file}")" >/dev/null 2>&1 || true
  done
  wait >/dev/null 2>&1 || true
  return "${status}"
}
trap cleanup EXIT

rm -rf "${RESULT_DIR}"
mkdir -p "${RESULT_DIR}"

pkill -f "bcache2-metaserver.*metaserver_cluster_name=${CLUSTER_NAME}" >/dev/null 2>&1 || true

for i in 1 2 3; do
  preflight_port "$((MS_PORT + (i - 1) * MS_PORT_STEP))"
  preflight_port "$((MS_RAFT_PORT + i - 1))"
  preflight_port "$((MS_SNAPSHOT_PORT + i - 1))"
done

peers_2="1,127.0.0.1:${MS_RAFT_PORT},127.0.0.1:${MS_SNAPSHOT_PORT},0,2,127.0.0.1:$((MS_RAFT_PORT + 1)),127.0.0.1:$((MS_SNAPSHOT_PORT + 1)),0"
peers_3="${peers_2},3,127.0.0.1:$((MS_RAFT_PORT + 2)),127.0.0.1:$((MS_SNAPSHOT_PORT + 2)),1"

start_metaserver() {
  local id="$1"
  local peers="$2"
  local port=$((MS_PORT + (id - 1) * MS_PORT_STEP))
  local dir="${RESULT_DIR}/metaserver${id}"
  mkdir -p "${dir}/data" "${dir}/log"

  "${OUT_DIR}/bcache2-metaserver" \
    --metaserver_cluster_name="${CLUSTER_NAME}" \
    --metaserver_server_port="${port}" \
    --metaserver_work_dir="${dir}/data" \
    --metaserver_log_dir="${dir}/log" \
    --metaserver_raft_id="${id}" \
    --metaserver_raft_peers="${peers}" \
    --metaserver_raft_heartbeat_cycle_ms=300 \
    --metaserver_raft_election_cycle_ms=1200 \
    --metaserver_raft_read_timeout_ms=3000 \
    --metaserver_raft_write_timeout_ms=5000 \
    --metaserver_raft_segment_size=16384 \
    --metaserver_snapshot_trigger_interval_sec=0 \
    --metaserver_meta_check_routine_interval_sec=1 \
    --metaserver_balance_routine_interval_ms=3000 \
    --metaserver_log_level=2 \
    > "${dir}/stdout" \
    2> "${dir}/stderr" &
  echo "$!" > "${RESULT_DIR}/metaserver${id}.pid"
}

start_metaserver 1 "${peers_2}"
start_metaserver 2 "${peers_2}"

leader_port="$(find_leader_port 160)" || {
  echo "timed out waiting for initial 2-node metaserver leader" >&2
  exit 1
}

request_id="{\"cluster_name\":\"${CLUSTER_NAME}\",\"operator_name\":\"${REQUEST_OPERATOR}\"}"
empty_body="{\"id\":${request_id}}"
membership_body="${empty_body}"

echo "result_dir=${RESULT_DIR}" | tee "${RESULT_DIR}/summary.txt"
echo "initial_leader=127.0.0.1:${leader_port}" | tee -a "${RESULT_DIR}/summary.txt"
membership_start_ms="$(date +%s%3N)"

wait_for_json_field \
  "${leader_port}" \
  "RaftControlService/ListMembership" \
  "${membership_body}" \
  "len(data.get('nodes', [])) == 2 and all(n.get('peer_id') in (1, 2) for n in data.get('nodes', []))" \
  "${RESULT_DIR}/membership_initial.json" \
  60

namespace_before="membership_before_$(date +%s)"
post_json \
  "${leader_port}" \
  "ManageService/AddNamespace" \
  "{\"id\":${request_id},\"name\":\"${namespace_before}\"}" \
  > "${RESULT_DIR}/add_namespace_before_add.json"
check_status_ok "${RESULT_DIR}/add_namespace_before_add.json"

wait_for_json_field \
  "${leader_port}" \
  "QueryService/ListNamespace" \
  "{\"id\":${request_id},\"read_stale\":false,\"namespace_name\":\"${namespace_before}\"}" \
  "len(data.get('namespaces', [])) >= 1" \
  "${RESULT_DIR}/list_namespace_before_add.json" \
  60

start_metaserver 3 "${peers_3}"

add_node_body="{
  \"id\": ${request_id},
  \"node\": {
    \"peer_id\": 3,
    \"raft_addr\": \"127.0.0.1:$((MS_RAFT_PORT + 2))\",
    \"snapshot_addr\": \"127.0.0.1:$((MS_SNAPSHOT_PORT + 2))\",
    \"role\": \"LEARNER\"
  }
}"
post_json "${leader_port}" "RaftControlService/AddNode" "${add_node_body}" \
  > "${RESULT_DIR}/add_node3.json"
check_status_ok "${RESULT_DIR}/add_node3.json"

wait_for_json_field \
  "${leader_port}" \
  "RaftControlService/ListMembership" \
  "${membership_body}" \
  "len(data.get('nodes', [])) == 3 and any(n.get('peer_id') == 3 for n in data.get('nodes', []))" \
  "${RESULT_DIR}/membership_after_add.json" \
  "${MEMBERSHIP_WAIT_ATTEMPTS}"

post_json "${leader_port}" "RaftControlService/TriggerSnapshot" "${empty_body}" \
  > "${RESULT_DIR}/snapshot_after_add.json"
check_status_ok "${RESULT_DIR}/snapshot_after_add.json"

node3_port="$(id_to_port 3)"
wait_for_json_field \
  "${node3_port}" \
  "QueryService/QueryClusterStatus" \
  "{\"id\":${request_id},\"read_stale\":true}" \
  "data.get('status', {}).get('code', 0) == 0 and data.get('cluster_name') == '${CLUSTER_NAME}' and len(data.get('raft_nodes', [])) == 3 and data.get('raft_applied_index', 0) > 0" \
  "${RESULT_DIR}/node3_cluster_status_after_add.json" \
  "${MEMBERSHIP_WAIT_ATTEMPTS}"

wait_for_json_field \
  "${node3_port}" \
  "QueryService/ListNamespace" \
  "{\"id\":${request_id},\"read_stale\":true,\"namespace_name\":\"${namespace_before}\"}" \
  "data.get('status', {}).get('code', 0) == 0 and len(data.get('namespaces', [])) >= 1" \
  "${RESULT_DIR}/node3_list_namespace_after_add.json" \
  "${MEMBERSHIP_WAIT_ATTEMPTS}"

node3_applied_index="$(python3 - "${RESULT_DIR}/node3_cluster_status_after_add.json" <<'PY'
import json
import sys
data = json.load(open(sys.argv[1], encoding="utf-8"))
print(data.get("raft_applied_index", 0))
PY
)"
echo "node3_stale_read_namespace=${namespace_before}" | tee -a "${RESULT_DIR}/summary.txt"
echo "node3_applied_index_after_add=${node3_applied_index}" | tee -a "${RESULT_DIR}/summary.txt"

remove_node_body="{
  \"id\": ${request_id},
  \"node\": {
    \"peer_id\": 3,
    \"raft_addr\": \"127.0.0.1:$((MS_RAFT_PORT + 2))\",
    \"snapshot_addr\": \"127.0.0.1:$((MS_SNAPSHOT_PORT + 2))\"
  }
}"
post_json "${leader_port}" "RaftControlService/RemoveNode" "${remove_node_body}" \
  > "${RESULT_DIR}/remove_node3.json"
check_status_ok "${RESULT_DIR}/remove_node3.json"

wait_for_json_field \
  "${leader_port}" \
  "RaftControlService/ListMembership" \
  "${membership_body}" \
  "len(data.get('nodes', [])) == 2 and all(n.get('peer_id') in (1, 2) for n in data.get('nodes', []))" \
  "${RESULT_DIR}/membership_after_remove.json" \
  "${MEMBERSHIP_WAIT_ATTEMPTS}"

kill "$(cat "${RESULT_DIR}/metaserver3.pid")" >/dev/null 2>&1 || true
rm -f "${RESULT_DIR}/metaserver3.pid"
assert_port_down "${node3_port}"
echo "removed_node_port_down=127.0.0.1:${node3_port}" | tee -a "${RESULT_DIR}/summary.txt"

leader_port="$(find_leader_port 80)" || {
  echo "timed out waiting for leader after removing node 3" >&2
  exit 1
}
echo "leader_after_remove=127.0.0.1:${leader_port}" | tee -a "${RESULT_DIR}/summary.txt"

namespace_name="membership_ns_$(date +%s)"
post_json \
  "${leader_port}" \
  "ManageService/AddNamespace" \
  "{\"id\":${request_id},\"name\":\"${namespace_name}\"}" \
  > "${RESULT_DIR}/add_namespace_after_remove.json"
check_status_ok "${RESULT_DIR}/add_namespace_after_remove.json"

wait_for_json_field \
  "${leader_port}" \
  "QueryService/ListNamespace" \
  "{\"id\":${request_id},\"read_stale\":false,\"namespace_name\":\"${namespace_name}\"}" \
  "len(data.get('namespaces', [])) >= 1" \
  "${RESULT_DIR}/list_namespace_after_remove.json" \
  60

wait_for_json_field \
  "${leader_port}" \
  "RaftControlService/ListMembership" \
  "${membership_body}" \
  "len(data.get('nodes', [])) == 2 and all(n.get('peer_id') in (1, 2) for n in data.get('nodes', []))" \
  "${RESULT_DIR}/membership_after_remove_write.json" \
  60

wait_for_json_field \
  "${leader_port}" \
  "QueryService/QueryClusterStatus" \
  "{\"id\":${request_id},\"read_stale\":false}" \
  "data.get('status', {}).get('code', 0) == 0 and data.get('cluster_name') == '${CLUSTER_NAME}' and len(data.get('raft_nodes', [])) == 2 and data.get('raft_leader_info', {}).get('peer_id') in (1, 2)" \
  "${RESULT_DIR}/cluster_status_after_remove_write.json" \
  60

membership_elapsed_ms="$(( $(date +%s%3N) - membership_start_ms ))"
echo "namespace_before_add=${namespace_before}" | tee -a "${RESULT_DIR}/summary.txt"
echo "namespace_after_remove=${namespace_name}" | tee -a "${RESULT_DIR}/summary.txt"
echo "metaserver_membership_add_remove_ms=${membership_elapsed_ms}" | tee -a "${RESULT_DIR}/summary.txt"

echo "PASS metaserver raft membership add/remove" | tee -a "${RESULT_DIR}/summary.txt"
echo "${RESULT_DIR}"
