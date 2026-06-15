#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${ROOT}/tools/temporalstore_runtime_env.sh"

BUILD_TYPE="${BUILD_TYPE:-Release}"
BUILD_FLAVOR="$(printf '%s' "${BUILD_TYPE}" | tr '[:upper:]' '[:lower:]')"
OUT_DIR="${OUT_DIR:-${ROOT}/output-ubuntu22/${BUILD_FLAVOR}}"
RESULT_DIR="${RESULT_DIR:-/tmp/temporalstore-metaserver-raft-snapshot-restore-$(date +%Y%m%d_%H%M%S)}"
CLUSTER_NAME="${CLUSTER_NAME:-meta_raft_snapshot_restore}"
MS_PORT="${MS_PORT:-36700}"
MS_PORT_STEP="${MS_PORT_STEP:-30}"
MS_RAFT_PORT="${MS_RAFT_PORT:-36710}"
MS_SNAPSHOT_PORT="${MS_SNAPSHOT_PORT:-36720}"
REQUEST_OPERATOR="${REQUEST_OPERATOR:-meta_raft_snapshot_restore}"
ADMIN_RPC_TIMEOUT_S="${ADMIN_RPC_TIMEOUT_S:-5}"
LEADER_WAIT_ATTEMPTS="${LEADER_WAIT_ATTEMPTS:-160}"
SNAPSHOT_WAIT_ATTEMPTS="${SNAPSHOT_WAIT_ATTEMPTS:-80}"
LOG_CHURN_NAMESPACES="${LOG_CHURN_NAMESPACES:-24}"

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

port_to_id() {
  local port="$1"
  echo "$(((port - MS_PORT) / MS_PORT_STEP + 1))"
}

wait_for_ports_down() {
  local attempts="${1:-40}"
  for _ in $(seq 1 "${attempts}"); do
    local up=0
    for i in 1 2 3; do
      local port=$((MS_PORT + (i - 1) * MS_PORT_STEP))
      if curl -fsS -m 1 "http://127.0.0.1:${port}/vars" >/dev/null 2>&1; then
        up=1
        break
      fi
    done
    [[ "${up}" == "0" ]] && return 0
    sleep 0.25
  done
  echo "one or more metaserver admin ports still respond after shutdown" >&2
  return 1
}

snapshot_file_count() {
  find "${RESULT_DIR}" -type f \
    \( -iname '*snapshot*' -o -path '*/snapshot/*' -o -path '*/snapshots/*' \) \
    2>/dev/null | wc -l
}

wait_for_snapshot_files() {
  local attempts="${1:-80}"
  for _ in $(seq 1 "${attempts}"); do
    local count
    count="$(snapshot_file_count)"
    if [[ "${count}" -gt 0 ]]; then
      echo "${count}"
      return 0
    fi
    sleep 0.5
  done
  echo "0"
  return 1
}

cleanup() {
  local status=$?
  for pid_file in "${RESULT_DIR}"/metaserver*.pid; do
    [[ -f "${pid_file}" ]] || continue
    kill "$(cat "${pid_file}")" >/dev/null 2>&1 || true
  done
  for _ in $(seq 1 20); do
    local alive=0
    for pid_file in "${RESULT_DIR}"/metaserver*.pid; do
      [[ -f "${pid_file}" ]] || continue
      if kill -0 "$(cat "${pid_file}")" >/dev/null 2>&1; then
        alive=1
        break
      fi
    done
    [[ "${alive}" == "0" ]] && break
    sleep 0.25
  done
  for pid_file in "${RESULT_DIR}"/metaserver*.pid; do
    [[ -f "${pid_file}" ]] || continue
    kill -9 "$(cat "${pid_file}")" >/dev/null 2>&1 || true
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

peers_3_voters="1,127.0.0.1:${MS_RAFT_PORT},127.0.0.1:${MS_SNAPSHOT_PORT},0,2,127.0.0.1:$((MS_RAFT_PORT + 1)),127.0.0.1:$((MS_SNAPSHOT_PORT + 1)),0,3,127.0.0.1:$((MS_RAFT_PORT + 2)),127.0.0.1:$((MS_SNAPSHOT_PORT + 2)),0"

start_metaserver() {
  local id="$1"
  local port=$((MS_PORT + (id - 1) * MS_PORT_STEP))
  local dir="${RESULT_DIR}/metaserver${id}"
  mkdir -p "${dir}/data" "${dir}/log"

  "${OUT_DIR}/bcache2-metaserver" \
    --metaserver_cluster_name="${CLUSTER_NAME}" \
    --metaserver_server_port="${port}" \
    --metaserver_work_dir="${dir}/data" \
    --metaserver_log_dir="${dir}/log" \
    --metaserver_raft_id="${id}" \
    --metaserver_raft_peers="${peers_3_voters}" \
    --metaserver_raft_heartbeat_cycle_ms=300 \
    --metaserver_raft_election_cycle_ms=1200 \
    --metaserver_raft_read_timeout_ms=3000 \
    --metaserver_raft_write_timeout_ms=5000 \
    --metaserver_raft_segment_size=8192 \
    --metaserver_snapshot_trigger_interval_sec=0 \
    --metaserver_meta_check_routine_interval_sec=1 \
    --metaserver_balance_routine_interval_ms=3000 \
    --metaserver_log_level=2 \
    > "${dir}/stdout" \
    2> "${dir}/stderr" &
  echo "$!" > "${RESULT_DIR}/metaserver${id}.pid"
}

for i in 1 2 3; do
  start_metaserver "${i}"
done

request_id="{\"cluster_name\":\"${CLUSTER_NAME}\",\"operator_name\":\"${REQUEST_OPERATOR}\"}"
empty_body="{\"id\":${request_id}}"
membership_body="${empty_body}"

leader_port="$(find_leader_port "${LEADER_WAIT_ATTEMPTS}")" || {
  echo "timed out waiting for initial metaserver leader" >&2
  exit 1
}
leader_id="$(port_to_id "${leader_port}")"

echo "result_dir=${RESULT_DIR}" | tee "${RESULT_DIR}/summary.txt"
echo "initial_leader=127.0.0.1:${leader_port}" | tee -a "${RESULT_DIR}/summary.txt"

wait_for_json_field \
  "${leader_port}" \
  "RaftControlService/ListMembership" \
  "${membership_body}" \
  "len(data.get('nodes', [])) == 3 and all(n.get('peer_id') in (1, 2, 3) for n in data.get('nodes', []))" \
  "${RESULT_DIR}/membership_before_snapshot.json" \
  90

namespace_anchor="snapshot_anchor_$(date +%s)"
post_json \
  "${leader_port}" \
  "ManageService/AddNamespace" \
  "{\"id\":${request_id},\"name\":\"${namespace_anchor}\"}" \
  > "${RESULT_DIR}/add_namespace_anchor.json"
check_status_ok "${RESULT_DIR}/add_namespace_anchor.json"

for n in $(seq 1 "${LOG_CHURN_NAMESPACES}"); do
  post_json \
    "${leader_port}" \
    "ManageService/AddNamespace" \
    "{\"id\":${request_id},\"name\":\"snapshot_churn_${n}_$(date +%s%N)\"}" \
    > "${RESULT_DIR}/add_namespace_churn_${n}.json"
  check_status_ok "${RESULT_DIR}/add_namespace_churn_${n}.json"
done

wait_for_json_field \
  "${leader_port}" \
  "QueryService/ListNamespace" \
  "{\"id\":${request_id},\"read_stale\":false,\"namespace_name\":\"${namespace_anchor}\"}" \
  "len(data.get('namespaces', [])) >= 1" \
  "${RESULT_DIR}/list_namespace_anchor_before_snapshot.json" \
  60

post_json "${leader_port}" "RaftControlService/TriggerSnapshot" "${empty_body}" \
  > "${RESULT_DIR}/trigger_snapshot.json"
check_status_ok "${RESULT_DIR}/trigger_snapshot.json"

snapshot_count_before_restart="$(wait_for_snapshot_files "${SNAPSHOT_WAIT_ATTEMPTS}")" || {
  echo "snapshot trigger did not create observable local snapshot files" >&2
  exit 1
}
echo "snapshot_file_count_before_restart=${snapshot_count_before_restart}" | tee -a "${RESULT_DIR}/summary.txt"

wait_for_json_field \
  "${leader_port}" \
  "QueryService/QueryClusterStatus" \
  "${empty_body}" \
  "data.get('cluster_name') == '${CLUSTER_NAME}' and len(data.get('raft_nodes', [])) == 3 and data.get('raft_leader_info', {}).get('peer_id') == ${leader_id} and data.get('raft_applied_index', 0) > 0" \
  "${RESULT_DIR}/cluster_status_before_restart.json" \
  60

restart_start_ms="$(date +%s%3N)"
for pid_file in "${RESULT_DIR}"/metaserver*.pid; do
  [[ -f "${pid_file}" ]] || continue
  kill "$(cat "${pid_file}")" >/dev/null 2>&1 || true
done
wait_for_ports_down 60
rm -f "${RESULT_DIR}"/metaserver*.pid

for i in 1 2 3; do
  start_metaserver "${i}"
done

restored_leader_port="$(find_leader_port "${LEADER_WAIT_ATTEMPTS}")" || {
  echo "timed out waiting for leader after snapshot restart" >&2
  exit 1
}
restored_leader_id="$(port_to_id "${restored_leader_port}")"
restart_elapsed_ms="$(( $(date +%s%3N) - restart_start_ms ))"
echo "restored_leader=127.0.0.1:${restored_leader_port}" | tee -a "${RESULT_DIR}/summary.txt"
echo "metaserver_snapshot_restart_ms=${restart_elapsed_ms}" | tee -a "${RESULT_DIR}/summary.txt"

wait_for_json_field \
  "${restored_leader_port}" \
  "RaftControlService/ListMembership" \
  "${membership_body}" \
  "len(data.get('nodes', [])) == 3 and all(n.get('peer_id') in (1, 2, 3) for n in data.get('nodes', []))" \
  "${RESULT_DIR}/membership_after_restore.json" \
  90

wait_for_json_field \
  "${restored_leader_port}" \
  "QueryService/ListNamespace" \
  "{\"id\":${request_id},\"read_stale\":false,\"namespace_name\":\"${namespace_anchor}\"}" \
  "len(data.get('namespaces', [])) >= 1" \
  "${RESULT_DIR}/list_namespace_anchor_after_restore.json" \
  90

wait_for_json_field \
  "${restored_leader_port}" \
  "QueryService/QueryClusterStatus" \
  "${empty_body}" \
  "data.get('cluster_name') == '${CLUSTER_NAME}' and len(data.get('raft_nodes', [])) == 3 and data.get('raft_leader_info', {}).get('peer_id') == ${restored_leader_id} and data.get('raft_applied_index', 0) > 0" \
  "${RESULT_DIR}/cluster_status_after_restore.json" \
  90

namespace_after="snapshot_after_restore_$(date +%s)"
post_json \
  "${restored_leader_port}" \
  "ManageService/AddNamespace" \
  "{\"id\":${request_id},\"name\":\"${namespace_after}\"}" \
  > "${RESULT_DIR}/add_namespace_after_restore.json"
check_status_ok "${RESULT_DIR}/add_namespace_after_restore.json"

wait_for_json_field \
  "${restored_leader_port}" \
  "QueryService/ListNamespace" \
  "{\"id\":${request_id},\"read_stale\":false,\"namespace_name\":\"${namespace_after}\"}" \
  "len(data.get('namespaces', [])) >= 1" \
  "${RESULT_DIR}/list_namespace_after_restore_write.json" \
  60

snapshot_count_after_restart="$(snapshot_file_count)"
echo "snapshot_file_count_after_restart=${snapshot_count_after_restart}" | tee -a "${RESULT_DIR}/summary.txt"
echo "namespace_anchor=${namespace_anchor}" | tee -a "${RESULT_DIR}/summary.txt"
echo "namespace_after_restore=${namespace_after}" | tee -a "${RESULT_DIR}/summary.txt"
echo "PASS metaserver raft snapshot restore" | tee -a "${RESULT_DIR}/summary.txt"
echo "${RESULT_DIR}"
