#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${ROOT}/tools/temporalstore_runtime_env.sh"

BUILD_TYPE="${BUILD_TYPE:-Release}"
BUILD_FLAVOR="$(printf '%s' "${BUILD_TYPE}" | tr '[:upper:]' '[:lower:]')"
OUT_DIR="${OUT_DIR:-${ROOT}/output-ubuntu22/${BUILD_FLAVOR}}"
RESULT_DIR="${RESULT_DIR:-/tmp/temporalstore-metaserver-raft-failover-$(date +%Y%m%d_%H%M%S)}"
CLUSTER_NAME="${CLUSTER_NAME:-meta_raft_failover}"
MS_PORT="${MS_PORT:-36400}"
MS_PORT_STEP="${MS_PORT_STEP:-30}"
MS_RAFT_PORT="${MS_RAFT_PORT:-36410}"
MS_SNAPSHOT_PORT="${MS_SNAPSHOT_PORT:-36420}"
REQUEST_OPERATOR="${REQUEST_OPERATOR:-meta_raft_failover}"
LEADER_WAIT_ATTEMPTS="${LEADER_WAIT_ATTEMPTS:-160}"
ADMIN_RPC_TIMEOUT_S="${ADMIN_RPC_TIMEOUT_S:-5}"
DIAGNOSTICS_COLLECTED=0

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
  local excluded_port="${2:-}"
  local id_body="{\"id\":{\"cluster_name\":\"${CLUSTER_NAME}\",\"operator_name\":\"${REQUEST_OPERATOR}\"}}"

  for _ in $(seq 1 "${attempts}"); do
    for i in 1 2 3; do
      local pid_file="${RESULT_DIR}/metaserver${i}.pid"
      [[ -f "${pid_file}" ]] || continue
      local port=$((MS_PORT + (i - 1) * MS_PORT_STEP))
      [[ -n "${excluded_port}" && "${port}" == "${excluded_port}" ]] && continue
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

id_to_port() {
  local id="$1"
  echo "$((MS_PORT + (id - 1) * MS_PORT_STEP))"
}

surviving_follower_port() {
  local leader_id="$1"
  local killed_id="$2"
  for i in 1 2 3; do
    [[ "${i}" == "${leader_id}" || "${i}" == "${killed_id}" ]] && continue
    [[ -f "${RESULT_DIR}/metaserver${i}.pid" ]] || continue
    id_to_port "${i}"
    return 0
  done
  return 1
}

process_gone_or_zombie() {
  local pid="$1"
  local state
  state="$(ps -o stat= -p "${pid}" 2>/dev/null | awk '{print $1}')"
  [[ -z "${state}" || "${state}" == Z* ]]
}

assert_port_down() {
  local port="$1"
  if curl -fsS -m 1 "http://127.0.0.1:${port}/vars" >/dev/null 2>&1; then
    echo "expected killed metaserver port to be down, but /vars responded: ${port}" >&2
    return 1
  fi
}

scrape_vars() {
  local phase="$1"
  local port="$2"
  local out="${RESULT_DIR}/vars_${phase}_${port}.txt"
  curl -fsS -m 5 "http://127.0.0.1:${port}/vars" > "${out}" 2>"${out}.err" || true
  python3 - "${out}" "${phase}" "${port}" "${RESULT_DIR}/metrics_summary.txt" <<'PY'
import re
import sys

path, phase, port, summary = sys.argv[1:]
try:
    text = open(path, encoding="utf-8", errors="replace").read()
except OSError:
    text = ""
matches = [
    line for line in text.splitlines()
    if re.search(r"(raft|leader|election|snapshot|apply|commit)", line, re.I)
]
with open(summary, "a", encoding="utf-8") as out:
    out.write(f"{phase},port={port},vars_lines={len(text.splitlines())},raft_like_lines={len(matches)}\n")
PY
}

collect_diagnostics() {
  local reason="$1"
  local out="${RESULT_DIR}/diagnostics_summary.txt"
  local expected_running=0
  local alive_count=0
  local unexpected_down_count=0
  local port_up_count=0
  local fatal_log_line_count=0
  local id

  [[ "${DIAGNOSTICS_COLLECTED}" == "0" ]] || return 0
  DIAGNOSTICS_COLLECTED=1

  {
    echo "diagnostic_reason=${reason}"
    echo "core_pattern=$(cat /proc/sys/kernel/core_pattern 2>/dev/null || true)"
  } > "${out}"

  for id in 1 2 3; do
    local pid_file="${RESULT_DIR}/metaserver${id}.pid"
    local port=$((MS_PORT + (id - 1) * MS_PORT_STEP))
    local dir="${RESULT_DIR}/metaserver${id}"
    local pid=""
    local stat=""
    local alive=0
    local port_up=0
    local fatal_lines=0

    if [[ -f "${pid_file}" ]]; then
      expected_running=$((expected_running + 1))
      pid="$(cat "${pid_file}" 2>/dev/null || true)"
      if [[ -n "${pid}" ]]; then
        stat="$(ps -o stat= -p "${pid}" 2>/dev/null | awk '{print $1}')"
        if [[ -n "${stat}" && "${stat}" != Z* ]] && kill -0 "${pid}" >/dev/null 2>&1; then
          alive=1
          alive_count=$((alive_count + 1))
        else
          unexpected_down_count=$((unexpected_down_count + 1))
        fi
      else
        unexpected_down_count=$((unexpected_down_count + 1))
      fi
    fi

    if curl -fsS -m 1 "http://127.0.0.1:${port}/vars" >/dev/null 2>&1; then
      port_up=1
      port_up_count=$((port_up_count + 1))
    fi

    if [[ -f "${dir}/stderr" ]]; then
      fatal_lines="$(grep -Eic 'segmentation fault|core dumped|fatal|assert|panic' "${dir}/stderr" || true)"
      fatal_log_line_count=$((fatal_log_line_count + fatal_lines))
      tail -80 "${dir}/stderr" > "${RESULT_DIR}/metaserver${id}_stderr_tail.txt" || true
    fi
    if [[ -f "${dir}/stdout" ]]; then
      tail -80 "${dir}/stdout" > "${RESULT_DIR}/metaserver${id}_stdout_tail.txt" || true
    fi

    {
      echo "node${id}_pid=${pid}"
      echo "node${id}_stat=${stat}"
      echo "node${id}_expected_running=$([[ -f "${pid_file}" ]] && echo 1 || echo 0)"
      echo "node${id}_alive=${alive}"
      echo "node${id}_port=${port}"
      echo "node${id}_port_up=${port_up}"
      echo "node${id}_fatal_log_lines=${fatal_lines}"
    } >> "${out}"
  done

  {
    echo "expected_running_count=${expected_running}"
    echo "alive_count=${alive_count}"
    echo "unexpected_down_count=${unexpected_down_count}"
    echo "port_up_count=${port_up_count}"
    echo "fatal_log_line_count=${fatal_log_line_count}"
  } >> "${out}"
}

cleanup() {
  local status=$?
  if [[ "${status}" != "0" ]]; then
    collect_diagnostics "exit_${status}"
  fi
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
    --metaserver_raft_segment_size=16384 \
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
  echo "timed out waiting for initial 3-node metaserver leader" >&2
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
  "${RESULT_DIR}/membership_initial.json" \
  90

wait_for_json_field \
  "${leader_port}" \
  "QueryService/QueryClusterStatus" \
  "${empty_body}" \
  "data.get('cluster_name') == '${CLUSTER_NAME}' and len(data.get('raft_nodes', [])) == 3 and data.get('raft_leader_info', {}).get('peer_id') == ${leader_id}" \
  "${RESULT_DIR}/cluster_status_before_failover.json" \
  60

namespace_before="failover_before_$(date +%s)"
post_json \
  "${leader_port}" \
  "ManageService/AddNamespace" \
  "{\"id\":${request_id},\"name\":\"${namespace_before}\"}" \
  > "${RESULT_DIR}/add_namespace_before_failover.json"
check_status_ok "${RESULT_DIR}/add_namespace_before_failover.json"

wait_for_json_field \
  "${leader_port}" \
  "QueryService/ListNamespace" \
  "{\"id\":${request_id},\"read_stale\":false,\"namespace_name\":\"${namespace_before}\"}" \
  "len(data.get('namespaces', [])) >= 1" \
  "${RESULT_DIR}/list_namespace_before_failover.json" \
  60

for i in 1 2 3; do
  scrape_vars "before_failover" "$((MS_PORT + (i - 1) * MS_PORT_STEP))"
done

leader_pid="$(cat "${RESULT_DIR}/metaserver${leader_id}.pid")"
echo "killing_initial_leader_id=${leader_id} pid=${leader_pid}" | tee -a "${RESULT_DIR}/summary.txt"
kill "${leader_pid}" >/dev/null 2>&1 || true
sleep 0.5
if kill -0 "${leader_pid}" >/dev/null 2>&1; then
  kill -9 "${leader_pid}" >/dev/null 2>&1 || true
fi
for _ in $(seq 1 40); do
  process_gone_or_zombie "${leader_pid}" && break
  sleep 0.25
done
if ! process_gone_or_zombie "${leader_pid}"; then
  echo "initial metaserver leader still alive after SIGKILL: ${leader_pid}" >&2
  exit 1
fi
wait "${leader_pid}" >/dev/null 2>&1 || true
rm -f "${RESULT_DIR}/metaserver${leader_id}.pid"
assert_port_down "${leader_port}"
echo "killed_leader_port_down=127.0.0.1:${leader_port}" | tee -a "${RESULT_DIR}/summary.txt"

failover_start_ms="$(date +%s%3N)"
new_leader_port="$(find_leader_port "${LEADER_WAIT_ATTEMPTS}" "${leader_port}")" || {
  echo "timed out waiting for metaserver leader after killing ${leader_id}" >&2
  exit 1
}
failover_end_ms="$(date +%s%3N)"
new_leader_id="$(port_to_id "${new_leader_port}")"
echo "leader_after_kill=127.0.0.1:${new_leader_port}" | tee -a "${RESULT_DIR}/summary.txt"
echo "metaserver_failover_ms=$((failover_end_ms - failover_start_ms))" | tee -a "${RESULT_DIR}/summary.txt"

if [[ "${new_leader_id}" == "${leader_id}" ]]; then
  echo "new leader id unexpectedly equals killed leader id ${leader_id}" >&2
  exit 1
fi

wait_for_json_field \
  "${new_leader_port}" \
  "RaftControlService/ListMembership" \
  "${membership_body}" \
  "len(data.get('nodes', [])) == 3 and all(n.get('peer_id') in (1, 2, 3) for n in data.get('nodes', []))" \
  "${RESULT_DIR}/membership_after_failover.json" \
  90

wait_for_json_field \
  "${new_leader_port}" \
  "QueryService/QueryClusterStatus" \
  "${empty_body}" \
  "data.get('cluster_name') == '${CLUSTER_NAME}' and data.get('raft_leader_info', {}).get('peer_id') == ${new_leader_id}" \
  "${RESULT_DIR}/cluster_status_after_failover.json" \
  90

wait_for_json_field \
  "${new_leader_port}" \
  "QueryService/ListNamespace" \
  "{\"id\":${request_id},\"read_stale\":false,\"namespace_name\":\"${namespace_before}\"}" \
  "len(data.get('namespaces', [])) >= 1" \
  "${RESULT_DIR}/list_namespace_after_failover.json" \
  90

follower_port="$(surviving_follower_port "${new_leader_id}" "${leader_id}")"
wait_for_json_field \
  "${follower_port}" \
  "QueryService/ListNamespace" \
  "{\"id\":${request_id},\"read_stale\":true,\"namespace_name\":\"${namespace_before}\"}" \
  "len(data.get('namespaces', [])) >= 1" \
  "${RESULT_DIR}/follower_stale_list_namespace_after_failover.json" \
  90
echo "surviving_follower_stale_read=127.0.0.1:${follower_port}" | tee -a "${RESULT_DIR}/summary.txt"

namespace_after="failover_after_$(date +%s)"
post_json \
  "${new_leader_port}" \
  "ManageService/AddNamespace" \
  "{\"id\":${request_id},\"name\":\"${namespace_after}\"}" \
  > "${RESULT_DIR}/add_namespace_after_failover.json"
check_status_ok "${RESULT_DIR}/add_namespace_after_failover.json"

wait_for_json_field \
  "${new_leader_port}" \
  "QueryService/ListNamespace" \
  "{\"id\":${request_id},\"read_stale\":false,\"namespace_name\":\"${namespace_after}\"}" \
  "len(data.get('namespaces', [])) >= 1" \
  "${RESULT_DIR}/list_namespace_new_after_failover.json" \
  60

wait_for_json_field \
  "${new_leader_port}" \
  "QueryService/QueryClusterStatus" \
  "${empty_body}" \
  "data.get('cluster_name') == '${CLUSTER_NAME}' and data.get('raft_leader_info', {}).get('peer_id') == ${new_leader_id}" \
  "${RESULT_DIR}/cluster_status_after_namespace_write.json" \
  60

post_json "${new_leader_port}" "RaftControlService/TriggerSnapshot" "${empty_body}" \
  > "${RESULT_DIR}/snapshot_after_failover.json"
check_status_ok "${RESULT_DIR}/snapshot_after_failover.json"

for i in 1 2 3; do
  [[ -f "${RESULT_DIR}/metaserver${i}.pid" ]] || continue
  scrape_vars "after_failover" "$((MS_PORT + (i - 1) * MS_PORT_STEP))"
done
collect_diagnostics "success"

echo "namespace_before=${namespace_before}" | tee -a "${RESULT_DIR}/summary.txt"
echo "namespace_after=${namespace_after}" | tee -a "${RESULT_DIR}/summary.txt"
echo "metrics_summary=${RESULT_DIR}/metrics_summary.txt" | tee -a "${RESULT_DIR}/summary.txt"
echo "PASS metaserver raft leader failover" | tee -a "${RESULT_DIR}/summary.txt"
echo "${RESULT_DIR}"
