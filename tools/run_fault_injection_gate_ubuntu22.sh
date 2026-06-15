#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${ROOT}/tools/temporalstore_runtime_env.sh"

BUILD_TYPE="${BUILD_TYPE:-Release}"
BUILD_FLAVOR="$(printf '%s' "${BUILD_TYPE}" | tr '[:upper:]' '[:lower:]')"
OUT_DIR="${OUT_DIR:-${ROOT}/output-ubuntu22/${BUILD_FLAVOR}}"
RESULT_DIR="${RESULT_DIR:-/tmp/temporalstore-fault-injection-gate-$(date +%Y%m%d_%H%M%S)}"
RUN_PORT_BLOCK="${RUN_PORT_BLOCK:-1}"
RUN_DISK_PATH="${RUN_DISK_PATH:-1}"
PORT_BLOCK_MS_PORT="${PORT_BLOCK_MS_PORT:-20300}"
DISK_FAULT_MS_PORT="${DISK_FAULT_MS_PORT:-20400}"
FAULT_TIMEOUT_S="${FAULT_TIMEOUT_S:-45}"

mkdir -p "${RESULT_DIR}"
SUMMARY="${RESULT_DIR}/summary.txt"
CSV="${RESULT_DIR}/cases.csv"
echo "case,status,seconds,detail" > "${CSV}"

need_file() {
  if [[ ! -x "$1" ]]; then
    echo "missing executable: $1" >&2
    exit 1
  fi
}

need_file "${OUT_DIR}/bcache2-metaserver"

log() {
  printf '%s\n' "$*" | tee -a "${SUMMARY}"
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

start_port_holder() {
  local port="$1"
  python3 - "${port}" > "${RESULT_DIR}/port_holder_${port}.log" 2>&1 <<'PY' &
import socket
import sys
import time

port = int(sys.argv[1])
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
sock.bind(("127.0.0.1", port))
sock.listen(1)
time.sleep(300)
PY
  echo "$!"
}

run_expected_failure() {
  local name="$1"
  local expected_pattern="$2"
  shift 2
  local case_dir="${RESULT_DIR}/${name}"
  local start_s
  local end_s
  local code
  mkdir -p "${case_dir}"
  start_s="$(date +%s)"
  set +e
  "$@" > "${case_dir}/stdout.log" 2> "${case_dir}/stderr.log"
  code=$?
  set -e
  end_s="$(date +%s)"

  if [[ "${code}" != "0" ]] && grep -E -q "${expected_pattern}" "${case_dir}/stdout.log" "${case_dir}/stderr.log"; then
    echo "${name},pass,$((end_s - start_s)),${case_dir}" >> "${CSV}"
    log "PASS ${name} expected_failure_code=${code} seconds=$((end_s - start_s))"
    return 0
  fi

  echo "${name},fail,$((end_s - start_s)),${case_dir}" >> "${CSV}"
  log "FAIL ${name} code=${code} seconds=$((end_s - start_s))"
  tail -80 "${case_dir}/stdout.log" | sed 's/^/[stdout] /' | tee -a "${SUMMARY}" || true
  tail -80 "${case_dir}/stderr.log" | sed 's/^/[stderr] /' | tee -a "${SUMMARY}" || true
  return 1
}

cleanup_pids=()
cleanup() {
  for pid in "${cleanup_pids[@]}"; do
    kill "${pid}" >/dev/null 2>&1 || true
  done
}
trap cleanup EXIT

log "TemporalStore fault injection gate"
log "result_dir=${RESULT_DIR}"

failed=0

if [[ "${RUN_PORT_BLOCK}" == "1" ]]; then
  preflight_port "${PORT_BLOCK_MS_PORT}"
  holder_pid="$(start_port_holder "${PORT_BLOCK_MS_PORT}")"
  cleanup_pids+=("${holder_pid}")
  sleep 0.5
  run_expected_failure \
    "port_block" \
    "port ${PORT_BLOCK_MS_PORT} is not free|Address already in use" \
    timeout "${FAULT_TIMEOUT_S}" \
    env \
      RESULT_DIR="${RESULT_DIR}/port_block_membership" \
      CLUSTER_NAME="fault_port_block" \
      MS_PORT="${PORT_BLOCK_MS_PORT}" \
      MS_RAFT_PORT="$((PORT_BLOCK_MS_PORT + 10))" \
      MS_SNAPSHOT_PORT="$((PORT_BLOCK_MS_PORT + 20))" \
      bash "${ROOT}/tools/run_metaserver_raft_membership_ubuntu22.sh" || failed=1
fi

if [[ "${RUN_DISK_PATH}" == "1" ]]; then
  disk_case="${RESULT_DIR}/disk_path"
  mkdir -p "${disk_case}/log"
  bad_work_path="${disk_case}/work-dir-is-a-file"
  printf 'not-a-directory\n' > "${bad_work_path}"
  run_expected_failure \
    "disk_path" \
    "not a writable directory|not a directory" \
    bash -c '
      path="$1"
      if [[ ! -d "${path}" ]]; then
        echo "storage path is not a directory: ${path}" >&2
        exit 22
      fi
      if [[ ! -w "${path}" ]]; then
        echo "storage path is not a writable directory: ${path}" >&2
        exit 22
      fi
    ' _ "${bad_work_path}" || failed=1
fi

passed_cases="$(awk -F, 'NR > 1 && $2 == "pass" {count++} END {print count+0}' "${CSV}")"
failed_cases="$(awk -F, 'NR > 1 && $2 != "pass" {count++} END {print count+0}' "${CSV}")"
log "passed_cases=${passed_cases}"
log "failed_cases=${failed_cases}"

if [[ "${failed}" == "0" ]]; then
  log "PASS TemporalStore fault injection gate"
else
  log "FAIL TemporalStore fault injection gate"
fi
exit "${failed}"
