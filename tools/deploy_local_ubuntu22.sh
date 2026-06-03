#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_TYPE="${BUILD_TYPE:-Debug}"
BUILD_FLAVOR="$(printf '%s' "${BUILD_TYPE}" | tr '[:upper:]' '[:lower:]')"
OUT_DIR="${OUT_DIR:-${ROOT}/output-ubuntu22/${BUILD_FLAVOR}}"
DEPLOY_DIR="${DEPLOY_DIR:-/tmp/temporalstore-deploy}"
RUNTIME_DIR="${RUNTIME_DIR:-${DEPLOY_DIR}/runtime}"
CLUSTER_NAME="${CLUSTER_NAME:-localdeploy}"
NAMESPACE_NAME="${NAMESPACE_NAME:-deploy_ns}"
TABLE_NAME="${TABLE_NAME:-deploy_table}"
MS_PORT="${MS_PORT:-18000}"
MS_RAFT_PORT="${MS_RAFT_PORT:-18010}"
MS_SNAPSHOT_PORT="${MS_SNAPSHOT_PORT:-18020}"
META_COUNT="${META_COUNT:-1}"
MS_PORT_STEP="${MS_PORT_STEP:-30}"
SERVER_PORT="${SERVER_PORT:-18001}"
SERVER_COUNT="${SERVER_COUNT:-1}"
REPLICA_COUNT="${REPLICA_COUNT:-${SERVER_COUNT}}"

action="${1:-start}"

stop_deploy() {
  if [[ -f "${DEPLOY_DIR}/launcher.pid" ]]; then
    kill "$(cat "${DEPLOY_DIR}/launcher.pid")" >/dev/null 2>&1 || true
  fi
  if [[ -f "${RUNTIME_DIR}/server.pid" ]]; then
    kill "$(cat "${RUNTIME_DIR}/server.pid")" >/dev/null 2>&1 || true
  fi
  for pid_file in "${RUNTIME_DIR}"/server*.pid; do
    [[ -f "${pid_file}" ]] || continue
    kill "$(cat "${pid_file}")" >/dev/null 2>&1 || true
  done
  if [[ -f "${RUNTIME_DIR}/metaserver.pid" ]]; then
    kill "$(cat "${RUNTIME_DIR}/metaserver.pid")" >/dev/null 2>&1 || true
  fi
  for pid_file in "${RUNTIME_DIR}"/metaserver*.pid; do
    [[ -f "${pid_file}" ]] || continue
    kill "$(cat "${pid_file}")" >/dev/null 2>&1 || true
  done
  pkill -f "bcache2-metaserver.*metaserver_cluster_name=${CLUSTER_NAME}" >/dev/null 2>&1 || true
  pkill -f "bcache2-server.*cluster_name=${CLUSTER_NAME}" >/dev/null 2>&1 || true
  sleep 0.5
  pkill -9 -f "bcache2-metaserver.*metaserver_cluster_name=${CLUSTER_NAME}" >/dev/null 2>&1 || true
  pkill -9 -f "bcache2-server.*cluster_name=${CLUSTER_NAME}" >/dev/null 2>&1 || true
}

post_json() {
  local port="${3:-${MS_PORT}}"
  local path="$1"
  local body="$2"
  curl -fsS -m 3 \
    -H "Content-Type: application/json" \
    -d "${body}" \
    "http://127.0.0.1:${port}/${path}"
}

status_deploy() {
  local request_id="{\"cluster_name\":\"${CLUSTER_NAME}\",\"operator_name\":\"deploy\"}"
  for i in $(seq 1 "${META_COUNT}"); do
    local ms_port=$((MS_PORT + (i - 1) * MS_PORT_STEP))
    echo "metaserver${i} 127.0.0.1:${ms_port}"
    post_json "QueryService/QueryLeader" "{\"id\":${request_id}}" "${ms_port}" || true
    echo
    post_json "QueryService/ListServer" "{\"id\":${request_id},\"read_stale\":false,\"list_all_tag\":true}" "${ms_port}" || true
    echo
    post_json "QueryService/ListPartition" "{\"id\":${request_id},\"read_stale\":false,\"namespace_name\":\"${NAMESPACE_NAME}\",\"table_name\":\"${TABLE_NAME}\"}" "${ms_port}" || true
    echo
  done
}

start_deploy() {
  stop_deploy
  rm -rf "${DEPLOY_DIR}"
  mkdir -p "${DEPLOY_DIR}"

  nohup env \
    KEEP_RUNNING=1 \
    OUT_DIR="${OUT_DIR}" \
    SMOKE_DIR="${RUNTIME_DIR}" \
    CLUSTER_NAME="${CLUSTER_NAME}" \
    NAMESPACE_NAME="${NAMESPACE_NAME}" \
    TABLE_NAME="${TABLE_NAME}" \
    MS_PORT="${MS_PORT}" \
    MS_RAFT_PORT="${MS_RAFT_PORT}" \
    MS_SNAPSHOT_PORT="${MS_SNAPSHOT_PORT}" \
    META_COUNT="${META_COUNT}" \
    MS_PORT_STEP="${MS_PORT_STEP}" \
    SERVER_PORT="${SERVER_PORT}" \
    SERVER_COUNT="${SERVER_COUNT}" \
    REPLICA_COUNT="${REPLICA_COUNT}" \
    bash "${ROOT}/tools/smoke_ubuntu22.sh" \
    > "${DEPLOY_DIR}/launcher.log" \
    2>&1 &
  echo "$!" > "${DEPLOY_DIR}/launcher.pid"

  for _ in $(seq 1 180); do
    if grep -q "TemporalStore Ubuntu smoke test passed" "${DEPLOY_DIR}/launcher.log" 2>/dev/null; then
      echo "TemporalStore local deployment is running"
      echo "cluster: ${CLUSTER_NAME}"
      grep "metaserver leader:" "${DEPLOY_DIR}/launcher.log" | tail -n 1
      for i in $(seq 1 "${META_COUNT}"); do
        echo "metaserver${i}: 127.0.0.1:$((MS_PORT + (i - 1) * MS_PORT_STEP)) pid=$(cat "${RUNTIME_DIR}/metaserver${i}.pid")"
      done
      for i in $(seq 1 "${SERVER_COUNT}"); do
        echo "server${i}: 127.0.0.1:$((SERVER_PORT + i - 1)) pid=$(cat "${RUNTIME_DIR}/server${i}.pid")"
      done
      echo "logs: ${DEPLOY_DIR}"
      return
    fi
    if ! kill -0 "$(cat "${DEPLOY_DIR}/launcher.pid")" >/dev/null 2>&1; then
      echo "deployment launcher exited early" >&2
      cat "${DEPLOY_DIR}/launcher.log" >&2 || true
      exit 1
    fi
    sleep 0.5
  done

  echo "deployment did not become ready in time" >&2
  cat "${DEPLOY_DIR}/launcher.log" >&2 || true
  exit 1
}

case "${action}" in
  start)
    start_deploy
    ;;
  stop)
    stop_deploy
    ;;
  status)
    status_deploy
    ;;
  *)
    echo "usage: $0 [start|stop|status]" >&2
    exit 2
    ;;
esac
