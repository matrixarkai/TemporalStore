#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${ROOT}/tools/temporalstore_runtime_env.sh"
BUILD_TYPE="${BUILD_TYPE:-Debug}"
BUILD_FLAVOR="$(printf '%s' "${BUILD_TYPE}" | tr '[:upper:]' '[:lower:]')"
BUILD_DIR="${BUILD_DIR:-${ROOT}/build-ubuntu22/${BUILD_FLAVOR}}"
OUT_DIR="${OUT_DIR:-${ROOT}/output-ubuntu22/${BUILD_FLAVOR}}"
SMOKE_DIR="${SMOKE_DIR:-/tmp/temporalstore-ssd-server-smoke}"
SSD_PATH="${SSD_PATH:-${TEMPORALSTORE_BLOCKCACHE_SSD_PATH}}"
LAUNCHER_LOG="${LAUNCHER_LOG:-${SMOKE_DIR}.log}"
LAUNCHER_PID="${LAUNCHER_PID:-${SMOKE_DIR}.pid}"
CLUSTER_NAME="${CLUSTER_NAME:-ssdblocksmoke}"
NAMESPACE_NAME="${NAMESPACE_NAME:-ns_ssd}"
TABLE_NAME="${TABLE_NAME:-tbl_ssd}"
MS_PORT="${MS_PORT:-18100}"
MS_RAFT_PORT="${MS_RAFT_PORT:-18110}"
MS_SNAPSHOT_PORT="${MS_SNAPSHOT_PORT:-18120}"
SERVER_PORT="${SERVER_PORT:-18101}"

cleanup() {
  local status=$?
  if [[ -f "${SMOKE_DIR}/server.pid" ]]; then
    kill "$(cat "${SMOKE_DIR}/server.pid")" >/dev/null 2>&1 || true
  fi
  if [[ -f "${SMOKE_DIR}/metaserver.pid" ]]; then
    kill "$(cat "${SMOKE_DIR}/metaserver.pid")" >/dev/null 2>&1 || true
  fi
  if [[ -f "${LAUNCHER_PID}" ]]; then
    kill "$(cat "${LAUNCHER_PID}")" >/dev/null 2>&1 || true
  fi
  pkill -f "bcache2-server.*cluster_name=${CLUSTER_NAME}" >/dev/null 2>&1 || true
  pkill -f "bcache2-metaserver.*metaserver_cluster_name=${CLUSTER_NAME}" >/dev/null 2>&1 || true
  return "${status}"
}
trap cleanup EXIT

pkill -f "bcache2-server.*cluster_name=${CLUSTER_NAME}" >/dev/null 2>&1 || true
pkill -f "bcache2-metaserver.*metaserver_cluster_name=${CLUSTER_NAME}" >/dev/null 2>&1 || true
rm -rf "${SMOKE_DIR}" "${SSD_PATH}" "${LAUNCHER_LOG}" "${LAUNCHER_PID}"
mkdir -p "${SMOKE_DIR}"

TEMPORALSTORE_BLOCKCACHE_SSD_PATH="${SSD_PATH}"
SERVER_EXTRA_FLAGS="$(temporalstore_blockcache_flags | tr '\n' ' ')"

(
  cd "${ROOT}"
  SMOKE_DIR="${SMOKE_DIR}" \
  CLUSTER_NAME="${CLUSTER_NAME}" \
  NAMESPACE_NAME="${NAMESPACE_NAME}" \
  TABLE_NAME="${TABLE_NAME}" \
  MS_PORT="${MS_PORT}" \
  MS_RAFT_PORT="${MS_RAFT_PORT}" \
  MS_SNAPSHOT_PORT="${MS_SNAPSHOT_PORT}" \
  SERVER_PORT="${SERVER_PORT}" \
  OUT_DIR="${OUT_DIR}" \
  KEEP_RUNNING=1 \
  SERVER_EXTRA_FLAGS="${SERVER_EXTRA_FLAGS}" \
  bash tools/smoke_ubuntu22.sh
) > "${LAUNCHER_LOG}" 2>&1 &
echo "$!" > "${LAUNCHER_PID}"

for _ in $(seq 1 120); do
  if grep -q "TemporalStore Ubuntu smoke test passed" "${LAUNCHER_LOG}"; then
    break
  fi
  if ! kill -0 "$(cat "${LAUNCHER_PID}")" >/dev/null 2>&1; then
    echo "server smoke launcher exited early" >&2
    tail -n 160 "${LAUNCHER_LOG}" >&2 || true
    exit 1
  fi
  sleep 0.5
done

if ! grep -q "TemporalStore Ubuntu smoke test passed" "${LAUNCHER_LOG}"; then
  echo "timed out waiting for server smoke readiness" >&2
  tail -n 160 "${LAUNCHER_LOG}" >&2 || true
  exit 1
fi

leader="$(awk '/metaserver leader:/ {print $3}' "${LAUNCHER_LOG}")"
if [[ -z "${leader}" ]]; then
  leader="127.0.0.1:${MS_PORT}"
fi

for attempt in $(seq 1 20); do
  if "${BUILD_DIR}/src/client/example/module_ingest_query_example" \
    "${leader}" vdc1 "${NAMESPACE_NAME}" "${TABLE_NAME}" \
    > "${SMOKE_DIR}/module_ingest.log" 2>&1; then
    echo "module_ingest_attempt=${attempt}" > "${SMOKE_DIR}/module_ingest_attempt.txt"
    break
  fi
  if ! grep -q "Slot not found" "${SMOKE_DIR}/module_ingest.log"; then
    cat "${SMOKE_DIR}/module_ingest.log" >&2 || true
    exit 1
  fi
  sleep 1
done

if ! grep -q "module_ingest_attempt=" "${SMOKE_DIR}/module_ingest_attempt.txt" 2>/dev/null; then
  echo "module ingest did not become ready" >&2
  cat "${SMOKE_DIR}/module_ingest.log" >&2 || true
  exit 1
fi

sleep 1

echo "SSD blockcache server smoke passed"
echo "leader=${leader}"
echo "smoke_dir=${SMOKE_DIR}"
echo "launcher_log=${LAUNCHER_LOG}"
echo "ssd_path=${SSD_PATH}"
echo "--- module ingest tail ---"
tail -n 80 "${SMOKE_DIR}/module_ingest.log" || true
echo "--- server log SSD-cache evidence ---"
grep -RInE "ssd_terarkdb|multi-ssd|StorageEngineTerarkDB|blockcache|UnifiedCache" \
  "${SMOKE_DIR}/server1/log" "${SMOKE_DIR}/server1/stderr" "${SMOKE_DIR}/server1/stdout" \
  2>/dev/null | tail -n 80 || true
echo "--- SSD files ---"
find "${SSD_PATH}" -maxdepth 3 -type f -printf "%p %s bytes\n" 2>/dev/null | sort | head -n 120
echo "--- SSD total ---"
du -sh "${SSD_PATH}" 2>/dev/null || true
