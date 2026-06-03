#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_TYPE="${BUILD_TYPE:-Debug}"
BUILD_FLAVOR="$(printf '%s' "${BUILD_TYPE}" | tr '[:upper:]' '[:lower:]')"
BUILD_DIR="${BUILD_DIR:-${ROOT}/build-ubuntu22/${BUILD_FLAVOR}}"
OUT_DIR="${OUT_DIR:-${ROOT}/output-ubuntu22/${BUILD_FLAVOR}}"
RESULT_DIR="${RESULT_DIR:-/tmp/temporalstore-module-latency-$(date +%Y%m%d-%H%M%S)}"

META_COUNT="${META_COUNT:-2}"
SERVER_COUNT="${SERVER_COUNT:-2}"
REPLICA_COUNT="${REPLICA_COUNT:-2}"
MS_PORT="${MS_PORT:-18300}"
MS_RAFT_PORT="${MS_RAFT_PORT:-18310}"
MS_SNAPSHOT_PORT="${MS_SNAPSHOT_PORT:-18320}"
SERVER_PORT="${SERVER_PORT:-18301}"
MS_PORT_STEP="${MS_PORT_STEP:-30}"
CLUSTER_NAME="${CLUSTER_NAME:-modulelat}"
NAMESPACE_NAME="${NAMESPACE_NAME:-module_ns}"
TABLE_NAME="${TABLE_NAME:-module_table}"
IDC="${IDC:-vdc1}"
OPS_PER_CASE="${OPS_PER_CASE:-200}"
WARMUP_SECONDS="${WARMUP_SECONDS:-5}"

BENCH="${BUILD_DIR}/src/client/example/module_latency_benchmark"
if [[ ! -x "${BENCH}" ]]; then
  echo "missing benchmark binary: ${BENCH}" >&2
  exit 1
fi

mkdir -p "${RESULT_DIR}"
runtime_dir="${RESULT_DIR}/runtime"
launcher_log="${RESULT_DIR}/launcher.log"
launcher_pid=""

cleanup() {
  for pid_file in "${runtime_dir}"/server*.pid "${runtime_dir}"/metaserver*.pid; do
    [[ -f "${pid_file}" ]] || continue
    kill "$(cat "${pid_file}")" >/dev/null 2>&1 || true
  done
  if [[ -n "${launcher_pid}" ]] && kill -0 "${launcher_pid}" >/dev/null 2>&1; then
    kill "${launcher_pid}" >/dev/null 2>&1 || true
  fi
  pkill -f "bcache2-metaserver.*metaserver_cluster_name=${CLUSTER_NAME}" >/dev/null 2>&1 || true
  pkill -f "bcache2-server.*cluster_name=${CLUSTER_NAME}" >/dev/null 2>&1 || true
  sleep 0.5
  pkill -9 -f "bcache2-metaserver.*metaserver_cluster_name=${CLUSTER_NAME}" >/dev/null 2>&1 || true
  pkill -9 -f "bcache2-server.*cluster_name=${CLUSTER_NAME}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "result_dir=${RESULT_DIR}"
echo "ops_per_case=${OPS_PER_CASE}"

KEEP_RUNNING=1 \
OUT_DIR="${OUT_DIR}" \
SMOKE_DIR="${runtime_dir}" \
CLUSTER_NAME="${CLUSTER_NAME}" \
NAMESPACE_NAME="${NAMESPACE_NAME}" \
TABLE_NAME="${TABLE_NAME}" \
META_COUNT="${META_COUNT}" \
SERVER_COUNT="${SERVER_COUNT}" \
REPLICA_COUNT="${REPLICA_COUNT}" \
MS_PORT="${MS_PORT}" \
MS_RAFT_PORT="${MS_RAFT_PORT}" \
MS_SNAPSHOT_PORT="${MS_SNAPSHOT_PORT}" \
MS_PORT_STEP="${MS_PORT_STEP}" \
SERVER_PORT="${SERVER_PORT}" \
bash "${ROOT}/tools/smoke_ubuntu22.sh" > "${launcher_log}" 2>&1 &
launcher_pid="$!"

for _ in $(seq 1 180); do
  if grep -q "TemporalStore Ubuntu smoke test passed" "${launcher_log}" 2>/dev/null; then
    break
  fi
  if ! kill -0 "${launcher_pid}" >/dev/null 2>&1; then
    echo "module benchmark cluster launcher exited early" >&2
    cat "${launcher_log}" >&2 || true
    exit 1
  fi
  sleep 0.5
done

if ! grep -q "TemporalStore Ubuntu smoke test passed" "${launcher_log}" 2>/dev/null; then
  echo "module benchmark cluster did not become ready" >&2
  cat "${launcher_log}" >&2 || true
  exit 1
fi

echo "cluster_ready=1"
grep "metaserver leader:" "${launcher_log}" | tail -n 1 || true
echo "warmup_seconds=${WARMUP_SECONDS}"
sleep "${WARMUP_SECONDS}"

"${BENCH}" "127.0.0.1:${MS_PORT}" "${IDC}" "${NAMESPACE_NAME}" "${TABLE_NAME}" \
  "${OPS_PER_CASE}" | tee "${RESULT_DIR}/module_latency.csv"

echo "wrote:"
echo "  ${RESULT_DIR}/module_latency.csv"
echo "  ${launcher_log}"
