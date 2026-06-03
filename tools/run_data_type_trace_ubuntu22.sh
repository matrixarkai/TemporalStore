#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_TYPE="${BUILD_TYPE:-Debug}"
BUILD_FLAVOR="$(printf '%s' "${BUILD_TYPE}" | tr '[:upper:]' '[:lower:]')"
BUILD_DIR="${BUILD_DIR:-${ROOT}/build-ubuntu22/${BUILD_FLAVOR}}"
OUT_DIR="${OUT_DIR:-${ROOT}/output-ubuntu22/${BUILD_FLAVOR}}"
RESULT_DIR="${RESULT_DIR:-/tmp/temporalstore-data-type-trace-$(date +%Y%m%d-%H%M%S)}"
RUNTIME_DIR="${RUNTIME_DIR:-/tmp/temporalstore-data-type-trace-runtime-$(date +%Y%m%d-%H%M%S)}"

META_COUNT="${META_COUNT:-2}"
SERVER_COUNT="${SERVER_COUNT:-2}"
REPLICA_COUNT="${REPLICA_COUNT:-2}"
MS_PORT="${MS_PORT:-20200}"
MS_RAFT_PORT="${MS_RAFT_PORT:-20210}"
MS_SNAPSHOT_PORT="${MS_SNAPSHOT_PORT:-20220}"
SERVER_PORT="${SERVER_PORT:-20201}"
MS_PORT_STEP="${MS_PORT_STEP:-30}"
CLUSTER_NAME="${CLUSTER_NAME:-datatype_trace}"
NAMESPACE_NAME="${NAMESPACE_NAME:-datatype_ns}"
TABLE_NAME="${TABLE_NAME:-datatype_table}"
IDC="${IDC:-vdc1}"
OPS_PER_CASE="${OPS_PER_CASE:-5}"
WARMUP_SECONDS="${WARMUP_SECONDS:-3}"
METASERVER_LOG_LEVEL="${METASERVER_LOG_LEVEL:-0}"
SERVER_LOG_LEVEL="${SERVER_LOG_LEVEL:-0}"

INGEST_QUERY="${BUILD_DIR}/src/client/example/module_ingest_query_example"
LATENCY_BENCH="${BUILD_DIR}/src/client/example/module_latency_benchmark"
if [[ ! -x "${INGEST_QUERY}" ]]; then
  echo "missing data type binary: ${INGEST_QUERY}" >&2
  exit 1
fi
if [[ ! -x "${LATENCY_BENCH}" ]]; then
  echo "missing latency binary: ${LATENCY_BENCH}" >&2
  exit 1
fi

mkdir -p "${RESULT_DIR}" "${RUNTIME_DIR}"
launcher_log="${RESULT_DIR}/launcher.log"
launcher_pid=""

copy_logs() {
  mkdir -p "${RESULT_DIR}/logs"
  for node_dir in "${RUNTIME_DIR}"/metaserver* "${RUNTIME_DIR}"/server*; do
    [[ -d "${node_dir}" ]] || continue
    local node_name
    node_name="$(basename "${node_dir}")"
    mkdir -p "${RESULT_DIR}/logs/${node_name}"
    cp -a "${node_dir}/stdout" "${RESULT_DIR}/logs/${node_name}/" 2>/dev/null || true
    cp -a "${node_dir}/stderr" "${RESULT_DIR}/logs/${node_name}/" 2>/dev/null || true
    cp -a "${node_dir}/log" "${RESULT_DIR}/logs/${node_name}/" 2>/dev/null || true
  done
}

cleanup() {
  local status=$?
  copy_logs
  for pid_file in "${RUNTIME_DIR}"/server*.pid "${RUNTIME_DIR}"/metaserver*.pid; do
    [[ -f "${pid_file}" ]] || continue
    kill "$(cat "${pid_file}")" >/dev/null 2>&1 || true
  done
  if [[ -n "${launcher_pid}" ]] && kill -0 "${launcher_pid}" >/dev/null 2>&1; then
    kill "${launcher_pid}" >/dev/null 2>&1 || true
  fi
  pkill -f "bcache2-metaserver.*metaserver_cluster_name=${CLUSTER_NAME}" >/dev/null 2>&1 || true
  pkill -f "bcache2-server.*cluster_name=${CLUSTER_NAME}" >/dev/null 2>&1 || true
  exit "${status}"
}
trap cleanup EXIT

cat > "${RESULT_DIR}/run_config.txt" <<EOF
build_dir=${BUILD_DIR}
runtime_dir=${RUNTIME_DIR}
meta_count=${META_COUNT}
server_count=${SERVER_COUNT}
replica_count=${REPLICA_COUNT}
metaserver_log_level=${METASERVER_LOG_LEVEL}
server_log_level=${SERVER_LOG_LEVEL}
ops_per_case=${OPS_PER_CASE}
EOF

KEEP_RUNNING=1 \
OUT_DIR="${OUT_DIR}" \
SMOKE_DIR="${RUNTIME_DIR}" \
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
METASERVER_LOG_LEVEL="${METASERVER_LOG_LEVEL}" \
SERVER_LOG_LEVEL="${SERVER_LOG_LEVEL}" \
bash "${ROOT}/tools/smoke_ubuntu22.sh" > "${launcher_log}" 2>&1 &
launcher_pid="$!"

for _ in $(seq 1 180); do
  if grep -q "TemporalStore Ubuntu smoke test passed" "${launcher_log}" 2>/dev/null; then
    break
  fi
  if ! kill -0 "${launcher_pid}" >/dev/null 2>&1; then
    echo "data type trace launcher exited early" >&2
    cat "${launcher_log}" >&2 || true
    exit 1
  fi
  sleep 0.5
done

if ! grep -q "TemporalStore Ubuntu smoke test passed" "${launcher_log}" 2>/dev/null; then
  echo "data type trace cluster did not become ready" >&2
  cat "${launcher_log}" >&2 || true
  exit 1
fi

sleep "${WARMUP_SECONDS}"

"${INGEST_QUERY}" "127.0.0.1:${MS_PORT}" "${IDC}" "${NAMESPACE_NAME}" "${TABLE_NAME}" \
  > "${RESULT_DIR}/data_type_functional.out" 2> "${RESULT_DIR}/data_type_functional.err"

"${LATENCY_BENCH}" "127.0.0.1:${MS_PORT}" "${IDC}" "${NAMESPACE_NAME}" "${TABLE_NAME}" \
  "${OPS_PER_CASE}" > "${RESULT_DIR}/data_type_latency.csv" 2> "${RESULT_DIR}/data_type_latency.err"

copy_logs

trace_tmp="${RESULT_DIR}/trace_lines.tmp"
find "${RESULT_DIR}/logs" -type f -print0 \
  | xargs -0 grep -h -E "RPC Received|RPC Finished|TraceId|ModuleId|rpc trace|manage rpc trace" \
  > "${trace_tmp}" 2>/dev/null || true
{
  echo "trace line counts"
  wc -l < "${trace_tmp}"
  echo
  echo "sample trace lines"
  head -n 80 "${trace_tmp}" || true
} > "${RESULT_DIR}/trace_summary.txt"
rm -f "${trace_tmp}"

echo "wrote:"
echo "  ${RESULT_DIR}/run_config.txt"
echo "  ${RESULT_DIR}/data_type_functional.out"
echo "  ${RESULT_DIR}/data_type_latency.csv"
echo "  ${RESULT_DIR}/trace_summary.txt"
echo "  ${RESULT_DIR}/logs"
