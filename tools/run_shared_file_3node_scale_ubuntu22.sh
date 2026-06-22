#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
: "${TEMPORALSTORE_STORAGE_ZONE_SIZE:=268435456}"
: "${TEMPORALSTORE_STREAM_MAX_BLOB_SIZE:=268435456}"
: "${TEMPORALSTORE_REPLICATOR_OUT_OF_SYNC_S:=120}"
source "${ROOT}/tools/temporalstore_runtime_env.sh"
BUILD_TYPE="${BUILD_TYPE:-Debug}"
BUILD_FLAVOR="$(printf '%s' "${BUILD_TYPE}" | tr '[:upper:]' '[:lower:]')"
OUT_DIR="${OUT_DIR:-${ROOT}/output-ubuntu22/${BUILD_FLAVOR}}"
BIN_DIR="${BIN_DIR:-${ROOT}/build-ubuntu22/${BUILD_FLAVOR}/src/client/example}"
RESULT_DIR="${RESULT_DIR:-/tmp/temporalstore-shared-file-3node-scale-$(date +%Y%m%d_%H%M%S)}"
SMOKE_DIR="${SMOKE_DIR:-${RESULT_DIR}/cluster}"
CLUSTER_NAME="${CLUSTER_NAME:-shared_file_3node_scale}"
NAMESPACE_NAME="${NAMESPACE_NAME:-ns1}"
TABLE_NAME="${TABLE_NAME:-table1}"
ID_C="${ID_C:-vdc1}"
MS_PORT="${MS_PORT:-23000}"
MS_RAFT_PORT="${MS_RAFT_PORT:-23100}"
MS_SNAPSHOT_PORT="${MS_SNAPSHOT_PORT:-23200}"
MS_PORT_STEP="${MS_PORT_STEP:-30}"
SERVER_PORT="${SERVER_PORT:-23300}"
PROXY_PORT="${PROXY_PORT:-23400}"
STRING_OPS="${STRING_OPS:-30000}"
STRING_THREADS="${STRING_THREADS:-16}"
STRING_VALUE_BYTES="${STRING_VALUE_BYTES:-256}"
STRING_REPLICA_WAIT_MS="${STRING_REPLICA_WAIT_MS:-3000}"
SEQUENCE_KEYS="${SEQUENCE_KEYS:-12}"
SEQUENCE_ROWS_PER_KEY="${SEQUENCE_ROWS_PER_KEY:-3000}"
SEQUENCE_QUERY_OPS="${SEQUENCE_QUERY_OPS:-3000}"
SEQUENCE_THREADS="${SEQUENCE_THREADS:-16}"
SEQUENCE_REPLICA_WAIT_MS="${SEQUENCE_REPLICA_WAIT_MS:-5000}"
RUN_PROXY_INGESTION_PRESSURE="${RUN_PROXY_INGESTION_PRESSURE:-0}"
PROXY_INGESTION_PRESSURE_OPS="${PROXY_INGESTION_PRESSURE_OPS:-1000}"
PROXY_INGESTION_PRESSURE_THREADS="${PROXY_INGESTION_PRESSURE_THREADS:-4}"
PROXY_INGESTION_PRESSURE_VALUE_BYTES="${PROXY_INGESTION_PRESSURE_VALUE_BYTES:-128}"
PROXY_INGESTION_PRESSURE_VERIFY_READS="${PROXY_INGESTION_PRESSURE_VERIFY_READS:-0}"
PROXY_INGESTION_PRESSURE_VERIFY_TIMEOUT_MS="${PROXY_INGESTION_PRESSURE_VERIFY_TIMEOUT_MS:-10000}"
PROXY_INGESTION_PRESSURE_VERIFY_POLL_MS="${PROXY_INGESTION_PRESSURE_VERIFY_POLL_MS:-20}"
PROXY_PIN_PRIMARY_READS="${PROXY_PIN_PRIMARY_READS:-1}"
REQUIRE_FRESH_BINARIES="${REQUIRE_FRESH_BINARIES:-0}"
REQUIRE_NO_TEMPORALSTORE_PROCESSES="${REQUIRE_NO_TEMPORALSTORE_PROCESSES:-0}"

mkdir -p "${RESULT_DIR}"
rm -rf "${SMOKE_DIR}"

fail_if_stale_binary() {
  local binary="$1"
  local source="$2"
  if [[ ! -e "${binary}" ]]; then
    echo "missing binary: ${binary}" >&2
    exit 1
  fi
  if [[ ! -e "${source}" ]]; then
    echo "missing source freshness input: ${source}" >&2
    exit 1
  fi
  if [[ "${source}" -nt "${binary}" ]]; then
    echo "stale binary: ${binary}" >&2
    echo "  newer source: ${source}" >&2
    echo "  rebuild before running C++ transport benchmark gates" >&2
    exit 1
  fi
}

if [[ "${REQUIRE_NO_TEMPORALSTORE_PROCESSES}" == "1" ]]; then
  stale_processes="$(pgrep -af 'bcache2-(metaserver|server|proxy)' || true)"
  if [[ -n "${stale_processes}" ]]; then
    echo "existing TemporalStore processes detected; stop them before benchmark gates:" >&2
    echo "${stale_processes}" >&2
    exit 1
  fi
fi

if [[ "${REQUIRE_FRESH_BINARIES}" == "1" ]]; then
  fail_if_stale_binary "${OUT_DIR}/bcache2-metaserver" "${ROOT}/src/metaserver_v2/main.cc"
  fail_if_stale_binary "${OUT_DIR}/bcache2-server" "${ROOT}/src/server/main.cc"
  fail_if_stale_binary "${OUT_DIR}/bcache2-proxy" "${ROOT}/src/proxy/service.cc"
  fail_if_stale_binary "${BIN_DIR}/proxy_smoke_example" "${ROOT}/src/client/example/proxy_smoke_example.cc"
  fail_if_stale_binary "${BIN_DIR}/proxy_ingestion_pressure_example" \
    "${ROOT}/src/client/example/proxy_ingestion_pressure_example.cc"
fi

cleanup() {
  local status=$?
  if [[ -f "${SMOKE_DIR}/bootstrap.pid" ]]; then
    kill "$(cat "${SMOKE_DIR}/bootstrap.pid")" >/dev/null 2>&1 || true
  fi
  for pid_file in "${SMOKE_DIR}"/server*.pid "${SMOKE_DIR}"/metaserver*.pid "${SMOKE_DIR}"/proxy*.pid; do
    [[ -f "${pid_file}" ]] || continue
    kill "$(cat "${pid_file}")" >/dev/null 2>&1 || true
  done
  pkill -f "bcache2-proxy.*proxy_cluster_name=${CLUSTER_NAME}" >/dev/null 2>&1 || true
  pkill -f "bcache2-metaserver.*metaserver_cluster_name=${CLUSTER_NAME}" >/dev/null 2>&1 || true
  pkill -f "bcache2-server.*cluster_name=${CLUSTER_NAME}" >/dev/null 2>&1 || true
  sleep 0.2
  return "${status}"
}
trap cleanup EXIT

run_and_capture() {
  local name="$1"
  shift
  echo "RUN ${name}: $*" | tee "${RESULT_DIR}/${name}.cmd"
  set +e
  "$@" > "${RESULT_DIR}/${name}.out" 2> "${RESULT_DIR}/${name}.err"
  local code=$?
  set -e
  echo "${code}" > "${RESULT_DIR}/${name}.exit_code"
  return 0
}

run_module_ingest_with_retry() {
  local name="module_ingest"
  echo "RUN ${name}: $*" | tee "${RESULT_DIR}/${name}.cmd"
  local code=1
  for attempt in $(seq 1 10); do
    set +e
    "$@" > "${RESULT_DIR}/${name}.out" 2> "${RESULT_DIR}/${name}.err"
    code=$?
    set -e
    if [[ "${code}" == "0" ]]; then
      break
    fi
    if ! grep -q "Slot not found" "${RESULT_DIR}/${name}.err"; then
      break
    fi
    echo "module_ingest attempt ${attempt} hit Slot not found; retrying" \
      >> "${RESULT_DIR}/${name}.err"
    sleep 1
  done
  echo "${code}" > "${RESULT_DIR}/${name}.exit_code"
  return 0
}


run_proxy_smoke_with_retry() {
  local name="proxy_smoke"
  echo "RUN ${name}: $*" | tee "${RESULT_DIR}/${name}.cmd"
  local code=1
  for attempt in $(seq 1 60); do
    set +e
    "$@" > "${RESULT_DIR}/${name}.out" 2> "${RESULT_DIR}/${name}.err"
    code=$?
    set -e
    if [[ "${code}" == "0" ]]; then
      break
    fi
    echo "proxy_smoke attempt ${attempt} failed; retrying" >> "${RESULT_DIR}/${name}.err"
    sleep 1
  done
  echo "${code}" > "${RESULT_DIR}/${name}.exit_code"
  return 0
}

storage_uri="file://${SMOKE_DIR}/shared/"
server_extra_flags=()
while IFS= read -r flag; do
  server_extra_flags+=("${flag}")
done < <(temporalstore_replicator_loop_flags)
server_extra_flags+=("--storage_enable_evict=false")
server_extra_flags+=("--storage_enable_expire=false")
server_extra_flags+=("--storage_enable_page_gc=false")
server_extra_flags+=("--storage_enable_page_compaction=false")
server_extra_flags+=("--storage_enable_index_gc=false")
server_extra_flags+=("--storage_enable_oplog_rolling=false")
proxy_extra_flags=()
if [[ -n "${PROXY_EXTRA_FLAGS:-}" ]]; then
  # shellcheck disable=SC2206
  proxy_extra_flags=(${PROXY_EXTRA_FLAGS})
fi

mkdir -p "${SMOKE_DIR}"
bootstrap_pid_file="${RESULT_DIR}/bootstrap.pid"
(
  cd "${ROOT}"
  env \
    OUT_DIR="${OUT_DIR}" \
    SMOKE_DIR="${SMOKE_DIR}" \
    CLUSTER_NAME="${CLUSTER_NAME}" \
    NAMESPACE_NAME="${NAMESPACE_NAME}" \
    TABLE_NAME="${TABLE_NAME}" \
    META_COUNT=3 \
    SERVER_COUNT=3 \
    REPLICA_COUNT=3 \
    MS_PORT="${MS_PORT}" \
    MS_RAFT_PORT="${MS_RAFT_PORT}" \
    MS_SNAPSHOT_PORT="${MS_SNAPSHOT_PORT}" \
    MS_PORT_STEP="${MS_PORT_STEP}" \
    SERVER_PORT="${SERVER_PORT}" \
    STORAGE_POOL_URI="${storage_uri}" \
    TEMPORALSTORE_REPLICATOR_OUT_OF_SYNC_S="${TEMPORALSTORE_REPLICATOR_OUT_OF_SYNC_S}" \
    SERVER_EXTRA_FLAGS="${server_extra_flags[*]}" \
    KEEP_RUNNING=1 \
    bash tools/smoke_ubuntu22.sh
) > "${RESULT_DIR}/bootstrap.log" 2>&1 &
echo "$!" > "${bootstrap_pid_file}"

for _ in $(seq 1 180); do
  if grep -q "KEEP_RUNNING=1" "${RESULT_DIR}/bootstrap.log" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$(cat "${bootstrap_pid_file}")" >/dev/null 2>&1; then
    echo "bootstrap exited early" >&2
    cat "${RESULT_DIR}/bootstrap.log" >&2 || true
    exit 1
  fi
  sleep 1
done

if ! grep -q "KEEP_RUNNING=1" "${RESULT_DIR}/bootstrap.log"; then
  echo "bootstrap timed out" >&2
  tail -100 "${RESULT_DIR}/bootstrap.log" >&2 || true
  exit 1
fi

leader="$(awk '/metaserver leader:/ {print $3}' "${RESULT_DIR}/bootstrap.log")"
if [[ -z "${leader}" ]]; then
  echo "could not parse metaserver leader" >&2
  cat "${RESULT_DIR}/bootstrap.log" >&2
  exit 1
fi

run_module_ingest_with_retry "${BIN_DIR}/module_ingest_query_example" \
  "${leader}" "${ID_C}" "${NAMESPACE_NAME}" "${TABLE_NAME}"


proxy_log_dir="${SMOKE_DIR}/proxy/log"
mkdir -p "${proxy_log_dir}"
(
  cd "${ROOT}"
  env BYTED_HOST_IP=127.0.0.1 BYTED_HOST_IPV6= \
    "${OUT_DIR}/bcache2-proxy" \
      --port="${PROXY_PORT}" \
      --master_endpoint="${leader}" \
      --idc="${ID_C}" \
      --proxy_cluster_name="${CLUSTER_NAME}" \
      --proxy_vregion="local" \
      --proxy_vdc="${ID_C}" \
      --proxy_vau="local" \
      --proxy_log_dir="${proxy_log_dir}" \
      --proxy_log_level=2 \
      --proxy_pin_primary_reads="${PROXY_PIN_PRIMARY_READS}" \
      "${proxy_extra_flags[@]}"
) > "${RESULT_DIR}/proxy.out" 2> "${RESULT_DIR}/proxy.err" &
echo "$!" > "${SMOKE_DIR}/proxy.pid"

run_proxy_smoke_with_retry "${BIN_DIR}/proxy_smoke_example" \
  "127.0.0.1:${PROXY_PORT}" "${NAMESPACE_NAME}" "${TABLE_NAME}" "proxy_scale"

if [[ "${RUN_PROXY_INGESTION_PRESSURE}" == "1" ]]; then
  run_and_capture proxy_ingestion_pressure "${BIN_DIR}/proxy_ingestion_pressure_example" \
    "127.0.0.1:${PROXY_PORT}" "${NAMESPACE_NAME}" "${TABLE_NAME}" "proxy_pressure" \
    "${PROXY_INGESTION_PRESSURE_OPS}" "${PROXY_INGESTION_PRESSURE_THREADS}" \
    "${PROXY_INGESTION_PRESSURE_VALUE_BYTES}" "${PROXY_INGESTION_PRESSURE_VERIFY_READS}" \
    "${PROXY_INGESTION_PRESSURE_VERIFY_TIMEOUT_MS}" \
    "${PROXY_INGESTION_PRESSURE_VERIFY_POLL_MS}"
fi

run_and_capture replication_smoke "${BIN_DIR}/replication_smoke_example" \
  "${leader}" "${ID_C}" "${NAMESPACE_NAME}" "${TABLE_NAME}"

run_and_capture string_primary "${BIN_DIR}/string_scale_benchmark" \
  "${leader}" "${ID_C}" "${NAMESPACE_NAME}" "${TABLE_NAME}" \
  "${STRING_OPS}" "${STRING_THREADS}" "${STRING_VALUE_BYTES}" 1 "${STRING_REPLICA_WAIT_MS}"

run_and_capture string_replica_eligible "${BIN_DIR}/string_scale_benchmark" \
  "${leader}" "${ID_C}" "${NAMESPACE_NAME}" "${TABLE_NAME}" \
  "${STRING_OPS}" "${STRING_THREADS}" "${STRING_VALUE_BYTES}" 0 "${STRING_REPLICA_WAIT_MS}"

run_and_capture sequence_primary "${BIN_DIR}/feature_sequence_benchmark" \
  "${leader}" "${ID_C}" "${NAMESPACE_NAME}" "${TABLE_NAME}" \
  "${SEQUENCE_KEYS}" "${SEQUENCE_ROWS_PER_KEY}" "${SEQUENCE_QUERY_OPS}" "${SEQUENCE_THREADS}" 1

run_and_capture sequence_replica_eligible "${BIN_DIR}/feature_sequence_benchmark" \
  "${leader}" "${ID_C}" "${NAMESPACE_NAME}" "${TABLE_NAME}" \
  "${SEQUENCE_KEYS}" "${SEQUENCE_ROWS_PER_KEY}" "${SEQUENCE_QUERY_OPS}" "${SEQUENCE_THREADS}" 0 "${SEQUENCE_REPLICA_WAIT_MS}"

{
  echo "result_dir=${RESULT_DIR}"
  echo "leader=${leader}"
  echo "storage_uri=${storage_uri}"
  echo "proxy_pin_primary_reads=${PROXY_PIN_PRIMARY_READS}"
  grep -E "TemporalStore Ubuntu smoke test passed|metaserver leader|metaserver[0-9]|server[0-9] pid|logs:" "${RESULT_DIR}/bootstrap.log" || true
  echo
  echo "exit_codes"
  for code_file in "${RESULT_DIR}"/*.exit_code; do
    [[ -f "${code_file}" ]] || continue
    echo "$(basename "${code_file}" .exit_code)=$(cat "${code_file}")"
  done
  echo
  echo "module_ingest"
  grep '^PASS' "${RESULT_DIR}/module_ingest.out" || true
  echo
  echo "proxy_smoke"
  grep '^PASS' "${RESULT_DIR}/proxy_smoke.out" || true
  if [[ -f "${RESULT_DIR}/proxy_ingestion_pressure.out" ]]; then
    echo
    echo "proxy_ingestion_pressure"
    grep -E '^(proxy_ingestion_pressure|ops=|threads=|value_size=|ok=|write_failed=|read_verified=|verify_timeout_ms=|verify_poll_ms=|rpc_failed=|status_failed=|read_failed=|first_status_code=|write_elapsed_ms=|elapsed_ms=|write_qps=|end_to_end_qps=)' \
      "${RESULT_DIR}/proxy_ingestion_pressure.out" || true
  fi
  echo
  echo "string_primary"
  grep '^TemporalStore,' "${RESULT_DIR}/string_primary.out" || true
  echo
  echo "string_replica_eligible"
  grep '^TemporalStore,' "${RESULT_DIR}/string_replica_eligible.out" || true
  echo
  echo "sequence_primary"
  grep '^TemporalStore,' "${RESULT_DIR}/sequence_primary.out" || true
  echo
  echo "sequence_replica_eligible"
  grep '^TemporalStore,' "${RESULT_DIR}/sequence_replica_eligible.out" || true
  echo
  echo "replication_smoke"
  cat "${RESULT_DIR}/replication_smoke.out"
  echo
  echo "storage_files=$(find "${SMOKE_DIR}/shared" -type f 2>/dev/null | wc -l)"
  echo "storage_bytes=$(du -sb "${SMOKE_DIR}/shared" 2>/dev/null | awk '{print $1}')"
  echo
  echo "process_snapshot"
  ps -o pid,pcpu,pmem,rss,vsz,cmd -p "$(tr '\n' ',' < <(cat "${SMOKE_DIR}"/metaserver*.pid "${SMOKE_DIR}"/server*.pid) | sed 's/,$//')" || true
} | tee "${RESULT_DIR}/summary.txt"

failed=0
for code_file in "${RESULT_DIR}"/*.exit_code; do
  [[ -f "${code_file}" ]] || continue
  if [[ "$(cat "${code_file}")" != "0" ]]; then
    failed=1
  fi
done

cleanup
trap - EXIT

if [[ "${failed}" != "0" ]]; then
  echo "FAIL shared-file 3-node scale"
  echo "${RESULT_DIR}"
  exit 1
fi

echo "PASS shared-file 3-node scale"
echo "${RESULT_DIR}"
