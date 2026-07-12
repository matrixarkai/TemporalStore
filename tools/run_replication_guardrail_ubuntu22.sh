#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_TYPE="${BUILD_TYPE:-Debug}"
BUILD_FLAVOR="$(printf '%s' "${BUILD_TYPE}" | tr '[:upper:]' '[:lower:]')"
BUILD_DIR="${BUILD_DIR:-${ROOT}/build-ubuntu22/${BUILD_FLAVOR}}"
OUT_DIR="${OUT_DIR:-${ROOT}/output-ubuntu22/${BUILD_FLAVOR}}"
BIN_DIR="${BIN_DIR:-${BUILD_DIR}/src/client/example}"
RESULT_DIR="${RESULT_DIR:-${ROOT}/replication-guardrail-results-$(date +%Y%m%d_%H%M%S)}"
OPS="${OPS:-2000}"
THREADS="${THREADS:-4}"
VALUE_BYTES="${VALUE_BYTES:-128}"
BUILD_TARGETS="${BUILD_TARGETS:-1}"
REPLICA_WAIT_MS="${REPLICA_WAIT_MS:-10000}"
REPLICATOR_LOOP_INTERVAL_US="${REPLICATOR_LOOP_INTERVAL_US:-5000}"
REPLICATOR_MAX_OPLOG_PER_LOOP="${REPLICATOR_MAX_OPLOG_PER_LOOP:-5000}"
REPLICATOR_MAX_INDEXLOG_PER_LOOP="${REPLICATOR_MAX_INDEXLOG_PER_LOOP:-5000}"
REPLICATOR_UPDATE_REMOTE_INTERVAL_MS="${REPLICATOR_UPDATE_REMOTE_INTERVAL_MS:-100}"
REPLICATOR_OUT_OF_SYNC_S="${REPLICATOR_OUT_OF_SYNC_S:-120}"
S3_PORT="${S3_PORT:-19100}"
MINIO_BIN="${MINIO_BIN:-/tmp/minio/bin/minio}"
MINIO_MC_BIN="${MINIO_MC_BIN:-/tmp/minio/bin/mc}"
MINIO_ROOT_USER="${MINIO_ROOT_USER:-minioadmin}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-minioadmin}"
MINIO_PORT="${MINIO_PORT:-19110}"
MINIO_CONSOLE_PORT="${MINIO_CONSOLE_PORT:-19011}"

mkdir -p "${RESULT_DIR}"

if [[ "${BUILD_TARGETS}" == "1" ]]; then
  cmake --build "${BUILD_DIR}" --parallel "${BUILD_JOBS:-8}" --target \
    bcache2-server \
    bcache2-metaserver \
    module_ingest_query_example \
    replication_smoke_example \
    string_scale_benchmark
fi

cleanup_cluster() {
  local dir="$1"
  local cluster="$2"
  if [[ -d "${dir}" ]]; then
    for pid_file in "${dir}"/server*.pid "${dir}"/metaserver*.pid; do
      [[ -f "${pid_file}" ]] || continue
      kill "$(cat "${pid_file}")" >/dev/null 2>&1 || true
    done
  fi
  pkill -f "bcache2-metaserver.*metaserver_cluster_name=${cluster}" >/dev/null 2>&1 || true
  pkill -f "bcache2-server.*cluster_name=${cluster}" >/dev/null 2>&1 || true
  sleep 1
}

cleanup_s3() {
  local pid_file="$1"
  if [[ -f "${pid_file}" ]]; then
    kill "$(cat "${pid_file}")" >/dev/null 2>&1 || true
  fi
}

start_minio() {
  local data_root="$1"
  local port="$2"
  local console_port="$3"
  local log_file="$4"
  local pid_file="$5"
  cleanup_s3 "${pid_file}"
  rm -rf "${data_root}"
  mkdir -p "${data_root}"
  if [[ ! -x "${MINIO_BIN}" ]]; then
    echo "minio binary not found at ${MINIO_BIN}" >&2
    return 1
  fi
  if [[ ! -x "${MINIO_MC_BIN}" ]]; then
    echo "minio client not found at ${MINIO_MC_BIN}" >&2
    return 1
  fi

  MINIO_ROOT_USER="${MINIO_ROOT_USER}" MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD}" \
    "${MINIO_BIN}" server --address "127.0.0.1:${port}" --console-address ":${console_port}" \
    "${data_root}" > "${log_file}" 2>&1 &
  local pid=$!
  echo "${pid}" > "${pid_file}"
  for _ in $(seq 1 120); do
    if curl -fsS "http://127.0.0.1:${port}/minio/health/live" >/dev/null 2>&1; then
      # keep the bucket creation idempotent, fail explicitly if it can't be created
      "${MINIO_MC_BIN}" alias set temporalstore "http://127.0.0.1:${port}" \
        "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}" --api S3v4 >/dev/null 2>&1 || true
      if "${MINIO_MC_BIN}" mb temporalstore/temporalstore-guardrail --ignore-existing >/dev/null 2>&1; then
        return 0
      fi
      if curl -fsS "http://127.0.0.1:${port}/temporalstore-guardrail" >/dev/null 2>&1; then
        return 0
      fi
    fi
    if ! kill -0 "${pid}" >/dev/null 2>&1; then
      cat "${log_file}" >&2 || true
      return 1
    fi
    sleep 0.5
  done
  echo "failed to start minio" >&2
  cat "${log_file}" >&2 || true
  return 1
}

start_fake_s3() {
  local root="$1"
  local port="$2"
  local log_file="$3"
  local pid_file="$4"
  cleanup_s3 "${pid_file}"
  rm -rf "${root}"
  mkdir -p "${root}"
  python3 "${ROOT}/tools/fake_s3_server.py" --host 127.0.0.1 --port "${port}" --root "${root}" --verbose \
    > "${log_file}" 2>&1 &
  echo "$!" > "${pid_file}"
  for _ in $(seq 1 60); do
    if grep -q "fake-s3 listening" "${log_file}" 2>/dev/null; then
      return 0
    fi
    if ! kill -0 "$(cat "${pid_file}")" >/dev/null 2>&1; then
      cat "${log_file}" >&2 || true
      return 1
    fi
    sleep 0.2
  done
  echo "timed out waiting for fake S3" >&2
  cat "${log_file}" >&2 || true
  return 1
}

wait_for_bootstrap() {
  local log="$1"
  local pid="$2"
  for _ in $(seq 1 180); do
    if grep -q "KEEP_RUNNING=1" "${log}" 2>/dev/null; then
      return 0
    fi
    if ! kill -0 "${pid}" >/dev/null 2>&1; then
      echo "bootstrap process exited early" >&2
      cat "${log}" >&2 || true
      return 1
    fi
    sleep 0.5
  done
  echo "timed out waiting for bootstrap" >&2
  cat "${log}" >&2 || true
  return 1
}

wait_for_module_ready() {
  local leader="$1"
  local output="$2"
  for attempt in $(seq 1 12); do
    if timeout 120s "${BIN_DIR}/module_ingest_query_example" \
      "127.0.0.1:${leader}" vdc1 ns1 table1 > "${output}" 2>&1; then
      echo "module_ready_attempt=${attempt}"
      return 0
    fi
    if grep -q "Slot not found" "${output}"; then
      sleep 2
      continue
    fi
    cat "${output}" >&2
    return 1
  done
  echo "module readiness failed after retries" >&2
  cat "${output}" >&2 || true
  return 1
}

capture_path_marker() {
  local marker="$1"
  local smoke_dir="$2"
  local marker_file="$3"

  grep -R "${marker}" "${smoke_dir}"/server*/log "${smoke_dir}"/server*/stderr \
    > "${marker_file}" 2>/dev/null || true
}

require_s3_traffic() {
  if [[ "${SKIP_TRAFFIC_CHECK:-0}" == "1" ]]; then
    return 0
  fi
  local log_file="$1"
  if ! grep -Eq '"(PUT|GET|HEAD|DELETE) ' "${log_file}"; then
    echo "s3_compat: fake S3 did not receive object-store traffic" >&2
    cat "${log_file}" >&2 || true
    return 1
  fi
}

run_positive_case() {
  local mode="$1"
  local cluster="$2"
  local smoke_dir="$3"
  local ms_port="$4"
  local raft_port="$5"
  local snapshot_port="$6"
  local server_port="$7"
  local storage_pool_uri="$8"
  local pull_from_primary="$9"
  local expected_marker="${10}"
  local require_replica_eligible="${11:-0}"
  local object_root="${12:-}"
  local bootstrap_log="${RESULT_DIR}/${mode}_bootstrap.log"

  cleanup_cluster "${smoke_dir}" "${cluster}"
  rm -rf "${smoke_dir}"
  mkdir -p "${smoke_dir}"

  echo "== ${mode} bootstrap =="
  (
    cd "${ROOT}"
    OUT_DIR="${OUT_DIR}" \
      SMOKE_DIR="${smoke_dir}" \
      CLUSTER_NAME="${cluster}" \
      NAMESPACE_NAME=ns1 \
      TABLE_NAME=table1 \
      META_COUNT=2 \
      SERVER_COUNT=2 \
      REPLICA_COUNT=2 \
      MS_PORT="${ms_port}" \
      MS_RAFT_PORT="${raft_port}" \
      MS_SNAPSHOT_PORT="${snapshot_port}" \
      MS_PORT_STEP=30 \
      SERVER_PORT="${server_port}" \
      KEEP_RUNNING=1 \
      METASERVER_LOG_LEVEL=1 \
      SERVER_LOG_LEVEL=2 \
      STORAGE_POOL_URI="${storage_pool_uri}" \
      TEMPORALSTORE_OBJECT_STORE_ROOT="${object_root}" \
      SERVER_EXTRA_FLAGS="${SERVER_EXTRA_FLAGS:-} \
        --secondary_pull_stream_from_primary=${pull_from_primary} \
        --replicator_loop_interval_us=${REPLICATOR_LOOP_INTERVAL_US} \
        --replicator_max_oplog_per_loop=${REPLICATOR_MAX_OPLOG_PER_LOOP} \
        --replicator_max_indexlog_per_loop=${REPLICATOR_MAX_INDEXLOG_PER_LOOP} \
        --replicator_update_remote_interval_ms=${REPLICATOR_UPDATE_REMOTE_INTERVAL_MS} \
        --replicator_out_of_sync_s=${REPLICATOR_OUT_OF_SYNC_S}" \
      TEMPORALSTORE_S3_ENDPOINT="${TEMPORALSTORE_S3_ENDPOINT:-}" \
      TEMPORALSTORE_S3_UNSIGNED="${TEMPORALSTORE_S3_UNSIGNED:-}" \
      AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-}" \
      AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-}" \
      AWS_REGION="${AWS_REGION:-us-east-1}" \
      bash tools/smoke_ubuntu22.sh
  ) > "${bootstrap_log}" 2>&1 &
  local boot_pid=$!
  echo "${boot_pid}" > "${RESULT_DIR}/${mode}_bootstrap.pid"
  wait_for_bootstrap "${bootstrap_log}" "${boot_pid}"

  local leader_port
  leader_port="$(sed -n 's/^metaserver leader: 127\.0\.0\.1://p' "${bootstrap_log}" | tail -1)"
  if [[ -z "${leader_port}" ]]; then
    echo "${mode}: could not parse leader port" >&2
    cat "${bootstrap_log}" >&2
    return 1
  fi

  wait_for_module_ready "${leader_port}" "${RESULT_DIR}/${mode}_module.log"
  timeout 180s "${BIN_DIR}/replication_smoke_example" \
    "127.0.0.1:${leader_port}" vdc1 ns1 table1 \
    > "${RESULT_DIR}/${mode}_replication.log" 2>&1
  timeout 180s "${BIN_DIR}/string_scale_benchmark" \
    "127.0.0.1:${leader_port}" vdc1 ns1 table1 \
    "${OPS}" "${THREADS}" "${VALUE_BYTES}" 1 1000 \
    > "${RESULT_DIR}/${mode}_string_primary.csv" 2>&1

  if [[ "${require_replica_eligible}" == "1" ]]; then
    timeout 240s "${BIN_DIR}/string_scale_benchmark" \
      "127.0.0.1:${leader_port}" vdc1 ns1 table1 \
      "${OPS}" "${THREADS}" "${VALUE_BYTES}" 0 "${REPLICA_WAIT_MS}" \
      > "${RESULT_DIR}/${mode}_string_replica_eligible.csv" 2>&1
    grep '^TemporalStore' "${RESULT_DIR}/${mode}_string_replica_eligible.csv"
  fi

  capture_path_marker "${expected_marker}" "${smoke_dir}" \
    "${RESULT_DIR}/${mode}_path_marker.log"

  grep '^TemporalStore' "${RESULT_DIR}/${mode}_string_primary.csv"
  cat "${RESULT_DIR}/${mode}_replication.log"

  cleanup_cluster "${smoke_dir}" "${cluster}"
}

trap 'cleanup_cluster /tmp/temporalstore-guardrail-primary temporal_guardrail_primary; cleanup_cluster /tmp/temporalstore-guardrail-shared temporal_guardrail_shared; cleanup_cluster /tmp/temporalstore-guardrail-local temporal_guardrail_local; cleanup_cluster /tmp/temporalstore-guardrail-s3 temporal_guardrail_s3; cleanup_cluster /tmp/temporalstore-guardrail-minio temporal_guardrail_minio; cleanup_s3 "${RESULT_DIR}/fake_s3.pid"; cleanup_s3 "${RESULT_DIR}/minio.pid"' EXIT

run_positive_case \
  primary_pull \
  temporal_guardrail_primary \
  /tmp/temporalstore-guardrail-primary \
  18900 18910 18920 18901 \
  "file:///tmp/temporalstore-guardrail-primary/storage/" \
  true \
  "Open remote primary-backed stream success" \
  1

run_positive_case \
  shared_store \
  temporal_guardrail_shared \
  /tmp/temporalstore-guardrail-shared \
  18950 18960 18970 18951 \
  "file:///tmp/temporalstore-guardrail-shared/storage/" \
  false \
  "Open readonly stream from Env" \
  1

object_root="/tmp/temporalstore-guardrail-object-store-$(date +%Y%m%d_%H%M%S)"
run_positive_case \
  matrixobjectstore_compat \
  temporal_guardrail_local \
  /tmp/temporalstore-guardrail-local \
  19050 19060 19070 19051 \
  "local://temporalstore-guardrail/pool/" \
  false \
  "Open readonly stream from Env" \
  0 \
  "${object_root}"

fake_s3_root="/tmp/temporalstore-guardrail-fake-s3-$(date +%Y%m%d_%H%M%S)"
start_fake_s3 "${fake_s3_root}" "${S3_PORT}" "${RESULT_DIR}/fake_s3.log" \
  "${RESULT_DIR}/fake_s3.pid"
TEMPORALSTORE_S3_ENDPOINT="http://127.0.0.1:${S3_PORT}" \
TEMPORALSTORE_S3_UNSIGNED=1 \
run_positive_case \
  s3_compat \
  temporal_guardrail_s3 \
  /tmp/temporalstore-guardrail-s3 \
  19150 19160 19170 19151 \
  "s3://temporalstore-guardrail/pool/" \
  false \
  "Open readonly stream from Env" \
  1
require_s3_traffic "${RESULT_DIR}/fake_s3.log"

if [[ "${ENABLE_MINIO_REPLICA_PATH:-1}" == "1" ]]; then
  minio_root="/tmp/temporalstore-guardrail-minio-$(date +%Y%m%d_%H%M%S)"
  start_minio "${minio_root}/data" "${MINIO_PORT}" "${MINIO_CONSOLE_PORT}" "${RESULT_DIR}/minio.log" "${RESULT_DIR}/minio.pid"
  TEMPORALSTORE_S3_ENDPOINT="http://127.0.0.1:${MINIO_PORT}" \
  AWS_ACCESS_KEY_ID="${MINIO_ROOT_USER}" \
  AWS_SECRET_ACCESS_KEY="${MINIO_ROOT_PASSWORD}" \
  AWS_REGION="${AWS_REGION:-us-east-1}" \
  TEMPORALSTORE_S3_UNSIGNED=0 \
  run_positive_case \
    minio_compat \
    temporal_guardrail_minio \
    /tmp/temporalstore-guardrail-minio \
    19250 19260 19270 19251 \
    "s3://temporalstore-guardrail/pool/" \
    false \
    "Open readonly stream from Env" \
    1 \
    "${minio_root}/dummy"
  require_s3_traffic "${RESULT_DIR}/minio.log"
fi

echo "replication guardrail passed"
echo "results: ${RESULT_DIR}"
