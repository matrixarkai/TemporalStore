#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${BUILD_DIR:-${ROOT}/build}"
BUILD_TYPE="${BUILD_TYPE:-Debug}"
BUILD_FLAVOR="$(printf '%s' "${BUILD_TYPE}" | tr '[:upper:]' '[:lower:]')"
OUT_DIR="${OUT_DIR:-}"
RESULT_DIR="${RESULT_DIR:-/tmp/temporalstore-replication-guardrails-$(date -u +%Y%m%dT%H%M%SZ)}"
SHARED_ITERATIONS="${SHARED_ITERATIONS:-5}"
FORCE_BUILD="${FORCE_BUILD:-0}"

if [[ -z "${OUT_DIR}" ]]; then
  if [[ -x "${ROOT}/output-ubuntu22/${BUILD_FLAVOR}/bcache2-server" ]]; then
    OUT_DIR="${ROOT}/output-ubuntu22/${BUILD_FLAVOR}"
  elif [[ -x "${ROOT}/output/bcache2-server" ]]; then
    OUT_DIR="${ROOT}/output"
  else
    OUT_DIR="${ROOT}/output-ubuntu22/${BUILD_FLAVOR}"
  fi
fi

mkdir -p "${RESULT_DIR}"
RUNTIME_BIN_DIR="${RESULT_DIR}/runtime-bin"
export LD_LIBRARY_PATH="${ROOT}/build/lib:${ROOT}/build/thirdparty/brpc/output/lib:${ROOT}/thirdparty/mtcache/third_party/install/lib:${LD_LIBRARY_PATH:-}"

log() {
  printf '[%(%Y-%m-%dT%H:%M:%SZ)T] %s\n' -1 "$*" | tee -a "${RESULT_DIR}/guardrails.log"
}

need_executable() {
  if [[ ! -x "$1" ]]; then
    log "missing executable: $1"
    exit 1
  fi
}

build_target() {
  local target="$1"
  log "building target ${target}"
  cmake --build "${BUILD_DIR}" --target "${target}" -j "${BUILD_JOBS:-1}" \
    > "${RESULT_DIR}/build-${target}.log" 2>&1 || {
      tail -n 160 "${RESULT_DIR}/build-${target}.log" >&2 || true
      exit 1
    }
}

find_binary() {
  local name="$1"
  shift
  local candidate
  for candidate in "$@"; do
    if [[ -x "${candidate}" ]]; then
      printf '%s\n' "${candidate}"
      return 0
    fi
  done
  return 1
}

build_if_missing() {
  local target="$1"
  local binary="$2"
  if [[ "${FORCE_BUILD}" == "1" || ! -x "${binary}" ]]; then
    build_target "${target}"
  else
    log "using existing ${target}: ${binary}"
  fi
}

prepare_runtime_bins() {
  local server_bin metaserver_bin
  server_bin="$(find_binary bcache2-server \
    "${OUT_DIR}/bcache2-server" \
    "${ROOT}/output/bcache2-server" \
    "${ROOT}/release-bin-20260606/output/bcache2-server")"
  metaserver_bin="$(find_binary bcache2-metaserver \
    "${OUT_DIR}/bcache2-metaserver" \
    "${ROOT}/output/bcache2-metaserver" \
    "${ROOT}/release-bin-20260606/output/bcache2-metaserver")"

  mkdir -p "${RUNTIME_BIN_DIR}"
  ln -sf "${server_bin}" "${RUNTIME_BIN_DIR}/bcache2-server"
  ln -sf "${metaserver_bin}" "${RUNTIME_BIN_DIR}/bcache2-metaserver"
  OUT_DIR="${RUNTIME_BIN_DIR}"
  log "runtime server=${server_bin}"
  log "runtime metaserver=${metaserver_bin}"
}

stop_cluster() {
  local smoke_dir="$1"
  [[ -d "${smoke_dir}" ]] || return 0
  for pid_file in "${smoke_dir}"/server*.pid "${smoke_dir}"/metaserver*.pid; do
    [[ -f "${pid_file}" ]] || continue
    kill "$(cat "${pid_file}")" >/dev/null 2>&1 || true
  done
}

wait_for_smoke_ready() {
  local launcher_pid="$1"
  local launcher_log="$2"
  local attempts="${3:-240}"
  for _ in $(seq 1 "${attempts}"); do
    if grep -q "TemporalStore Ubuntu smoke test passed" "${launcher_log}" 2>/dev/null; then
      return 0
    fi
    if ! kill -0 "${launcher_pid}" >/dev/null 2>&1; then
      log "cluster launcher exited early"
      cat "${launcher_log}" >&2 || true
      return 1
    fi
    sleep 0.5
  done
  log "cluster did not become ready"
  cat "${launcher_log}" >&2 || true
  return 1
}

run_raft_codec_guardrail() {
  log "running raft codec smoke"
  "${BUILD_DIR}/src/partition/storage/test/data_raft_replication_codec_smoke" \
    > "${RESULT_DIR}/raft-codec-smoke.log" 2>&1
  cat "${RESULT_DIR}/raft-codec-smoke.log" | tee -a "${RESULT_DIR}/guardrails.log"
}

run_raft_mode_guardrail() {
  log "checking raft_consensus guardrail flags"
  local server_help="${RESULT_DIR}/server-help.txt"
  if ! grep -q "raft_consensus" "${server_help}"; then
    log "server help does not expose raft_consensus"
    exit 1
  fi
  if ! grep -q "data_raft_enable_experimental_direct_writes" "${server_help}"; then
    log "server help does not expose fail-closed direct-write override"
    exit 1
  fi
  log "raft_consensus write path remains fail-closed until proposal/FSM/snapshot path is complete"
}

run_shared_store_guardrail() {
  local smoke_dir="${RESULT_DIR}/shared-store-cluster"
  local launcher_log="${RESULT_DIR}/shared-store-launcher.log"
  local cluster_name="sharedguard$(date -u +%H%M%S)"
  local ns="shared_guard_ns"
  local table="shared_guard_table"
  local meta="127.0.0.1:19000"

  log "starting local shared-store two-replica cluster"
  env \
    KEEP_RUNNING=1 \
    OUT_DIR="${OUT_DIR}" \
    SMOKE_DIR="${smoke_dir}" \
    CLUSTER_NAME="${cluster_name}" \
    NAMESPACE_NAME="${ns}" \
    TABLE_NAME="${table}" \
    SERVER_COUNT=2 \
    REPLICA_COUNT=2 \
    META_COUNT=1 \
    MS_PORT=19000 \
    MS_RAFT_PORT=19010 \
    MS_SNAPSHOT_PORT=19020 \
    SERVER_PORT=19001 \
    STORAGE_POOL_URI="file://${smoke_dir}/shared-store/" \
    TEMPORALSTORE_REPLICATOR_OUT_OF_SYNC_S=10 \
    TEMPORALSTORE_REPLICATOR_LOOP_INTERVAL_US=1000 \
    TEMPORALSTORE_STORAGE_OPLOG_DELAY_DUMP_LENGTH=0 \
    SERVER_EXTRA_FLAGS="--data_replication_mode=shared_store --secondary_pull_stream_from_primary=false" \
    bash "${ROOT}/tools/smoke_ubuntu22.sh" \
    > "${launcher_log}" 2>&1 &
  local launcher_pid=$!

  trap 'stop_cluster "'"${smoke_dir}"'"; kill "'"${launcher_pid}"'" >/dev/null 2>&1 || true' RETURN
  wait_for_smoke_ready "${launcher_pid}" "${launcher_log}"

  log "running ${SHARED_ITERATIONS} secondary visibility checks"
  for i in $(seq 1 "${SHARED_ITERATIONS}"); do
    "${BUILD_DIR}/src/client/example/replication_smoke_example" "${meta}" "vdc1" "${ns}" "${table}" \
      > "${RESULT_DIR}/shared-store-replication-${i}.log" 2>&1 || {
        cat "${RESULT_DIR}/shared-store-replication-${i}.log" >&2 || true
        return 1
      }
    cat "${RESULT_DIR}/shared-store-replication-${i}.log" | tee -a "${RESULT_DIR}/guardrails.log"
  done

  if grep -R "Partition out of sync\|DeadlineExceeded: replicator out of sync" \
      "${smoke_dir}"/server*/stderr "${smoke_dir}"/server*/stdout \
      > "${RESULT_DIR}/shared-store-out-of-sync.log" 2>/dev/null; then
    log "shared-store guardrail saw out-of-sync logs"
    cat "${RESULT_DIR}/shared-store-out-of-sync.log" >&2
    return 1
  fi

  log "shared-store guardrail passed"
}

log "result_dir=${RESULT_DIR}"
log "root=${ROOT}"
log "build_dir=${BUILD_DIR}"
log "out_dir=${OUT_DIR}"

build_if_missing bcache2-server "${ROOT}/output/bcache2-server"
if ! find_binary bcache2-metaserver \
    "${OUT_DIR}/bcache2-metaserver" \
    "${ROOT}/output/bcache2-metaserver" \
    "${ROOT}/release-bin-20260606/output/bcache2-metaserver" >/dev/null; then
  build_target bcache2-metaserver
fi
build_if_missing replication_smoke_example \
  "${BUILD_DIR}/src/client/example/replication_smoke_example"
build_if_missing data_raft_replication_codec_smoke \
  "${BUILD_DIR}/src/partition/storage/test/data_raft_replication_codec_smoke"

prepare_runtime_bins

need_executable "${OUT_DIR}/bcache2-server"
need_executable "${OUT_DIR}/bcache2-metaserver"
need_executable "${BUILD_DIR}/src/client/example/replication_smoke_example"
need_executable "${BUILD_DIR}/src/partition/storage/test/data_raft_replication_codec_smoke"

server_help="${RESULT_DIR}/server-help.txt"
"${OUT_DIR}/bcache2-server" --help > "${server_help}" 2>&1 || true
if ! grep -q "data_replication_mode" "${server_help}"; then
  log "server help does not expose data_replication_mode"
  exit 1
fi

run_raft_codec_guardrail
run_raft_mode_guardrail
run_shared_store_guardrail

log "PASS replication guardrails"
