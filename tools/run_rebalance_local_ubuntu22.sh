#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

BUILD_TYPE="${BUILD_TYPE:-Release}"
RESULT_DIR="${RESULT_DIR:-/tmp/temporalstore-rebalance-local-$(date +%Y%m%d_%H%M%S)}"
RUN_DATA_RAFT_REBALANCE="${RUN_DATA_RAFT_REBALANCE:-1}"
RUN_SHARED_STORE_REBALANCE="${RUN_SHARED_STORE_REBALANCE:-0}"
BASE_MS_PORT="${BASE_MS_PORT:-23100}"
BASE_SERVER_PORT="${BASE_SERVER_PORT:-12100}"
OPS="${OPS:-300}"
THREADS="${THREADS:-2}"
VALUE_BYTES="${VALUE_BYTES:-128}"
BENCH_TIMEOUT_S="${BENCH_TIMEOUT_S:-180}"

mkdir -p "${RESULT_DIR}"
SUMMARY="${RESULT_DIR}/summary.md"
CSV="${RESULT_DIR}/cases.csv"
echo "case,status,seconds,result_dir" > "${CSV}"

log() {
  printf '%s\n' "$*" | tee -a "${SUMMARY}"
}

run_case() {
  local name="$1"
  shift
  local case_dir="${RESULT_DIR}/${name}"
  local start_s
  local end_s
  local code
  mkdir -p "${case_dir}"

  log "## ${name}"
  start_s="$(date +%s)"
  set +e
  "$@" > "${case_dir}/stdout.log" 2> "${case_dir}/stderr.log"
  code=$?
  set -e
  end_s="$(date +%s)"

  if [[ "${code}" == "0" ]]; then
    echo "${name},pass,$((end_s - start_s)),${case_dir}" >> "${CSV}"
    log "- status: pass"
    log "- seconds: $((end_s - start_s))"
    log "- result_dir: ${case_dir}"
    return 0
  fi

  echo "${name},fail,$((end_s - start_s)),${case_dir}" >> "${CSV}"
  log "- status: fail"
  log "- code: ${code}"
  log "- seconds: $((end_s - start_s))"
  log "- result_dir: ${case_dir}"
  log "stdout tail:"
  tail -80 "${case_dir}/stdout.log" | sed 's/^/[stdout] /' | tee -a "${SUMMARY}" || true
  log "stderr tail:"
  tail -80 "${case_dir}/stderr.log" | sed 's/^/[stderr] /' | tee -a "${SUMMARY}" || true
  return "${code}"
}

write_case_overview() {
  local name="$1"
  local case_run_dir="$2"
  local source_summary="${case_run_dir}/run/summary.txt"
  [[ -f "${source_summary}" ]] || return 0
  {
    echo
    echo "### ${name} redistribution evidence"
    grep -E 'scale_up_server3_partition_id|scale_down_server3_active_partitions|PASS data-raft scale up/down|FAIL data-raft scale up/down' "${source_summary}" || true
    if [[ -f "${case_run_dir}/run/topology_baseline.csv" ]]; then
      echo
      echo "baseline topology:"
      cat "${case_run_dir}/run/topology_baseline.csv"
    fi
    if [[ -f "${case_run_dir}/run/topology_after_scale_up.csv" ]]; then
      echo
      echo "after scale up topology:"
      cat "${case_run_dir}/run/topology_after_scale_up.csv"
    fi
    if [[ -f "${case_run_dir}/run/topology_after_scale_down.csv" ]]; then
      echo
      echo "after scale down topology:"
      cat "${case_run_dir}/run/topology_after_scale_down.csv"
    fi
  } >> "${SUMMARY}"
}

log "# TemporalStore Local Rebalance Harness"
log
log "- result_dir=${RESULT_DIR}"
log "- build_type=${BUILD_TYPE}"
log "- run_data_raft_rebalance=${RUN_DATA_RAFT_REBALANCE}"
log "- run_shared_store_rebalance=${RUN_SHARED_STORE_REBALANCE}"
log

failed=0

if [[ "${RUN_DATA_RAFT_REBALANCE}" == "1" ]]; then
  data_case_root="${RESULT_DIR}/rebalance_add_node_data_raft"
  run_case \
    "rebalance_add_node_data_raft" \
    env \
      BUILD_TYPE="${BUILD_TYPE}" \
      RESULT_DIR="${data_case_root}/run" \
      SMOKE_DIR="${data_case_root}/cluster" \
      CLUSTER_NAME="rebalance_data_raft" \
      MS_PORT="${BASE_MS_PORT}" \
      SERVER_PORT="${BASE_SERVER_PORT}" \
      OPS="${OPS}" \
      THREADS="${THREADS}" \
      VALUE_BYTES="${VALUE_BYTES}" \
      BENCH_TIMEOUT_S="${BENCH_TIMEOUT_S}" \
      bash "${ROOT}/tools/run_data_raft_scale_up_down_ubuntu22.sh" || failed=1
  write_case_overview "rebalance_add_node_data_raft" "${data_case_root}"
fi

if [[ "${RUN_SHARED_STORE_REBALANCE}" == "1" ]]; then
  run_case \
    "rebalance_add_node_shared_store" \
    env \
      BUILD_TYPE="${BUILD_TYPE}" \
      RESULT_DIR="${RESULT_DIR}/rebalance_add_node_shared_store/run" \
      SMOKE_DIR="${RESULT_DIR}/rebalance_add_node_shared_store/cluster" \
      STRING_OPS="${OPS}" \
      STRING_THREADS="${THREADS}" \
      RUN_PROXY_INGESTION_PRESSURE=0 \
      bash "${ROOT}/tools/run_shared_file_3node_scale_ubuntu22.sh" || failed=1
else
  log "## rebalance_add_node_shared_store"
  log "- status: skipped"
  log "- reason: set RUN_SHARED_STORE_REBALANCE=1 to run the shared-store scale profile"
fi

passed_cases="$(awk -F, 'NR > 1 && $2 == "pass" {count++} END {print count+0}' "${CSV}")"
failed_cases="$(awk -F, 'NR > 1 && $2 == "fail" {count++} END {print count+0}' "${CSV}")"
log
log "## Aggregate"
log "- passed_cases=${passed_cases}"
log "- failed_cases=${failed_cases}"

if [[ "${failed}" == "0" ]]; then
  log "PASS TemporalStore local rebalance harness"
else
  log "FAIL TemporalStore local rebalance harness"
fi
exit "${failed}"
