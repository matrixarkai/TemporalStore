#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

BUILD_TYPE="${BUILD_TYPE:-Release}"
RESULT_DIR="${RESULT_DIR:-/tmp/temporalstore-stale-local-data-restart-$(date +%Y%m%d_%H%M%S)}"
OPS="${OPS:-160}"
THREADS="${THREADS:-2}"
VALUE_BYTES="${VALUE_BYTES:-128}"
MS_PORT="${MS_PORT:-39100}"
SERVER_PORT="${SERVER_PORT:-14100}"
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

  start_s="$(date +%s)"
  set +e
  "$@" > "${case_dir}/stdout.log" 2> "${case_dir}/stderr.log"
  code=$?
  set -e
  end_s="$(date +%s)"

  if [[ "${code}" == "0" ]]; then
    echo "${name},pass,$((end_s - start_s)),${case_dir}" >> "${CSV}"
    log "PASS ${name} seconds=$((end_s - start_s))"
    return 0
  fi

  echo "${name},fail,$((end_s - start_s)),${case_dir}" >> "${CSV}"
  log "FAIL ${name} code=${code} seconds=$((end_s - start_s))"
  tail -100 "${case_dir}/stdout.log" | sed 's/^/[stdout] /' | tee -a "${SUMMARY}" || true
  tail -100 "${case_dir}/stderr.log" | sed 's/^/[stderr] /' | tee -a "${SUMMARY}" || true
  return "${code}"
}

log "# TemporalStore Stale Local Data Restart Gate"
log
log "- result_dir=${RESULT_DIR}"
log "- build_type=${BUILD_TYPE}"
log "- ops=${OPS}"
log "- threads=${THREADS}"
log

case_root="${RESULT_DIR}/data_raft_snapshot_restart"
failed=0
run_case \
  "data_raft_restart_existing_local_data" \
  env \
    BUILD_TYPE="${BUILD_TYPE}" \
    RESULT_DIR="${case_root}/run" \
    SMOKE_DIR="${case_root}/cluster" \
    CLUSTER_NAME="stale_local_restart" \
    MS_PORT="${MS_PORT}" \
    SERVER_PORT="${SERVER_PORT}" \
    OPS="${OPS}" \
    THREADS="${THREADS}" \
    VALUE_BYTES="${VALUE_BYTES}" \
    BENCH_TIMEOUT_S="${BENCH_TIMEOUT_S}" \
    bash "${ROOT}/tools/run_data_raft_snapshot_restore_ubuntu22.sh" || failed=1

source_summary="${case_root}/run/summary.txt"
if [[ -f "${source_summary}" ]]; then
  {
    echo
    echo "## Restart Evidence"
    grep -E 'snapshot|restart|PASS data-raft snapshot restore|FAIL data-raft snapshot restore|replication smoke|STRING' "${source_summary}" || true
    echo
    echo "## Stale Local Data Marker"
    echo "The gate intentionally reuses the same server data and raft work directories across the restart path, then verifies read/write serving after replay."
  } >> "${SUMMARY}"
fi

passed_cases="$(awk -F, 'NR > 1 && $2 == "pass" {count++} END {print count+0}' "${CSV}")"
failed_cases="$(awk -F, 'NR > 1 && $2 == "fail" {count++} END {print count+0}' "${CSV}")"
log
log "## Aggregate"
log "- passed_cases=${passed_cases}"
log "- failed_cases=${failed_cases}"

if [[ "${failed}" == "0" ]]; then
  log "PASS TemporalStore stale local data restart gate"
else
  log "FAIL TemporalStore stale local data restart gate"
fi
exit "${failed}"
