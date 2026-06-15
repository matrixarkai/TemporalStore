#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

BUILD_TYPE="${BUILD_TYPE:-Release}"
RESULT_DIR="${RESULT_DIR:-/tmp/temporalstore-stale-local-data-restart-$(date +%Y%m%d_%H%M%S)}"
TEXTFILE_DIR="${TEXTFILE_DIR:-${RESULT_DIR}/metrics}"
METRICS_FILE="${METRICS_FILE:-${TEXTFILE_DIR}/temporalstore-stale-local-data.prom}"
OPS="${OPS:-160}"
THREADS="${THREADS:-2}"
VALUE_BYTES="${VALUE_BYTES:-128}"
MS_PORT="${MS_PORT:-39100}"
SERVER_PORT="${SERVER_PORT:-14100}"
BENCH_TIMEOUT_S="${BENCH_TIMEOUT_S:-180}"

mkdir -p "${RESULT_DIR}" "${TEXTFILE_DIR}"
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

raft_dir="${case_root}/cluster/data-raft"
applied_index_file_count="$(find "${raft_dir}/applied" -type f 2>/dev/null | wc -l)"
wal_file_count="$(find "${raft_dir}/wal" -type f 2>/dev/null | wc -l)"
snapshot_file_count="$(find "${raft_dir}/snapshot" -type f 2>/dev/null | wc -l)"
missing_applied_guard_present=0
invalid_applied_guard_present=0
if grep -q 'missing data raft applied-index checkpoint for existing raft WAL' \
    "${ROOT}/src/partition/partition.cc"; then
  missing_applied_guard_present=1
fi
if grep -q 'invalid data raft applied index file' "${ROOT}/src/partition/partition.cc"; then
  invalid_applied_guard_present=1
fi

{
  echo
  echo "## Persisted Local State Evidence"
  echo "- applied_index_file_count=${applied_index_file_count}"
  echo "- wal_file_count=${wal_file_count}"
  echo "- snapshot_file_count=${snapshot_file_count}"
  echo "- missing_applied_guard_present=${missing_applied_guard_present}"
  echo "- invalid_applied_guard_present=${invalid_applied_guard_present}"
} >> "${SUMMARY}"

passed_cases="$(awk -F, 'NR > 1 && $2 == "pass" {count++} END {print count+0}' "${CSV}")"
failed_cases="$(awk -F, 'NR > 1 && $2 == "fail" {count++} END {print count+0}' "${CSV}")"
log
log "## Aggregate"
log "- passed_cases=${passed_cases}"
log "- failed_cases=${failed_cases}"

gate_pass=0
if [[ "${failed}" == "0" &&
      "${applied_index_file_count}" -gt 0 &&
      "${wal_file_count}" -gt 0 &&
      "${missing_applied_guard_present}" == "1" &&
      "${invalid_applied_guard_present}" == "1" ]]; then
  gate_pass=1
fi

cat > "${METRICS_FILE}" <<EOF
# HELP temporalstore_stale_local_data_restart_pass Whether stale local data restart gate passed.
# TYPE temporalstore_stale_local_data_restart_pass gauge
temporalstore_stale_local_data_restart_pass ${gate_pass}
# HELP temporalstore_stale_local_data_restart_cases Cases by result.
# TYPE temporalstore_stale_local_data_restart_cases gauge
temporalstore_stale_local_data_restart_cases{status="pass"} ${passed_cases}
temporalstore_stale_local_data_restart_cases{status="fail"} ${failed_cases}
# HELP temporalstore_stale_local_data_files Persisted local raft/storage evidence after restart.
# TYPE temporalstore_stale_local_data_files gauge
temporalstore_stale_local_data_files{kind="applied_index"} ${applied_index_file_count}
temporalstore_stale_local_data_files{kind="wal"} ${wal_file_count}
temporalstore_stale_local_data_files{kind="snapshot"} ${snapshot_file_count}
# HELP temporalstore_stale_local_data_guard_present Static stale/corrupt local data guard evidence.
# TYPE temporalstore_stale_local_data_guard_present gauge
temporalstore_stale_local_data_guard_present{guard="missing_applied_index_with_wal"} ${missing_applied_guard_present}
temporalstore_stale_local_data_guard_present{guard="invalid_applied_index"} ${invalid_applied_guard_present}
EOF
cat "${METRICS_FILE}" >> "${SUMMARY}"

if [[ "${gate_pass}" == "1" ]]; then
  log "PASS TemporalStore stale local data restart gate"
else
  log "FAIL TemporalStore stale local data restart gate"
  failed=1
fi
exit "${failed}"
