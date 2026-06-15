#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RESULT_DIR="${RESULT_DIR:-/tmp/temporalstore-soak-profile-$(date +%Y%m%d_%H%M%S)}"
SOAK_MINUTES="${SOAK_MINUTES:-30}"
SOAK_MIN_ITERATIONS="${SOAK_MIN_ITERATIONS:-1}"
SOAK_QUEUE_LEVEL="${SOAK_QUEUE_LEVEL:-quick}"
RUN_QUEUE_EXECUTION="${RUN_QUEUE_EXECUTION:-1}"

mkdir -p "${RESULT_DIR}"
SUMMARY="${RESULT_DIR}/summary.txt"
CSV="${RESULT_DIR}/iterations.csv"
echo "iteration,status,seconds,result_dir" > "${CSV}"

log() {
  printf '%s\n' "$*" | tee -a "${SUMMARY}"
}

log "TemporalStore soak profile"
log "result_dir=${RESULT_DIR}"
log "soak_minutes=${SOAK_MINUTES}"
log "soak_queue_level=${SOAK_QUEUE_LEVEL}"
log "run_queue_execution=${RUN_QUEUE_EXECUTION}"

deadline="$(( $(date +%s) + SOAK_MINUTES * 60 ))"
iteration=0
failed=0

while :; do
  now="$(date +%s)"
  if (( iteration >= SOAK_MIN_ITERATIONS && now >= deadline )); then
    break
  fi
  iteration=$((iteration + 1))
  iter_dir="${RESULT_DIR}/iteration_${iteration}"
  start_s="$(date +%s)"
  set +e
  env \
    QUEUE_LEVEL="${SOAK_QUEUE_LEVEL}" \
    RUN_EXECUTION="${RUN_QUEUE_EXECUTION}" \
    RESULT_DIR="${iter_dir}" \
    bash "${ROOT}/tools/run_production_gap_queue_ubuntu22.sh" \
      > "${iter_dir}.stdout" 2> "${iter_dir}.stderr"
  code=$?
  set -e
  end_s="$(date +%s)"
  if [[ "${code}" == "0" ]]; then
    echo "${iteration},pass,$((end_s - start_s)),${iter_dir}" >> "${CSV}"
    log "PASS iteration=${iteration} seconds=$((end_s - start_s))"
  else
    echo "${iteration},fail,$((end_s - start_s)),${iter_dir}" >> "${CSV}"
    log "FAIL iteration=${iteration} code=${code} seconds=$((end_s - start_s))"
    tail -80 "${iter_dir}.stdout" | sed 's/^/[stdout] /' | tee -a "${SUMMARY}" || true
    tail -80 "${iter_dir}.stderr" | sed 's/^/[stderr] /' | tee -a "${SUMMARY}" || true
    failed=1
    break
  fi
done

passed_iterations="$(awk -F, 'NR > 1 && $2 == "pass" {count++} END {print count+0}' "${CSV}")"
failed_iterations="$(awk -F, 'NR > 1 && $2 != "pass" {count++} END {print count+0}' "${CSV}")"
log "passed_iterations=${passed_iterations}"
log "failed_iterations=${failed_iterations}"

if [[ "${failed}" == "0" ]]; then
  log "PASS TemporalStore soak profile"
else
  log "FAIL TemporalStore soak profile"
fi
exit "${failed}"
