#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RESULT_DIR="${RESULT_DIR:-/tmp/temporalstore-ci-guard-$(date +%Y%m%d_%H%M%S)}"
ITERATIONS="${ITERATIONS:-5}"
RUN_FULL_GATE="${RUN_FULL_GATE:-0}"

mkdir -p "${RESULT_DIR}"
SUMMARY="${RESULT_DIR}/summary.txt"
CSV="${RESULT_DIR}/cases.csv"
echo "iteration,case,status,seconds" > "${CSV}"

log() {
  printf '%s\n' "$*" | tee -a "${SUMMARY}"
}

run_case() {
  local iteration="$1"
  local name="$2"
  shift 2
  local start_s
  local end_s
  local code
  local case_dir="${RESULT_DIR}/${iteration}_${name}"
  mkdir -p "${case_dir}"

  start_s="$(date +%s)"
  set +e
  "$@" > "${case_dir}/stdout.log" 2> "${case_dir}/stderr.log"
  code=$?
  set -e
  end_s="$(date +%s)"

  if [[ "${code}" == "0" ]]; then
    echo "${iteration},${name},pass,$((end_s - start_s))" >> "${CSV}"
    log "PASS iteration=${iteration} case=${name} seconds=$((end_s - start_s))"
    return 0
  fi

  echo "${iteration},${name},fail,$((end_s - start_s))" >> "${CSV}"
  log "FAIL iteration=${iteration} case=${name} code=${code} seconds=$((end_s - start_s))"
  tail -80 "${case_dir}/stdout.log" | sed 's/^/[stdout] /' | tee -a "${SUMMARY}" || true
  tail -80 "${case_dir}/stderr.log" | sed 's/^/[stderr] /' | tee -a "${SUMMARY}" || true
  return "${code}"
}

syntax_check() {
  bash -n \
    "${ROOT}/tools/run_ci_guard_ubuntu22.sh" \
    "${ROOT}/tools/run_prometheus_local_ubuntu22.sh" \
    "${ROOT}/tools/run_production_readiness_local_ubuntu22.sh" \
    "${ROOT}/tools/run_production_gap_queue_ubuntu22.sh" \
    "${ROOT}/tools/run_raft_production_gate_ubuntu22.sh" \
    "${ROOT}/tools/run_raft_stress_suite_ubuntu22.sh" \
    "${ROOT}/tools/run_data_raft_scale_up_down_ubuntu22.sh"
  bash -n \
    "${ROOT}/tools/run_data_raft_5node_scale_ubuntu22.sh" \
    "${ROOT}/tools/run_rebalance_local_ubuntu22.sh" \
    "${ROOT}/tools/run_stale_local_data_restart_gate_ubuntu22.sh" \
    "${ROOT}/tools/run_multitenant_noisy_neighbor_ubuntu22.sh" \
    "${ROOT}/tools/run_metaserver_raft_snapshot_restore_ubuntu22.sh" \
    "${ROOT}/tools/run_live_raft_metrics_ubuntu22.sh" \
    "${ROOT}/tools/run_cache_fallback_metrics_ubuntu22.sh" \
    "${ROOT}/tools/run_fault_injection_gate_ubuntu22.sh" \
    "${ROOT}/tools/run_remote_auth_gate_ubuntu22.sh" \
    "${ROOT}/tools/run_soak_profile_ubuntu22.sh"
  PYTHONDONTWRITEBYTECODE=1 python3 -m py_compile \
    "${ROOT}/tools/summarize_raft_gate_results.py" \
    "${ROOT}/tools/test_summarize_raft_gate_results.py" \
    "${ROOT}/tools/temporalstore-prometheus/vars-exporter/vars_to_prom.py" \
    "${ROOT}/tools/temporalstore-prometheus/vars-exporter/test_vars_to_prom.py"
}

prometheus_unit_tests() {
  (
    cd "${ROOT}/tools/temporalstore-prometheus/vars-exporter"
    PYTHONDONTWRITEBYTECODE=1 python3 -m unittest -v test_vars_to_prom.py
  )
}

raft_summary_tests() {
  (
    cd "${ROOT}/tools"
    PYTHONDONTWRITEBYTECODE=1 python3 -m unittest -v test_summarize_raft_gate_results.py
  )
}

full_gate_smoke() {
  env \
    ITERATIONS=1 \
    RUN_BUILD="${FULL_GATE_RUN_BUILD:-0}" \
    RUN_TEST_BUILD="${FULL_GATE_RUN_TEST_BUILD:-0}" \
    RUN_UNIT="${FULL_GATE_RUN_UNIT:-0}" \
    RUN_API="${FULL_GATE_RUN_API:-0}" \
    RUN_PROMETHEUS="${FULL_GATE_RUN_PROMETHEUS:-1}" \
    RUN_INGESTION="${FULL_GATE_RUN_INGESTION:-1}" \
    RUN_REDIS="${FULL_GATE_RUN_REDIS:-0}" \
    RUN_RAFT="${FULL_GATE_RUN_RAFT:-0}" \
    RESULT_DIR="${RESULT_DIR}/full_gate_$(date +%s%N)" \
    "${ROOT}/tools/run_production_readiness_local_ubuntu22.sh"
}

log "TemporalStore CI guard"
log "result_dir=${RESULT_DIR}"
log "iterations=${ITERATIONS}"
log "run_full_gate=${RUN_FULL_GATE}"

overall_failed=0
for iteration in $(seq 1 "${ITERATIONS}"); do
  run_case "${iteration}" syntax syntax_check || overall_failed=1
  run_case "${iteration}" prometheus_unit prometheus_unit_tests || overall_failed=1
  run_case "${iteration}" raft_summary raft_summary_tests || overall_failed=1
  if [[ "${RUN_FULL_GATE}" == "1" ]]; then
    run_case "${iteration}" full_gate full_gate_smoke || overall_failed=1
  fi
done

python3 - "${CSV}" "${SUMMARY}" <<'PY'
import csv
import sys

csv_path, summary_path = sys.argv[1], sys.argv[2]
rows = list(csv.DictReader(open(csv_path, encoding="utf-8")))
passed = sum(1 for row in rows if row["status"] == "pass")
failed = sum(1 for row in rows if row["status"] != "pass")
with open(summary_path, "a", encoding="utf-8") as out:
    out.write(f"passed_cases={passed}\n")
    out.write(f"failed_cases={failed}\n")
print(f"passed_cases={passed}")
print(f"failed_cases={failed}")
PY

log "summary=${SUMMARY}"
log "cases=${CSV}"
if [[ "${overall_failed}" == "0" ]]; then
  log "PASS TemporalStore CI guard"
else
  log "FAIL TemporalStore CI guard"
fi
exit "${overall_failed}"
