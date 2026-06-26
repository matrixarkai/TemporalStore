#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

BUILD_TYPE="${BUILD_TYPE:-Release}"
RESULT_DIR="${RESULT_DIR:-/tmp/temporalstore-raft-stress-suite-$(date +%Y%m%d_%H%M%S)}"
ITERATIONS="${ITERATIONS:-2}"
BASE_MS_PORT="${BASE_MS_PORT:-21000}"
BASE_SERVER_PORT="${BASE_SERVER_PORT:-11000}"
PORT_STRIDE="${PORT_STRIDE:-500}"
THREAD_LIST="${THREAD_LIST:-2 4}"
OPS="${OPS:-6000}"
MEMBERSHIP_OPS="${MEMBERSHIP_OPS:-1200}"
VALUE_BYTES="${VALUE_BYTES:-128}"
PIN_PRIMARY_READS="${PIN_PRIMARY_READS:-0}"
REPLICA_WAIT_MS="${REPLICA_WAIT_MS:-1000}"
RUN_2NODE_SCALE="${RUN_2NODE_SCALE:-1}"
RUN_MIXED_RW="${RUN_MIXED_RW:-1}"
RUN_DATA_MEMBERSHIP="${RUN_DATA_MEMBERSHIP:-1}"
RUN_META_MEMBERSHIP="${RUN_META_MEMBERSHIP:-1}"
RUN_META_FAILOVER="${RUN_META_FAILOVER:-1}"
RUN_DATA_SNAPSHOT="${RUN_DATA_SNAPSHOT:-1}"
RUN_FAILOVER="${RUN_FAILOVER:-1}"
FAILOVER_ITERATIONS="${FAILOVER_ITERATIONS:-1}"
MIXED_PROBE_OPS="${MIXED_PROBE_OPS:-300}"
MIXED_PROBE_THREADS="${MIXED_PROBE_THREADS:-4}"
MIXED_BACKGROUND_WRITER_THREADS="${MIXED_BACKGROUND_WRITER_THREADS:-2}"
MIXED_BACKGROUND_READER_THREADS="${MIXED_BACKGROUND_READER_THREADS:-4}"
MIXED_BACKGROUND_WRITER_PAUSE_US="${MIXED_BACKGROUND_WRITER_PAUSE_US:-0}"
MIXED_BACKGROUND_READER_PAUSE_US="${MIXED_BACKGROUND_READER_PAUSE_US:-0}"
MIXED_DATA_RAFT_BOUNDED_STALE_MAX_INDEX_LAG="${MIXED_DATA_RAFT_BOUNDED_STALE_MAX_INDEX_LAG:-16}"
BENCH_TIMEOUT_S="${BENCH_TIMEOUT_S:-300}"
RETRY_PORT_CONFLICT="${RETRY_PORT_CONFLICT:-1}"
PORT_CONFLICT_RETRIES="${PORT_CONFLICT_RETRIES:-3}"
PROM_TEXTFILE_DIR="${PROM_TEXTFILE_DIR:-${ROOT}/tools/temporalstore-prometheus/vars-exporter/metrics}"
PROM_TEXTFILE_NAME="${PROM_TEXTFILE_NAME:-temporalstore-raft-gate.prom}"
RAFT_PRODUCTION_ASSERTIONS="${RAFT_PRODUCTION_ASSERTIONS:-0}"
RAFT_MAX_METASERVER_FAILOVER_MS="${RAFT_MAX_METASERVER_FAILOVER_MS:-10000}"
RAFT_MAX_DATA_FAILOVER_WRITE_READ_MS="${RAFT_MAX_DATA_FAILOVER_WRITE_READ_MS:-10000}"
RAFT_MAX_SECONDARY_VISIBILITY_P99_US="${RAFT_MAX_SECONDARY_VISIBILITY_P99_US:-50000}"
RAFT_MAX_POST_FAILOVER_APPLY_LAG="${RAFT_MAX_POST_FAILOVER_APPLY_LAG:-128}"
RAFT_MAX_2NODE_SCALE_P99_US="${RAFT_MAX_2NODE_SCALE_P99_US:-150000}"
ALLOW_EPHEMERAL_PORT_RANGE="${ALLOW_EPHEMERAL_PORT_RANGE:-0}"

read_ephemeral_range() {
  if [[ -r /proc/sys/net/ipv4/ip_local_port_range ]]; then
    awk '{print $1, $2}' /proc/sys/net/ipv4/ip_local_port_range
  else
    echo "32768 60999"
  fi
}

assert_port_outside_ephemeral_range() {
  local label="$1"
  local port="$2"
  local eph_start="$3"
  local eph_end="$4"
  if [[ "${ALLOW_EPHEMERAL_PORT_RANGE}" == "1" ]]; then
    return 0
  fi
  if (( port >= eph_start && port <= eph_end )); then
    echo "${label} port ${port} overlaps Linux ephemeral range ${eph_start}-${eph_end}; " \
      "choose lower BASE_MS_PORT/BASE_SERVER_PORT or set ALLOW_EPHEMERAL_PORT_RANGE=1" >&2
    return 2
  fi
}

validate_planned_port_ranges() {
  local eph_start
  local eph_end
  read -r eph_start eph_end < <(read_ephemeral_range)
  echo "ephemeral_port_range=${eph_start}-${eph_end}" | tee -a "${SUMMARY}"

  local max_iter=$((ITERATIONS > FAILOVER_ITERATIONS ? ITERATIONS : FAILOVER_ITERATIONS))
  local max_meta_base=$((BASE_MS_PORT + (max_iter + 3) * PORT_STRIDE + 500))
  local max_server_base=$((BASE_SERVER_PORT + (max_iter + 3) * PORT_STRIDE + 500))

  assert_port_outside_ephemeral_range "BASE_MS_PORT" "${BASE_MS_PORT}" "${eph_start}" "${eph_end}"
  assert_port_outside_ephemeral_range "max planned metaserver" "$((max_meta_base + 120))" "${eph_start}" "${eph_end}"
  assert_port_outside_ephemeral_range "BASE_SERVER_PORT" "${BASE_SERVER_PORT}" "${eph_start}" "${eph_end}"
  assert_port_outside_ephemeral_range "max planned data server" "$((max_server_base + 540 + 2000))" "${eph_start}" "${eph_end}"
}

mkdir -p "${RESULT_DIR}"

SUMMARY="${RESULT_DIR}/summary.txt"
CSV="${RESULT_DIR}/cases.csv"
echo "result_dir=${RESULT_DIR}" | tee "${SUMMARY}"
echo "iterations=${ITERATIONS}" | tee -a "${SUMMARY}"
echo "thread_list=${THREAD_LIST}" | tee -a "${SUMMARY}"
echo "ops=${OPS}" | tee -a "${SUMMARY}"
echo "production_assertions=${RAFT_PRODUCTION_ASSERTIONS}" | tee -a "${SUMMARY}"
echo "case,iteration,status,result_dir,seconds" > "${CSV}"
validate_planned_port_ranges

cleanup_case_dir() {
  local case_dir="$1"
  local pids=()
  local pid

  while IFS= read -r pid_file; do
    [[ -f "${pid_file}" ]] || continue
    pid="$(cat "${pid_file}" 2>/dev/null || true)"
    [[ -n "${pid}" ]] || continue
    pids+=("${pid}")
    kill "${pid}" >/dev/null 2>&1 || true
  done < <(find "${case_dir}" -name '*.pid' -type f 2>/dev/null)

  for _ in $(seq 1 20); do
    local alive=0
    for pid in "${pids[@]}"; do
      if kill -0 "${pid}" >/dev/null 2>&1; then
        alive=1
        break
      fi
    done
    [[ "${alive}" == "0" ]] && return 0
    sleep 0.25
  done

  for pid in "${pids[@]}"; do
    kill -9 "${pid}" >/dev/null 2>&1 || true
  done
}

run_case_once() {
  local case_dir="$1"
  shift
  set +e
  "$@" > "${case_dir}/stdout.log" 2> "${case_dir}/stderr.log"
  local code=$?
  set -e
  return "${code}"
}

run_case() {
  local name="$1"
  local iteration="$2"
  shift 2
  local case_dir="${RESULT_DIR}/${name}_iter${iteration}"
  local start_s
  local end_s
  local code
  mkdir -p "${case_dir}"

  echo "== ${name} iteration ${iteration} ==" | tee -a "${SUMMARY}"
  start_s="$(date +%s)"
  local attempt=1
  while :; do
    run_case_once "${case_dir}" "$@"
    code=$?
    if [[ "${code}" == "0" || "${RETRY_PORT_CONFLICT}" != "1" ]] ||
        ! grep -qE 'port [0-9]+ is not free' "${case_dir}/stderr.log"; then
      break
    fi
    if (( attempt >= PORT_CONFLICT_RETRIES )); then
      break
    fi
    echo "${name} iteration ${iteration}: port conflict, retry ${attempt}/${PORT_CONFLICT_RETRIES} after cleanup" \
      | tee -a "${SUMMARY}"
    cleanup_case_dir "${case_dir}"
    sleep $((5 * attempt))
    attempt=$((attempt + 1))
  done
  end_s="$(date +%s)"
  cleanup_case_dir "${case_dir}"

  if [[ "${code}" == "0" ]]; then
    echo "${name} iteration ${iteration}: PASS $((end_s - start_s))s" | tee -a "${SUMMARY}"
    echo "${name},${iteration},pass,${case_dir},$((end_s - start_s))" >> "${CSV}"
  else
    echo "${name} iteration ${iteration}: FAIL code=${code} $((end_s - start_s))s" | tee -a "${SUMMARY}"
    echo "${name},${iteration},fail,${case_dir},$((end_s - start_s))" >> "${CSV}"
    tail -120 "${case_dir}/stdout.log" | sed 's/^/[stdout] /' | tee -a "${SUMMARY}" || true
    tail -120 "${case_dir}/stderr.log" | sed 's/^/[stderr] /' | tee -a "${SUMMARY}" || true
    return "${code}"
  fi
}

case_failed=0
for iteration in $(seq 1 "${ITERATIONS}"); do
  port_base=$((BASE_MS_PORT + (iteration - 1) * PORT_STRIDE))
  server_base=$((BASE_SERVER_PORT + (iteration - 1) * PORT_STRIDE))

  if [[ "${RUN_META_MEMBERSHIP}" == "1" ]]; then
    run_case "metaserver_membership" "${iteration}" \
      env \
        BUILD_TYPE="${BUILD_TYPE}" \
        RESULT_DIR="${RESULT_DIR}/metaserver_membership_iter${iteration}/run" \
        CLUSTER_NAME="stress_meta_membership_${iteration}" \
        MS_PORT="${port_base}" \
        MS_RAFT_PORT="$((port_base + 10))" \
        MS_SNAPSHOT_PORT="$((port_base + 20))" \
        bash "${ROOT}/tools/run_metaserver_raft_membership_ubuntu22.sh" || case_failed=1
  fi

  if [[ "${RUN_META_FAILOVER}" == "1" ]]; then
    run_case "metaserver_failover" "${iteration}" \
      env \
        BUILD_TYPE="${BUILD_TYPE}" \
        RESULT_DIR="${RESULT_DIR}/metaserver_failover_iter${iteration}/run" \
        CLUSTER_NAME="stress_meta_failover_${iteration}" \
        MS_PORT="$((port_base + 50))" \
        MS_RAFT_PORT="$((port_base + 60))" \
        MS_SNAPSHOT_PORT="$((port_base + 70))" \
        bash "${ROOT}/tools/run_metaserver_raft_failover_ubuntu22.sh" || case_failed=1
  fi

  if [[ "${RUN_DATA_MEMBERSHIP}" == "1" ]]; then
    run_case "data_membership" "${iteration}" \
      env \
        BUILD_TYPE="${BUILD_TYPE}" \
        RESULT_DIR="${RESULT_DIR}/data_membership_iter${iteration}/run" \
        SMOKE_DIR="${RESULT_DIR}/data_membership_iter${iteration}/cluster" \
        CLUSTER_NAME="stress_data_membership_${iteration}" \
        MS_PORT="$((port_base + 100))" \
        SERVER_PORT="$((server_base + 100))" \
        OPS="${MEMBERSHIP_OPS}" \
        THREADS=2 \
        VALUE_BYTES="${VALUE_BYTES}" \
        PIN_PRIMARY_READS="${PIN_PRIMARY_READS}" \
        REPLICA_WAIT_MS="${REPLICA_WAIT_MS}" \
        BENCH_TIMEOUT_S="${BENCH_TIMEOUT_S}" \
        bash "${ROOT}/tools/run_data_raft_scale_up_down_ubuntu22.sh" || case_failed=1
  fi

  if [[ "${RUN_2NODE_SCALE}" == "1" ]]; then
    run_case "data_2node_scale" "${iteration}" \
      env \
        BUILD_TYPE="${BUILD_TYPE}" \
        RESULT_DIR="${RESULT_DIR}/data_2node_scale_iter${iteration}/run" \
        SMOKE_DIR="${RESULT_DIR}/data_2node_scale_iter${iteration}/cluster" \
        CLUSTER_NAME="stress_data_2node_${iteration}" \
        MS_PORT="$((port_base + 220))" \
        SERVER_PORT="$((server_base + 220))" \
        OPS="${OPS}" \
        THREAD_LIST="${THREAD_LIST}" \
        VALUE_BYTES="${VALUE_BYTES}" \
        PIN_PRIMARY_READS="${PIN_PRIMARY_READS}" \
        REPLICA_WAIT_MS="${REPLICA_WAIT_MS}" \
        BENCH_TIMEOUT_S="${BENCH_TIMEOUT_S}" \
        bash "${ROOT}/tools/run_data_raft_2node_scale_ubuntu22.sh" || case_failed=1
  fi

  if [[ "${RUN_MIXED_RW}" == "1" ]]; then
    run_case "data_mixed_rw" "${iteration}" \
      env \
        BUILD_TYPE="${BUILD_TYPE}" \
        RESULT_DIR="${RESULT_DIR}/data_mixed_rw_iter${iteration}/run" \
        SMOKE_DIR="${RESULT_DIR}/data_mixed_rw_iter${iteration}/cluster" \
        CLUSTER_NAME="stress_data_mixed_rw_${iteration}" \
        MS_PORT="$((port_base + 340))" \
        SERVER_PORT="$((server_base + 340))" \
        PROBE_OPS="${MIXED_PROBE_OPS}" \
        PROBE_THREADS="${MIXED_PROBE_THREADS}" \
        VALUE_BYTES="${VALUE_BYTES}" \
        BACKGROUND_WRITER_THREADS="${MIXED_BACKGROUND_WRITER_THREADS}" \
        BACKGROUND_READER_THREADS="${MIXED_BACKGROUND_READER_THREADS}" \
        BACKGROUND_WRITER_PAUSE_US="${MIXED_BACKGROUND_WRITER_PAUSE_US}" \
        BACKGROUND_READER_PAUSE_US="${MIXED_BACKGROUND_READER_PAUSE_US}" \
        DATA_RAFT_BOUNDED_STALE_MAX_INDEX_LAG="${MIXED_DATA_RAFT_BOUNDED_STALE_MAX_INDEX_LAG}" \
        BENCH_TIMEOUT_S="${BENCH_TIMEOUT_S}" \
        bash "${ROOT}/tools/run_data_raft_mixed_rw_ubuntu22.sh" || case_failed=1
  fi

  if [[ "${RUN_DATA_SNAPSHOT}" == "1" ]]; then
    run_case "data_snapshot_restore" "${iteration}" \
      env \
        BUILD_TYPE="${BUILD_TYPE}" \
        RESULT_DIR="${RESULT_DIR}/data_snapshot_restore_iter${iteration}/run" \
        SMOKE_DIR="${RESULT_DIR}/data_snapshot_restore_iter${iteration}/cluster" \
        CLUSTER_NAME="stress_data_snapshot_${iteration}" \
        MS_PORT="$((port_base + 460))" \
        SERVER_PORT="$((server_base + 460))" \
        OPS="${RAFT_SNAPSHOT_OPS:-240}" \
        THREADS="${RAFT_SNAPSHOT_THREADS:-2}" \
        VALUE_BYTES="${VALUE_BYTES}" \
        BENCH_TIMEOUT_S="${BENCH_TIMEOUT_S}" \
        bash "${ROOT}/tools/run_data_raft_snapshot_restore_ubuntu22.sh" || case_failed=1
  fi
done

if [[ "${RUN_FAILOVER}" == "1" ]]; then
  for iteration in $(seq 1 "${FAILOVER_ITERATIONS}"); do
    # bcache2-metaserver also opens a compatibility meta-query service on
    # metaserver_server_port - 1000. Keep failover ports one extra stride away
    # from earlier cases so the suite cannot self-interfere during cleanup lag.
    port_base=$((BASE_MS_PORT + (ITERATIONS + 2) * PORT_STRIDE + 500 + (iteration - 1) * PORT_STRIDE))
    server_base=$((BASE_SERVER_PORT + (ITERATIONS + 2) * PORT_STRIDE + 500 + (iteration - 1) * PORT_STRIDE))
    run_case "data_failover" "${iteration}" \
      env \
        BUILD_TYPE="${BUILD_TYPE}" \
        SMOKE_DIR="${RESULT_DIR}/data_failover_iter${iteration}/cluster" \
        RUN_LOG_DIR="${RESULT_DIR}/data_failover_iter${iteration}/runner" \
        CLUSTER_NAME="stress_data_failover_${iteration}" \
        MS_PORT="${port_base}" \
        SERVER_PORT="${server_base}" \
        PIN_PRIMARY_READS="${PIN_PRIMARY_READS}" \
        REPLICA_WAIT_MS="${REPLICA_WAIT_MS}" \
        bash "${ROOT}/tools/run_data_raft_failover_ubuntu22.sh" || case_failed=1
  done
fi

python3 - "${CSV}" "${SUMMARY}" <<'PY'
import csv
import sys

csv_path, summary_path = sys.argv[1], sys.argv[2]
rows = list(csv.DictReader(open(csv_path, encoding="utf-8")))
passed = sum(1 for row in rows if row["status"] == "pass")
failed = sum(1 for row in rows if row["status"] != "pass")
with open(summary_path, "a", encoding="utf-8") as out:
    out.write("\n== aggregate ==\n")
    out.write(f"passed={passed}\n")
    out.write(f"failed={failed}\n")
    for row in rows:
        out.write(
            f"{row['case']} iter={row['iteration']} status={row['status']} "
            f"seconds={row['seconds']} dir={row['result_dir']}\n"
        )
PY

cat "${SUMMARY}"
summary_status=0
summary_args=("${ROOT}/tools/summarize_raft_gate_results.py" "${RESULT_DIR}")
if [[ "${RAFT_PRODUCTION_ASSERTIONS}" == "1" ]]; then
  summary_args+=(
    "--production-assertions"
    "--max-metaserver-failover-ms" "${RAFT_MAX_METASERVER_FAILOVER_MS}"
    "--max-data-failover-write-read-ms" "${RAFT_MAX_DATA_FAILOVER_WRITE_READ_MS}"
    "--max-secondary-visibility-p99-us" "${RAFT_MAX_SECONDARY_VISIBILITY_P99_US}"
    "--max-post-failover-apply-lag" "${RAFT_MAX_POST_FAILOVER_APPLY_LAG}"
    "--max-2node-scale-p99-us" "${RAFT_MAX_2NODE_SCALE_P99_US}"
  )
fi
python3 "${summary_args[@]}" | tee -a "${SUMMARY}" || summary_status=$?
if [[ -n "${PROM_TEXTFILE_DIR}" && -f "${RESULT_DIR}/metrics.prom" ]]; then
  mkdir -p "${PROM_TEXTFILE_DIR}"
  cp "${RESULT_DIR}/metrics.prom" "${PROM_TEXTFILE_DIR}/${PROM_TEXTFILE_NAME}"
  echo "metrics_textfile=${PROM_TEXTFILE_DIR}/${PROM_TEXTFILE_NAME}" | tee -a "${SUMMARY}"
fi
if [[ "${case_failed}" != "0" ]]; then
  echo "FAIL raft stress suite"
  echo "${RESULT_DIR}"
  exit 1
fi
if [[ "${summary_status}" != "0" ]]; then
  echo "FAIL raft metrics summarizer"
  echo "${RESULT_DIR}"
  exit "${summary_status}"
fi

echo "PASS raft stress suite"
echo "${RESULT_DIR}"
