#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RESULT_DIR="${RESULT_DIR:-/tmp/temporalstore-production-gap-queue-$(date +%Y%m%d_%H%M%S)}"
QUEUE_LEVEL="${QUEUE_LEVEL:-quick}"
RUN_EXECUTION="${RUN_EXECUTION:-0}"
BUILD_TYPE="${BUILD_TYPE:-Release}"
BASE_MS_PORT="${BASE_MS_PORT:-21000}"
BASE_SERVER_PORT="${BASE_SERVER_PORT:-11000}"
RUN_DEDUPE_COMMANDS="${RUN_DEDUPE_COMMANDS:-1}"

mkdir -p "${RESULT_DIR}"
SUMMARY="${RESULT_DIR}/summary.md"
CSV="${RESULT_DIR}/gaps.csv"
RUN_CSV="${RESULT_DIR}/runs.csv"
declare -A RAN_COMMANDS=()

level_rank() {
  case "$1" in
    quick) echo 1 ;;
    pr) echo 2 ;;
    nightly) echo 3 ;;
    release) echo 4 ;;
    manual) echo 5 ;;
    *) echo 99 ;;
  esac
}

selected_for_level() {
  local item_level="$1"
  local requested="$2"
  [[ "$(level_rank "${item_level}")" -le "$(level_rank "${requested}")" ]]
}

write_header() {
  {
    echo "# TemporalStore Production Gap Queue"
    echo
    echo "- result_dir=${RESULT_DIR}"
    echo "- queue_level=${QUEUE_LEVEL}"
    echo "- run_execution=${RUN_EXECUTION}"
    echo "- build_type=${BUILD_TYPE}"
    echo "- run_dedupe_commands=${RUN_DEDUPE_COMMANDS}"
    echo
  } > "${SUMMARY}"
  echo "id,title,level,status,owner_gate" > "${CSV}"
  echo "id,status,seconds,case_dir" > "${RUN_CSV}"
}

append_gap() {
  local id="$1"
  local title="$2"
  local level="$3"
  local status="$4"
  local owner_gate="$5"
  local command="$6"
  local notes="$7"

  echo "${id},\"${title}\",${level},${status},\"${owner_gate}\"" >> "${CSV}"
  {
    echo "## ${id}. ${title}"
    echo
    echo "- level: ${level}"
    echo "- status: ${status}"
    echo "- owner_gate: ${owner_gate}"
    echo "- command: \`${command:-manual}\`"
    echo "- notes: ${notes}"
    echo
  } >> "${SUMMARY}"
}

run_gap() {
  local id="$1"
  local command="$2"
  local case_dir="${RESULT_DIR}/${id}"
  local start_s
  local end_s
  local code

  [[ -n "${command}" ]] || return 0
  [[ "${RUN_EXECUTION}" == "1" ]] || return 0

  if [[ "${RUN_DEDUPE_COMMANDS}" == "1" && -n "${RAN_COMMANDS[${command}]:-}" ]]; then
    echo "${id},skip,0,${RAN_COMMANDS[${command}]}" >> "${RUN_CSV}"
    {
      echo "### ${id} execution skipped"
      echo
      echo "- reason=duplicate command"
      echo "- first_case=${RAN_COMMANDS[${command}]}"
      echo
    } >> "${SUMMARY}"
    return 0
  fi
  RAN_COMMANDS["${command}"]="${case_dir}"

  mkdir -p "${case_dir}"
  start_s="$(date +%s)"
  set +e
  (cd "${ROOT}" && eval "${command}") > "${case_dir}/stdout.log" 2> "${case_dir}/stderr.log"
  code=$?
  set -e
  end_s="$(date +%s)"

  if [[ "${code}" == "0" ]]; then
    echo "${id},pass,$((end_s - start_s)),${case_dir}" >> "${RUN_CSV}"
    return 0
  fi

  echo "${id},fail,$((end_s - start_s)),${case_dir}" >> "${RUN_CSV}"
  {
    echo "### ${id} execution failure"
    echo
    echo "- code=${code}"
    echo "- case_dir=${case_dir}"
    echo
    echo "stdout tail:"
    tail -80 "${case_dir}/stdout.log" 2>/dev/null || true
    echo
    echo "stderr tail:"
    tail -80 "${case_dir}/stderr.log" 2>/dev/null || true
    echo
  } >> "${SUMMARY}"
  return "${code}"
}

register_gap() {
  local id="$1"
  local title="$2"
  local level="$3"
  local status="$4"
  local owner_gate="$5"
  local command="$6"
  local notes="$7"

  append_gap "${id}" "${title}" "${level}" "${status}" "${owner_gate}" "${command}" "${notes}"
  if selected_for_level "${level}" "${QUEUE_LEVEL}"; then
    run_gap "${id}" "${command}"
  fi
}

raft_pr_command='env BUILD_TYPE="${BUILD_TYPE}" RESULT_DIR="${RESULT_DIR}/raft_pr" ITERATIONS=1 FAILOVER_ITERATIONS=1 OPS=300 MEMBERSHIP_OPS=120 THREAD_LIST=2 BENCH_TIMEOUT_S=180 RUN_META_MEMBERSHIP=1 RUN_META_FAILOVER=1 RUN_DATA_MEMBERSHIP=1 RUN_2NODE_SCALE=1 RUN_MIXED_RW=1 RUN_DATA_SNAPSHOT=1 RUN_FAILOVER=1 bash tools/run_raft_stress_suite_ubuntu22.sh'
raft_release_command='env BUILD_TYPE="${BUILD_TYPE}" RESULT_DIR="${RESULT_DIR}/raft_release" ITERATIONS=5 FAILOVER_ITERATIONS=5 OPS=3000 MEMBERSHIP_OPS=600 THREAD_LIST="2 4" BENCH_TIMEOUT_S=300 bash tools/run_raft_production_gate_ubuntu22.sh'
prometheus_command='env BUILD_TYPE="${BUILD_TYPE}" RESULT_DIR="${RESULT_DIR}/prometheus" ITERATIONS=1 START_PROMETHEUS=0 RUN_CLIENT_SCALE=1 THREAD_LIST=2 STRING_OPS=200 bash tools/run_prometheus_local_ubuntu22.sh'
live_raft_metrics_command='env BUILD_TYPE="${BUILD_TYPE}" RESULT_DIR="${RESULT_DIR}/live_raft_metrics" MS_PORT="$((BASE_MS_PORT + 9000))" SERVER_PORT="$((BASE_SERVER_PORT + 5000))" MAX_APPLY_LAG=16 bash tools/run_live_raft_metrics_ubuntu22.sh'
ci_command='env RESULT_DIR="${RESULT_DIR}/ci_guard" ITERATIONS=1 RUN_FULL_GATE=1 FULL_GATE_RUN_BUILD=0 FULL_GATE_RUN_TEST_BUILD=0 FULL_GATE_RUN_UNIT=0 FULL_GATE_RUN_API=1 FULL_GATE_RUN_PROMETHEUS=1 FULL_GATE_RUN_INGESTION=1 FULL_GATE_RUN_REDIS=0 FULL_GATE_RUN_RAFT=0 bash tools/run_ci_guard_ubuntu22.sh'
shared_scale_command='env BUILD_TYPE="${BUILD_TYPE}" RESULT_DIR="${RESULT_DIR}/shared_3node" SMOKE_DIR="${RESULT_DIR}/shared_3node/cluster" STRING_OPS=1500 STRING_THREADS=4 SEQUENCE_KEYS=4 SEQUENCE_ROWS_PER_KEY=300 SEQUENCE_QUERY_OPS=300 SEQUENCE_THREADS=4 RUN_PROXY_INGESTION_PRESSURE=1 PROXY_INGESTION_PRESSURE_OPS=500 PROXY_INGESTION_PRESSURE_THREADS=4 PROXY_INGESTION_PRESSURE_VERIFY_READS=1 bash tools/run_shared_file_3node_scale_ubuntu22.sh'
ingestion_command='env RESULT_DIR="${RESULT_DIR}/ingestion" ITERATIONS=2 RECORDS=1200 BATCH_SIZE=128 SOURCES=api,kafka,flink DEAD_LETTER_EVERY=97 FAIL_FIRST_ATTEMPT_EVERY=53 POISON_EVERY=211 bash tools/run_queue_ingestion_replay_ubuntu22.sh'
cache_fallback_command='env RESULT_DIR="${RESULT_DIR}/cache_fallback" RUN_RUNTIME_TEST=auto bash tools/run_cache_fallback_metrics_ubuntu22.sh'
follower_read_sla_command='env BUILD_TYPE="${BUILD_TYPE}" RESULT_DIR="${RESULT_DIR}/follower_read_sla" PROBE_OPS=120 PROBE_THREADS=2 BACKGROUND_WRITER_THREADS=1 BACKGROUND_READER_THREADS=2 BENCH_TIMEOUT_S=180 DATA_RAFT_BOUNDED_STALE_MAX_INDEX_LAG=16 SERVER_HEARTBEAT_INTERVAL_MS=500 SERVER_HEARTBEAT_TIMEOUT_MS=1000 SERVER_META_TINKER_INTERVAL_MS=500 MAX_SECONDARY_VISIBILITY_P95_US=50000 MAX_SECONDARY_VISIBILITY_P99_US=100000 bash tools/run_follower_read_sla_ubuntu22.sh'
fault_injection_command='env BUILD_TYPE="${BUILD_TYPE}" RESULT_DIR="${RESULT_DIR}/fault_injection" RUN_PORT_BLOCK=1 RUN_DISK_PATH=1 bash tools/run_fault_injection_gate_ubuntu22.sh'
rebalance_command='env BUILD_TYPE="${BUILD_TYPE}" RESULT_DIR="${RESULT_DIR}/rebalance" BASE_MS_PORT="${BASE_MS_PORT}" BASE_SERVER_PORT="${BASE_SERVER_PORT}" OPS=240 THREADS=2 RUN_DATA_RAFT_REBALANCE=1 RUN_SHARED_STORE_REBALANCE=0 bash tools/run_rebalance_local_ubuntu22.sh'
data_raft_5node_command='env BUILD_TYPE="${BUILD_TYPE}" RESULT_DIR="${RESULT_DIR}/data_raft_5node" OPS=600 THREAD_LIST="2" bash tools/run_data_raft_5node_scale_ubuntu22.sh'
stale_restart_command='env BUILD_TYPE="${BUILD_TYPE}" RESULT_DIR="${RESULT_DIR}/stale_restart" MS_PORT="$((BASE_MS_PORT + 18000))" SERVER_PORT="$((BASE_SERVER_PORT + 3000))" OPS=120 THREADS=2 bash tools/run_stale_local_data_restart_gate_ubuntu22.sh'
multitenant_command='env BUILD_TYPE="${BUILD_TYPE}" RESULT_DIR="${RESULT_DIR}/multitenant" NOISY_OPS=150 NOISY_THREADS=1 VICTIM_OPS=120 VICTIM_THREADS=1 MAX_VICTIM_P99_US=100000 MIN_VICTIM_QPS=1 bash tools/run_multitenant_noisy_neighbor_ubuntu22.sh'
metaserver_snapshot_command='env BUILD_TYPE="${BUILD_TYPE}" RESULT_DIR="${RESULT_DIR}/metaserver_snapshot" MS_PORT="$((BASE_MS_PORT + 16000))" MS_RAFT_PORT="$((BASE_MS_PORT + 16010))" MS_SNAPSHOT_PORT="$((BASE_MS_PORT + 16020))" LOG_CHURN_NAMESPACES=12 bash tools/run_metaserver_raft_snapshot_restore_ubuntu22.sh'
soak_command='env BUILD_TYPE="${BUILD_TYPE}" RESULT_DIR="${RESULT_DIR}/soak" SOAK_MINUTES="${SOAK_MINUTES:-30}" SOAK_QUEUE_LEVEL=quick RUN_QUEUE_EXECUTION=1 bash tools/run_soak_profile_ubuntu22.sh'
remote_auth_command='env RESULT_DIR="${RESULT_DIR}/remote_auth" REMOTE_TIMEOUT_S=30 bash tools/run_remote_auth_gate_ubuntu22.sh'

write_header

register_gap 01 "Metaserver failover during membership change" pr partial "run_raft_stress_suite_ubuntu22.sh" "${raft_pr_command}" "Existing suite runs metaserver membership and failover in one PR gate; next step is an interleaved same-window fault."
register_gap 02 "Data primary kill during scale up/down" pr partial "run_data_raft_scale_up_down_ubuntu22.sh + run_data_raft_failover_ubuntu22.sh" "${raft_pr_command}" "Covered as adjacent membership and failover gates; same-window primary kill remains a targeted enhancement."
register_gap 03 "Removed data-node restart/rejoin safety" pr covered "run_data_raft_scale_up_down_ubuntu22.sh" "${raft_pr_command}" "Data membership gate exercises add/remove and serving validation."
register_gap 04 "Removed metaserver restart/rejoin safety" pr covered "run_metaserver_raft_membership_ubuntu22.sh" "${raft_pr_command}" "Metaserver membership gate validates raft-backed membership."
register_gap 05 "Dedicated rebalance harness" pr partial "tools/run_rebalance_local_ubuntu22.sh" "${rebalance_command}" "Dedicated harness now executes the data-raft add/remove redistribution path; shared-store scheduler rebalance remains a follow-up."
register_gap 06 "Live raft gauges from data node/metaserver" quick covered "tools/run_live_raft_metrics_ubuntu22.sh" "${live_raft_metrics_command}" "Dedicated local gate collects live metaserver QueryClusterStatus and data-node GetDataRaftStatus samples into Prometheus textfile gauges, then fails on missing leaders, fatal events, or excessive apply lag."
register_gap 07 "Client/proxy retry metrics" quick covered "run_prometheus_local_ubuntu22.sh" "${prometheus_command}" "Local Prometheus gate now emits and requires client/proxy retry-attempt and retry-failure counters, with alert coverage for exhausted retries."
register_gap 08 "Ingestion queue/backpressure metrics" quick covered "run_queue_ingestion_replay_ubuntu22.sh" "${ingestion_command}" "Queue replay gate now emits Prometheus textfile metrics for input, dedupe, retries, dead letters, checkpoints, watermark, max partition lag, backpressure records, committed QPS, and validation health, with alert coverage."
register_gap 09 "Cache/storage fallback metrics" quick covered "tools/run_cache_fallback_metrics_ubuntu22.sh" "${cache_fallback_command}" "Dedicated gate validates page-store blockcache get/hit and persistent-read instrumentation, the evicted-object fallback unit assertions, and Prometheus exporter coverage; runtime test is executed when a healthy local partition_test binary is available."
register_gap 10 "5-node data raft local scale" nightly partial "run_data_raft_5node_scale_ubuntu22.sh" "${data_raft_5node_command}" "Five data-node raft cluster gate starts 5 servers with 3 replicas and validates write/read plus replication smoke; sustained 5-node perf remains nightly/release work."
register_gap 11 "3-replica sustained write/read lag" pr covered "run_raft_stress_suite_ubuntu22.sh" "${raft_pr_command}" "Mixed read/write and secondary visibility assertions cover lag in short PR mode; release mode increases duration."
register_gap 12 "Snapshot restore under write pressure" pr partial "run_data_raft_snapshot_restore_ubuntu22.sh" "${raft_pr_command}" "Snapshot restore gate exists; write-pressure overlap should be hardened in the data snapshot script."
register_gap 13 "Metaserver snapshot restore gate" pr partial "run_metaserver_raft_snapshot_restore_ubuntu22.sh" "${metaserver_snapshot_command}" "Dedicated local gate triggers metaserver raft snapshot, restarts all metaserver nodes from existing work dirs, verifies membership and metadata survive, then writes again after restore."
register_gap 14 "Follower-read bounded-stale SLA gate" pr covered "tools/run_follower_read_sla_ubuntu22.sh" "${follower_read_sla_command}" "Dedicated gate runs data-node raft bounded-stale follower reads under background read/write load, emits Prometheus SLA metrics, and fails on errors or p95/p99 visibility latency violations."
register_gap 15 "Network timeout/port-block fault gate" pr covered "tools/run_fault_injection_gate_ubuntu22.sh" "${fault_injection_command}" "Local gate reserves a metaserver port and verifies the service harness fails fast with a bounded diagnostic."
register_gap 16 "Process restart with stale local data gate" pr partial "tools/run_stale_local_data_restart_gate_ubuntu22.sh" "${stale_restart_command}" "Dedicated gate exercises data-node restart from existing raft/storage directories after writes; explicit corrupted-stale rejection remains follow-up."
register_gap 17 "Disk/path failure simulation" pr covered "tools/run_fault_injection_gate_ubuntu22.sh" "" "Covered by the same local fault-injection gate, which rejects invalid storage paths before service startup."
register_gap 18 "Prometheus alert rules" quick covered "tools/temporalstore-prometheus/temporalstore-alerts.yml" "test -f tools/temporalstore-prometheus/temporalstore-alerts.yml && grep -q TemporalStoreServiceDown tools/temporalstore-prometheus/temporalstore-alerts.yml" "Alert rules are installed into Prometheus config by this queue change."
register_gap 19 "CI full-gate mode for Docker Prometheus" quick covered "run_ci_guard_ubuntu22.sh" "${ci_command}" "CI guard can run full API/prometheus/ingestion smoke with START_PROMETHEUS governed by the production gate."
register_gap 20 "Build/test CI with dependency cache" manual partial "tools/build_ubuntu22.sh + CI design" "" "Local cache reuse is documented; remote workflow update still requires workflow-scope branch handling."
register_gap 21 "Full 5-repeat raft production gate" release covered "run_raft_production_gate_ubuntu22.sh" "${raft_release_command}" "Release gate is executable and repeats raft/failover production assertions 5 times."
register_gap 22 "30-minute soak profile" nightly covered "tools/run_soak_profile_ubuntu22.sh" "${soak_command}" "Nightly soak wrapper repeatedly runs the executable quick queue for the configured duration; default target is 30 minutes."
register_gap 23 "Multi-tenant noisy-neighbor gate" pr covered "tools/run_multitenant_noisy_neighbor_ubuntu22.sh" "${multitenant_command}" "Local gate runs noisy and victim namespaces concurrently, verifies victim/noisy correctness, emits Prometheus isolation metrics, and fails when victim p99 or QPS violates the configured SLA."
register_gap 24 "Failover/lag/rebalance runbook" quick covered "docs/production_gap_queue_runbook.md" "test -f docs/production_gap_queue_runbook.md && grep -q Failover docs/production_gap_queue_runbook.md" "Runbook is installed by this queue change."
register_gap 25 "Push/remote CI once WSL auth is fixed" manual partial "tools/run_remote_auth_gate_ubuntu22.sh" "${remote_auth_command}" "Remote auth is a bounded manual gate because this WSL session still requires gh or Git Credential Manager setup."

python3 - "${CSV}" "${SUMMARY}" <<'PY'
import csv
import sys

csv_path, summary_path = sys.argv[1], sys.argv[2]
rows = list(csv.DictReader(open(csv_path, encoding="utf-8")))
counts = {}
for row in rows:
    counts[row["status"]] = counts.get(row["status"], 0) + 1
with open(summary_path, "a", encoding="utf-8") as out:
    out.write("## Aggregate\n\n")
    for status in sorted(counts):
        out.write(f"- {status}: {counts[status]}\n")
    out.write(f"- total: {len(rows)}\n")
print(f"summary={summary_path}")
print(f"csv={csv_path}")
for status in sorted(counts):
    print(f"{status}={counts[status]}")
PY

if [[ "${RUN_EXECUTION}" == "1" ]]; then
  failed_runs="$(awk -F, 'NR > 1 && $2 == "fail" {count++} END {print count+0}' "${RUN_CSV}")"
  if [[ "${failed_runs}" != "0" ]]; then
    echo "FAIL production gap queue execution"
    echo "${RESULT_DIR}"
    exit 1
  fi
fi

echo "PASS production gap queue"
echo "${RESULT_DIR}"
