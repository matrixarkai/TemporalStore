#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_TYPE="${BUILD_TYPE:-Release}"
BUILD_FLAVOR="$(printf '%s' "${BUILD_TYPE}" | tr '[:upper:]' '[:lower:]')"
BUILD_DIR="${BUILD_DIR:-${ROOT}/build-ubuntu22/${BUILD_FLAVOR}}"
OUT_DIR="${OUT_DIR:-${ROOT}/output-ubuntu22/${BUILD_FLAVOR}}"
TEST_BUILD_DIR="${TEST_BUILD_DIR:-${ROOT}/build-ubuntu22/test-${BUILD_FLAVOR}}"
TEST_OUT_DIR="${TEST_OUT_DIR:-${ROOT}/output-ubuntu22/test-${BUILD_FLAVOR}}"
RESULT_DIR="${RESULT_DIR:-/tmp/temporalstore-production-readiness-$(date +%Y%m%d_%H%M%S)}"
ITERATIONS="${ITERATIONS:-3}"
BASE_PORT="${BASE_PORT:-26000}"
PORT_STRIDE="${PORT_STRIDE:-700}"

RUN_BUILD="${RUN_BUILD:-1}"
RUN_TEST_BUILD="${RUN_TEST_BUILD:-0}"
RUN_UNIT="${RUN_UNIT:-1}"
RUN_API="${RUN_API:-1}"
RUN_PROMETHEUS="${RUN_PROMETHEUS:-1}"
RUN_CI_GUARD="${RUN_CI_GUARD:-0}"
RUN_INGESTION="${RUN_INGESTION:-1}"
RUN_REDIS="${RUN_REDIS:-1}"
RUN_RAFT="${RUN_RAFT:-1}"
START_PROMETHEUS="${START_PROMETHEUS:-0}"
CONTINUE_ON_FAILURE="${CONTINUE_ON_FAILURE:-0}"

BUILD_TARGETS="${BUILD_TARGETS:-bcache2-server bcache2-metaserver replication_smoke_example queue_ingestion_replay_example customer_client_example}"
UNIT_BUILD_TARGETS="${UNIT_BUILD_TARGETS:-common_test storage_pool_uri_guardrail_test stream_test store_test object_store_guardrail_test stream_unittest hash_model_test feature_model_test ips_model_test risk_hash_model_test cpc_model_test index_test storage_test data_raft_replication_codec_smoke partition_test server_test ms_test smoketest consistency_smoketest}"
UNIT_BUILD_DIR="${UNIT_BUILD_DIR:-${BUILD_DIR}}"
UNIT_CTEST_PARALLEL="${UNIT_CTEST_PARALLEL:-2}"
UNIT_CTEST_TIMEOUT_S="${UNIT_CTEST_TIMEOUT_S:-180}"
UNIT_MIN_TESTS="${UNIT_MIN_TESTS:-1}"
RAFT_GATE_LEVEL="${RAFT_GATE_LEVEL:-pr}"
RAFT_OPS="${RAFT_OPS:-300}"
RAFT_MEMBERSHIP_OPS="${RAFT_MEMBERSHIP_OPS:-120}"
RAFT_TIMEOUT_S="${RAFT_TIMEOUT_S:-180}"

mkdir -p "${RESULT_DIR}"
SUMMARY="${RESULT_DIR}/summary.txt"
CSV="${RESULT_DIR}/cases.csv"
echo "phase,iteration,status,seconds,result_dir" > "${CSV}"

log() {
  printf '%s\n' "$*" | tee -a "${SUMMARY}"
}

record_case() {
  local phase="$1"
  local iteration="$2"
  local status="$3"
  local seconds="$4"
  local case_dir="$5"
  echo "${phase},${iteration},${status},${seconds},${case_dir}" >> "${CSV}"
}

run_case() {
  local phase="$1"
  local iteration="$2"
  shift 2
  local case_dir="${RESULT_DIR}/${iteration}_${phase}"
  local start_s
  local end_s
  local code
  mkdir -p "${case_dir}"

  log "== ${phase} iteration ${iteration}/${ITERATIONS} =="
  start_s="$(date +%s)"
  set +e
  (
    set -euo pipefail
    "$@"
  ) > "${case_dir}/stdout.log" 2> "${case_dir}/stderr.log"
  code=$?
  set -e
  end_s="$(date +%s)"

  if [[ "${code}" == "0" ]]; then
    log "${phase} iteration ${iteration}: PASS $((end_s - start_s))s"
    record_case "${phase}" "${iteration}" "pass" "$((end_s - start_s))" "${case_dir}"
    return 0
  fi

  log "${phase} iteration ${iteration}: FAIL code=${code} $((end_s - start_s))s"
  record_case "${phase}" "${iteration}" "fail" "$((end_s - start_s))" "${case_dir}"
  tail -120 "${case_dir}/stdout.log" | sed 's/^/[stdout] /' | tee -a "${SUMMARY}" || true
  tail -120 "${case_dir}/stderr.log" | sed 's/^/[stderr] /' | tee -a "${SUMMARY}" || true
  return "${code}"
}

run_unit_ctest() {
  local ctest_total=0

  local ctest_args=(
    "--output-on-failure"
    "-j" "${UNIT_CTEST_PARALLEL}"
    "--timeout" "${UNIT_CTEST_TIMEOUT_S}"
    "--exclude-regex" "NOT_BUILT"
  )
  run_ctest_dir() {
    local dir="$1"
    local label="$2"
    local json_file="${RESULT_DIR}/ctest_${label}.json"
    local count_file="${RESULT_DIR}/ctest_${label}.count"
    local count

    [[ -d "${dir}" ]] || return 0
    (
      cd "${dir}"
      env \
        BYTED_HOST_IP="${BYTED_HOST_IP:-127.0.0.1}" \
        BYTED_HOST_IPV6="${BYTED_HOST_IPV6:-::1}" \
        MY_HOST_IP="${MY_HOST_IP:-127.0.0.1}" \
        BDC_PRIVATE_CLOUD="${BDC_PRIVATE_CLOUD:-True}" \
        ASAN_OPTIONS="${ASAN_OPTIONS:-detect_leaks=false,abort_on_error=true}" \
        ctest --show-only=json-v1 --exclude-regex "NOT_BUILT" > "${json_file}"
    )
    count="$(python3 - "${json_file}" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as fh:
    data = json.load(fh)
tests = [
    test for test in data.get("tests", [])
    if "NOT_BUILT" not in test.get("name", "")
]
print(len(tests))
PY
)"
    echo "${count}" > "${count_file}"
    if (( count == 0 )); then
      echo "CTest ${label} discovered zero runnable tests in ${dir}" >&2
      return 1
    fi
    ctest_total=$((ctest_total + count))
    (
      cd "${dir}"
      env \
        BYTED_HOST_IP="${BYTED_HOST_IP:-127.0.0.1}" \
        BYTED_HOST_IPV6="${BYTED_HOST_IPV6:-::1}" \
        MY_HOST_IP="${MY_HOST_IP:-127.0.0.1}" \
        BDC_PRIVATE_CLOUD="${BDC_PRIVATE_CLOUD:-True}" \
        ASAN_OPTIONS="${ASAN_OPTIONS:-detect_leaks=false,abort_on_error=true}" \
        ctest "${ctest_args[@]}"
    )
  }

  if [[ "${RUN_TEST_BUILD}" == "1" ]]; then
    UNIT_BUILD_DIR="${TEST_BUILD_DIR}"
  else
    local unit_targets=()
    read -r -a unit_targets <<< "${UNIT_BUILD_TARGETS}"
    local help_file="${RESULT_DIR}/unit_targets.help"
    cmake --build "${UNIT_BUILD_DIR}" --target help > "${help_file}" 2>/dev/null || true
    local buildable_targets=()
    for target in "${unit_targets[@]}"; do
      if grep -qE "^[.][.][.] ${target}($| )" "${help_file}"; then
        buildable_targets+=("${target}")
      fi
    done
    if (( ${#buildable_targets[@]} > 0 )); then
      cmake --build "${UNIT_BUILD_DIR}" --parallel "${BUILD_JOBS:-2}" --target "${buildable_targets[@]}"
    fi
  fi

  if [[ -f "${UNIT_BUILD_DIR}/CTestTestfile.cmake" ]]; then
    run_ctest_dir "${UNIT_BUILD_DIR}" "root"
  else
    run_ctest_dir "${UNIT_BUILD_DIR}/src" "src"
    run_ctest_dir "${UNIT_BUILD_DIR}/test" "test"
  fi
  if (( ctest_total < UNIT_MIN_TESTS )); then
    echo "CTest discovered ${ctest_total} tests, below UNIT_MIN_TESTS=${UNIT_MIN_TESTS}" >&2
    return 1
  fi
  log "ctest_total=${ctest_total}"
}

run_build() {
  local targets=()
  read -r -a targets <<< "${BUILD_TARGETS}"
  cmake --build "${BUILD_DIR}" --parallel "${BUILD_JOBS:-2}" --target "${targets[@]}"
}

wait_http_ready() {
  local label="$1"
  local url="$2"
  local deadline_s="${3:-30}"
  local deadline=$((SECONDS + deadline_s))

  while (( SECONDS < deadline )); do
    if curl -fsS -m 2 "${url}" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done

  echo "${label} did not become ready at ${url} within ${deadline_s}s" >&2
  return 1
}

wait_server_load_ready() {
  local server_port="$1"
  local deadline_s="${2:-30}"
  local status_text
  local load_count
  local load_concurrency
  local deadline=$((SECONDS + deadline_s))

  while (( SECONDS < deadline )); do
    status_text="$(curl -fsS -m 2 "http://127.0.0.1:${server_port}/status" 2>/dev/null || true)"
    if [[ -n "${status_text}" ]]; then
      read -r load_count load_concurrency <<< "$(python3 - <<'PY' "${status_text}"
import re
import sys

status = sys.argv[1]
match = re.search(r"Load \(LoadRequest\).*?count: ([0-9]+).*?concurrency: ([0-9]+)",
                  status, re.S)
if not match:
    print("0 1")
else:
    print(match.group(1), match.group(2))
PY
)"
      if (( load_count > 0 && load_concurrency == 0 )); then
        return 0
      fi
    fi
    sleep 1
  done

  echo "server ${server_port} did not finish partition load within ${deadline_s}s" >&2
  return 1
}

run_customer_client_with_retry() {
  local metaserver_endpoint="$1"
  local namespace_name="$2"
  local table_name="$3"
  local deadline_s="${4:-30}"
  local log_dir="$5"
  local deadline=$((SECONDS + deadline_s))
  local attempt=0
  local status=0

  mkdir -p "${log_dir}"
  while (( SECONDS < deadline )); do
    attempt=$((attempt + 1))
    set +e
    "${BUILD_DIR}/src/client/example/customer_client_example" \
      "${metaserver_endpoint}" \
      "vdc1" \
      "${namespace_name}" \
      "${table_name}" \
      > "${log_dir}/customer_attempt_${attempt}.out" \
      2> "${log_dir}/customer_attempt_${attempt}.err"
    status=$?
    set -e
    if [[ "${status}" == "0" ]]; then
      if ! grep -q "PASS customer production client example" \
        "${log_dir}/customer_attempt_${attempt}.out"; then
        cat "${log_dir}/customer_attempt_${attempt}.out" || true
        echo "customer client exited successfully without PASS marker" >&2
        return 1
      fi
      cat "${log_dir}/customer_attempt_${attempt}.out"
      return 0
    fi
    if ! grep -qE "Slot not found|Partition info not found|Partition no primary|Request server failed" \
      "${log_dir}/customer_attempt_${attempt}.err" \
      "${log_dir}/customer_attempt_${attempt}.out" 2>/dev/null; then
      cat "${log_dir}/customer_attempt_${attempt}.out" || true
      cat "${log_dir}/customer_attempt_${attempt}.err" >&2 || true
      return "${status}"
    fi
    sleep 1
  done

  cat "${log_dir}/customer_attempt_${attempt}.out" || true
  cat "${log_dir}/customer_attempt_${attempt}.err" >&2 || true
  return "${status}"
}

run_test_build() {
  env \
    BUILD_TYPE="${BUILD_TYPE}" \
    BUILD_DIR="${TEST_BUILD_DIR}" \
    OUTPUT_DIR="${TEST_OUT_DIR}" \
    BCACHE2_BUILD_TESTS=ON \
    BUILD_TARGETS="${UNIT_BUILD_TARGETS}" \
    JOBS="${BUILD_JOBS:-2}" \
    "${ROOT}/tools/build_ubuntu22.sh"
  UNIT_BUILD_DIR="${TEST_BUILD_DIR}"
}

run_api_smoke() {
  local iteration="$1"
  local port_base=$((BASE_PORT + (iteration - 1) * PORT_STRIDE))
  local namespace_name="prod_gate_api_ns_${iteration}"
  local table_name="prod_gate_api_table_${iteration}"
  local deploy_dir="${RESULT_DIR}/${iteration}_api_runtime"
  local status=0

  timeout "${API_DEPLOY_TIMEOUT_S:-120}" env \
    BUILD_TYPE="${BUILD_TYPE}" \
    OUT_DIR="${OUT_DIR}" \
    DEPLOY_DIR="${deploy_dir}" \
    RUNTIME_DIR="${deploy_dir}/runtime" \
    CLUSTER_NAME="prod_gate_api_${iteration}_$$" \
    NAMESPACE_NAME="${namespace_name}" \
    TABLE_NAME="${table_name}" \
    MS_PORT="${port_base}" \
    MS_RAFT_PORT="$((port_base + 10))" \
    MS_SNAPSHOT_PORT="$((port_base + 20))" \
    SERVER_PORT="$((port_base + 1))" \
    META_COUNT=1 \
    SERVER_COUNT=1 \
    REPLICA_COUNT=1 \
    SERVER_EXTRA_FLAGS="${API_SERVER_EXTRA_FLAGS:---server_stopping_wait_s=1}" \
    bash "${ROOT}/tools/deploy_local_ubuntu22.sh" start

  wait_http_ready "metaserver status" \
    "http://127.0.0.1:${port_base}/status" \
    "${API_READY_TIMEOUT_S:-30}" || status=$?
  wait_http_ready "metaserver vars" \
    "http://127.0.0.1:${port_base}/vars" \
    "${API_READY_TIMEOUT_S:-30}" || status=$?
  wait_http_ready "nodeserver vars" \
    "http://127.0.0.1:$((port_base + 1))/vars" \
    "${API_READY_TIMEOUT_S:-30}" || status=$?
  if [[ "${status}" == "0" ]]; then
    wait_server_load_ready "$((port_base + 1))" "${API_READY_TIMEOUT_S:-30}" || status=$?
  fi

  if [[ "${status}" == "0" ]]; then
    run_customer_client_with_retry \
      "127.0.0.1:${port_base}" \
      "${namespace_name}" \
      "${table_name}" \
      "${API_CLIENT_READY_TIMEOUT_S:-30}" \
      "${deploy_dir}/client" || status=$?
  fi

  env \
    DEPLOY_DIR="${deploy_dir}" \
    RUNTIME_DIR="${deploy_dir}/runtime" \
    CLUSTER_NAME="prod_gate_api_${iteration}_$$" \
    bash "${ROOT}/tools/deploy_local_ubuntu22.sh" stop || true

  return "${status}"
}

run_prometheus_gate() {
  local iteration="$1"
  local port_base=$((BASE_PORT + 100 + (iteration - 1) * PORT_STRIDE))
  env \
    BUILD_TYPE="${BUILD_TYPE}" \
    ITERATIONS=1 \
    RUN_CLIENT_SCALE=0 \
    START_PROMETHEUS="${START_PROMETHEUS}" \
    RESULT_DIR="${RESULT_DIR}/${iteration}_prometheus_runtime" \
    MS_PORT="${port_base}" \
    SERVER_PORT="$((port_base + 1))" \
    SERVER_EXTRA_FLAGS="${PROMETHEUS_SERVER_EXTRA_FLAGS:---server_stopping_wait_s=1}" \
    bash "${ROOT}/tools/run_prometheus_local_ubuntu22.sh"
}

run_ci_guard() {
  env \
    ITERATIONS="${CI_GUARD_ITERATIONS:-1}" \
    RUN_FULL_GATE=0 \
    RESULT_DIR="${RESULT_DIR}/ci_guard_$(date +%s%N)" \
    bash "${ROOT}/tools/run_ci_guard_ubuntu22.sh"
}

run_ingestion_gate() {
  env \
    BUILD_TYPE="${BUILD_TYPE}" \
    ITERATIONS=1 \
    RECORDS="${INGESTION_RECORDS:-1000}" \
    BATCH_SIZE="${INGESTION_BATCH_SIZE:-128}" \
    DRY_RUN=1 \
    RESULT_DIR="${RESULT_DIR}/ingestion_$(date +%s%N)" \
    bash "${ROOT}/tools/run_queue_ingestion_replay_ubuntu22.sh"
}

run_redis_gate() {
  local iteration="$1"
  local port_base=$((BASE_PORT + 200 + (iteration - 1) * PORT_STRIDE))
  env \
    BUILD_DIR="${BUILD_DIR}" \
    OUT_DIR="${OUT_DIR}" \
    REPEAT=1 \
    RUN_BENCH=0 \
    BASE_PORT="${port_base}" \
    SERVER_EXTRA_FLAGS="${REDIS_SERVER_EXTRA_FLAGS:---storage_async=true --server_stopping_wait_s=1}" \
    REDIS_PARTITION_READY_SLEEP_S="${REDIS_PARTITION_READY_SLEEP_S:-0.2}" \
    RESULT_ROOT="${RESULT_DIR}/${iteration}_redis_runtime" \
    bash "${ROOT}/tools/run_redis_production_gate_ubuntu22.sh"
}

run_raft_gate() {
  local iteration="$1"
  local port_base=$((BASE_PORT + 300 + (iteration - 1) * PORT_STRIDE))
  local server_base=$((BASE_PORT + 400 + (iteration - 1) * PORT_STRIDE))
  local raft_iterations=1
  local failover_iterations=1
  local thread_list="${RAFT_THREAD_LIST:-2}"
  local mixed_probe_ops="${RAFT_MIXED_PROBE_OPS:-300}"
  local mixed_probe_threads="${RAFT_MIXED_PROBE_THREADS:-4}"
  case "${RAFT_GATE_LEVEL}" in
    pr)
      ;;
    nightly)
      raft_iterations="${RAFT_NIGHTLY_ITERATIONS:-2}"
      failover_iterations="${RAFT_NIGHTLY_FAILOVER_ITERATIONS:-2}"
      thread_list="${RAFT_THREAD_LIST:-2 4}"
      ;;
    release)
      raft_iterations="${RAFT_RELEASE_ITERATIONS:-3}"
      failover_iterations="${RAFT_RELEASE_FAILOVER_ITERATIONS:-3}"
      thread_list="${RAFT_THREAD_LIST:-2 4}"
      mixed_probe_ops="${RAFT_MIXED_PROBE_OPS:-1000}"
      ;;
    *)
      echo "unsupported RAFT_GATE_LEVEL=${RAFT_GATE_LEVEL}; use pr, nightly, or release" >&2
      return 2
      ;;
  esac
  env \
    BUILD_TYPE="${BUILD_TYPE}" \
    RESULT_DIR="${RESULT_DIR}/${iteration}_raft_runtime" \
    ITERATIONS="${raft_iterations}" \
    BASE_MS_PORT="${port_base}" \
    BASE_SERVER_PORT="${server_base}" \
    OPS="${RAFT_OPS}" \
    MEMBERSHIP_OPS="${RAFT_MEMBERSHIP_OPS}" \
    THREAD_LIST="${thread_list}" \
    BENCH_TIMEOUT_S="${RAFT_TIMEOUT_S}" \
    MIXED_PROBE_OPS="${mixed_probe_ops}" \
    MIXED_PROBE_THREADS="${mixed_probe_threads}" \
    RUN_2NODE_SCALE=1 \
    RUN_MIXED_RW=1 \
    RUN_DATA_MEMBERSHIP=1 \
    RUN_META_MEMBERSHIP=1 \
    RUN_META_FAILOVER=1 \
    RUN_FAILOVER=1 \
    FAILOVER_ITERATIONS="${failover_iterations}" \
    bash "${ROOT}/tools/run_raft_stress_suite_ubuntu22.sh"
}

log "result_dir=${RESULT_DIR}"
log "iterations=${ITERATIONS}"
log "build_type=${BUILD_TYPE}"
log "build_dir=${BUILD_DIR}"
log "out_dir=${OUT_DIR}"
log "test_build_dir=${TEST_BUILD_DIR}"
log "run_test_build=${RUN_TEST_BUILD}"
log "run_ci_guard=${RUN_CI_GUARD}"
log "raft_gate_level=${RAFT_GATE_LEVEL}"

overall_failed=0
for iteration in $(seq 1 "${ITERATIONS}"); do
  if [[ "${RUN_BUILD}" == "1" ]]; then
    run_case "build" "${iteration}" run_build || {
      overall_failed=1
      [[ "${CONTINUE_ON_FAILURE}" == "1" ]] || break
    }
  fi
  if [[ "${RUN_TEST_BUILD}" == "1" ]]; then
    run_case "test_build" "${iteration}" run_test_build || {
      overall_failed=1
      [[ "${CONTINUE_ON_FAILURE}" == "1" ]] || break
    }
  fi
  if [[ "${RUN_UNIT}" == "1" ]]; then
    run_case "unit" "${iteration}" run_unit_ctest || {
      overall_failed=1
      [[ "${CONTINUE_ON_FAILURE}" == "1" ]] || break
    }
  fi
  if [[ "${RUN_API}" == "1" ]]; then
    run_case "api" "${iteration}" run_api_smoke "${iteration}" || {
      overall_failed=1
      [[ "${CONTINUE_ON_FAILURE}" == "1" ]] || break
    }
  fi
  if [[ "${RUN_PROMETHEUS}" == "1" ]]; then
    run_case "prometheus" "${iteration}" run_prometheus_gate "${iteration}" || {
      overall_failed=1
      [[ "${CONTINUE_ON_FAILURE}" == "1" ]] || break
    }
  fi
  if [[ "${RUN_CI_GUARD}" == "1" ]]; then
    run_case "ci_guard" "${iteration}" run_ci_guard || {
      overall_failed=1
      [[ "${CONTINUE_ON_FAILURE}" == "1" ]] || break
    }
  fi
  if [[ "${RUN_INGESTION}" == "1" ]]; then
    run_case "ingestion" "${iteration}" run_ingestion_gate || {
      overall_failed=1
      [[ "${CONTINUE_ON_FAILURE}" == "1" ]] || break
    }
  fi
  if [[ "${RUN_REDIS}" == "1" ]]; then
    run_case "redis" "${iteration}" run_redis_gate "${iteration}" || {
      overall_failed=1
      [[ "${CONTINUE_ON_FAILURE}" == "1" ]] || break
    }
  fi
  if [[ "${RUN_RAFT}" == "1" ]]; then
    run_case "raft" "${iteration}" run_raft_gate "${iteration}" || {
      overall_failed=1
      [[ "${CONTINUE_ON_FAILURE}" == "1" ]] || break
    }
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
  log "PASS TemporalStore production readiness local gate"
else
  log "FAIL TemporalStore production readiness local gate"
fi
exit "${overall_failed}"
