#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_TYPE="${BUILD_TYPE:-Release}"
BUILD_FLAVOR="$(printf '%s' "${BUILD_TYPE}" | tr '[:upper:]' '[:lower:]')"
OUT_DIR="${OUT_DIR:-${ROOT}/output-ubuntu22/${BUILD_FLAVOR}}"
BIN_DIR="${BIN_DIR:-${ROOT}/build-ubuntu22/${BUILD_FLAVOR}/src/client/example}"
RESULT_DIR="${RESULT_DIR:-/tmp/temporalstore-prometheus-local-$(date +%Y%m%d_%H%M%S)}"
CLUSTER_NAME="${CLUSTER_NAME:-prometheus_validation}"
NAMESPACE_NAME="${NAMESPACE_NAME:-prom_ns}"
TABLE_NAME="${TABLE_NAME:-prom_table}"
IDC="${IDC:-vdc1}"
MS_PORT="${MS_PORT:-18000}"
MS_RAFT_PORT="${MS_RAFT_PORT:-18010}"
MS_SNAPSHOT_PORT="${MS_SNAPSHOT_PORT:-18020}"
SERVER_PORT="${SERVER_PORT:-18001}"
PROXY_PORT="${PROXY_PORT:-18090}"
ITERATIONS="${ITERATIONS:-2}"
STRING_OPS="${STRING_OPS:-200}"
BENCH_TIMEOUT_S="${BENCH_TIMEOUT_S:-60}"
RUN_CLIENT_SCALE="${RUN_CLIENT_SCALE:-0}"
THREAD_LIST="${THREAD_LIST:-2 4}"
PROXY_BACKEND_IO_TIMEOUT_MS="${PROXY_BACKEND_IO_TIMEOUT_MS:-30000}"
PROXY_BACKEND_CONNECT_TIMEOUT_MS="${PROXY_BACKEND_CONNECT_TIMEOUT_MS:-5000}"
PROXY_SMOKE_TIMEOUT_MS="${PROXY_SMOKE_TIMEOUT_MS:-30000}"
PROM_DIR="${ROOT}/tools/temporalstore-prometheus"
START_PROMETHEUS="${START_PROMETHEUS:-1}"
if [[ "${START_PROMETHEUS}" == "1" ]]; then
  TEXTFILE_DIR="${TEXTFILE_DIR:-${PROM_DIR}/vars-exporter/metrics}"
else
  TEXTFILE_DIR="${TEXTFILE_DIR:-${RESULT_DIR}/metrics}"
fi
PROM_FILE="${TEXTFILE_DIR}/temporalstore-vars.prom"
CLIENT_FILE="${TEXTFILE_DIR}/temporalstore-client.prom"
CLIENT_RETRY_ATTEMPTS=0
CLIENT_RETRY_FAILURES=0
PROXY_RETRY_ATTEMPTS=0
PROXY_RETRY_FAILURES=0

mkdir -p "${RESULT_DIR}" "${TEXTFILE_DIR}"

require_exec() {
  local path="$1"
  if [[ ! -x "${path}" ]]; then
    echo "missing executable: ${path}" >&2
    exit 1
  fi
}

require_exec "${OUT_DIR}/bcache2-server"
require_exec "${OUT_DIR}/bcache2-metaserver"
require_exec "${BIN_DIR}/string_scale_benchmark"
HAS_PROXY=0
if [[ -x "${OUT_DIR}/bcache2-proxy" && -x "${BIN_DIR}/proxy_smoke_example" ]]; then
  HAS_PROXY=1
fi

cleanup_temporalstore() {
  DEPLOY_DIR="${RESULT_DIR}/deploy" \
  CLUSTER_NAME="${CLUSTER_NAME}" \
  MS_PORT="${MS_PORT}" \
  SERVER_PORT="${SERVER_PORT}" \
  "${ROOT}/tools/deploy_local_ubuntu22.sh" stop >/dev/null 2>&1 || true
  if [[ -f "${RESULT_DIR}/proxy.pid" ]]; then
    kill "$(cat "${RESULT_DIR}/proxy.pid")" >/dev/null 2>&1 || true
  fi
  pkill -f "bcache2-proxy.*proxy_cluster_name=${CLUSTER_NAME}" >/dev/null 2>&1 || true
}

trap cleanup_temporalstore EXIT
cleanup_temporalstore

echo "Starting local TemporalStore cluster" | tee "${RESULT_DIR}/summary.txt"
BUILD_TYPE="${BUILD_TYPE}" \
OUT_DIR="${OUT_DIR}" \
DEPLOY_DIR="${RESULT_DIR}/deploy" \
CLUSTER_NAME="${CLUSTER_NAME}" \
NAMESPACE_NAME="${NAMESPACE_NAME}" \
TABLE_NAME="${TABLE_NAME}" \
MS_PORT="${MS_PORT}" \
MS_RAFT_PORT="${MS_RAFT_PORT}" \
MS_SNAPSHOT_PORT="${MS_SNAPSHOT_PORT}" \
SERVER_PORT="${SERVER_PORT}" \
SERVER_COUNT=1 \
REPLICA_COUNT=1 \
bash "${ROOT}/tools/deploy_local_ubuntu22.sh" start | tee "${RESULT_DIR}/deploy.out"

leader="$(awk '/metaserver leader:/ {print $3}' "${RESULT_DIR}/deploy/launcher.log" | tail -n 1)"
if [[ -z "${leader}" ]]; then
  echo "could not parse metaserver leader" >&2
  cat "${RESULT_DIR}/deploy/launcher.log" >&2 || true
  exit 1
fi
sleep 3

if [[ "${HAS_PROXY}" == "1" ]]; then
  echo "Starting proxy at 127.0.0.1:${PROXY_PORT}" | tee -a "${RESULT_DIR}/summary.txt"
  mkdir -p "${RESULT_DIR}/proxy-log"
  (
    cd "${ROOT}"
    env BYTED_HOST_IP=127.0.0.1 BYTED_HOST_IPV6= \
      "${OUT_DIR}/bcache2-proxy" \
        --port="${PROXY_PORT}" \
        --master_endpoint="${leader}" \
        --idc="${IDC}" \
        --proxy_cluster_name="${CLUSTER_NAME}" \
        --proxy_vregion=local \
        --proxy_vdc="${IDC}" \
        --proxy_vau=local \
        --proxy_log_dir="${RESULT_DIR}/proxy-log" \
        --proxy_log_level=2 \
        --proxy_backend_io_timeout_ms="${PROXY_BACKEND_IO_TIMEOUT_MS}" \
        --proxy_backend_connect_timeout_ms="${PROXY_BACKEND_CONNECT_TIMEOUT_MS}" \
        --proxy_pin_primary_reads=false
  ) > "${RESULT_DIR}/proxy.out" 2> "${RESULT_DIR}/proxy.err" &
  echo "$!" > "${RESULT_DIR}/proxy.pid"

  for _ in $(seq 1 60); do
    if curl -fsS -m 1 "http://127.0.0.1:${PROXY_PORT}/vars" >/dev/null 2>&1; then
      break
    fi
    sleep 0.5
  done
  curl -fsS -m 2 "http://127.0.0.1:${PROXY_PORT}/vars" >/dev/null
else
  echo "Proxy artifact missing; skipping proxy live target validation" | tee -a "${RESULT_DIR}/summary.txt"
fi

write_client_metric() {
  local iteration="$1"
  local threads="$2"
  local phase="$3"
  local qps="$4"
  local errors="$5"
  {
    echo "temporalstore_client_benchmark_qps{service_role=\"client\",phase=\"${phase}\",threads=\"${threads}\",iteration=\"${iteration}\"} ${qps}"
    echo "temporalstore_client_benchmark_errors{service_role=\"client\",phase=\"${phase}\",threads=\"${threads}\",iteration=\"${iteration}\"} ${errors}"
  } >> "${CLIENT_FILE}"
}

write_retry_metric() {
  local metric="$1"
  local value="$2"
  local service_role="$3"
  local phase="$4"
  local iteration="$5"
  local threads="${6:-}"
  if [[ -n "${threads}" ]]; then
    echo "${metric}{service_role=\"${service_role}\",phase=\"${phase}\",iteration=\"${iteration}\",threads=\"${threads}\"} ${value}" >> "${CLIENT_FILE}"
  else
    echo "${metric}{service_role=\"${service_role}\",phase=\"${phase}\",iteration=\"${iteration}\"} ${value}" >> "${CLIENT_FILE}"
  fi
}

write_client_validation_metric() {
  local iteration="$1"
  echo "temporalstore_client_validation_up{service_role=\"client\",iteration=\"${iteration}\"} 1" \
    >> "${CLIENT_FILE}"
}

write_proxy_metric() {
  local metric="$1"
  local value="$2"
  local iteration="${3:-}"
  if [[ -n "${iteration}" ]]; then
    echo "${metric}{service_role=\"proxy\",iteration=\"${iteration}\"} ${value}" >> "${CLIENT_FILE}"
  else
    echo "${metric}{service_role=\"proxy\"} ${value}" >> "${CLIENT_FILE}"
  fi
}

require_metric() {
  local pattern="$1"
  local file="$2"
  local label="$3"
  if [[ ! -f "${file}" ]]; then
    echo "missing metrics file while checking ${label}: ${file}" >&2
    return 1
  fi
  if ! grep -q "${pattern}" "${file}"; then
    echo "missing metric ${label} in ${file}" >&2
    return 1
  fi
}

warn_if_missing_metric() {
  local pattern="$1"
  local file="$2"
  local label="$3"
  if [[ ! -f "${file}" ]]; then
    echo "missing metrics file while checking optional ${label}: ${file}" \
      >> "${RESULT_DIR}/summary.txt"
    return 0
  fi
  if ! grep -q "${pattern}" "${file}"; then
    echo "optional metric ${label} not present in this smoke run" \
      >> "${RESULT_DIR}/summary.txt"
  fi
}

scrape_and_validate_metrics() {
  local iteration="$1"
  local attempt
  local code=1
  for attempt in $(seq 1 "${PROMETHEUS_SCRAPE_ATTEMPTS:-5}"); do
    set +e
    python3 "${PROM_DIR}/vars-exporter/vars_to_prom.py" \
      --targets "${host_targets}" \
      --interval 1 \
      --output-dir "${TEXTFILE_DIR}" \
      --output-file "temporalstore-vars.prom" \
      --timeout 3 \
      --once > "${RESULT_DIR}/vars_exporter_${iteration}_${attempt}.out" \
      2> "${RESULT_DIR}/vars_exporter_${iteration}_${attempt}.err"
    code=$?
    set -e

    if [[ "${code}" == "0" ]] && \
       require_metric 'temporalstore_service_role_up{service_role="nodeserver"' \
         "${PROM_FILE}" "nodeserver role up" && \
       require_metric 'temporalstore_service_role_up{service_role="metaserver"' \
         "${PROM_FILE}" "metaserver role up" && \
       require_metric 'temporalstore_vars_exporter_target_samples_scraped{service_role="nodeserver"' \
         "${PROM_FILE}" "nodeserver sample count" && \
       require_metric 'temporalstore_client_validation_up' \
         "${CLIENT_FILE}" "client validation" && \
       require_metric 'temporalstore_client_retry_attempts_total' \
         "${CLIENT_FILE}" "client retry attempts" && \
       require_metric 'temporalstore_client_retry_failures_total' \
         "${CLIENT_FILE}" "client retry failures" && \
       require_metric 'temporalstore_proxy_retry_attempts_total' \
         "${CLIENT_FILE}" "proxy retry attempts" && \
       require_metric 'temporalstore_proxy_retry_failures_total' \
         "${CLIENT_FILE}" "proxy retry failures" && \
       require_metric 'temporalstore_proxy_artifact_present' \
         "${CLIENT_FILE}" "proxy artifact metric"; then
      if [[ "${HAS_PROXY}" == "1" ]]; then
        require_metric 'temporalstore_service_role_up{service_role="proxy"' \
          "${PROM_FILE}" "proxy role up" || code=$?
      fi
      if [[ "${code}" == "0" ]]; then
        warn_if_missing_metric 'bcache2_server_partition_page_store_persistent_read_qps' \
          "${PROM_FILE}" "page-store persistent read metric"
        return 0
      fi
    fi

    echo "metrics scrape attempt ${attempt} failed for iteration ${iteration}; retrying" \
      >> "${RESULT_DIR}/summary.txt"
    sleep 1
  done

  echo "metrics scrape failed after ${PROMETHEUS_SCRAPE_ATTEMPTS:-5} attempts" >&2
  for attempt in $(seq 1 "${PROMETHEUS_SCRAPE_ATTEMPTS:-5}"); do
    echo "== vars exporter attempt ${attempt} stdout ==" >&2
    cat "${RESULT_DIR}/vars_exporter_${iteration}_${attempt}.out" >&2 2>/dev/null || true
    echo "== vars exporter attempt ${attempt} stderr ==" >&2
    cat "${RESULT_DIR}/vars_exporter_${iteration}_${attempt}.err" >&2 2>/dev/null || true
  done
  [[ -f "${PROM_FILE}" ]] && tail -120 "${PROM_FILE}" >&2 || true
  [[ -f "${CLIENT_FILE}" ]] && tail -120 "${CLIENT_FILE}" >&2 || true
  return 1
}

rm -f "${PROM_FILE}" "${CLIENT_FILE}"
{
  echo "# HELP temporalstore_client_validation_up Client validation status from local smoke deployment."
  echo "# TYPE temporalstore_client_validation_up gauge"
  echo "# HELP temporalstore_client_benchmark_qps Client benchmark QPS from local validation."
  echo "# TYPE temporalstore_client_benchmark_qps gauge"
  echo "# HELP temporalstore_client_benchmark_errors Client benchmark errors from local validation."
  echo "# TYPE temporalstore_client_benchmark_errors gauge"
  echo "# HELP temporalstore_client_retry_attempts_total Client-side retry attempts observed by local validation."
  echo "# TYPE temporalstore_client_retry_attempts_total counter"
  echo "# HELP temporalstore_client_retry_failures_total Client-side retries that still failed after retry budget."
  echo "# TYPE temporalstore_client_retry_failures_total counter"
  echo "# HELP temporalstore_proxy_retry_attempts_total Proxy smoke retry attempts observed by local validation."
  echo "# TYPE temporalstore_proxy_retry_attempts_total counter"
  echo "# HELP temporalstore_proxy_retry_failures_total Proxy smoke retries that still failed after retry budget."
  echo "# TYPE temporalstore_proxy_retry_failures_total counter"
  echo "# HELP temporalstore_proxy_artifact_present Whether the local proxy binary and smoke client are present."
  echo "# TYPE temporalstore_proxy_artifact_present gauge"
  echo "# HELP temporalstore_proxy_validation_up Whether the proxy process was live during validation."
  echo "# TYPE temporalstore_proxy_validation_up gauge"
  echo "# HELP temporalstore_proxy_smoke_success Whether proxy smoke succeeded for an iteration."
  echo "# TYPE temporalstore_proxy_smoke_success gauge"
} > "${CLIENT_FILE}"
write_proxy_metric "temporalstore_proxy_artifact_present" "${HAS_PROXY}"
targets="nodeserver=http://host.docker.internal:${SERVER_PORT}/vars,metaserver=http://host.docker.internal:${MS_PORT}/vars"
host_targets="nodeserver=http://127.0.0.1:${SERVER_PORT}/vars,metaserver=http://127.0.0.1:${MS_PORT}/vars"
if [[ "${HAS_PROXY}" == "1" ]]; then
  targets+=",proxy=http://host.docker.internal:${PROXY_PORT}/vars"
  host_targets+=",proxy=http://127.0.0.1:${PROXY_PORT}/vars"
fi

for iteration in $(seq 1 "${ITERATIONS}"); do
  if [[ "${HAS_PROXY}" == "1" ]]; then
    echo "Iteration ${iteration}: proxy smoke" | tee -a "${RESULT_DIR}/summary.txt"
    write_proxy_metric "temporalstore_proxy_validation_up" 1 "${iteration}"
    proxy_smoke_code=1
    proxy_attempts=0
    proxy_failed_attempts=0
    set +e
    for attempt in $(seq 1 "${PROXY_SMOKE_ATTEMPTS:-3}"); do
      proxy_attempts="${attempt}"
      PROXY_SMOKE_TIMEOUT_MS="${PROXY_SMOKE_TIMEOUT_MS}" \
        "${BIN_DIR}/proxy_smoke_example" "127.0.0.1:${PROXY_PORT}" "${NAMESPACE_NAME}" "${TABLE_NAME}" \
        "prom_proxy_${iteration}" > "${RESULT_DIR}/proxy_smoke_${iteration}_${attempt}.out" \
        2> "${RESULT_DIR}/proxy_smoke_${iteration}_${attempt}.err"
      proxy_smoke_code=$?
      [[ "${proxy_smoke_code}" == "0" ]] && break
      proxy_failed_attempts=$((proxy_failed_attempts + 1))
      echo "proxy_smoke attempt ${attempt} failed; retrying" >> "${RESULT_DIR}/summary.txt"
      sleep 1
    done
    set -e
    proxy_retries=$((proxy_attempts > 0 ? proxy_attempts - 1 : 0))
    PROXY_RETRY_ATTEMPTS=$((PROXY_RETRY_ATTEMPTS + proxy_retries))
    if [[ "${proxy_smoke_code}" != "0" ]]; then
      PROXY_RETRY_FAILURES=$((PROXY_RETRY_FAILURES + 1))
    fi
    write_retry_metric "temporalstore_proxy_retry_attempts_total" "${PROXY_RETRY_ATTEMPTS}" \
      "proxy" "proxy_smoke" "${iteration}"
    write_retry_metric "temporalstore_proxy_retry_failures_total" "${PROXY_RETRY_FAILURES}" \
      "proxy" "proxy_smoke" "${iteration}"
    if [[ "${proxy_smoke_code}" == "0" ]]; then
      write_proxy_metric "temporalstore_proxy_smoke_success" 1 "${iteration}"
    else
      write_proxy_metric "temporalstore_proxy_smoke_success" 0 "${iteration}"
      cat "${RESULT_DIR}"/proxy_smoke_"${iteration}"_*.out >&2 || true
      cat "${RESULT_DIR}"/proxy_smoke_"${iteration}"_*.err >&2 || true
      exit "${proxy_smoke_code}"
    fi
  else
    write_proxy_metric "temporalstore_proxy_validation_up" 0 "${iteration}"
    write_proxy_metric "temporalstore_proxy_smoke_success" 0 "${iteration}"
    write_retry_metric "temporalstore_proxy_retry_attempts_total" "${PROXY_RETRY_ATTEMPTS}" \
      "proxy" "proxy_smoke" "${iteration}"
    write_retry_metric "temporalstore_proxy_retry_failures_total" "${PROXY_RETRY_FAILURES}" \
      "proxy" "proxy_smoke" "${iteration}"
  fi

  write_client_validation_metric "${iteration}"
  if [[ "${RUN_CLIENT_SCALE}" != "1" ]]; then
    write_retry_metric "temporalstore_client_retry_attempts_total" "${CLIENT_RETRY_ATTEMPTS}" \
      "client" "validation" "${iteration}"
    write_retry_metric "temporalstore_client_retry_failures_total" "${CLIENT_RETRY_FAILURES}" \
      "client" "validation" "${iteration}"
  fi
  if [[ "${RUN_CLIENT_SCALE}" == "1" ]]; then
    for threads in ${THREAD_LIST}; do
      echo "Iteration ${iteration}: client scale threads=${threads}" | tee -a "${RESULT_DIR}/summary.txt"
      out="${RESULT_DIR}/string_scale_${iteration}_${threads}.out"
      code=1
      client_attempts=0
      for attempt in $(seq 1 5); do
        client_attempts="${attempt}"
        set +e
        timeout "${BENCH_TIMEOUT_S}" "${BIN_DIR}/string_scale_benchmark" \
          "127.0.0.1:${MS_PORT}" "${IDC}" "${NAMESPACE_NAME}" \
          "${TABLE_NAME}" "${STRING_OPS}" "${threads}" 128 1 1000 both > "${out}" 2>&1
        code=$?
        set -e
        [[ "${code}" == "0" ]] && break
        echo "string_scale attempt ${attempt} failed; retrying" >> "${out}"
        sleep 2
      done
      client_retries=$((client_attempts > 0 ? client_attempts - 1 : 0))
      CLIENT_RETRY_ATTEMPTS=$((CLIENT_RETRY_ATTEMPTS + client_retries))
      if [[ "${code}" != "0" ]]; then
        CLIENT_RETRY_FAILURES=$((CLIENT_RETRY_FAILURES + 1))
      fi
      visibility_retries="$(grep -E '^get_retry_attempts=' "${out}" | tail -n 1 | cut -d= -f2 || true)"
      if [[ "${visibility_retries}" =~ ^[0-9]+$ ]]; then
        CLIENT_RETRY_ATTEMPTS=$((CLIENT_RETRY_ATTEMPTS + visibility_retries))
      fi
      write_retry_metric "temporalstore_client_retry_attempts_total" "${CLIENT_RETRY_ATTEMPTS}" \
        "client" "string_scale" "${iteration}" "${threads}"
      write_retry_metric "temporalstore_client_retry_failures_total" "${CLIENT_RETRY_FAILURES}" \
        "client" "string_scale" "${iteration}" "${threads}"
      if [[ "${code}" != "0" ]]; then
        cat "${out}" >&2
        exit "${code}"
      fi
      awk -F, -v iteration="${iteration}" -v threads="${threads}" '
        NR > 1 && $1 == "TemporalStore" {
          printf "%s,%s,%s,%s,%s\n", iteration, threads, $2, $7, $6
        }' "${out}" | while IFS=, read -r iter th phase qps errors; do
          write_client_metric "${iter}" "${th}" "${phase}" "${qps:-0}" "${errors:-0}"
        done
    done
  fi

  scrape_and_validate_metrics "${iteration}"
done

if [[ "${START_PROMETHEUS}" == "1" ]]; then
  echo "Starting local Prometheus at http://localhost:9090" | tee -a "${RESULT_DIR}/summary.txt"
  compose_cmd=(docker compose)
  if ! docker compose version >/dev/null 2>&1; then
    compose_cmd=(docker-compose)
  fi
  docker rm -f temporalstore-node-exporter temporalstore-vars-exporter temporalstore-prometheus \
    >/dev/null 2>&1 || true
  (
    cd "${PROM_DIR}"
    VARS_TARGETS="${targets}" \
    VARS_INTERVAL_SECONDS=5 \
    VARS_OUTPUT_DIR=/var/lib/node_exporter/textfile_collector \
    VARS_OUTPUT_FILE=temporalstore-vars.prom \
    "${compose_cmd[@]}" up -d --force-recreate vars-exporter node-exporter prometheus
  ) | tee "${RESULT_DIR}/docker_compose.out"

  for _ in $(seq 1 60); do
    if curl -fsS -m 2 "http://127.0.0.1:9090/-/ready" >/dev/null 2>&1; then
      break
    fi
    sleep 1
  done
  curl -fsS -m 3 "http://127.0.0.1:9090/-/ready" >/dev/null
  python3 - <<'PY'
import json
import time
import urllib.parse
import urllib.request

queries = [
    'temporalstore_service_role_up',
    'temporalstore_client_validation_up',
]
deadline = time.monotonic() + 90
pending = set(queries)
last = {}
while pending and time.monotonic() < deadline:
    for query in list(pending):
        url = 'http://127.0.0.1:9090/api/v1/query?' + urllib.parse.urlencode({'query': query})
        with urllib.request.urlopen(url, timeout=5) as response:
            payload = json.loads(response.read().decode('utf-8'))
        last[query] = payload
        if payload.get('status') == 'success' and payload.get('data', {}).get('result'):
            pending.remove(query)
    if pending:
        time.sleep(3)
if pending:
    raise SystemExit(f'Prometheus query returned no data: {sorted(pending)} last={last}')
PY
fi

{
  echo "PASS TemporalStore local Prometheus validation"
  echo "result_dir=${RESULT_DIR}"
  echo "iterations=${ITERATIONS}"
  echo "proxy_live_validation=${HAS_PROXY}"
  echo "prometheus=http://localhost:9090"
  echo "node_exporter=http://localhost:9100/metrics"
  echo "metrics_file=${PROM_FILE}"
  echo "client_metrics_file=${CLIENT_FILE}"
} | tee -a "${RESULT_DIR}/summary.txt"
