#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_TYPE="${BUILD_TYPE:-Debug}"
BUILD_FLAVOR="$(printf '%s' "${BUILD_TYPE}" | tr '[:upper:]' '[:lower:]')"
BUILD_DIR="${BUILD_DIR:-${ROOT}/build-ubuntu22/${BUILD_FLAVOR}}"
RESULT_DIR="${RESULT_DIR:-/tmp/temporalstore-scale-$(date +%Y%m%d-%H%M%S)}"
META_COUNT="${META_COUNT:-2}"
SERVER_COUNT="${SERVER_COUNT:-2}"
REPLICA_COUNT="${REPLICA_COUNT:-2}"
MS_PORT="${MS_PORT:-18000}"
SERVER_PORT="${SERVER_PORT:-18001}"
NAMESPACE_NAME="${NAMESPACE_NAME:-deploy_ns}"
TABLE_NAME="${TABLE_NAME:-deploy_table}"
IDC="${IDC:-vdc1}"
CASES="${CASES:-50000:32:128:1:1000 50000:64:128:1:1000 50000:32:1024:1:1000 50000:32:128:0:2000}"
REDIS_PORT="${REDIS_PORT:-6379}"
REDIS_MODES="${REDIS_MODES:-none everysec always}"

BENCH="${BUILD_DIR}/src/client/example/string_scale_benchmark"
if [[ ! -x "${BENCH}" ]]; then
  echo "missing benchmark binary: ${BENCH}" >&2
  exit 1
fi

if ! command -v redis-server >/dev/null 2>&1; then
  echo "missing redis-server; install redis-server first" >&2
  exit 1
fi
if ! command -v redis-benchmark >/dev/null 2>&1; then
  echo "missing redis-benchmark; install redis-tools first" >&2
  exit 1
fi

mkdir -p "${RESULT_DIR}"

stop_redis() {
  redis-cli -p "${REDIS_PORT}" shutdown nosave >/dev/null 2>&1 || true
}

start_redis() {
  local mode="$1"
  local dir="${RESULT_DIR}/redis-${mode}"
  stop_redis
  rm -rf "${dir}"
  mkdir -p "${dir}"
  case "${mode}" in
    none)
      redis-server --port "${REDIS_PORT}" --bind 127.0.0.1 --dir "${dir}" \
        --appendonly no --save "" --daemonize yes
      ;;
    everysec|always)
      redis-server --port "${REDIS_PORT}" --bind 127.0.0.1 --dir "${dir}" \
        --appendonly yes --appendfsync "${mode}" --save "" --daemonize yes
      ;;
    *)
      echo "unknown redis mode: ${mode}" >&2
      exit 2
      ;;
  esac

  for _ in $(seq 1 50); do
    if redis-cli -p "${REDIS_PORT}" ping >/dev/null 2>&1; then
      return
    fi
    sleep 0.1
  done
  echo "redis did not become ready for mode ${mode}" >&2
  exit 1
}

echo "result_dir=${RESULT_DIR}"
echo "cases=${CASES}"

env META_COUNT="${META_COUNT}" SERVER_COUNT="${SERVER_COUNT}" REPLICA_COUNT="${REPLICA_COUNT}" \
  BUILD_TYPE="${BUILD_TYPE}" MS_PORT="${MS_PORT}" SERVER_PORT="${SERVER_PORT}" \
  bash "${ROOT}/tools/deploy_local_ubuntu22.sh" start

temporal_csv="${RESULT_DIR}/temporalstore.csv"
echo "case,system,phase,ops,threads,value_bytes,errors,qps,avg_us,p50_us,p95_us,p99_us,min_us,max_us,total_ms" \
  > "${temporal_csv}"

for case_spec in ${CASES}; do
  IFS=: read -r ops threads value_bytes pin_primary_reads replica_wait_ms <<<"${case_spec}"
  case_name="ops${ops}_c${threads}_v${value_bytes}_pin${pin_primary_reads}"
  out_file="${RESULT_DIR}/temporalstore_${case_name}.csv"
  echo "running TemporalStore ${case_name}"
  "${BENCH}" "127.0.0.1:${MS_PORT}" "${IDC}" "${NAMESPACE_NAME}" "${TABLE_NAME}" \
    "${ops}" "${threads}" "${value_bytes}" "${pin_primary_reads}" "${replica_wait_ms}" \
    > "${out_file}"
  awk -v c="${case_name}" 'BEGIN{FS=OFS=","} /^TemporalStore,/ {print c,$0}' "${out_file}" \
    >> "${temporal_csv}"
done

redis_csv="${RESULT_DIR}/redis.csv"
echo "case,mode,command,qps" > "${redis_csv}"
for mode in ${REDIS_MODES}; do
  start_redis "${mode}"
  for case_spec in ${CASES}; do
    IFS=: read -r ops threads value_bytes _pin_primary_reads _replica_wait_ms <<<"${case_spec}"
    case_name="ops${ops}_c${threads}_v${value_bytes}_pin${_pin_primary_reads}"
    out_file="${RESULT_DIR}/redis_${mode}_${case_name}.csv"
    echo "running Redis ${mode} ${case_name}"
    redis-benchmark -h 127.0.0.1 -p "${REDIS_PORT}" -t set,get \
      -n "${ops}" -c "${threads}" -d "${value_bytes}" --csv > "${out_file}"
    awk -v c="${case_name}" -v m="${mode}" 'BEGIN{FS=OFS=","} {gsub(/"/, "", $1); gsub(/"/, "", $2); print c,m,$1,$2}' \
      "${out_file}" >> "${redis_csv}"
  done
done
stop_redis

du -sh /tmp/temporalstore-deploy/runtime/storage "${RESULT_DIR}"/redis-* \
  > "${RESULT_DIR}/disk_usage.txt" 2>/dev/null || true

echo "wrote:"
echo "  ${temporal_csv}"
echo "  ${redis_csv}"
echo "  ${RESULT_DIR}/disk_usage.txt"
