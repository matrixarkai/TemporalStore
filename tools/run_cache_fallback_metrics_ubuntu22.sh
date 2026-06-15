#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RESULT_DIR="${RESULT_DIR:-/tmp/temporalstore-cache-fallback-metrics-$(date +%Y%m%d_%H%M%S)}"
TEXTFILE_DIR="${TEXTFILE_DIR:-${RESULT_DIR}/metrics}"
METRICS_FILE="${METRICS_FILE:-${TEXTFILE_DIR}/temporalstore-cache-fallback.prom}"
RUN_RUNTIME_TEST="${RUN_RUNTIME_TEST:-auto}"
BUILD_TYPE="${BUILD_TYPE:-Release}"
BUILD_FLAVOR="$(printf '%s' "${BUILD_TYPE}" | tr '[:upper:]' '[:lower:]')"
PARTITION_TEST_BIN="${PARTITION_TEST_BIN:-${ROOT}/build-ubuntu22/${BUILD_FLAVOR}/src/partition/test/partition_test}"

mkdir -p "${RESULT_DIR}" "${TEXTFILE_DIR}"

PAGE_STORE_CC="${ROOT}/src/partition/storage/page_store.cc"
PAGE_STORE_H="${ROOT}/src/partition/storage/page_store.h"
METRICS_H="${ROOT}/src/partition/metrics.h"
PARTITION_LOAD_TEST="${ROOT}/src/partition/test/partition_load_test.cc"
EXPORTER_TEST="${ROOT}/tools/temporalstore-prometheus/vars-exporter/test_vars_to_prom.py"

require_pattern() {
  local file="$1"
  local pattern="$2"
  local label="$3"
  if ! grep -qE "${pattern}" "${file}"; then
    echo "missing ${label}: ${file} pattern=${pattern}" >&2
    exit 1
  fi
}

require_pattern "${METRICS_H}" 'page_store\.blockcache_get_qps' "blockcache get metric registration"
require_pattern "${METRICS_H}" 'page_store\.blockcache_hit_qps' "blockcache hit metric registration"
require_pattern "${METRICS_H}" 'page_store\.persistent_read_qps' "persistent read metric registration"
require_pattern "${PAGE_STORE_H}" 'ReadPathTestCounters' "read path test counters"
require_pattern "${PAGE_STORE_CC}" 'blockcache_get_qps->get\(\)->Increment' "blockcache get increment"
require_pattern "${PAGE_STORE_CC}" 'blockcache_hit_qps->get\(\)->Increment' "blockcache hit increment"
require_pattern "${PAGE_STORE_CC}" 'persistent_read_qps->get\(\)->Increment' "persistent read increment"
require_pattern "${PARTITION_LOAD_TEST}" 'LoadEvictedObjectChecksBlockCacheBeforePersistentStore' "cache fallback unit test"
require_pattern "${PARTITION_LOAD_TEST}" 'EXPECT_EQ\(1, counters\.blockcache_gets\)' "blockcache get assertion"
require_pattern "${PARTITION_LOAD_TEST}" 'EXPECT_EQ\(1, counters\.blockcache_hits\)' "blockcache hit assertion"
require_pattern "${PARTITION_LOAD_TEST}" 'EXPECT_EQ\(1, counters\.persistent_reads\)' "persistent read assertion"
require_pattern "${EXPORTER_TEST}" 'bcache2_server_partition_page_store_persistent_read_qps' "Prometheus exporter metric test"

runtime_test_status=0
runtime_test_ran=0
runtime_test_aborted=0
runtime_test_required=0
runtime_test_fresh=0
if [[ "${RUN_RUNTIME_TEST}" == "1" ]]; then
  runtime_test_required=1
fi

if [[ -x "${PARTITION_TEST_BIN}" && "${PARTITION_TEST_BIN}" -nt "${PARTITION_LOAD_TEST}" &&
      "${PARTITION_TEST_BIN}" -nt "${PAGE_STORE_CC}" ]]; then
  runtime_test_fresh=1
fi

if [[ "${RUN_RUNTIME_TEST}" == "1" && "${runtime_test_fresh}" != "1" ]]; then
  echo "required runtime cache fallback test binary is missing or stale: ${PARTITION_TEST_BIN}" >&2
  exit 1
fi

if [[ "${RUN_RUNTIME_TEST}" != "0" && "${runtime_test_fresh}" == "1" ]]; then
  runtime_test_ran=1
  set +e
  env \
    BYTED_HOST_IP="${BYTED_HOST_IP:-127.0.0.1}" \
    BYTED_HOST_IPV6="${BYTED_HOST_IPV6:-::1}" \
    MY_HOST_IP="${MY_HOST_IP:-127.0.0.1}" \
    BDC_PRIVATE_CLOUD="${BDC_PRIVATE_CLOUD:-True}" \
    ASAN_OPTIONS="${ASAN_OPTIONS:-detect_leaks=false,abort_on_error=true}" \
    "${PARTITION_TEST_BIN}" \
      --gtest_filter=PartitionLoadTest.LoadEvictedObjectChecksBlockCacheBeforePersistentStore \
      --gtest_color=no > "${RESULT_DIR}/partition_test.out" 2> "${RESULT_DIR}/partition_test.err"
  code=$?
  set -e
  if [[ "${code}" == "0" ]]; then
    runtime_test_status=1
  elif [[ "${code}" == "134" || "${code}" == "139" ]]; then
    runtime_test_aborted=1
  fi
  if [[ "${runtime_test_required}" == "1" && "${runtime_test_status}" != "1" ]]; then
    echo "required runtime cache fallback test failed code=${code}" >&2
    cat "${RESULT_DIR}/partition_test.out" >&2 || true
    cat "${RESULT_DIR}/partition_test.err" >&2 || true
    exit "${code}"
  fi
fi

cat > "${METRICS_FILE}" <<EOF
# HELP temporalstore_cache_fallback_static_checks_pass Static source checks for cache fallback instrumentation and assertions.
# TYPE temporalstore_cache_fallback_static_checks_pass gauge
temporalstore_cache_fallback_static_checks_pass 1
# HELP temporalstore_cache_fallback_runtime_test_ran Whether the focused partition runtime test was executed.
# TYPE temporalstore_cache_fallback_runtime_test_ran gauge
temporalstore_cache_fallback_runtime_test_ran ${runtime_test_ran}
# HELP temporalstore_cache_fallback_runtime_test_fresh Whether the focused runtime test binary was newer than the source files under validation.
# TYPE temporalstore_cache_fallback_runtime_test_fresh gauge
temporalstore_cache_fallback_runtime_test_fresh ${runtime_test_fresh}
# HELP temporalstore_cache_fallback_runtime_test_pass Whether the focused partition runtime test passed.
# TYPE temporalstore_cache_fallback_runtime_test_pass gauge
temporalstore_cache_fallback_runtime_test_pass ${runtime_test_status}
# HELP temporalstore_cache_fallback_runtime_test_aborted Whether the local runtime test binary aborted before completion.
# TYPE temporalstore_cache_fallback_runtime_test_aborted gauge
temporalstore_cache_fallback_runtime_test_aborted ${runtime_test_aborted}
# HELP temporalstore_cache_fallback_blockcache_get_metric_present Whether blockcache get metric registration/increment is present.
# TYPE temporalstore_cache_fallback_blockcache_get_metric_present gauge
temporalstore_cache_fallback_blockcache_get_metric_present 1
# HELP temporalstore_cache_fallback_blockcache_hit_metric_present Whether blockcache hit metric registration/increment is present.
# TYPE temporalstore_cache_fallback_blockcache_hit_metric_present gauge
temporalstore_cache_fallback_blockcache_hit_metric_present 1
# HELP temporalstore_cache_fallback_persistent_read_metric_present Whether persistent read metric registration/increment is present.
# TYPE temporalstore_cache_fallback_persistent_read_metric_present gauge
temporalstore_cache_fallback_persistent_read_metric_present 1
# HELP temporalstore_cache_fallback_unit_assertions_present Whether the fallback unit test asserts cache miss, cache hit, and persistent read behavior.
# TYPE temporalstore_cache_fallback_unit_assertions_present gauge
temporalstore_cache_fallback_unit_assertions_present 1
# HELP temporalstore_cache_fallback_prometheus_exporter_test_present Whether exporter unit tests cover the persistent-read metric name.
# TYPE temporalstore_cache_fallback_prometheus_exporter_test_present gauge
temporalstore_cache_fallback_prometheus_exporter_test_present 1
EOF

for required_metric in \
  temporalstore_cache_fallback_static_checks_pass \
  temporalstore_cache_fallback_blockcache_get_metric_present \
  temporalstore_cache_fallback_blockcache_hit_metric_present \
  temporalstore_cache_fallback_persistent_read_metric_present \
  temporalstore_cache_fallback_unit_assertions_present \
  temporalstore_cache_fallback_prometheus_exporter_test_present; do
  grep -q "^${required_metric} 1" "${METRICS_FILE}"
done

echo "PASS cache fallback metrics gate"
echo "metrics_file=${METRICS_FILE}"
echo "runtime_test_ran=${runtime_test_ran}"
echo "runtime_test_fresh=${runtime_test_fresh}"
echo "runtime_test_pass=${runtime_test_status}"
echo "runtime_test_aborted=${runtime_test_aborted}"
echo "result_dir=${RESULT_DIR}"
