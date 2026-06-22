#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_TYPE="${BUILD_TYPE:-Debug}"
BUILD_FLAVOR="$(printf '%s' "${BUILD_TYPE}" | tr '[:upper:]' '[:lower:]')"
BUILD_DIR="${BUILD_DIR:-${ROOT}/build-ubuntu22/${BUILD_FLAVOR}}"
OUT_DIR="${OUT_DIR:-${ROOT}/output-ubuntu22/${BUILD_FLAVOR}}"
RESULT_DIR="${RESULT_DIR:-/tmp/temporalstore-sdk-smoke-$(date +%Y%m%d-%H%M%S)}"
RUNTIME_DIR="${RUNTIME_DIR:-/tmp/temporalstore-sdk-smoke-runtime-$(date +%Y%m%d-%H%M%S)}"
MS_PORT="${MS_PORT:-18200}"
MS_RAFT_PORT="${MS_RAFT_PORT:-18210}"
MS_SNAPSHOT_PORT="${MS_SNAPSHOT_PORT:-18220}"
SERVER_PORT="${SERVER_PORT:-18201}"
CLUSTER_NAME="${CLUSTER_NAME:-sdkclient}"
NAMESPACE_NAME="${NAMESPACE_NAME:-sdk_ns}"
TABLE_NAME="${TABLE_NAME:-sdk_table}"
IDC="${IDC:-vdc1}"
META_COUNT="${META_COUNT:-2}"
SERVER_COUNT="${SERVER_COUNT:-2}"
REPLICA_COUNT="${REPLICA_COUNT:-2}"
WARMUP_SECONDS="${WARMUP_SECONDS:-5}"
RUN_CUSTOMER_EXAMPLES="${RUN_CUSTOMER_EXAMPLES:-0}"
RUN_PYTHON_SDK="${RUN_PYTHON_SDK:-0}"
RUN_PYTHON_DIRECT_STRESS="${RUN_PYTHON_DIRECT_STRESS:-0}"
RUN_CPP_PROXY_PARITY="${RUN_CPP_PROXY_PARITY:-0}"
PYTHON_DIRECT_STRESS_HASH_OPS="${PYTHON_DIRECT_STRESS_HASH_OPS:-2000}"
PYTHON_DIRECT_STRESS_FEATURE_KEYS="${PYTHON_DIRECT_STRESS_FEATURE_KEYS:-32}"
PYTHON_DIRECT_STRESS_FEATURE_POINTS_PER_KEY="${PYTHON_DIRECT_STRESS_FEATURE_POINTS_PER_KEY:-64}"
PYTHON_DIRECT_STRESS_VALUE_BYTES="${PYTHON_DIRECT_STRESS_VALUE_BYTES:-512}"
PYTHON_DIRECT_STRESS_REQUEST_TIMEOUT_MS="${PYTHON_DIRECT_STRESS_REQUEST_TIMEOUT_MS:-10000}"
PYTHON_DIRECT_STRESS_IO_TIMEOUT_MS="${PYTHON_DIRECT_STRESS_IO_TIMEOUT_MS:-5000}"
CPP_PROXY_PORT="${CPP_PROXY_PORT:-18780}"
CPP_PROXY_PRESSURE_OPS="${CPP_PROXY_PRESSURE_OPS:-200}"
CPP_PROXY_PRESSURE_THREADS="${CPP_PROXY_PRESSURE_THREADS:-2}"
CPP_PROXY_PRESSURE_VALUE_BYTES="${CPP_PROXY_PRESSURE_VALUE_BYTES:-128}"
CPP_PROXY_PRESSURE_VERIFY_TIMEOUT_MS="${CPP_PROXY_PRESSURE_VERIFY_TIMEOUT_MS:-20000}"
CPP_PROXY_PRESSURE_VERIFY_POLL_MS="${CPP_PROXY_PRESSURE_VERIFY_POLL_MS:-20}"
CPP_PROXY_PRESSURE_WRITE_RETRIES="${CPP_PROXY_PRESSURE_WRITE_RETRIES:-3}"
RUN_GO_SDK="${RUN_GO_SDK:-0}"
RUN_JAVA_SDK="${RUN_JAVA_SDK:-0}"
RUN_RUST_SDK="${RUN_RUST_SDK:-0}"
RUN_UNIFIED_TESTS="${RUN_UNIFIED_TESTS:-${RUN_RUST_UNIFIED_TESTS:-0}}"
RUN_RUST_UNIFIED_TESTS="${RUN_RUST_UNIFIED_TESTS:-0}"
RUST_UNIFIED_VALIDATE_ONLY="${RUST_UNIFIED_VALIDATE_ONLY:-0}"
RUST_UNIFIED_CORPUS="${RUST_UNIFIED_CORPUS:-${ROOT}/sdk/unified/temporalstore_unified_corpus.json}"
REQUIRE_FRESH_BINARIES="${REQUIRE_FRESH_BINARIES:-1}"
REQUIRE_NO_TEMPORALSTORE_PROCESSES="${REQUIRE_NO_TEMPORALSTORE_PROCESSES:-0}"
SDK_LIB_NAME="${SDK_LIB_NAME:-}"

if [[ -z "${SDK_LIB_NAME}" ]]; then
  if [[ -f "${OUT_DIR}/sdk/lib/libbcache2.so" ]]; then
    SDK_LIB_NAME="bcache2"
  elif [[ -f "${OUT_DIR}/sdk/lib/libbcache2d.so" ]]; then
    SDK_LIB_NAME="bcache2d"
  else
    SDK_LIB_NAME="bcache2"
  fi
fi

fail_if_stale_binary() {
  local binary="$1"
  local source="$2"
  if [[ ! -e "${binary}" ]]; then
    echo "missing binary: ${binary}" >&2
    exit 1
  fi
  if [[ ! -e "${source}" ]]; then
    echo "missing source freshness input: ${source}" >&2
    exit 1
  fi
  if [[ "${source}" -nt "${binary}" ]]; then
    echo "stale binary: ${binary}" >&2
    echo "  newer source: ${source}" >&2
    echo "  rebuild before running direct SDK benchmark gates" >&2
    exit 1
  fi
}

if [[ "${REQUIRE_NO_TEMPORALSTORE_PROCESSES}" == "1" ]]; then
  stale_processes="$(pgrep -af 'bcache2-(metaserver|server)' || true)"
  if [[ -n "${stale_processes}" ]]; then
    echo "existing TemporalStore server processes detected; stop them before benchmark gates:" >&2
    echo "${stale_processes}" >&2
    exit 1
  fi
fi

if [[ "${REQUIRE_FRESH_BINARIES}" == "1" ]]; then
  fail_if_stale_binary "${OUT_DIR}/bcache2-metaserver" "${ROOT}/src/metaserver_v2/main.cc"
  fail_if_stale_binary "${OUT_DIR}/bcache2-server" "${ROOT}/src/server/main.cc"
  fail_if_stale_binary "${OUT_DIR}/sdk/lib/lib${SDK_LIB_NAME}.so" "${ROOT}/src/client/temporalstore_client.cc"
  fail_if_stale_binary "${OUT_DIR}/sdk/lib/lib${SDK_LIB_NAME}.so" "${ROOT}/src/client/temporalstore_c_client.cc"
  if [[ "${RUN_CPP_PROXY_PARITY}" == "1" ]]; then
    fail_if_stale_binary "${OUT_DIR}/bcache2-proxy" "${ROOT}/src/proxy/service.cc"
    fail_if_stale_binary "${BUILD_DIR}/src/client/example/proxy_smoke_example" \
      "${ROOT}/src/client/example/proxy_smoke_example.cc"
    fail_if_stale_binary "${BUILD_DIR}/src/client/example/proxy_ingestion_pressure_example" \
      "${ROOT}/src/client/example/proxy_ingestion_pressure_example.cc"
  fi
fi

mkdir -p "${RESULT_DIR}"
runtime_dir="${RUNTIME_DIR}"
launcher_log="${RESULT_DIR}/launcher.log"
launcher_pid=""
proxy_pid=""

cleanup() {
  if [[ -n "${proxy_pid}" ]] && kill -0 "${proxy_pid}" >/dev/null 2>&1; then
    kill "${proxy_pid}" >/dev/null 2>&1 || true
  fi
  for pid_file in "${runtime_dir}"/server*.pid "${runtime_dir}"/metaserver*.pid; do
    [[ -f "${pid_file}" ]] || continue
    kill "$(cat "${pid_file}")" >/dev/null 2>&1 || true
  done
  if [[ -n "${launcher_pid}" ]] && kill -0 "${launcher_pid}" >/dev/null 2>&1; then
    kill "${launcher_pid}" >/dev/null 2>&1 || true
  fi
  pkill -f "bcache2-metaserver.*metaserver_cluster_name=${CLUSTER_NAME}" >/dev/null 2>&1 || true
  pkill -f "bcache2-server.*cluster_name=${CLUSTER_NAME}" >/dev/null 2>&1 || true
  pkill -f "bcache2-proxy.*proxy_cluster_name=${CLUSTER_NAME}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

KEEP_RUNNING=1 \
OUT_DIR="${OUT_DIR}" \
SMOKE_DIR="${runtime_dir}" \
CLUSTER_NAME="${CLUSTER_NAME}" \
NAMESPACE_NAME="${NAMESPACE_NAME}" \
TABLE_NAME="${TABLE_NAME}" \
META_COUNT="${META_COUNT}" \
SERVER_COUNT="${SERVER_COUNT}" \
REPLICA_COUNT="${REPLICA_COUNT}" \
MS_PORT="${MS_PORT}" \
MS_RAFT_PORT="${MS_RAFT_PORT}" \
MS_SNAPSHOT_PORT="${MS_SNAPSHOT_PORT}" \
SERVER_PORT="${SERVER_PORT}" \
bash "${ROOT}/tools/smoke_ubuntu22.sh" > "${launcher_log}" 2>&1 &
launcher_pid="$!"

for _ in $(seq 1 180); do
  if grep -q "TemporalStore Ubuntu smoke test passed" "${launcher_log}" 2>/dev/null; then
    break
  fi
  if ! kill -0 "${launcher_pid}" >/dev/null 2>&1; then
    echo "SDK smoke launcher exited early" >&2
    cat "${launcher_log}" >&2 || true
    exit 1
  fi
  sleep 0.5
done

if ! grep -q "TemporalStore Ubuntu smoke test passed" "${launcher_log}" 2>/dev/null; then
  echo "SDK smoke cluster did not become ready" >&2
  cat "${launcher_log}" >&2 || true
  exit 1
fi

sleep "${WARMUP_SECONDS}"

if [[ "${RUN_CUSTOMER_EXAMPLES}" == "1" ]]; then
  "${BUILD_DIR}/src/client/example/customer_client_example" \
    "127.0.0.1:${MS_PORT}" "${IDC}" "${NAMESPACE_NAME}" "${TABLE_NAME}" \
    | tee "${RESULT_DIR}/customer_cpp.out"

  "${BUILD_DIR}/src/client/example/customer_c_client_example" \
    "127.0.0.1:${MS_PORT}" "${IDC}" "${NAMESPACE_NAME}" "${TABLE_NAME}" \
    | tee "${RESULT_DIR}/customer_c.out"
fi

if [[ "${RUN_PYTHON_SDK}" == "1" && -f "${ROOT}/sdk/python/examples/sequence_features.py" ]]; then
  python_lib="${TEMPORALSTORE_PYTHON_LIB:-${OUT_DIR}/sdk/lib/libbcache2.so}"
  python_preload="${LD_PRELOAD:-}"
  if [[ "${TEMPORALSTORE_PYTHON_PRELOAD:-0}" == "1" ]]; then
    python_preload="${python_lib}${python_preload:+:${python_preload}}"
  fi
  LD_LIBRARY_PATH="${OUT_DIR}/sdk/lib:${LD_LIBRARY_PATH:-}" \
  LD_PRELOAD="${python_preload}" \
  TEMPORALSTORE_LIB="${python_lib}" \
  PYTHONPATH="${ROOT}/sdk/python" \
  python3 "${ROOT}/sdk/python/examples/sequence_features.py" \
    | tee "${RESULT_DIR}/customer_python.out"
fi

if [[ "${RUN_PYTHON_DIRECT_STRESS}" == "1" && -f "${ROOT}/sdk/python/examples/direct_sdk_stress.py" ]]; then
  python_lib="${TEMPORALSTORE_PYTHON_LIB:-${OUT_DIR}/sdk/lib/libbcache2.so}"
  python_preload="${LD_PRELOAD:-}"
  if [[ "${TEMPORALSTORE_PYTHON_PRELOAD:-0}" == "1" ]]; then
    python_preload="${python_lib}${python_preload:+:${python_preload}}"
  fi
  LD_LIBRARY_PATH="${OUT_DIR}/sdk/lib:${LD_LIBRARY_PATH:-}" \
  LD_PRELOAD="${python_preload}" \
  TEMPORALSTORE_LIB="${python_lib}" \
  PYTHONPATH="${ROOT}/sdk/python" \
  python3 "${ROOT}/sdk/python/examples/direct_sdk_stress.py" \
    --metaserver "127.0.0.1:${MS_PORT}" \
    --namespace "${NAMESPACE_NAME}" \
    --table "${TABLE_NAME}" \
    --prefix "${CLUSTER_NAME}" \
    --hash-ops "${PYTHON_DIRECT_STRESS_HASH_OPS}" \
    --feature-keys "${PYTHON_DIRECT_STRESS_FEATURE_KEYS}" \
    --feature-points-per-key "${PYTHON_DIRECT_STRESS_FEATURE_POINTS_PER_KEY}" \
    --value-bytes "${PYTHON_DIRECT_STRESS_VALUE_BYTES}" \
    --request-timeout-ms "${PYTHON_DIRECT_STRESS_REQUEST_TIMEOUT_MS}" \
    --io-timeout-ms "${PYTHON_DIRECT_STRESS_IO_TIMEOUT_MS}" \
    --report-json "${RESULT_DIR}/python_direct_stress.json" \
    | tee "${RESULT_DIR}/python_direct_stress.out"
fi

if [[ "${RUN_CPP_PROXY_PARITY}" == "1" ]]; then
  proxy_log_dir="${runtime_dir}/proxy/log"
  mkdir -p "${proxy_log_dir}"
  (
    cd "${ROOT}"
    env BYTED_HOST_IP=127.0.0.1 BYTED_HOST_IPV6= \
      "${OUT_DIR}/bcache2-proxy" \
        --port="${CPP_PROXY_PORT}" \
        --master_endpoint="127.0.0.1:${MS_PORT}" \
        --idc="${IDC}" \
        --proxy_cluster_name="${CLUSTER_NAME}" \
        --proxy_vregion=local \
        --proxy_vdc="${IDC}" \
        --proxy_vau=local \
        --proxy_log_dir="${proxy_log_dir}" \
        --proxy_log_level=2 \
        --proxy_pin_primary_reads=true
  ) > "${RESULT_DIR}/cpp_proxy.out" 2> "${RESULT_DIR}/cpp_proxy.err" &
  proxy_pid="$!"

  for _ in $(seq 1 60); do
    if kill -0 "${proxy_pid}" >/dev/null 2>&1; then
      if PROXY_SMOKE_TIMEOUT_MS=10000 \
        "${BUILD_DIR}/src/client/example/proxy_smoke_example" \
          "127.0.0.1:${CPP_PROXY_PORT}" "${NAMESPACE_NAME}" "${TABLE_NAME}" \
          "${CLUSTER_NAME}_proxy_smoke" \
          > "${RESULT_DIR}/cpp_proxy_smoke.out" 2> "${RESULT_DIR}/cpp_proxy_smoke.err"; then
        break
      fi
    else
      echo "C++ proxy exited early" >&2
      cat "${RESULT_DIR}/cpp_proxy.err" >&2 || true
      exit 1
    fi
    sleep 1
  done

  if [[ ! -s "${RESULT_DIR}/cpp_proxy_smoke.out" ]] ||
     ! grep -q "PASS proxy thrift smoke" "${RESULT_DIR}/cpp_proxy_smoke.out"; then
    echo "C++ proxy smoke failed" >&2
    cat "${RESULT_DIR}/cpp_proxy_smoke.out" >&2 || true
    cat "${RESULT_DIR}/cpp_proxy_smoke.err" >&2 || true
    exit 1
  fi

  proxy_pressure_code=1
  for attempt in $(seq 1 10); do
    set +e
    PROXY_SMOKE_TIMEOUT_MS=10000 \
    "${BUILD_DIR}/src/client/example/proxy_ingestion_pressure_example" \
      "127.0.0.1:${CPP_PROXY_PORT}" "${NAMESPACE_NAME}" "${TABLE_NAME}" \
      "${CLUSTER_NAME}_proxy_pressure_${attempt}" \
      "${CPP_PROXY_PRESSURE_OPS}" "${CPP_PROXY_PRESSURE_THREADS}" \
      "${CPP_PROXY_PRESSURE_VALUE_BYTES}" 1 \
      "${CPP_PROXY_PRESSURE_VERIFY_TIMEOUT_MS}" \
      "${CPP_PROXY_PRESSURE_VERIFY_POLL_MS}" \
      "${CPP_PROXY_PRESSURE_WRITE_RETRIES}" \
      | tee "${RESULT_DIR}/cpp_proxy_pressure.out"
    proxy_pressure_code=${PIPESTATUS[0]}
    set -e
    [[ "${proxy_pressure_code}" == "0" ]] && break
    echo "proxy pressure attempt ${attempt} failed; retrying" \
      >> "${RESULT_DIR}/cpp_proxy_pressure.err"
    sleep 1
  done
  if [[ "${proxy_pressure_code}" != "0" ]]; then
    cat "${RESULT_DIR}/cpp_proxy_pressure.err" >&2 || true
    exit "${proxy_pressure_code}"
  fi
fi

if [[ "${RUN_GO_SDK}" == "1" && -f "${ROOT}/sdk/go/temporalstore/examples/sequence_features.go" ]]; then
  (
    cd "${ROOT}/sdk/go/temporalstore"
    LD_LIBRARY_PATH="${OUT_DIR}/sdk/lib:${LD_LIBRARY_PATH:-}" \
    CGO_LDFLAGS="-L${OUT_DIR}/sdk/lib -l${SDK_LIB_NAME} ${CGO_LDFLAGS:-}" \
    go run -tags temporalstore_direct ./examples
  ) | tee "${RESULT_DIR}/customer_go.out"
fi

if [[ "${RUN_JAVA_SDK}" == "1" && -f "${ROOT}/sdk/java/temporalstore/pom.xml" ]]; then
  if ! command -v mvn >/dev/null 2>&1; then
    echo "RUN_JAVA_SDK=1 requires mvn on PATH" >&2
    exit 1
  fi
  (
    cd "${ROOT}/sdk/java/temporalstore"
    mvn -q -DskipTests package
    LD_LIBRARY_PATH="${OUT_DIR}/sdk/lib:${LD_LIBRARY_PATH:-}" \
    TEMPORALSTORE_JAVA_LIB="${SDK_LIB_NAME}" \
    java -cp "target/classes:${HOME}/.m2/repository/net/java/dev/jna/jna/5.14.0/jna-5.14.0.jar" \
      com.temporalstore.example.SequenceFeatures \
      "127.0.0.1:${MS_PORT}" "${NAMESPACE_NAME}" "${TABLE_NAME}"
  ) | tee "${RESULT_DIR}/customer_java.out"
fi

if [[ "${RUN_RUST_SDK}" == "1" && -f "${ROOT}/sdk/rust/temporalstore/Cargo.toml" ]]; then
  (
    cd "${ROOT}/sdk/rust/temporalstore"
    LD_LIBRARY_PATH="${OUT_DIR}/sdk/lib:${LD_LIBRARY_PATH:-}" \
    TEMPORALSTORE_LIB_DIR="${OUT_DIR}/sdk/lib" \
    TEMPORALSTORE_LIB_NAME="${SDK_LIB_NAME}" \
    cargo run --example sequence_features
  ) | tee "${RESULT_DIR}/customer_rust.out"
fi

if [[ "${RUN_UNIFIED_TESTS}" == "1" ]]; then
  if [[ ! -f "${RUST_UNIFIED_CORPUS}" ]]; then
    echo "RUN_UNIFIED_TESTS=1 requires an existing RUST_UNIFIED_CORPUS, got ${RUST_UNIFIED_CORPUS}" >&2
    exit 1
  fi
  RUST_UNIFIED_CORPUS="${RUST_UNIFIED_CORPUS}" \
  RUST_UNIFIED_VALIDATE_ONLY="${RUST_UNIFIED_VALIDATE_ONLY}" \
  bash "${ROOT}/tools/run_rust_unified_tests.sh" | tee "${RESULT_DIR}/rust_unified.out"
fi

echo "wrote:"
if [[ -f "${RESULT_DIR}/customer_cpp.out" ]]; then
  echo "  ${RESULT_DIR}/customer_cpp.out"
fi
if [[ -f "${RESULT_DIR}/customer_c.out" ]]; then
  echo "  ${RESULT_DIR}/customer_c.out"
fi
if [[ -f "${RESULT_DIR}/customer_python.out" ]]; then
  echo "  ${RESULT_DIR}/customer_python.out"
fi
if [[ -f "${RESULT_DIR}/python_direct_stress.out" ]]; then
  echo "  ${RESULT_DIR}/python_direct_stress.out"
  echo "  ${RESULT_DIR}/python_direct_stress.json"
fi
if [[ -f "${RESULT_DIR}/cpp_proxy_smoke.out" ]]; then
  echo "  ${RESULT_DIR}/cpp_proxy_smoke.out"
  echo "  ${RESULT_DIR}/cpp_proxy_pressure.out"
fi
if [[ -f "${RESULT_DIR}/customer_go.out" ]]; then
  echo "  ${RESULT_DIR}/customer_go.out"
fi
if [[ -f "${RESULT_DIR}/customer_java.out" ]]; then
  echo "  ${RESULT_DIR}/customer_java.out"
fi
if [[ -f "${RESULT_DIR}/customer_rust.out" ]]; then
  echo "  ${RESULT_DIR}/customer_rust.out"
fi
if [[ -f "${RESULT_DIR}/rust_unified.out" ]]; then
  echo "  ${RESULT_DIR}/rust_unified.out"
fi
echo "  ${launcher_log}"
