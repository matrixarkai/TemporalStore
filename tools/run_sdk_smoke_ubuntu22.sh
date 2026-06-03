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
RUN_PYTHON_SDK="${RUN_PYTHON_SDK:-0}"
RUN_GO_SDK="${RUN_GO_SDK:-0}"
RUN_JAVA_SDK="${RUN_JAVA_SDK:-0}"
RUN_RUST_SDK="${RUN_RUST_SDK:-0}"
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

mkdir -p "${RESULT_DIR}"
runtime_dir="${RUNTIME_DIR}"
launcher_log="${RESULT_DIR}/launcher.log"
launcher_pid=""

cleanup() {
  for pid_file in "${runtime_dir}"/server*.pid "${runtime_dir}"/metaserver*.pid; do
    [[ -f "${pid_file}" ]] || continue
    kill "$(cat "${pid_file}")" >/dev/null 2>&1 || true
  done
  if [[ -n "${launcher_pid}" ]] && kill -0 "${launcher_pid}" >/dev/null 2>&1; then
    kill "${launcher_pid}" >/dev/null 2>&1 || true
  fi
  pkill -f "bcache2-metaserver.*metaserver_cluster_name=${CLUSTER_NAME}" >/dev/null 2>&1 || true
  pkill -f "bcache2-server.*cluster_name=${CLUSTER_NAME}" >/dev/null 2>&1 || true
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

"${BUILD_DIR}/src/client/example/customer_client_example" \
  "127.0.0.1:${MS_PORT}" "${IDC}" "${NAMESPACE_NAME}" "${TABLE_NAME}" \
  | tee "${RESULT_DIR}/customer_cpp.out"

"${BUILD_DIR}/src/client/example/customer_c_client_example" \
  "127.0.0.1:${MS_PORT}" "${IDC}" "${NAMESPACE_NAME}" "${TABLE_NAME}" \
  | tee "${RESULT_DIR}/customer_c.out"

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

echo "wrote:"
echo "  ${RESULT_DIR}/customer_cpp.out"
echo "  ${RESULT_DIR}/customer_c.out"
if [[ -f "${RESULT_DIR}/customer_python.out" ]]; then
  echo "  ${RESULT_DIR}/customer_python.out"
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
echo "  ${launcher_log}"
