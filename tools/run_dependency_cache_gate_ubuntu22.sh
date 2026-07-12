#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RESULT_DIR="${RESULT_DIR:-/tmp/temporalstore-dependency-cache-gate-$(date +%Y%m%d_%H%M%S)}"
TEXTFILE_DIR="${TEXTFILE_DIR:-${RESULT_DIR}/metrics}"
METRICS_FILE="${METRICS_FILE:-${TEXTFILE_DIR}/temporalstore-dependency-cache.prom}"
BUILD_SCRIPT="${BUILD_SCRIPT:-${ROOT}/tools/build_ubuntu22.sh}"
RUN_BUILD_SMOKE="${RUN_BUILD_SMOKE:-0}"
BUILD_TYPE="${BUILD_TYPE:-Release}"
BUILD_TARGETS="${BUILD_TARGETS:-bcache2-server}"
JOBS="${JOBS:-2}"

mkdir -p "${RESULT_DIR}" "${TEXTFILE_DIR}"
SUMMARY="${RESULT_DIR}/summary.txt"
CHECKS_CSV="${RESULT_DIR}/checks.csv"
echo "kind,name,status,detail" > "${CHECKS_CSV}"

critical_missing=0
optional_missing=0
tool_missing=0
build_knob_missing=0
release_binary_missing=0
syntax_pass=0
build_smoke_pass=0

log() {
  printf '%s\n' "$*" | tee -a "${SUMMARY}"
}

record_check() {
  local kind="$1"
  local name="$2"
  local status="$3"
  local detail="$4"
  printf '%s,"%s",%s,"%s"\n' "${kind}" "${name}" "${status}" "${detail//\"/\"\"}" >> "${CHECKS_CSV}"
  log "${status} ${kind} ${name} ${detail}"
}

write_metrics() {
  local pass="$1"
  cat > "${METRICS_FILE}" <<METRICS
# HELP temporalstore_dependency_cache_gate_pass Whether the Ubuntu 22 dependency-cache CI gate passed.
# TYPE temporalstore_dependency_cache_gate_pass gauge
temporalstore_dependency_cache_gate_pass ${pass}
# HELP temporalstore_dependency_cache_critical_missing Number of critical dependency-cache paths missing.
# TYPE temporalstore_dependency_cache_critical_missing gauge
temporalstore_dependency_cache_critical_missing ${critical_missing}
# HELP temporalstore_dependency_cache_optional_missing Number of optional dependency-cache paths missing.
# TYPE temporalstore_dependency_cache_optional_missing gauge
temporalstore_dependency_cache_optional_missing ${optional_missing}
# HELP temporalstore_dependency_cache_tool_missing Number of required build tools missing.
# TYPE temporalstore_dependency_cache_tool_missing gauge
temporalstore_dependency_cache_tool_missing ${tool_missing}
# HELP temporalstore_dependency_cache_build_knob_missing Number of expected build-cache environment knobs missing from the build script.
# TYPE temporalstore_dependency_cache_build_knob_missing gauge
temporalstore_dependency_cache_build_knob_missing ${build_knob_missing}
# HELP temporalstore_dependency_cache_release_binary_missing Number of expected cached release binaries missing.
# TYPE temporalstore_dependency_cache_release_binary_missing gauge
temporalstore_dependency_cache_release_binary_missing ${release_binary_missing}
# HELP temporalstore_dependency_cache_build_script_syntax_pass Whether tools/build_ubuntu22.sh passed shell syntax validation.
# TYPE temporalstore_dependency_cache_build_script_syntax_pass gauge
temporalstore_dependency_cache_build_script_syntax_pass ${syntax_pass}
# HELP temporalstore_dependency_cache_build_smoke_pass Whether the optional build smoke ran and passed.
# TYPE temporalstore_dependency_cache_build_smoke_pass gauge
temporalstore_dependency_cache_build_smoke_pass ${build_smoke_pass}
METRICS
}

trap 'write_metrics 0' EXIT

log "TemporalStore dependency cache gate"
log "result_dir=${RESULT_DIR}"
log "metrics_file=${METRICS_FILE}"
log "run_build_smoke=${RUN_BUILD_SMOKE}"

for tool in cmake gcc g++ make git protoc; do
  if command -v "${tool}" >/dev/null 2>&1; then
    record_check tool "${tool}" pass "$(command -v "${tool}")"
  else
    tool_missing=$((tool_missing + 1))
    record_check tool "${tool}" fail "not found on PATH"
  fi
done

critical_paths=(
  "${BUILD_SCRIPT}"
  "${ROOT}/CMakeLists.txt"
  "${ROOT}/thirdparty/byte"
  "${ROOT}/thirdparty/byteraft"
  "${ROOT}/.local/deps-src/byte-master"
  "${ROOT}/.local/deps-src/byteraft-master"
)

for path in "${critical_paths[@]}"; do
  if [[ -e "${path}" ]]; then
    record_check critical_path "${path#${ROOT}/}" pass "$(readlink -f "${path}" 2>/dev/null || printf '%s' "${path}")"
  else
    critical_missing=$((critical_missing + 1))
    record_check critical_path "${path#${ROOT}/}" fail "missing"
  fi
done

optional_paths=(
  "/usr/include/isa-l/erasure_code.h"
  "/usr/lib/x86_64-linux-gnu/libthrift.so"
  "<repo>-main-no-deps/build-ubuntu22/release/_open_source_brpc/output/lib/libbrpc.a"
  "<repo>/cmake-glue/lib/libco.a"
  "<repo>/cmake-glue/lib/libfiu.a"
)

for path in "${optional_paths[@]}"; do
  if [[ -e "${path}" ]]; then
    record_check optional_path "${path}" pass "$(readlink -f "${path}" 2>/dev/null || printf '%s' "${path}")"
  else
    optional_missing=$((optional_missing + 1))
    record_check optional_path "${path}" warn "missing; gate continues because this build may use system packages or link shims"
  fi
done

required_build_knobs=(
  BUILD_TARGETS
  BUILD_TYPE
  MATRIXOBJECTSTORE_COMPAT_INCLUDE_DIR
  OBJECT_STORE_COMPAT_INCLUDE_DIR
  BRPC_STATIC_LIBRARY
  EXTRA_CMAKE_ARGS
  BCACHE2_BUILD_TESTS
  ENABLE_MTCACHE
)

for knob in "${required_build_knobs[@]}"; do
  if grep -q "${knob}" "${BUILD_SCRIPT}"; then
    record_check build_knob "${knob}" pass "present"
  else
    build_knob_missing=$((build_knob_missing + 1))
    record_check build_knob "${knob}" fail "missing from ${BUILD_SCRIPT#${ROOT}/}"
  fi
done

release_binaries=(
  "${ROOT}/output-ubuntu22/release/bcache2-server"
  "${ROOT}/output-ubuntu22/release/bcache2-metaserver"
  "${ROOT}/output-ubuntu22/release/bcache2-proxy"
)

for binary in "${release_binaries[@]}"; do
  if [[ -x "${binary}" ]]; then
    record_check release_binary "${binary#${ROOT}/}" pass "executable"
  else
    release_binary_missing=$((release_binary_missing + 1))
    record_check release_binary "${binary#${ROOT}/}" warn "missing or not executable; build script can regenerate it"
  fi
done

if bash -n "${BUILD_SCRIPT}"; then
  syntax_pass=1
  record_check build_script_syntax "${BUILD_SCRIPT#${ROOT}/}" pass "bash -n"
else
  record_check build_script_syntax "${BUILD_SCRIPT#${ROOT}/}" fail "bash -n failed"
fi

if [[ "${RUN_BUILD_SMOKE}" == "1" ]]; then
  set +e
  (
    cd "${ROOT}"
    env BUILD_TYPE="${BUILD_TYPE}" BUILD_TARGETS="${BUILD_TARGETS}" JOBS="${JOBS}" "${BUILD_SCRIPT}"
  ) > "${RESULT_DIR}/build_smoke.stdout.log" 2> "${RESULT_DIR}/build_smoke.stderr.log"
  build_code=$?
  set -e
  if [[ "${build_code}" == "0" ]]; then
    build_smoke_pass=1
    record_check build_smoke "${BUILD_TARGETS}" pass "BUILD_TYPE=${BUILD_TYPE}"
  else
    record_check build_smoke "${BUILD_TARGETS}" fail "code=${build_code}"
  fi
else
  build_smoke_pass=1
  record_check build_smoke "${BUILD_TARGETS}" skip "RUN_BUILD_SMOKE=0"
fi

if [[ "${critical_missing}" == "0" && "${tool_missing}" == "0" && "${build_knob_missing}" == "0" && "${syntax_pass}" == "1" && "${build_smoke_pass}" == "1" ]]; then
  log "PASS TemporalStore dependency cache gate"
  write_metrics 1
  trap - EXIT
  exit 0
fi

log "FAIL TemporalStore dependency cache gate"
write_metrics 0
trap - EXIT
exit 1
