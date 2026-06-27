#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_TYPE="${BUILD_TYPE:-Release}"
BUILD_TARGETS="${BUILD_TARGETS:-customer_client_example}"
JOBS="${JOBS:-2}"
BUILD_TIMEOUT_S="${BUILD_TIMEOUT_S:-300}"
ARTIFACT_DIR="${ARTIFACT_DIR:-/tmp/temporalstore-cpp-client-target-gate-$(date +%Y%m%d-%H%M%S)}"
DRY_RUN="${DRY_RUN:-0}"

mkdir -p "${ARTIFACT_DIR}"
BUILD_LOG="${ARTIFACT_DIR}/build.log"
DIAG_JSON="${ARTIFACT_DIR}/diagnostics.json"
DIAG_MD="${ARTIFACT_DIR}/diagnostics.md"
PROCESS_SNAPSHOT="${ARTIFACT_DIR}/process_snapshot.txt"
TARGET_HELP="${ARTIFACT_DIR}/target_help.txt"

build_cmd=(
  env
  "BUILD_TYPE=${BUILD_TYPE}"
  "BUILD_TARGETS=${BUILD_TARGETS}"
  "JOBS=${JOBS}"
  bash "${ROOT}/tools/build_ubuntu22.sh"
)

printf '%q ' "${build_cmd[@]}" > "${ARTIFACT_DIR}/command.txt"
printf '\n' >> "${ARTIFACT_DIR}/command.txt"

if [[ "${DRY_RUN}" == "1" ]]; then
  cat > "${DIAG_MD}" <<EOF
# C++ Client Target Build Gate Dry Run

- root: ${ROOT}
- build_type: ${BUILD_TYPE}
- build_targets: ${BUILD_TARGETS}
- jobs: ${JOBS}
- timeout_s: ${BUILD_TIMEOUT_S}
- command: $(cat "${ARTIFACT_DIR}/command.txt")
EOF
  python3 - "${DIAG_JSON}" <<PY
import json, sys
json.dump({
    "status": "dry_run",
    "root": "${ROOT}",
    "build_type": "${BUILD_TYPE}",
    "build_targets": "${BUILD_TARGETS}",
    "jobs": int("${JOBS}"),
    "timeout_s": int("${BUILD_TIMEOUT_S}"),
    "artifact_dir": "${ARTIFACT_DIR}",
}, open(sys.argv[1], "w"), indent=2)
PY
  echo "dry_run: ${ARTIFACT_DIR}"
  exit 0
fi

set +e
setsid "${build_cmd[@]}" >"${BUILD_LOG}" 2>&1 &
build_pid=$!
start_epoch=$(date +%s)
rc=0
while kill -0 "${build_pid}" >/dev/null 2>&1; do
  now_epoch=$(date +%s)
  if (( now_epoch - start_epoch >= BUILD_TIMEOUT_S )); then
    kill -TERM "-${build_pid}" >/dev/null 2>&1 || true
    sleep 2
    if kill -0 "${build_pid}" >/dev/null 2>&1; then
      kill -KILL "-${build_pid}" >/dev/null 2>&1 || true
    fi
    wait "${build_pid}" >/dev/null 2>&1
    rc=124
    break
  fi
  sleep 1
done
if [[ "${rc}" == "0" ]]; then
  wait "${build_pid}"
  rc=$?
fi
set -e

if [[ "${rc}" == "0" ]]; then
  python3 - "${DIAG_JSON}" <<PY
import json, sys
json.dump({
    "status": "passed",
    "root": "${ROOT}",
    "build_type": "${BUILD_TYPE}",
    "build_targets": "${BUILD_TARGETS}",
    "jobs": int("${JOBS}"),
    "timeout_s": int("${BUILD_TIMEOUT_S}"),
    "artifact_dir": "${ARTIFACT_DIR}",
    "build_log": "${BUILD_LOG}",
}, open(sys.argv[1], "w"), indent=2)
PY
  cat > "${DIAG_MD}" <<EOF
# C++ Client Target Build Gate

Status: passed

- root: ${ROOT}
- build_type: ${BUILD_TYPE}
- build_targets: ${BUILD_TARGETS}
- jobs: ${JOBS}
- timeout_s: ${BUILD_TIMEOUT_S}
- build_log: ${BUILD_LOG}
EOF
  echo "passed: ${ARTIFACT_DIR}"
  exit 0
fi

ps -eo pid,ppid,stat,etime,pcpu,pmem,comm,args \
  | grep -E 'cmake|make|ninja|g\+\+|gcc|cc1plus|ld|ar|collect2' \
  | grep -v grep > "${PROCESS_SNAPSHOT}" || true

if [[ -d "${ROOT}/build-ubuntu22/release" ]]; then
  cmake --build "${ROOT}/build-ubuntu22/release" --target help > "${TARGET_HELP}" 2>&1 || true
fi

if grep -Eiq '(error:|fatal error:|undefined reference|collect2: error|CMake Error)' "${BUILD_LOG}"; then
  timeout_without_compiler_error=false
else
  timeout_without_compiler_error=true
fi

if [[ "${rc}" == "124" || "${rc}" == "137" || "${rc}" == "143" ]]; then
  status="timeout"
else
  status="failed"
fi

python3 - "${DIAG_JSON}" <<PY
import json, sys
json.dump({
    "status": "${status}",
    "exit_code": int("${rc}"),
    "timeout_without_compiler_error": "${timeout_without_compiler_error}" == "true",
    "root": "${ROOT}",
    "build_type": "${BUILD_TYPE}",
    "build_targets": "${BUILD_TARGETS}",
    "jobs": int("${JOBS}"),
    "timeout_s": int("${BUILD_TIMEOUT_S}"),
    "artifact_dir": "${ARTIFACT_DIR}",
    "build_log": "${BUILD_LOG}",
    "process_snapshot": "${PROCESS_SNAPSHOT}",
    "target_help": "${TARGET_HELP}",
}, open(sys.argv[1], "w"), indent=2)
PY

{
  echo "# C++ Client Target Build Gate"
  echo
  echo "Status: ${status}"
  echo
  echo "- exit_code: ${rc}"
  echo "- timeout_without_compiler_error: ${timeout_without_compiler_error}"
  echo "- root: ${ROOT}"
  echo "- build_type: ${BUILD_TYPE}"
  echo "- build_targets: ${BUILD_TARGETS}"
  echo "- jobs: ${JOBS}"
  echo "- timeout_s: ${BUILD_TIMEOUT_S}"
  echo "- build_log: ${BUILD_LOG}"
  echo "- process_snapshot: ${PROCESS_SNAPSHOT}"
  echo "- target_help: ${TARGET_HELP}"
  echo
  echo "## Last Build Log Lines"
  echo '```text'
  tail -80 "${BUILD_LOG}" || true
  echo '```'
  echo
  echo "## Active Build Processes"
  echo '```text'
  cat "${PROCESS_SNAPSHOT}" || true
  echo '```'
} > "${DIAG_MD}"

echo "${status}: ${ARTIFACT_DIR}" >&2
exit "${rc}"