#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_TYPE="${BUILD_TYPE:-Release}"
ITERATIONS="${ITERATIONS:-8}"
RECORDS="${RECORDS:-5000}"
BATCH_SIZE="${BATCH_SIZE:-256}"
VALUE_SIZE="${VALUE_SIZE:-128}"
DUPLICATE_EVERY="${DUPLICATE_EVERY:-17}"
SOURCES="${SOURCES:-${SOURCE:-api kafka flink}}"
DEAD_LETTER_EVERY="${DEAD_LETTER_EVERY:-0}"
FAIL_FIRST_ATTEMPT_EVERY="${FAIL_FIRST_ATTEMPT_EVERY:-0}"
RESULT_DIR="${RESULT_DIR:-/tmp/temporalstore-queue-ingestion-$(date +%Y%m%d-%H%M%S)}"
DRY_RUN="${DRY_RUN:-1}"
PROXY_ENDPOINT="${PROXY_ENDPOINT:-}"
COMPAT_ROOT="${COMPAT_ROOT:-/mnt/c/Users/Deeproute/Documents/Codex/2026-06-06/set-up-wsl-with-ubuntu-2022/work/cmake-glue/compat-include}"
BRPC_STATIC_LIBRARY="${BRPC_STATIC_LIBRARY:-/mnt/c/Users/Deeproute/Documents/Codex/2026-06-10/pull-rust-temporalstore-code-from-matrixarkai/work/TemporalStore/build-ubuntu22/_open_source_brpc/output/lib/libbrpc.a}"
EXTRA_CMAKE_ARGS="${EXTRA_CMAKE_ARGS:--DTEMPORALSTORE_USE_ROOT_CMAKE_GLUE=ON}"
FORCE_BUILD="${FORCE_BUILD:-0}"

mkdir -p "${RESULT_DIR}"

BIN="${BIN:-${ROOT}/build-ubuntu22/${BUILD_TYPE,,}/src/client/example/queue_ingestion_replay_example}"
if [[ "${FORCE_BUILD}" == "1" || ! -x "${BIN}" ]]; then
  BUILD_TARGETS=queue_ingestion_replay_example \
  BCACHE2_BUILD_TESTS=OFF \
  BYTESTORE_COMPAT_INCLUDE_DIR="${BYTESTORE_COMPAT_INCLUDE_DIR:-${COMPAT_ROOT}}" \
  OBJECT_STORE_COMPAT_INCLUDE_DIR="${OBJECT_STORE_COMPAT_INCLUDE_DIR:-${COMPAT_ROOT}/object_store_compat/include}" \
  BRPC_STATIC_LIBRARY="${BRPC_STATIC_LIBRARY}" \
  EXTRA_CMAKE_ARGS="${EXTRA_CMAKE_ARGS}" \
  BUILD_TYPE="${BUILD_TYPE}" \
  JOBS="${JOBS:-2}" \
  "${ROOT}/tools/build_ubuntu22.sh" > "${RESULT_DIR}/build.log" 2>&1
else
  echo "reuse_existing_binary=${BIN}" > "${RESULT_DIR}/build.log"
fi

if [[ ! -x "${BIN}" ]]; then
  echo "missing queue ingestion binary: ${BIN}" >&2
  tail -120 "${RESULT_DIR}/build.log" >&2 || true
  exit 1
fi

read -r expected_input expected_unique expected_duplicates expected_dead_letters expected_retries <<EOF
$(python3 - <<'PY' "${RECORDS}" "${DUPLICATE_EVERY}" "${DEAD_LETTER_EVERY}" "${FAIL_FIRST_ATTEMPT_EVERY}"
import sys

records = int(sys.argv[1])
duplicate_every = int(sys.argv[2])
dead_letter_every = int(sys.argv[3])
fail_first_attempt_every = int(sys.argv[4])
seen = set()
input_records = 0
unique_records = 0
duplicate_records = 0
dead_letter_records = 0
retries = 0

def is_dead(offset):
    return dead_letter_every > 0 and offset > 0 and offset % dead_letter_every == 0

def should_retry(offset):
    return fail_first_attempt_every > 0 and offset > 0 and offset % fail_first_attempt_every == 0

def consume(offset):
    global input_records, unique_records, duplicate_records, dead_letter_records, retries
    input_records += 1
    if is_dead(offset):
        dead_letter_records += 1
        return
    key = offset
    if key in seen:
        duplicate_records += 1
        return
    seen.add(key)
    unique_records += 1
    if should_retry(offset):
        retries += 1

for offset in range(records):
    consume(offset)
    if duplicate_every > 0 and offset > 0 and offset % duplicate_every == 0:
        consume(offset)

print(input_records, unique_records, duplicate_records, dead_letter_records, retries)
PY
)
EOF

source_list="${SOURCES//,/ }"
for iteration in $(seq 1 "${ITERATIONS}"); do
  for source in ${source_list}; do
    out="${RESULT_DIR}/iteration_${iteration}_${source}.out"
    args=(
      "--dry_run=${DRY_RUN}"
      "--records=${RECORDS}"
      "--batch_size=${BATCH_SIZE}"
      "--value_size=${VALUE_SIZE}"
      "--duplicate_every=${DUPLICATE_EVERY}"
      "--dead_letter_every=${DEAD_LETTER_EVERY}"
      "--fail_first_attempt_every=${FAIL_FIRST_ATTEMPT_EVERY}"
      "--source=${source}"
    )
    if [[ "${DRY_RUN}" == "0" ]]; then
      args+=("--proxy=${PROXY_ENDPOINT}")
    fi
    "${BIN}" "${args[@]}" | tee "${out}"

    grep -q '^queue_ingestion_replay$' "${out}"
    grep -q "^source=${source}$" "${out}"
    grep -q "^input_records=${expected_input}$" "${out}"
    grep -q "^unique_records=${expected_unique}$" "${out}"
    grep -q "^duplicate_records=${expected_duplicates}$" "${out}"
    grep -q "^dead_letter_records=${expected_dead_letters}$" "${out}"
    grep -q "^committed=${expected_unique}$" "${out}"
    grep -q '^failed=0$' "${out}"
    grep -q "^retries=${expected_retries}$" "${out}"
    grep -q '^checkpointed_partitions=8$' "${out}"
  done
done

echo "PASS queue ingestion replay"
echo "iterations=${ITERATIONS}"
echo "records=${RECORDS}"
echo "batch_size=${BATCH_SIZE}"
echo "duplicate_every=${DUPLICATE_EVERY}"
echo "dead_letter_every=${DEAD_LETTER_EVERY}"
echo "fail_first_attempt_every=${FAIL_FIRST_ATTEMPT_EVERY}"
echo "sources=${SOURCES}"
echo "result_dir=${RESULT_DIR}"
