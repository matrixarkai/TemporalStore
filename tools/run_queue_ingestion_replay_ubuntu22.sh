#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_TYPE="${BUILD_TYPE:-Release}"
ITERATIONS="${ITERATIONS:-8}"
RECORDS="${RECORDS:-5000}"
BATCH_SIZE="${BATCH_SIZE:-256}"
VALUE_SIZE="${VALUE_SIZE:-128}"
PARTITIONS="${PARTITIONS:-8}"
DUPLICATE_EVERY="${DUPLICATE_EVERY:-17}"
SOURCES="${SOURCES:-${SOURCE:-api kafka flink}}"
DEAD_LETTER_EVERY="${DEAD_LETTER_EVERY:-0}"
FAIL_FIRST_ATTEMPT_EVERY="${FAIL_FIRST_ATTEMPT_EVERY:-0}"
POISON_EVERY="${POISON_EVERY:-0}"
RESULT_DIR="${RESULT_DIR:-/tmp/temporalstore-queue-ingestion-$(date +%Y%m%d-%H%M%S)}"
TEXTFILE_DIR="${TEXTFILE_DIR:-${RESULT_DIR}/metrics}"
METRICS_FILE="${METRICS_FILE:-${TEXTFILE_DIR}/temporalstore-ingestion.prom}"
DRY_RUN="${DRY_RUN:-1}"
PROXY_ENDPOINT="${PROXY_ENDPOINT:-}"
COMPAT_ROOT="${COMPAT_ROOT:-/mnt/c/Users/Deeproute/Documents/Codex/2026-06-06/set-up-wsl-with-ubuntu-2022/work/cmake-glue/compat-include}"
BRPC_STATIC_LIBRARY="${BRPC_STATIC_LIBRARY:-/mnt/c/Users/Deeproute/Documents/Codex/2026-06-10/pull-rust-temporalstore-code-from-matrixarkai/work/TemporalStore/build-ubuntu22/_open_source_brpc/output/lib/libbrpc.a}"
EXTRA_CMAKE_ARGS="${EXTRA_CMAKE_ARGS:--DTEMPORALSTORE_USE_ROOT_CMAKE_GLUE=ON}"
FORCE_BUILD="${FORCE_BUILD:-0}"

mkdir -p "${RESULT_DIR}" "${TEXTFILE_DIR}"

BIN="${BIN:-${ROOT}/build-ubuntu22/${BUILD_TYPE,,}/src/client/example/queue_ingestion_replay_example}"
if [[ "${FORCE_BUILD}" != "1" && -x "${BIN}" ]]; then
  bin_help="$("${BIN}" --help 2>&1 || true)"
  if ! grep -q -- "--partitions" <<< "${bin_help}"; then
    FORCE_BUILD=1
  fi
fi
if [[ "${FORCE_BUILD}" == "1" || ! -x "${BIN}" ]]; then
  BUILD_TARGETS=queue_ingestion_replay_example \
  BCACHE2_BUILD_TESTS=OFF \
  BYTESTORE_COMPAT_INCLUDE_DIR="${BYTESTORE_COMPAT_INCLUDE_DIR:-${COMPAT_ROOT}}" \
  OBJECT_STORE_COMPAT_INCLUDE_DIR="${OBJECT_STORE_COMPAT_INCLUDE_DIR:-${COMPAT_ROOT}/object_store_compat/include}" \
  BRPC_STATIC_LIBRARY="${BRPC_STATIC_LIBRARY}" \
  EXTRA_CMAKE_ARGS="${EXTRA_CMAKE_ARGS}" \
  BUILD_TYPE="${BUILD_TYPE}" \
  JOBS="${JOBS:-2}" \
  timeout "${BUILD_TIMEOUT_S:-600}" bash "${ROOT}/tools/build_ubuntu22.sh" > "${RESULT_DIR}/build.log" 2>&1
else
  echo "reuse_existing_binary=${BIN}" > "${RESULT_DIR}/build.log"
fi

if [[ ! -x "${BIN}" ]]; then
  echo "missing queue ingestion binary: ${BIN}" >&2
  tail -120 "${RESULT_DIR}/build.log" >&2 || true
  exit 1
fi

read -r expected_input expected_unique expected_duplicates expected_dead_letters expected_retries expected_retry_exhausted expected_committed expected_max_checkpoint expected_watermark expected_max_lag <<EOF
$(python3 - <<'PY' "${RECORDS}" "${PARTITIONS}" "${DUPLICATE_EVERY}" "${DEAD_LETTER_EVERY}" "${FAIL_FIRST_ATTEMPT_EVERY}" "${POISON_EVERY}"
import sys

records = int(sys.argv[1])
partitions = int(sys.argv[2])
duplicate_every = int(sys.argv[3])
dead_letter_every = int(sys.argv[4])
fail_first_attempt_every = int(sys.argv[5])
poison_every = int(sys.argv[6])
seen = set()
input_records = 0
unique_records = 0
duplicate_records = 0
dead_letter_records = 0
retries = 0
retry_exhausted_records = 0
checkpoints = {}
high_offsets = {}

def is_dead(offset):
    return dead_letter_every > 0 and offset > 0 and offset % dead_letter_every == 0

def should_retry(offset):
    return fail_first_attempt_every > 0 and offset > 0 and offset % fail_first_attempt_every == 0

def is_poison(offset):
    return poison_every > 0 and offset > 0 and offset % poison_every == 0

def consume(offset):
    global input_records, unique_records, duplicate_records, dead_letter_records, retries
    global retry_exhausted_records
    input_records += 1
    if is_dead(offset):
        dead_letter_records += 1
        return
    partition = offset % partitions
    high_offsets[partition] = max(high_offsets.get(partition, -1), offset)
    key = (partition, offset)
    if key in seen:
        duplicate_records += 1
        return
    seen.add(key)
    unique_records += 1
    if is_poison(offset):
        dead_letter_records += 1
        retry_exhausted_records += 1
        retries += 3
        return
    if should_retry(offset):
        retries += 1
    checkpoints[partition] = max(checkpoints.get(partition, -1), offset)

for offset in range(records):
    consume(offset)
    if duplicate_every > 0 and offset > 0 and offset % duplicate_every == 0:
        consume(offset)

max_checkpoint = max(checkpoints.values()) if checkpoints else -1
watermark = min(checkpoints.values()) if checkpoints else -1
max_lag = 0
for partition, high in high_offsets.items():
    max_lag = max(max_lag, high - checkpoints.get(partition, -1))

committed = unique_records - retry_exhausted_records
print(input_records, unique_records, duplicate_records, dead_letter_records, retries,
      retry_exhausted_records, committed, max_checkpoint, watermark, max_lag)
PY
)
EOF

source_list="${SOURCES//,/ }"
cat > "${METRICS_FILE}" <<'EOF'
# HELP temporalstore_ingestion_input_records_total Input records seen by the local queue ingestion replay gate.
# TYPE temporalstore_ingestion_input_records_total counter
# HELP temporalstore_ingestion_unique_records_total Unique records after source/partition/offset dedupe.
# TYPE temporalstore_ingestion_unique_records_total counter
# HELP temporalstore_ingestion_duplicate_records_total Duplicate records skipped by source/partition/offset dedupe.
# TYPE temporalstore_ingestion_duplicate_records_total counter
# HELP temporalstore_ingestion_batches_total Batches flushed by the local queue ingestion replay gate.
# TYPE temporalstore_ingestion_batches_total counter
# HELP temporalstore_ingestion_committed_records_total Records committed by the local queue ingestion replay gate.
# TYPE temporalstore_ingestion_committed_records_total counter
# HELP temporalstore_ingestion_failed_records_total Records that failed without being dead-lettered.
# TYPE temporalstore_ingestion_failed_records_total counter
# HELP temporalstore_ingestion_retries_total Retry attempts observed by the local queue ingestion replay gate.
# TYPE temporalstore_ingestion_retries_total counter
# HELP temporalstore_ingestion_dead_letters_total Records sent to the dead-letter path.
# TYPE temporalstore_ingestion_dead_letters_total counter
# HELP temporalstore_ingestion_retry_exhausted_records_total Records that exhausted retry budget.
# TYPE temporalstore_ingestion_retry_exhausted_records_total counter
# HELP temporalstore_ingestion_checkpointed_partitions Partitions with committed checkpoints.
# TYPE temporalstore_ingestion_checkpointed_partitions gauge
# HELP temporalstore_ingestion_max_checkpoint_offset Highest committed checkpoint offset.
# TYPE temporalstore_ingestion_max_checkpoint_offset gauge
# HELP temporalstore_ingestion_committed_watermark_offset Lowest committed checkpoint across partitions.
# TYPE temporalstore_ingestion_committed_watermark_offset gauge
# HELP temporalstore_ingestion_max_partition_lag_records Max per-partition input offset minus checkpoint offset.
# TYPE temporalstore_ingestion_max_partition_lag_records gauge
# HELP temporalstore_ingestion_backpressure_records Backpressure-sized backlog proxy: uncommitted unique records plus max partition lag.
# TYPE temporalstore_ingestion_backpressure_records gauge
# HELP temporalstore_ingestion_committed_qps Committed records per second for the replay gate.
# TYPE temporalstore_ingestion_committed_qps gauge
# HELP temporalstore_ingestion_validation_up Whether the local queue ingestion replay gate passed.
# TYPE temporalstore_ingestion_validation_up gauge
EOF

metric_value() {
  local file="$1"
  local key="$2"
  awk -F= -v key="${key}" '$1 == key {print $2; found=1} END {if (!found) print ""}' "${file}"
}

write_ingestion_metrics() {
  local iteration="$1"
  local source="$2"
  local out="$3"
  local input_records unique_records duplicate_records batches committed failed retries
  local dead_letters retry_exhausted checkpointed max_checkpoint watermark max_lag qps
  input_records="$(metric_value "${out}" input_records)"
  unique_records="$(metric_value "${out}" unique_records)"
  duplicate_records="$(metric_value "${out}" duplicate_records)"
  batches="$(metric_value "${out}" batches)"
  committed="$(metric_value "${out}" committed)"
  failed="$(metric_value "${out}" failed)"
  retries="$(metric_value "${out}" retries)"
  dead_letters="$(metric_value "${out}" dead_letter_records)"
  retry_exhausted="$(metric_value "${out}" retry_exhausted_records)"
  checkpointed="$(metric_value "${out}" checkpointed_partitions)"
  max_checkpoint="$(metric_value "${out}" max_checkpoint_offset)"
  watermark="$(metric_value "${out}" committed_watermark_offset)"
  max_lag="$(metric_value "${out}" max_partition_lag)"
  qps="$(metric_value "${out}" committed_qps)"
  backpressure=$(( (unique_records - committed) + max_lag ))
  cat >> "${METRICS_FILE}" <<EOF
temporalstore_ingestion_input_records_total{source="${source}",iteration="${iteration}"} ${input_records}
temporalstore_ingestion_unique_records_total{source="${source}",iteration="${iteration}"} ${unique_records}
temporalstore_ingestion_duplicate_records_total{source="${source}",iteration="${iteration}"} ${duplicate_records}
temporalstore_ingestion_batches_total{source="${source}",iteration="${iteration}"} ${batches}
temporalstore_ingestion_committed_records_total{source="${source}",iteration="${iteration}"} ${committed}
temporalstore_ingestion_failed_records_total{source="${source}",iteration="${iteration}"} ${failed}
temporalstore_ingestion_retries_total{source="${source}",iteration="${iteration}"} ${retries}
temporalstore_ingestion_dead_letters_total{source="${source}",iteration="${iteration}"} ${dead_letters}
temporalstore_ingestion_retry_exhausted_records_total{source="${source}",iteration="${iteration}"} ${retry_exhausted}
temporalstore_ingestion_checkpointed_partitions{source="${source}",iteration="${iteration}"} ${checkpointed}
temporalstore_ingestion_max_checkpoint_offset{source="${source}",iteration="${iteration}"} ${max_checkpoint}
temporalstore_ingestion_committed_watermark_offset{source="${source}",iteration="${iteration}"} ${watermark}
temporalstore_ingestion_max_partition_lag_records{source="${source}",iteration="${iteration}"} ${max_lag}
temporalstore_ingestion_backpressure_records{source="${source}",iteration="${iteration}"} ${backpressure}
temporalstore_ingestion_committed_qps{source="${source}",iteration="${iteration}"} ${qps}
temporalstore_ingestion_validation_up{source="${source}",iteration="${iteration}"} 1
EOF
}

for iteration in $(seq 1 "${ITERATIONS}"); do
  for source in ${source_list}; do
    out="${RESULT_DIR}/iteration_${iteration}_${source}.out"
    args=(
      "--dry_run=${DRY_RUN}"
      "--records=${RECORDS}"
      "--batch_size=${BATCH_SIZE}"
      "--value_size=${VALUE_SIZE}"
      "--partitions=${PARTITIONS}"
      "--duplicate_every=${DUPLICATE_EVERY}"
      "--dead_letter_every=${DEAD_LETTER_EVERY}"
      "--fail_first_attempt_every=${FAIL_FIRST_ATTEMPT_EVERY}"
      "--poison_every=${POISON_EVERY}"
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
    grep -q "^committed=${expected_committed}$" "${out}"
    grep -q '^failed=0$' "${out}"
    grep -q "^retries=${expected_retries}$" "${out}"
    grep -q "^retry_exhausted_records=${expected_retry_exhausted}$" "${out}"
    grep -q "^checkpointed_partitions=${PARTITIONS}$" "${out}"
    grep -q "^max_checkpoint_offset=${expected_max_checkpoint}$" "${out}"
    grep -q "^committed_watermark_offset=${expected_watermark}$" "${out}"
    grep -q "^max_partition_lag=${expected_max_lag}$" "${out}"
    write_ingestion_metrics "${iteration}" "${source}" "${out}"
  done
done

for required_metric in \
  temporalstore_ingestion_retries_total \
  temporalstore_ingestion_dead_letters_total \
  temporalstore_ingestion_retry_exhausted_records_total \
  temporalstore_ingestion_committed_watermark_offset \
  temporalstore_ingestion_max_partition_lag_records \
  temporalstore_ingestion_backpressure_records \
  temporalstore_ingestion_validation_up; do
  if ! grep -q "^${required_metric}" "${METRICS_FILE}"; then
    echo "missing ingestion metric ${required_metric} in ${METRICS_FILE}" >&2
    exit 1
  fi
done

python3 - "${METRICS_FILE}" "${expected_retries}" "${expected_dead_letters}" \
  "${expected_retry_exhausted}" "${expected_max_lag}" <<'PY'
import sys

path = sys.argv[1]
expected_retries = float(sys.argv[2])
expected_dead_letters = float(sys.argv[3])
expected_retry_exhausted = float(sys.argv[4])
expected_max_lag = float(sys.argv[5])
payload = open(path, encoding="utf-8").read().splitlines()
values = {}
for line in payload:
    if not line or line.startswith("#"):
        continue
    name = line.split("{", 1)[0].split(" ", 1)[0]
    value = float(line.rsplit(" ", 1)[1])
    values.setdefault(name, []).append(value)

if expected_retries > 0 and not any(v > 0 for v in values.get("temporalstore_ingestion_retries_total", [])):
    raise SystemExit("expected positive ingestion retry metric")
if expected_dead_letters > 0 and not any(v > 0 for v in values.get("temporalstore_ingestion_dead_letters_total", [])):
    raise SystemExit("expected positive ingestion dead-letter metric")
if expected_retry_exhausted > 0 and not any(v > 0 for v in values.get("temporalstore_ingestion_retry_exhausted_records_total", [])):
    raise SystemExit("expected positive ingestion retry-exhausted metric")
if (expected_retry_exhausted > 0 or expected_max_lag > 0) and not any(
    v > 0 for v in values.get("temporalstore_ingestion_backpressure_records", [])
):
    raise SystemExit("expected positive ingestion backpressure metric")
if any(v != 1 for v in values.get("temporalstore_ingestion_validation_up", [])):
    raise SystemExit("ingestion validation metric was not healthy")
PY

echo "PASS queue ingestion replay"
echo "iterations=${ITERATIONS}"
echo "records=${RECORDS}"
echo "batch_size=${BATCH_SIZE}"
echo "partitions=${PARTITIONS}"
echo "duplicate_every=${DUPLICATE_EVERY}"
echo "dead_letter_every=${DEAD_LETTER_EVERY}"
echo "fail_first_attempt_every=${FAIL_FIRST_ATTEMPT_EVERY}"
echo "poison_every=${POISON_EVERY}"
echo "sources=${SOURCES}"
echo "metrics_file=${METRICS_FILE}"
echo "result_dir=${RESULT_DIR}"
