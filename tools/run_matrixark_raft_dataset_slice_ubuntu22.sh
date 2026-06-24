#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_TYPE="${BUILD_TYPE:-Release}"
BUILD_FLAVOR="$(printf "%s" "${BUILD_TYPE}" | tr "[:upper:]" "[:lower:]")"
OUT_DIR="${OUT_DIR:-${ROOT}/output-ubuntu22/${BUILD_FLAVOR}}"
RESULT_DIR="${RESULT_DIR:-/tmp/matrixark-raft-dataset-slice-$(date +%Y%m%d_%H%M%S)}"
ARTIFACT_DIR="${ARTIFACT_DIR:-${ROOT}/docs/benchmarks/matrixark_raft_dataset_slice_$(date +%Y%m%d_%H%M%S)}"
SMOKE_DIR="${SMOKE_DIR:-${RESULT_DIR}/cluster}"
NAMESPACE_NAME="${NAMESPACE_NAME:-ns1}"
TABLE_NAME="${TABLE_NAME:-table1}"
MS_PORT="${MS_PORT:-28200}"
SERVER_PORT="${SERVER_PORT:-14201}"
SERVER_COUNT="${SERVER_COUNT:-3}"
REPLICA_COUNT="${REPLICA_COUNT:-3}"
DATA_RAFT_RAFT_PORT_DELTA="${DATA_RAFT_RAFT_PORT_DELTA:-1000}"
DATA_RAFT_SNAPSHOT_PORT_DELTA="${DATA_RAFT_SNAPSHOT_PORT_DELTA:-2000}"
MATRIXARK_RAFT_STORAGE_ASYNC="${MATRIXARK_RAFT_STORAGE_ASYNC:-true}"
LOCOMO_DATA_PATH="${LOCOMO_DATA_PATH:-/root/matrixark_benchmarks/data/locomo10.json}"
LONGMEMEVAL_DATA_PATH="${LONGMEMEVAL_DATA_PATH:-/root/matrixark_benchmarks/data/longmemeval_s_cleaned_official_hf.json}"
LOCOMO_QUESTION_LIMIT="${LOCOMO_QUESTION_LIMIT:-5}"
LOCOMO_CONVERSATION_LIMIT="${LOCOMO_CONVERSATION_LIMIT:-1}"
LONGMEMEVAL_QUESTION_LIMIT="${LONGMEMEVAL_QUESTION_LIMIT:-5}"
LONGMEMEVAL_SESSION_LIMIT="${LONGMEMEVAL_SESSION_LIMIT:-5}"
LONGMEMEVAL_SESSIONS_PER_ITEM_LIMIT="${LONGMEMEVAL_SESSIONS_PER_ITEM_LIMIT:-5}"
MATRIXARK_BATCH_SIZE="${MATRIXARK_BATCH_SIZE:-20}"
MATRIXARK_REQUEST_TIMEOUT_MS="${MATRIXARK_REQUEST_TIMEOUT_MS:-30000}"
MATRIXARK_IO_TIMEOUT_MS="${MATRIXARK_IO_TIMEOUT_MS:-30000}"

cleanup() {
  local status=$?
  if [[ -f "${RESULT_DIR}/bootstrap.pid" ]]; then
    kill "$(cat "${RESULT_DIR}/bootstrap.pid")" >/dev/null 2>&1 || true
  fi
  for pid_file in "${SMOKE_DIR}"/server*.pid "${SMOKE_DIR}"/metaserver*.pid; do
    [[ -f "${pid_file}" ]] || continue
    kill "$(cat "${pid_file}")" >/dev/null 2>&1 || true
  done
  wait >/dev/null 2>&1 || true
  return "${status}"
}
trap cleanup EXIT

need_file() {
  if [[ ! -x "$1" ]]; then
    echo "missing executable: $1" >&2
    exit 1
  fi
}

need_file "${OUT_DIR}/bcache2-server"
need_file "${OUT_DIR}/bcache2-metaserver"
[[ -f "${LOCOMO_DATA_PATH}" ]] || { echo "missing LOCOMO dataset: ${LOCOMO_DATA_PATH}" >&2; exit 1; }
[[ -f "${LONGMEMEVAL_DATA_PATH}" ]] || { echo "missing LongMemEval_s dataset: ${LONGMEMEVAL_DATA_PATH}" >&2; exit 1; }
mkdir -p "${RESULT_DIR}" "${ARTIFACT_DIR}"
rm -rf "${SMOKE_DIR}"

(
  cd "${ROOT}"
  env \
    BUILD_TYPE="${BUILD_TYPE}" \
    OUT_DIR="${OUT_DIR}" \
    SMOKE_DIR="${SMOKE_DIR}" \
    CLUSTER_NAME=matrixark_raft_dataset_slice \
    NAMESPACE_NAME="${NAMESPACE_NAME}" \
    TABLE_NAME="${TABLE_NAME}" \
    META_COUNT=1 \
    SERVER_COUNT="${SERVER_COUNT}" \
    REPLICA_COUNT="${REPLICA_COUNT}" \
    MS_PORT="${MS_PORT}" \
    MS_RAFT_PORT="$((MS_PORT + 10))" \
    MS_SNAPSHOT_PORT="$((MS_PORT + 20))" \
    SERVER_PORT="${SERVER_PORT}" \
    TABLE_ELECTION_POLICY=PROMOTE_SECONDARY \
    TABLE_PARTITION_UNIT_RELATION=ANTI_ENTROPY \
    SERVER_EXTRA_FLAGS="--data_replication_mode=raft_consensus --data_raft_work_dir=${SMOKE_DIR}/data-raft --data_raft_raft_port_delta=${DATA_RAFT_RAFT_PORT_DELTA} --data_raft_snapshot_port_delta=${DATA_RAFT_SNAPSHOT_PORT_DELTA} --data_raft_enable_empty_snapshot_for_tests=false --data_raft_read_mode=bounded_stale --data_raft_bounded_stale_max_index_lag=16 --data_raft_propose_timeout_ms=5000 --server_heartbeat_interval_ms=500 --server_heartbeat_timeout_ms=1000 --server_meta_tinker_interval_ms=500 --storage_async=${MATRIXARK_RAFT_STORAGE_ASYNC} --storage_enable_evict=false --storage_enable_expire=false --storage_enable_page_gc=false --storage_enable_page_compaction=false --storage_enable_index_gc=false --storage_enable_oplog_rolling=false" \
    KEEP_RUNNING=1 \
    bash tools/smoke_ubuntu22.sh
) > "${RESULT_DIR}/bootstrap.log" 2>&1 &
echo "$!" > "${RESULT_DIR}/bootstrap.pid"

for _ in $(seq 1 180); do
  if grep -q "KEEP_RUNNING=1" "${RESULT_DIR}/bootstrap.log" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$(cat "${RESULT_DIR}/bootstrap.pid")" >/dev/null 2>&1; then
    echo "bootstrap exited early" >&2
    cat "${RESULT_DIR}/bootstrap.log" >&2 || true
    exit 1
  fi
  sleep 1
done
if ! grep -q "KEEP_RUNNING=1" "${RESULT_DIR}/bootstrap.log"; then
  echo "bootstrap timed out" >&2
  tail -120 "${RESULT_DIR}/bootstrap.log" >&2 || true
  exit 1
fi
leader="$(awk '/metaserver leader:/ {print $3}' "${RESULT_DIR}/bootstrap.log" | tail -1)"
if [[ -z "${leader}" ]]; then
  echo "could not parse metaserver leader" >&2
  cat "${RESULT_DIR}/bootstrap.log" >&2
  exit 1
fi
{
  echo "leader=${leader}"
  echo "replication_mode=raft_consensus"
  echo "storage_async=${MATRIXARK_RAFT_STORAGE_ASYNC}"
  echo "server_count=${SERVER_COUNT}"
  echo "replica_count=${REPLICA_COUNT}"
  echo "result_dir=${RESULT_DIR}"
  echo "artifact_dir=${ARTIFACT_DIR}"
} | tee "${ARTIFACT_DIR}/raft_summary.txt"

run_benchmark() {
  local dataset="$1"
  local data_path="$2"
  local name="$3"
  shift 3
  local run_prefix="${name}_$(date +%Y%m%d_%H%M%S)"
  local run_dir="${ARTIFACT_DIR}/${name}"
  mkdir -p "${run_dir}"
  PYTHONPATH=. python3 tools/run_matrixark_dataset_benchmark.py \
    --dataset "${dataset}" \
    --data-path "${data_path}" \
    --artifact-dir "${run_dir}" \
    --artifact-prefix "${run_prefix}" \
    --backend temporalstore-direct \
    --metaserver "${leader}" \
    --namespace "${NAMESPACE_NAME}" \
    --table "${TABLE_NAME}" \
    --storage-prefix "matrixark:raft:${name}:$(date +%s)" \
    --batch-size "${MATRIXARK_BATCH_SIZE}" \
    --request-timeout-ms "${MATRIXARK_REQUEST_TIMEOUT_MS}" \
    --io-timeout-ms "${MATRIXARK_IO_TIMEOUT_MS}" \
    "$@" \
    > "${run_dir}/stdout.txt" 2> "${run_dir}/stderr.txt"
}

run_benchmark locomo "${LOCOMO_DATA_PATH}" "locomo_q${LOCOMO_QUESTION_LIMIT}" \
  --question-limit "${LOCOMO_QUESTION_LIMIT}" \
  --conversation-limit "${LOCOMO_CONVERSATION_LIMIT}"
run_benchmark longmemeval_s "${LONGMEMEVAL_DATA_PATH}" "longmemeval_q${LONGMEMEVAL_QUESTION_LIMIT}_s${LONGMEMEVAL_SESSION_LIMIT}" \
  --question-limit "${LONGMEMEVAL_QUESTION_LIMIT}" \
  --session-limit "${LONGMEMEVAL_SESSION_LIMIT}" \
  --sessions-per-item-limit "${LONGMEMEVAL_SESSIONS_PER_ITEM_LIMIT}"

python3 - "${ARTIFACT_DIR}" <<"PY"
import json, pathlib, sys
base = pathlib.Path(sys.argv[1])
summary = {"artifact_dir": str(base), "runs": {}}
for run_dir in sorted(p for p in base.iterdir() if p.is_dir()):
    reports = sorted(run_dir.glob("*.report.json"))
    if not reports:
        summary["runs"][run_dir.name] = {"status": "missing_report"}
        continue
    report = json.loads(reports[-1].read_text())
    summary["runs"][run_dir.name] = {
        "status": "ok",
        "report": str(reports[-1]),
        "dataset": report.get("dataset", {}),
        "scores": report.get("scores", {}),
        "latency": report.get("latency", {}),
        "artifacts": report.get("artifacts", {}),
    }
summary_path = base / "matrixark_raft_slice_summary.json"
summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True))
print(json.dumps(summary, indent=2, sort_keys=True))
PY

echo "PASS MatrixArk Raft dataset slice"
echo "${ARTIFACT_DIR}"
