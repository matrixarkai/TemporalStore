#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

BUILD_TYPE="${BUILD_TYPE:-Release}"
DATASET="${DATASET:-locomo}"
DATA_PATH="${DATA_PATH:-/root/matrixark_benchmarks/data/locomo10.json}"
QUESTION_LIMIT="${QUESTION_LIMIT:-100}"
BATCH_SIZE="${BATCH_SIZE:-40}"
MAX_CONTEXT_TOKENS="${MAX_CONTEXT_TOKENS:-1200}"
REQUEST_TIMEOUT_MS="${REQUEST_TIMEOUT_MS:-120000}"
IO_TIMEOUT_MS="${IO_TIMEOUT_MS:-120000}"
RESULT_ROOT="${RESULT_ROOT:-/tmp/matrixark-async-storage-matrix-$(date +%Y%m%d_%H%M%S)}"
DOC_PATH="${DOC_PATH:-${ROOT}/docs/matrixark_async_storage_matrix_20260623.md}"
JSON_PATH="${JSON_PATH:-${ROOT}/docs/matrixark_async_storage_matrix_20260623.json}"
RUN_3NODE="${RUN_3NODE:-1}"
RUN_SHARDED="${RUN_SHARDED:-1}"
RUN_BATCH="${RUN_BATCH:-1}"
RUN_RAFT="${RUN_RAFT:-1}"
SHARD_COUNT="${SHARD_COUNT:-3}"
SHARD_QUESTION_LIMIT="${SHARD_QUESTION_LIMIT:-40}"
RAFT_OPS="${RAFT_OPS:-1000}"
RAFT_THREAD_LIST="${RAFT_THREAD_LIST:-1 2}"
SERVER_COUNT="${SERVER_COUNT:-3}"
REPLICA_COUNT="${REPLICA_COUNT:-1}"
MS_PORT="${MS_PORT:-18000}"
SERVER_PORT="${SERVER_PORT:-18001}"
CLUSTER_NAME="${CLUSTER_NAME:-matrixark_async_matrix}"
NAMESPACE_NAME="${NAMESPACE_NAME:-deploy_ns}"
TABLE_NAME="${TABLE_NAME:-deploy_table}"

mkdir -p "${RESULT_ROOT}" "$(dirname "${DOC_PATH}")"

log() {
  printf '[matrixark-matrix] %s\n' "$*"
}

start_cluster() {
  local server_count="$1"
  local replica_count="$2"
  log "starting C++ TemporalStore: servers=${server_count} replicas=${replica_count} async=true"
  (
    cd "${ROOT}"
    env \
      BUILD_TYPE="${BUILD_TYPE}" \
      TEMPORALSTORE_STORAGE_ASYNC=true \
      TEMPORALSTORE_STORAGE_OPLOG_DELAY_DUMP_LENGTH="${TEMPORALSTORE_STORAGE_OPLOG_DELAY_DUMP_LENGTH:-10000}" \
      SERVER_COUNT="${server_count}" \
      REPLICA_COUNT="${replica_count}" \
      MS_PORT="${MS_PORT}" \
      SERVER_PORT="${SERVER_PORT}" \
      CLUSTER_NAME="${CLUSTER_NAME}" \
      NAMESPACE_NAME="${NAMESPACE_NAME}" \
      TABLE_NAME="${TABLE_NAME}" \
      bash ./tools/deploy_local_ubuntu22.sh start
  ) | tee "${RESULT_ROOT}/deploy_${server_count}node.log"
}

stop_cluster() {
  (
    cd "${ROOT}"
    env \
      CLUSTER_NAME="${CLUSTER_NAME}" \
      NAMESPACE_NAME="${NAMESPACE_NAME}" \
      TABLE_NAME="${TABLE_NAME}" \
      bash ./tools/deploy_local_ubuntu22.sh stop
  ) >/dev/null 2>&1 || true
}

run_dataset() {
  local name="$1"
  local prefix="$2"
  local qlimit="$3"
  local artifact_dir="${RESULT_ROOT}/${name}"
  mkdir -p "${artifact_dir}"
  log "running dataset case=${name} prefix=${prefix} questions=${qlimit}"
  (
    cd "${ROOT}"
    env \
      MATRIXARK_DIRECT_AUDIT_MODE=deferred \
      MATRIXARK_DIRECT_WRITE_RETRIES="${MATRIXARK_DIRECT_WRITE_RETRIES:-5}" \
      MATRIXARK_DIRECT_WRITE_BACKOFF_MS="${MATRIXARK_DIRECT_WRITE_BACKOFF_MS:-50}" \
      MATRIXARK_DIRECT_RECORD_BUNDLE_MAX_BYTES="${MATRIXARK_DIRECT_RECORD_BUNDLE_MAX_BYTES:-65536}" \
      python3 tools/run_matrixark_dataset_benchmark.py \
        --dataset "${DATASET}" \
        --data-path "${DATA_PATH}" \
        --artifact-dir "${artifact_dir}" \
        --artifact-prefix "${name}" \
        --backend temporalstore-direct \
        --metaserver "127.0.0.1:${MS_PORT}" \
        --namespace "${NAMESPACE_NAME}" \
        --table "${TABLE_NAME}" \
        --storage-prefix "${prefix}" \
        --request-timeout-ms "${REQUEST_TIMEOUT_MS}" \
        --io-timeout-ms "${IO_TIMEOUT_MS}" \
        --batch-size "${BATCH_SIZE}" \
        --max-context-tokens "${MAX_CONTEXT_TOKENS}" \
        --question-limit "${qlimit}"
  ) > "${artifact_dir}/stdout.txt" 2> "${artifact_dir}/stderr.txt"
}

extract_report() {
  local name="$1"
  local artifact_dir="${RESULT_ROOT}/${name}"
  python3 - "${artifact_dir}" "${name}" <<'PY'
import json
import pathlib
import sys

artifact_dir = pathlib.Path(sys.argv[1])
name = sys.argv[2]
report_files = list(artifact_dir.glob("*.report.json"))
if not report_files:
    print(json.dumps({"name": name, "status": "missing_report", "artifact_dir": str(artifact_dir)}))
    raise SystemExit(0)
report = json.loads(report_files[0].read_text(encoding="utf-8"))
lat = report.get("latency", {})
ing = report.get("ingestion", {})
scores = report.get("scores", {})
dataset = report.get("dataset", {})
print(json.dumps({
    "name": name,
    "status": "completed",
    "artifact_dir": str(artifact_dir),
    "dataset": dataset.get("name"),
    "questions": dataset.get("questions_run"),
    "sessions": dataset.get("sessions") or dataset.get("sessions_ingested"),
    "turns": dataset.get("turns_ingested"),
    "ingestion_elapsed_ms": ing.get("elapsed_ms") or lat.get("ingestion_elapsed_ms"),
    "throughput_turns_per_sec": ing.get("turns_per_sec") or lat.get("ingestion_throughput_turns_per_sec"),
    "p50_retrieval_ms": lat.get("p50_latency_ms"),
    "p95_retrieval_ms": lat.get("p95_latency_ms"),
    "context_recall": scores.get("context_recall"),
    "final_judge_score": scores.get("final_judge_score"),
    "answer_support_hit": scores.get("answer_support_hit"),
    "compression_answer_hidden_count": scores.get("compression_answer_hidden_count"),
}))
PY
}

summaries=()
trap stop_cluster EXIT

if [[ "${RUN_3NODE}" == "1" || "${RUN_SHARDED}" == "1" || "${RUN_BATCH}" == "1" ]]; then
  stop_cluster
  start_cluster "${SERVER_COUNT}" "${REPLICA_COUNT}"
fi

if [[ "${RUN_3NODE}" == "1" ]]; then
  case_name="${DATASET}_async_3node"
  prefix="matrixark:bench:async3:${DATASET}:$(date +%Y%m%d_%H%M%S)"
  run_dataset "${case_name}" "${prefix}" "${QUESTION_LIMIT}"
  summaries+=("$(extract_report "${case_name}")")
fi

if [[ "${RUN_SHARDED}" == "1" ]]; then
  for shard in $(seq 0 $((SHARD_COUNT - 1))); do
    case_name="${DATASET}_async_3node_shard${shard}"
    prefix="matrixark:bench:async3sharded:${DATASET}:$(date +%Y%m%d_%H%M%S):shard${shard}"
    run_dataset "${case_name}" "${prefix}" "${SHARD_QUESTION_LIMIT}"
    summaries+=("$(extract_report "${case_name}")")
  done
fi

if [[ "${RUN_BATCH}" == "1" ]]; then
  case_name="${DATASET}_async_3node_batch_bundle"
  prefix="matrixark:bench:async3batch:${DATASET}:$(date +%Y%m%d_%H%M%S)"
  MATRIXARK_DIRECT_RECORD_BUNDLE_MAX_BYTES="${MATRIXARK_DIRECT_RECORD_BUNDLE_MAX_BYTES:-131072}" \
    run_dataset "${case_name}" "${prefix}" "${QUESTION_LIMIT}"
  summaries+=("$(extract_report "${case_name}")")
fi

if [[ "${RUN_RAFT}" == "1" ]]; then
  stop_cluster
  raft_dir="${RESULT_ROOT}/raft_storage_gate"
  log "running Raft storage HA/correctness gate"
  set +e
  (
    cd "${ROOT}"
    env \
      BUILD_TYPE="${BUILD_TYPE}" \
      RESULT_DIR="${raft_dir}" \
      OPS="${RAFT_OPS}" \
      THREAD_LIST="${RAFT_THREAD_LIST}" \
      BENCH_TIMEOUT_S="${RAFT_BENCH_TIMEOUT_S:-180}" \
      bash tools/run_data_raft_2node_scale_ubuntu22.sh
  ) > "${RESULT_ROOT}/raft_stdout.txt" 2> "${RESULT_ROOT}/raft_stderr.txt"
  raft_code=$?
  set -e
  summaries+=("{\"name\":\"raft_storage_gate\",\"status\":\"$([[ "${raft_code}" == "0" ]] && echo completed || echo failed)\",\"exit_code\":${raft_code},\"artifact_dir\":\"${raft_dir}\"}")
fi

python3 - "${JSON_PATH}" "${DOC_PATH}" "${RESULT_ROOT}" "${summaries[@]}" <<'PY'
import json
import pathlib
import sys
from datetime import datetime, timezone

json_path = pathlib.Path(sys.argv[1])
doc_path = pathlib.Path(sys.argv[2])
result_root = sys.argv[3]
summaries = [json.loads(item) for item in sys.argv[4:]]
payload = {
    "generated_at": datetime.now(timezone.utc).isoformat(),
    "result_root": result_root,
    "summaries": summaries,
}
json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

lines = [
    "# MatrixArk Async Storage Multi-Node Benchmark Matrix - 2026-06-23",
    "",
    "## Purpose",
    "",
    "This run follows the staged plan for high-throughput MatrixArk context ingestion on C++ TemporalStore:",
    "",
    "1. async storage plus three data nodes, no data Raft;",
    "2. async storage plus sharded MatrixArk prefixes;",
    "3. async storage plus larger record bundles as the current native-batch stand-in;",
    "4. Raft storage gate after non-Raft throughput is stable.",
    "",
    "Async oplog remains the recommended high-throughput context-ingestion setting. Raft is treated as the HA/correctness profile, not the first throughput tuning knob.",
    "",
    f"Result root: `{result_root}`",
    "",
    "## Results",
    "",
    "| Case | Status | Questions | Turns | Throughput turns/sec | p50 retrieval ms | p95 retrieval ms | Context recall | Judge score | Artifacts |",
    "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
]
for row in summaries:
    lines.append(
        "| {name} | {status} | {questions} | {turns} | {throughput} | {p50} | {p95} | {recall} | {judge} | `{notes}` |".format(
            name=row.get("name", ""),
            status=row.get("status", ""),
            questions=row.get("questions", ""),
            turns=row.get("turns", ""),
            throughput=("" if row.get("throughput_turns_per_sec") is None else f"{row.get('throughput_turns_per_sec'):.2f}"),
            p50=("" if row.get("p50_retrieval_ms") is None else f"{row.get('p50_retrieval_ms'):.2f}"),
            p95=("" if row.get("p95_retrieval_ms") is None else f"{row.get('p95_retrieval_ms'):.2f}"),
            recall=("" if row.get("context_recall") is None else f"{row.get('context_recall'):.4f}"),
            judge=("" if row.get("final_judge_score") is None else f"{row.get('final_judge_score'):.4f}"),
            notes=row.get("artifact_dir", ""),
        )
    )
lines.extend([
    "",
    "## Interpretation",
    "",
    "- Multi-node, non-Raft async storage is the next throughput baseline after the single-node async-oplog LOCOMO pass.",
    "- Sharded MatrixArk prefixes reduce hot-prefix pressure and make future partition-aware routing easier.",
    "- The current batch path uses MatrixArk record bundles. A true native server-side batch append remains the next code-level storage improvement.",
    "- Raft should be benchmarked after non-Raft throughput is stable because it answers a different question: HA and correctness under replication.",
    "",
    "## Reproduce",
    "",
    "```bash",
    "cd /root/src/github-services/TemporalStore",
    "BUILD_TYPE=Release \\",
    "DATASET=locomo \\",
    "DATA_PATH=/root/matrixark_benchmarks/data/locomo10.json \\",
    "QUESTION_LIMIT=100 \\",
    "BATCH_SIZE=40 \\",
    "RUN_3NODE=1 RUN_SHARDED=1 RUN_BATCH=1 RUN_RAFT=1 \\",
    "bash tools/run_matrixark_async_storage_matrix.sh",
    "```",
    "",
])
doc_path.write_text("\n".join(lines), encoding="utf-8")
PY

log "wrote ${DOC_PATH}"
log "wrote ${JSON_PATH}"
