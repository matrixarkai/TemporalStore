#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${BUILD_DIR:-${ROOT}/build-ubuntu22/release}"
OUT_DIR="${OUT_DIR:-${ROOT}/output-ubuntu22/release}"
RESULT_ROOT="${RESULT_ROOT:-/tmp/temporalstore-redis-production-gate}"
REPEAT="${REPEAT:-2}"
BASE_PORT="${BASE_PORT:-23500}"
RUN_BENCH="${RUN_BENCH:-1}"
BENCH_REQUESTS="${BENCH_REQUESTS:-1000}"
BENCH_CLIENTS="${BENCH_CLIENTS:-8}"
BENCH_KEYSPACE="${BENCH_KEYSPACE:-100000}"
REDIS_BENCH_MIN_OVERALL_QPS="${REDIS_BENCH_MIN_OVERALL_QPS:-0}"

mkdir -p "${RESULT_ROOT}"

echo "== Redis production gate: open-source surface manifest == "
cp "${ROOT}/compat/redis_open_source_surface_manifest.json" \
  "${RESULT_ROOT}/redis_open_source_surface_manifest.json"
cp "${ROOT}/compat/redis_cpp_rust_surface_parity_contract.json" \
  "${RESULT_ROOT}/redis_cpp_rust_surface_parity_contract.json"
python3 "${ROOT}/tools/validate_open_source_surface.py" \
  | tee "${RESULT_ROOT}/redis_open_source_surface_validation.txt"
python3 "${ROOT}/tools/validate_redis_cpp_rust_surface_consistency.py" \
  | tee "${RESULT_ROOT}/redis_cpp_rust_surface_consistency.txt"
python3 "${ROOT}/tools/validate_matrixobjectstore_names.py" \
  | tee "${RESULT_ROOT}/matrixobject_name_validation.txt"

echo "== Redis production gate: release build =="
cmake --build "${BUILD_DIR}" --target bcache2-server -j "${BUILD_JOBS:-2}"

echo "== Redis production gate: no fake OK audit =="
if grep -n 'handler_ == nullptr' "${ROOT}/src/server/redis_command_handler.cc" | grep -vq 'handler_ == nullptr'; then
  echo "unexpected handler null path audit failure" >&2
  exit 1
fi
if grep -n 'output->SetStatus("OK")' "${ROOT}/src/server/redis_command_handler.cc"; then
  echo "Redis null-handler path must not fake OK" >&2
  exit 1
fi
if grep -n 'CONFIG REWRITE.*SetStatus("OK")\|SLAVEOF.*SetStatus("OK")' \
    "${ROOT}/src/server/redis_command_handler.cc"; then
  echo "Redis management unsupported paths must not fake OK" >&2
  exit 1
fi
if grep -n 'nullptr' "${ROOT}/src/server/redis_service.cc"; then
  echo "Redis commands must be explicitly handled or explicitly unsupported; nullptr handlers are forbidden" >&2
  exit 1
fi

for i in $(seq 1 "${REPEAT}"); do
  port_base=$((BASE_PORT + i * 100))
  result_dir="${RESULT_ROOT}/run-${i}"
  smoke_dir="/tmp/temporalstore-redis-production-gate-${i}"
  cluster_name="redis-production-gate-${i}-$$"

  echo "== Redis production gate: live storage smoke run ${i}/${REPEAT} =="
  RUN_COMPAT_SMOKE=1 \
    REDIS_COMPAT_SURFACE=trimmed \
    REDIS_EXPECT_UNSUPPORTED_COLLECTIONS=1 \
    RUN_BENCH="${RUN_BENCH}" \
    BENCH_REQUESTS="${BENCH_REQUESTS}" \
    BENCH_CLIENTS="${BENCH_CLIENTS}" \
    BENCH_KEYSPACE="${BENCH_KEYSPACE}" \
    REDIS_BENCH_MIN_OVERALL_QPS="${REDIS_BENCH_MIN_OVERALL_QPS}" \
    CLUSTER_NAME="${cluster_name}" \
    MS_PORT="${port_base}" \
    MS_RAFT_PORT="$((port_base + 10))" \
    MS_SNAPSHOT_PORT="$((port_base + 20))" \
    SERVER_PORT="$((port_base + 1))" \
    SERVER_OUT_DIR="${OUT_DIR}" \
    METASERVER_OUT_DIR="${OUT_DIR}" \
    SMOKE_DIR="${smoke_dir}" \
    RESULT_DIR="${result_dir}" \
    "${ROOT}/tools/run_redis_live_storage_smoke_ubuntu22.sh"
done

python3 - "${RESULT_ROOT}" "${REPEAT}" <<'LIVEROLLUPPY'
import json
import sys
from pathlib import Path

result_root = Path(sys.argv[1])
repeat = int(sys.argv[2])
summaries = []
for i in range(1, repeat + 1):
    path = result_root / f"run-{i}" / "redis-live-storage-smoke-summary.json"
    if not path.exists():
        raise SystemExit(f"missing Redis live storage smoke summary for production gate run {i}: {path}")
    summary = json.loads(path.read_text(encoding="utf-8"))
    summaries.append(summary)

surface = summaries[0].get("redis_surface")
manifest_sha = summaries[0].get("redis_surface_manifest_sha256")
command_count = summaries[0].get("command_count")
for i, summary in enumerate(summaries, start=1):
    for field, expected in (
        ("schema", "temporalstore_trimmed_redis_live_storage_smoke_summary_v1"),
        ("redis_surface", surface),
        ("redis_surface_manifest_sha256", manifest_sha),
        ("command_count", command_count),
        ("unsupported_collections_expected", True),
    ):
        if summary.get(field) != expected:
            raise SystemExit(
                f"live smoke summary run {i} has {field}={summary.get(field)!r}, expected {expected!r}"
            )

rollup = {
    "schema": "temporalstore_trimmed_redis_live_storage_smoke_rollup_v1",
    "run_count": len(summaries),
    "redis_surface": surface,
    "redis_surface_manifest_sha256": manifest_sha,
    "command_count": command_count,
    "unsupported_collections_expected": True,
    "runs": [
        {
            "run": i,
            "summary": str(result_root / f"run-{i}" / "redis-live-storage-smoke-summary.json"),
            "benchmark_summary": summary.get("benchmark_summary"),
        }
        for i, summary in enumerate(summaries, start=1)
    ],
}
(result_root / "redis-live-storage-smoke-rollup.json").write_text(
    json.dumps(rollup, indent=2, sort_keys=True) + "\n",
    encoding="utf-8",
)
LIVEROLLUPPY

if [[ "${RUN_BENCH}" == "1" ]]; then
  python3 - "${RESULT_ROOT}" "${REPEAT}" <<'ROLLUPPY'
import json
import sys
from pathlib import Path

result_root = Path(sys.argv[1])
repeat = int(sys.argv[2])
summaries = []
for i in range(1, repeat + 1):
    path = result_root / f"run-{i}" / "redis-benchmark-summary.json"
    if not path.exists():
        raise SystemExit(f"missing redis benchmark summary for production gate run {i}: {path}")
    summary = json.loads(path.read_text(encoding="utf-8"))
    summaries.append(summary)

if not summaries:
    raise SystemExit("no Redis benchmark summaries found for production gate rollup")

surface = summaries[0].get("redis_surface")
manifest_sha = summaries[0].get("redis_surface_manifest_sha256")
expected_commands = summaries[0].get("expected_benchmark_commands")
min_qps_threshold = summaries[0].get("min_overall_qps_threshold")
for i, summary in enumerate(summaries, start=1):
    for field, expected in (
        ("redis_surface", surface),
        ("redis_surface_manifest_sha256", manifest_sha),
        ("expected_benchmark_commands", expected_commands),
        ("min_overall_qps_threshold", min_qps_threshold),
    ):
        if summary.get(field) != expected:
            raise SystemExit(
                f"benchmark summary run {i} has {field}={summary.get(field)!r}, expected {expected!r}"
            )

overall_mins = [summary["requests_per_second_overall_min"] for summary in summaries]
overall_avgs = [summary["requests_per_second_overall_avg"] for summary in summaries]
rollup = {
    "schema": "temporalstore_trimmed_redis_production_benchmark_rollup_v1",
    "run_count": len(summaries),
    "redis_surface": surface,
    "redis_surface_manifest_sha256": manifest_sha,
    "expected_benchmark_commands": expected_commands,
    "min_overall_qps_threshold": min_qps_threshold,
    "requests_per_second_overall_min_min": min(overall_mins),
    "requests_per_second_overall_min_max": max(overall_mins),
    "requests_per_second_overall_avg_avg": sum(overall_avgs) / len(overall_avgs),
    "runs": [
        {
            "run": i,
            "requests_per_second_overall_min": summary["requests_per_second_overall_min"],
            "requests_per_second_overall_avg": summary["requests_per_second_overall_avg"],
            "benchmark_command_count": summary["benchmark_command_count"],
            "min_overall_qps_threshold": summary["min_overall_qps_threshold"],
        }
        for i, summary in enumerate(summaries, start=1)
    ],
}
(result_root / "redis-production-benchmark-rollup.json").write_text(
    json.dumps(rollup, indent=2, sort_keys=True) + "\n",
    encoding="utf-8",
)
ROLLUPPY
fi

python3 - "${RESULT_ROOT}" "${REPEAT}" "${RUN_BENCH}" <<'GATESUMMARYPY'
import hashlib
import json
import sys
from pathlib import Path

result_root = Path(sys.argv[1])
repeat = int(sys.argv[2])
run_bench = sys.argv[3] == "1"

manifest_path = result_root / "redis_open_source_surface_manifest.json"
surface_validation_path = result_root / "redis_open_source_surface_validation.txt"
matrixobject_validation_path = result_root / "matrixobject_name_validation.txt"
benchmark_rollup_path = result_root / "redis-production-benchmark-rollup.json"
live_rollup_path = result_root / "redis-live-storage-smoke-rollup.json"

for path in (manifest_path, surface_validation_path, matrixobject_validation_path, live_rollup_path):
    if not path.exists():
        raise SystemExit(f"missing Redis production gate evidence artifact: {path}")

surface_validation = surface_validation_path.read_text(encoding="utf-8", errors="replace")
matrixobject_validation = matrixobject_validation_path.read_text(encoding="utf-8", errors="replace")
if "open-source surface validation passed" not in surface_validation:
    raise SystemExit(f"Redis surface validation did not pass: {surface_validation_path}")
if "matrixobject_names: ok" not in matrixobject_validation:
    raise SystemExit(f"MatrixObject naming validation did not pass: {matrixobject_validation_path}")
if run_bench and not benchmark_rollup_path.exists():
    raise SystemExit(f"missing Redis production benchmark rollup: {benchmark_rollup_path}")

manifest_bytes = manifest_path.read_bytes()
manifest = json.loads(manifest_bytes.decode("utf-8"))
gate_artifacts = manifest.get("production_gate_artifacts", {})
for artifact_name in gate_artifacts.get("required", []):
    if artifact_name == "redis-production-gate-summary.json":
        continue
    artifact_path = result_root / artifact_name
    if not artifact_path.exists():
        raise SystemExit(f"missing manifest-declared production gate artifact: {artifact_path}")
for artifact_name in gate_artifacts.get("benchmark_enabled", []):
    artifact_path = result_root / artifact_name
    if run_bench and not artifact_path.exists():
        raise SystemExit(f"missing manifest-declared benchmark production gate artifact: {artifact_path}")
summary = {
    "schema": "temporalstore_trimmed_redis_production_gate_summary_v1",
    "result_root": str(result_root),
    "run_count": repeat,
    "run_bench": run_bench,
    "redis_surface": manifest.get("surface"),
    "redis_surface_schema": manifest.get("schema"),
    "redis_surface_manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
    "production_gate_artifacts": gate_artifacts,
    "open_source_surface_validation": {
        "path": str(surface_validation_path),
        "status": "passed",
    },
    "matrixobject_name_validation": {
        "path": str(matrixobject_validation_path),
        "status": "passed",
    },
    "matrixobject_boundary": "below_temporalstore_storage_backfill_no_redis_api_expansion",
    "benchmark_rollup": str(benchmark_rollup_path) if benchmark_rollup_path.exists() else None,
    "live_storage_smoke_rollup": str(live_rollup_path),
    "unsupported_collections_expected": True,
}
(result_root / "redis-production-gate-summary.json").write_text(
    json.dumps(summary, indent=2, sort_keys=True) + "\n",
    encoding="utf-8",
)
GATESUMMARYPY

echo "PASS Redis production gate"
echo "${RESULT_ROOT}"
