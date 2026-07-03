#!/usr/bin/env bash
# MatrixArk context backfill CI gate for Ubuntu 22.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

RECORDS="${MATRIXARK_BACKFILL_CI_RECORDS:-128}"
BATCH_SIZES="${MATRIXARK_BACKFILL_CI_BATCH_SIZES:-32,64}"
INCREMENTAL_RECORDS="${MATRIXARK_BACKFILL_CI_INCREMENTAL_RECORDS:-32}"
REPEAT="${MATRIXARK_BACKFILL_CI_REPEAT:-2}"
JSON_OUTPUT="${MATRIXARK_BACKFILL_CI_JSON_OUTPUT:-matrixark_context_backfill_readiness.json}"
EVIDENCE_DIR="${MATRIXARK_BACKFILL_CI_EVIDENCE_DIR:-matrixark_context_backfill_evidence}"
EVIDENCE_MANIFEST="${MATRIXARK_BACKFILL_CI_EVIDENCE_MANIFEST:-${EVIDENCE_DIR}/manifest.json}"
EVIDENCE_MANIFEST_PROMETHEUS="${EVIDENCE_MANIFEST%.json}.prom"

python3 -m py_compile \
  tools/matrixark_context_backfill.py \
  tools/matrixark_context_backfill_benchmark.py \
  tools/matrixark_dual_write_ingestion_benchmark.py \
  tools/validate_matrixark_context_backfill_readiness.py \
  tools/validate_open_source_readiness.py \
  tools/verify_matrixark_context_backfill_ci_evidence.py

python3 tools/test_matrixark_context_backfill.py
python3 tools/test_matrixark_context_backfill_benchmark.py
python3 tools/test_matrixark_dual_write_ingestion_benchmark.py
python3 tools/test_verify_matrixark_context_backfill_ci_evidence.py
python3 tools/test_validate_matrixark_context_backfill_readiness.py
python3 tools/test_validate_open_source_readiness.py
python3 tools/validate_open_source_readiness.py
python3 tools/validate_matrixark_context_backfill_readiness.py \
  --records="${RECORDS}" \
  --batch-sizes="${BATCH_SIZES}" \
  --incremental-records="${INCREMENTAL_RECORDS}" \
  --repeat="${REPEAT}" \
  --dual-write-evidence-dir="${EVIDENCE_DIR}/dual_write" \
  --json-output="${JSON_OUTPUT}"

python3 - \
  "${JSON_OUTPUT}" \
  "${EVIDENCE_DIR}" \
  "${EVIDENCE_MANIFEST}" \
  "${RECORDS}" \
  "${BATCH_SIZES}" \
  "${INCREMENTAL_RECORDS}" \
  "${REPEAT}" <<'PY'
import hashlib
import json
import os
import sys
import time
from pathlib import Path

readiness_path = Path(sys.argv[1])
evidence_dir = Path(sys.argv[2])
manifest_path = Path(sys.argv[3])
records = int(sys.argv[4])
batch_sizes = sys.argv[5]
incremental_records = int(sys.argv[6])
repeat = int(sys.argv[7])

def artifact(path: Path) -> dict:
    stored_path = os.path.relpath(path.resolve(), manifest_path.parent.resolve())
    return {
        "path": stored_path,
        "bytes": path.stat().st_size,
        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
    }

artifacts = {
    "matrixark_context_backfill_readiness_json": artifact(readiness_path),
}
dual_write_dir = evidence_dir / "dual_write"
for name in ["dual_write_readiness.json", "dual_write_readiness.prom", "manifest.json"]:
    path = dual_write_dir / name
    if path.exists():
        artifacts[f"dual_write_{name.replace('.', '_')}"] = artifact(path)

payload = {
    "schema": "matrixark_context_backfill_ci_evidence_manifest_v1",
    "generated_at_ms": int(time.time() * 1000),
    "status": "ok",
    "records": records,
    "batch_sizes": batch_sizes,
    "incremental_records": incremental_records,
    "repeat": repeat,
    "artifacts": artifacts,
}
manifest_path.parent.mkdir(parents=True, exist_ok=True)
manifest_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
PY

python3 tools/verify_matrixark_context_backfill_ci_evidence.py \
  --manifest="${EVIDENCE_MANIFEST}" \
  --require-relative-paths=1 \
  --prometheus-output="${EVIDENCE_MANIFEST_PROMETHEUS}"
grep -q 'matrixark_context_backfill_ci_evidence_verification_status' "${EVIDENCE_MANIFEST_PROMETHEUS}"
grep -q 'check="readiness_status_ok"' "${EVIDENCE_MANIFEST_PROMETHEUS}"
grep -q 'check="readiness_checks_all_passed"' "${EVIDENCE_MANIFEST_PROMETHEUS}"
grep -q 'check="readiness_required_sections_ok"' "${EVIDENCE_MANIFEST_PROMETHEUS}"
grep -q 'check="dual_write_readiness_status_ok"' "${EVIDENCE_MANIFEST_PROMETHEUS}"
grep -q 'check="dual_write_manifest_schema_supported"' "${EVIDENCE_MANIFEST_PROMETHEUS}"
grep -q 'check="dual_write_manifest_status_ok"' "${EVIDENCE_MANIFEST_PROMETHEUS}"
grep -q 'check="dual_write_manifest_required_artifacts_present"' "${EVIDENCE_MANIFEST_PROMETHEUS}"
grep -q 'check="dual_write_manifest_artifact_paths_relative"' "${EVIDENCE_MANIFEST_PROMETHEUS}"
grep -q 'check="dual_write_manifest_artifact_paths_within_dir"' "${EVIDENCE_MANIFEST_PROMETHEUS}"
grep -q 'check="dual_write_manifest_artifact_paths_readable"' "${EVIDENCE_MANIFEST_PROMETHEUS}"
grep -q 'check="dual_write_manifest_artifact_sizes_match"' "${EVIDENCE_MANIFEST_PROMETHEUS}"
grep -q 'check="dual_write_manifest_artifact_sha256_match"' "${EVIDENCE_MANIFEST_PROMETHEUS}"

echo "matrixark_context_backfill_ci_gate_status=ok"
echo "matrixark_context_backfill_readiness_json=${JSON_OUTPUT}"
echo "matrixark_context_backfill_evidence_dir=${EVIDENCE_DIR}"
echo "matrixark_context_backfill_evidence_manifest=${EVIDENCE_MANIFEST}"
echo "matrixark_context_backfill_evidence_manifest_prometheus=${EVIDENCE_MANIFEST_PROMETHEUS}"
