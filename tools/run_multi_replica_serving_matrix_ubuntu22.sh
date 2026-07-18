#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RESULT_DIR="${RESULT_DIR:-/tmp/temporalstore-multi-replica-serving-$(date +%Y%m%d_%H%M%S)}"
BUILD_TYPE="${BUILD_TYPE:-Release}"
REPLICATION_RESULT_DIR="${REPLICATION_RESULT_DIR:-${RESULT_DIR}/replication}"
RAW_RESULT_DIR="${RAW_RESULT_DIR:-${RESULT_DIR}/raw_backends}"
SUMMARY_JSON="${SUMMARY_JSON:-${RESULT_DIR}/summary.json}"
SUMMARY_MD="${SUMMARY_MD:-${RESULT_DIR}/summary.md}"
PROMETHEUS_FILE="${PROMETHEUS_FILE:-${RESULT_DIR}/multi_replica_serving.prom}"

OPS="${OPS:-2000}"
REPLICA_OPS="${REPLICA_OPS:-${OPS}}"
REPLICA_WAIT_MS="${REPLICA_WAIT_MS:-5000}"
THREAD_LIST="${THREAD_LIST:-2 4}"
VALUE_BYTES="${VALUE_BYTES:-256}"
RUN_DOCKER_REPLICATION="${RUN_DOCKER_REPLICATION:-1}"
RUN_RAW_BACKENDS="${RUN_RAW_BACKENDS:-1}"
RUN_FAILOVER="${RUN_FAILOVER:-0}"
RAW_RECORDS="${RAW_RECORDS:-2000}"
RAW_WORKERS="${RAW_WORKERS:-4}"
RAW_BATCH_SIZE="${RAW_BATCH_SIZE:-128}"
RAW_PAYLOAD_BYTES="${RAW_PAYLOAD_BYTES:-256}"
RAW_BACKENDS="${RAW_BACKENDS:-temporalstore,matrixkv,objectstore}"
MAX_SECONDARY_VISIBILITY_P95_MS="${MAX_SECONDARY_VISIBILITY_P95_MS:-0}"
MIN_REPLICA_READ_QPS="${MIN_REPLICA_READ_QPS:-0}"
MIN_OBJECTSTORE_QPS_RATIO="${MIN_OBJECTSTORE_QPS_RATIO:-0}"

mkdir -p "${RESULT_DIR}" "${REPLICATION_RESULT_DIR}" "${RAW_RESULT_DIR}"

log() {
  printf '[%(%Y-%m-%dT%H:%M:%SZ)T] %s\n' -1 "$*" | tee -a "${RESULT_DIR}/run.log"
}

run_replication_matrix() {
  if [[ "${RUN_DOCKER_REPLICATION}" != "1" ]]; then
    log "skipping replication matrix because RUN_DOCKER_REPLICATION=${RUN_DOCKER_REPLICATION}"
    return 0
  fi
  local docker_inside=0
  if [[ ! -S /var/run/docker.sock ]]; then
    docker_inside=1
    log "Docker socket is unavailable; running replication matrix directly on the local Ubuntu host"
  else
    log "running local Docker replication matrix: shared_async, shared_sync, raft"
  fi
  env \
    TEMPORALSTORE_DOCKER_INSIDE="${docker_inside}" \
    RESULT_DIR="${REPLICATION_RESULT_DIR}" \
    BUILD_TYPE="${BUILD_TYPE}" \
    OPS="${OPS}" \
    REPLICA_OPS="${REPLICA_OPS}" \
    REPLICA_WAIT_MS="${REPLICA_WAIT_MS}" \
    THREAD_LIST="${THREAD_LIST}" \
    VALUE_BYTES="${VALUE_BYTES}" \
    RUN_FAILOVER="${RUN_FAILOVER}" \
    bash "${ROOT}/tools/run_local_docker_replication_matrix_ubuntu22.sh"
}

run_raw_backend_sweep() {
  if [[ "${RUN_RAW_BACKENDS}" != "1" ]]; then
    log "skipping raw backend sweep because RUN_RAW_BACKENDS=${RUN_RAW_BACKENDS}"
    return 0
  fi
  log "running raw backend sweep including MatrixObjectStore-compatible objectstore mode"
  python3 "${ROOT}/tools/matrixark_dual_write_ingestion_benchmark.py" \
    --mode local \
    --records "${RAW_RECORDS}" \
    --workers "${RAW_WORKERS}" \
    --batch-size "${RAW_BATCH_SIZE}" \
    --payload-bytes "${RAW_PAYLOAD_BYTES}" \
    --raw-backends "${RAW_BACKENDS}" \
    --min-backend-qps-ratio "${MIN_OBJECTSTORE_QPS_RATIO}" \
    --require-dual-write-counts 1 \
    --json-output "${RAW_RESULT_DIR}/raw_backend_sweep.json" \
    --prometheus-output "${RAW_RESULT_DIR}/raw_backend_sweep.prom" \
    > "${RAW_RESULT_DIR}/raw_backend_sweep.stdout" \
    2> "${RAW_RESULT_DIR}/raw_backend_sweep.stderr"
}

run_replication_matrix
run_raw_backend_sweep

python3 - "${REPLICATION_RESULT_DIR}" "${RAW_RESULT_DIR}" "${SUMMARY_JSON}" "${SUMMARY_MD}" "${PROMETHEUS_FILE}" \
  "${MAX_SECONDARY_VISIBILITY_P95_MS}" "${MIN_REPLICA_READ_QPS}" "${MIN_OBJECTSTORE_QPS_RATIO}" <<'PY'
import csv
import json
import pathlib
import sys
from typing import Any

replication_dir = pathlib.Path(sys.argv[1])
raw_dir = pathlib.Path(sys.argv[2])
summary_json = pathlib.Path(sys.argv[3])
summary_md = pathlib.Path(sys.argv[4])
prom_file = pathlib.Path(sys.argv[5])
max_visibility_p95_ms = float(sys.argv[6] or 0)
min_replica_read_qps = float(sys.argv[7] or 0)
min_objectstore_qps_ratio = float(sys.argv[8] or 0)
Json = dict[str, Any]

def read_csv(path: pathlib.Path) -> list[Json]:
    if not path.exists():
        return []
    with path.open(encoding="utf-8") as fh:
        return list(csv.DictReader(fh))

def read_json(path: pathlib.Path) -> Json:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))

def f(value: Any) -> float:
    try:
        return float(value)
    except Exception:
        return 0.0

def mode_table(rows: list[Json]) -> dict[str, Json]:
    out: dict[str, Json] = {}
    for row in rows:
        mode = str(row.get("mode") or "")
        if not mode:
            continue
        item = out.setdefault(mode, {"mode": mode, "thread_results": [], "max_p95_ms": 0.0, "min_qps": 0.0})
        item["thread_results"].append(row)
        qps = f(row.get("qps"))
        p95_ms = f(row.get("p95_us")) / 1000.0
        item["max_p95_ms"] = max(float(item["max_p95_ms"]), p95_ms)
        item["min_qps"] = qps if float(item["min_qps"]) == 0.0 else min(float(item["min_qps"]), qps)
    return out

def md_table(headers: list[str], rows: list[Json]) -> str:
    lines = ["| " + " | ".join(headers) + " |", "| " + " | ".join(["---"] * len(headers)) + " |"]
    for row in rows:
        lines.append("| " + " | ".join(str(row.get(h, "")) for h in headers) + " |")
    return "\n".join(lines)

matrix_rows = read_csv(replication_dir / "matrix.csv")
visibility_rows = read_csv(replication_dir / "secondary_visibility.csv")
raw_summary = read_json(raw_dir / "raw_backend_sweep.json")
replica_read_rows = [row for row in matrix_rows if row.get("read_policy") == "replica_eligible" and row.get("phase") == "get_raw_success_attempt"]
replica_retry_rows = [row for row in matrix_rows if row.get("read_policy") == "replica_eligible" and row.get("phase") == "get_visibility_retry"]
visibility_lag_rows = [row for row in visibility_rows if row.get("phase") == "secondary_visibility_lag_after_primary_set"]
replica_read_by_mode = mode_table(replica_read_rows)
replica_retry_by_mode = mode_table(replica_retry_rows)
visibility_by_mode: dict[str, Json] = {}
for row in visibility_lag_rows:
    mode = str(row.get("mode") or "")
    if not mode:
        continue
    item = visibility_by_mode.setdefault(mode, {"mode": mode, "thread_results": [], "max_p95_ms": 0.0, "max_p99_ms": 0.0, "errors": 0})
    item["thread_results"].append(row)
    item["max_p95_ms"] = max(float(item["max_p95_ms"]), f(row.get("p95_us")) / 1000.0)
    item["max_p99_ms"] = max(float(item["max_p99_ms"]), f(row.get("p99_us")) / 1000.0)
    item["errors"] += int(f(row.get("errors")))
raw_results = raw_summary.get("results") if isinstance(raw_summary.get("results"), list) else []
raw_qps = {str(r.get("raw_backend")): f(r.get("ingestion_qps")) for r in raw_results}
objectstore_ratio = 0.0
if raw_qps:
    max_qps = max(raw_qps.values())
    objectstore_ratio = (raw_qps.get("objectstore", 0.0) / max_qps) if max_qps > 0 else 0.0
checks: list[Json] = []
for mode, item in sorted(visibility_by_mode.items()):
    if max_visibility_p95_ms > 0:
        checks.append({"name": f"{mode}_secondary_visibility_p95_ms", "observed": round(float(item["max_p95_ms"]), 3), "maximum": max_visibility_p95_ms, "passed": float(item["max_p95_ms"]) <= max_visibility_p95_ms})
    checks.append({"name": f"{mode}_secondary_visibility_errors", "observed": int(item["errors"]), "maximum": 0, "passed": int(item["errors"]) == 0})
for mode, item in sorted(replica_read_by_mode.items()):
    if min_replica_read_qps > 0:
        checks.append({"name": f"{mode}_replica_read_min_qps", "observed": round(float(item["min_qps"]), 3), "minimum": min_replica_read_qps, "passed": float(item["min_qps"]) >= min_replica_read_qps})
if min_objectstore_qps_ratio > 0:
    checks.append({"name": "objectstore_qps_ratio", "observed": round(objectstore_ratio, 6), "minimum": min_objectstore_qps_ratio, "passed": objectstore_ratio >= min_objectstore_qps_ratio})
expected_replication_modes = {"shared_async", "shared_sync", "raft"} if matrix_rows else set()
observed_matrix_modes = {str(row.get("mode") or "") for row in matrix_rows if row.get("mode")}
observed_visibility_modes = {str(row.get("mode") or "") for row in visibility_lag_rows if row.get("mode")}
observed_replica_read_modes = set(replica_read_by_mode)
for mode in sorted(expected_replication_modes - observed_matrix_modes):
    checks.append({"name": f"{mode}_matrix_rows_present", "observed": "missing", "expected": "present", "passed": False})
for mode in sorted(expected_replication_modes - observed_replica_read_modes):
    checks.append({"name": f"{mode}_replica_read_evidence", "observed": "missing", "expected": "present", "passed": False})
for mode in sorted(expected_replication_modes - observed_visibility_modes):
    checks.append({"name": f"{mode}_secondary_visibility_evidence", "observed": "missing", "expected": "present", "passed": False})
for row in matrix_rows:
    errors = int(f(row.get("errors")))
    if errors:
        checks.append({"name": f"{row.get('mode')}_{row.get('read_policy')}_{row.get('phase')}_errors", "observed": errors, "maximum": 0, "passed": False})
for exit_file in sorted(replication_dir.glob("*_t*/*.exit_code")):
    value = exit_file.read_text(encoding="utf-8", errors="replace").strip()
    if value and value != "0":
        checks.append({"name": f"{exit_file.parent.name}_{exit_file.stem}", "observed": value, "expected": "0", "passed": False})
if raw_summary:
    checks.append({"name": "raw_backend_sweep_status", "observed": raw_summary.get("status"), "expected": "ok", "passed": raw_summary.get("status") == "ok"})
status = "ok" if all(check.get("passed", False) for check in checks) else "failed"
summary: Json = {
    "schema": "temporalstore.multi_replica_serving_matrix.v1",
    "status": status,
    "replication_dir": str(replication_dir),
    "raw_backend_dir": str(raw_dir),
    "replica_read_by_mode": replica_read_by_mode,
    "replica_read_after_write_retry_by_mode": replica_retry_by_mode,
    "secondary_visibility_by_mode": visibility_by_mode,
    "raw_backend_qps": raw_qps,
    "objectstore_qps_ratio": round(objectstore_ratio, 6),
    "raw_backend_sweep": raw_summary,
    "checks": checks,
    "interpretation": {
        "multi_replica_serving": "replica_eligible read rows use non-primary routing and force-secondary lag probes use force-secondary-read",
        "shared_async": "tests shared-store secondary visibility with async local storage",
        "shared_sync": "tests shared-store secondary visibility with sync local storage",
        "raft": "tests data-node raft with bounded-stale reads and lag guardrails",
        "matrixobjectstore": "objectstore backend sweep validates MatrixObjectStore-style raw message object references and caller-visible dual-write latency",
    },
}
summary_json.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
vis_rows = [{"mode": mode, "max_p95_ms": round(float(item["max_p95_ms"]), 3), "max_p99_ms": round(float(item["max_p99_ms"]), 3), "errors": item["errors"]} for mode, item in sorted(visibility_by_mode.items())]
read_rows = [{"mode": mode, "min_qps": round(float(item["min_qps"]), 3), "max_p95_ms": round(float(item["max_p95_ms"]), 3)} for mode, item in sorted(replica_read_by_mode.items())]
retry_rows = [{"mode": mode, "min_qps": round(float(item["min_qps"]), 3), "max_p95_ms": round(float(item["max_p95_ms"]), 3)} for mode, item in sorted(replica_retry_by_mode.items())]
raw_rows = [{"backend": k, "qps": round(v, 3)} for k, v in sorted(raw_qps.items())]
check_rows = []
for check in checks:
    row = dict(check)
    row["passed"] = "yes" if check.get("passed") else "no"
    check_rows.append(row)
content = "# TemporalStore Multi-Replica Serving And Objectstore Matrix\n\n"
content += f"Status: `{status}`\n\n"
content += "## What This Covers\n\n"
content += "- Multi-replica serving reads through the existing replica-eligible and force-secondary read paths.\n"
content += "- Secondary replication lag for shared async storage, shared sync storage, and data-node Raft mode.\n"
content += "- MatrixObjectStore-compatible raw-message mode through the `objectstore` backend contract, compared with `temporalstore` and `matrixkv` raw backends.\n\n"
content += "## Secondary Visibility Lag\n\n" + md_table(["mode", "max_p95_ms", "max_p99_ms", "errors"], vis_rows) + "\n\n"
content += "## Replica-Eligible Raw Read Throughput\n\n" + md_table(["mode", "min_qps", "max_p95_ms"], read_rows) + "\n\n"
content += "## Replica Read-After-Write Retry Phase\n\n" + md_table(["mode", "min_qps", "max_p95_ms"], retry_rows) + "\n\n"
content += f"## Raw Backend / MatrixObjectStore Sweep\n\nObjectstore QPS ratio versus fastest backend: `{round(objectstore_ratio, 6)}`\n\n"
content += md_table(["backend", "qps"], raw_rows) + "\n\n"
content += "## Gates\n\n" + md_table(["name", "observed", "minimum", "maximum", "expected", "passed"], check_rows) + "\n\n"
content += "## Artifacts\n\n"
content += f"- Summary JSON: `{summary_json}`\n- Prometheus text: `{prom_file}`\n- Replication matrix: `{replication_dir}`\n- Raw backend sweep: `{raw_dir}`\n"
summary_md.write_text(content, encoding="utf-8")
prom_lines = [
    "# HELP temporalstore_multi_replica_serving_status Overall multi-replica serving matrix status.",
    "# TYPE temporalstore_multi_replica_serving_status gauge",
    f'temporalstore_multi_replica_serving_status{{status="{status}"}} {1 if status == "ok" else 0}',
    "# HELP temporalstore_secondary_visibility_lag_ms Secondary read visibility lag by mode.",
    "# TYPE temporalstore_secondary_visibility_lag_ms gauge",
]
for mode, item in sorted(visibility_by_mode.items()):
    prom_lines.append(f'temporalstore_secondary_visibility_lag_ms{{mode="{mode}",quantile="p95"}} {float(item["max_p95_ms"])}')
    prom_lines.append(f'temporalstore_secondary_visibility_lag_ms{{mode="{mode}",quantile="p99"}} {float(item["max_p99_ms"])}')
prom_lines.extend(["# HELP temporalstore_replica_read_qps Replica-eligible read throughput by replication mode.", "# TYPE temporalstore_replica_read_qps gauge"])
for mode, item in sorted(replica_read_by_mode.items()):
    prom_lines.append(f'temporalstore_replica_read_qps{{mode="{mode}"}} {float(item["min_qps"])}')
prom_lines.extend(["# HELP matrixobjectstore_raw_backend_qps MatrixObjectStore/objectstore raw ingestion backend QPS.", "# TYPE matrixobjectstore_raw_backend_qps gauge"])
for backend, qps in sorted(raw_qps.items()):
    prom_lines.append(f'matrixobjectstore_raw_backend_qps{{backend="{backend}"}} {qps}')
prom_lines.append(f'matrixobjectstore_raw_backend_qps_ratio{{backend="objectstore"}} {objectstore_ratio}')
prom_file.write_text("\n".join(prom_lines) + "\n", encoding="utf-8")
print(summary_json)
print(summary_md)
print(prom_file)
if status != "ok":
    raise SystemExit(2)
PY

cat "${SUMMARY_MD}"
