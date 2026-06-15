#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_TYPE="${BUILD_TYPE:-Release}"
RESULT_DIR="${RESULT_DIR:-/tmp/temporalstore-follower-read-sla-$(date +%Y%m%d_%H%M%S)}"
TEXTFILE_DIR="${TEXTFILE_DIR:-${RESULT_DIR}/metrics}"
METRICS_FILE="${METRICS_FILE:-${TEXTFILE_DIR}/temporalstore-follower-read-sla.prom}"
MAX_SECONDARY_VISIBILITY_P99_US="${MAX_SECONDARY_VISIBILITY_P99_US:-50000}"
MAX_SECONDARY_VISIBILITY_P95_US="${MAX_SECONDARY_VISIBILITY_P95_US:-50000}"
PROBE_OPS="${PROBE_OPS:-120}"
PROBE_THREADS="${PROBE_THREADS:-2}"
BACKGROUND_WRITER_THREADS="${BACKGROUND_WRITER_THREADS:-1}"
BACKGROUND_READER_THREADS="${BACKGROUND_READER_THREADS:-2}"
BENCH_TIMEOUT_S="${BENCH_TIMEOUT_S:-180}"
MS_PORT="${MS_PORT:-29200}"
SERVER_PORT="${SERVER_PORT:-15200}"

mkdir -p "${RESULT_DIR}" "${TEXTFILE_DIR}"

case_dir="${RESULT_DIR}/mixed_rw"
env \
  BUILD_TYPE="${BUILD_TYPE}" \
  RESULT_DIR="${case_dir}" \
  MS_PORT="${MS_PORT}" \
  SERVER_PORT="${SERVER_PORT}" \
  PROBE_OPS="${PROBE_OPS}" \
  PROBE_THREADS="${PROBE_THREADS}" \
  BACKGROUND_WRITER_THREADS="${BACKGROUND_WRITER_THREADS}" \
  BACKGROUND_READER_THREADS="${BACKGROUND_READER_THREADS}" \
  BENCH_TIMEOUT_S="${BENCH_TIMEOUT_S}" \
  bash "${ROOT}/tools/run_data_raft_mixed_rw_ubuntu22.sh"

python3 - \
  "${case_dir}/mixed_visibility.out" \
  "${METRICS_FILE}" \
  "${MAX_SECONDARY_VISIBILITY_P99_US}" \
  "${MAX_SECONDARY_VISIBILITY_P95_US}" \
  > "${RESULT_DIR}/summary.txt" <<'PY'
import csv
import sys
from pathlib import Path

visibility_path = Path(sys.argv[1])
metrics_path = Path(sys.argv[2])
max_p99 = float(sys.argv[3])
max_p95 = float(sys.argv[4])

rows = []
with visibility_path.open(encoding="utf-8", newline="") as fh:
    for raw in fh:
        raw = raw.strip()
        if not raw or raw.startswith("config,"):
            continue
        rows.append(raw)

lag = None
attempts = None
background = None
headers = None
for raw in rows:
    parts = next(csv.reader([raw]))
    if parts[0] == "phase":
        headers = parts
        continue
    if parts[0] == "secondary_visibility_lag_after_primary_set":
        lag = dict(zip(headers, parts))
    elif parts[0] == "secondary_visibility_poll_attempts":
        attempts = dict(zip(headers, parts))
    elif parts[0] == "background":
        if len(parts) == 5 and parts[1] != "writes":
            background = {
                "writes": parts[1],
                "reads": parts[2],
                "write_errors": parts[3],
                "read_errors": parts[4],
            }

if lag is None or attempts is None or background is None:
    raise SystemExit(f"missing follower-read rows in {visibility_path}")

def num(row, key):
    return float(row.get(key, 0) or 0)

lag_errors = num(lag, "errors")
lag_p95 = num(lag, "p95_us")
lag_p99 = num(lag, "p99_us")
attempt_p99 = num(attempts, "p99_us")
background_write_errors = float(background["write_errors"])
background_read_errors = float(background["read_errors"])
passed = (
    lag_errors == 0
    and background_write_errors == 0
    and background_read_errors == 0
    and lag_p95 <= max_p95
    and lag_p99 <= max_p99
)

metrics_path.parent.mkdir(parents=True, exist_ok=True)
with metrics_path.open("w", encoding="utf-8") as out:
    out.write("# HELP temporalstore_follower_read_sla_pass Whether bounded-stale follower-read SLA passed.\n")
    out.write("# TYPE temporalstore_follower_read_sla_pass gauge\n")
    out.write(f"temporalstore_follower_read_sla_pass {1 if passed else 0}\n")
    out.write("# HELP temporalstore_follower_read_visibility_p95_us Secondary visibility p95 latency after primary writes.\n")
    out.write("# TYPE temporalstore_follower_read_visibility_p95_us gauge\n")
    out.write(f"temporalstore_follower_read_visibility_p95_us {lag_p95}\n")
    out.write("# HELP temporalstore_follower_read_visibility_p99_us Secondary visibility p99 latency after primary writes.\n")
    out.write("# TYPE temporalstore_follower_read_visibility_p99_us gauge\n")
    out.write(f"temporalstore_follower_read_visibility_p99_us {lag_p99}\n")
    out.write("# HELP temporalstore_follower_read_errors_total Visibility errors from follower-read SLA gate.\n")
    out.write("# TYPE temporalstore_follower_read_errors_total counter\n")
    out.write(f"temporalstore_follower_read_errors_total {lag_errors}\n")
    out.write("# HELP temporalstore_follower_read_poll_attempts_p99 Poll attempts p99 before follower read saw primary write.\n")
    out.write("# TYPE temporalstore_follower_read_poll_attempts_p99 gauge\n")
    out.write(f"temporalstore_follower_read_poll_attempts_p99 {attempt_p99}\n")
    out.write("# HELP temporalstore_follower_read_background_writes_total Background writes during SLA gate.\n")
    out.write("# TYPE temporalstore_follower_read_background_writes_total counter\n")
    out.write(f"temporalstore_follower_read_background_writes_total {float(background['writes'])}\n")
    out.write("# HELP temporalstore_follower_read_background_reads_total Background reads during SLA gate.\n")
    out.write("# TYPE temporalstore_follower_read_background_reads_total counter\n")
    out.write(f"temporalstore_follower_read_background_reads_total {float(background['reads'])}\n")
    out.write("# HELP temporalstore_follower_read_background_errors_total Background read/write errors during SLA gate.\n")
    out.write("# TYPE temporalstore_follower_read_background_errors_total counter\n")
    out.write(
        "temporalstore_follower_read_background_errors_total "
        f"{background_write_errors + background_read_errors}\n"
    )

print(f"visibility_p95_us={lag_p95}")
print(f"visibility_p99_us={lag_p99}")
print(f"visibility_errors={lag_errors}")
print(f"poll_attempts_p99={attempt_p99}")
print(f"background_write_errors={background_write_errors}")
print(f"background_read_errors={background_read_errors}")
print(f"sla_pass={1 if passed else 0}")
print(f"metrics_file={metrics_path}")

if not passed:
    raise SystemExit("follower-read bounded-stale SLA failed")
PY

cat "${RESULT_DIR}/summary.txt"
grep -q '^temporalstore_follower_read_sla_pass 1' "${METRICS_FILE}"
echo "PASS follower-read bounded-stale SLA"
echo "${RESULT_DIR}"
