#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_TYPE="${BUILD_TYPE:-Release}"
RESULT_DIR="${RESULT_DIR:-/tmp/temporalstore-cpp-transport-parity-$(date +%Y%m%d-%H%M%S)}"
TRANSPORT_REQUIRE_FRESH_BINARIES="${TRANSPORT_REQUIRE_FRESH_BINARIES:-1}"

DIRECT_HASH_OPS="${DIRECT_HASH_OPS:-2000}"
DIRECT_FEATURE_KEYS="${DIRECT_FEATURE_KEYS:-16}"
DIRECT_FEATURE_POINTS_PER_KEY="${DIRECT_FEATURE_POINTS_PER_KEY:-32}"
DIRECT_VALUE_BYTES="${DIRECT_VALUE_BYTES:-512}"

PROXY_PRESSURE_OPS="${PROXY_PRESSURE_OPS:-200}"
PROXY_PRESSURE_THREADS="${PROXY_PRESSURE_THREADS:-2}"
PROXY_PRESSURE_VALUE_BYTES="${PROXY_PRESSURE_VALUE_BYTES:-128}"

mkdir -p "${RESULT_DIR}"
printf '%s\n' "${TRANSPORT_REQUIRE_FRESH_BINARIES}" > "${RESULT_DIR}/fresh_binary_gate.txt"

direct_dir="${RESULT_DIR}/direct_sdk_oracle"
proxy_dir="${RESULT_DIR}/live_proxy_verified"

stop_cluster_processes() {
  local cluster_name="$1"
  local patterns=(
    "bcache2-proxy.*proxy_cluster_name=${cluster_name}"
    "bcache2-metaserver.*metaserver_cluster_name=${cluster_name}"
    "bcache2-server.*cluster_name=${cluster_name}"
  )
  local pattern
  local pids
  for pattern in "${patterns[@]}"; do
    pids="$(pgrep -f "${pattern}" || true)"
    [[ -z "${pids}" ]] || kill ${pids} >/dev/null 2>&1 || true
  done
  sleep 1
  for pattern in "${patterns[@]}"; do
    pids="$(pgrep -f "${pattern}" || true)"
    [[ -z "${pids}" ]] || kill -9 ${pids} >/dev/null 2>&1 || true
  done
}

wait_for_no_temporalstore_processes() {
  for _ in $(seq 1 50); do
    if ! pgrep -af 'bcache2-(metaserver|server|proxy)' >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.2
  done
  echo "TemporalStore processes are still running after cleanup:" >&2
  pgrep -af 'bcache2-(metaserver|server|proxy)' >&2 || true
  return 1
}

echo "RUN direct SDK oracle parity"
(
  cd "${ROOT}"
  RESULT_DIR="${direct_dir}" \
  BUILD_TYPE="${BUILD_TYPE}" \
  REQUIRE_FRESH_BINARIES="${TRANSPORT_REQUIRE_FRESH_BINARIES}" \
  REQUIRE_NO_TEMPORALSTORE_PROCESSES=1 \
  RUN_PYTHON_DIRECT_STRESS=1 \
  PYTHON_DIRECT_STRESS_HASH_OPS="${DIRECT_HASH_OPS}" \
  PYTHON_DIRECT_STRESS_FEATURE_KEYS="${DIRECT_FEATURE_KEYS}" \
  PYTHON_DIRECT_STRESS_FEATURE_POINTS_PER_KEY="${DIRECT_FEATURE_POINTS_PER_KEY}" \
  PYTHON_DIRECT_STRESS_VALUE_BYTES="${DIRECT_VALUE_BYTES}" \
  PYTHON_DIRECT_STRESS_REQUEST_TIMEOUT_MS=20000 \
  PYTHON_DIRECT_STRESS_IO_TIMEOUT_MS=20000 \
  MS_PORT=18700 \
  MS_RAFT_PORT=18710 \
  MS_SNAPSHOT_PORT=18720 \
  SERVER_PORT=18701 \
  CLUSTER_NAME=benchdirectparity \
  ./tools/run_sdk_smoke_ubuntu22.sh
) | tee "${RESULT_DIR}/direct_sdk_oracle.out"

stop_cluster_processes benchdirectparity
wait_for_no_temporalstore_processes

echo "RUN live C++ proxy verified parity"
(
  cd "${ROOT}"
  RESULT_DIR="${proxy_dir}" \
  BUILD_TYPE="${BUILD_TYPE}" \
  REQUIRE_FRESH_BINARIES="${TRANSPORT_REQUIRE_FRESH_BINARIES}" \
  REQUIRE_NO_TEMPORALSTORE_PROCESSES=1 \
  CLUSTER_NAME=benchproxyparity \
  NAMESPACE_NAME=sdk_ns \
  TABLE_NAME=sdk_table \
  MS_PORT=18740 \
  MS_RAFT_PORT=18750 \
  MS_SNAPSHOT_PORT=18760 \
  SERVER_PORT=18741 \
  CPP_PROXY_PORT=18780 \
  RUN_CPP_PROXY_PARITY=1 \
  CPP_PROXY_PRESSURE_OPS="${PROXY_PRESSURE_OPS}" \
  CPP_PROXY_PRESSURE_THREADS="${PROXY_PRESSURE_THREADS}" \
  CPP_PROXY_PRESSURE_VALUE_BYTES="${PROXY_PRESSURE_VALUE_BYTES}" \
  CPP_PROXY_PRESSURE_VERIFY_TIMEOUT_MS=20000 \
  CPP_PROXY_PRESSURE_VERIFY_POLL_MS=20 \
  ./tools/run_sdk_smoke_ubuntu22.sh
) | tee "${RESULT_DIR}/live_proxy_verified.out"

stop_cluster_processes benchproxyparity
wait_for_no_temporalstore_processes

python3 - "$RESULT_DIR" <<'PY'
import json
import pathlib
import re
import sys

root = pathlib.Path(sys.argv[1])
direct = json.loads((root / "direct_sdk_oracle" / "python_direct_stress.json").read_text())
proxy_pressure = (root / "live_proxy_verified" / "cpp_proxy_pressure.out").read_text()

def extract(name: str) -> str:
    match = re.search(rf"^{re.escape(name)}=(.+)$", proxy_pressure, re.MULTILINE)
    return match.group(1).strip() if match else ""

report = {
    "status": "passed",
    "fresh_binary_gate": (root / "fresh_binary_gate.txt").read_text().strip(),
    "direct_sdk": {
        "status": direct.get("status"),
        "parity_checked": direct.get("parity_checked"),
        "hash_ops": direct.get("hash_ops"),
        "hash_reads": direct.get("hash_reads"),
        "feature_points_written": direct.get("feature_points_written"),
        "feature_points_read": direct.get("feature_points_read"),
        "hash_oracle_digest": direct.get("hash_oracle_digest"),
        "feature_oracle_digest": direct.get("feature_oracle_digest"),
    },
    "proxy": {
        "proxy_smoke_passed": "PASS proxy thrift smoke" in (root / "live_proxy_verified" / "cpp_proxy_smoke.out").read_text(),
        "proxy_ingestion_pressure_exit": "0",
        "read_verified": extract("read_verified"),
        "read_failed": extract("read_failed"),
        "write_failed": extract("write_failed"),
        "write_retry_attempts": extract("write_retry_attempts"),
        "rpc_failed": extract("rpc_failed"),
        "status_failed": extract("status_failed"),
    },
}
if direct.get("status") != "passed" or direct.get("parity_checked") is not True:
    raise SystemExit("direct SDK parity did not pass")
if not report["proxy"]["proxy_smoke_passed"] or report["proxy"]["proxy_ingestion_pressure_exit"] != "0":
    raise SystemExit("proxy parity did not pass")
for key in ("read_failed", "write_failed", "rpc_failed", "status_failed"):
    if report["proxy"][key] not in {"", "0"}:
        raise SystemExit(f"proxy {key}={report['proxy'][key]}")

(root / "transport_parity_report.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
(root / "transport_parity_report.md").write_text(
    "# C++ Benchmark Transport Parity\n\n"
    f"- status: `{report['status']}`\n"
    f"- direct SDK parity checked: `{report['direct_sdk']['parity_checked']}`\n"
    f"- direct SDK hash ops: `{report['direct_sdk']['hash_ops']}`\n"
    f"- direct SDK feature points: `{report['direct_sdk']['feature_points_read']}`\n"
    f"- proxy smoke passed: `{report['proxy']['proxy_smoke_passed']}`\n"
    f"- proxy pressure exit: `{report['proxy']['proxy_ingestion_pressure_exit']}`\n"
    f"- proxy read failed: `{report['proxy']['read_failed']}`\n"
    f"- proxy write failed: `{report['proxy']['write_failed']}`\n"
    f"- proxy status failed: `{report['proxy']['status_failed']}`\n"
    f"- proxy write retry attempts: `{report['proxy']['write_retry_attempts']}`\n"
)
print(json.dumps(report, indent=2, sort_keys=True))
PY

echo "PASS C++ benchmark transport parity"
echo "wrote:"
echo "  ${RESULT_DIR}/transport_parity_report.json"
echo "  ${RESULT_DIR}/transport_parity_report.md"
