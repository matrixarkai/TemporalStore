#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

BUILD_TYPE="${BUILD_TYPE:-Release}"
BUILD_FLAVOR="$(printf '%s' "${BUILD_TYPE}" | tr '[:upper:]' '[:lower:]')"
OUT_DIR="${OUT_DIR:-${ROOT}/output-ubuntu22/${BUILD_FLAVOR}}"
RESULT_DIR="${RESULT_DIR:-/tmp/temporalstore-live-raft-metrics-$(date +%Y%m%d_%H%M%S)}"
SMOKE_DIR="${SMOKE_DIR:-${RESULT_DIR}/cluster}"
TEXTFILE_DIR="${TEXTFILE_DIR:-${RESULT_DIR}/metrics}"
CLUSTER_NAME="${CLUSTER_NAME:-live_raft_metrics}"
NAMESPACE_NAME="${NAMESPACE_NAME:-ns1}"
TABLE_NAME="${TABLE_NAME:-table1}"
MS_PORT="${MS_PORT:-30100}"
SERVER_PORT="${SERVER_PORT:-16100}"
DATA_RAFT_RAFT_PORT_DELTA="${DATA_RAFT_RAFT_PORT_DELTA:-1000}"
DATA_RAFT_SNAPSHOT_PORT_DELTA="${DATA_RAFT_SNAPSHOT_PORT_DELTA:-2000}"
ADMIN_RPC_TIMEOUT_S="${ADMIN_RPC_TIMEOUT_S:-10}"
BOOTSTRAP_TIMEOUT_S="${BOOTSTRAP_TIMEOUT_S:-180}"
RAFT_STATUS_WAIT_S="${RAFT_STATUS_WAIT_S:-90}"
MAX_APPLY_LAG="${MAX_APPLY_LAG:-16}"

mkdir -p "${RESULT_DIR}" "${TEXTFILE_DIR}"
SUMMARY="${RESULT_DIR}/summary.txt"
METRICS_FILE="${TEXTFILE_DIR}/temporalstore-live-raft.prom"

log() {
  printf '%s\n' "$*" | tee -a "${SUMMARY}"
}

need_file() {
  if [[ ! -x "$1" ]]; then
    echo "missing executable: $1" >&2
    exit 1
  fi
}

post_json() {
  local port="$1"
  local path="$2"
  local body="$3"
  curl -fsS -m "${ADMIN_RPC_TIMEOUT_S}" \
    -H "Content-Type: application/json" \
    -d "${body}" \
    "http://127.0.0.1:${port}/${path}"
}

preflight_port() {
  local port="$1"
  python3 - "${port}" <<'PY'
import socket
import sys

port = int(sys.argv[1])
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        sock.bind(("127.0.0.1", port))
    except OSError as exc:
        raise SystemExit(f"port {port} is not free: {exc}")
PY
}

cleanup() {
  local status=$?
  if [[ -f "${RESULT_DIR}/bootstrap.pid" ]]; then
    kill "$(cat "${RESULT_DIR}/bootstrap.pid")" >/dev/null 2>&1 || true
  fi
  for pid_file in "${SMOKE_DIR}"/server*.pid "${SMOKE_DIR}"/metaserver*.pid; do
    [[ -f "${pid_file}" ]] || continue
    kill "$(cat "${pid_file}")" >/dev/null 2>&1 || true
  done
  sleep 0.2
  for pid_file in "${SMOKE_DIR}"/server*.pid "${SMOKE_DIR}"/metaserver*.pid; do
    [[ -f "${pid_file}" ]] || continue
    kill -9 "$(cat "${pid_file}")" >/dev/null 2>&1 || true
  done
  wait >/dev/null 2>&1 || true
  return "${status}"
}
trap cleanup EXIT

need_file "${OUT_DIR}/bcache2-metaserver"
need_file "${OUT_DIR}/bcache2-server"

if ! command -v curl >/dev/null 2>&1; then
  echo "missing required tool: curl" >&2
  exit 1
fi
if ! command -v python3 >/dev/null 2>&1; then
  echo "missing required tool: python3" >&2
  exit 1
fi

if (( SERVER_PORT + 1 + DATA_RAFT_RAFT_PORT_DELTA > 65535 ||
      SERVER_PORT + 1 + DATA_RAFT_SNAPSHOT_PORT_DELTA > 65535 )); then
  echo "data raft transport ports exceed 65535; lower SERVER_PORT or deltas" >&2
  exit 2
fi

preflight_port "${MS_PORT}"
preflight_port "$((MS_PORT + 10))"
preflight_port "$((MS_PORT + 20))"
for i in 0 1; do
  preflight_port "$((SERVER_PORT + i))"
  preflight_port "$((SERVER_PORT + i + DATA_RAFT_RAFT_PORT_DELTA))"
  preflight_port "$((SERVER_PORT + i + DATA_RAFT_SNAPSHOT_PORT_DELTA))"
done

rm -rf "${SMOKE_DIR}"
log "TemporalStore live raft metrics gate"
log "result_dir=${RESULT_DIR}"
log "metrics_file=${METRICS_FILE}"

(
  cd "${ROOT}"
  env \
    BUILD_TYPE="${BUILD_TYPE}" \
    OUT_DIR="${OUT_DIR}" \
    SMOKE_DIR="${SMOKE_DIR}" \
    CLUSTER_NAME="${CLUSTER_NAME}" \
    NAMESPACE_NAME="${NAMESPACE_NAME}" \
    TABLE_NAME="${TABLE_NAME}" \
    META_COUNT=1 \
    SERVER_COUNT=2 \
    REPLICA_COUNT=2 \
    MS_PORT="${MS_PORT}" \
    MS_RAFT_PORT="$((MS_PORT + 10))" \
    MS_SNAPSHOT_PORT="$((MS_PORT + 20))" \
    SERVER_PORT="${SERVER_PORT}" \
    TABLE_ELECTION_POLICY=PROMOTE_SECONDARY \
    TABLE_PARTITION_UNIT_RELATION=ANTI_ENTROPY \
    SERVER_EXTRA_FLAGS="--data_replication_mode=raft_consensus --data_raft_work_dir=${SMOKE_DIR}/data-raft --data_raft_raft_port_delta=${DATA_RAFT_RAFT_PORT_DELTA} --data_raft_snapshot_port_delta=${DATA_RAFT_SNAPSHOT_PORT_DELTA} --data_raft_enable_empty_snapshot_for_tests=false --data_raft_read_mode=bounded_stale --data_raft_bounded_stale_max_index_lag=${MAX_APPLY_LAG} --data_raft_propose_timeout_ms=5000 --storage_async=true --storage_enable_evict=false --storage_enable_expire=false --storage_enable_page_gc=false --storage_enable_page_compaction=false --storage_enable_index_gc=false --storage_enable_oplog_rolling=false" \
    KEEP_RUNNING=1 \
    bash tools/smoke_ubuntu22.sh
) > "${RESULT_DIR}/bootstrap.log" 2>&1 &
echo "$!" > "${RESULT_DIR}/bootstrap.pid"

deadline=$((SECONDS + BOOTSTRAP_TIMEOUT_S))
while (( SECONDS < deadline )); do
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

request_id="{\"cluster_name\":\"${CLUSTER_NAME}\",\"operator_name\":\"live_raft_metrics\"}"
list_partition_body="{\"id\":${request_id},\"read_stale\":false,\"namespace_name\":\"${NAMESPACE_NAME}\",\"table_name\":\"${TABLE_NAME}\"}"

post_json "${MS_PORT}" "QueryService/QueryClusterStatus" "{}" \
  > "${RESULT_DIR}/metaserver_cluster_status.json"

deadline=$((SECONDS + RAFT_STATUS_WAIT_S))
while (( SECONDS < deadline )); do
  if post_json "${MS_PORT}" "QueryService/ListPartition" "${list_partition_body}" \
      > "${RESULT_DIR}/list_partition.json" 2>"${RESULT_DIR}/list_partition.json.err"; then
    if python3 - "${RESULT_DIR}/list_partition.json" <<'PY'
import json
import sys

data = json.load(open(sys.argv[1], encoding="utf-8"))
infos = data.get("info", [])
if not infos:
    sys.exit(1)
unit = infos[0].get("set_info", {}).get("membership", {}).get("units", [{}])[0]
active = {int(x) for x in unit.get("active_id_list", [])}
placed_active = set()
for partition in infos[0].get("partition_info", []):
    partition_id = int(partition.get("id", 0) or 0)
    port = int(partition.get("placement_actual", {}).get("server", {}).get("port", 0) or 0)
    if partition_id in active and port > 0:
        placed_active.add(partition_id)
sys.exit(0 if len(active) >= 2 and active == placed_active else 1)
PY
    then
      break
    fi
  fi
  sleep 1
done

if ! python3 - "${RESULT_DIR}/list_partition.json" <<'PY'
import json
import sys

data = json.load(open(sys.argv[1], encoding="utf-8"))
infos = data.get("info", [])
if not infos:
    raise SystemExit(1)
unit = infos[0].get("set_info", {}).get("membership", {}).get("units", [{}])[0]
active = {int(x) for x in unit.get("active_id_list", [])}
placed_active = {
    int(partition.get("id", 0) or 0)
    for partition in infos[0].get("partition_info", [])
    if int(partition.get("placement_actual", {}).get("server", {}).get("port", 0) or 0) > 0
}
raise SystemExit(0 if len(active) >= 2 and active <= placed_active else 1)
PY
then
  echo "timed out waiting for active data raft replicas to get actual server placement" >&2
  cat "${RESULT_DIR}/list_partition.json" >&2 || true
  exit 1
fi

python3 - "${RESULT_DIR}/list_partition.json" "${RESULT_DIR}/topology.csv" <<'PY'
import csv
import json
import sys

src, dst = sys.argv[1], sys.argv[2]
data = json.load(open(src, encoding="utf-8"))
infos = data.get("info", [])
if not infos:
    raise SystemExit("missing partition info")
unit = infos[0].get("set_info", {}).get("membership", {}).get("units", [{}])[0]
primary = int(unit.get("primary_id", 0) or 0)
active = {int(x) for x in unit.get("active_id_list", [])}
with open(dst, "w", encoding="utf-8", newline="") as fh:
    writer = csv.writer(fh)
    writer.writerow(["partition_id", "server_port", "role", "state", "active", "primary"])
    for partition in infos[0].get("partition_info", []):
        partition_id = int(partition.get("id", 0) or 0)
        placement = partition.get("placement_actual", {})
        server = placement.get("server", {})
        writer.writerow([
            partition_id,
            int(server.get("port", 0) or 0),
            partition.get("role", ""),
            partition.get("state", ""),
            1 if partition_id in active else 0,
            1 if partition_id == primary else 0,
        ])
PY

deadline=$((SECONDS + RAFT_STATUS_WAIT_S))
while (( SECONDS < deadline )); do
  rm -f "${RESULT_DIR}"/data_raft_status_*.json
  while IFS=, read -r partition_id server_port role state active primary; do
    [[ "${partition_id}" != "partition_id" ]] || continue
    [[ -n "${partition_id}" && -n "${server_port}" && "${server_port}" != "0" ]] || continue
    [[ "${active}" == "1" ]] || continue
    status_file="${RESULT_DIR}/data_raft_status_${server_port}_${partition_id}.json"
    post_json "${server_port}" "ServerService/GetDataRaftStatus" \
      "{\"partition_id\":${partition_id}}" > "${status_file}" 2>"${status_file}.err" || true
  done < "${RESULT_DIR}/topology.csv"

  if python3 - "${RESULT_DIR}" "${MAX_APPLY_LAG}" <<'PY'
import glob
import json
import sys

result_dir, max_lag_raw = sys.argv[1:]
max_lag = int(max_lag_raw)
samples = 0
leaders = 0
for path in glob.glob(f"{result_dir}/data_raft_status_*.json"):
    data = json.load(open(path, encoding="utf-8"))
    if data.get("status", {}).get("code", 0) != 0:
        continue
    if data.get("running") is not True:
        continue
    if int(data.get("fatal_event_count", 0) or 0) != 0:
        continue
    if int(data.get("voter_count", 0) or 0) < 2:
        continue
    lag = max(0, int(data.get("committed_index", 0) or 0) - int(data.get("applied_index", 0) or 0))
    if lag > max_lag:
        continue
    samples += 1
    leaders += 1 if data.get("leader") is True else 0
sys.exit(0 if samples >= 2 and leaders >= 1 else 1)
PY
  then
    break
  fi
  sleep 1
done

python3 - \
  "${RESULT_DIR}/metaserver_cluster_status.json" \
  "${RESULT_DIR}/topology.csv" \
  "${RESULT_DIR}" \
  "${METRICS_FILE}" \
  "${MAX_APPLY_LAG}" \
  > "${RESULT_DIR}/metrics_summary.txt" <<'PY'
import csv
import glob
import json
import re
import sys

meta_path, topology_path, result_dir, metrics_path, max_lag_raw = sys.argv[1:]
max_lag = int(max_lag_raw)
meta = json.load(open(meta_path, encoding="utf-8"))
topology = list(csv.DictReader(open(topology_path, encoding="utf-8")))
metrics = []
errors = []

def number(value, default=0):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default

def metric(name, value, labels=None):
    if labels:
        label_text = ",".join(f'{key}="{val}"' for key, val in labels.items())
        metrics.append(f"{name}{{{label_text}}} {value}")
    else:
        metrics.append(f"{name} {value}")

meta_status = meta.get("status", {})
if meta_status.get("code", 0) != 0:
    errors.append(f"metaserver status code={meta_status.get('code')}")
raft_nodes = meta.get("raft_nodes", [])
leader = meta.get("raft_leader_info", {}) or {}
metric("temporalstore_live_metaserver_raft_applied_index", number(meta.get("raft_applied_index")))
metric("temporalstore_live_metaserver_raft_node_count", len(raft_nodes))
metric("temporalstore_live_metaserver_raft_leader_peer_id", number(leader.get("id") or leader.get("peer_id")))
metric("temporalstore_live_metaserver_raft_leader_present", 1 if leader else 0)
if not leader:
    errors.append("missing metaserver raft leader")

active_rows = [row for row in topology if row.get("active") == "1"]
if not active_rows:
    errors.append("missing active data raft partitions")

leader_samples = 0
status_samples = 0
max_observed_lag = 0
fatal_events = 0
for path in sorted(glob.glob(f"{result_dir}/data_raft_status_*.json")):
    match = re.search(r"data_raft_status_(\d+)_(\d+)\.json$", path)
    if not match:
        continue
    server_port, partition_id = match.groups()
    data = json.load(open(path, encoding="utf-8"))
    status = data.get("status", {})
    if status.get("code", 0) != 0:
        errors.append(f"data raft status code={status.get('code')} partition={partition_id} port={server_port}")
        continue
    labels = {"partition_id": partition_id, "server_port": server_port}
    committed = number(data.get("committed_index"))
    applied = number(data.get("applied_index"))
    lag = max(0, committed - applied)
    fatal = number(data.get("fatal_event_count"))
    is_leader = 1 if data.get("leader") is True else 0
    running = 1 if data.get("running") is True else 0
    status_samples += 1
    leader_samples += is_leader
    max_observed_lag = max(max_observed_lag, lag)
    fatal_events += fatal
    metric("temporalstore_live_data_raft_running", running, labels)
    metric("temporalstore_live_data_raft_leader", is_leader, labels)
    metric("temporalstore_live_data_raft_term", number(data.get("term")), labels)
    metric("temporalstore_live_data_raft_committed_index", committed, labels)
    metric("temporalstore_live_data_raft_applied_index", applied, labels)
    metric("temporalstore_live_data_raft_apply_lag", lag, labels)
    metric("temporalstore_live_data_raft_voter_count", number(data.get("voter_count")), labels)
    metric("temporalstore_live_data_raft_learner_count", number(data.get("learner_count")), labels)
    metric("temporalstore_live_data_raft_fatal_events", fatal, labels)
    metric("temporalstore_live_data_raft_snapshot_creating", 1 if data.get("snapshot_creating") is True else 0, labels)
    metric("temporalstore_live_data_raft_snapshot_loading", 1 if data.get("snapshot_loading") is True else 0, labels)
    if running != 1:
        errors.append(f"data raft not running partition={partition_id} port={server_port}")
    if fatal:
        errors.append(f"data raft fatal events={fatal} partition={partition_id} port={server_port}")
    if lag > max_lag:
        errors.append(f"data raft apply lag={lag} exceeds max={max_lag} partition={partition_id} port={server_port}")

metric("temporalstore_live_data_raft_status_samples", status_samples)
metric("temporalstore_live_data_raft_leader_samples", leader_samples)
metric("temporalstore_live_data_raft_max_apply_lag", max_observed_lag)
metric("temporalstore_live_data_raft_total_fatal_events", fatal_events)
if status_samples == 0:
    errors.append("missing data raft status samples")
if leader_samples == 0:
    errors.append("missing data raft leader sample")

with open(metrics_path, "w", encoding="utf-8") as out:
    out.write("# HELP temporalstore_live_raft_metrics Live TemporalStore raft health samples from local APIs.\n")
    out.write("# TYPE temporalstore_live_data_raft_apply_lag gauge\n")
    out.write("\n".join(metrics))
    out.write("\n")

print(f"status_samples={status_samples}")
print(f"leader_samples={leader_samples}")
print(f"max_apply_lag={max_observed_lag}")
print(f"fatal_events={fatal_events}")
print(f"metrics_file={metrics_path}")
if errors:
    print("errors:")
    for error in errors:
        print(f"- {error}")
    raise SystemExit(1)
PY

cat "${RESULT_DIR}/metrics_summary.txt" | tee -a "${SUMMARY}"
grep -q '^temporalstore_live_data_raft_apply_lag{' "${METRICS_FILE}"
grep -q '^temporalstore_live_metaserver_raft_applied_index ' "${METRICS_FILE}"

log "PASS live raft metrics"
log "${RESULT_DIR}"
