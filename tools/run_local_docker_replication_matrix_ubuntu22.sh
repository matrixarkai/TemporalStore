#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RESULT_DIR="${RESULT_DIR:-/tmp/temporalstore-local-docker-replication-$(date +%Y%m%d_%H%M%S)}"
IMAGE="${IMAGE:-temporalstore-ubuntu22-runtime:local}"
BUILD_TYPE="${BUILD_TYPE:-Release}"
BUILD_FLAVOR="$(printf '%s' "${BUILD_TYPE}" | tr '[:upper:]' '[:lower:]')"
OPS="${OPS:-8000}"
REPLICA_OPS="${REPLICA_OPS:-${OPS}}"
REPLICA_WAIT_MS="${REPLICA_WAIT_MS:-10000}"
THREAD_LIST="${THREAD_LIST:-2 4}"
THREAD_LIST="${THREAD_LIST//,/ }"
VALUE_BYTES="${VALUE_BYTES:-256}"
BASE_PORT="${BASE_PORT:-41000}"
BENCH_TIMEOUT_S="${BENCH_TIMEOUT_S:-600}"
RUN_FAILOVER="${RUN_FAILOVER:-1}"
DATA_RAFT_RAFT_PORT_DELTA="${DATA_RAFT_RAFT_PORT_DELTA:-1000}"
DATA_RAFT_SNAPSHOT_PORT_DELTA="${DATA_RAFT_SNAPSHOT_PORT_DELTA:-2000}"

ensure_image() {
  if docker image inspect "${IMAGE}" >/dev/null 2>&1 && [[ "${REBUILD_DOCKER_IMAGE:-0}" != "1" ]]; then
    return
  fi
  local dockerfile
  dockerfile="$(mktemp)"
  cat > "${dockerfile}" <<'DOCKERFILE'
FROM ubuntu:22.04
ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl python3 procps psmisc \
    libabsl-dev libboost-filesystem1.74.0 libboost-system1.74.0 \
    libbz2-1.0 libcurl4 libev4 libevent-2.1-7 libfmt8 libgflags2.2 \
    libleveldb1d liblz4-1 libnuma1 libprotobuf23 librocksdb6.11 \
    libsnappy1v5 libssl3 libthrift-0.16.0 libunwind8 zlib1g \
    && rm -rf /var/lib/apt/lists/*
DOCKERFILE
  docker build -t "${IMAGE}" -f "${dockerfile}" .
  rm -f "${dockerfile}"
}

if [[ "${TEMPORALSTORE_DOCKER_INSIDE:-0}" != "1" ]]; then
  mkdir -p "${RESULT_DIR}"
  ensure_image
  volume="${DOCKER_VOLUME:-temporalstore-repl-matrix-$(date +%Y%m%d%H%M%S)-$$}"
  docker volume create "${volume}" >/dev/null
  cleanup_volume() {
    if [[ "${KEEP_DOCKER_VOLUME:-0}" != "1" ]]; then
      docker volume rm "${volume}" >/dev/null 2>&1 || true
    fi
  }
  trap cleanup_volume EXIT

  tar -C "${ROOT}" -cf - \
    tools \
    output-ubuntu22 \
    build-ubuntu22/"${BUILD_FLAVOR}"/src/client/example \
    | docker run -i --rm -v "${volume}:/workspace" "${IMAGE}" \
        tar -C /workspace -xf -

  set +e
  docker run --rm --network host \
    -e TEMPORALSTORE_DOCKER_INSIDE=1 \
    -e RESULT_DIR=/workspace/results \
    -e BUILD_TYPE="${BUILD_TYPE}" \
    -e OPS="${OPS}" \
    -e REPLICA_OPS="${REPLICA_OPS}" \
    -e REPLICA_WAIT_MS="${REPLICA_WAIT_MS}" \
    -e THREAD_LIST="${THREAD_LIST}" \
    -e VALUE_BYTES="${VALUE_BYTES}" \
    -e BASE_PORT="${BASE_PORT}" \
    -e BENCH_TIMEOUT_S="${BENCH_TIMEOUT_S}" \
    -e RUN_FAILOVER="${RUN_FAILOVER}" \
    -e DATA_RAFT_RAFT_PORT_DELTA="${DATA_RAFT_RAFT_PORT_DELTA}" \
    -e DATA_RAFT_SNAPSHOT_PORT_DELTA="${DATA_RAFT_SNAPSHOT_PORT_DELTA}" \
    -v "${volume}:/workspace" \
    -w /workspace \
    "${IMAGE}" \
    bash /workspace/tools/run_local_docker_replication_matrix_ubuntu22.sh
  inner_code=$?
  set -e

  if docker run --rm -v "${volume}:/workspace" "${IMAGE}" test -d /workspace/results; then
    docker run --rm -v "${volume}:/workspace" "${IMAGE}" \
      tar -C /workspace/results -cf - . | tar -C "${RESULT_DIR}" -xf -
  fi
  echo "copied Docker results to ${RESULT_DIR}"
  exit "${inner_code}"
fi

source "${ROOT}/tools/temporalstore_runtime_env.sh"

OUT_DIR="${OUT_DIR:-${ROOT}/output-ubuntu22/${BUILD_FLAVOR}}"
BIN_DIR="${BIN_DIR:-${ROOT}/build-ubuntu22/${BUILD_FLAVOR}/src/client/example}"
MATRIX_CSV="${RESULT_DIR}/matrix.csv"
VISIBILITY_CSV="${RESULT_DIR}/secondary_visibility.csv"
SUMMARY_MD="${RESULT_DIR}/summary.md"

need_file() {
  if [[ ! -x "$1" ]]; then
    echo "missing executable: $1" >&2
    exit 1
  fi
}

need_file "${OUT_DIR}/bcache2-server"
need_file "${OUT_DIR}/bcache2-metaserver"
need_file "${BIN_DIR}/string_scale_benchmark"
need_file "${BIN_DIR}/replication_smoke_example"
need_file "${BIN_DIR}/secondary_visibility_lag_benchmark"

mkdir -p "${RESULT_DIR}"
echo "mode,threads,read_policy,phase,ops,value_bytes,errors,qps,avg_us,p50_us,p95_us,p99_us,min_us,max_us,total_ms,case_dir" > "${MATRIX_CSV}"
echo "mode,threads,phase,samples,errors,total_ms,avg_us,p50_us,p95_us,p99_us,min_us,max_us,case_dir" > "${VISIBILITY_CSV}"

append_string_rows() {
  local mode="$1"
  local threads="$2"
  local read_policy="$3"
  local out="$4"
  local case_dir="$5"
  python3 - "$mode" "$threads" "$read_policy" "$out" "$case_dir" "$MATRIX_CSV" <<'PY'
import csv
import sys

mode, threads, read_policy, path, case_dir, dest = sys.argv[1:]
with open(path, encoding="utf-8") as fh, open(dest, "a", encoding="utf-8", newline="") as out:
    writer = csv.writer(out)
    for row in csv.reader(fh):
        if row and row[0] == "TemporalStore":
            writer.writerow([mode, threads, read_policy, row[1], row[2], row[4], row[5],
                             row[6], row[7], row[8], row[9], row[10], row[11], row[12],
                             row[13], case_dir])
PY
}

append_visibility_rows() {
  local mode="$1"
  local threads="$2"
  local out="$3"
  local case_dir="$4"
  python3 - "$mode" "$threads" "$out" "$case_dir" "$VISIBILITY_CSV" <<'PY'
import csv
import sys

mode, threads, path, case_dir, dest = sys.argv[1:]
with open(path, encoding="utf-8") as fh, open(dest, "a", encoding="utf-8", newline="") as out:
    writer = csv.writer(out)
    for row in csv.reader(fh):
        if row and row[0] in ("secondary_visibility_lag_after_primary_set",
                              "secondary_visibility_poll_attempts"):
            writer.writerow([mode, threads, row[0], row[2], row[3], row[4], row[5],
                             row[6], row[7], row[8], row[9], row[10], case_dir])
PY
}

wait_for_bootstrap() {
  local pid_file="$1"
  local log_file="$2"
  for _ in $(seq 1 180); do
    if grep -q "KEEP_RUNNING=1" "${log_file}" 2>/dev/null; then
      return 0
    fi
    if ! kill -0 "$(cat "${pid_file}")" >/dev/null 2>&1; then
      echo "bootstrap exited early" >&2
      cat "${log_file}" >&2 || true
      return 1
    fi
    sleep 1
  done
  echo "bootstrap timed out" >&2
  tail -120 "${log_file}" >&2 || true
  return 1
}

check_port_free() {
  local port="$1"
  python3 - "$port" <<'PY'
import socket
import sys

port = int(sys.argv[1])
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
try:
    sock.bind(("127.0.0.1", port))
except OSError as exc:
    print(f"port {port} is not free: {exc}", file=sys.stderr)
    sys.exit(1)
finally:
    sock.close()
PY
}

stop_cluster() {
  local smoke_dir="$1"
  local bootstrap_pid_file="$2"
  if [[ -f "${bootstrap_pid_file}" ]]; then
    kill "$(cat "${bootstrap_pid_file}")" >/dev/null 2>&1 || true
  fi
  for pid_file in "${smoke_dir}"/server*.pid "${smoke_dir}"/metaserver*.pid; do
    [[ -f "${pid_file}" ]] || continue
    kill "$(cat "${pid_file}")" >/dev/null 2>&1 || true
  done
  sleep 0.2
}

run_case() {
  local mode="$1"
  local threads="$2"
  local ordinal="$3"
  local case_dir="${RESULT_DIR}/${mode}_t${threads}"
  local smoke_dir="${case_dir}/cluster"
  local ms_port=$((BASE_PORT + ordinal * 100))
  local server_port=$((ms_port + 10))
  local cluster_name="docker_${mode}_t${threads}"
  local bootstrap_pid_file="${case_dir}/bootstrap.pid"
  local server_extra_flags=()
  local storage_uri="file://${smoke_dir}/storage/"
  local table_election_policy="PROMOTE_DERIVED"
  local table_relation="ANTI_ENTROPY"
  local storage_async="false"

  mkdir -p "${case_dir}"
  rm -rf "${smoke_dir}"

  if [[ "${mode}" == "shared_async" || "${mode}" == "shared_sync" ]]; then
    storage_uri="file://${smoke_dir}/shared/"
    if [[ "${mode}" == "shared_async" ]]; then
      storage_async="true"
    fi
    while IFS= read -r flag; do
      server_extra_flags+=("${flag}")
    done < <(temporalstore_replicator_loop_flags)
  elif [[ "${mode}" == "raft" ]]; then
    table_election_policy="PROMOTE_SECONDARY"
    table_relation="INDEPENDENT"
    server_extra_flags+=("--data_replication_mode=raft_consensus")
    server_extra_flags+=("--data_raft_work_dir=${smoke_dir}/data-raft")
    server_extra_flags+=("--data_raft_raft_port_delta=${DATA_RAFT_RAFT_PORT_DELTA}")
    server_extra_flags+=("--data_raft_snapshot_port_delta=${DATA_RAFT_SNAPSHOT_PORT_DELTA}")
    server_extra_flags+=("--data_raft_enable_empty_snapshot_for_tests=false")
    server_extra_flags+=("--data_raft_read_mode=bounded_stale")
    server_extra_flags+=("--data_raft_bounded_stale_max_index_lag=16")
    server_extra_flags+=("--data_raft_propose_timeout_ms=5000")
    storage_async="true"
  else
    echo "unknown mode: ${mode}" >&2
    exit 2
  fi

  local ports=(
    "${ms_port}" "$((ms_port + 30))" "$((ms_port + 60))"
    "${server_port}" "$((server_port + 1))" "$((server_port + 2))"
  )
  if [[ "${mode}" == "raft" ]]; then
    ports+=(
      "$((server_port + DATA_RAFT_RAFT_PORT_DELTA))"
      "$((server_port + 1 + DATA_RAFT_RAFT_PORT_DELTA))"
      "$((server_port + 2 + DATA_RAFT_RAFT_PORT_DELTA))"
      "$((server_port + DATA_RAFT_SNAPSHOT_PORT_DELTA))"
      "$((server_port + 1 + DATA_RAFT_SNAPSHOT_PORT_DELTA))"
      "$((server_port + 2 + DATA_RAFT_SNAPSHOT_PORT_DELTA))"
    )
  fi
  for port in "${ports[@]}"; do
    check_port_free "${port}"
  done

  server_extra_flags+=("--storage_enable_evict=false")
  server_extra_flags+=("--storage_enable_expire=false")
  server_extra_flags+=("--storage_enable_page_gc=false")
  server_extra_flags+=("--storage_enable_page_compaction=false")
  server_extra_flags+=("--storage_enable_index_gc=false")
  server_extra_flags+=("--storage_enable_oplog_rolling=false")

  (
    cd "${ROOT}"
    env \
      BUILD_TYPE="${BUILD_TYPE}" \
      OUT_DIR="${OUT_DIR}" \
      SMOKE_DIR="${smoke_dir}" \
      CLUSTER_NAME="${cluster_name}" \
      NAMESPACE_NAME=ns1 \
      TABLE_NAME=table1 \
      META_COUNT=1 \
      SERVER_COUNT=3 \
      REPLICA_COUNT=3 \
      MS_PORT="${ms_port}" \
      MS_RAFT_PORT="$((ms_port + 30))" \
      MS_SNAPSHOT_PORT="$((ms_port + 60))" \
      SERVER_PORT="${server_port}" \
      STORAGE_POOL_URI="${storage_uri}" \
      TEMPORALSTORE_STORAGE_ASYNC="${storage_async}" \
      TEMPORALSTORE_STORAGE_ZONE_SIZE=268435456 \
      TEMPORALSTORE_STREAM_MAX_BLOB_SIZE=268435456 \
      TABLE_ELECTION_POLICY="${table_election_policy}" \
      TABLE_PARTITION_UNIT_RELATION="${table_relation}" \
      SERVER_EXTRA_FLAGS="${server_extra_flags[*]}" \
      KEEP_RUNNING=1 \
      bash tools/smoke_ubuntu22.sh
  ) > "${case_dir}/bootstrap.log" 2>&1 &
  echo "$!" > "${bootstrap_pid_file}"
  wait_for_bootstrap "${bootstrap_pid_file}" "${case_dir}/bootstrap.log"
  local leader
  leader="$(awk '/metaserver leader:/ {print $3}' "${case_dir}/bootstrap.log")"
  echo "${leader}" > "${case_dir}/leader.txt"

  timeout 120 "${BIN_DIR}/replication_smoke_example" "${leader}" vdc1 ns1 table1 \
    > "${case_dir}/replication_smoke.out" 2> "${case_dir}/replication_smoke.err"

  set +e
  timeout "${BENCH_TIMEOUT_S}" "${BIN_DIR}/string_scale_benchmark" "${leader}" vdc1 ns1 table1 \
    "${OPS}" "${threads}" "${VALUE_BYTES}" 1 1000 \
    > "${case_dir}/string_primary.out" 2> "${case_dir}/string_primary.err"
  primary_code=$?
  set -e
  echo "${primary_code}" > "${case_dir}/string_primary.exit_code"
  append_string_rows "${mode}" "${threads}" primary "${case_dir}/string_primary.out" "${case_dir}"

  set +e
  timeout "${BENCH_TIMEOUT_S}" "${BIN_DIR}/string_scale_benchmark" "${leader}" vdc1 ns1 table1 \
    "${REPLICA_OPS}" "${threads}" "${VALUE_BYTES}" 0 "${REPLICA_WAIT_MS}" \
    > "${case_dir}/string_replica_eligible.out" 2> "${case_dir}/string_replica_eligible.err"
  replica_code=$?
  set -e
  echo "${replica_code}" > "${case_dir}/string_replica_eligible.exit_code"
  append_string_rows "${mode}" "${threads}" replica_eligible "${case_dir}/string_replica_eligible.out" "${case_dir}"

  set +e
  timeout 180 "${BIN_DIR}/secondary_visibility_lag_benchmark" "${leader}" vdc1 ns1 table1 \
    100 1 "${VALUE_BYTES}" 10000 0 0 \
    > "${case_dir}/secondary_visibility.out" 2> "${case_dir}/secondary_visibility.err"
  visibility_code=$?
  set -e
  echo "${visibility_code}" > "${case_dir}/secondary_visibility.exit_code"
  append_visibility_rows "${mode}" "${threads}" "${case_dir}/secondary_visibility.out" "${case_dir}"

  if [[ "${mode}" == "raft" ]]; then
    find "${smoke_dir}/data-raft" -maxdepth 4 -type f 2>/dev/null | sort \
      > "${case_dir}/data_raft_files.txt" || true
  fi

  stop_cluster "${smoke_dir}" "${bootstrap_pid_file}"
}

ordinal=0
for mode in shared_async shared_sync raft; do
  for threads in ${THREAD_LIST}; do
    echo "RUN mode=${mode} threads=${threads}"
    run_case "${mode}" "${threads}" "${ordinal}"
    ordinal=$((ordinal + 1))
  done
done

failover_status="skipped"
failover_dir="${RESULT_DIR}/raft_failover"
if [[ "${RUN_FAILOVER}" == "1" ]]; then
  mkdir -p "${failover_dir}"
  set +e
  BUILD_TYPE="${BUILD_TYPE}" \
    RESULT_DIR="${failover_dir}" \
    SMOKE_DIR="${failover_dir}/cluster" \
    RUN_LOG_DIR="${failover_dir}/runner" \
    MS_PORT="$((BASE_PORT + 900))" \
    SERVER_PORT="$((BASE_PORT + 930))" \
    CLUSTER_NAME=docker_raft_failover \
    bash "${ROOT}/tools/run_data_raft_failover_ubuntu22.sh" \
      > "${failover_dir}/failover.log" 2>&1
  code=$?
  set -e
  echo "${code}" > "${failover_dir}/exit_code"
  if [[ "${code}" == "0" ]]; then
    failover_status="pass"
  else
    failover_status="fail"
  fi
fi

python3 - "${MATRIX_CSV}" "${VISIBILITY_CSV}" "${SUMMARY_MD}" "${RESULT_DIR}" "${OPS}" "${REPLICA_OPS}" "${REPLICA_WAIT_MS}" "${VALUE_BYTES}" "${failover_status}" <<'PY'
import csv
import pathlib
import sys

matrix_path, visibility_path, summary_path, result_dir, ops, replica_ops, replica_wait_ms, value_bytes, failover_status = sys.argv[1:]

def rows(path):
    with open(path, encoding="utf-8") as fh:
        return list(csv.DictReader(fh))

matrix = rows(matrix_path)
visibility = rows(visibility_path)

def table(headers, data):
    out = []
    out.append("| " + " | ".join(headers) + " |")
    out.append("| " + " | ".join(["---"] * len(headers)) + " |")
    for row in data:
        out.append("| " + " | ".join(str(row.get(h, "")) for h in headers) + " |")
    return "\n".join(out)

write_read = []
for row in matrix:
    if row["phase"] in ("set", "get_raw_success_attempt", "get_visibility_retry"):
        write_read.append({
            "mode": row["mode"],
            "threads": row["threads"],
            "read_policy": row["read_policy"],
            "phase": row["phase"],
            "qps": row["qps"],
            "p50_us": row["p50_us"],
            "p95_us": row["p95_us"],
            "p99_us": row["p99_us"],
            "errors": row["errors"],
        })

vis = []
for row in visibility:
    if row["phase"] == "secondary_visibility_lag_after_primary_set":
        vis.append({
            "mode": row["mode"],
            "threads": row["threads"],
            "samples": row["samples"],
            "avg_us": row["avg_us"],
            "p50_us": row["p50_us"],
            "p95_us": row["p95_us"],
            "p99_us": row["p99_us"],
            "errors": row["errors"],
        })

content = f"""# TemporalStore Local Docker Replication Matrix

## Scope

This run validates local Ubuntu 22 Docker execution of TemporalStore release artifacts across:

- shared-store with async storage
- shared-store with sync storage
- data-node Raft replication

Each mode used 3 data nodes, 1 metaserver, 256-byte values, and separate 2-thread and 4-thread client passes. The STRING benchmark reports a write-only `set` phase followed by read-only `get` phases against the keys written by that pass.

## Environment

| Item | Value |
|---|---|
| Result dir | `{result_dir}` |
| Primary operations per benchmark pass | `{ops}` |
| Replica-eligible operations per benchmark pass | `{replica_ops}` |
| Replica wait before read phase | `{replica_wait_ms} ms` |
| Value bytes | `{value_bytes}` |
| Docker network | `host` |
| Docker image | runtime Ubuntu 22 dependency image |

## Write And Read QPS

{table(["mode", "threads", "read_policy", "phase", "qps", "p50_us", "p95_us", "p99_us", "errors"], write_read)}

## Secondary Visibility

{table(["mode", "threads", "samples", "avg_us", "p50_us", "p95_us", "p99_us", "errors"], vis)}

## Raft Failover

Raft failover status: `{failover_status}`.

The failover harness starts a 3-node Raft data cluster, verifies replica reads, kills the original primary, waits for metaserver promotion to a secondary, and then runs post-failover write/read validation.

## Snapshot Coverage

The local matrix records data-Raft working files under each raft case directory. Data-node Raft snapshot creation/loading is implemented in the partition path and is exercised indirectly by Byteraft when snapshots are triggered internally. This harness does not yet expose a direct data-node snapshot trigger RPC; that is the remaining gap for deterministic snapshot-install testing.

## Artifacts

- Matrix CSV: `{matrix_path}`
- Secondary visibility CSV: `{visibility_path}`
- Per-case logs: `{result_dir}`
"""

pathlib.Path(summary_path).write_text(content, encoding="utf-8")
print(summary_path)
PY

cat "${SUMMARY_MD}"
