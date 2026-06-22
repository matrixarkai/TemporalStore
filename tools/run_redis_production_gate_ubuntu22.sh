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

mkdir -p "${RESULT_ROOT}"

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
    RUN_BENCH="${RUN_BENCH}" \
    BENCH_REQUESTS="${BENCH_REQUESTS}" \
    BENCH_CLIENTS="${BENCH_CLIENTS}" \
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

echo "PASS Redis production gate"
echo "${RESULT_ROOT}"
