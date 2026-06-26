#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MATRIXARK_CONTEXT_STORAGE_BENCHMARK="${ROOT}/third_party/TemporalStoreTestCorpus/tools/run_matrixark_context_storage_benchmark.py"
if [[ ! -f "${MATRIXARK_CONTEXT_STORAGE_BENCHMARK}" && -f "${ROOT}/../TemporalStoreTestCorpus/tools/run_matrixark_context_storage_benchmark.py" ]]; then
  MATRIXARK_CONTEXT_STORAGE_BENCHMARK="${ROOT}/../TemporalStoreTestCorpus/tools/run_matrixark_context_storage_benchmark.py"
fi
if [[ ! -f "${MATRIXARK_CONTEXT_STORAGE_BENCHMARK}" ]]; then
  echo "MatrixArk context storage benchmark is shared in TemporalStoreTestCorpus; initialize third_party/TemporalStoreTestCorpus or check it out as a sibling repo." >&2
  exit 2
fi
IMAGE="${IMAGE:-temporalstore-context-oss:local}"
BASE_IMAGE="${BASE_IMAGE:-python:3.12-slim}"
BUILD_IMAGE="${BUILD_IMAGE:-0}"
CONTAINER_NAME="${CONTAINER_NAME:-matrixark-cpp-oss-scale-$(date +%s)}"
DEBUG_DIR="${DEBUG_DIR:-${ROOT}/.local/context-debug/cpp-docker-oss-scale}"
MODEL_DIR="${MODEL_DIR:-${ROOT}/.local/context-oss-models}"
EMBEDDING_MODEL_PATH="${EMBEDDING_MODEL_PATH:-${MODEL_DIR}/sentence-transformers/all-MiniLM-L6-v2}"

EVENTS="${EVENTS:-120}"
QUERIES="${QUERIES:-30}"
EVENTS_PER_LANE="${EVENTS_PER_LANE:-10}"
BATCH_SIZE="${BATCH_SIZE:-20}"
SAMPLE_INTERVAL_SECONDS="${SAMPLE_INTERVAL_SECONDS:-2}"

MS_PORT="${MS_PORT:-19300}"
MS_RAFT_PORT="${MS_RAFT_PORT:-19310}"
MS_SNAPSHOT_PORT="${MS_SNAPSHOT_PORT:-19320}"
SERVER_PORT="${SERVER_PORT:-19301}"
DEPLOY_DIR="${DEPLOY_DIR:-/tmp/matrixark-cpp-docker-oss-${MS_PORT}}"
CLUSTER_NAME="${CLUSTER_NAME:-matrixarkcppdocker${MS_PORT}}"
NAMESPACE_NAME="${NAMESPACE_NAME:-deploy_ns}"
TABLE_NAME="${TABLE_NAME:-deploy_table}"
OUT_DIR="${OUT_DIR:-${ROOT}/output-ubuntu22/release}"
TEMPORALSTORE_LIB="${TEMPORALSTORE_LIB:-${OUT_DIR}/sdk/lib/libbcache2.so}"
STORAGE_PREFIX="${STORAGE_PREFIX:-matrixark:cpp:docker:oss:$(date +%s%3N)}"
START_CPP="${START_CPP:-1}"
STOP_CPP_ON_EXIT="${STOP_CPP_ON_EXIT:-0}"

OSS_RESULT="${DEBUG_DIR}/oss_model_pipeline_result.json"
CPP_REPORT="${DEBUG_DIR}/cpp_temporalstore_benchmark.json"
DOCKER_STATS="${DEBUG_DIR}/docker_stats.jsonl"
CPP_STATS="${DEBUG_DIR}/cpp_process_stats.jsonl"
RUN_LOG="${DEBUG_DIR}/run.log"
SUMMARY_JSON="${DEBUG_DIR}/summary.json"

mkdir -p "${DEBUG_DIR}"
: > "${DOCKER_STATS}"
: > "${CPP_STATS}"
: > "${RUN_LOG}"

log() {
  printf '[%s] %s\n' "$(date -Is)" "$*" | tee -a "${RUN_LOG}" >&2
}

require_file() {
  if [[ ! -e "$1" ]]; then
    log "missing required file: $1"
    exit 2
  fi
}

require_file "${TEMPORALSTORE_LIB}"
require_file "${ROOT}/tools/deploy_local_ubuntu22.sh"
require_file "${ROOT}/tools/run_context_pipeline_scale_e2e.py"

if [[ ! -f "${EMBEDDING_MODEL_PATH}/modules.json" ]]; then
  log "missing local OSS embedding model at ${EMBEDDING_MODEL_PATH}"
  log "download it first with: python3 tools/download_context_oss_models.py --source modelscope --skip-vlm"
  exit 2
fi

if ! command -v docker >/dev/null 2>&1; then
  log "docker CLI is not available in this shell"
  exit 2
fi

if ! docker info >/dev/null 2>&1; then
  log "docker daemon is not reachable from this shell"
  log "start Docker first, for example: sudo service docker start"
  exit 2
fi

cleanup() {
  set +e
  if [[ -n "${SAMPLER_PID:-}" ]]; then
    kill "${SAMPLER_PID}" >/dev/null 2>&1 || true
    wait "${SAMPLER_PID}" >/dev/null 2>&1 || true
  fi
  docker rm -f "${CONTAINER_NAME}" >/dev/null 2>&1 || true
  if [[ "${STOP_CPP_ON_EXIT}" == "1" && "${START_CPP}" == "1" ]]; then
    BUILD_TYPE=Release OUT_DIR="${OUT_DIR}" DEPLOY_DIR="${DEPLOY_DIR}" \
      CLUSTER_NAME="${CLUSTER_NAME}" NAMESPACE_NAME="${NAMESPACE_NAME}" TABLE_NAME="${TABLE_NAME}" \
      MS_PORT="${MS_PORT}" MS_RAFT_PORT="${MS_RAFT_PORT}" MS_SNAPSHOT_PORT="${MS_SNAPSHOT_PORT}" \
      SERVER_PORT="${SERVER_PORT}" "${ROOT}/tools/deploy_local_ubuntu22.sh" stop >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

if [[ "${START_CPP}" == "1" ]]; then
  log "starting local C++ TemporalStore deployment on metaserver 127.0.0.1:${MS_PORT}"
  BUILD_TYPE=Release OUT_DIR="${OUT_DIR}" DEPLOY_DIR="${DEPLOY_DIR}" \
    CLUSTER_NAME="${CLUSTER_NAME}" NAMESPACE_NAME="${NAMESPACE_NAME}" TABLE_NAME="${TABLE_NAME}" \
    MS_PORT="${MS_PORT}" MS_RAFT_PORT="${MS_RAFT_PORT}" MS_SNAPSHOT_PORT="${MS_SNAPSHOT_PORT}" \
    SERVER_PORT="${SERVER_PORT}" TEMPORALSTORE_STORAGE_ZONE_SIZE="${TEMPORALSTORE_STORAGE_ZONE_SIZE:-536870912}" \
    TEMPORALSTORE_STREAM_MAX_BLOB_SIZE="${TEMPORALSTORE_STREAM_MAX_BLOB_SIZE:-67108864}" \
    "${ROOT}/tools/deploy_local_ubuntu22.sh" start 2>&1 | tee -a "${RUN_LOG}"
fi

META_PID=""
SERVER_PID=""
[[ -f "${DEPLOY_DIR}/runtime/metaserver1.pid" ]] && META_PID="$(cat "${DEPLOY_DIR}/runtime/metaserver1.pid")"
[[ -f "${DEPLOY_DIR}/runtime/server1.pid" ]] && SERVER_PID="$(cat "${DEPLOY_DIR}/runtime/server1.pid")"
log "C++ process pids: metaserver=${META_PID:-unknown}, server=${SERVER_PID:-unknown}"

IMAGE_TO_RUN="${BASE_IMAGE}"
CONTAINER_SETUP='apt-get update && apt-get install -y --no-install-recommends g++ rapidjson-dev procps && rm -rf /var/lib/apt/lists/* && python3 -m pip install --index-url https://download.pytorch.org/whl/cpu "torch>=2.2.0" && python3 -m pip install "sentence-transformers>=3.0.0" "transformers>=4.40.0" "pillow>=10.0.0"'

if [[ "${BUILD_IMAGE}" == "1" ]]; then
  log "building Docker OSS model image ${IMAGE} from ${BASE_IMAGE}"
  docker build --build-arg BASE_IMAGE="${BASE_IMAGE}" -t "${IMAGE}" - <<'DOCKERFILE' 2>&1 | tee -a "${RUN_LOG}"
ARG BASE_IMAGE=python:3.12-slim
FROM ${BASE_IMAGE}
RUN apt-get update \
    && apt-get install -y --no-install-recommends g++ rapidjson-dev procps \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /work
RUN python3 -m pip install --index-url https://download.pytorch.org/whl/cpu "torch>=2.2.0" \
    && python3 -m pip install \
    "sentence-transformers>=3.0.0" \
    "transformers>=4.40.0" \
    "pillow>=10.0.0"
DOCKERFILE
  IMAGE_TO_RUN="${IMAGE}"
  CONTAINER_SETUP='true'
else
  log "using ${BASE_IMAGE} directly and installing CPU OSS model dependencies at container runtime"
fi

sample_metrics() {
  while true; do
    ts="$(date +%s%3N)"
    docker stats "${CONTAINER_NAME}" --no-stream --format '{{json .}}' 2>/dev/null \
      | sed "s/^/{\"ts_ms\":${ts},\"docker\":/" | sed 's/$/}/' >> "${DOCKER_STATS}" || true
    for role_pid in "metaserver:${META_PID}" "server:${SERVER_PID}"; do
      role="${role_pid%%:*}"
      pid="${role_pid#*:}"
      if [[ -n "${pid}" ]] && ps -p "${pid}" >/dev/null 2>&1; then
        ps -p "${pid}" -o pid=,pcpu=,pmem=,rss=,vsz=,comm= \
          | awk -v ts="${ts}" -v role="${role}" '{
              printf("{\"ts_ms\":%s,\"role\":\"%s\",\"pid\":%s,\"cpu_pct\":%s,\"mem_pct\":%s,\"rss_kb\":%s,\"vsz_kb\":%s,\"comm\":\"%s\"}\n", ts, role, $1, $2, $3, $4, $5, $6)
            }' >> "${CPP_STATS}" || true
      fi
    done
    sleep "${SAMPLE_INTERVAL_SECONDS}"
  done
}
sample_metrics &
SAMPLER_PID=$!

CONTAINER_OSS_RESULT="/work/${OSS_RESULT#"${ROOT}/"}"
CONTAINER_CPP_REPORT="/work/${CPP_REPORT#"${ROOT}/"}"

log "running OSS model probe plus C++ TemporalStore MatrixArk benchmark in Docker"
docker run --rm --name "${CONTAINER_NAME}" --network host \
  -v "${ROOT}:/work" \
  -v "${MODEL_DIR}:/models:ro" \
  -v /lib/x86_64-linux-gnu:/host-lib:ro \
  -v /usr/lib/x86_64-linux-gnu:/host-usr-lib:ro \
  -w /work \
  -e TEMPORALSTORE_LIB="/work/output-ubuntu22/release/sdk/lib/libbcache2.so" \
  -e PYTHONPATH="/work/sdk/python" \
  "${IMAGE_TO_RUN}" \
  bash -lc "set -euo pipefail
    ${CONTAINER_SETUP}
    mkdir -p /tmp/ts-libs
    for lib in \
      libleveldb.so.1d libfmt.so.8 libssl.so.3 libcrypto.so.3 \
      libgflags.so.2.2 libev.so.4 libprotobuf.so.23 libsnappy.so.1 \
      libabsl_*.so.20210324; do
      for path in /host-lib/\${lib} /host-usr-lib/\${lib}; do
        if compgen -G \"\${path}\" >/dev/null; then
          for match in \${path}; do ln -sf \"\${match}\" /tmp/ts-libs/; done
        fi
      done
    done
    python3 tools/run_context_pipeline_scale_e2e.py \
      --events-per-lane ${EVENTS_PER_LANE} \
      --require-models \
      --skip-rust \
      --embedding-model /models/sentence-transformers/all-MiniLM-L6-v2 \
      --write-results ${CONTAINER_OSS_RESULT}
    LD_LIBRARY_PATH=/work/output-ubuntu22/release/sdk/lib:/tmp/ts-libs \
    python3 "${MATRIXARK_CONTEXT_STORAGE_BENCHMARK}" \
      --backend temporalstore-direct \
      --metaserver 127.0.0.1:${MS_PORT} \
      --namespace ${NAMESPACE_NAME} \
      --table ${TABLE_NAME} \
      --temporalstore-lib /work/output-ubuntu22/release/sdk/lib/libbcache2.so \
      --storage-prefix ${STORAGE_PREFIX} \
      --events ${EVENTS} \
      --queries ${QUERIES} \
      --ingest-mode batch \
      --batch-size ${BATCH_SIZE} \
      --restart-before-query \
      --report-json ${CONTAINER_CPP_REPORT}
  " 2>&1 | tee -a "${RUN_LOG}"

kill "${SAMPLER_PID}" >/dev/null 2>&1 || true
wait "${SAMPLER_PID}" >/dev/null 2>&1 || true
SAMPLER_PID=""

python3 - "${OSS_RESULT}" "${CPP_REPORT}" "${DOCKER_STATS}" "${CPP_STATS}" "${SUMMARY_JSON}" <<'PY'
import json
import re
import sys
from pathlib import Path

oss_path, cpp_path, docker_path, cpp_stats_path, summary_path = map(Path, sys.argv[1:])

def load(path):
    if not path.exists():
        return None
    return json.loads(path.read_text())

def parse_pct(value):
    if value is None:
        return None
    try:
        return float(str(value).strip().rstrip("%"))
    except ValueError:
        return None

def parse_mem_mib(value):
    if not value:
        return None
    first = str(value).split("/")[0].strip()
    match = re.match(r"([0-9.]+)\s*([KMGT]?i?B)", first)
    if not match:
        return None
    amount = float(match.group(1))
    unit = match.group(2).lower()
    scale = {"kb": 1/1024, "kib": 1/1024, "mb": 1, "mib": 1, "gb": 1024, "gib": 1024, "tb": 1024*1024, "tib": 1024*1024}
    return amount * scale.get(unit, 1)

docker_cpu = []
docker_mem = []
if docker_path.exists():
    for line in docker_path.read_text().splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        stats = row.get("docker", {})
        cpu = parse_pct(stats.get("CPUPerc"))
        mem = parse_mem_mib(stats.get("MemUsage"))
        if cpu is not None:
            docker_cpu.append(cpu)
        if mem is not None:
            docker_mem.append(mem)

cpp_rows = []
if cpp_stats_path.exists():
    for line in cpp_stats_path.read_text().splitlines():
        if line.strip():
            cpp_rows.append(json.loads(line))

def max_or_none(values):
    return max(values) if values else None

cpp_by_role = {}
for row in cpp_rows:
    role = row.get("role", "unknown")
    cpp_by_role.setdefault(role, {"cpu_pct": [], "rss_mb": []})
    cpp_by_role[role]["cpu_pct"].append(float(row.get("cpu_pct", 0)))
    cpp_by_role[role]["rss_mb"].append(float(row.get("rss_kb", 0)) / 1024.0)

summary = {
    "oss_model_result_path": str(oss_path),
    "cpp_temporalstore_benchmark_path": str(cpp_path),
    "docker_stats_path": str(docker_path),
    "cpp_process_stats_path": str(cpp_stats_path),
    "oss_model_result": load(oss_path),
    "cpp_benchmark": load(cpp_path),
    "resource_metrics": {
        "docker_samples": len(docker_cpu) or len(docker_mem),
        "docker_cpu_pct_max": max_or_none(docker_cpu),
        "docker_memory_mib_max": max_or_none(docker_mem),
        "cpp_processes": {
            role: {
                "samples": len(vals["cpu_pct"]),
                "cpu_pct_max": max_or_none(vals["cpu_pct"]),
                "rss_mb_max": max_or_none(vals["rss_mb"]),
            }
            for role, vals in cpp_by_role.items()
        },
    },
}
summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
print(json.dumps(summary, indent=2, sort_keys=True))
PY

log "summary written to ${SUMMARY_JSON}"
