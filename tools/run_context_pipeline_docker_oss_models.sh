#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODEL_DIR="${MODEL_DIR:-${ROOT}/.local/context-oss-models}"
EMBEDDING_MODEL_PATH="${EMBEDDING_MODEL_PATH:-${MODEL_DIR}/sentence-transformers/all-MiniLM-L6-v2}"
IMAGE="${IMAGE:-temporalstore-context-oss:local}"
EVENTS_PER_LANE="${EVENTS_PER_LANE:-5}"
DEBUG_DIR="${DEBUG_DIR:-${ROOT}/.local/context-debug}"
RESULT_PATH="${RESULT_PATH:-${DEBUG_DIR}/context_pipeline_docker_oss_models.json}"
KEEP_CONTAINER="${KEEP_CONTAINER:-0}"
CONTAINER_NAME="${CONTAINER_NAME:-temporalstore-context-oss-debug-$(date +%s)}"

mkdir -p "$(dirname "${RESULT_PATH}")"
CONTAINER_RESULT_PATH="${RESULT_PATH}"
if [[ "${RESULT_PATH}" == "${ROOT}/"* ]]; then
  CONTAINER_RESULT_PATH="/work/${RESULT_PATH#"${ROOT}/"}"
fi

if [[ ! -f "${EMBEDDING_MODEL_PATH}/modules.json" ]]; then
  echo "missing local embedding model at ${EMBEDDING_MODEL_PATH}" >&2
  echo "download it first:" >&2
  echo "  python3 -m pip install --user modelscope" >&2
  echo "  python3 tools/download_context_oss_models.py --source modelscope --skip-vlm" >&2
  exit 2
fi

docker build -t "${IMAGE}" - <<'DOCKERFILE'
FROM rust:1.87-bookworm
RUN apt-get update \
    && apt-get install -y --no-install-recommends rapidjson-dev python3-pip \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /work
RUN python3 -m pip install --break-system-packages \
    "sentence-transformers>=3.0.0" \
    "transformers>=4.40.0" \
    "torch>=2.2.0" \
    "pillow>=10.0.0"
DOCKERFILE

DOCKER_RUN_ARGS=()
if [[ "${KEEP_CONTAINER}" == "1" ]]; then
  DOCKER_RUN_ARGS+=(--name "${CONTAINER_NAME}")
  echo "keeping stopped debug container: ${CONTAINER_NAME}" >&2
else
  DOCKER_RUN_ARGS+=(--rm)
fi

docker run "${DOCKER_RUN_ARGS[@]}" \
  -v "${ROOT}:/work" \
  -v "${MODEL_DIR}:/models:ro" \
  -w /work \
  "${IMAGE}" \
  bash -lc "export PATH=/usr/local/cargo/bin:\$PATH && python3 tools/run_context_pipeline_scale_e2e.py \
    --events-per-lane ${EVENTS_PER_LANE} \
    --require-models \
    --embedding-model /models/sentence-transformers/all-MiniLM-L6-v2 \
    --write-results ${CONTAINER_RESULT_PATH} \
    && cat ${CONTAINER_RESULT_PATH}"
