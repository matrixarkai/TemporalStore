#!/usr/bin/env bash
set -euo pipefail

repo="${TEMPORALSTORE_REPO:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
venv_dir="${TEMPORALSTORE_OSS_MODEL_VENV:-${repo}/.local/context-oss-models-venv}"
model_dir="${TEMPORALSTORE_OSS_MODEL_DIR:-${repo}/.local/context-oss-models}"
source_name="${TEMPORALSTORE_OSS_MODEL_SOURCE:-huggingface}"
embedding_model="${MATRIXARK_EMBEDDING_MODEL:-sentence-transformers/all-MiniLM-L6-v2}"
vlm_model="${MATRIXARK_VLM_MODEL:-Salesforce/blip-image-captioning-base}"
reader_model="${TEMPORALSTORE_READER_MODEL:-qwen2.5:0.5b}"
ollama_models="${TEMPORALSTORE_OLLAMA_MODELS:-qwen2.5:0.5b qwen2.5:1.5b nomic-embed-text}"
install_ollama=0
pull_ollama=0
install_vllm=0
skip_python_packages=0
skip_model_download=0
skip_vlm=1
write_env=1

usage() {
  cat <<'EOF'
Usage: install_context_oss_models.sh [options]

Install OpenViking/VikingMem-style OSS model dependencies for MatrixArk
TemporalStore context ingestion, extraction, retrieval, and benchmarks.

Options:
  --repo PATH                 Repo root. Default: script parent
  --venv PATH                 Python venv path. Default: <repo>/.local/context-oss-models-venv
  --model-dir PATH            Model cache path. Default: <repo>/.local/context-oss-models
  --source NAME               huggingface or modelscope. Default: huggingface
  --embedding-model NAME      Default: sentence-transformers/all-MiniLM-L6-v2
  --vlm-model NAME            Default: Salesforce/blip-image-captioning-base
  --reader-model NAME         Default: qwen2.5:0.5b
  --install-ollama            Install Ollama when it is missing
  --pull-ollama               Pull Ollama models listed in TEMPORALSTORE_OLLAMA_MODELS
  --ollama-models "A B"       Models to pull. Default: qwen2.5:0.5b qwen2.5:1.5b nomic-embed-text
  --install-vllm              Install vLLM into the venv
  --download-vlm              Download the VLM model too; default downloads embeddings only
  --skip-python-packages      Do not install Python model packages
  --skip-model-download       Do not download embedding/VLM model snapshots
  --no-env                    Do not write the env file
  -h, --help                  Show this help

Outputs:
  <model-dir>/manifest.json
  <model-dir>/context_oss_models.env

The env file can be sourced before starting TemporalStore hooks or benchmarks.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --repo) repo="$2"; shift 2 ;;
    --venv) venv_dir="$2"; shift 2 ;;
    --model-dir) model_dir="$2"; shift 2 ;;
    --source) source_name="$2"; shift 2 ;;
    --embedding-model) embedding_model="$2"; shift 2 ;;
    --vlm-model) vlm_model="$2"; shift 2 ;;
    --reader-model) reader_model="$2"; shift 2 ;;
    --install-ollama) install_ollama=1; shift ;;
    --pull-ollama) pull_ollama=1; shift ;;
    --ollama-models) ollama_models="$2"; shift 2 ;;
    --install-vllm) install_vllm=1; shift ;;
    --download-vlm) skip_vlm=0; shift ;;
    --skip-python-packages) skip_python_packages=1; shift ;;
    --skip-model-download) skip_model_download=1; shift ;;
    --no-env) write_env=0; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown option: $1" >&2; usage >&2; exit 2 ;;
  esac
done

step() {
  printf '\n== %s ==\n' "$1"
}

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing required command: $1" >&2
    exit 1
  }
}

repo="$(cd "$repo" && pwd)"
mkdir -p "$model_dir" "$(dirname "$venv_dir")"

step "Resolve OSS model dependencies"
need_cmd python3

if [[ ! -d "$venv_dir" ]]; then
  python3 -m venv "$venv_dir"
fi
# shellcheck disable=SC1091
source "$venv_dir/bin/activate"
python -m pip install --upgrade pip setuptools wheel

if [[ "$skip_python_packages" -eq 0 ]]; then
  step "Install Python OSS model packages"
  python -m pip install -r "$repo/tools/context_oss_models_requirements.txt"
  if [[ "$source_name" == "modelscope" ]]; then
    python -m pip install modelscope
  else
    python -m pip install huggingface_hub
  fi
  if [[ "$install_vllm" -eq 1 ]]; then
    python -m pip install vllm
  fi
fi

if [[ "$skip_model_download" -eq 0 ]]; then
  step "Download OSS embedding model"
  download_args=(
    "$repo/tools/download_context_oss_models.py"
    --source "$source_name"
    --cache-dir "$model_dir"
    --embedding-model "$embedding_model"
    --vlm-model "$vlm_model"
    --manifest "$model_dir/manifest.json"
  )
  if [[ "$skip_vlm" -eq 1 ]]; then
    download_args+=(--skip-vlm)
  fi
  python "${download_args[@]}"
fi

if [[ "$install_ollama" -eq 1 && ! -x "$(command -v ollama || true)" ]]; then
  step "Install Ollama"
  curl -fsSL https://ollama.com/install.sh | sh
fi

if [[ "$pull_ollama" -eq 1 ]]; then
  step "Pull Ollama models"
  need_cmd ollama
  for model in $ollama_models; do
    ollama pull "$model"
  done
fi

if [[ "$write_env" -eq 1 ]]; then
  step "Write OSS model env file"
  embedding_path="${model_dir}/${embedding_model}"
  if [[ -f "$model_dir/manifest.json" ]]; then
    embedding_path="$(python - "$model_dir/manifest.json" "$embedding_path" <<'PY'
import json
import sys
from pathlib import Path

manifest = Path(sys.argv[1])
fallback = sys.argv[2]
try:
    print(json.loads(manifest.read_text()).get("embedding_model_path") or fallback)
except Exception:
    print(fallback)
PY
)"
  fi
  env_file="$model_dir/context_oss_models.env"
  cat > "$env_file" <<EOF
MATRIXARK_EMBEDDING_PROVIDER=oss
MATRIXARK_EMBEDDING_MODEL=$embedding_model
MATRIXARK_EMBEDDING_MODEL_PATH=$embedding_path
MATRIXARK_EXTRACTION_MODEL=$reader_model
MATRIXARK_SUMMARY_MODEL=$reader_model
TEMPORALSTORE_READER_MODEL=$reader_model
TEMPORALSTORE_READER_BASE_URL=\${TEMPORALSTORE_READER_BASE_URL:-http://127.0.0.1:11434/v1}
OPENVIKING_MODEL_API_KEY=\${OPENVIKING_MODEL_API_KEY:-ollama}
EOF
  echo "$env_file"
fi

step "OSS model setup complete"
echo "venv:      $venv_dir"
echo "model dir: $model_dir"
echo "reader:    $reader_model"
