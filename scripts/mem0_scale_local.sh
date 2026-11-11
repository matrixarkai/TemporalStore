#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
set -euo pipefail

repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
work_dir="$repo/target/mem0-scale-local"
port_base=18700
bin_dir=""
skip_build=0
keep_data=0
workload=()
while (($#)); do
  case "$1" in
    --work-dir|--port-base|--bin-dir)
      (($# >= 2)) || { echo "Missing value for $1" >&2; exit 2; }
      case "$1" in
        --work-dir) work_dir=$2 ;;
        --port-base) port_base=$2 ;;
        --bin-dir) bin_dir=$2 ;;
      esac
      shift 2 ;;
    --skip-build) skip_build=1; shift ;;
    --keep-data) keep_data=1; shift ;;
    --records|--batch-size|--tenant-shards|--users|--searches|--payload-bytes|--provider-json)
      (($# >= 2)) || { echo "Missing value for $1" >&2; exit 2; }
      workload+=("$1" "$2"); shift 2 ;;
    --help)
      printf '%s\n' 'Usage: scripts/mem0_scale_local.sh [--work-dir DIR] [--port-base PORT] [--skip-build] [--bin-dir DIR] [--keep-data] [workload options]' \
        'Workload options: --records --batch-size --tenant-shards --users --searches --payload-bytes --provider-json' \
        'Results: report.json, source.json, and three service logs in the work directory.' \
        'The isolated data directory must be absent; it is removed on exit unless --keep-data is set.'
      exit 0 ;;
    *) echo "Unknown option: $1" >&2; exit 2 ;;
  esac
done
[[ -z $bin_dir || $skip_build == 1 ]] || {
  echo '--bin-dir requires --skip-build' >&2; exit 2;
}
if [[ -n $bin_dir ]]; then bin_dir=$(realpath -e -- "$bin_dir"); fi
[[ $port_base =~ ^[0-9]+$ ]] && ((port_base >= 1024 && port_base <= 65533)) || {
  echo 'Port base must be between 1024 and 65533' >&2; exit 2;
}
for tool in cargo curl flock ss sha256sum; do command -v "$tool" >/dev/null; done
mkdir -p -- "$work_dir"
work_dir=$(realpath -- "$work_dir")
exec 9>"$work_dir/runner.lock"
flock -n 9 || { echo 'Another runner is using this work directory' >&2; exit 1; }
data_dir="$work_dir/data"
[[ ! -e $data_dir && ! -L $data_dir ]] || {
  echo "Data directory already exists; preserve it or select a different work directory: $data_dir" >&2
  exit 1
}
rm -f -- "$work_dir/report.json"
meta_port=$((port_base + 1))
server_port=$((port_base + 2))
for port in "$port_base" "$meta_port" "$server_port"; do
  if [[ -n $(ss -H -ltn "sport = :$port") ]]; then
    echo "Port is already in use: $port" >&2; exit 1
  fi
done
cd -- "$repo"
export CARGO_TARGET_DIR=${CARGO_TARGET_DIR:-"$repo/target"}
revision=$(git rev-parse HEAD)
diff_hash=$(git diff --binary HEAD | sha256sum | cut -d' ' -f1)
if (( ! skip_build )); then
  cargo build --release -p temporalstore-rust --bin server --bin proxy --bin metaserver --bin mem0_scale_harness
  [[ $(git rev-parse HEAD) == "$revision" && $(git diff --binary HEAD | sha256sum | cut -d' ' -f1) == "$diff_hash" ]] || {
    echo 'Tracked source changed during the build; rerun against a stable checkout' >&2; exit 1;
  }
fi
if [[ -n $bin_dir ]]; then
  bin=$(realpath -- "$bin_dir")
else
  bin=$(realpath -- "$CARGO_TARGET_DIR")/release
fi
for executable in server proxy metaserver mem0_scale_harness; do test -x "$bin/$executable"; done
printf '{"checkout_revision":"%s","tracked_diff_sha256":"%s","build_skipped":%s}\n' \
  "$revision" "$diff_hash" "$([[ $skip_build == 1 ]] && echo true || echo false)" > "$work_dir/source.json"
sha256sum "$bin/server" "$bin/proxy" "$bin/metaserver" "$bin/mem0_scale_harness" > "$work_dir/binaries.sha256"
pids=()
cleanup() {
  if ((${#pids[@]})); then
    kill "${pids[@]}" 2>/dev/null || true
    wait "${pids[@]}" 2>/dev/null || true
  fi
  if (( ! keep_data )); then
    [[ $(realpath -m -- "$data_dir") == "$work_dir/data" ]] && rm -rf -- "$data_dir"
  fi
}
mkdir -- "$data_dir"
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
wait_ready() {
  local port=$1 pid=$2
  for _ in $(seq 1 180); do
    if curl --max-time 2 -fsS "http://127.0.0.1:$port/health" >/dev/null 2>&1; then return 0; fi
    kill -0 "$pid" || return 1
    sleep 1
  done
  echo "Readiness timed out on port $port" >&2
  return 1
}
RUST_LOG=warn TS_META_ADDR="127.0.0.1:$meta_port" TS_META_MUTATION_LOG="$data_dir/meta.wal" \
  "$bin/metaserver" > "$work_dir/meta.log" 2>&1 &
meta=$!; pids+=("$meta")
wait_ready "$meta_port" "$meta"
RUST_LOG=warn TS_META_ADDR="127.0.0.1:$meta_port" TS_SERVER_ADDR="127.0.0.1:$server_port" TS_SHARD_ID=1 \
  TS_CACHE_DIR="$data_dir/cache" TS_BLOCK_STORE_DIR="$data_dir/block" TS_INDEX_DIR="$data_dir/index" \
  MATRIXARK_STORAGE_MANAGER_INTERVAL_MS=30000 "$bin/server" > "$work_dir/server.log" 2>&1 &
server=$!; pids+=("$server")
wait_ready "$server_port" "$server"
RUST_LOG=warn TS_META_ADDR="127.0.0.1:$meta_port" TS_PROXY_ADDR="127.0.0.1:$port_base" \
  TS_PROXY_CONTEXT_FIRST_SHARD=1 TS_PROXY_CONTEXT_SHARD_COUNT=1 \
  "$bin/proxy" > "$work_dir/proxy.log" 2>&1 &
proxy=$!; pids+=("$proxy")
wait_ready "$port_base" "$proxy"
"$bin/mem0_scale_harness" --addr "127.0.0.1:$port_base" --data-root "$data_dir" \
  --service-pids "$server,$proxy,$meta" --out "$work_dir/report.json" "${workload[@]}"
