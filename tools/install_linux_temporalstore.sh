#!/usr/bin/env bash
set -euo pipefail

repo="${TEMPORALSTORE_REPO:-/root/src/github-services/TemporalStore}"
install_dir="${TEMPORALSTORE_INSTALL_DIR:-$HOME/.local/temporalstore}"
data_dir="${TEMPORALSTORE_DATA_DIR:-$HOME/.local/share/temporalstore}"
meta_port="${TEMPORALSTORE_META_PORT:-17101}"
data_port="${TEMPORALSTORE_DATA_PORT:-17102}"
cache_memory_bytes="${TEMPORALSTORE_CACHE_MEMORY_BYTES:-67108864}"
hook_prefix="${MATRIXARK_TEMPORALSTORE_PREFIX:-matrixark:codex-hook:rust}"
hook_dir="${MATRIXARK_HOOK_DIR:-$HOME/.matrixark/hooks}"
do_build=0
skip_build=0
skip_run=0
skip_smoke=0
install_hook=0
write_systemd_user=0

usage() {
  cat <<'EOF'
Usage: install_linux_temporalstore.sh [options]

Build and deploy Rust TemporalStore on Linux.

Options:
  --repo PATH                 Source repo. Default: /root/src/github-services/TemporalStore
  --install-dir PATH          Install prefix. Default: ~/.local/temporalstore
  --data-dir PATH             Persistent data root. Default: ~/.local/share/temporalstore
  --meta-port PORT            Metaserver port. Default: 17101
  --data-port PORT            Datanode port. Default: 17102
  --build                     Build release binaries before install
  --skip-build                Do not build; require release binaries to exist
  --skip-run                  Install only; do not start services
  --skip-smoke                Skip health/write/read validation
  --install-codex-hook        Generate Linux Codex hook wrappers
  --hook-dir PATH             Hook wrapper dir. Default: ~/.matrixark/hooks
  --hook-prefix PREFIX        Hook prefix. Default: matrixark:codex-hook:rust
  --write-systemd-user        Write user-mode systemd unit files
  -h, --help                  Show this help

Environment overrides:
  TEMPORALSTORE_REPO, TEMPORALSTORE_INSTALL_DIR, TEMPORALSTORE_DATA_DIR
  TEMPORALSTORE_META_PORT, TEMPORALSTORE_DATA_PORT
  TEMPORALSTORE_CACHE_MEMORY_BYTES, MATRIXARK_TEMPORALSTORE_PREFIX
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --repo) repo="$2"; shift 2 ;;
    --install-dir) install_dir="$2"; shift 2 ;;
    --data-dir) data_dir="$2"; shift 2 ;;
    --meta-port) meta_port="$2"; shift 2 ;;
    --data-port) data_port="$2"; shift 2 ;;
    --build) do_build=1; shift ;;
    --skip-build) skip_build=1; shift ;;
    --skip-run) skip_run=1; shift ;;
    --skip-smoke) skip_smoke=1; shift ;;
    --install-codex-hook) install_hook=1; shift ;;
    --hook-dir) hook_dir="$2"; shift 2 ;;
    --hook-prefix) hook_prefix="$2"; shift 2 ;;
    --write-systemd-user) write_systemd_user=1; shift ;;
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

json_post() {
  local url="$1"
  local body="$2"
  python3 - "$url" "$body" <<'PY'
import sys
import urllib.request

url = sys.argv[1]
body = sys.argv[2].encode()
req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"})
with urllib.request.urlopen(req, timeout=10) as resp:
    print(resp.read().decode())
PY
}

http_get() {
  python3 - "$1" <<'PY'
import sys
import urllib.request

with urllib.request.urlopen(sys.argv[1], timeout=10) as resp:
    print(resp.read().decode())
PY
}

stop_pid_file() {
  local pid_file="$1"
  if [[ -f "$pid_file" ]]; then
    local pid
    pid="$(cat "$pid_file" 2>/dev/null || true)"
    if [[ -n "$pid" ]] && kill -0 "$pid" >/dev/null 2>&1; then
      kill "$pid" >/dev/null 2>&1 || true
      for _ in $(seq 1 20); do
        kill -0 "$pid" >/dev/null 2>&1 || break
        sleep 0.2
      done
    fi
    rm -f "$pid_file"
  fi
}

repo="$(cd "$repo" && pwd)"
bin_dir="$install_dir/bin"
run_dir="$data_dir/run"
log_dir="$data_dir/logs"
cache_dir="$data_dir/cache"
page_dir="$data_dir/pages"
index_dir="$data_dir/indexes"
cursor_dir="$data_dir/replica-replay-cursors"

step "Resolve dependencies"
need_cmd python3
if [[ "$do_build" -eq 1 ]]; then
  need_cmd git
  need_cmd cargo
  need_cmd rustc
fi

if [[ "$do_build" -eq 0 && "$skip_build" -eq 0 ]]; then
  if [[ ! -x "$repo/target/release/matrixark_rust_metaserver" ]]; then
    do_build=1
  fi
fi

if [[ "$do_build" -eq 1 ]]; then
  step "Build Rust TemporalStore release binaries"
  (cd "$repo" && git fetch origin main && cargo build --release -p temporalstore-rust --bins)
fi

step "Install release binaries"
mkdir -p "$bin_dir" "$run_dir" "$log_dir" "$cache_dir" "$page_dir" "$index_dir" "$cursor_dir"
for name in matrixark_rust_metaserver matrixark_rust_datanode matrixark_rust_proxy matrixark_rust_direct_sdk; do
  src="$repo/target/release/$name"
  if [[ ! -x "$src" ]]; then
    echo "missing release binary: $src" >&2
    echo "rerun with --build or build manually with cargo build --release -p temporalstore-rust --bins" >&2
    exit 1
  fi
  install -m 0755 "$src" "$bin_dir/$name"
  printf '%s -> %s\n' "$name" "$bin_dir/$name"
done

write_env_file() {
  cat > "$install_dir/temporalstore.env" <<EOF
TEMPORALSTORE_INSTALL_DIR=$install_dir
TEMPORALSTORE_DATA_DIR=$data_dir
TS_META_ADDR=127.0.0.1:$meta_port
TS_META_BIND_ADDR=127.0.0.1:$meta_port
TS_SERVER_ADDR=127.0.0.1:$data_port
TS_SERVER_BIND_ADDR=127.0.0.1:$data_port
TS_SERVER_ADVERTISE_ADDR=127.0.0.1:$data_port
TS_CACHE_DIR=$cache_dir
TS_PAGE_STORE_DIR=$page_dir
TS_INDEX_DIR=$index_dir
TS_REPLICA_REPLAY_CURSOR_DIR=$cursor_dir
TS_CACHE_MEMORY_BYTES=$cache_memory_bytes
EOF
}

write_env_file

if [[ "$write_systemd_user" -eq 1 ]]; then
  step "Write user-mode systemd units"
  unit_dir="$HOME/.config/systemd/user"
  mkdir -p "$unit_dir"
  cat > "$unit_dir/temporalstore-rust-metaserver.service" <<EOF
[Unit]
Description=Rust TemporalStore metaserver

[Service]
EnvironmentFile=$install_dir/temporalstore.env
ExecStart=$bin_dir/matrixark_rust_metaserver
Restart=on-failure
RestartSec=2

[Install]
WantedBy=default.target
EOF
  cat > "$unit_dir/temporalstore-rust-datanode.service" <<EOF
[Unit]
Description=Rust TemporalStore datanode
After=temporalstore-rust-metaserver.service

[Service]
EnvironmentFile=$install_dir/temporalstore.env
ExecStart=$bin_dir/matrixark_rust_datanode
Restart=on-failure
RestartSec=2

[Install]
WantedBy=default.target
EOF
  systemctl --user daemon-reload || true
  echo "systemd user units written under $unit_dir"
fi

if [[ "$skip_run" -eq 0 ]]; then
  step "Start Rust TemporalStore services"
  stop_pid_file "$run_dir/datanode.pid"
  stop_pid_file "$run_dir/metaserver.pid"
  (
    set -a
    # shellcheck disable=SC1090
    source "$install_dir/temporalstore.env"
    set +a
    nohup "$bin_dir/matrixark_rust_metaserver" > "$log_dir/metaserver.log" 2>&1 &
    echo $! > "$run_dir/metaserver.pid"
  )
  sleep 1
  (
    set -a
    # shellcheck disable=SC1090
    source "$install_dir/temporalstore.env"
    set +a
    nohup "$bin_dir/matrixark_rust_datanode" > "$log_dir/datanode.log" 2>&1 &
    echo $! > "$run_dir/datanode.pid"
  )
  sleep 3
  echo "metaserver pid: $(cat "$run_dir/metaserver.pid")"
  echo "datanode pid:   $(cat "$run_dir/datanode.pid")"
  tail -n 40 "$log_dir/metaserver.log" "$log_dir/datanode.log" || true
fi

if [[ "$skip_smoke" -eq 0 ]]; then
  step "Validate health and smoke write/read"
  http_get "http://127.0.0.1:$meta_port/health"
  http_get "http://127.0.0.1:$data_port/health"
  write_body='{"shard_id":1,"command":{"kind":"string_set","key":"linux-native-smoke","value":[116,101,109,112,111,114,97,108,115,116,111,114,101,45,114,117,115,116,45,108,105,110,117,120,45,111,107]}}'
  read_body='{"shard_id":1,"command":{"kind":"string_get","key":"linux-native-smoke"}}'
  json_post "http://127.0.0.1:$data_port/execute" "$write_body"
  read_response="$(json_post "http://127.0.0.1:$data_port/execute" "$read_body")"
  echo "$read_response"
  if [[ "$read_response" != *"116,101,109,112,111,114,97,108,115,116,111,114,101"* ]]; then
    echo "smoke read did not return the expected TemporalStore value" >&2
    exit 1
  fi
fi

if [[ "$install_hook" -eq 1 ]]; then
  step "Install Codex hook wrappers"
  mkdir -p "$hook_dir"
  proxy_wrapper="$hook_dir/matrixark-rust-proxy-linux.sh"
  hook_wrapper="$hook_dir/matrixark-codex-hook-rust-linux.sh"
  cat > "$proxy_wrapper" <<EOF
#!/usr/bin/env bash
set -euo pipefail
export TS_CACHE_DIR="$cache_dir"
export TS_PAGE_STORE_DIR="$page_dir"
export TS_INDEX_DIR="$index_dir"
export TS_REPLICA_REPLAY_CURSOR_DIR="$cursor_dir"
exec "$bin_dir/matrixark_rust_proxy" --serve
EOF
  cat > "$hook_wrapper" <<EOF
#!/usr/bin/env bash
set -euo pipefail
export MATRIXARK_MCP_BACKEND=temporalstore-rust
export MATRIXARK_TEMPORALSTORE_RUST_PROXY="$proxy_wrapper"
export MATRIXARK_TEMPORALSTORE_PREFIX="$hook_prefix"
export MATRIXARK_TEMPORALSTORE_METASERVER="127.0.0.1:$meta_port"
export MATRIXARK_TEMPORALSTORE_REQUEST_TIMEOUT_MS=60000
export MATRIXARK_TEMPORALSTORE_IO_TIMEOUT_MS=60000
exec python3 "$repo/tools/matrixark_agent_hook.py" --agent codex --event UserPromptSubmit --backend temporalstore-rust "\$@"
EOF
  chmod +x "$proxy_wrapper" "$hook_wrapper"
  echo "Rust proxy wrapper: $proxy_wrapper"
  echo "Codex hook wrapper: $hook_wrapper"
fi

step "Linux TemporalStore deploy complete"
echo "Install dir: $install_dir"
echo "Data dir:    $data_dir"
echo "Metaserver:  http://127.0.0.1:$meta_port"
echo "Datanode:    http://127.0.0.1:$data_port"
