#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
default_repo="$(cd "$script_dir/.." && pwd)"

repo="${TEMPORALSTORE_REPO:-$default_repo}"
install_dir="${TEMPORALSTORE_INSTALL_DIR:-$HOME/.local/temporalstore}"
data_dir="${TEMPORALSTORE_DATA_DIR:-$HOME/Library/Application Support/TemporalStore}"
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
write_launchd=0
check_prereqs=0

usage() {
  cat <<'EOF'
Usage: install_macos_temporalstore.sh [options]

Build and deploy Rust TemporalStore on macOS.

Options:
  --repo PATH                 Source repo. Default: repo containing this script
  --install-dir PATH          Install prefix. Default: ~/.local/temporalstore
  --data-dir PATH             Persistent data root. Default: ~/Library/Application Support/TemporalStore
  --meta-port PORT            Metaserver port. Default: 17101
  --data-port PORT            Datanode port. Default: 17102
  --build                     Build release binaries before install
  --skip-build                Do not build; require release binaries to exist
  --skip-run                  Install only; do not start services
  --check-prereqs             Check dependencies and paths, then exit
  --skip-smoke                Skip health/write/read validation
  --install-codex-hook        Generate macOS Codex hook wrappers
  --hook-dir PATH             Hook wrapper dir. Default: ~/.matrixark/hooks
  --hook-prefix PREFIX        Hook prefix. Default: matrixark:codex-hook:rust
  --write-launchd             Write launchd user plist files
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
    --check-prereqs) check_prereqs=1; shift ;;
    --install-codex-hook) install_hook=1; shift ;;
    --hook-dir) hook_dir="$2"; shift 2 ;;
    --hook-prefix) hook_prefix="$2"; shift 2 ;;
    --write-launchd) write_launchd=1; shift ;;
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

print_plan() {
  cat <<EOF
Resolved TemporalStore install plan:
  repo:        $repo
  install dir: $install_dir
  data dir:    $data_dir
  metaserver:  127.0.0.1:$meta_port
  datanode:    127.0.0.1:$data_port
  build:       $do_build
  skip build:  $skip_build
  skip run:    $skip_run
  smoke test:  $((1 - skip_smoke))
  hook dir:    $hook_dir
  hook prefix: $hook_prefix
EOF
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
print_plan
need_cmd python3
need_cmd git
if [[ "$do_build" -eq 1 || "$skip_build" -eq 0 || "$check_prereqs" -eq 1 ]]; then
  command -v cargo >/dev/null 2>&1 || echo "warning: cargo not found; required when release binaries are missing or --build is used" >&2
  command -v rustc >/dev/null 2>&1 || echo "warning: rustc not found; required when release binaries are missing or --build is used" >&2
fi

if [[ ! -f "$repo/Cargo.toml" ]]; then
  echo "repo does not look like a TemporalStore checkout: $repo" >&2
  echo "clone it first, then run this script from the repo root:" >&2
  echo "  git clone https://github.com/bjmeetsfo/TemporalStore.git" >&2
  echo "  cd TemporalStore" >&2
  exit 1
fi

if [[ "$do_build" -eq 0 && "$skip_build" -eq 0 ]]; then
  if [[ ! -x "$repo/target/release/matrixark_rust_metaserver" ]]; then
    do_build=1
  fi
fi

if [[ "$check_prereqs" -eq 1 ]]; then
  echo "Prerequisite check complete. Next: ./tools/install_macos_temporalstore.sh --build"
  exit 0
fi

if [[ "$do_build" -eq 1 ]]; then
  need_cmd cargo
  need_cmd rustc
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
TS_CACHE_MEMORY_BYTES=$cache_memory_bytes
EOF

if [[ "$write_launchd" -eq 1 ]]; then
  step "Write launchd user plists"
  plist_dir="$HOME/Library/LaunchAgents"
  mkdir -p "$plist_dir"
  cat > "$plist_dir/com.matrixark.temporalstore.metaserver.plist" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
  <key>Label</key><string>com.matrixark.temporalstore.metaserver</string>
  <key>ProgramArguments</key><array><string>$bin_dir/matrixark_rust_metaserver</string></array>
  <key>EnvironmentVariables</key><dict>
    <key>TS_META_BIND_ADDR</key><string>127.0.0.1:$meta_port</string>
    <key>TS_META_ADDR</key><string>127.0.0.1:$meta_port</string>
  </dict>
  <key>StandardOutPath</key><string>$log_dir/metaserver.log</string>
  <key>StandardErrorPath</key><string>$log_dir/metaserver.err.log</string>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
</dict></plist>
EOF
  cat > "$plist_dir/com.matrixark.temporalstore.datanode.plist" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
  <key>Label</key><string>com.matrixark.temporalstore.datanode</string>
  <key>ProgramArguments</key><array><string>$bin_dir/matrixark_rust_datanode</string></array>
  <key>EnvironmentVariables</key><dict>
    <key>TS_META_ADDR</key><string>127.0.0.1:$meta_port</string>
    <key>TS_SERVER_BIND_ADDR</key><string>127.0.0.1:$data_port</string>
    <key>TS_SERVER_ADDR</key><string>127.0.0.1:$data_port</string>
    <key>TS_SERVER_ADVERTISE_ADDR</key><string>127.0.0.1:$data_port</string>
    <key>TS_CACHE_DIR</key><string>$cache_dir</string>
    <key>TS_PAGE_STORE_DIR</key><string>$page_dir</string>
    <key>TS_INDEX_DIR</key><string>$index_dir</string>
    <key>TS_CACHE_MEMORY_BYTES</key><string>$cache_memory_bytes</string>
  </dict>
  <key>StandardOutPath</key><string>$log_dir/datanode.log</string>
  <key>StandardErrorPath</key><string>$log_dir/datanode.err.log</string>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
</dict></plist>
EOF
  echo "launchd plists written under $plist_dir"
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
  write_body='{"shard_id":1,"command":{"kind":"string_set","key":"macos-native-smoke","value":[116,101,109,112,111,114,97,108,115,116,111,114,101,45,114,117,115,116,45,109,97,99,111,115,45,111,107]}}'
  read_body='{"shard_id":1,"command":{"kind":"string_get","key":"macos-native-smoke"}}'
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
  proxy_wrapper="$hook_dir/matrixark-rust-proxy-macos.sh"
  hook_wrapper="$hook_dir/matrixark-codex-hook-rust-macos.sh"
  cat > "$proxy_wrapper" <<EOF
#!/usr/bin/env bash
set -euo pipefail
export TS_CACHE_DIR="$cache_dir"
export TS_PAGE_STORE_DIR="$page_dir"
export TS_INDEX_DIR="$index_dir"
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

step "macOS TemporalStore deploy complete"
echo "Install dir: $install_dir"
echo "Data dir:    $data_dir"
echo "Metaserver:  http://127.0.0.1:$meta_port"
echo "Datanode:    http://127.0.0.1:$data_port"
