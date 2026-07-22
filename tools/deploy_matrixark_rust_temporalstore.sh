#!/usr/bin/env bash
set -euo pipefail

SCRIPT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ROOT="${MATRIXARK_REPO_ROOT:-$SCRIPT_ROOT}"
RUNTIME_DIR="${MATRIXARK_RUST_SERVICE_RUNTIME_DIR:-$ROOT/.local/runtime/matrixark-rust-temporalstore-service}"
LOG_DIR="${MATRIXARK_RUST_SERVICE_LOG_DIR:-$RUNTIME_DIR/logs}"
PID_DIR="${MATRIXARK_RUST_SERVICE_PID_DIR:-$RUNTIME_DIR/pids}"
DATA_DIR="${MATRIXARK_RUST_SERVICE_DATA_DIR:-$ROOT/.local/rust-temporalstore-service}"
TARGET_DIR="${CARGO_TARGET_DIR:-$ROOT/target}"

META_ADDR="${MATRIXARK_RUST_SERVICE_META_ADDR:-127.0.0.1:17101}"
DATANODE_ADDR="${MATRIXARK_RUST_SERVICE_DATANODE_ADDR:-127.0.0.1:17102}"
PROXY_ADDR="${MATRIXARK_RUST_SERVICE_PROXY_ADDR:-127.0.0.1:17100}"
NAMESPACE="${MATRIXARK_RUST_SERVICE_NAMESPACE:-deploy_ns}"
SHARD_ID="${MATRIXARK_RUST_SERVICE_SHARD_ID:-1}"

BUILD_PROFILE="${MATRIXARK_RUST_SERVICE_BUILD_PROFILE:-release}"
if [[ "$BUILD_PROFILE" == "release" ]]; then
  BIN_DIR="$TARGET_DIR/release"
  CARGO_PROFILE_FLAG="--release"
else
  BIN_DIR="$TARGET_DIR/debug"
  CARGO_PROFILE_FLAG=""
fi

META_BIN="${MATRIXARK_RUST_METASERVER_BIN:-$BIN_DIR/matrixark_rust_metaserver}"
DATANODE_BIN="${MATRIXARK_RUST_DATANODE_BIN:-$BIN_DIR/matrixark_rust_datanode}"
PROXY_BIN="${MATRIXARK_RUST_SERVICE_PROXY_BIN:-$BIN_DIR/matrixark_rust_service_proxy}"

usage() {
  cat <<'USAGE'
Usage: deploy_matrixark_rust_temporalstore.sh {start|stop|restart|status|build}

Starts a long-lived Rust TemporalStore topology:
  matrixark_rust_metaserver
  matrixark_rust_datanode
  matrixark_rust_service_proxy

Local/dev MatrixArk hooks can keep using the warm embedded proxy daemon.
Production/parity hooks should start this service topology and talk through
the Rust proxy/client layer rather than embedding storage in Python.
USAGE
}

mkdir -p "$LOG_DIR" "$PID_DIR" "$DATA_DIR"

service_pid_file() {
  printf '%s/%s.pid\n' "$PID_DIR" "$1"
}

pid_alive() {
  local pid_file="$1"
  [[ -f "$pid_file" ]] || return 1
  local pid
  pid="$(cat "$pid_file" 2>/dev/null || true)"
  [[ -n "$pid" ]] || return 1
  kill -0 "$pid" >/dev/null 2>&1
}

wait_tcp() {
  local addr="$1"
  local host="${addr%%:*}"
  local port="${addr##*:}"
  local name="$2"
  for _ in {1..100}; do
    if timeout 1 bash -c "</dev/tcp/$host/$port" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.05
  done
  echo "timed out waiting for $name at $addr" >&2
  return 1
}

build_bins() {
  cd "$ROOT"
  CARGO_TARGET_DIR="$TARGET_DIR" cargo build $CARGO_PROFILE_FLAG -p temporalstore-rust \
    --bin matrixark_rust_metaserver \
    --bin matrixark_rust_datanode \
    --bin matrixark_rust_service_proxy
}

ensure_bins() {
  if [[ ! -x "$META_BIN" || ! -x "$DATANODE_BIN" || ! -x "$PROXY_BIN" ]]; then
    build_bins
  fi
}

start_service() {
  local name="$1"
  local pid_file
  pid_file="$(service_pid_file "$name")"
  if pid_alive "$pid_file"; then
    echo "$name already running pid=$(cat "$pid_file")"
    return 0
  fi
  shift
  "$@" >"$LOG_DIR/$name.out" 2>"$LOG_DIR/$name.err" &
  echo "$!" >"$pid_file"
  echo "started $name pid=$(cat "$pid_file")"
}

start_all() {
  ensure_bins
  mkdir -p \
    "$DATA_DIR/cache" \
    "$DATA_DIR/pages" \
    "$DATA_DIR/indexes" \
    "$DATA_DIR/replica-replay-cursors" \
    "$DATA_DIR/meta"

  start_service matrixark_rust_metaserver env \
    TS_META_BIND_ADDR="$META_ADDR" \
    TS_META_ADDR="$META_ADDR" \
    TS_META_MUTATION_LOG="$DATA_DIR/meta/mutation-log.jsonl" \
    "$META_BIN"
  wait_tcp "$META_ADDR" matrixark_rust_metaserver

  start_service matrixark_rust_datanode env \
    TS_META_ADDR="$META_ADDR" \
    TS_SERVER_BIND_ADDR="$DATANODE_ADDR" \
    TS_SERVER_ADVERTISE_ADDR="$DATANODE_ADDR" \
    TS_SHARD_ID="$SHARD_ID" \
    TS_CACHE_DIR="$DATA_DIR/cache" \
    TS_PAGE_STORE_DIR="$DATA_DIR/pages" \
    TS_INDEX_DIR="$DATA_DIR/indexes" \
    TS_REPLICA_REPLAY_CURSOR_DIR="$DATA_DIR/replica-replay-cursors" \
    "$DATANODE_BIN"
  wait_tcp "$DATANODE_ADDR" matrixark_rust_datanode

  start_service matrixark_rust_service_proxy env \
    TS_META_ADDR="$META_ADDR" \
    TS_PROXY_BIND_ADDR="$PROXY_ADDR" \
    TS_PROXY_ADVERTISED_ADDR="$PROXY_ADDR" \
    TS_PROXY_NAMESPACE="$NAMESPACE" \
    "$PROXY_BIN"
  wait_tcp "$PROXY_ADDR" matrixark_rust_service_proxy
}

stop_service() {
  local name="$1"
  local pid_file pid
  pid_file="$(service_pid_file "$name")"
  if ! [[ -f "$pid_file" ]]; then
    return 0
  fi
  pid="$(cat "$pid_file" 2>/dev/null || true)"
  if [[ -n "$pid" ]] && kill -0 "$pid" >/dev/null 2>&1; then
    kill "$pid" >/dev/null 2>&1 || true
    for _ in {1..50}; do
      if ! kill -0 "$pid" >/dev/null 2>&1; then
        break
      fi
      sleep 0.05
    done
    if kill -0 "$pid" >/dev/null 2>&1; then
      kill -9 "$pid" >/dev/null 2>&1 || true
    fi
  fi
  rm -f "$pid_file"
}

stop_all() {
  stop_service matrixark_rust_service_proxy
  stop_service matrixark_rust_datanode
  stop_service matrixark_rust_metaserver
}

status_one() {
  local name="$1"
  local addr="$2"
  local pid_file pid state
  pid_file="$(service_pid_file "$name")"
  pid="$(cat "$pid_file" 2>/dev/null || true)"
  if [[ -n "$pid" ]] && kill -0 "$pid" >/dev/null 2>&1; then
    state="running"
  else
    state="stopped"
  fi
  printf '%s\t%s\tpid=%s\taddr=%s\n' "$name" "$state" "${pid:-}" "$addr"
}

status_all() {
  status_one matrixark_rust_metaserver "$META_ADDR"
  status_one matrixark_rust_datanode "$DATANODE_ADDR"
  status_one matrixark_rust_service_proxy "$PROXY_ADDR"
}

cmd="${1:-status}"
case "$cmd" in
  build) build_bins ;;
  start) start_all ;;
  stop) stop_all ;;
  restart) stop_all; start_all ;;
  status) status_all ;;
  -h|--help|help) usage ;;
  *) usage >&2; exit 2 ;;
esac
