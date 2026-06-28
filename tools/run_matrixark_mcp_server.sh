#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CPP_REPO="${MATRIXARK_CPP_TEMPORALSTORE_REPO:-}"
SERVER="${MATRIXARK_MCP_SERVER:-${CPP_REPO:+$CPP_REPO/tools/matrixark_mcp_server.py}}"
BACKEND="${MATRIXARK_MCP_BACKEND:-temporalstore-rust}"
TARGET_DIR="${CARGO_TARGET_DIR:-/tmp/temporalstore-matrixark-mcp-target}"
RUST_PROXY="${MATRIXARK_TEMPORALSTORE_RUST_PROXY:-$TARGET_DIR/debug/matrixark_rust_proxy}"
RUST_DIRECT_SDK="${MATRIXARK_TEMPORALSTORE_RUST_DIRECT_SDK:-$TARGET_DIR/debug/matrixark_rust_direct_sdk}"

# Supported shared-server backends: local, temporalstore-direct, temporalstore-rust,
# temporalstore-rust-direct.

if [[ -z "$SERVER" || ! -f "$SERVER" ]]; then
  echo "MatrixArk MCP server not found: $SERVER" >&2
  echo "Set MATRIXARK_MCP_SERVER or MATRIXARK_CPP_TEMPORALSTORE_REPO to the C++ TemporalStore checkout." >&2
  exit 2
fi

cd "$ROOT"
if [[ "$BACKEND" == "temporalstore-rust" ]] && [[ ! -x "$RUST_PROXY" ]]; then
  CARGO_TARGET_DIR="$TARGET_DIR" cargo build -p temporalstore-rust --bin matrixark_rust_proxy
fi
if [[ "$BACKEND" == "temporalstore-rust-direct" ]] && [[ ! -x "$RUST_DIRECT_SDK" ]]; then
  CARGO_TARGET_DIR="$TARGET_DIR" cargo build -p temporalstore-rust --bin matrixark_rust_direct_sdk
fi

if [[ "$BACKEND" == "temporalstore-rust" ]]; then
  export MATRIXARK_TEMPORALSTORE_RUST_PROXY="$RUST_PROXY"
  export MATRIXARK_TEMPORALSTORE_RUST_CLI="$RUST_PROXY"
elif [[ "$BACKEND" == "temporalstore-rust-direct" ]]; then
  export MATRIXARK_TEMPORALSTORE_RUST_DIRECT_SDK="$RUST_DIRECT_SDK"
  export MATRIXARK_TEMPORALSTORE_RUST_PROXY="${MATRIXARK_TEMPORALSTORE_RUST_PROXY:-$RUST_PROXY}"
fi

exec python3 "$SERVER" --backend "$BACKEND" "$@"
