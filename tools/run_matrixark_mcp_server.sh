#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CPP_REPO="${MATRIXARK_CPP_TEMPORALSTORE_REPO:-/root/src/github-services/TemporalStore}"
SERVER="${MATRIXARK_MCP_SERVER:-$CPP_REPO/tools/matrixark_mcp_server.py}"
BACKEND="${MATRIXARK_MCP_BACKEND:-temporalstore-rust}"
TARGET_DIR="${CARGO_TARGET_DIR:-/tmp/temporalstore-matrixark-mcp-target}"
RUST_CLI="${MATRIXARK_TEMPORALSTORE_RUST_CLI:-$TARGET_DIR/debug/matrixark_record_log}"

# Supported shared-server backends: local, temporalstore-direct, temporalstore-rust.

if [[ ! -f "$SERVER" ]]; then
  echo "MatrixArk MCP server not found: $SERVER" >&2
  echo "Set MATRIXARK_MCP_SERVER or MATRIXARK_CPP_TEMPORALSTORE_REPO to the C++ TemporalStore checkout." >&2
  exit 2
fi

cd "$ROOT"
if [[ "$BACKEND" == "temporalstore-rust" && ! -x "$RUST_CLI" ]]; then
  CARGO_TARGET_DIR="$TARGET_DIR" cargo build -p temporalstore-rust --bin matrixark_record_log
fi

if [[ "$BACKEND" == "temporalstore-rust" ]]; then
  export MATRIXARK_TEMPORALSTORE_RUST_CLI="$RUST_CLI"
fi

exec python3 "$SERVER" --backend "$BACKEND" "$@"
