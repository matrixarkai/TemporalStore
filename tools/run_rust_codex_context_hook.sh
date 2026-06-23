#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET_DIR="${CARGO_TARGET_DIR:-/tmp/temporalstore-context-workflow-target}"
BIN="$TARGET_DIR/debug/codex_context_hook"

cd "$ROOT"
if [[ ! -x "$BIN" ]]; then
  CARGO_TARGET_DIR="$TARGET_DIR" cargo build -p temporalstore-rust --bin codex_context_hook
fi

exec "$BIN" "$@"
