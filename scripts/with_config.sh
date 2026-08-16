#!/usr/bin/env bash
# =============================================================================
# with_config.sh - launch a TemporalStore / MatrixArk process with the
# centralized config file applied to the environment first.
#
# It reads config/temporalstore.toml (or $MATRIXARK_CONFIG_FILE, or --config
# PATH) via tools/matrixark_load_config.py and exports every mapped env var that
# is NOT already set, then exec's the given command. Precedence:
#
#     built-in code default  <  config file  <  explicit environment variable
#
# The gateway and datanode read their existing env-var names unchanged, so this
# is a non-invasive shim - no reader edits required.
#
# Usage:
#     scripts/with_config.sh python3 tools/matrixark_v1_gateway.py
#     scripts/with_config.sh --config /etc/matrixark/prod.toml matrixark_rust_datanode
#     MATRIXARK_TOP_K_PER_LAYER=32 scripts/with_config.sh ...   # env still wins
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
LOADER="${REPO_ROOT}/tools/matrixark_load_config.py"

CONFIG_ARGS=()
if [[ "${1:-}" == "--config" ]]; then
  CONFIG_ARGS=(--config "${2:?--config needs a path}")
  shift 2
fi

if [[ $# -eq 0 ]]; then
  echo "usage: with_config.sh [--config PATH] <command> [args...]" >&2
  exit 2
fi

PYTHON="${PYTHON:-python3}"

# Seed the environment (env-set vars are preserved; env wins) then exec.
# shellcheck disable=SC2046
eval "$("${PYTHON}" "${LOADER}" "${CONFIG_ARGS[@]}" --print-exports)"

exec "$@"
