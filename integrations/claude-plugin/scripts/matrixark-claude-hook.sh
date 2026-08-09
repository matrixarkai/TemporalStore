#!/usr/bin/env bash
# MatrixArk Memory plugin — Claude Code lifecycle hook wrapper.
#
# Routes a plugin hook event to the MatrixArk / TemporalStore engine
# (tools/matrixark_claude_hook.sh in your TemporalStore checkout).
#
# Fail-open by design: a memory layer must never block a turn. If the engine
# is not configured or not present, emit a benign result and exit 0.
#
# The TemporalStore checkout path comes from the plugin's `matrixark_home`
# user config (exported by Claude Code as CLAUDE_PLUGIN_OPTION_MATRIXARK_HOME),
# or from a MATRIXARK_HOME environment override.
set -uo pipefail

EVENT="${1:-}"
HOME_DIR="${MATRIXARK_HOME:-${CLAUDE_PLUGIN_OPTION_MATRIXARK_HOME:-}}"
HOOK="${HOME_DIR%/}/tools/matrixark_claude_hook.sh"

if [[ -z "${HOME_DIR}" || ! -f "${HOOK}" ]]; then
  # Engine not configured/present — do not block the turn.
  printf '{"continue":true,"suppressOutput":true}\n'
  exit 0
fi

exec bash "${HOOK}" --event "${EVENT}"
