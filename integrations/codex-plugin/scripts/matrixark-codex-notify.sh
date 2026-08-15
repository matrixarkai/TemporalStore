#!/usr/bin/env bash
# MatrixArk memory — Codex `notify` handler.
#
# Codex calls this with a single JSON argument describing the completed turn:
#   { "type": "agent-turn-complete", "thread-id": ..., "turn-id": ..., "cwd": ...,
#     "input-messages": [...], "last-assistant-message": "..." }
#
# We ingest that turn into MatrixArk / TemporalStore memory, fire-and-forget.
# Fail-open by design: never block or slow Codex, even if the engine is absent.
set -uo pipefail

HOME_DIR="${MATRIXARK_HOME:-__MATRIXARK_HOME__}"
PAYLOAD="${1:-}"
HOOK="${HOME_DIR%/}/tools/run_rust_codex_context_hook.sh"

# Engine not present, or nothing to ingest -> exit quietly.
[[ -z "${HOME_DIR}" || ! -f "${HOOK}" || -z "${PAYLOAD}" ]] && exit 0

# Fire-and-forget: pass the notify payload on stdin; drop all output; never fail the turn.
printf '%s' "${PAYLOAD}" | "${HOOK}" --event notify >/dev/null 2>&1 || true
exit 0
