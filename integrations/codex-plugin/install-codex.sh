#!/usr/bin/env bash
# Install the MatrixArk / TemporalStore memory integration for the OpenAI Codex CLI.
#
# Merges an [mcp_servers.matrixark] block and a `notify` handler into ~/.codex/config.toml,
# idempotently (delimited by markers). Re-running updates the block in place.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# repo root = integrations/codex-plugin/../..
DEFAULT_HOME="$(cd "${SCRIPT_DIR}/../.." && pwd)"
MATRIXARK_HOME="${MATRIXARK_HOME:-${DEFAULT_HOME}}"
CODEX_CONFIG="${CODEX_CONFIG:-${HOME}/.codex/config.toml}"
BEGIN="# >>> matrixark-memory (managed by install-codex.sh) >>>"
END="# <<< matrixark-memory <<<"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --matrixark-home) MATRIXARK_HOME="$2"; shift 2 ;;
    --config) CODEX_CONFIG="$2"; shift 2 ;;
    -h|--help)
      echo "Usage: install-codex.sh [--matrixark-home PATH] [--config ~/.codex/config.toml]"; exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

if [[ ! -f "${MATRIXARK_HOME}/tools/run_matrixark_mcp_server.sh" ]]; then
  echo "error: ${MATRIXARK_HOME} does not look like a TemporalStore checkout" >&2
  echo "       (missing tools/run_matrixark_mcp_server.sh). Pass --matrixark-home PATH." >&2
  exit 2
fi

chmod +x "${SCRIPT_DIR}/scripts/matrixark-codex-notify.sh" 2>/dev/null || true
mkdir -p "$(dirname "${CODEX_CONFIG}")"
touch "${CODEX_CONFIG}"

# Render the managed block from the example, substituting the checkout path.
BLOCK="$(sed "s#__MATRIXARK_HOME__#${MATRIXARK_HOME%/}#g" "${SCRIPT_DIR}/config.example.toml")"

# Remove any existing managed block, then append the fresh one.
tmp="$(mktemp)"
awk -v b="${BEGIN}" -v e="${END}" '
  $0==b {skip=1}
  skip==1 && $0==e {skip=0; next}
  skip==1 {next}
  {print}
' "${CODEX_CONFIG}" > "${tmp}"

{
  cat "${tmp}"
  printf '\n%s\n%s\n%s\n' "${BEGIN}" "${BLOCK}" "${END}"
} > "${CODEX_CONFIG}"
rm -f "${tmp}"

echo "Codex integration written to: ${CODEX_CONFIG}"
echo "  matrixark_home = ${MATRIXARK_HOME}"
echo "  - [mcp_servers.matrixark]  -> recall/remember memory tools"
echo "  - notify handler           -> automatic memory write per turn"
echo "Start a new Codex session; run '/mcp' to confirm the matrixark server is connected."
