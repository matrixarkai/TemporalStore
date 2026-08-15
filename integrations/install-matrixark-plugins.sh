#!/usr/bin/env bash
# Install MatrixArk / TemporalStore memory into Claude Code and/or the OpenAI Codex CLI,
# using the modern plugin surfaces:
#   - Claude Code: the marketplace plugin `matrixark-memory@temporalstore`
#   - Codex CLI:   [mcp_servers.matrixark] + notify in ~/.codex/config.toml
#
# Usage:
#   bash integrations/install-matrixark-plugins.sh [--agent claude|codex|both]
#                                                  [--matrixark-home PATH]
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
AGENT="both"
MATRIXARK_HOME="${MATRIXARK_HOME:-${REPO_ROOT}}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --agent) AGENT="$2"; shift 2 ;;
    --matrixark-home) MATRIXARK_HOME="$2"; shift 2 ;;
    -h|--help)
      echo "Usage: install-matrixark-plugins.sh [--agent claude|codex|both] [--matrixark-home PATH]"; exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

# Make bundled scripts executable.
chmod +x "${SCRIPT_DIR}/claude-plugin/scripts/matrixark-claude-hook.sh" 2>/dev/null || true
chmod +x "${SCRIPT_DIR}/codex-plugin/scripts/matrixark-codex-notify.sh" 2>/dev/null || true
chmod +x "${SCRIPT_DIR}/codex-plugin/install-codex.sh" 2>/dev/null || true

install_claude() {
  cat <<EOF

== Claude Code plugin ==
This TemporalStore repo is a Claude Code marketplace. In Claude Code, run:

    /plugin marketplace add bjmeetsfo/TemporalStore
    /plugin install matrixark-memory@temporalstore

When enabling, set the plugin config:
    matrixark_home = ${MATRIXARK_HOME}
(or export MATRIXARK_HOME=${MATRIXARK_HOME} before launching Claude Code).

Local marketplace alternative (this checkout):
    /plugin marketplace add ${REPO_ROOT}
    /plugin install matrixark-memory@temporalstore

Slash commands after enable: /matrixark-memory:memory-recall, :memory-remember, :memory-status
EOF
}

install_codex() {
  echo
  echo "== Codex CLI integration =="
  bash "${SCRIPT_DIR}/codex-plugin/install-codex.sh" --matrixark-home "${MATRIXARK_HOME}"
}

case "${AGENT}" in
  claude) install_claude ;;
  codex)  install_codex ;;
  both)   install_claude; install_codex ;;
  *) echo "unsupported --agent: ${AGENT}" >&2; exit 2 ;;
esac

echo
echo "Done. MatrixArk memory home: ${MATRIXARK_HOME}"
echo "See integrations/PLUGINS.md for details and troubleshooting."
