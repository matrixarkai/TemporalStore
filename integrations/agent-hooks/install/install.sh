#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
AGENT="codex"
MODE="dry-run"
DEST="${HOME}/plugins/temporalstore-agent-hooks"
NODE_BIN="${CODEX_NODE_PATH:-${NODE_BIN:-node}}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --agent)
      AGENT="$2"
      shift 2
      ;;
    --mode)
      MODE="$2"
      shift 2
      ;;
    --dest)
      DEST="$2"
      shift 2
      ;;
    --node)
      NODE_BIN="$2"
      shift 2
      ;;
    *)
      echo "unknown argument: $1" >&2
      exit 2
      ;;
  esac
done

mkdir -p "$DEST"
cp -R "$ROOT/codex/plugin/." "$DEST/"

PLUGIN_ROOT="$DEST"
HOOK_TEMPLATE="$DEST/hooks/hooks.template.json"
HOOK_FILE="$DEST/hooks/hooks.json"
sed \
  -e "s#__NODE__#${NODE_BIN//\\/\\\\}#g" \
  -e "s#__PLUGIN_ROOT__#${PLUGIN_ROOT//\\/\\\\}#g" \
  "$HOOK_TEMPLATE" > "$HOOK_FILE"

cat > "$DEST/.env.example" <<EOF
TEMPORALSTORE_AGENT_MODE=$MODE
TEMPORALSTORE_REPO=${TEMPORALSTORE_REPO:-/root/src/github-services/TemporalStore}
TEMPORALSTORE_AGENT_PROJECT=${TEMPORALSTORE_AGENT_PROJECT:-TemporalStore}
EOF

if [[ "$AGENT" == "codex" ]]; then
  echo "Codex plugin copied to: $DEST"
  echo "Install it with your Codex marketplace flow, or point a personal marketplace at this plugin."
elif [[ "$AGENT" == "claude" ]]; then
  echo "Claude settings template: $ROOT/claude/settings.example.json"
else
  echo "Installed generic hook launcher to: $DEST"
fi

echo "Run smoke test:"
echo "  TEMPORALSTORE_AGENT_MODE=$MODE $ROOT/install/smoke_test.sh"
