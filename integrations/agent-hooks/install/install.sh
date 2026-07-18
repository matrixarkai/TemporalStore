#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
AGENT="codex"
MODE="dry-run"
DEST="${HOME}/plugins/temporalstore-agent-hooks"
NODE_BIN="${CODEX_NODE_PATH:-${NODE_BIN:-node}}"
CODEX_BIN="${CODEX_BIN:-codex}"
CLAUDE_SETTINGS="${CLAUDE_SETTINGS:-${HOME}/.claude/settings.json}"
SKIP_CODEX_ADD="0"
ENDPOINT="${TEMPORALSTORE_AGENT_ENDPOINT:-http://127.0.0.1:18080}"
REPO="${TEMPORALSTORE_REPO:-/root/src/github-services/TemporalStore}"
WSL_REPO="${TEMPORALSTORE_WSL_REPO:-/root/src/github-services/TemporalStore}"
PROJECT="${TEMPORALSTORE_AGENT_PROJECT:-TemporalStore}"
USER_ID="${MATRIXARK_USER_ID:-${USER:-local_user}}"

usage() {
  cat <<USAGE
Usage: install.sh [--agent codex|claude|generic] [--mode dry-run|wsl|native|remote|docker]
                  [--dest PATH] [--node PATH] [--codex-bin PATH]
                  [--claude-settings PATH] [--endpoint URL]
                  [--repo PATH] [--wsl-repo PATH] [--skip-codex-add]
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --agent) AGENT="$2"; shift 2 ;;
    --mode) MODE="$2"; shift 2 ;;
    --dest) DEST="$2"; shift 2 ;;
    --node) NODE_BIN="$2"; shift 2 ;;
    --codex-bin) CODEX_BIN="$2"; shift 2 ;;
    --claude-settings) CLAUDE_SETTINGS="$2"; shift 2 ;;
    --endpoint) ENDPOINT="$2"; shift 2 ;;
    --repo) REPO="$2"; shift 2 ;;
    --wsl-repo) WSL_REPO="$2"; shift 2 ;;
    --skip-codex-add) SKIP_CODEX_ADD="1"; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

mkdir -p "$DEST"
cp -R "$ROOT/codex/plugin/." "$DEST/"

PLUGIN_ROOT="$DEST"
HOOK_TEMPLATE="$DEST/hooks/hooks.template.json"
HOOK_FILE="$DEST/hooks/hooks.json"
sed \
  -e "s#__NODE__#${NODE_BIN//\/\\}#g" \
  -e "s#__PLUGIN_ROOT__#${PLUGIN_ROOT//\/\\}#g" \
  "$HOOK_TEMPLATE" > "$HOOK_FILE"

cat > "$DEST/.env" <<ENV
TEMPORALSTORE_AGENT_MODE=$MODE
TEMPORALSTORE_AGENT_ENDPOINT=$ENDPOINT
TEMPORALSTORE_REPO=$REPO
TEMPORALSTORE_WSL_REPO=$WSL_REPO
TEMPORALSTORE_AGENT_PROJECT=$PROJECT
TEMPORALSTORE_AGENT_NAME=$AGENT
TEMPORALSTORE_METASERVER=${TEMPORALSTORE_METASERVER:-127.0.0.1:18000}
TEMPORALSTORE_NAMESPACE=${TEMPORALSTORE_NAMESPACE:-deploy_ns}
TEMPORALSTORE_TABLE=${TEMPORALSTORE_TABLE:-deploy_table}
TEMPORALSTORE_LIBRARY=${TEMPORALSTORE_LIBRARY:-output-ubuntu22/release/sdk/lib/libbcache2.so}
MATRIXARK_STORAGE_PREFIX=${MATRIXARK_STORAGE_PREFIX:-matrixark:agent-hook}
MATRIXARK_ACCOUNT_ID=${MATRIXARK_ACCOUNT_ID:-acct_local}
MATRIXARK_TENANT_ID=${MATRIXARK_TENANT_ID:-tenant_codex}
MATRIXARK_USER_ID=$USER_ID
MATRIXARK_TEAM=${MATRIXARK_TEAM:-agent}
MATRIXARK_MAX_CONTEXT_TOKENS=${MATRIXARK_MAX_CONTEXT_TOKENS:-10000}
MATRIXARK_HOOK_ADDITIONAL_CONTEXT_CHAR_LIMIT=${MATRIXARK_HOOK_ADDITIONAL_CONTEXT_CHAR_LIMIT:-40000}
ENV
cp "$DEST/.env" "$DEST/.env.example"

install_codex() {
  local marketplace_root="${HOME}"
  local marketplace_file="${marketplace_root}/.agents/plugins/marketplace.json"
  mkdir -p "$(dirname "$marketplace_file")" "${marketplace_root}/plugins"
  if [[ "$DEST" != "${marketplace_root}/plugins/temporalstore-agent-hooks" ]]; then
    rm -rf "${marketplace_root}/plugins/temporalstore-agent-hooks"
    cp -R "$DEST" "${marketplace_root}/plugins/temporalstore-agent-hooks"
    DEST="${marketplace_root}/plugins/temporalstore-agent-hooks"
  fi
  MARKETPLACE_FILE="$marketplace_file" node - <<'NODE'
const fs = require('fs');
const file = process.env.MARKETPLACE_FILE;
let data = { name: 'personal', interface: { displayName: 'Personal' }, plugins: [] };
if (fs.existsSync(file)) data = JSON.parse(fs.readFileSync(file, 'utf8'));
data.name ||= 'personal';
data.interface ||= { displayName: 'Personal' };
data.plugins ||= [];
const entry = {
  name: 'temporalstore-agent-hooks',
  source: { source: 'local', path: './plugins/temporalstore-agent-hooks' },
  policy: { installation: 'AVAILABLE', authentication: 'ON_INSTALL' },
  category: 'Productivity'
};
const idx = data.plugins.findIndex((item) => item.name === entry.name);
if (idx >= 0) data.plugins[idx] = entry; else data.plugins.push(entry);
fs.writeFileSync(file, JSON.stringify(data, null, 2) + '\n');
NODE
  echo "Codex marketplace updated: $marketplace_file"
  if [[ "$SKIP_CODEX_ADD" != "1" ]] && command -v "$CODEX_BIN" >/dev/null 2>&1; then
    "$CODEX_BIN" plugin add temporalstore-agent-hooks@personal || true
  else
    echo "Codex plugin add skipped. Run: $CODEX_BIN plugin add temporalstore-agent-hooks@personal"
  fi
}

install_claude() {
  mkdir -p "$(dirname "$CLAUDE_SETTINGS")"
  local launcher="$DEST/scripts/temporalstore_hook_launcher.mjs"
  CLAUDE_SETTINGS="$CLAUDE_SETTINGS" NODE_BIN="$NODE_BIN" LAUNCHER="$launcher" node - <<'NODE'
const fs = require('fs');
const file = process.env.CLAUDE_SETTINGS;
let data = {};
if (fs.existsSync(file)) data = JSON.parse(fs.readFileSync(file, 'utf8'));
data.hooks ||= {};
for (const event of ['UserPromptSubmit', 'Stop']) {
  data.hooks[event] = [{ hooks: [{ type: 'command', command: `"${process.env.NODE_BIN}" "${process.env.LAUNCHER}" ${event}` }] }];
}
fs.writeFileSync(file, JSON.stringify(data, null, 2) + '\n');
NODE
  echo "Claude settings updated: $CLAUDE_SETTINGS"
}

case "$AGENT" in
  codex) install_codex ;;
  claude) install_claude ;;
  generic) echo "Generic hook launcher installed to: $DEST" ;;
  *) echo "unsupported agent: $AGENT" >&2; exit 2 ;;
esac

echo "Installed TemporalStore agent hooks to: $DEST"
echo "Smoke test: TEMPORALSTORE_AGENT_MODE=$MODE bash $ROOT/install/smoke_test.sh"
