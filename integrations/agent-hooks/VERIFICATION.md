# TemporalStore Agent Hooks Verification

## Local Static Checks

```bash
node --check codex/plugin/scripts/config_loader.mjs
node --check codex/plugin/scripts/payload_normalizer.mjs
node --check codex/plugin/scripts/session_resolver.mjs
node --check codex/plugin/scripts/temporalstore_client.mjs
node --check codex/plugin/scripts/temporalstore_hook_launcher.mjs
```

## Dry-Run Smoke

```bash
bash ./install/smoke_test.sh --mode dry-run
```

Expected result:

```json
{
  "status": "dry_run",
  "event": {
    "agent": "codex",
    "event": "UserPromptSubmit",
    "session_id": "..."
  }
}
```

## WSL Smoke

```powershell
.\install\smoke_test.ps1 -Mode wsl
```

Expected result:

- Hook command exits zero.
- TemporalStore/MatrixArk returns hook-specific output or a successful ingest
  summary.
- Marker text can be retrieved from TemporalStore.

## Native Smoke

```bash
bash ./install/smoke_test.sh --mode native
```

Expected result is the same as WSL smoke, but the Python hook runs directly.

## Remote Smoke

```bash
TEMPORALSTORE_AGENT_MODE=remote \
TEMPORALSTORE_AGENT_ENDPOINT=http://127.0.0.1:18080 \
bash ./install/smoke_test.sh
```

The remote endpoint should accept:

```text
POST /api/agent/hook
```

and return a JSON hook response.

## Agent Install Checks

Codex:

```bash
codex plugin list | grep temporalstore-agent-hooks
```

Claude:

- Confirm the generated settings include the hook launcher command.
- Start a new Claude session and verify the smoke marker is ingested.


## Installer Verification

Codex install without modifying the active Codex config can be tested with:

```bash
bash install/install.sh --agent codex --mode dry-run --dest /tmp/temporalstore-agent-hooks-test --skip-codex-add
```

Claude settings generation can be tested with:

```bash
bash install/install.sh --agent claude --mode dry-run --dest /tmp/temporalstore-agent-hooks-claude --claude-settings /tmp/claude-settings.json
python3 -m json.tool /tmp/claude-settings.json >/dev/null
```
