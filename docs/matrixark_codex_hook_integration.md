# MatrixArk Codex Hook Integration

Codex supports command hooks for lifecycle events such as `UserPromptSubmit`,
`PostToolUse`, and `Stop`. MatrixArk can use those hooks to ingest Codex
messages and retrieve context without modifying Codex itself.

Official Codex hook behavior used here:

- Hook config can live in `~/.codex/hooks.json`, `~/.codex/config.toml`, or a
  trusted project `.codex/hooks.json`.
- `UserPromptSubmit` runs at turn scope and captures user prompt submissions.
- `Stop` runs when a turn stops.
- Hooks are command handlers today; non-managed hooks must be reviewed/trusted
  by Codex before they run.

## What We Capture

The MatrixArk hook script is tolerant of payload shape. It reads JSON from
stdin and looks for fields such as:

```text
prompt
user_prompt
input
text
message
params.prompt
params.input
messages[].content
```

Then it calls:

```text
matrixark_ingest
matrixark_retrieve   # for UserPromptSubmit or explicit --query
```

The raw hook payload is stored in metadata for replay.

## Local E2E Test

From the TemporalStore repo:

```bash
python3 tools/matrixark_codex_hook.py \
  --event UserPromptSubmit \
  --event-log /tmp/matrixark-codex-hook.jsonl \
  --account-id acct_codex \
  --tenant-id tenant_codex \
  --user-id deeproute \
  --session-id codex-thread-1 <<'JSON'
{"prompt":"Remember that Alice approved the GPU purchase for Project Orion."}
JSON

python3 tools/matrixark_codex_hook.py \
  --event UserPromptSubmit \
  --event-log /tmp/matrixark-codex-hook.jsonl \
  --account-id acct_codex \
  --tenant-id tenant_codex \
  --user-id deeproute \
  --session-id codex-thread-1 <<'JSON'
{"prompt":"What was approved for Project Orion?"}
JSON
```

Inspect records:

```bash
tail -n 20 /tmp/matrixark-codex-hook.jsonl
```

Run the automated test:

```bash
PYTHONPATH=. python3 tools/test_matrixark_codex_hook.py
```

## Hook Config

For a trusted project-local setup, create `.codex/hooks.json`:

```json
{
  "hooks": {
    "UserPromptSubmit": [
      {
        "hooks": [
          {
            "type": "command",
            "command": "python3 /root/src/github-services/TemporalStore/tools/matrixark_codex_hook.py --event UserPromptSubmit --event-log /tmp/matrixark-codex-hook.jsonl --account-id acct_codex --tenant-id tenant_codex --user-id deeproute --session-id codex-thread-local",
            "timeout": 30,
            "statusMessage": "Ingesting prompt into MatrixArk"
          }
        ]
      }
    ],
    "Stop": [
      {
        "hooks": [
          {
            "type": "command",
            "command": "python3 /root/src/github-services/TemporalStore/tools/matrixark_codex_hook.py --event Stop --event-log /tmp/matrixark-codex-hook.jsonl --account-id acct_codex --tenant-id tenant_codex --user-id deeproute --session-id codex-thread-local",
            "timeout": 30,
            "statusMessage": "Ingesting final Codex turn signal into MatrixArk"
          }
        ]
      }
    ]
  }
}
```

On Windows, use `commandWindows` if Codex runs hooks through PowerShell/CMD:

```json
{
  "type": "command",
  "command": "python3 /root/src/github-services/TemporalStore/tools/matrixark_codex_hook.py --event UserPromptSubmit",
  "commandWindows": "wsl -d Ubuntu2204Deeproute -- bash -lc 'cd /root/src/github-services/TemporalStore && python3 tools/matrixark_codex_hook.py --event UserPromptSubmit --event-log /tmp/matrixark-codex-hook.jsonl --account-id acct_codex --tenant-id tenant_codex --user-id deeproute --session-id codex-thread-local'",
  "timeout": 30
}
```

After adding or changing hooks, open `/hooks` in Codex and trust the hook
definition before expecting it to run.

## C++ TemporalStore Mode

The hook defaults to a local JSONL MatrixArk adapter. To use C++ TemporalStore
storage, set:

```bash
export MATRIXARK_MCP_BACKEND=temporalstore-direct
export MATRIXARK_TEMPORALSTORE_METASERVER=127.0.0.1:18000
export MATRIXARK_TEMPORALSTORE_NAMESPACE=deploy_ns
export MATRIXARK_TEMPORALSTORE_TABLE=deploy_table
export MATRIXARK_TEMPORALSTORE_PREFIX=matrixark:codex-hook
export TEMPORALSTORE_LIB=/root/src/github-services/TemporalStore/output-ubuntu22/release/sdk/lib/libbcache2.so
```

Then run the same hook command. The hook will persist Codex-derived context
records into TemporalStore through the MatrixArk direct adapter.

## Product Shape

This is the first integration layer:

```text
Codex hook event
-> matrixark_codex_hook.py
-> matrixark_ingest
-> MatrixArk extraction/entity/summary/index records
-> matrixark_retrieve for user prompts
-> ContextPack audit for replay
```

For deeper hosted integrations, use Codex app-server notifications. Hooks are
the easiest local path because they can capture prompts and turn-stop signals
without requiring a custom Codex client.
