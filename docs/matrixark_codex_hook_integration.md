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

Session scope is dynamic by default:

```text
explicit --session-id or MATRIXARK_SESSION_ID
-> payload session/thread/conversation id
-> payload transcript/thread path hash
-> persisted local fallback session state
```

That means hooks do not need a hard-coded `--session-id` for normal use. Add
`--session-id` only when intentionally forcing multiple hook invocations into
one named session.

## C++ Always Mode

For this repo, Codex hooks should use the C++ TemporalStore path by default:

```bash
<repo>/tools/matrixark_codex_cpp_hook.sh \
  --event UserPromptSubmit \
  --account-id acct_codex \
  --tenant-id tenant_codex \
  --user-id deeproute
```

The launcher sets:

```text
MATRIXARK_MCP_BACKEND=temporalstore-direct
MATRIXARK_TEMPORALSTORE_METASERVER=127.0.0.1:18000
MATRIXARK_TEMPORALSTORE_NAMESPACE=deploy_ns
MATRIXARK_TEMPORALSTORE_TABLE=deploy_table
MATRIXARK_TEMPORALSTORE_PREFIX=matrixark:codex-hook
TEMPORALSTORE_LIB=<repo>/output-ubuntu22/release/sdk/lib/libbcache2.so
MATRIXARK_EMBEDDING_PROVIDER=oss
MATRIXARK_REQUIRE_OSS_EMBEDDINGS=1
MATRIXARK_UNDERSTANDING_PROVIDER=oss_encoder
MATRIXARK_REQUIRE_OSS_UNDERSTANDING=1
```

That means Codex prompts, tool events, stop events, session commits, extraction,
retrieval, summaries, entities, segments, indexes, embeddings, and ContextPack
audits all go through the MatrixArk direct adapter backed by the live C++
TemporalStore SDK.

Validation report:

- [matrixark_codex_cpp_hook_e2e.md](matrixark_codex_cpp_hook_e2e.md)
- [matrixark_codex_cpp_hook_e2e.html](matrixark_codex_cpp_hook_e2e.html)
- [matrixark_codex_cpp_hook_e2e.json](matrixark_codex_cpp_hook_e2e.json)

## 2026-06-23 Live C++ Hook Validation

The Windows hook command path was validated end to end:

```text
Codex hook payload
-> commandWindows PowerShell wrapper
-> WSL matrixark_codex_cpp_hook.sh
-> matrixark_codex_hook.py
-> matrixark_ingest / matrixark_retrieve / matrixark_session_commit
-> C++ TemporalStore direct SDK
```

Validation result from `tools/run_matrixark_codex_cpp_hook_e2e.py`:

```json
{
  "status": "passed",
  "backend": "temporalstore-direct",
  "storage_prefix": "matrixark:codex-hook:e2e-live:fixed-20260623",
  "record_count": 65,
  "final_selected_ref_count": 9
}
```

Record types written in C++ TemporalStore included `context_event`,
`session_buffer_event`, `context_batch_commit`, `context_summary`,
`context_embedding`, `context_node`, `context_child_ref`, and audit records.

The hook wrapper now defaults `MATRIXARK_HOOK_AUTOSTART_CPP=1`, so a local C++
deployment is started automatically when the metaserver is not already listening.
For interactive Codex safety, the production hook still defaults
`MATRIXARK_HOOK_FAIL_OPEN=1`; the E2E harness forces fail-closed behavior.

If `.codex/hooks.json` changes, open `/hooks` in Codex and trust the updated
commands. The direct command path can be tested without waiting for a new Codex
turn by running the Windows `commandWindows` entry with a JSON payload on stdin.

## Local JSONL E2E Test

This mode remains useful for unit tests and debugging, but it is not the
preferred MatrixArk-on-Codex path.

From the TemporalStore repo:

```bash
python3 tools/matrixark_codex_hook.py \
  --event UserPromptSubmit \
  --event-log /tmp/matrixark-codex-hook.jsonl \
  --account-id acct_codex \
  --tenant-id tenant_codex \
  --user-id deeproute <<'JSON'
{"prompt":"Remember that Alice approved the GPU purchase for Project Orion."}
JSON

python3 tools/matrixark_codex_hook.py \
  --event UserPromptSubmit \
  --event-log /tmp/matrixark-codex-hook.jsonl \
  --account-id acct_codex \
  --tenant-id tenant_codex \
  --user-id deeproute <<'JSON'
{"prompt":"What was approved for Project Orion?","thread_id":"codex-thread-1"}
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
            "command": "python3 <repo>/tools/matrixark_codex_hook.py --event UserPromptSubmit --event-log /tmp/matrixark-codex-hook.jsonl --account-id acct_codex --tenant-id tenant_codex --user-id deeproute",
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
            "command": "python3 <repo>/tools/matrixark_codex_hook.py --event Stop --event-log /tmp/matrixark-codex-hook.jsonl --account-id acct_codex --tenant-id tenant_codex --user-id deeproute",
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
  "command": "python3 <repo>/tools/matrixark_codex_hook.py --event UserPromptSubmit",
  "commandWindows": "wsl -d Ubuntu2204LocalUser -- bash -lc 'cd <repo> && python3 tools/matrixark_codex_hook.py --event UserPromptSubmit --event-log /tmp/matrixark-codex-hook.jsonl --account-id acct_codex --tenant-id tenant_codex --user-id deeproute'",
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
export TEMPORALSTORE_LIB=<repo>/output-ubuntu22/release/sdk/lib/libbcache2.so
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
