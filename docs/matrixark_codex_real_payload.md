# What MatrixArk Really Gets From Codex

Date: 2026-06-23

## Short Answer

Codex does not natively send MatrixArk this full schema:

```json
{
  "messages": [{"role": "user", "content": "..."}],
  "scope": {
    "account_id": "acct_acme",
    "tenant_id": "tenant_prod",
    "user_id": "user_123",
    "session_id": "codex_thread_456"
  }
}
```

That is the MatrixArk normalized envelope. The Codex hook receives a raw hook payload from Codex plus the hook event name. Our hook wrapper then constructs the MatrixArk envelope.

## Raw Codex Hook Input

For `UserPromptSubmit`, the useful raw field is usually one of these text-like fields:

```json
{"prompt": "Alice approved the GPU purchase after finance reviewed the budget."}
```

The hook parser also accepts variants such as `user_prompt`, `input`, `text`, `message`, `params.prompt`, `params.input`, `params.text`, `turn.input`, or a list under `messages`/`items`/`input`.

For `Stop` and `PostCompact`, the useful field is usually a message or compacted summary text. For `PostToolUse`, the payload is tool-related text/JSON.

## Scope Comes From Hook Config

`account_id`, `tenant_id`, `user_id`, and `session_id` are not reliably provided by Codex as a clean MatrixArk schema. In our local setup they come from `.codex/hooks.json` command arguments:

```text
--account-id acct_codex
--tenant-id tenant_codex
--user-id deeproute
--session-id codex-thread-local
```

So MatrixArk receives those values because our hook config supplies them, not because Codex naturally emits that schema.

## What The Hook Sends To MatrixArk

The hook converts raw Codex input into:

```json
{
  "messages": [{"role": "user", "content": "Alice approved the GPU purchase after finance reviewed the budget."}],
  "scope": {
    "account_id": "acct_codex",
    "tenant_id": "tenant_codex",
    "user_id": "deeproute",
    "session_id": "codex-thread-local",
    "team": "codex",
    "project": "local"
  },
  "metadata": {
    "source": "codex_hook",
    "codex_event": "UserPromptSubmit",
    "raw_hook_payload": {"prompt": "..."}
  },
  "agent_hook": {
    "source": "codex",
    "hook_type": "before_llm",
    "auto_captured": true
  }
}
```

The hook no longer forces a deep `metadata.node_path`. MatrixArk uses the shallow default tree:

```text
user:<user_id> / session:<session_id>
```

Account and tenant stay in `scope` for access control and isolation, not as automatic ContextNode layers.

## MCP Difference

If Codex calls MatrixArk through MCP tools, then Codex/the agent explicitly calls a tool such as `matrixark_ingest` or `matrixark_retrieve` with that tool's schema. MCP calls are explicit. They do not automatically fire for every message unless Codex is instructed or hooked to call them.
