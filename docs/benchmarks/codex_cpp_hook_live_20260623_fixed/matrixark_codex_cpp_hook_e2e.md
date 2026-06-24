# MatrixArk Codex Hook E2E With C++ TemporalStore

- status: `passed`
- backend: `temporalstore-direct`
- storage_prefix: `matrixark:codex-hook:e2e-live:fixed-20260623`
- account/user/session: `acct_codex` / `root` / `codex-session-1782260669`
- C++ record count: `65`
- final selected refs: `9`

## Record Counts

```json
{
  "context_batch_commit": 2,
  "context_child_ref": 1,
  "context_embedding": 12,
  "context_event": 6,
  "context_node": 2,
  "context_summary": 6,
  "context_summary_dirty": 16,
  "matrixark_audit_log": 12,
  "session_buffer_event": 6,
  "unknown": 2
}
```

## Hook Events

```json
[
  {
    "event": "UserPromptSubmit",
    "ingest_status": "accepted",
    "selected_ref_count": 1,
    "session_commit": {},
    "status": "ok"
  },
  {
    "event": "UserPromptSubmit",
    "ingest_status": "accepted",
    "selected_ref_count": 1,
    "session_commit": {},
    "status": "ok"
  },
  {
    "event": "UserPromptSubmit",
    "ingest_status": "accepted",
    "selected_ref_count": 0,
    "session_commit": {},
    "status": "ok"
  },
  {
    "event": "UserPromptSubmit",
    "ingest_status": "accepted",
    "selected_ref_count": 5,
    "session_commit": {},
    "status": "ok"
  },
  {
    "event": "Stop",
    "ingest_status": "accepted",
    "selected_ref_count": 0,
    "session_commit": {
      "commit_id_hash": 8898898695867981001,
      "commit_reason": "hook_boundary",
      "entities_written": 1,
      "raw_events_duplicated": false,
      "segments_written": 0,
      "source_event_count": 1,
      "status": "committed",
      "trigger_policy": "force"
    },
    "status": "ok"
  },
  {
    "event": "UserPromptSubmit",
    "ingest_status": "accepted",
    "selected_ref_count": 9,
    "session_commit": {},
    "status": "ok"
  }
]
```
