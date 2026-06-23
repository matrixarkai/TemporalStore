# MatrixArk Codex Hook E2E With C++ TemporalStore

- status: `passed`
- backend: `temporalstore-direct`
- storage_prefix: `matrixark:codex-hook:e2e:1782234338160`
- account/user/session: `acct_codex` / `root` / `codex-session-1782234338`
- C++ record count: `168`
- final selected refs: `3`

## Record Counts

```json
{
  "context_batch_commit": 2,
  "context_embedding": 33,
  "context_entity": 14,
  "context_entity_update_audit": 12,
  "context_event": 6,
  "context_extraction_audit": 2,
  "context_index": 16,
  "context_pack_audit": 5,
  "context_segment": 5,
  "context_summary": 8,
  "context_summary_dirty": 47,
  "matrixark_audit_log": 12,
  "session_buffer_event": 6
}
```

## Hook Events

```json
[
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
    "selected_ref_count": 2,
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
    "selected_ref_count": 4,
    "session_commit": {},
    "status": "ok"
  },
  {
    "event": "Stop",
    "ingest_status": "accepted",
    "selected_ref_count": 0,
    "session_commit": {
      "commit_id_hash": 3314978138627742253,
      "commit_reason": "hook_boundary",
      "entities_written": 7,
      "raw_events_duplicated": false,
      "segments_written": 1,
      "source_event_count": 1,
      "status": "committed",
      "trigger_policy": "force"
    },
    "status": "ok"
  },
  {
    "event": "UserPromptSubmit",
    "ingest_status": "accepted",
    "selected_ref_count": 3,
    "session_commit": {},
    "status": "ok"
  }
]
```
