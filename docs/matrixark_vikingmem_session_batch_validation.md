# MatrixArk VikingMem-Style Session Batch Extraction Validation

MatrixArk now explicitly validates the VikingMem-style extraction pattern:

```text
single incoming message
-> append raw ContextEvent
-> append session_buffer_event
-> defer one-pass extraction while pending < threshold
-> when pending >= threshold, run matrixark_session_commit
-> one-pass extraction over the logical session window
-> write ContextSegment + ContextEntity + ContextSummary + ContextIndex
-> keep raw ContextEvent records replayable, without duplication
```

## Why This Matters

VikingMem describes memory extraction over a logical session, for example a batch of `N` messages, and notes that a threshold around `>=20` messages often produces more consistent memories. MatrixArk follows the same production shape while still supporting one-message-at-a-time hooks from Codex, Cursor, Claude, or vertical agents.

The important point is that the AI agent does not need to send a 20-message batch manually. It can send one message at a time. MatrixArk groups messages by `account_id / tenant_id / user_id / session_id`, buffers raw events, and commits the session when either:

- `auto_batch_extract=true` and `session_buffer_threshold` is reached, or
- the agent/hook sends `matrixark_session_commit` at a session boundary such as Stop, PostCompact, task completion, or conversation close.

## Validated Regression Test

Test added:

```text
tools.test_matrixark_mcp_server.MatrixArkMcpServerTest
  .test_vikingmem_style_twenty_message_session_window_auto_extracts_once
```

Command:

```bash
PYTHONPATH=. python3 -m unittest   tools.test_matrixark_mcp_server.MatrixArkMcpServerTest.test_vikingmem_style_twenty_message_session_window_auto_extracts_once
```

Result:

```text
Ran 1 test in 0.185s
OK
```

## What The Test Proves

The test sends 20 messages one by one with:

```json
{
  "auto_batch_extract": true,
  "session_buffer_threshold": 20
}
```

Expected behavior:

- messages 1-19: `auto_batch_extract_result = null`
- message 20: `auto_batch_extract_result.status = committed`
- `threshold_messages = 20`
- `events_written = 0` during batch extraction, because raw events already exist
- `source_event_count = 20`
- `raw_events_duplicated = false`
- multiple `ContextSegment` records are created
- multiple `ContextEntity` records are created
- `ContextIndex` records are created
- `batch_l0` summary is created
- every segment/entity keeps `source_event_ids`
- retrieval uses tree traversal and does not fall back to flat scan

## Data Model Flow

### Raw online ingestion

Each incoming message writes:

```text
ContextEvent
ContextEmbedding(event_text)
ContextSummary(node_l0/node_l1/session_l0)
ContextEmbedding(node_l0/node_l1/session_l0)
session_buffer_event
```

These records make the message immediately retrievable even before the batch is committed.

### Batch/session commit

When the threshold is reached:

```text
pending_session_events(scope)
-> message_from_event_record(...)
-> batch_extract(derive_from_existing_events=true)
-> one_pass_memory_extraction(...)
```

The commit writes:

```text
ContextSegment
ContextEmbedding(segment_text)
ContextEntity
ContextEmbedding(entity_state)
ContextSummary(batch_l0)
ContextEmbedding(batch_l0)
ContextIndex
context_extraction_audit
context_batch_commit
```

It does not rewrite the 20 raw events.

## Why This Is Good For MatrixArk

This gives us the best of both paths:

- online hooks can send one message at a time;
- raw events are durable and immediately retrievable;
- extraction quality improves because one-pass extraction sees a full logical session window;
- source replay remains exact because segments/entities point back to the original event IDs;
- L0/L1 summaries and embeddings are ready for tree-first retrieval;
- secondary indexes can prefilter before semantic scoring.

## Recommended Defaults

For production:

```text
session_buffer_threshold = 20
auto_batch_extract = true for long-running agent hooks
session_commit on explicit Stop/PostCompact/task boundary
force=true only for explicit user/session boundary commits
```

For short sessions, MatrixArk should still commit on boundary even if fewer than 20 messages are present. That preserves memories from short but important tasks.
