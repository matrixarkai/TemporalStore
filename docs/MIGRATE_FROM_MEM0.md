# Migrating From Mem0

TemporalStore accepts Mem0's identity dimensions and provides a compatible
`Memory` class from this repository.

## Install

```bash
python3 -m pip install -e sdk/python
export TEMPORALSTORE_BASE_URL=http://127.0.0.1:17000
```

Change the import:

```python
# before
from mem0 import Memory

# after
from temporalstore.llm_context import Memory
```

Existing `user_id`, `agent_id`, and `run_id` keyword arguments continue to
work. TemporalStore maps `run_id` to its session scope.

## Supported Operations

```python
memory = Memory()

created = memory.add("I prefer dark mode", user_id="alice")
memory_id = created["results"][0]["id"]

results = memory.search("display setting", user_id="alice", limit=8)
current = memory.get(memory_id)
memory.update(memory_id, "I prefer light mode")
changes = memory.history(memory_id)
memory.feedback(memory_id, "POSITIVE")
memory.delete(memory_id)
```

Batch update and delete calls accept at most 1,000 entries and use one bounded
HTTP request. `delete_all(user_id=...)` removes one subject's catalog records;
`reset()` clears the caller's tenant.

## Retrieval Shape

Results keep Mem0's `id`, `memory`, `score`, and `metadata` fields.
`metadata.record_type` identifies `matched_chunk`, `raw_message`, or
`entity`. A compact embedding index selects a bounded candidate set before
exact reranking and payload hydration.

## Storage

The memory catalog and history use protobuf records. Arbitrary metadata is
stored as MessagePack bytes inside protobuf. The Python-compatible HTTP edge
uses JSON.
