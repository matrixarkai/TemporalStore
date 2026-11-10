# Memory API Examples

TemporalStore exposes a non-versioned memory API through the Rust proxy.

## Python

```bash
python3 -m pip install -e sdk/python
export TEMPORALSTORE_BASE_URL=http://127.0.0.1:17000
```

```python
from temporalstore.llm_context import Memory

memory = Memory()
added = memory.add(
    "Use compact embeddings and preserve the model hash.",
    user_id="alice",
    agent_id="assistant",
    run_id="session-42",
)

found = memory.search("How should vectors be stored?", user_id="alice", limit=8)
memory.update(added["results"][0]["id"], "Use int8 when quality remains acceptable.")
history = memory.history(added["results"][0]["id"])
```

Search results are ordered and carry `metadata.record_type` with one of
`matched_chunk`, `raw_message`, or `entity`.

## HTTP

```bash
curl -s http://127.0.0.1:17000/memories \
  -H 'content-type: application/json' \
  -d '{
    "messages": [{"role": "user", "content": "Prefer compact embeddings."}],
    "user_id": "alice",
    "agent_id": "assistant",
    "run_id": "session-42"
  }'

curl -s http://127.0.0.1:17000/memories/search \
  -H 'content-type: application/json' \
  -d '{"query":"vector storage","user_id":"alice","limit":8}'
```

Other routes:

| Method | Route |
| --- | --- |
| `POST` | `/memories/list` |
| `POST` | `/memories/users` |
| `GET` | `/memories/{id}` |
| `PUT` | `/memories/{id}` |
| `DELETE` | `/memories/{id}` |
| `GET` | `/memories/{id}/history` |
| `POST` | `/memories/{id}/feedback` |
| `POST` | `/memories/batch/update` |
| `POST` | `/memories/batch/delete` |
| `POST` | `/memories/delete_all` |
| `POST` | `/memories/reset` |
