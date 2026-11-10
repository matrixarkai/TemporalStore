# TemporalStore LLM-Context Python SDK

This package provides a small, standard-library client for ingesting and
retrieving long-term LLM context through TemporalStore. It includes a
Mem0-compatible `Memory` class and does not require code from another
repository.

## Install

```bash
python3 -m pip install -e sdk/python
```

Point the client at a running TemporalStore Rust proxy:

```bash
export TEMPORALSTORE_BASE_URL=http://127.0.0.1:17000
```

## Ingest And Retrieve

```python
from temporalstore.llm_context import Memory

memory = Memory()
memory.add(
    [
        {"role": "user", "content": "Use int8 embeddings when quality stays acceptable."},
        {"role": "assistant", "content": "I will keep the model hash with every vector."},
    ],
    user_id="alice",
    agent_id="coding-assistant",
    run_id="session-42",
)

result = memory.search(
    "How should embeddings be stored?",
    user_id="alice",
    limit=8,
)
for item in result["results"]:
    print(item["id"], item["memory"], item["metadata"])
```

The API uses non-versioned routes under `/memories`. Adds synchronously run
context extraction. Search first uses the compact embedding index to select a
bounded node set, reranks that set, and only then hydrates text and entities.
Returned records identify their kind as `matched_chunk`, `raw_message`, or
`entity`, allowing applications to organize results without requesting
duplicate collections.

Durable memory catalog and history values are protobuf records. Arbitrary
metadata is encoded as MessagePack bytes inside those records. JSON is used
only at the public HTTP edge for Python and Mem0 client compatibility.

## Tests

```bash
PYTHONPATH=sdk/python \
  python3 -m unittest discover -s sdk/python/tests/llm_context -p 'test_*.py'
```
