# MatrixArk Intelligent Memory Segmentation

## OpenViking Status

The local open-source Viking/OpenViking code checkout does not contain the
VikingMem paper's intelligent memory segmentation module. Searches for semantic
saliency, event-centric partitioning, memory segmentation, and coordinate tuple
memory extraction only found vector-index partitioning and recall utilities.

So MatrixArk implements this as its own internal batch extraction stage.

## Problem

Conversation memory is often interleaved:

```text
A1: recursion definition and base case
B: game algorithm / minimax
A2: merge sort recursion and efficiency
```

Flat chunking may return `A1, B, A2`, adding noise. Session-level consolidation
may include all of B. Fragmented retrieval may return only A1 or A2 and miss the
complete answer.

## Strategy

MatrixArk uses a two-phase segmentation stage inside one-pass batch extraction:

```text
Phase 1: semantic saliency filtering
  prune greetings, acknowledgements, and filler

Phase 2: event-centric partitioning
  assign salient messages to coherent topics
  output coordinate tuples over message indexes
  merge non-contiguous segments with the same topic
```

MatrixArk now supports two segment-boundary providers:

- `segment_provider=deterministic`: dependency-free local rules for CI and
  offline debugging.
- `segment_provider=oss`: calls a local `transformers` causal/instruct model and
  asks it to emit the same JSON shape with topics, coordinate tuples, message
  indexes, saliency scores, and summaries.

The deterministic implementation is deliberately not limited to a few demo
topics: it keeps generic business/resource facts such as owners, deadlines,
reviewer requirements, runbooks, incidents, and metrics so useful context is not
pruned just because it is outside the original keyword set.

`segment_provider=oss-fallback` or `segment_provider_fallback=true` tries the OSS
model first and falls back to deterministic rules if the local model cannot load
or returns invalid JSON.

## Record Shape

`ContextSegment`:

```json
{
  "record_type": "context_segment",
  "topic": "recursion",
  "coordinate_tuples": [[2, 3], [6, 7]],
  "message_indexes": [2, 3, 6, 7],
  "saliency_score": 0.9,
  "summary_text": "recursion definition, base case, merge sort, efficiency",
  "non_contiguous": true
}
```

Each segment also gets a `segment_text` embedding record. Retrieval scores
segments alongside events and entities.

## Why It Helps

For a recursion query, MatrixArk can retrieve:

```text
recursion segment = A1 + A2
```

and avoid:

```text
game_algorithm segment = B
```

This is stronger than raw filesystem-style chunks because segments are not
limited to contiguous file/session spans.

## Validation

Local deterministic:

```bash
python3 tools/run_matrixark_memory_segmentation_test.py \
  --backend local \
  --report-json /tmp/matrixark_segmentation_local.json
```

Local OSS model through MCP/API:

```json
{
  "messages": [
    {"role": "user", "content": "Recursion means solving smaller subproblems."},
    {"role": "assistant", "content": "Game minimax is unrelated here."},
    {"role": "user", "content": "Merge sort uses recursion to split arrays."}
  ],
  "scope": {"user_id": "u1", "session_id": "s1"},
  "threshold_messages": 1,
  "segment_provider": "oss",
  "segment_model": "Qwen/Qwen2.5-0.5B-Instruct"
}
```

Offline cached model path:

```json
{
  "segment_provider": "oss",
  "segment_model_path": "/models/qwen2.5-0.5b-instruct",
  "segment_max_new_tokens": 512
}
```

C++ TemporalStore direct:

```bash
PYTHONPATH=$PWD/sdk/python \
TEMPORALSTORE_LIB=$PWD/output-ubuntu22/release/sdk/lib/libbcache2.so \
python3 tools/run_matrixark_memory_segmentation_test.py \
  --backend temporalstore-direct \
  --metaserver 127.0.0.1:18000 \
  --namespace deploy_ns \
  --table deploy_table \
  --temporalstore-lib $PWD/output-ubuntu22/release/sdk/lib/libbcache2.so \
  --report-json /tmp/matrixark_segmentation_cpp_direct.json
```
