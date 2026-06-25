# Context Benchmark Entity Segment Index Contract

This is the shared C++/Rust contract for
`context_benchmark_injection_entity_segment_index`.

The case proves that benchmark ingestion, retrieval, and injection use TemporalStore Context
models directly:

- Benchmark `ContextEntity`/node blocks map to the node-level Context model and carry the stable
  node hash, canonical name, L0 routing summary, L1 key-fact summary, and raw source ref. Rust also
  exposes first-class C++ `ContextEntityModel` storage for extracted entity attributes through
  `UPSERT_ENTITY`, `GET_ENTITY`, and `QUERY_ENTITIES`.
- `ContextSegment` maps to the timestamp-keyed event/segment Context model and carries the event
  id hash, timestamp key, source text, source ref, and related entity node hashes.
- `ContextIndexRef` provides the source secondary index from benchmark source id to the exact
  entity/segment pair.
- Retrieval must materialize L0/L1 blocks from the entity and L2 blocks from the segment.
- Injection must pack those blocks into `<context>` and persist selected audit refs that point back
  to the same entity and segment.

Rust executes this with:

```bash
cargo test -p temporalstore-rust \
  context_benchmark_injection_uses_entity_segment_l0_l1_and_secondary_index \
  --lib -- --test-threads=1
```

C++ should execute the same logical workflow through its native Context model API or corpus runner:
extract a LOCOMO-style conversation turn, query the source secondary index, retrieve L0/L1/L2
blocks, inject the prompt pack, and verify the selected refs.
