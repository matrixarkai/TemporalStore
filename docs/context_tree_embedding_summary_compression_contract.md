# Context Tree Embedding Summary Compression Contract

Shared case: `context_tree_embedding_summary_compression`.

This translates the C++ `ContextModuleTest.TreeEmbeddingSummaryAndCompressionRoundTrip` product
behavior into the shared C++/Rust corpus.

Required behavior:

- `ContextChildModel` upserts parent/child refs, reports duplicate child refs as not-created, and
  queries children in deterministic updated-time/child-hash order.
- `ContextEmbeddingModel` stores finite embedding vectors by ref hash and supports tree traversal
  by cosine similarity with global top-k per layer.
- `ContextSummaryModel` stores timestamped summaries and answers as-of queries without returning
  future summaries.
- `ContextCompressionModel` writes and queries cold-window compression events by source window.
- `QUERY_NODE_CONTEXT` returns node existence, latest summary as of the request time, and matching
  cold-window compression summaries.

Rust executable evidence:

```bash
cargo test -p temporalstore-rust \
  context_tree_embedding_summary_and_compression_match_cpp_round_trip \
  --lib -- --test-threads=1
```

C++ source evidence:

- `<cpp-temporalstore-checkout>/src/extension/context/test.cc`
- `<cpp-temporalstore-checkout>/src/extension/context/interface.proto`
