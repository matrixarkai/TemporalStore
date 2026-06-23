# Context Temporal Compression Replayable Summary Contract

Shared case: `context_temporal_compression_replayable_summary`.

This translates the C++ `ContextModuleTest.TemporalCompressionBuildsReplayableSummaryWithoutDeletingSources`
product behavior into the shared C++/Rust corpus.

Required behavior:

- `COMPRESS_EVENTS` selects source events from a time window using confidence and importance
  filters.
- The generated compression summary is replayable, includes selected source snippets, records the
  source window, and reports truncation when more matching source events exist than the source
  limit.
- Compression writes a `ContextCompressionModel` event under `ctx:compress:<tenant>:<node>`.
- Raw `ContextEventModel` source events remain queryable after compression.

Rust executable evidence:

```bash
cargo test -p temporalstore-rust \
  context_temporal_compression_builds_replayable_summary_without_deleting_sources \
  --lib -- --test-threads=1
```

C++ source evidence:

- `/root/src/github-services/TemporalStore/src/extension/context/test.cc`
- `/root/src/github-services/TemporalStore/src/extension/context/interface.proto`
