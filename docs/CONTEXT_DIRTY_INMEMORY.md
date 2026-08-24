# In-Memory Coalesced Summary-Dirty Tracking

Status: implemented (Rust +). Landed on `main` in commit
`9390d110` ("TemporalStore: in-memory coalesced summary-dirty tracking (Rust +)").

## Problem

Summary-dirty markers used to be a persisted, timestamp-keyed page family
(`ctx:dirty:{tenant}:{node}`, model `ContextDirtyModel`, id 13). Every event write
appended a new dirty marker, and a single event also propagates dirtiness up to its
parent summary nodes. So dirty records grew as **O(events × propagation fan-out)**
to express only **O(distinct dirty nodes)** of real information.

Real evidence: a 168-record end-to-end Codex-hook capture stored **47
`context_summary_dirty` records for only 6 `context_event` records** — the single
largest record type in the capture, ~8× the event count. One `UserPromptSubmit`
produced `summary_refresh.dirty_hashes` with 6 hashes (self + propagated parents).

## Change

Replace the persisted `ctx:dirty` page family with an **in-memory, coalescing map
keyed by the dirty object key** (`ctx:dirty:{tenant}:{node}`).

- `MarkSummaryDirty` no longer writes a page. It upserts a single entry per node,
  keeping the **latest `event_time_ms`** and the **max `propagate_depth`**, and
  incrementing a mark count. Repeated marks for the same node — including the
  propagated parent nodes — collapse into one entry. 500 marks on one node → 1 entry.
- `QuerySummaryDirty` returns that single coalesced marker when it overlaps the
  requested `[start_time_ms, end_time_ms]` window.

Result: dirty records are bounded by **distinct dirty nodes**, not event volume.

### Coalesced entry

```
key   = "ctx:dirty:{tenant_hash}:{node_hash}"
value = { node_hash,
          first_event_time_ms,   // min over marks
          last_event_time_ms,    // max over marks  -> reported as marker.event_time_ms
          reason,                // latest
          propagate_depth,       // max over marks
          mark_count }
```

## Why ephemeral loss is acceptable

The in-memory index is deliberately not persisted (Rust: `#[serde(skip)]`;:
process-local static map). It may be lost on restart, and that is safe:

1. The async summary-refresh worker re-marks a node dirty on its next event.
2. Dirtiness is **derivable** from records that *are* persisted:
   `node.last_event_time_ms > latest ContextSummary.valid_from_ms`. A bounded cold
   scan (or a lazy check at retrieval time) can rebuild the "known dirty" set, so the
   hashmap is a cache, not a source of truth.

## Implementation

### Rust (`crates/temporalstore-rust`)

- `engine.rs`: `ContextMarkSummaryDirty` / `ContextQuerySummaryDirty` handlers
  rewritten to use `shard.context_dirty_index` (a `HashMap<String, ContextDirtyEntry>`).
- `engine/state.rs`: added `context_dirty_index` (`#[serde(skip)]`) and the
  `ContextDirtyEntry` struct. The legacy `context_dirty` field is retained as an
  **always-empty vestigial map** so snapshot load/save and page-rebuild code compiles
  and no-ops (candidate for a later removal).
- Removed `context_dirty` from the persisted reporting paths:
  `engine/storage_pages.rs`, `engine/compaction.rs`, `engine/page_layout.rs`,
  `engine/records.rs`; `engine/expiration.rs` now expires the in-memory index.
- Verification: `examples/inmemory_dirty_check.rs` (runtime proof) and
  `tests/inmemory_dirty_summary.rs` (public-API integration test) cover coalescing,
  latest-time / max-depth semantics, the time-window filter, and the bounded
  500-marks → 1-entry property.

```
$ cargo run -p temporalstore-rust --example inmemory_dirty_check
inmemory_dirty_check: OK (coalescing, latest-ts, max-depth, window-filter, bounded 500->1)
```

### Native context extension (in-memory dirty coalescing)

`MarkSummaryDirty` / `QuerySummaryDirty` use a process-local, mutex-guarded
coalescing map (`InMemoryDirtyEntry`) instead of the `ContextDirtyModel` `OrSet`
timeline — conformance with the Rust behavior. Existing context reference-test expectations
(mark once → one marker, `propagate_depth == 1`) are preserved. Compile-verified
against the project's `compile_commands.json` flags.

## Follow-ups

- Multi-shard / separate-summary-worker topology: keep the derivable-staleness scan
  (or shard the dirty index by node ownership) as the durable fallback, since a
  process-local map is not visible to a worker in another process. On the current
  single-node deployment the hashmap is strictly better.
- Remove the now-vestigial persisted `context_dirty` field and the `ContextDirtyModel`
  page family once no snapshot compatibility concerns remain.
