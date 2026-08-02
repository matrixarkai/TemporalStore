//! Integration coverage for the in-memory, coalesced summary-dirty tracking.
//!
//! Summary-dirty markers are no longer persisted as one `ctx:dirty` page per event.
//! They are tracked in an in-memory, coalescing hashmap keyed by dirty object key so
//! repeated marks for the same node collapse into a single entry. These tests exercise
//! the public engine API to prove:
//!   * repeated marks for one node coalesce into a single queried marker,
//!   * the coalesced marker carries the latest event time and the max propagate depth,
//!   * distinct nodes remain independently queryable,
//!   * the time-window filter still applies,
//!   * coalescing is bounded (N marks -> 1 marker), which is the whole point of the change.

use std::path::PathBuf;

use temporalstore_rust::{
    Command, CommandResponse, ContextSummaryDirtyMarker, ExecuteRequest, TemporalEngine,
};

const SHARD_ID: u64 = 1;
const TENANT: u64 = 4242;
const CACHE_BYTES: usize = 4096;

fn unique_root(name: &str) -> PathBuf {
    // Avoid Instant/SystemTime nondeterminism concerns: use pid + a per-name suffix.
    let pid = std::process::id();
    let mut root = std::env::temp_dir();
    root.push(format!("ts-inmemory-dirty-{name}-{pid}"));
    root
}

fn new_engine(name: &str) -> TemporalEngine {
    let root = unique_root(name);
    let _ = std::fs::remove_dir_all(&root);
    for sub in ["cache", "pages", "indexes"] {
        std::fs::create_dir_all(root.join(sub)).expect("create engine dir");
    }
    let engine = TemporalEngine::with_local_dirs(
        CACHE_BYTES,
        root.join("cache"),
        root.join("pages"),
        root.join("indexes"),
    );
    engine.load_shard(SHARD_ID);
    engine
}

fn mark(engine: &TemporalEngine, node_hash: u64, event_time_ms: u64, propagate_depth: u32) {
    let response = engine
        .execute(ExecuteRequest {
            shard_id: SHARD_ID,
            command: Command::ContextMarkSummaryDirty {
                tenant_hash: TENANT,
                marker: ContextSummaryDirtyMarker {
                    node_hash,
                    event_time_ms,
                    reason: 1,
                    propagate_depth,
                },
            },
        })
        .response;
    assert!(
        matches!(response, CommandResponse::ContextObjectKey { .. }),
        "mark should return the dirty object key, got {response:?}"
    );
}

fn query(
    engine: &TemporalEngine,
    node_hash: u64,
    start_time_ms: u64,
    end_time_ms: u64,
) -> Vec<ContextSummaryDirtyMarker> {
    match engine
        .execute(ExecuteRequest {
            shard_id: SHARD_ID,
            command: Command::ContextQuerySummaryDirty {
                tenant_hash: TENANT,
                node_hash,
                start_time_ms,
                end_time_ms,
                limit: None,
            },
        })
        .response
    {
        CommandResponse::ContextSummaryDirtyMarkers { markers, .. } => markers,
        other => panic!("expected ContextSummaryDirtyMarkers, got {other:?}"),
    }
}

#[test]
fn repeated_marks_for_one_node_coalesce_to_latest_and_max_depth() {
    let engine = new_engine("coalesce");
    // Mark the same node three times, out of order, with different depths.
    mark(&engine, 42, 1_000, 1);
    mark(&engine, 42, 3_000, 2);
    mark(&engine, 42, 2_000, 1);

    let markers = query(&engine, 42, 0, 10_000);
    assert_eq!(
        markers.len(),
        1,
        "three marks for one node must coalesce into a single dirty marker"
    );
    let marker = &markers[0];
    assert_eq!(marker.node_hash, 42);
    assert_eq!(
        marker.event_time_ms, 3_000,
        "coalesced marker keeps the latest event time"
    );
    assert_eq!(
        marker.propagate_depth, 2,
        "coalesced marker keeps the deepest requested propagate depth"
    );
}

#[test]
fn distinct_nodes_are_tracked_independently() {
    let engine = new_engine("independent");
    mark(&engine, 42, 1_000, 1);
    mark(&engine, 99, 5_000, 3);

    let node_42 = query(&engine, 42, 0, 10_000);
    let node_99 = query(&engine, 99, 0, 10_000);
    assert_eq!(node_42.len(), 1);
    assert_eq!(node_99.len(), 1);
    assert_eq!(node_42[0].event_time_ms, 1_000);
    assert_eq!(node_99[0].event_time_ms, 5_000);
    assert_eq!(node_99[0].propagate_depth, 3);
}

#[test]
fn time_window_filter_still_applies() {
    let engine = new_engine("window");
    mark(&engine, 42, 1_000, 1);
    mark(&engine, 42, 3_000, 1);

    // Window overlapping [1000, 3000] returns the marker.
    assert_eq!(query(&engine, 42, 2_999, 3_001).len(), 1);
    // Window entirely after the coalesced range returns nothing.
    assert!(query(&engine, 42, 8_000, 9_000).is_empty());
    // Window entirely before the coalesced range returns nothing.
    assert!(query(&engine, 42, 0, 500).is_empty());
    // Unknown node returns nothing.
    assert!(query(&engine, 7, 0, 10_000).is_empty());
}

#[test]
fn coalescing_is_bounded_regardless_of_mark_count() {
    let engine = new_engine("bounded");
    // A hot node marked dirty hundreds of times must still be exactly one marker.
    for i in 0..500u64 {
        mark(&engine, 42, 1_000 + i, (i % 4) as u32);
    }
    let markers = query(&engine, 42, 0, 10_000_000);
    assert_eq!(
        markers.len(),
        1,
        "500 marks for one node must remain a single coalesced dirty marker"
    );
    assert_eq!(markers[0].event_time_ms, 1_499, "latest event time wins");
    assert_eq!(markers[0].propagate_depth, 3, "max depth across marks wins");
}
