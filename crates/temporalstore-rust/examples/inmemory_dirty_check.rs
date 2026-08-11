// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Runtime proof for the in-memory, coalesced summary-dirty tracking.
//!
//! Run with: cargo run -q -p temporalstore-rust --example inmemory_dirty_check
//!
//! This compiles only the (clean) library plus this example — it does not build the
//! other binaries or the internal unit-test modules, so it verifies the dirty change
//! independently of unrelated pre-existing breakage elsewhere in the crate.

use temporalstore_rust::{
    Command, CommandResponse, ContextSummaryDirtyMarker, ExecuteRequest, TemporalEngine,
};

const SHARD_ID: u64 = 1;
const TENANT: u64 = 4242;

fn engine() -> TemporalEngine {
    let mut root = std::env::temp_dir();
    root.push(format!("ts-inmemory-dirty-example-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    for sub in ["cache", "pages", "indexes"] {
        std::fs::create_dir_all(root.join(sub)).expect("create dir");
    }
    let engine = TemporalEngine::with_local_dirs(
        4096,
        root.join("cache"),
        root.join("pages"),
        root.join("indexes"),
    );
    engine.load_shard(SHARD_ID);
    engine
}

fn mark(engine: &TemporalEngine, node: u64, ts: u64, depth: u32) {
    let r = engine
        .execute(ExecuteRequest {
            shard_id: SHARD_ID,
            command: Command::ContextMarkSummaryDirty {
                tenant_hash: TENANT,
                marker: ContextSummaryDirtyMarker {
                    node_hash: node,
                    event_time_ms: ts,
                    reason: 1,
                    propagate_depth: depth,
                },
            },
        })
        .response;
    assert!(matches!(r, CommandResponse::ContextObjectKey { .. }));
}

fn query(engine: &TemporalEngine, node: u64, start: u64, end: u64) -> Vec<ContextSummaryDirtyMarker> {
    match engine
        .execute(ExecuteRequest {
            shard_id: SHARD_ID,
            command: Command::ContextQuerySummaryDirty {
                tenant_hash: TENANT,
                node_hash: node,
                start_time_ms: start,
                end_time_ms: end,
                limit: None,
            },
        })
        .response
    {
        CommandResponse::ContextSummaryDirtyMarkers { markers, .. } => markers,
        other => panic!("unexpected: {other:?}"),
    }
}

fn main() {
    // 1) Coalescing: three out-of-order marks for one node -> one marker, latest ts, max depth.
    let e = engine();
    mark(&e, 42, 1_000, 1);
    mark(&e, 42, 3_000, 2);
    mark(&e, 42, 2_000, 1);
    let m = query(&e, 42, 0, 10_000);
    assert_eq!(m.len(), 1, "coalesce to one");
    assert_eq!(m[0].event_time_ms, 3_000, "latest ts wins");
    assert_eq!(m[0].propagate_depth, 2, "max depth wins");

    // 2) Distinct nodes independent.
    mark(&e, 99, 5_000, 3);
    assert_eq!(query(&e, 42, 0, 10_000).len(), 1);
    assert_eq!(query(&e, 99, 0, 10_000)[0].propagate_depth, 3);

    // 3) Time window still filters; unknown node empty.
    assert!(query(&e, 42, 8_000, 9_000).is_empty(), "after-range empty");
    assert!(query(&e, 42, 0, 500).is_empty(), "before-range empty");
    assert!(query(&e, 7, 0, 10_000).is_empty(), "unknown node empty");

    // 4) Bounded: 500 marks on one node stay one marker (the whole point).
    let e2 = engine();
    for i in 0..500u64 {
        mark(&e2, 42, 1_000 + i, (i % 4) as u32);
    }
    let m2 = query(&e2, 42, 0, 10_000_000);
    assert_eq!(m2.len(), 1, "500 marks -> 1 coalesced marker");
    assert_eq!(m2[0].event_time_ms, 1_499);
    assert_eq!(m2[0].propagate_depth, 3);

    println!("inmemory_dirty_check: OK (coalescing, latest-ts, max-depth, window-filter, bounded 500->1)");
}
