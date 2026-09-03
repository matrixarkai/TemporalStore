// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Runtime proof for the configurable auto temporal-compression trigger.
//!
//! Run with: cargo run -q -p temporalstore-rust --example compression_trigger_check
//!
//! Compiles only the (clean) library plus this example. Enables the policy via env with
//! small thresholds, writes events on one node, and verifies that older windows get
//! folded into ContextCompressionEvents while the newest `keep_recent` stay raw.

use temporalstore_rust::{
    Command, CommandResponse, ContextEvent, ExecuteRequest, TemporalEngine,
};

const SHARD_ID: u64 = 1;
const TENANT: u64 = 7777;
const NODE: u64 = 4242;

fn engine() -> TemporalEngine {
    let mut root = std::env::temp_dir();
    root.push(format!("ts-compress-trigger-{}", std::process::id()));
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

fn write_event(engine: &TemporalEngine, node: u64, ts: u64) {
    let event = ContextEvent {
        event_id_hash: ts.wrapping_mul(2654435761),
        event_time_ms: ts,
        ingestion_time_ms: ts,
        kind: 0,
        event_type: 1,
        actor_hash: 0,
        status: 0,
        valid_until_ms: 0,
        confidence: 1.0,
        importance: 0.5,
        text: format!("event at {ts}"),
        source_ref: String::new(),
        related_node_hashes: Vec::new(),
        compact_attrs: Vec::new(),
        // No vector on this fixture; empty is what a record without one holds.
        vector: Vec::new(),
    };
    let r = engine
        .execute(ExecuteRequest {
            shard_id: SHARD_ID,
            command: Command::ContextWriteEvent {
                tenant_hash: TENANT,
                node_hash: node,
                event: Box::new(event),
                first_write_only: false,
                cold_storage: false,
            },
        })
        .response;
    assert!(matches!(r, CommandResponse::ContextObjectKey { .. }));
}

fn query_compression(engine: &TemporalEngine, node: u64) -> Vec<(u64, u64)> {
    match engine
        .execute(ExecuteRequest {
            shard_id: SHARD_ID,
            command: Command::ContextQueryCompressionEvents {
                tenant_hash: TENANT,
                node_hashes: vec![node],
                start_time_ms: 0,
                end_time_ms: 10_000_000,
                limit: None,
            },
        })
        .response
    {
        CommandResponse::ContextCompressionEvents { events, .. } => events
            .into_iter()
            .map(|e| (e.source_start_ms, e.source_end_ms))
            .collect(),
        other => panic!("unexpected: {other:?}"),
    }
}

fn main() {
    // Enable the trigger with small thresholds so it fires quickly.
    std::env::set_var("MATRIXARK_CONTEXT_COMPRESSION_ENABLED", "1");
    std::env::set_var("MATRIXARK_CONTEXT_COMPRESSION_MAX_RAW_EVENTS", "8");
    std::env::set_var("MATRIXARK_CONTEXT_COMPRESSION_KEEP_RECENT", "4");
    std::env::set_var("MATRIXARK_CONTEXT_COMPRESSION_WINDOW", "4");
    // Large age so only the count trigger is in play for this test.
    std::env::set_var("MATRIXARK_CONTEXT_COMPRESSION_MAX_AGE_MS", "1000000000000");

    let e = engine();
    // 20 events at times 1000..1019 on one node.
    for ts in 1000u64..1020 {
        write_event(&e, NODE, ts);
    }

    let windows = query_compression(&e, NODE);
    assert!(
        !windows.is_empty(),
        "expected auto-compression to produce at least one window"
    );
    // Newest KEEP_RECENT=4 events (1016..1019) must never be compressed.
    let max_end = windows.iter().map(|(_, end)| *end).max().unwrap();
    assert!(
        max_end <= 1015,
        "newest 4 raw events must stay uncompressed; max compressed end was {max_end}"
    );
    // Oldest event must have been compressed.
    let min_start = windows.iter().map(|(start, _)| *start).min().unwrap();
    assert_eq!(min_start, 1000, "oldest event should be in the first window");

    // Control: with the trigger disabled, no compression is written.
    std::env::set_var("MATRIXARK_CONTEXT_COMPRESSION_ENABLED", "0");
    let e2 = engine();
    for ts in 2000u64..2020 {
        write_event(&e2, NODE, ts);
    }
    assert!(
        query_compression(&e2, NODE).is_empty(),
        "disabled policy must not compress"
    );

    println!(
        "compression_trigger_check: OK ({} windows, oldest={}, newest_kept_raw>{}; disabled=no-op)",
        windows.len(),
        min_start,
        max_end
    );
}
