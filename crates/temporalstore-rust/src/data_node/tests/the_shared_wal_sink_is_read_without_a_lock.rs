// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE SHARED WAL SINK IS INSTALLED ONCE AND READ WITHOUT A LOCK.
//!
//! It sat behind an `RwLock`, and every write command held the READ guard across the whole engine
//! call, durable barrier included. A write therefore took one shared lock it had no use for, and the
//! one installer at startup could queue behind a write in flight. It is a `OnceLock` now: a read is
//! an atomic load, and a second install is refused.
//!
//! The source scan excludes comments, so prose naming the old shape cannot trip it, and it reports how
//! many reads it saw so a scan that stopped matching cannot read as a clean pass.
//!
//! rust-internal: measures this crate's own lock use, no product behaviour

#![allow(clippy::all)]
use super::*;

#[derive(Debug, Default)]
struct Sink;
impl SharedWalSink for Sink {
    fn record_write(&self, _: ShardId, _: &Command) {}
}

#[test]
fn the_shared_wal_sink_is_read_without_a_lock() {
    const SOURCES: [(&str, &str); 2] = [
        ("data_node.rs", include_str!("../../data_node.rs")),
        ("data_node/borrowed_hash_rows.rs", include_str!("../borrowed_hash_rows.rs")),
    ];
    let mut reads = 0usize;
    for (name, source) in SOURCES {
        let code: String = source
            .lines()
            .map(|line| line.split("//").next().unwrap_or(""))
            .collect::<Vec<_>>()
            .join("\n");
        let squashed: String = code.split_whitespace().collect();
        reads += squashed.matches("shared_wal_sink.get()").count();
        for banned in [
            "shared_wal_sink.read()",
            "shared_wal_sink.write()",
            "RwLock<Option<Arc<dynSharedWalSink>>>",
        ] {
            assert!(!squashed.contains(banned), "{name}: the sink is behind a lock again: {banned}");
        }
    }
    assert!(reads >= 8, "the scan saw {reads} lock-free reads; it stopped matching");
}

#[test]
fn a_second_install_of_the_shared_wal_sink_is_refused() {
    let runtime = DataNodeRuntime::new_without_workers_for_test(TemporalEngine::default(), 4);
    runtime.set_shared_wal_sink(Arc::new(Sink));
    let again = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        runtime.set_shared_wal_sink(Arc::new(Sink));
    }));
    assert!(again.is_err(), "a second install must be refused, not swapped in");
}
