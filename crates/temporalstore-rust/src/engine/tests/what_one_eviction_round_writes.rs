// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What one eviction round writes, against what it dropped.
//!
//! WHY THIS EXISTS. The delete/drop eviction path takes a FULL CLONE of the shard state, encodes
//! the whole index from it, and then writes those bytes TWICE -- once through `persist_index_bytes`
//! and once as an index-log append. It does that however few objects the round dropped. So the cost
//! of a round is set by the size of the STORE, not by the size of the round, and this measures the
//! slope.
//!
//! WHY IT IS ARGUABLY REDUNDANT, in the code's own words. The compaction flush beside it records
//! why IT may not be deferred, and in doing so says what is true of this one:
//!
//!     "An expiry sweep and a delete_drop eviction both write a WAL tombstone per key before
//!      they flush, so a flush that lands late, lands stale, or never lands is re-derived by
//!      replay."
//!
//! A write whose loss is re-derived by replay is not carrying durability. It is bounding REPLAY
//! LENGTH, which is a real job -- but one that a mutation count can do per interval instead of per
//! round. This test does not make that change; it prices it, and it is the gate that will show the
//! change working.
//!
//! THE REGIME IS ASSERTED, NOT HOPED FOR. A round that drops nothing writes nothing and would
//! report a flattering zero, so every arm asserts `dropped_object_count > 0` and asserts the seed
//! landed before any ratio is taken. A probe sized so the contested case cannot arise agrees with
//! whatever it is asked.
//!
//! THE COUNTERS ARE THREAD-LOCAL. `index_encode_counts` tallies per thread and the flush runs on
//! the calling thread after the guard drops, so a single-threaded arm sees all of it. Running this
//! arm concurrently with another that encodes would not corrupt it -- each thread has its own --
//! but it would stop being one round's figure, which is why the seed and the round are in the same
//! function.

#![allow(clippy::all)]
use super::*;

const SHARD: u64 = 7;

/// Seed `keys` string records and read each one back, so they are cached and therefore evictable.
fn seed(engine: &TemporalEngine, keys: usize) {
    for item in 0..keys {
        let key = format!("drop-{item}");
        let response = engine.execute(ExecuteRequest {
            shard_id: SHARD,
            command: Command::StringSet {
                key: key.clone(),
                value: vec![b'v'; 128],
            },
        });
        assert!(response.status.ok, "seed must ack at {item}: {response:?}");
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: SHARD,
                    command: Command::StringGet { key },
                })
                .status
                .ok,
            "the read-back must ack at {item}, or the record is not cached and cannot be evicted"
        );
    }
}

/// The round's cost, swept over the store size, with the dump stage on or off.
///
/// THE DUMP ARM IS THE ATTRIBUTION. One round encodes the whole index TWICE, and only one of the
/// two encodes is the eviction flush at `storage_lifecycle_methods.rs:2265` -- there are eleven
/// whole-index encode sites in production and the round reaches more than one of them. Turning
/// `dump_before_evict` off is the control that says whether the second belongs to the dump, which
/// is cheaper and more reliable than reading the call graph.
fn sweep(dump_before_evict: bool) -> Vec<(usize, u64, u64, u64, f64)> {
    let mut rows: Vec<(usize, u64, u64, u64, f64)> = Vec::new();

    for keys in [500usize, 2_000, 8_000] {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = TemporalEngine::with_local_dirs(
            64,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(SHARD);
        seed(&engine, keys);

        // THE DENOMINATOR, ASSERTED BEFORE THE RATIO. An empty store evicts nothing and divides
        // by nothing.
        let seeded = engine
            .shards
            .read()
            .expect("engine lock poisoned")
            .get(&SHARD)
            .expect("shard is loaded")
            .strings()
            .len();
        assert_eq!(keys, seeded, "the seed must have landed, or nothing below means anything");

        crate::engine::shard_write_guard::reset_index_encode_counts();
        let report = engine.apply_storage_eviction(SHARD, 1, 2, dump_before_evict, true);
        let counts = crate::engine::shard_write_guard::index_encode_counts();

        // THE REGIME. A round that dropped nothing is not a round.
        assert_eq!("delete_drop", report.mode, "{report:?}");
        assert!(report.pressure_gate_open, "the gate must open: {report:?}");
        assert!(
            report.dropped_object_count > 0,
            "the round must drop something, or its cost is not the cost of a round: {report:?}"
        );

        let dropped = report.dropped_object_count as u64;
        let per_dropped = counts.encode_bytes_total as f64 / dropped as f64;
        println!(
            "dump={dump_before_evict:<5} keys={keys:<6} dropped={dropped:<4} encodes={:<3} \
             encode_bytes={:<9} bytes/dropped={per_dropped:.1}",
            counts.encodes_total, counts.encode_bytes_total
        );
        rows.push((
            keys,
            dropped,
            counts.encodes_total,
            counts.encode_bytes_total,
            per_dropped,
        ));
    }

    rows
}

#[test]
fn what_one_eviction_round_writes_grows_with_the_store_not_the_round() {
    let rows = sweep(true);
    let without_dump = sweep(false);

    // THE ATTRIBUTION, printed beside the figure it explains rather than left to inference.
    for ((keys, _, with_encodes, with_bytes, _), (_, _, no_encodes, no_bytes, _)) in
        rows.iter().zip(without_dump.iter())
    {
        println!(
            "keys={keys:<6} encodes with dump={with_encodes} without={no_encodes}   \
             bytes with dump={with_bytes} without={no_bytes}"
        );
    }

    // THE SHAPE, stated as an assertion rather than left to the reader of the printout: the bytes
    // one round writes are set by the store, so they grow when the store does while the round
    // itself does not.
    let (first_keys, _, _, first_bytes, _) = rows[0];
    let (last_keys, _, _, last_bytes, _) = rows[rows.len() - 1];
    assert!(
        last_keys > first_keys,
        "the sweep must actually widen: {first_keys} -> {last_keys}"
    );
    assert!(
        last_bytes > first_bytes,
        "one round's encoded bytes did not grow with the store ({first_bytes} -> {last_bytes}), \
         so the premise of this measurement is wrong and the reading needs revisiting rather \
         than reporting"
    );
    for (keys, dropped, encodes, bytes, per) in &rows {
        assert!(
            *encodes > 0 && *bytes > 0,
            "keys={keys}: a round that dropped {dropped} objects encoded {encodes} indexes and \
             {bytes} bytes, which cannot be right -- the counter is thread-local and the flush \
             runs on this thread, so zero means the flush did not happen"
        );
        assert!(
            per.is_finite(),
            "keys={keys}: the per-object figure is not finite"
        );
    }
}
