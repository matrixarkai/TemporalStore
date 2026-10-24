// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What one shard LOAD writes, and why the whole-index encode inside it is not the same defect.
//!
//! WHY THIS EXISTS. `replay_wal_into_shard_windowed` ends by encoding the whole served index and
//! writing it (`lifecycle.rs`' `let _ = self.persist_index_bytes(shard_id, &index_bytes)`), which
//! is the same whole-store encode that a per-operation site is charged for. It is NOT the same
//! shape, and the difference is a count rather than an argument: this one happens ONCE PER LOAD,
//! inside a lock-held block that has just run `promote_model_maps_to_bucket_index_authority` and
//! `rebuild_bucket_first_index` over the whole shard, in an operation that decoded the whole index
//! to begin with. An interval in front of it would never fire differently -- a countdown that is
//! serde-skipped reads as DUE on every fresh load, and there is only ever one of these per load --
//! so putting one there would change nothing and cost a knob.
//!
//! IT IS PRODUCTION-REACHABLE, which is the claim that had to be established before it could be
//! left alone: the arm below reaches it on an ordinary reopen of an ordinary store, with no test
//! hook and no `#[cfg(test)]` setter in the path.
//!
//! THE ATTRIBUTION IS A CONTROL, NOT A READING OF THE CALL GRAPH. The site is reached only when
//! `replayed_through > watermark` -- a WAL tail beyond what the persisted index already covers. A
//! store flushed with `flush_shard_index()` before it closed has no such tail, so the second arm
//! is the same load with the condition false, and the difference between the two arms is this
//! site.
//!
//! THE COUNTERS ARE THREAD-LOCAL and the load runs on the calling thread.

#![allow(clippy::all)]
use super::*;

const SHARD: u64 = 37;

/// Seed through the async-storage arm, the same regime the publish measurement uses, so the
//// records are in the log and the base index on disk is behind them.
fn seed(engine: &TemporalEngine, keys: usize) {
    for item in 0..keys {
        let response = engine.execute_replicated(
            ExecuteRequest {
                shard_id: SHARD,
                command: Command::StringSet {
                    key: format!("loaded-{item:06}"),
                    value: vec![b'v'; 128],
                },
            }
            .with_async_storage(),
        );
        assert!(response.status.ok, "seed must ack at {item}: {response:?}");
    }
}

fn engine_over(dir: &std::path::Path) -> TemporalEngine {
    TemporalEngine::with_local_dirs(
        64,
        dir.join("cache"),
        dir.join("pages"),
        dir.join("indexes"),
    )
}

/// One reading: what the LOAD of a store of `keys` encoded.
struct LoadCost {
    keys: usize,
    encodes: u64,
    encode_bytes: u64,
    served: usize,
}

/// Seed a store, close it, and measure what reopening it encodes.
///
/// `flush_before_close` is the control: it persists the base index and advances the watermark, so
/// the reopen has no WAL tail beyond it and the replay fold does not run.
fn load_cost(keys: usize, flush_before_close: bool) -> LoadCost {
    let dir = tempfile::tempdir().expect("tempdir");
    {
        let engine = engine_over(dir.path());
        engine.load_shard(SHARD);
        seed(&engine, keys);
        let seeded = engine
            .shards
            .read()
            .expect("engine lock poisoned")
            .get(&SHARD)
            .expect("shard is loaded")
            .strings()
            .len();
        assert_eq!(
            keys, seeded,
            "the seed must have landed, or nothing below means anything"
        );
        if flush_before_close {
            engine.flush_shard_index(SHARD);
        }
        // The engine drops here, so the reopen below has to rebuild from the disk.
    }

    // THE RESET IS AFTER THE CONSTRUCTION AND BEFORE THE LOAD, so the figure is the load's and
    // not the engine's.
    let engine = engine_over(dir.path());
    crate::engine::shard_write_guard::reset_index_encode_counts();
    engine.load_shard(SHARD);
    let counts = crate::engine::shard_write_guard::index_encode_counts();

    // THE REGIME. A load that served nothing did not load anything, and would report a
    // flattering zero.
    let served = engine
        .shards
        .read()
        .expect("engine lock poisoned")
        .get(&SHARD)
        .expect("shard is loaded after a reopen")
        .strings()
        .len();
    assert_eq!(
        keys, served,
        "the reopened engine serves {served} of {keys} key(s), so this is a load that lost rows \
         rather than a load whose cost is being measured"
    );

    println!(
        "flushed_before_close={flush_before_close:<5} keys={keys:<6} encodes={:<3} \
         encode_bytes={:<9} served={served}",
        counts.encodes_total, counts.encode_bytes_total
    );
    LoadCost {
        keys,
        encodes: counts.encodes_total,
        encode_bytes: counts.encode_bytes_total,
        served,
    }
}

/// THE REPLAY FOLD'S CHECKPOINT IS ONE ENCODE PER LOAD, AND IT DOES NOT MULTIPLY.
#[test]
fn the_replay_fold_checkpoint_is_one_whole_index_encode_per_load() {
    println!();
    println!("--- reopened with a WAL tail beyond the persisted index ---");
    let stale: Vec<LoadCost> = [500usize, 2_000]
        .into_iter()
        .map(|keys| load_cost(keys, false))
        .collect();
    println!("--- the control: flushed before close, so there is no tail to fold ---");
    let flushed: Vec<LoadCost> = [500usize, 2_000]
        .into_iter()
        .map(|keys| load_cost(keys, true))
        .collect();

    for row in stale.iter() {
        assert!(
            row.encodes > 0,
            "keys={}: a load that served {} key(s) off a stale base index encoded nothing, so \
             the replay fold's checkpoint did not run and this measurement is not reading it",
            row.keys,
            row.served
        );
        // THE WHOLE POINT. The COUNT does not grow with the store: the bytes do, because it is a
        // whole-index encode, but there is exactly ONE of them however large the shard is and
        // however long the replayed tail is. That is what makes it a per-LOAD cost inside an
        // already-O(store) load rather than a per-operation cost charged to the store.
        assert_eq!(
            1, row.encodes,
            "keys={}: the load encoded the whole index {} times, so it is no longer the \
             one-encode-per-load shape that is the reason this site is left alone",
            row.keys, row.encodes
        );
    }

    // THE ATTRIBUTION. With the base index current there is no tail beyond it, so the fold does
    // not run and the encode is not paid -- which is what says the encode above belongs to the
    // fold and not to the rest of the load.
    for row in flushed.iter() {
        assert_eq!(
            0, row.encodes,
            "keys={}: a load with no WAL tail beyond the persisted index still encoded the whole \
             index {} time(s) for {} bytes, so the encode measured above is not the replay fold's \
             and the attribution is wrong",
            row.keys, row.encodes, row.encode_bytes
        );
    }

    // AND THE BYTES DO GROW, which is why the site looks like the per-operation defect until the
    // count is read beside them.
    assert!(
        stale[1].encode_bytes > stale[0].encode_bytes,
        "the one encode per load is {} bytes at {} keys and {} at {}, so it is not the \
         whole-index encode this is about",
        stale[0].encode_bytes,
        stale[0].keys,
        stale[1].encode_bytes,
        stale[1].keys
    );
}
