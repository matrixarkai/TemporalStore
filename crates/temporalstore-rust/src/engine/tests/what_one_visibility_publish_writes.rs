// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What one visibility publish writes, against what it published.
//!
//! WHY THIS EXISTS. `publish_shard_index_snapshot_for_keys` ends by encoding the WHOLE served
//! index out of the shard and writing it through `persist_index_bytes`, however few keys the call
//! named -- its own comment calls that "the O(n^2) rewrite". The call takes a key list and
//! republishes only those keys' pages, so the PAGE half of the work is already O(published). This
//! measures the other half: the encode, swept over the size of the store with the published set
//! held at one key.
//!
//! THE DENOMINATOR IS KEYS PUBLISHED, and the sweep varies the store underneath it. If the encoded
//! bytes track the store while the published set does not move, the cost of a publish is set by
//! the size of the STORE and not by the size of the publish.
//!
//! THE REGIME IS ASSERTED, NOT HOPED FOR. A publish that found nothing to republish returns zero
//! and writes nothing, and would report a flattering zero here; every arm asserts the seed landed
//! and asserts the publish reported bytes before any ratio is taken.
//!
//! THE COUNTERS ARE THREAD-LOCAL. `index_encode_counts` tallies per thread and the encode happens
//! on the calling thread, so a single-threaded arm sees all of it and only it.

#![allow(clippy::all)]
use super::*;

const SHARD: u64 = 29;

/// Seed `keys` string records THROUGH THE ASYNC-STORAGE ARM, which is the only write path that
/// mints a log-resident block address -- `append_value_inner` writes straight to the block store
/// unless `async_storage` is set. A publish's whole job is to move those blocks out, so a fixture
/// seeded through the synchronous arm leaves it nothing to republish and it reports zero.
///
/// This is also the regime the production caller runs in: the proxy's `matrixark_publish_visibility`
/// exists for stores opened with `MATRIXARK_RUST_PROXY_ASYNC_STORAGE`.
fn seed(engine: &TemporalEngine, keys: usize) {
    for item in 0..keys {
        let key = format!("publish-{item:06}");
        let response = engine.execute_replicated(
            ExecuteRequest {
                shard_id: SHARD,
                command: Command::StringSet {
                    key,
                    value: vec![b'v'; 128],
                },
            }
            .with_async_storage(),
        );
        assert!(response.status.ok, "seed must ack at {item}: {response:?}");
    }
}

/// One reading: the store size, how many keys the publish named, and what the publish encoded.
struct PublishCost {
    keys: usize,
    named: usize,
    encodes: u64,
    encode_bytes: u64,
    reported_bytes: usize,
}

/// Publish `named` keys out of a store of `keys`, and read what the publish encoded.
fn publish_cost(keys: usize, named: usize) -> PublishCost {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = TemporalEngine::with_local_dirs(
        64,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(SHARD);
    seed(&engine, keys);

    // THE DENOMINATOR, ASSERTED BEFORE THE RATIO. An empty store publishes nothing and divides by
    // nothing.
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

    // THE MOST RECENTLY WRITTEN KEYS, because those are the ones still log-resident after the
    // resident-set bound has swept the older ones out. Naming an already-materialized key would
    // give a publish with nothing to republish, which is the flattering zero this refuses.
    let selected: Vec<String> = ((keys - named)..keys)
        .map(|item| format!("publish-{item:06}"))
        .collect();
    assert_eq!(named, selected.len(), "the named set must be the named set");

    crate::engine::shard_write_guard::reset_index_encode_counts();
    let reported_bytes = engine
        .publish_shard_index_snapshot_for_keys(SHARD, selected)
        .expect("publish must succeed");
    let counts = crate::engine::shard_write_guard::index_encode_counts();

    assert!(
        reported_bytes > 0,
        "the publish reported 0 bytes at keys={keys} named={named}, so it found nothing \
         log-resident to republish and its cost is not the cost of a publish"
    );

    println!(
        "keys={keys:<6} named={named:<6} encodes={:<3} encode_bytes={:<9} \
         bytes/named={:<10.1} reported={reported_bytes}",
        counts.encodes_total,
        counts.encode_bytes_total,
        counts.encode_bytes_total as f64 / named as f64
    );
    PublishCost {
        keys,
        named,
        encodes: counts.encodes_total,
        encode_bytes: counts.encode_bytes_total,
        reported_bytes,
    }
}

/// ONE PUBLISH OF ONE KEY COSTS THE WHOLE STORE.
#[test]
fn what_one_visibility_publish_writes_grows_with_the_store_not_the_publish() {
    println!();
    println!("--- one key named, store swept ---");
    let one_key: Vec<PublishCost> = [500usize, 2_000, 8_000]
        .into_iter()
        .map(|keys| publish_cost(keys, 1))
        .collect();

    // THE OTHER DIRECTION OF THE SAME TABLE: hold the store and widen the publish. If the encode
    // is the store's and not the publish's, this column barely moves.
    println!("--- store held at 2,000, named set swept ---");
    let widening: Vec<PublishCost> = [1usize, 16, 256]
        .into_iter()
        .map(|named| publish_cost(2_000, named))
        .collect();

    for row in one_key.iter().chain(widening.iter()) {
        assert!(
            row.encodes > 0 && row.encode_bytes > 0,
            "keys={} named={}: a publish that reported {} bytes encoded {} indexes and {} bytes, \
             which cannot be right -- the counter is thread-local and the encode runs on this \
             thread, so zero means the encode did not happen",
            row.keys,
            row.named,
            row.reported_bytes,
            row.encodes,
            row.encode_bytes
        );
        assert_eq!(
            1, row.encodes,
            "keys={} named={}: a publish encoded the whole index {} times, so this is no longer \
             the one-encode shape this measurement reads",
            row.keys, row.named, row.encodes
        );
    }

    // THE SHAPE, ASSERTED. The encoded bytes grow with the store while the publish does not.
    let first = &one_key[0];
    let last = &one_key[one_key.len() - 1];
    assert!(
        last.keys > first.keys,
        "the sweep must actually widen: {} -> {}",
        first.keys,
        last.keys
    );
    assert_eq!(
        first.named, last.named,
        "the published set must be held still across the sweep, or the growth below is the \
         publish's and not the store's"
    );
    assert!(
        last.encode_bytes > first.encode_bytes * 4,
        "one publish of {} key encoded {} bytes on a {}-key store and {} on a {}-key store; a \
         16x store that does not multiply the encode means the premise of this measurement is \
         wrong and the reading needs revisiting rather than reporting",
        first.named,
        first.encode_bytes,
        first.keys,
        last.encode_bytes,
        last.keys
    );

    // AND THE CONVERSE, which is what makes the first claim an attribution rather than a
    // coincidence: widening the publish 256-fold on a fixed store does not widen the encode.
    let narrow = &widening[0];
    let wide = &widening[widening.len() - 1];
    assert!(
        wide.named > narrow.named * 16,
        "the named sweep must actually widen: {} -> {}",
        narrow.named,
        wide.named
    );
    assert!(
        wide.encode_bytes < narrow.encode_bytes * 2,
        "publishing {} keys encoded {} bytes against {} for {} key on the same store, so the \
         encode is tracking the publish after all",
        wide.named,
        wide.encode_bytes,
        narrow.encode_bytes,
        narrow.named
    );
}
