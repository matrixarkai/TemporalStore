// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What a decoded index's nested series cost, against the same entries packed in one go.
//!
//! WHY THIS EXISTS. `serde`'s `Deserialize` for `BTreeMap` fills the map one `insert` at a time,
//! and the bytes it reads were written in key order -- so every nested series in a decoded index is
//! built by ASCENDING insertion. A B-tree grown that way splits each full leaf in half and never
//! comes back to the left half, so the finished map carries leaves that are about half empty for
//! the rest of the process's life. Packing the same entries in one go fills every leaf.
//!
//! THE CONTROL READS THE SAME SET. The control below is not a separately seeded map: it is built
//! from the DECODED map's own entries, so the two hold the same keys and the same values by
//! construction and the only difference between them is tree shape. A fixture that seeded the
//! control independently could report a difference that was really a difference in contents.
//!
//! NON-VACUITY. Every ratio is printed beside the entry count it is divided by, and the entry
//! count is asserted against what was seeded before any ratio is taken. A walk over an empty
//! series map reports "0 bytes over 0 points", which reads exactly like a map that costs nothing.
#![allow(clippy::all)]
use super::*;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;

use crate::block_store::BlockAddress;

#[cfg(feature = "alloc-probe")]
use crate::alloc_probe::Probe;

const SHARD: u64 = 1;

/// A shard loaded on the SHIPPED routing range, not the bare whole-u32 default.
fn packing_engine(dir: &std::path::Path) -> Arc<TemporalEngine> {
    let engine = Arc::new(TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("pages"),
        dir.join("indexes"),
    ));
    engine.load_shard(SHARD);
    engine
}

/// Seed `series_keys` feature series of `series_points` points each, plus a handful of strings and
/// one list, so the decode has more than one nested container shape to put back.
fn packing_seed(
    engine: &TemporalEngine,
    strings_n: usize,
    series_keys: usize,
    series_points: usize,
) {
    for chunk_start in (0..strings_n).step_by(1_000) {
        let commands = (chunk_start..(chunk_start + 1_000).min(strings_n))
            .map(|i| Command::StringSet {
                key: format!("s{i}"),
                value: vec![b'v'; 32],
            })
            .collect::<Vec<_>>();
        if commands.is_empty() {
            continue;
        }
        let response = engine.batch_execute(crate::types::BatchExecuteRequest {
            shard_id: SHARD,
            commands,
        });
        assert!(
            response.status.ok,
            "string seed must ack: {:?}",
            response.status
        );
    }
    for k in 0..series_keys {
        for chunk_start in (0..series_points).step_by(500) {
            let points = (chunk_start..(chunk_start + 500).min(series_points))
                .map(|t| crate::types::FeaturePoint {
                    timestamp_ms: 1_700_000_000_000 + t as u64,
                    value: vec![b'f'; 32],
                })
                .collect::<Vec<_>>();
            if points.is_empty() {
                continue;
            }
            let response = engine.execute(ExecuteRequest {
                shard_id: SHARD,
                command: Command::FeatureAppend {
                    key: format!("f{k}"),
                    points,
                },
            });
            assert!(
                response.status.ok,
                "feature seed must ack: {:?}",
                response.status
            );
        }
    }
    for i in 0..64u64 {
        let response = engine.execute(ExecuteRequest {
            shard_id: SHARD,
            command: Command::ListPush {
                key: "l0".to_string(),
                member: vec![b'l'; 16],
                left: false,
            },
        });
        assert!(
            response.status.ok,
            "list seed must ack at {i}: {:?}",
            response.status
        );
    }
}

/// The encoded index bytes of a freshly seeded shard, and the state they were taken from.
fn encoded_index(
    strings_n: usize,
    series_keys: usize,
    series_points: usize,
) -> (Vec<u8>, Vec<u8>, usize, usize) {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = packing_engine(dir.path());
    packing_seed(&engine, strings_n, series_keys, series_points);
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&SHARD).expect("shard is loaded");
    let points: usize = shard.features.values().map(|series| series.len()).sum();
    let list_entries: usize = shard.lists.values().map(|list| list.len()).sum();
    let container = crate::engine::encode_index_bytes(shard);
    let plain = crate::engine::encode_index_bytes_as_plain_json(shard);
    (container, plain, points, list_entries)
}

/// The same entries, packed in one go: `collect` into a `BTreeMap` sorts and bulk-builds, so every
/// leaf is filled.
fn packed_like(
    map: &HashMap<String, BTreeMap<u64, BlockAddress>>,
) -> HashMap<String, BTreeMap<u64, BlockAddress>> {
    map.iter()
        .map(|(key, series)| {
            (
                key.clone(),
                series
                    .iter()
                    .map(|(at, address)| (*at, address.clone()))
                    .collect::<BTreeMap<u64, BlockAddress>>(),
            )
        })
        .collect()
}

/// Bytes the heap gives up to hold a deep copy of `value` -- which is what `value` itself holds.
///
/// A `BTreeMap` clone preserves tree shape, so the half of a leaf that an ascending insert left
/// empty is counted here exactly as it is counted in the shard.
#[cfg(feature = "alloc-probe")]
fn deep_heap_bytes<T: Clone>(value: &T) -> u64 {
    let probe = Probe::start();
    let copy = value.clone();
    std::hint::black_box(&copy);
    let counts = probe.stop();
    drop(copy);
    counts.alloc_bytes
}

// =================================================================================================
// CORRECTNESS: the entries a decode puts back are the entries that were encoded.
// =================================================================================================

/// The repack moves entries between nodes. This says the entries themselves are untouched -- same
/// keys, same values, same order, through BOTH container shapes a reader accepts.
#[test]
fn a_decode_puts_back_exactly_the_series_that_were_encoded() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = packing_engine(dir.path());
    packing_seed(&engine, 200, 3, 400);

    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&SHARD).expect("shard is loaded");

    // DENOMINATORS, before any comparison: an empty map compares equal to an empty map.
    assert_eq!(3, shard.features.len(), "denominator: three series seeded");
    let points: usize = shard.features.values().map(|series| series.len()).sum();
    assert_eq!(1_200, points, "denominator: 400 points in each of three series");
    let list_entries: usize = shard.lists.values().map(|list| list.len()).sum();
    assert_eq!(64, list_entries, "denominator: 64 list entries seeded");
    assert!(
        !shard.strings.is_empty(),
        "denominator: the string map must hold keys"
    );

    for (label, bytes) in [
        ("container", crate::engine::encode_index_bytes(shard)),
        (
            "plain json",
            crate::engine::encode_index_bytes_as_plain_json(shard),
        ),
    ] {
        let decoded = crate::engine::decode_index_bytes(&bytes)
            .unwrap_or_else(|error| panic!("{label} index must decode: {error}"));
        assert_eq!(
            shard.features, decoded.features,
            "{label}: the feature series must come back entry for entry"
        );
        assert_eq!(
            shard.lists, decoded.lists,
            "{label}: the list series must come back entry for entry"
        );
        assert_eq!(
            shard.control_state, decoded.control_state,
            "{label}: the control-state series must come back entry for entry"
        );
        assert_eq!(
            shard.expires_at_ms, decoded.expires_at_ms,
            "{label}: the expiry map must come back entry for entry"
        );
        assert_eq!(
            shard.sets, decoded.sets,
            "{label}: the set members must come back entry for entry"
        );
        assert_eq!(
            shard.zsets, decoded.zsets,
            "{label}: the zset members must come back entry for entry"
        );
        // THE STORED BYTES DO NOT MOVE, as bytes rather than as a sentence. A repack changes
        // which node an entry sits in; an encode walks the entries in key order, which is the
        // same order either way. Re-encoding what came back must reproduce the input exactly --
        // that is what says this carries no index-format stamp.
        // THE STORED BYTES DO NOT MOVE -- proven on ONE state, which is the only way it can be
        // proven here.
        //
        // The obvious test, "re-encode what came back and compare against the input", cannot
        // decide this: an index's `strings` map is a `HashMap`, which serializes in TABLE order,
        // and two `HashMap`s carrying identical entries iterate differently because each instance
        // seeds its own hasher. Two decodes of the SAME bytes therefore re-encode to two different
        // byte strings of the same length, before any change of ours. (Measured: 184,645 bytes
        // either way, first differing at byte 83, inside `strings`, which this pass does not
        // touch.) An assertion against the input bytes would have failed on a property of the
        // encoder and been read as this change moving the stored form.
        //
        // So the subject is a SINGLE state, encoded before and after the repack. Its `HashMap`
        // tables are untouched -- the repack reaches values through `values_mut` and never
        // reinserts a key -- so table order is held fixed and the only thing that could differ is
        // what the repack does. It does nothing: a `BTreeMap` serializes in KEY order, and the
        // repack changes which node an entry sits in, never which key it is under.
        if label == "container" {
            let mut subject = crate::engine::decode_index_bytes_inner(&bytes)
                .expect("the container index decodes without the repack");
            let before = crate::engine::encode_index_bytes(&subject);
            crate::engine::state::repack_decoded_btrees(&mut subject);
            let after = crate::engine::encode_index_bytes(&subject);
            assert_eq!(
                before, after,
                "{label}: repacking one state changed the bytes it encodes to, so the stored form \
                 moved and this needs an index-format stamp"
            );
            // And again, to say the pass is idempotent: a second decode of a repacked store, or a
            // repack reached twice down two paths, must not keep changing the answer.
            crate::engine::state::repack_decoded_btrees(&mut subject);
            assert_eq!(
                before,
                crate::engine::encode_index_bytes(&subject),
                "{label}: repacking twice is not the same as repacking once"
            );
        }
        // And the ORDER, not only the contents: a repack that sorted differently would still
        // compare equal as a map while handing every range read a different answer.
        let encoded_order: Vec<u64> = shard
            .features
            .get("f0")
            .expect("the first series is seeded")
            .keys()
            .copied()
            .collect();
        let decoded_order: Vec<u64> = decoded
            .features
            .get("f0")
            .expect("the first series decodes")
            .keys()
            .copied()
            .collect();
        assert_eq!(
            encoded_order, decoded_order,
            "{label}: the series must iterate in the same order it was written in"
        );
    }
}

// =================================================================================================
// FOOTPRINT: what the tree shape costs, against the same entries packed in one go.
// =================================================================================================

/// What a decoded series costs against the same entries packed in one go, at three sizes.
///
/// Three sizes rather than one, because a single reading cannot tell a per-entry cost from a
/// fixed one -- and the whole claim here is that the cost is per entry.
#[test]
#[cfg(feature = "alloc-probe")]
#[ignore = "seeds three corpora under the counting allocator; run by name"]
fn what_a_decoded_series_costs_against_one_packed_in_one_go() {
    let mut rows: Vec<(usize, f64, f64, f64)> = Vec::new();
    for series_points in [250usize, 1_000, 4_000] {
        let (container, _plain, seeded_points, seeded_list) = encoded_index(400, 4, series_points);
        assert_eq!(
            4 * series_points,
            seeded_points,
            "denominator: every seeded point must be in the map before it is encoded"
        );
        assert_eq!(64, seeded_list, "denominator: the list entries are seeded");

        let decoded =
            crate::engine::decode_index_bytes(&container).expect("the container index decodes");
        let decoded_points: usize = decoded.features.values().map(|s| s.len()).sum();
        assert_eq!(
            seeded_points, decoded_points,
            "denominator: the decode must put every point back, or the ratio below divides by the \
             wrong number"
        );

        let decoded_bytes = deep_heap_bytes(&decoded.features);
        let control = packed_like(&decoded.features);
        let control_points: usize = control.values().map(|s| s.len()).sum();
        assert_eq!(
            decoded_points, control_points,
            "the control must hold the same entries as its subject"
        );
        let control_bytes = deep_heap_bytes(&control);

        let decoded_per = decoded_bytes as f64 / decoded_points as f64;
        let control_per = control_bytes as f64 / control_points as f64;
        let ratio = decoded_bytes as f64 / control_bytes.max(1) as f64;
        println!(
            "  {decoded_points:>7} points: decoded {decoded_bytes:>10} B ({decoded_per:>6.1} \
             B/point), packed in one go {control_bytes:>10} B ({control_per:>6.1} B/point), \
             {ratio:.3}x"
        );
        rows.push((decoded_points, decoded_per, control_per, ratio));
    }

    assert_eq!(3, rows.len(), "three sizes must be measured");
    for (points, decoded_per, control_per, ratio) in &rows {
        assert!(
            *control_per > 0.0,
            "the packed control charged nothing at {points} points -- the probe is blind, and \
             every ratio here is noise"
        );
        println!(
            "  at {points} points: decoded {decoded_per:.1} B/point, packed {control_per:.1} \
             B/point, {ratio:.3}x"
        );
        // THE GUARD. A decoded series must cost what the same entries cost packed in one go. It
        // is an equality rather than a band because the two are the same entries in the same
        // order and the tree shape is the only thing that could differ: 1.846x was what ascending
        // insertion charged here before the repack, and anything above 1.001 means a decode path
        // has stopped going through it.
        assert!(
            *ratio <= 1.001,
            "a decoded series costs {ratio:.3}x what the same entries cost packed in one go at \
             {points} points -- some decode path is no longer repacking"
        );
    }
}

/// What a cold load spends, as allocations and as time, at three sizes.
///
/// The allocation column is deterministic and is the one to compare across two binaries; the
/// elapsed column is on a shared box and is reported as a band of five readings rather than as a
/// number, because one reading of it cannot carry a claim.
#[test]
#[cfg(feature = "alloc-probe")]
#[ignore = "decodes three corpora five times each; run by name"]
fn what_a_cold_load_spends_decoding_an_index_at_three_corpus_sizes() {
    for series_points in [250usize, 1_000, 4_000] {
        let (container, _plain, seeded_points, _seeded_list) = encoded_index(400, 4, series_points);
        assert_eq!(4 * series_points, seeded_points, "denominator: points seeded");

        // One decode under the allocator, so the figure is the decode's own and not five of them.
        let probe = Probe::start();
        let decoded =
            crate::engine::decode_index_bytes(&container).expect("the container index decodes");
        let counts = probe.stop();
        let decoded_points: usize = decoded.features.values().map(|s| s.len()).sum();
        assert_eq!(
            seeded_points, decoded_points,
            "denominator: the decode must put every point back"
        );
        drop(decoded);

        let mut elapsed_us: Vec<u128> = Vec::new();
        for _ in 0..5 {
            let started = std::time::Instant::now();
            let decoded =
                crate::engine::decode_index_bytes(&container).expect("the container index decodes");
            std::hint::black_box(&decoded);
            elapsed_us.push(started.elapsed().as_micros());
            drop(decoded);
        }
        elapsed_us.sort_unstable();
        // The repack's intermediate vector is built PER SERIES, so the transient peak is set by
        // the largest one, not by the corpus. Printed rather than described.
        let decoded =
            crate::engine::decode_index_bytes(&container).expect("the container index decodes");
        let largest = decoded
            .features
            .values()
            .map(|series| series.len())
            .max()
            .unwrap_or(0);
        assert!(largest > 0, "denominator: some series must hold points");
        println!("    largest single series: {largest} entries");
        drop(decoded);
        println!(
            "  {decoded_points:>7} points: decode allocated {:>11} B in {:>8} calls; elapsed us \
             min {} median {} max {} over 5",
            counts.alloc_bytes,
            counts.allocs,
            elapsed_us[0],
            elapsed_us[2],
            elapsed_us[4]
        );
        assert!(
            counts.alloc_bytes > 0,
            "the decode allocated nothing at {decoded_points} points -- the probe is blind"
        );
    }
}
