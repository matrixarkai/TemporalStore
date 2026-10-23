// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! HOW MANY KEYS ONE CACHED FRAME SERVES, which is what prices the payload cache's 2.0x.
//!
//! # THE CLAIM THIS CHECKS IS MY OWN, AND IT WAS UNVERIFIED
//!
//! `the_payload_cache_per_user_byte` established that a point read populates TWO cache layers and
//! both hold the payload — the command response under `CacheKey::string(shard, key)` and the block
//! frame under `CacheKey::page_with_slot(shard, slab, offset, length, bucket)` — at exactly 2.00
//! fills per key with zero evictions, costing `2 * value + 21` bytes an entry.
//!
//! Its doc then says the duplication "is not a defect" because "the frame entry earns its place on
//! multi-element blocks, where one frame answers several keys". **That was asserted, not measured.**
//! If blocks hold one element each in practice, the frame entry answers exactly one key. The
//! inference then drawn from that -- that it therefore duplicates the response entry entirely and
//! the 2.0x is waste -- is the one the next section shows does NOT hold.
//!
//! # WHAT THIS ARM DOES NOT ANSWER, AND WHAT WAS WRONGLY READ OFF IT
//!
//! It answers *do two KEYS share a frame*. It does **not** answer *can anything else reach this
//! frame*, and the conclusion drawn from it originally — that the frame entry duplicates the
//! response entry entirely and the 2.0x is waste — used it for the second question.
//!
//! The frame key is read by the 23 callers of `read_block_bytes` and the two of
//! `read_block_frame_bytes`, and it is the key of the persistent block cache that six `part2` arms
//! assert refills a tiny memory cache without a block-store read. One key per frame, 25 call paths
//! per key. Removing the frame put on the string read path broke all six of those arms, which is
//! how the overreach was caught.
//!
//! So read the number below as what it is: a statement about packing, which bears on whether a
//! frame can AMORTISE across keys — not on whether the entry has readers.
//!
//! # ANSWERABLE FROM THE INDEX ALONE
//!
//! Two keys share a frame if and only if they share `(slab, offset, length)` — those are the
//! components of the frame's cache key that come from the block. So grouping the live object rows by
//! their address triple and counting the keys per group answers it, with no cache internals
//! involved.
//!
//! # ACROSS VALUE SIZES, BECAUSE THE ANSWER MAY DEPEND ON THEM
//!
//! Small values may pack several to a block, in which case the frame cache earns its place for them
//! and my claim holds in a REGIME rather than universally. That is a more useful answer than either
//! "always" or "never", and it is the shape a reader can act on.
//!
//! Values are incompressible: a run of one byte compresses to nothing and changes how much fits in a
//! block, which is the variable under test.
//!
//! rust-internal: measures this crate's own index, no product behaviour
//!
//!   cargo test -p temporalstore-rust --lib how_many_keys_a_cached_frame_serves -- --ignored --nocapture

use std::collections::BTreeMap;

use crate::types::{Command, ExecuteRequest};

const KEYS: usize = 500;

fn noisy_value(index: usize, len: usize) -> Vec<u8> {
    let mut state = (index as u64)
        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .wrapping_add(0x1234_5678_9ABC_DEF0)
        | 1;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state & 0xFF) as u8
        })
        .collect()
}

/// (value size, distinct frames, keys, largest group, groups serving more than one key)
fn measure(value: usize) -> (usize, usize, usize, usize, usize) {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = crate::TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    for index in 0..KEYS {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("frame-{index:06}"),
                value: noisy_value(index, value),
            },
        });
        assert!(response.status.ok, "write {index}: {:?}", response.status);
    }
    // READ EVERY KEY BACK, so both cache layers are populated on the path this is about.
    for index in 0..KEYS {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet { key: format!("frame-{index:06}") },
        });
        assert!(response.status.ok, "read {index}: {:?}", response.status);
    }

    // GROUP BY THE FRAME'S OWN IDENTITY. `(slab, offset, length)` are the block-derived components
    // of `CacheKey::page_with_slot`, so two rows in one group are two keys one frame would serve.
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    let mut groups: BTreeMap<(u64, u64, u64), usize> = BTreeMap::new();
    let mut rows = 0_usize;
    for (_, slot) in shard.strings().iter() {
        let address = slot.address();
        *groups
            .entry((address.block_slab_id(), address.offset(), address.length()))
            .or_default() += 1;
        rows += 1;
    }
    let largest = groups.values().copied().max().unwrap_or(0);
    let shared = groups.values().filter(|count| **count > 1).count();
    (value, groups.len(), rows, largest, shared)
}

#[test]
#[ignore = "seeds four stores and walks their indexes; run by name"]
fn how_many_keys_a_cached_frame_serves() {
    let readings: Vec<(usize, usize, usize, usize, usize)> =
        [64_usize, 256, 1_024, 4_096].into_iter().map(measure).collect();

    println!();
    println!("  HOW MANY KEYS ONE FRAME SERVES -- {KEYS} keys, incompressible");
    println!();
    println!(
        "    {:>6} {:>8} {:>8} {:>10} {:>8} {:>12}",
        "value", "frames", "keys", "keys/frame", "largest", "shared>1"
    );
    for (value, frames, rows, largest, shared) in &readings {
        println!(
            "    {:>6} {:>8} {:>8} {:>10.3} {:>8} {:>12}",
            value,
            frames,
            rows,
            *rows as f64 / (*frames).max(1) as f64,
            largest,
            shared
        );
    }
    println!();

    // ---- NON-VACUITY FIRST. A walk that found no rows would make every group count meaningless.
    for (value, frames, rows, _, _) in &readings {
        assert!(
            *rows > 0,
            "value {value}: the index holds no string rows, so the grouping below is empty and \
             this arm measures nothing"
        );
        assert!(*frames > 0, "value {value}: no frames at all");
        assert_eq!(
            *rows, KEYS,
            "value {value}: the index holds {rows} rows for {KEYS} keys. The grouping prices one \
             frame per key, so a row count that disagrees with the key count means some keys are \
             missing and the ratio is over the wrong population"
        );
    }

    // ---- THE FINDING, WHICHEVER WAY IT GOES, is stated as the arm's own conclusion rather than
    // ---- left to the printout. The claim under test is that frames serve several keys; what gets
    // ---- asserted is the relation the numbers support, so a change in packing makes this speak.
    let all_single = readings.iter().all(|(_, frames, rows, _, _)| frames == rows);
    let any_shared = readings.iter().any(|(_, _, _, _, shared)| *shared > 0);
    println!(
        "    every frame serves exactly one key at every value size: {all_single}"
    );
    println!("    any frame serves more than one key: {any_shared}");
    println!();
    if all_single {
        println!(
            "    SO A FRAME NEVER AMORTISES ACROSS KEYS on this workload: the claim that it 'earns"
        );
        println!(
            "    its place on multi-element blocks' does not apply at any value size measured here."
        );
        println!();
        println!(
            "    THIS DOES NOT MAKE THE FRAME ENTRY THE SPARE COPY, and reading it that way was an"
        );
        println!(
            "    error: the frame key is read by 25 call paths and is the key of the persistent"
        );
        println!(
            "    block cache. One key per frame is not the same as one reader per frame."
        );
    } else {
        println!(
            "    SO THE FRAME ENTRY DOES EARN ITS PLACE in at least one regime, and the 2.0x is a"
        );
        println!("    trade whose price depends on the value size.");
    }
    println!();

    // Exactly one of the two must hold, and they are computed from the same groups, so this
    // catches a grouping that silently produced neither shape.
    assert!(
        all_single != any_shared,
        "the groups report all-single={all_single} and any-shared={any_shared}, which cannot both \
         be true or both be false -- the grouping is not partitioning the rows"
    );
}
