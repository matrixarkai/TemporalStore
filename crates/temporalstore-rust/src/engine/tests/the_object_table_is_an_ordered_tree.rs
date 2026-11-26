// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The per-kind object table is an ordered tree, so no insert pays for the size of the table.
//!
//! # WHY THIS EXISTS
//!
//! The table of one kind's objects (`bucket_index.objects[kind]`, one row per key) was a hash table.
//! A hash table doubles when it fills and a doubling rehashes every row at once, and the insert that
//! triggers it runs under the engine's write guard, so every writer waits for it. Measured on a
//! shard filled with 1M string keys: guard holds of 402 ms and 454 ms at exactly the lengths where
//! the table crosses 458,752 and 917,504 rows (7/8 of 2^19 and 2^20, the growth thresholds), none of
//! them explained by a log write, a lock wait or a sync; and the same table type, alone, costs
//! 182-824 ms and 343-2,152 ms for the inserts at those counts. The cost doubles with the table, so
//! it is invisible at 100k keys and large at millions. An ordered tree grows one node per insert:
//! 2-9 ms for the worst insert of the same 1.1M, whatever the size.
//!
//! So the table's type is a statement about latency, and nothing else in the suite would notice it
//! changing back: every row would still be found. This guard says so out loud.
//!
//! # WHAT IT PINS
//!
//!   1. no hashed spelling of the table type is left in the source (and the matcher is shown able to
//!      fail, so a green result is not an empty search);
//!   2. an image's bytes do not depend on the order the rows were inserted in. They used to, because
//!      a hash table iterates in an order the process picked, so two encodes of one content could
//!      differ; an ordered table encodes in key order;
//!   3. an image round-trips row for row, and the rows come back in key order;
//!   4. the table's serialized form is the same map in both representations, in BOTH directions: the
//!      rows an older build wrote from a hash table decode into the tree, and what the tree writes is
//!      read by a hash table. The wire carries a map, and a map has no order;
//!   5. a walk can resume from a key: `range(after..)` is how a maintenance pass can be cut into
//!      slices that release the guard between them.

use crate::block_store::ElementEntry;
use crate::engine::object_record::ObjectSlot;
use crate::engine::state::{ModelKey, ShardState};
use std::collections::HashMap;
use std::ops::Bound;
use std::sync::Arc;

/// The source of the module that owns the table type.
const SOURCE: &str = include_str!("../engine_storage_state.rs");

/// Every non-comment line that spells the table as a hash map, with its line number.
fn hashed_object_table_spellings(source: &str) -> Vec<(usize, String)> {
    source
        .lines()
        .enumerate()
        .filter(|(_, line)| !line.trim_start().starts_with("//"))
        .filter(|(_, line)| {
            line.contains("HashMap<ModelKey, super::object_record::ObjectSlot>")
                || line.contains("HashMap<ModelKey, ObjectSlot>")
        })
        .map(|(index, line)| (index + 1, line.trim().to_string()))
        .collect()
}

fn address(i: usize) -> ElementEntry {
    ElementEntry::from_parts(1, i as u64 * 64, 48, None, None)
}

fn key_of(i: usize) -> String {
    format!("k{i:06}")
}

/// A shard holding string rows `order` names, inserted in that order.
fn shard_with(order: impl Iterator<Item = usize>) -> ShardState {
    let mut shard = ShardState::default();
    shard.index_format_version = crate::engine::SHARD_INDEX_FORMAT_VERSION;
    for i in order {
        shard.set_string_address(Arc::from(key_of(i).as_str()), &address(i));
    }
    shard
}

/// rust-internal: reads the engine source, no product behaviour
#[test]
fn no_object_table_is_a_hash_map() {
    let found = hashed_object_table_spellings(SOURCE);
    assert!(
        found.is_empty(),
        "the per-kind object table must stay an ordered tree: a hash table doubles under the engine \
         write guard and stalls every writer for as long as the rehash takes (0.4 s at 0.5M-1M rows, \
         growing with the table). Hashed spellings found: {found:?}"
    );
    assert!(
        SOURCE.lines().any(|line| line
            .trim_start()
            .starts_with("pub(super) type ObjectTable = BTreeMap<ModelKey,")),
        "`ObjectTable` must be defined as a `BTreeMap` keyed by the model key"
    );
}

/// The matcher above is not vacuous: it finds the spelling it exists to refuse, and it ignores a
/// comment that names it.
///
/// rust-internal: exercises the guard's matcher, no product behaviour
#[test]
fn the_guard_against_a_hashed_table_can_fail() {
    let planted = "    ) -> &mut HashMap<ModelKey, super::object_record::ObjectSlot> {\n";
    assert_eq!(1, hashed_object_table_spellings(planted).len());
    let short = "    table: HashMap<ModelKey, ObjectSlot>,\n";
    assert_eq!(1, hashed_object_table_spellings(short).len());
    let comment = "    // was HashMap<ModelKey, ObjectSlot> before\n";
    assert!(hashed_object_table_spellings(comment).is_empty());
}

/// Two shards holding the same rows, inserted in opposite orders, write the same bytes.
///
/// rust-internal: reads the index image, no product behaviour
#[test]
fn an_images_bytes_do_not_depend_on_the_order_the_rows_were_inserted_in() {
    const N: usize = 2_000;
    let ascending = shard_with(0..N);
    let descending = shard_with((0..N).rev());
    assert_eq!(N, ascending.strings().len());
    assert_eq!(ascending.strings(), descending.strings(), "denominator: same rows");
    let a = crate::engine::encode_index_bytes(&ascending);
    let b = crate::engine::encode_index_bytes(&descending);
    assert_eq!(
        a, b,
        "the same content must encode to the same bytes whatever order it was built in"
    );
    // and encoding twice is the same
    assert_eq!(a, crate::engine::encode_index_bytes(&ascending));
}

/// An image decodes to the same rows, and the rows are in key order.
///
/// rust-internal: reads the index image, no product behaviour
#[test]
fn an_image_round_trips_row_for_row_in_key_order() {
    const N: usize = 1_500;
    let shard = shard_with((0..N).map(|i| (i * 7919) % N));
    let bytes = crate::engine::encode_index_bytes(&shard);
    let decoded = crate::engine::decode_index_bytes(&bytes).expect("the image decodes");
    assert_eq!(N, decoded.strings().len());
    assert_eq!(shard.strings(), decoded.strings(), "every row must come back unchanged");
    let keys: Vec<String> = decoded.strings().keys().map(|key| key.to_string()).collect();
    let mut sorted = keys.clone();
    sorted.sort();
    assert_eq!(sorted, keys, "the table iterates in key order");
    // and the re-encode of the decoded state is the original image
    assert_eq!(bytes, crate::engine::encode_index_bytes(&decoded));
}

/// The table's serialized form is a map, readable by either representation.
///
/// What an older build wrote came from a hash table, in whatever order the process's hasher chose;
/// what this build writes is in key order. Both are a map of the same entries, so each reads the
/// other.
///
/// rust-internal: reads the table's serde form, no product behaviour
#[test]
fn the_tables_serialized_form_is_a_map_that_a_hash_table_reads_and_writes() {
    const N: usize = 300;
    let shard = shard_with(0..N);
    let tree = shard.strings().clone();
    let hashed: HashMap<ModelKey, ObjectSlot> = tree
        .iter()
        .map(|(key, slot)| (Arc::clone(key), slot.clone()))
        .collect();
    assert_eq!(N, hashed.len());

    // an older build's table (a hash map) into the tree
    let from_hash = serde_json::to_vec(&hashed).expect("a hash table serializes");
    let into_tree: crate::engine::state::ObjectTable =
        serde_json::from_slice(&from_hash).expect("the tree reads a hash table's map");
    assert_eq!(tree, into_tree);

    // the tree into a hash table
    let from_tree = serde_json::to_vec(&tree).expect("the tree serializes");
    let into_hash: HashMap<ModelKey, ObjectSlot> =
        serde_json::from_slice(&from_tree).expect("a hash table reads the tree's map");
    assert_eq!(hashed, into_hash);

    // a table of ONE row writes the same bytes in both representations
    let one_tree: crate::engine::state::ObjectTable = tree.iter().take(1).map(|(k, v)| (Arc::clone(k), v.clone())).collect();
    let one_hash: HashMap<ModelKey, ObjectSlot> = one_tree.iter().map(|(k, v)| (Arc::clone(k), v.clone())).collect();
    assert_eq!(
        serde_json::to_vec(&one_tree).unwrap(),
        serde_json::to_vec(&one_hash).unwrap()
    );
}

/// A walk resumes from the key it stopped at: the property a sliced maintenance pass needs.
///
/// Cut into slices of 100 rows, each slice taken from the first key after the last one handled,
/// the walk visits every row once, in order, and a row inserted behind the cursor between slices is
/// not seen twice.
///
/// rust-internal: exercises the table's ordered cursor, no product behaviour
#[test]
fn a_walk_resumes_from_the_key_it_stopped_at() {
    const N: usize = 1_000;
    let mut shard = shard_with((0..N).map(|i| i * 2));
    let mut seen: Vec<String> = Vec::new();
    let mut after: Option<String> = None;
    loop {
        let lower = match &after {
            Some(key) => Bound::Excluded(key.as_str()),
            None => Bound::Unbounded,
        };
        let slice: Vec<String> = shard
            .strings()
            .range::<str, _>((lower, Bound::Unbounded))
            .take(100)
            .map(|(key, _)| key.to_string())
            .collect();
        if slice.is_empty() {
            break;
        }
        after = slice.last().cloned();
        seen.extend(slice);
        // a write between slices, BEHIND the cursor: it must not be revisited
        if seen.len() == 300 {
            shard.set_string_address(Arc::from("k000001"), &address(1));
        }
    }
    let mut expected: Vec<String> = (0..N).map(|i| key_of(i * 2)).collect();
    expected.sort();
    assert_eq!(expected, seen, "every original row exactly once, in order");
    assert_eq!(N + 1, shard.strings().len(), "the row written between slices is in the table");
}
