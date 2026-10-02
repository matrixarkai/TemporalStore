// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE TIE BETWEEN THE TWO LISTS OF MODEL SPELLINGS, AND WHAT THE MISSING ONES COST.
//!
//! `index_log::MODEL_ID_NUMBERS` decides which spellings a delta row may write as a NUMBER rather
//! than as a string. `model_kind_registry!` decides which spellings a stored entry may hold at
//! all. They are two hand-maintained lists of the same vocabulary, and the registry already states
//! the hazard about itself: it derives its retired half as the complement of its live half "so it
//! cannot disagree with the declaration".
//!
//! The number list had no such tie, and it did go stale. `"zset"` and `"list"` were added to the
//! registry as LIVE kinds -- taking codes 6 and 12, the two the old packed report had skipped --
//! and nobody added them to the number list, so every index-log row naming either kind wrote its
//! full string spelling instead of one byte. Nothing failed. That is what a second list with no
//! tie costs, and these tests are the tie.
//!
//! WHY THIS LIVES IN `engine` AND NOT BESIDE THE LIST IT GUARDS. `StoredModelKind` is
//! `pub(super)` inside `engine`, and `index_log` sits BELOW `engine` and cannot see it. The guard
//! has to be where both lists are visible, and that is here.
//!
//! AND WHY THE TWO DIRECTIONS ARE TWO TESTS. "Every live spelling can be numbered" and "every
//! numbered spelling is declared" are different failures with different costs -- the first is
//! bytes, the second is a number that decodes to a kind no entry can hold -- and one assertion
//! covering both would report whichever it noticed first.

use crate::engine::storage_bucket_internals::{ModelKind, StoredModelKind};
use crate::index_log::MODEL_ID_NUMBERS;

/// EVERY LIVE SPELLING THE REGISTRY DECLARES CAN BE WRITTEN AS A NUMBER.
///
/// This is the defect `"zset"` and `"list"` were: live kinds the engine writes on ordinary paths,
/// absent from the number list, paying a string on every row. A kind added to the registry and not
/// to the number list fails here rather than quietly costing bytes.
///
/// rust-internal: reads two of the engine's own declarations, no product behaviour
#[test]
fn every_live_model_spelling_can_be_written_as_a_number() {
    let numbered: std::collections::BTreeSet<&str> = MODEL_ID_NUMBERS.iter().copied().collect();
    assert_eq!(
        MODEL_ID_NUMBERS.len(),
        numbered.len(),
        "the number list holds a duplicate spelling, so one position is unreachable: {MODEL_ID_NUMBERS:?}",
    );

    // THE DENOMINATOR, PRINTED. A sweep over an empty set passes while asserting nothing.
    let live: Vec<&'static str> = ModelKind::ALL
        .iter()
        .map(|kind| StoredModelKind::from(*kind).as_str())
        .collect();
    assert!(
        live.len() >= 15,
        "the registry's live half reports {} kinds, too few to be it",
        live.len(),
    );

    let missing: Vec<&str> = live
        .iter()
        .copied()
        .filter(|name| !numbered.contains(name))
        .collect();
    println!(
        "{} live spellings checked against {} numbered",
        live.len(),
        MODEL_ID_NUMBERS.len()
    );
    assert!(
        missing.is_empty(),
        "{} LIVE spelling(s) cannot be written as a number and pay a full string on every row: \
         {missing:?}. Append them to `MODEL_ID_NUMBERS` -- at the END, because the position IS \
         the wire value and inserting renumbers every row already written.",
        missing.len(),
    );
}

/// AND EVERY SPELLING THE NUMBER LIST NAMES IS ONE THE REGISTRY DECLARES, LIVE OR RETIRED.
///
/// A number list naming a spelling the registry has never heard of encodes a row that decodes back
/// to a string no entry can hold -- a number that round-trips into nothing. Retired names are
/// legitimate here: a row written before a retirement still carries one, which is why the registry
/// keeps them rather than deleting them.
///
/// rust-internal: reads two of the engine's own declarations, no product behaviour
#[test]
fn every_numbered_model_spelling_is_one_the_registry_declares() {
    assert!(
        StoredModelKind::ALL.len() >= 17,
        "the registry declares {} spellings across both halves, too few to be it",
        StoredModelKind::ALL.len(),
    );
    assert!(
        !MODEL_ID_NUMBERS.is_empty(),
        "an empty number list would make this sweep vacuous",
    );

    let undeclared: Vec<&str> = MODEL_ID_NUMBERS
        .iter()
        .copied()
        .filter(|name| StoredModelKind::from_stored_name(name).is_none())
        .collect();
    assert!(
        undeclared.is_empty(),
        "the number list names {} spelling(s) the registry does not declare: {undeclared:?}. A \
         number that decodes to a spelling no entry can hold is worse than a string.",
        undeclared.len(),
    );

    // The retired half IS allowed here, and this says so rather than leaving it to be inferred
    // from the sweep passing: a row written before a retirement still names one.
    let retired_and_numbered: Vec<&str> = MODEL_ID_NUMBERS
        .iter()
        .copied()
        .filter(|name| {
            StoredModelKind::from_stored_name(name)
                .map(StoredModelKind::is_retired)
                .unwrap_or(false)
        })
        .collect();
    assert!(
        !retired_and_numbered.is_empty(),
        "no retired spelling is numbered, so this test no longer demonstrates that retired names \
         are permitted here -- which was the thing it was written to pin",
    );
    println!("retired spellings that are numbered, legitimately: {retired_and_numbered:?}");
}

/// WHAT THE TWO MISSING SPELLINGS COST, AS BYTES OFF THE ENCODER.
///
/// A correctness claim with no number attached reads as a tidy-up. This prices the model slot
/// through the adapter the row actually writes it with, which is also the control: a spelling the
/// list does not name falls through to `serialize_str`, and the difference between those two paths
/// is the whole saving.
///
/// rust-internal: encodes one slot, no product behaviour
#[test]
fn a_numbered_model_spelling_costs_one_byte_where_a_string_costs_five() {
    let slot = crate::index_log::model_id_slot_bytes;

    // The control: a spelling NO list names falls through to the string path. Four characters, so
    // it is the same length as `zset` and `list` and the comparison is like-for-like.
    let unnamed = slot("qqqq");
    let string_spelling = slot("string");
    let zset = slot("zset");
    let list = slot("list");

    println!("=== the model slot, by spelling ===");
    println!("  a spelling no list names  {unnamed} B  (falls through to a string)");
    println!("  string                    {string_spelling} B");
    println!("  zset                      {zset} B");
    println!("  list                      {list} B");
    println!("  saving per row, each      {} B", unnamed - zset);

    assert_eq!(
        5, unnamed,
        "a four-character spelling written as a string is one header byte plus four; this priced \
         at {unnamed}, so it is not going through the adapter the row uses",
    );
    assert_eq!(
        1, string_spelling,
        "`string` has always been numbered and must cost one byte, not {string_spelling}",
    );
    assert_eq!(1, zset, "`zset` is numbered now and must cost one byte, not {zset}");
    assert_eq!(1, list, "`list` is numbered now and must cost one byte, not {list}");
    assert_eq!(
        4,
        unnamed - zset,
        "the saving for each of the two spellings is supposed to be four bytes a row",
    );
}
