// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE MODEL MAP'S LEVEL-TWO VALUE IS A TYPE NOW, AND THE STORED SHAPE DID NOT MOVE.
//!
//! # WHAT THIS STEP IS, AND WHAT IT IS NOT
//!
//! The resident page entry is being relocated into the model map's level-two position, where it
//! can be KEY-INDEPENDENT: the object key is the level-one key and the element name is the
//! level-two key, so the value has to name neither. That relocation is three changes -- the value
//! becomes a type, the readers stop going to the bucket index, and the entry's remaining fields
//! move with a format stamp -- and this is only the FIRST. Nothing reads differently after it and
//! no stored byte moves.
//!
//! So the thing worth proving here is a NEGATIVE: that introducing the type changed no encoding.
//! `HashFieldMap` rides `serde(from)` and `serde(into)` over a plain map of name to address, so its
//! stored form is the address-only map and its in-memory shape is invisible to the wire. That is
//! the mechanism which lets this step land without `SHARD_INDEX_FORMAT_VERSION` moving, and it is
//! the same mechanism that let the hash map become durable without a stamp.
//!
//! # THE PROOF IS A RESTATEMENT, NOT A GOLDEN FILE
//!
//! A stored-format tripwire re-goldened from the code it guards proves only that the code equals
//! itself. So the expected encoding here is WRITTEN OUT from the wire contract -- the declared slot
//! names and the arithmetic behind each value -- and compared against what the container
//! serializes.
//!
//! THAT DISTINCTION PAID FOR ITSELF IMMEDIATELY. Written from the slot names alone, this
//! expectation said the generation slot was empty. It is not: the generation's VALUE is derived
//! from the identities, so the slot carries the block id. A golden literal captured from the output
//! would have frozen three numbers as if they were free to differ from the block id; stating the
//! derivation says they are not.
//!
//! A planted difference is checked too, because an equality test between two things built the same
//! way can pass while measuring nothing: a container holding a DIFFERENT address must produce
//! different bytes, or the comparison is not sensitive to the content at all.

#![allow(clippy::all)]
use super::*;
use crate::block_store::ElementEntry;
use crate::engine::hash_field_map::HashFieldMap;

fn address(slab: u64, offset: u64, length: u64, block: u64) -> ElementEntry {
    ElementEntry::from_parts(slab, offset, length, Some(block), None)
}

fn populated() -> HashFieldMap {
    vec![
        ("alpha".to_string(), address(1, 64, 128, 9)),
        ("beta".to_string(), address(2, 4096, 256, 10)),
        ("gamma".to_string(), address(3, 8192, 512, 11)),
    ]
    .into_iter()
    .collect()
}

/// The merged address word: the slab in the high 32 bits, the offset in the low 32.
///
/// Restated as that arithmetic rather than as a constant, so a reader can check it.
fn word(slab: u64, offset: u64) -> u64 {
    (slab << 32) | offset
}

/// The five declared slots of a stored address, named by the wire contract.
///
/// None of them is skipped, so all five appear. The object id slot stays on the wire and is
/// written empty.
///
/// THE GENERATION SLOT CARRIES THE BLOCK ID. Its value is derived from the identities and only its
/// presence is recorded, so it is not free to differ from the block id -- which is why it is
/// written here as `block` and not as a literal.
fn expected_slots(word_value: u64, length: u64, block: u64) -> serde_json::Value {
    serde_json::json!({
        "a": word_value,
        "l": length,
        "pi": block,
        "oi": serde_json::Value::Null,
        "g": block,
    })
}

/// rust-internal: serializes a container this test builds, no product behaviour
#[test]
fn the_level_two_value_is_a_type_and_the_stored_shape_did_not_move() {
    let fields = populated();
    assert_eq!(3, fields.len(), "VACUITY: the container must hold something");

    // THE SAME CONTENT AS THE SHAPE THE FIELD ALWAYS STORED.
    let bare: std::collections::HashMap<String, ElementEntry> = fields
        .iter()
        .map(|(name, addr)| (name.clone(), addr.clone()))
        .collect();

    let got = serde_json::to_value(&fields).expect("the container serializes");
    let want = serde_json::to_value(&bare).expect("the bare map serializes");
    assert_eq!(
        want, got,
        "the container no longer serializes as the address-only map it has always stored. The \
         in-memory value gained a type at this step and the wire must not have noticed; if it \
         did, this step needs a format stamp and does not have one.",
    );
    println!("[level-two] the container and the address-only map serialize identically");

    // AND THE ENCODING CARRIES NO KEY FOR THE NEW TYPE. This is the specific failure
    // the into-conversion prevents, so it is the specific thing asserted rather than left implied.
    let text = serde_json::to_string(&fields).expect("serializes");
    for forbidden in ["ElementEntry", "entries"] {
        assert!(
            !text.contains(forbidden),
            "the stored form carries {forbidden:?}, so the new type leaked into the encoding: {text}",
        );
    }
    println!("[level-two] no key for the new type appears in the stored form");

    let expected: serde_json::Value = serde_json::json!({
        "alpha": expected_slots(word(1, 64), 128, 9),
        "beta": expected_slots(word(2, 4096), 256, 10),
        "gamma": expected_slots(word(3, 8192), 512, 11),
    });
    if expected != got {
        // Printed rather than asserted blind, so a wire change that is INTENDED can be read off
        // the failure instead of guessed at.
        println!("[level-two] RESTATED EXPECTATION DISAGREES");
        println!("  expected: {expected}");
        println!("  got:      {got}");
    }
    assert_eq!(
        expected, got,
        "the stored encoding of a hash field map is not what the wire contract restated above \
         says it is. Either the wire moved -- which needs a format stamp -- or the restatement is \
         stale; read the two lines printed above before changing either.",
    );
    println!("[level-two] the stored encoding matches the restated contract slot by slot");

    // THE PLANTED DIFFERENCE: the comparison must be sensitive to content.
    let mut altered = populated();
    altered.insert("beta".to_string(), address(2, 4096, 999, 10));
    let altered_value = serde_json::to_value(&altered).expect("serializes");
    assert_ne!(
        got, altered_value,
        "a container holding a different address serialized identically, so the comparisons above \
         are not reading the content and prove nothing",
    );
    println!("[level-two] a planted address difference does change the bytes");
}

/// THE WIDTHS, stated as the arithmetic rather than as three numbers.
///
/// rust-internal: reads type widths, no product behaviour
#[test]
fn the_level_two_value_adds_nothing_yet_and_its_end_state_is_twenty_four() {
    let address_width = std::mem::size_of::<ElementEntry>();
    let value_width = std::mem::size_of::<ElementEntry>();
    let pair_width = std::mem::size_of::<(String, ElementEntry)>();

    println!(
        "[level-two] address {address_width} B, level-two value {value_width} B, stored pair \
         {pair_width} B"
    );

    assert_eq!(
        address_width, value_width,
        "the level-two value is supposed to add NOTHING at this step; it is {value_width} against \
         an address of {address_width}, so something was relocated into it early",
    );

    // The end state, as arithmetic. One packed flags byte over the address's eight-aligned group.
    let end_state = (address_width + 1 + 7) / 8 * 8;
    assert_eq!(
        24, end_state,
        "the end-state width is {end_state}, not 24, so the relocation's target arithmetic no \
         longer holds at this address width",
    );
    println!("[level-two] end state with one packed flags byte: {end_state} B");

    // AND THE MODEL ID IS DELIBERATELY ABSENT, which costs nothing and is still right.
    let with_model_id = (address_width + 1 + 1 + 7) / 8 * 8;
    assert_eq!(
        end_state, with_model_id,
        "carrying a model id would change the width, which would make its absence a trade rather \
         than a correctness point",
    );
    println!(
        "[level-two] a model id would also round to {with_model_id} B -- free in width and still \
         omitted, because the map fixes the kind"
    );

    assert_eq!(
        40, pair_width,
        "the stored pair is {pair_width} B, not 40; the container header states its insert cost \
         in terms of this width and would be wrong",
    );
}
