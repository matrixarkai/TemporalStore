// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! FIVE FLAGS IN ONE BYTE, AND THE ONE WAY THAT GOES WRONG.
//!
//! Packing `dirty`, `deleted`, `meta_loaded`, `loading` and `in_memory` into `BucketFlags` takes
//! `BucketNode` from 184 bytes to 176. It also replaces five independent fields, which the
//! compiler kept apart for free, with five masks over one byte -- and masks are hand-written. A
//! flag that answers another flag's question is not a crash and not a failing type check: it is a
//! bucket that reports itself resident while it is loading, or clean while it is dirty, and the
//! engine acts on the answer.
//!
//! So the packing is only as good as the claim that each accessor reads its OWN bit. This module
//! makes that claim checkable three ways, and every one of them has a control that fails.
//!
//!   1. THE MASKS THEMSELVES. Five single-bit masks, pairwise distinct, five bits set between
//!      them. A duplicated mask -- the copy-paste this shape invites -- is caught here before any
//!      accessor is called.
//!   2. THE ACCESSORS, EXHAUSTIVELY. Not one fixture but all thirty-two states of five bits,
//!      written through the setters and read back through the getters. A fixture that happens to
//!      set two flags the same way cannot tell them apart; thirty-two states can, because every
//!      pair of flags disagrees in some state.
//!      THE CONTROL: `MisMaskedNode` is the same five accessors with `loading` reading
//!      `in_memory`'s bit. It is fed to the SAME checker, and the checker must reject it. A guard
//!      that cannot fail is not a guard, and this one is shown failing.
//!   3. THE WIRE, IN BOTH DIRECTIONS AND ALSO EXHAUSTIVELY. The five flags are five separate
//!      boolean keys of the stored index and one byte in memory, and the hand-written serde impls
//!      are the join between them -- a second place to wire a flag to the wrong name. All
//!      thirty-two states round-trip, and a hand-built index with two of the keys SWAPPED must
//!      decode to a different node than the one that wrote it.
//!
//! WHAT THIS MODULE DOES NOT DO. It does not measure bytes; `per_item_byte_budget` owns the
//! accounting and asserts the reconstruction. It proposes no change. It exists because the change
//! it guards replaced a type-checked fact with an arithmetic one.

#![allow(clippy::all)]

use crate::engine::state::{BucketFlags, BucketNode};

/// The five flags, under the names the engine reads them by, each with its own accessor pair.
///
/// Hand-written on purpose and CHECKED AGAINST `BucketFlags::MASKS` below rather than trusted: a
/// flag added to the declaration and not to this table would otherwise be a flag no test here
/// ever reads, and the count check is what refuses that.
type Getter = fn(&BucketNode) -> bool;
type Setter = fn(&mut BucketNode, bool);

fn accessors() -> Vec<(&'static str, Getter, Setter)> {
    vec![
        ("dirty", BucketNode::dirty, BucketNode::set_dirty),
        ("deleted", BucketNode::deleted, BucketNode::set_deleted),
        (
            "meta_loaded",
            BucketNode::meta_loaded,
            BucketNode::set_meta_loaded,
        ),
        ("loading", BucketNode::loading, BucketNode::set_loading),
        ("in_memory", BucketNode::in_memory, BucketNode::set_in_memory),
    ]
}

// -------------------------------------------------------------------------------------------
// 1. THE MASKS.
// -------------------------------------------------------------------------------------------

/// EVERY MASK IS ONE BIT, AND NO TWO ARE THE SAME BIT.
///
/// This is the whole of the arithmetic the accessors rest on, and it is the check that catches
/// the mistake this shape actually invites: a mask copied from the line above and not edited.
/// Two flags sharing a bit is not a type error and every single-flag fixture still passes.
#[test]
fn the_five_flag_masks_are_five_distinct_single_bits() {
    let masks = BucketFlags::MASKS;
    assert_eq!(
        5,
        masks.len(),
        "BucketFlags::MASKS lists {} flags; the node packs five, and a table that has drifted \
         from the declaration proves nothing about it",
        masks.len()
    );

    println!("\n=== the five masks ===");
    let mut union = 0u8;
    for (name, mask) in masks {
        println!("  {name:<12} {mask:#010b}");
        assert_eq!(
            1,
            mask.count_ones(),
            "the mask for `{name}` is {mask:#010b} and sets {} bits; every flag must be exactly \
             one bit or the accessors cannot be independent",
            mask.count_ones()
        );
        assert_eq!(
            0,
            union & mask,
            "the mask for `{name}` ({mask:#010b}) overlaps a mask already taken ({union:#010b}); \
             two flags on one bit answer each other's questions"
        );
        union |= mask;
    }
    assert_eq!(
        5,
        union.count_ones(),
        "five distinct single-bit masks must cover five bits; these cover {} ({union:#010b})",
        union.count_ones()
    );

    // The table the rest of this module drives has to name the same five.
    let accessor_names: Vec<&str> = accessors().iter().map(|(name, _, _)| *name).collect();
    let mask_names: Vec<&str> = masks.iter().map(|(name, _)| *name).collect();
    assert_eq!(
        mask_names, accessor_names,
        "the accessor table and `BucketFlags::MASKS` name different flags, in different orders \
         or counts; one of them is not describing the node"
    );
}

// -------------------------------------------------------------------------------------------
// 2. THE ACCESSORS, AND A MIRROR THAT GETS THEM WRONG.
// -------------------------------------------------------------------------------------------

/// A flag byte read through five accessors, whichever five those are.
///
/// The live node and the deliberately broken mirror both implement it, so the checker below is
/// the SAME code for both and cannot be accidentally weakened for one of them.
trait FlagView {
    fn fresh() -> Self;
    fn put(&mut self, index: usize, on: bool);
    fn take(&self, index: usize) -> bool;
    /// The packed byte, for diagnostics only.
    fn raw(&self) -> u8;
}

impl FlagView for BucketNode {
    fn fresh() -> Self {
        BucketNode::default()
    }

    fn put(&mut self, index: usize, on: bool) {
        (accessors()[index].2)(self, on);
    }

    fn take(&self, index: usize) -> bool {
        (accessors()[index].1)(self)
    }

    fn raw(&self) -> u8 {
        self.flags.bits()
    }
}

/// THE CONTROL. The same five flags in the same byte, with ONE accessor reading the wrong bit:
/// `loading` is written to its own bit and read from `in_memory`'s.
///
/// This is not a hypothetical shape. It is what a mask table gets when a line is copied and the
/// constant on it is not changed, and it is invisible to any test that sets one flag at a time
/// and to any test that sets every flag the same way.
#[derive(Default)]
struct MisMaskedNode(u8);

impl MisMaskedNode {
    const READ_MASKS: [u8; 5] = [
        BucketFlags::DIRTY,
        BucketFlags::DELETED,
        BucketFlags::META_LOADED,
        // The defect, and the only line that differs from the live shape.
        BucketFlags::IN_MEMORY,
        BucketFlags::IN_MEMORY,
    ];
    const WRITE_MASKS: [u8; 5] = [
        BucketFlags::DIRTY,
        BucketFlags::DELETED,
        BucketFlags::META_LOADED,
        BucketFlags::LOADING,
        BucketFlags::IN_MEMORY,
    ];
}

impl FlagView for MisMaskedNode {
    fn fresh() -> Self {
        Self::default()
    }

    fn put(&mut self, index: usize, on: bool) {
        let mask = Self::WRITE_MASKS[index];
        if on {
            self.0 |= mask;
        } else {
            self.0 &= !mask;
        }
    }

    fn take(&self, index: usize) -> bool {
        self.0 & Self::READ_MASKS[index] != 0
    }

    fn raw(&self) -> u8 {
        self.0
    }
}

/// Write all thirty-two states of five flags and read each one back. `Err` names the first
/// disagreement; `Ok` means every flag answered its own question in every state.
fn every_state_reads_back<V: FlagView>() -> Result<usize, String> {
    let names: Vec<&str> = accessors().iter().map(|(name, _, _)| *name).collect();
    let mut checked = 0usize;
    for state in 0u8..32 {
        let wanted: Vec<bool> = (0..5).map(|bit| state & (1 << bit) != 0).collect();
        let mut view = V::fresh();
        for (index, on) in wanted.iter().enumerate() {
            view.put(index, *on);
        }
        for (index, on) in wanted.iter().enumerate() {
            let got = view.take(index);
            if got != *on {
                return Err(format!(
                    "in state {state:#07b} the flag `{}` was written {on} and read back {got}; \
                     the flags written were {:?}",
                    names[index], wanted
                ));
            }
            checked += 1;
        }
        // The byte itself, so a disagreement above can be read against the masks printed by
        // `the_five_flag_masks_are_five_distinct_single_bits`.
        debug_assert_eq!(
            state,
            view.raw(),
            "the five flags were written as state {state:#07b} and the byte holds {:#07b}",
            view.raw()
        );
    }
    Ok(checked)
}

/// EVERY FLAG READS ITS OWN BIT, IN EVERY STATE THE FIVE CAN BE IN -- AND THE CHECK THAT SAYS SO
/// IS SHOWN FAILING ON A MIRROR THAT GETS ONE MASK WRONG.
///
/// The positive half is exhaustive rather than illustrative: thirty-two states, five reads each,
/// 160 answers. Exhaustiveness is what matters here, because any two flags agree in most states
/// and a fixture that lands in one of those cannot tell them apart.
///
/// The negative half is the reason to believe the positive one. `MisMaskedNode` reads `loading`
/// through `in_memory`'s bit and is fed to the same function; if that came back `Ok`, the
/// function would be proving nothing about the live node either.
#[test]
fn every_bucket_flag_is_read_through_its_own_bit_and_a_mis_masked_one_is_caught() {
    // --- The live shape. ---
    match every_state_reads_back::<BucketNode>() {
        Ok(checked) => {
            println!("\n  the live node: {checked} flag reads across 32 states, all correct");
            assert_eq!(
                160, checked,
                "five flags over thirty-two states is 160 reads; this checked {checked}, so the \
                 sweep is not covering what it says it is"
            );
        }
        Err(complaint) => panic!("a bucket flag does not read its own bit: {complaint}"),
    }

    // --- The control: the same check, a mirror with one mask wrong, and it must FAIL. ---
    let caught = every_state_reads_back::<MisMaskedNode>();
    println!(
        "  the mis-masked mirror: {}",
        match &caught {
            Ok(_) => "accepted -- the check cannot fail".to_string(),
            Err(complaint) => format!("rejected -- {complaint}"),
        }
    );
    assert!(
        caught.is_err(),
        "a node whose `loading` accessor reads `in_memory`'s bit passed the very check that is \
         supposed to catch it; the check above proves nothing about the live node either"
    );
}

// -------------------------------------------------------------------------------------------
// 3. THE WIRE.
// -------------------------------------------------------------------------------------------

/// Set the five flags from a five-bit state, leaving every other field at its default.
fn node_in_state(state: u8) -> BucketNode {
    let mut node = BucketNode::default();
    for (index, (_, _, set)) in accessors().into_iter().enumerate() {
        set(&mut node, state & (1 << index) != 0);
    }
    node
}

fn flags_of(node: &BucketNode) -> Vec<bool> {
    accessors().into_iter().map(|(_, get, _)| get(node)).collect()
}

/// ALL THIRTY-TWO FLAG STATES SURVIVE THE STORED SPELLING, AND TWO SWAPPED KEYS DO NOT DECODE
/// THE SAME.
///
/// The five flags are one byte in memory and five separate boolean keys on the wire, and the
/// hand-written `Serialize`/`Deserialize` impls are the join. That join is a second place to wire
/// a flag to the wrong name, and a wrong wiring there is worse than a wrong mask: it survives a
/// restart and it is written into the index.
///
/// The round trip is exhaustive for the same reason the accessor sweep is. The swap is the
/// control: an index whose `loading` and `in_memory` keys are exchanged must decode to a
/// different node, because if it decodes to the same one the two keys are not being read apart.
#[test]
fn every_flag_state_survives_the_stored_spelling_and_swapped_keys_do_not() {
    for state in 0u8..32 {
        let node = node_in_state(state);
        let json = serde_json::to_string(&node).expect("a bucket node serializes");
        let back: BucketNode = serde_json::from_str(&json).expect("a bucket node deserializes");
        assert_eq!(
            flags_of(&node),
            flags_of(&back),
            "flag state {state:#07b} did not survive the wire; it was written as {json}"
        );
    }
    println!("\n  all 32 flag states round-tripped through the stored spelling");

    // --- The control. Two keys exchanged in the text must land on a different node. ---
    //
    // State 8 sets `loading` alone, so exchanging the two keys is a real change and any decoder
    // that reads them apart must see it.
    let node = node_in_state(1 << 3);
    assert!(
        node.loading() && !node.in_memory(),
        "the fixture for the swap must set exactly one of the two keys being exchanged"
    );
    let written = serde_json::to_string(&node).expect("a bucket node serializes");
    let swapped = written
        .replace("\"loading\":true", "\"loading\":\x00")
        .replace("\"in_memory\":false", "\"in_memory\":true")
        .replace("\"loading\":\x00", "\"loading\":false");
    assert_ne!(
        written, swapped,
        "the swap did not change the text, so it cannot test anything"
    );
    let decoded: BucketNode = serde_json::from_str(&swapped).expect("the swapped node decodes");
    println!("  swapped: {swapped}");
    assert!(
        !decoded.loading() && decoded.in_memory(),
        "an index with `loading` and `in_memory` exchanged decoded to loading={} in_memory={}; \
         the decoder is not reading the two keys apart",
        decoded.loading(),
        decoded.in_memory()
    );
    assert_ne!(
        flags_of(&node),
        flags_of(&decoded),
        "exchanging two flag keys on the wire produced the same node; the stored spelling does \
         not distinguish them"
    );
}


// -------------------------------------------------------------------------------------------
// 4. PRESENCE COMES FROM THE WIRE.
// -------------------------------------------------------------------------------------------

/// A NODE THAT OMITS A REQUIRED KEY IS REFUSED, AND ONE THAT OMITS AN OPTIONAL KEY IS NOT.
///
/// The hand-written deserializer has to reproduce the derive's two different answers to a missing
/// key: the fields that carried `#[serde(default)]` fill in, and the seven that did not are
/// errors. The difference matters because a flag filled in with `false` is not an absent flag --
/// it is a bucket that reads as clean, or as not resident, on the strength of a key that was
/// never written. A store whose index is truncated mid-node would load as a valid node with
/// plausible flags.
///
/// THIS TEST EXISTS BECAUSE A MUTATION SURVIVED. Replacing the refusal on `dirty` with
/// `unwrap_or_default()` passed every other test in this module and in `per_item_byte_budget`,
/// which is to say the rule was stated in two comments and enforced nowhere. Each of the seven
/// required keys is dropped in turn and the decode must FAIL; `deleted`, which really is
/// optional, is dropped as the control and must SUCCEED.
#[test]
fn a_bucket_node_that_omits_a_required_key_is_refused_and_an_optional_one_is_not() {
    let node = node_in_state(0b10101);
    let json = serde_json::to_string(&node).expect("a bucket node serializes");

    /// Remove one top-level key and its value from a flat JSON object.
    fn without(json: &str, key: &str) -> String {
        let needle = format!("\"{key}\":");
        let at = json.find(&needle).unwrap_or_else(|| panic!("`{key}` is not in {json}"));
        let end = json[at..]
            .find(',')
            .map(|offset| at + offset + 1)
            .expect("every key under test is followed by another");
        let mut out = String::with_capacity(json.len());
        out.push_str(&json[..at]);
        out.push_str(&json[end..]);
        out
    }

    const REQUIRED: [&str; 7] = [
        "routing_slot",
        "dirty",
        "meta_loaded",
        "loading",
        "in_memory",
        "dirty_generation",
        "last_dump_sequence",
    ];

    println!("\n=== a node missing each required key ===");
    for key in REQUIRED {
        let damaged = without(&json, key);
        assert!(
            !damaged.contains(&format!("\"{key}\":")),
            "the fixture for `{key}` still contains the key, so it tests nothing: {damaged}"
        );
        let outcome = serde_json::from_str::<BucketNode>(&damaged);
        let complaint = match &outcome {
            Ok(_) => "ACCEPTED".to_string(),
            Err(error) => error.to_string(),
        };
        println!("  {key:<20} {complaint}");
        assert!(
            outcome.is_err(),
            "a bucket node with no `{key}` decoded successfully; the field would be filled in \
             with a zero that reads as a real answer"
        );
        let message = outcome.unwrap_err().to_string();
        assert!(
            message.contains(key),
            "the refusal for a missing `{key}` says {message:?}, which does not name the key \
             that is missing"
        );
    }

    // --- THE CONTROL. `deleted` carried `#[serde(default)]` and still must. ---
    let without_deleted = without(&json, "deleted");
    assert!(
        !without_deleted.contains("\"deleted\":"),
        "the control fixture still contains the key it is supposed to drop"
    );
    let loaded: BucketNode =
        serde_json::from_str(&without_deleted).expect("a node without `deleted` must still load");
    assert!(
        !loaded.deleted(),
        "an absent `deleted` must default to false, not to true"
    );
    assert_eq!(
        node.dirty(),
        loaded.dirty(),
        "dropping the optional key must not disturb the flags that were written"
    );
    println!("  {:<20} accepted, and defaults to false", "deleted (optional)");
}
