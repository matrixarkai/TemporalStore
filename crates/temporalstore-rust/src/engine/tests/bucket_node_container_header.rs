// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT THE BUCKET NODE'S 88 BYTES ARE SPENT ON, AND WHICH OF THEM THE CONTAINER HEADER IS.
//!
//! A per-bucket descriptor of 24 bytes is the width this campaign is aiming the node at. The node
//! is 88. The one-line explanation that has been carried from lane to lane is that the node holds
//! a CONTAINER of entries where a 24-byte descriptor holds a single address word, so the node's
//! width is set by a collection header while the narrow descriptor's is set by a pointer it can
//! overload with a tag.
//!
//! THAT EXPLANATION IS TRUE AND IT ACCOUNTS FOR A QUARTER OF THE DIFFERENCE. This module takes the
//! 64-byte shortfall apart into three groups, each derived from `size_of` over the live
//! declarations rather than written down:
//!
//!   * 24 B -- THREE LOG-SEQUENCE WORDS. `dirty_generation`, `first_dirty_wal_sequence` and
//!     `first_dirty_index_log_sequence`. A descriptor that does not anchor write-ahead-log
//!     reclaim has no equivalent of any of them.
//!   * 24 B -- A SECOND INDEX. `object_index` plus `deleted_object_index`: which object ids this
//!     bucket holds, and which it held. A descriptor that is itself per-item has no per-bucket
//!     object list to carry.
//!   * 16 B -- AND ONLY 16 -- THE CONTAINER HEADER ABOVE ONE WORD. `block_index` is 24 B; a single
//!     tagged address word would be 8. So the container costs 16 of the 64.
//!
//! WHY THAT MATTERS MORE THAN THE ARITHMETIC. Every route anyone has proposed for this node so far
//! has been a route at the container, and the container is the SMALLEST of the three groups. Even
//! a container narrowed to one tagged word -- which this engine cannot reach, for the entry-width
//! reason below -- leaves the node at 72 against a goal of 24. The container is not what makes the
//! node wide; two jobs the narrow descriptor does not do are.
//!
//! AND THE ENTRY'S WIDTH IS INVISIBLE AT THE NODE, WHICH IS THE CLAIM THIS MODULE EXISTS TO PIN.
//! `BlockIndexMap` holds its single block behind a `Box` and its many blocks behind a `Vec`, so
//! NEITHER arm exposes the entry's width to the node. `the_node_cannot_see_how_wide_an_entry_is`
//! instantiates the same three-arm shape over stand-in entries of 8, 40, 56 and 200 bytes and
//! asserts one width for all four. A landing that narrows the entry therefore moves this structure
//! by zero, and the arithmetic says so before anyone measures it. That is not a prediction about
//! one change; it is a property of holding entries behind pointers, and it will hold for the next
//! entry narrowing too.
//!
//! WHAT IS ACTUALLY REACHABLE AT THE CONTAINER, and it is LESS THAN THE ARITHMETIC SUGGESTS.
//! This module was drafted asserting a 24 -> 16 -> 8 ladder: drop the `Vec`'s capacity word to
//! reach 16, then put the length inside the allocation to reach 8. BOTH RUNGS WERE WRONG, measured,
//! and the reason is the same one that refuses `ObjectIndex` at 8 -- A NICHE GIVES ONE SPARE
//! ENCODING AND THREE ARMS NEED TWO:
//!
//!   * 24 -> 24. Swapping the many-entry arm's `Vec` for a boxed slice drops a word of PAYLOAD and
//!     buys NOTHING, because the shape then has two 16-byte arms and no niche left to tell them
//!     apart, so the aligner hands the saved word straight back as a tag. The capacity word was
//!     being carried for free inside the tag's rounding.
//!   * 16, not 8. Putting BOTH arms behind a single pointer gets the handle to 16 and no further,
//!     for the same reason: two 8-byte pointer arms plus an empty arm is three variants over one
//!     null niche.
//!
//! So every SAFE three-arm shape floors at 16, which leaves the node at 80. One word is reachable
//! only by hand-rolling the tag into the pointer's low bits, which is unsafe code on the read path
//! and leaves the node at 72. `the_container_header_ladder_floors_at_sixteen_not_eight` asserts all
//! four rungs and the node width each lands on. Every rung below the first also trades the capacity
//! word for a reallocation on every growth step, which is a HEAP question and not a width one --
//! `inline_arm_trade.rs` is where that trade gets priced, and this module deliberately does not
//! price it. What this module establishes is the CEILING: 80 B by any safe shape, 72 B with a
//! hand-rolled tag, and 64 B if the small-field tail went too.
//!
//! WHAT REDDENS THIS MODULE, each one driven rather than asserted to work:
//!
//!   * INLINING the single arm of the shared mirror reddens the CONTROL in all six tests and not
//!     the invariance assertion, because a mirror that inlines has by definition stopped matching
//!     a declaration that does not. Recorded because it is a mutation that LOOKS like a fail-first
//!     for `the_node_cannot_see_how_wide_an_entry_is` and is not one -- the control got there
//!     first, which is the trap of a guard that passes under mutation because an earlier stage
//!     already did the work.
//!   * WIDENING the hand-rolled tag stand-in to two words reddens exactly one test, at the ladder
//!     equality itself, with the control and the other five green. That is the fail-first for the
//!     ladder.
//!   * ADDING a field to the node reddens the exhaustive pattern at compile time, as above.
//!
//! THE DECOMPOSITION IS TAKEN OVER A REAL INSTANCE, AND THE CLAIM THAT MOTIVATED THAT WAS WRONG.
//! This module was drafted saying that `every_byte_of_the_bucket_node_is_accounted_for` in
//! `per_item_byte_budget.rs` would stay GREEN on a field added to the node, because its guard is
//! `table.len() == 10` and a ten-row table stays ten rows when the struct grows to eleven. MEASURED
//! BY PLANTING AN ELEVENTH FIELD (`spare: u8`, chosen because the tail absorbs it and the width
//! stays 88, so the width assertions stay REACHABLE rather than becoming compile errors): that
//! module does NOT stay green. It fails to compile, at two `BucketNode { .. }` struct literals
//! (E0063). The table is blind; the literals beside it are not, and they are what covers it.
//!
//! SO WHAT THE EXHAUSTIVE PATTERN HERE ADDS IS NARROWER THAN THE DRAFT CLAIMED, and it is still
//! worth having. It fails AT THE DECOMPOSITION (E0027, `pattern does not mention field`), so the
//! error names the structure whose sum has gone wrong rather than an unrelated constructor; and it
//! keeps failing if those two literals ever become `..Default::default()`, at which point the
//! table's row count is the only thing left and it does not count fields. Widths come from
//! `size_of_val` on the pattern's own bindings rather than from a type named a second time, so a
//! field whose declared TYPE moved is caught here and would be summed at its old width there.
#![allow(clippy::all)]
use std::mem::{align_of, align_of_val, size_of, size_of_val};

use crate::engine::state::{
    BlockIndex, BlockIndexMap, BucketFlags, BucketLayoutState, BucketNode, BucketTtl,
    DeletedObjectIndex, ObjectIndex,
};

/// The per-bucket width this campaign is aiming the node at.
const GOAL: usize = 24;

/// One machine word here, derived rather than written as 8.
const WORD: usize = size_of::<u64>();

// =============================================================================================
// THE WIDTH, BRACKETED.
//
// A single `==` that compiles gives the value. The pair beside it proves the value is not an
// artefact of the comparison chosen -- a mistake this campaign has made often enough to make the
// bracket cheap insurance.
// =============================================================================================

const _: () = assert!(size_of::<BucketNode>() == 88);
const _: () = assert!(size_of::<BucketNode>() != 87 && size_of::<BucketNode>() != 89);

const _: () = assert!(size_of::<BlockIndexMap>() == 24);
const _: () = assert!(size_of::<BlockIndexMap>() != 23 && size_of::<BlockIndexMap>() != 25);

const _: () = assert!(size_of::<ObjectIndex>() == 16);
const _: () = assert!(size_of::<ObjectIndex>() != 15 && size_of::<ObjectIndex>() != 17);

const _: () = assert!(size_of::<DeletedObjectIndex>() == 8);
const _: () = assert!(size_of::<DeletedObjectIndex>() != 7 && size_of::<DeletedObjectIndex>() != 9);

const _: () = assert!(size_of::<BucketTtl>() == 8);
const _: () = assert!(size_of::<BucketLayoutState>() == 1);
const _: () = assert!(size_of::<BucketFlags>() == 1);

// =============================================================================================
// STAND-IN ENTRIES, AND THE SHAPE THAT HOLDS THEM.
// =============================================================================================

/// An entry stand-in of `N` words.
///
/// Eight-aligned like the live entry, which is asserted rather than assumed: a stand-in of a
/// different alignment would lay out differently inside the arms below and the invariance this
/// module claims would be a claim about alignment instead.
#[repr(transparent)]
struct Entry<const N: usize>([u64; N]);

/// The live block index's shape, over any entry type.
///
/// Spelled once and instantiated, so the four widths compared below cannot differ in anything but
/// the entry.
#[allow(dead_code)]
enum MirrorIndex<E> {
    Empty,
    One(u64, Box<E>),
    Many(Vec<(u64, E)>),
}

/// The same, with the many-entry arm's capacity word dropped.
#[allow(dead_code)]
enum MirrorIndexBoxedSlice<E> {
    Empty,
    One(u64, Box<E>),
    Many(Box<[(u64, E)]>),
}

/// A length-prefixed allocation: what a one-word container handle would have to point at.
#[allow(dead_code)]
struct LengthPrefixed<E> {
    len: usize,
    capacity: usize,
    first: E,
}

/// The same shape again, with BOTH arms behind a single pointer.
///
/// DRAFTED AS "the narrowest a three-arm handle can be: one word". It is not -- measured at 16,
/// because two pointer arms and an empty arm are three variants over ONE null niche and the
/// aligner adds a tag word. Kept as the rung it actually is.
#[allow(dead_code)]
enum MirrorIndexOneWord<E> {
    Empty,
    One(Box<(u64, E)>),
    Many(Box<LengthPrefixed<E>>),
}

/// A hand-rolled tagged pointer: the tag in the pointer's low bits, which a `Box` cannot express.
///
/// The ONLY shape that reaches one word here. It is a raw word with an unsafe discipline around
/// every read, and it is priced as a width only -- what it would cost the read path is not this
/// module's measurement.
#[allow(dead_code)]
struct MirrorIndexHandRolledTag(usize);

/// The live node, over any block-index shape.
#[allow(dead_code)]
struct MirrorNode<I> {
    routing_bucket: u32,
    layout: BucketLayoutState,
    flags: BucketFlags,
    ttl_ms: BucketTtl,
    dirty_generation: u64,
    first_dirty_wal_sequence: u64,
    first_dirty_index_log_sequence: u64,
    object_index: ObjectIndex,
    deleted_object_index: DeletedObjectIndex,
    block_index: I,
}

/// The live node with the whole small-field tail gone.
///
/// Not a proposal -- `flags` is live state and `routing_bucket`'s removal is priced elsewhere as a
/// stored-format change for zero bytes. It is here to establish that the tail is ONE rounding, so
/// a claim that some single tail field is worth narrowing can be refused with a number.
#[allow(dead_code)]
struct MirrorNodeEmptyTail<I> {
    ttl_ms: BucketTtl,
    dirty_generation: u64,
    first_dirty_wal_sequence: u64,
    first_dirty_index_log_sequence: u64,
    object_index: ObjectIndex,
    deleted_object_index: DeletedObjectIndex,
    block_index: I,
}

// =============================================================================================
// THE CONTROLS. Both run first, in every test that prices anything.
// =============================================================================================

/// The mirror tracks the declaration, or nothing below describes this engine.
fn assert_mirrors_track_the_declaration() {
    assert_eq!(
        size_of::<BlockIndexMap>(),
        size_of::<MirrorIndex<BlockIndex>>(),
        "the mirror of the block index is {} B against the declaration's {} B, so every width \
         below is a width of something this engine does not hold",
        size_of::<MirrorIndex<BlockIndex>>(),
        size_of::<BlockIndexMap>()
    );
    assert_eq!(
        size_of::<BucketNode>(),
        size_of::<MirrorNode<BlockIndexMap>>(),
        "the mirror of the node is {} B against the declaration's {} B",
        size_of::<MirrorNode<BlockIndexMap>>(),
        size_of::<BucketNode>()
    );
    assert_eq!(
        8,
        align_of::<BlockIndex>(),
        "the live entry is {}-aligned; the word-array stand-ins below are 8-aligned and would not \
         lay out as it does",
        align_of::<BlockIndex>()
    );
}

// =============================================================================================
// THE DECOMPOSITION, OVER A REAL INSTANCE.
// =============================================================================================

/// EVERY BYTE OF THE NODE, SUMMED OVER AN INSTANCE THE ENGINE BUILT.
///
/// `size_of_val` over the fields of a `BucketNode::default()`, so the sum reads the DECLARATION
/// and not a table beside it. The eight-aligned group and the small-field tail are summed
/// separately and the slack is named as the aligner's, because a width quoted without its sum
/// cannot tell a structure that is full from one that is a third padding, and those two want
/// opposite fixes.
///
/// THE FIELD SET IS ENUMERATED BY THE COMPILER, not by a table. The node is destructured with an
/// EXHAUSTIVE pattern and no `..`, so a field added to the declaration is a compile error here
/// rather than a row this test silently fails to sum. That is the half a `table.len() == 10` check
/// cannot cover: it asserts the table has ten rows, which stays true when the struct grows to
/// eleven. And because every width below comes from `size_of_val` on the binding rather than from
/// a type named a second time, a field whose declared TYPE changed is caught too -- the other half.
/// Between them a type change, a rename, an addition and a removal all fail, and three of the four
/// fail at compile time.
#[test]
fn every_byte_of_the_node_sums_over_an_instance_and_not_a_table() {
    assert_mirrors_track_the_declaration();

    let node = BucketNode::default();

    // EXHAUSTIVE. No `..` -- adding a field to `BucketNode` must not compile here.
    let BucketNode {
        routing_bucket,
        layout,
        flags,
        ttl_ms,
        dirty_generation,
        first_dirty_wal_sequence,
        first_dirty_index_log_sequence,
        object_index,
        deleted_object_index,
        block_index,
    } = &node;

    // Each field's width AND alignment read off the binding. No type is named twice.
    let fields: [(&str, usize, usize); 10] = [
        ("routing_bucket", size_of_val(routing_bucket), align_of_val(routing_bucket)),
        ("layout", size_of_val(layout), align_of_val(layout)),
        ("flags", size_of_val(flags), align_of_val(flags)),
        ("ttl_ms", size_of_val(ttl_ms), align_of_val(ttl_ms)),
        ("dirty_generation", size_of_val(dirty_generation), align_of_val(dirty_generation)),
        (
            "first_dirty_wal_sequence",
            size_of_val(first_dirty_wal_sequence),
            align_of_val(first_dirty_wal_sequence),
        ),
        (
            "first_dirty_index_log_sequence",
            size_of_val(first_dirty_index_log_sequence),
            align_of_val(first_dirty_index_log_sequence),
        ),
        ("object_index", size_of_val(object_index), align_of_val(object_index)),
        (
            "deleted_object_index",
            size_of_val(deleted_object_index),
            align_of_val(deleted_object_index),
        ),
        ("block_index", size_of_val(block_index), align_of_val(block_index)),
    ];

    println!("\n=== BucketNode, summed over an instance ===");
    println!("  {:<34} {:>5} {:>6}", "field", "size", "align");
    let mut eight_aligned = 0usize;
    let mut tail = 0usize;
    for (name, size, align) in &fields {
        println!("  {name:<34} {size:>5} {align:>6}");
        if *align == align_of::<BucketNode>() {
            eight_aligned += size;
        } else {
            tail += size;
        }
    }
    let sum = eight_aligned + tail;
    let slack = size_of::<BucketNode>() - sum;
    println!(
        "  -> {eight_aligned} B eight-aligned + {tail} B tail = {sum} B of field, {slack} B slack, \
         {} B declared",
        size_of::<BucketNode>()
    );

    assert_eq!(
        86, sum,
        "the fields sum to {sum} B and not 86; a field has been added, removed or retyped and \
         every figure in this module is about the previous declaration"
    );
    assert_eq!(80, eight_aligned, "the eight-aligned group is {eight_aligned} B and not 80");
    assert_eq!(6, tail, "the small-field tail is {tail} B and not 6");
    assert_eq!(2, slack, "the aligner's slack is {slack} B and not 2");
    assert_eq!(
        size_of::<BucketNode>(),
        eight_aligned + tail.div_ceil(align_of::<BucketNode>()) * align_of::<BucketNode>(),
        "eight-aligned + round_up(tail) does not reconstruct the width, so this is not the layout \
         rule it claims to be"
    );
}

// =============================================================================================
// THE SHORTFALL, IN THREE GROUPS.
// =============================================================================================

/// WHERE THE 64 BYTES ABOVE THE GOAL ACTUALLY ARE, and the container is the smallest group.
///
/// Both arms of every ratio are named where the ratio is written. The denominator throughout is
/// `size_of::<BucketNode>() - GOAL` = the whole shortfall, 64 B; the numerators are the three
/// groups, each derived from the live declarations.
#[test]
fn the_container_header_is_a_quarter_of_the_shortfall_and_not_the_whole_of_it() {
    assert_mirrors_track_the_declaration();

    let node = BucketNode::default();
    let shortfall = size_of::<BucketNode>() - GOAL;

    // Group 1: the log-sequence words.
    let log_sequences = size_of_val(&node.dirty_generation)
        + size_of_val(&node.first_dirty_wal_sequence)
        + size_of_val(&node.first_dirty_index_log_sequence);
    // Group 2: the second index.
    let object_pair = size_of_val(&node.object_index) + size_of_val(&node.deleted_object_index);
    // Group 3: what the container header costs ABOVE one tagged address word.
    let container_above_one_word = size_of_val(&node.block_index) - WORD;

    println!("\n=== the {shortfall} B above the {GOAL} B goal, in three groups ===");
    for (name, bytes) in [
        ("three log-sequence words", log_sequences),
        ("a second index (object ids held, and held-and-deleted)", object_pair),
        ("the container header above one tagged word", container_above_one_word),
    ] {
        println!(
            "  {name:<56} {bytes:>3} B   {:>6.2}% of the {shortfall} B shortfall",
            100.0 * bytes as f64 / shortfall as f64
        );
    }

    assert_eq!(
        shortfall,
        log_sequences + object_pair + container_above_one_word,
        "the three groups sum to {} B against a {shortfall} B shortfall, so they do not partition \
         it and the percentages above have no denominator",
        log_sequences + object_pair + container_above_one_word
    );

    // THE FINDING, as an inequality rather than a literal: the container is the SMALLEST group.
    assert!(
        container_above_one_word < log_sequences,
        "the container header ({container_above_one_word} B) is no longer smaller than the \
         log-sequence words ({log_sequences} B); the claim that container routes chase the \
         smallest group is stale"
    );
    assert!(
        container_above_one_word < object_pair,
        "the container header ({container_above_one_word} B) is no longer smaller than the object \
         index pair ({object_pair} B)"
    );
    assert_eq!(
        16, container_above_one_word,
        "the container header costs {container_above_one_word} B above one word and not 16"
    );
}

// =============================================================================================
// THE INVARIANCE. The claim that a narrower entry moves this structure by zero.
// =============================================================================================

/// THE NODE CANNOT SEE HOW WIDE AN ENTRY IS, so narrowing the entry cannot narrow the node.
///
/// Four stand-in entries an order of magnitude apart in width, through one spelling of the live
/// three-arm shape. Both arms that carry an entry carry it behind a pointer -- a `Box` in the
/// single arm, a `Vec` in the many arm -- so the entry's width never reaches the node's layout.
///
/// THE ANTI-CONSTANT CHECK. The stand-ins are asserted to actually DIFFER in width first. Four
/// equal widths proves nothing if the four inputs were equal too, and a stand-in that silently
/// collapsed to one width would make this test pass by reading itself.
#[test]
fn the_node_cannot_see_how_wide_an_entry_is() {
    assert_mirrors_track_the_declaration();

    // The inputs differ. Asserted before anything downstream is compared.
    let entry_widths = [
        size_of::<Entry<1>>(),
        size_of::<Entry<5>>(),
        size_of::<Entry<7>>(),
        size_of::<Entry<25>>(),
    ];
    assert_eq!([8, 40, 56, 200], entry_widths, "the stand-in entries are not the widths claimed");
    for pair in entry_widths.windows(2) {
        assert!(
            pair[0] < pair[1],
            "the stand-in entries do not differ in width ({entry_widths:?}), so an equal OUTPUT \
             below would be a tautology"
        );
    }

    let index_widths = [
        size_of::<MirrorIndex<Entry<1>>>(),
        size_of::<MirrorIndex<Entry<5>>>(),
        size_of::<MirrorIndex<Entry<7>>>(),
        size_of::<MirrorIndex<Entry<25>>>(),
    ];
    let node_widths = [
        size_of::<MirrorNode<MirrorIndex<Entry<1>>>>(),
        size_of::<MirrorNode<MirrorIndex<Entry<5>>>>(),
        size_of::<MirrorNode<MirrorIndex<Entry<7>>>>(),
        size_of::<MirrorNode<MirrorIndex<Entry<25>>>>(),
    ];

    println!("\n=== the entry's width, and what it does to the index and the node ===");
    println!("  {:<14} {:>12} {:>12}", "entry", "block index", "node");
    for i in 0..4 {
        println!("  {:<14} {:>12} {:>12}", entry_widths[i], index_widths[i], node_widths[i]);
    }

    assert_eq!(
        [24, 24, 24, 24], index_widths,
        "the block index's width moved with the entry's: {index_widths:?}. One of its arms is \
         holding an entry INLINE, and the invariance this module asserts is not a property of this \
         shape any more"
    );
    assert_eq!(
        [88, 88, 88, 88], node_widths,
        "the node's width moved with the entry's: {node_widths:?}"
    );
    // And the live width is one of the four, so this is not a statement about stand-ins alone.
    assert!(
        entry_widths.contains(&size_of::<BlockIndex>()),
        "the live entry is {} B, which is not one of the widths sampled ({entry_widths:?}); add it \
         or this test is not covering the declaration it is about",
        size_of::<BlockIndex>()
    );
    println!(
        "  the live entry is {} B and the node is {} B; an entry narrowed to any of the four \
         widths above leaves the node at {}",
        size_of::<BlockIndex>(),
        size_of::<BucketNode>(),
        size_of::<BucketNode>()
    );
}

// =============================================================================================
// THE LADDER, AND THE CEILING IT REACHES.
// =============================================================================================

/// WHAT A NARROWER CONTAINER HANDLE IS WORTH, as four rungs and the node width each lands on.
///
/// THIS TEST WAS DRAFTED ASSERTING 24 / 16 / 8 AND THE MEASUREMENT REFUSED IT at the second and
/// third rungs. The draft reasoned about PAYLOAD -- a boxed slice is one word narrower than a
/// `Vec`, a single pointer is two -- and forgot the TAG. A three-arm enum over arms that both end
/// in a pointer has exactly one null niche to spend and needs two spare encodings, so the aligner
/// adds a tag word and hands the payload saving straight back. The same sentence refuses
/// `ObjectIndex` at 8; it refuses every safe shape of this handle below 16 as well, and nobody had
/// applied it here.
///
/// So: dropping the capacity word is worth ZERO, and one word is reachable only outside the type
/// system. Each rung below the first also trades capacity for a reallocation on every growth step,
/// which is a HEAP trade this module does not price.
#[test]
fn the_container_header_ladder_floors_at_sixteen_not_eight() {
    assert_mirrors_track_the_declaration();

    let rungs = [
        ("as declared: many-entry arm is a Vec", size_of::<MirrorIndex<BlockIndex>>()),
        (
            "many-entry arm is a boxed slice (no capacity word)",
            size_of::<MirrorIndexBoxedSlice<BlockIndex>>(),
        ),
        (
            "both arms behind one pointer (length in the pointee)",
            size_of::<MirrorIndexOneWord<BlockIndex>>(),
        ),
        (
            "a hand-rolled tag in the pointer's low bits (unsafe)",
            size_of::<MirrorIndexHandRolledTag>(),
        ),
    ];
    let nodes = [
        size_of::<MirrorNode<MirrorIndex<BlockIndex>>>(),
        size_of::<MirrorNode<MirrorIndexBoxedSlice<BlockIndex>>>(),
        size_of::<MirrorNode<MirrorIndexOneWord<BlockIndex>>>(),
        size_of::<MirrorNode<MirrorIndexHandRolledTag>>(),
    ];

    println!("\n=== the container handle, and the node it leaves ===");
    println!("  {:<54} {:>7} {:>7} {:>9}", "shape", "handle", "node", "vs today");
    for (i, (name, handle)) in rungs.iter().enumerate() {
        println!(
            "  {name:<54} {handle:>7} {:>7} {:>9}",
            nodes[i],
            format!("{:+}", nodes[i] as isize - size_of::<BucketNode>() as isize)
        );
    }

    assert_eq!(
        [24, 24, 16, 8],
        rungs.map(|(_, w)| w),
        "the handle ladder is not 24/24/16/8. If the second rung has come IN under the first, a \
         niche has appeared that was not there and dropping the capacity word has started paying"
    );
    assert_eq!([88, 88, 80, 72], nodes, "the node ladder is not 88/88/80/72");

    // THE RUNG THAT BUYS NOTHING, asserted as the equality it is rather than left to the table.
    assert_eq!(
        rungs[0].1, rungs[1].1,
        "dropping the many-entry arm's capacity word moved the handle from {} B to {} B; it is \
         supposed to move nothing, because the tag takes the word back",
        rungs[0].1, rungs[1].1
    );
    // AND THE BOUND ON THAT NEGATIVE: something below it DOES move, so this is a statement about
    // the tag and not about the handle being immovable.
    assert!(
        rungs[2].1 < rungs[0].1,
        "no rung moves the handle at all, so this is not describing a tag the way it claims to"
    );

    // THE CEILING. Stated against the goal, which is the claim that matters.
    let safe_floor = nodes[2];
    let unsafe_floor = nodes[3];
    let unsafe_floor_with_tail_too =
        size_of::<MirrorNodeEmptyTail<MirrorIndexHandRolledTag>>();
    println!(
        "  CEILING: {safe_floor} B is the floor of every SAFE container shape. {unsafe_floor} B \
         with a hand-rolled tag. {unsafe_floor_with_tail_too} B if the small-field tail went too \
         -- which is not on offer, `flags` is live state. The goal is {GOAL} B."
    );
    assert_eq!(
        80, safe_floor,
        "the safe floor is {safe_floor} B and not 80"
    );
    assert_eq!(
        64, unsafe_floor_with_tail_too,
        "the most optimistic floor -- a hand-rolled tag AND an emptied tail -- is \
         {unsafe_floor_with_tail_too} B and not 64"
    );
    assert!(
        unsafe_floor_with_tail_too > GOAL,
        "the container route now reaches the {GOAL} B goal, which would overturn this module's \
         conclusion -- revisit it rather than deleting this assertion"
    );
    println!(
        "  so the container route closes at most {} B of the {} B shortfall by any safe shape, and \
         the floor it reaches is {:.2}x the goal; even the hand-rolled, emptied-tail best case is \
         {:.2}x",
        size_of::<BucketNode>() - safe_floor,
        size_of::<BucketNode>() - GOAL,
        safe_floor as f64 / GOAL as f64,
        unsafe_floor_with_tail_too as f64 / GOAL as f64
    );
}

// =============================================================================================
// THE SAME NICHE RULE, APPLIED TO THE OTHER TWO CONTAINERS.
// =============================================================================================

/// WHY `ObjectIndex` IS 16 AND WHY MERGING IT WITH ITS DELETED TWIN DOES NOT REACH 8.
///
/// Re-derived by construction rather than taken on trust. The recorded refusal is that the single
/// arm holds a full-width hash, a full word has no niche, and `NonZeroU64` does not rescue it
/// because a niche gives one spare encoding and three arms need two. That is asserted here against
/// a `NonZeroU64` mirror, which is the shape the refusal is about.
///
/// AND IT EXTENDS, which the recorded refusal did not say: MERGING the two object containers into
/// one tagged member -- 24 B of node spent on two fields -- is refused by the same sentence and
/// reaches 16, not 8. So the merge is worth 8 B and costs the common bucket an allocation, which
/// is the trade `inline_arm_trade.rs` already refuses for the block index's common arm.
#[test]
fn the_object_containers_are_refused_at_eight_by_the_same_one_niche_rule() {
    assert_mirrors_track_the_declaration();

    #[allow(dead_code)]
    enum MirrorObjectIndexNonZero {
        Empty,
        One(std::num::NonZeroU64),
        Many(Box<Vec<u64>>),
    }
    /// Both object containers as ONE tagged member.
    #[allow(dead_code)]
    enum MirrorMergedObjectPair {
        Empty,
        OneLive(std::num::NonZeroU64),
        General(Box<(Vec<u64>, Option<Vec<u64>>)>),
    }
    /// The control: TWO arms over one niche, which DOES reach a word.
    #[allow(dead_code)]
    enum MirrorTwoArmsOnly {
        Empty,
        Many(Box<Vec<u64>>),
    }

    let live_pair = size_of::<ObjectIndex>() + size_of::<DeletedObjectIndex>();
    println!("\n=== the object containers, and what a niche can and cannot do ===");
    for (name, width) in [
        ("`ObjectIndex` as declared (3 arms, bare u64)", size_of::<ObjectIndex>()),
        ("the same with a NonZeroU64 single arm (3 arms)", size_of::<MirrorObjectIndexNonZero>()),
        ("both object containers merged into one (3 arms)", size_of::<MirrorMergedObjectPair>()),
        ("CONTROL: two arms over one niche", size_of::<MirrorTwoArmsOnly>()),
    ] {
        println!("  {name:<52} {width:>4} B");
    }
    println!("  the live pair costs {live_pair} B of the node across two fields");

    assert_eq!(
        size_of::<ObjectIndex>(),
        size_of::<MirrorObjectIndexNonZero>(),
        "a `NonZeroU64` single arm narrowed the object index from {} B to {} B; the recorded \
         refusal says a niche cannot rescue a three-arm shape and it would be stale",
        size_of::<ObjectIndex>(),
        size_of::<MirrorObjectIndexNonZero>()
    );
    assert_eq!(
        16,
        size_of::<MirrorMergedObjectPair>(),
        "merging the two object containers gives {} B and not 16",
        size_of::<MirrorMergedObjectPair>()
    );
    assert_eq!(
        8,
        size_of::<MirrorTwoArmsOnly>(),
        "the CONTROL is {} B. Two arms over one niche must reach a word, or the explanation \
         offered above -- that it is the THIRD arm that costs the word -- is not what is happening",
        size_of::<MirrorTwoArmsOnly>()
    );
    println!(
        "  so merging the pair is worth {} B of node, not {}, and it boxes the arm nearly every \
         bucket is in",
        live_pair - size_of::<MirrorMergedObjectPair>(),
        live_pair - WORD
    );
}

// =============================================================================================
// WHETHER THERE IS A COMMON SINGLE-ENTRY BUCKET TO INLINE AT ALL.
// =============================================================================================

/// THE COMMON BUCKET DOES NOT HOLD ONE ENTRY, AT THE RANGE WE ACTUALLY RUN.
///
/// Every inline route for this node needs the common bucket to hold ONE entry, the way a narrow
/// per-item descriptor holds one address. Whether it does is decided by the ROUTING RANGE rather
/// than by the workload, and the range that ships is [`crate::DEFAULT_END_ROUTING_BUCKET`].
///
/// MEASURED IN THIS TREE by `engine::tests::routing_range_default::
/// the_pages_a_bucket_holds_at_every_candidate_range_as_percentiles_and_max`, routed keys at one
/// page a record, on 0..1023:
///
///   *  4,000 records: 1,024 occupied buckets over 4,000 pages. mean 3.9062, min 1, p50 4, p90 6,
///      p99 7, MAX 8. Buckets holding exactly one page: 54 of 1,024 = 5.273%. Buckets holding more
///      than one: 970 of 1,024 = 94.727%.
///   * 40,000 records: 1,024 occupied buckets over 40,000 pages. mean 39.0625, MIN 29, p50 39,
///      p90 44, p99 48, MAX 50. Buckets holding exactly one page: 0 of 1,024 = 0.000%. Buckets
///      holding more than one: 1,024 of 1,024 = 100.000%.
///
/// At ten times the corpus the single-entry bucket is not rare, it is ABSENT, and the NARROWEST
/// bucket in the store holds twenty-nine entries. There is no common single-entry shape to inline,
/// so the handle's width is set by the many-entry arm -- which is what the ladder above measures.
///
/// WHY THIS IS ASSERTED HERE RATHER THAN CITED FROM THERE. That module prints its `u32::MAX` arm
/// as `0..u32::MAX (shipped default)` and says "the shipped default" in its module doc and in its
/// assertion messages, twenty-five times in all -- and `u32::MAX` is the OLD default. The module
/// predates the move and is the module that CHOSE 1023; its own constant for the wide arm is
/// named `OLD_DEFAULT_END_ROUTING_BUCKET` and its docstring says "before the default moved", while
/// `crate::DEFAULT_END_ROUTING_BUCKET` is 1023. At `u32::MAX` every key lands alone by
/// construction and the fill is exactly 1.000 page a bucket at every percentile, so a reader of
/// that output concludes the shipped default IS the single-entry case. It is the opposite case.
/// This test asserts the live constant and the arithmetic that follows from it, so the premise
/// cannot be read off a stale label.
///
/// NOT A TAUTOLOGY, and the control is what makes it one or not. `records / buckets` is arithmetic
/// either way; what is being asserted is its DIRECTION, and the direction REVERSES between the two
/// ranges. The old default is carried as that control: above 1 at the range that ships, far below 1
/// at the range that used to, from the same formula.
#[test]
fn the_common_bucket_does_not_hold_one_entry_at_the_range_that_ships() {
    /// The end bucket a shard loads with today, read from the shipped constant.
    const SHIPPED_END: u32 = crate::DEFAULT_END_ROUTING_BUCKET;
    /// What `load_shard` defaulted to before the default moved. The control.
    const OLD_END: u32 = u32::MAX;

    // The constant itself, bracketed.
    assert_eq!(1023, SHIPPED_END, "the shipped end bucket is {SHIPPED_END} and not 1023");
    assert!(
        SHIPPED_END != 1022 && SHIPPED_END != 1024,
        "the shipped end bucket is {SHIPPED_END}"
    );
    assert_ne!(
        OLD_END, SHIPPED_END,
        "the shipped default is back at the whole keyspace, where every key lands alone and the \
         single-entry bucket IS the common case. Everything this module concludes about the \
         many-entry arm being the common one is then wrong and must be re-measured"
    );

    let shipped_buckets = SHIPPED_END as f64 + 1.0;
    let old_buckets = OLD_END as f64 + 1.0;

    println!("\n=== entries a bucket holds, by routing range, at one page a record ===");
    println!("  {:<34} {:>12} {:>14} {:>16}", "range", "buckets", "4,000 recs", "40,000 recs");
    for (label, buckets) in
        [("0..1023 (ships today)", shipped_buckets), ("0..u32::MAX (the old default)", old_buckets)]
    {
        println!(
            "  {label:<34} {buckets:>12.0} {:>14.4} {:>16.4}",
            4_000.0 / buckets,
            40_000.0 / buckets
        );
    }

    for records in [4_000.0_f64, 40_000.0_f64] {
        let shipped_mean = records / shipped_buckets;
        let old_mean = records / old_buckets;
        // THE CLAIM: at the range that ships, a bucket holds more than one entry on average.
        assert!(
            shipped_mean > 1.0,
            "at {records} records over {shipped_buckets} buckets a bucket holds {shipped_mean:.4} \
             entries, which is not more than one -- the single-entry bucket would be the common \
             case and an inline arm would be carrying the common shape, not the rare one"
        );
        // THE CONTROL: the same formula at the old default goes the OTHER way, so the assertion
        // above is about the range and not about the formula.
        assert!(
            old_mean < 1.0,
            "at {records} records over {old_buckets} buckets a bucket holds {old_mean:.4} entries. \
             The control is supposed to fall on the other side of one; if it does not, the \
             assertion above is satisfied by arithmetic rather than by the range"
        );
    }

    println!(
        "  so the many-entry arm is the common arm at the range that ships, and the handle's {} B \
         is set by it. The single-entry arm is the rare one, and the measured narrowest bucket at \
         40,000 records holds 29 entries.",
        size_of::<BlockIndexMap>()
    );
}

// =============================================================================================
// THE TAIL, as a refusal with a number behind it.
// =============================================================================================

/// NO SINGLE SMALL FIELD IN THE TAIL IS WORTH A BYTE, and emptying the whole tail is worth 8.
///
/// Re-derived here rather than taken on trust: the tail is 6 B inside an 8 B rounding, so dropping
/// `routing_bucket` (4 B), `layout` (1 B) or `flags` (1 B) individually -- or folding `layout` into
/// `flags`' spare bits, which is three of the five bits free -- leaves the node at 88. Only an
/// EMPTY tail crosses.
#[test]
fn only_an_entirely_empty_tail_moves_the_node_and_no_single_tail_field_does() {
    assert_mirrors_track_the_declaration();

    #[allow(dead_code)]
    struct NoRoutingBucket<I> {
        layout: BucketLayoutState,
        flags: BucketFlags,
        ttl_ms: BucketTtl,
        dirty_generation: u64,
        first_dirty_wal_sequence: u64,
        first_dirty_index_log_sequence: u64,
        object_index: ObjectIndex,
        deleted_object_index: DeletedObjectIndex,
        block_index: I,
    }
    #[allow(dead_code)]
    struct LayoutFoldedIntoFlags<I> {
        routing_bucket: u32,
        flags: BucketFlags,
        ttl_ms: BucketTtl,
        dirty_generation: u64,
        first_dirty_wal_sequence: u64,
        first_dirty_index_log_sequence: u64,
        object_index: ObjectIndex,
        deleted_object_index: DeletedObjectIndex,
        block_index: I,
    }
    #[allow(dead_code)]
    struct NoRoutingBucketAndLayoutFolded<I> {
        flags: BucketFlags,
        ttl_ms: BucketTtl,
        dirty_generation: u64,
        first_dirty_wal_sequence: u64,
        first_dirty_index_log_sequence: u64,
        object_index: ObjectIndex,
        deleted_object_index: DeletedObjectIndex,
        block_index: I,
    }

    let rows = [
        ("as declared", size_of::<MirrorNode<BlockIndexMap>>()),
        ("without `routing_bucket` (4 B)", size_of::<NoRoutingBucket<BlockIndexMap>>()),
        (
            "`layout` folded into `flags`' spare bits",
            size_of::<LayoutFoldedIntoFlags<BlockIndexMap>>(),
        ),
        (
            "both: only `flags` left in the tail",
            size_of::<NoRoutingBucketAndLayoutFolded<BlockIndexMap>>(),
        ),
        ("tail emptied entirely", size_of::<MirrorNodeEmptyTail<BlockIndexMap>>()),
    ];
    println!("\n=== the small-field tail, priced ===");
    for (name, width) in &rows {
        println!(
            "  {name:<46} {width:>4} B  {:>6}",
            format!("{:+}", *width as isize - size_of::<BucketNode>() as isize)
        );
    }

    for (name, width) in &rows[..4] {
        assert_eq!(
            size_of::<BucketNode>(),
            *width,
            "`{name}` moved the node to {width} B. The tail is no longer 6 B inside one rounding \
             and this refusal is stale"
        );
    }
    // The bound on the negative: something COULD cross, so the refusal is about the tail's width
    // and not about nothing ever mattering.
    assert_eq!(
        80, rows[4].1,
        "emptying the tail entirely gives {} B; if even that does not cross, this is not \
         describing the rounding it claims to",
        rows[4].1
    );
}
