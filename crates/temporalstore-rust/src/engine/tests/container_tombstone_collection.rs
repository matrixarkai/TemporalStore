// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT MAKES THE TOMBSTONE CODEPOINT FREE, AND WHAT MAKES DROPPING ONE UNSAFE.
//!
//! Four claims that the rest of this change rests on, each driven here rather than argued in a
//! comment. They are grouped because they are the same subject seen from two ends: the first two are
//! why the second page shape costs nothing, and the second two are why a tombstone cannot be
//! collected whenever it is convenient.
//!
//! # 1. AN EMPTY VALUE IS NOT A TOMBSTONE, AND THE BYTES PROVE IT
//!
//! The whole reason a removal costs no per-item flag byte is that the value tag had an unused
//! codepoint: an empty INLINE value would be tag 1, and the encoder never emits it, because
//! `suffix_offset_of(key, b"")` always answers `Some(key.len())` and takes the suffix branch. If that
//! were ever untrue, a tombstone and an element holding the empty string would be the SAME BYTES, and
//! every empty-valued hash field in the store would decode as a removal. So the assertion is on the
//! bytes and not on the round trip: the two encodings must DIFFER.
//!
//! # 2. AND A LIVE SUFFIX PAGE IS THE SAME BYTES IN BOTH SHAPES
//!
//! Asserted by rewriting the magic in place and decoding the result as the first shape. That is a
//! stronger statement than comparing two encoders' lengths, and it is the one that matters for a
//! store being upgraded: the pages a `set` and a `zset` write are byte-identical across the change
//! apart from one character of magic.
//!
//! # 3. THE ORDER IS THE APPEND POSITION, AND THE FOLD HONOURS IT
//!
//! Fed the same pages in the wrong order, the derivation must give the same answer -- it sorts by
//! append position rather than trusting its input. A derivation that happened to be handed pages in
//! order would pass without sorting at all, so the input here is deliberately reversed.
//!
//! # 4. DROPPING A TOMBSTONE WHILE AN OLDER PAGE SURVIVES RESURRECTS ITS ELEMENT
//!
//! This is the trap, and it is driven as a resurrection that actually happens rather than as a rule
//! that forbids it. The naive collection rule -- "the fold rewrote these elements, so their
//! tombstones are spent" -- is applied, the element comes back, and THEN the rule is asked and
//! declines the same round. A guard that only asserted the rule's return value would pass against a
//! rule that was wrong.

#![allow(clippy::all)]
use crate::block_store::BlockAddress;
use crate::engine::container_membership::{append_position, derive_membership, may_drop_tombstones};
use crate::engine::container_pages::{
    decode_container_page, encode_container_page_items, ContainerPageDecode, ContainerPageShape,
    ContainerPageWrite, ElementKeySpelling, CONTAINER_PAGE_MAGIC, CONTAINER_PAGE_MAGIC_V2,
};

/// An address at a chosen append position, which is what orders pages.
///
/// The block id and object id are set because a real page carries both, and a fixture that left them
/// absent would be exercising a shape the write path does not produce. Neither participates in the
/// ordering, which is the point being driven.
fn at(slab: u64, offset: u64, len: u32) -> BlockAddress {
    BlockAddress::from_parts(slab, offset, u64::from(len), Some(0), Some(7))
}

/// rust-internal: operates on the page codec directly
#[test]
fn the_two_shapes_put_their_spelling_byte_at_one_offset() {
    // EVERY CURSOR IN THE CODEC STARTS AT `MAGIC.len() + 1` and there are two magics. If they were
    // ever different lengths, one shape would be read one byte into the wrong field -- and the symptom
    // would be a spelling byte that happens to be a valid spelling, so the page would decode to
    // plausible nonsense rather than refusing. `container_pages` carries a const assert on this; this
    // states it where a reader looking for the guard will find it.
    assert_eq!(
        CONTAINER_PAGE_MAGIC.len(),
        CONTAINER_PAGE_MAGIC_V2.len(),
        "the two magics are different lengths, so the spelling byte is at two offsets"
    );
    assert_ne!(
        CONTAINER_PAGE_MAGIC, CONTAINER_PAGE_MAGIC_V2,
        "the two magics are equal, so nothing DISCRIMINATES the shapes and a tombstone in a new page \
         would be read by the old rule as an empty live value"
    );
    // And they differ in exactly one byte, which is what makes the rewrite in
    // `a_live_suffix_page_is_the_same_bytes_under_either_shape` a magic swap and not a reshaping.
    let differing = CONTAINER_PAGE_MAGIC
        .iter()
        .zip(CONTAINER_PAGE_MAGIC_V2.iter())
        .filter(|(left, right)| left != right)
        .count();
    println!(
        "magics {:?} and {:?} differ in {differing} byte(s)",
        String::from_utf8_lossy(CONTAINER_PAGE_MAGIC),
        String::from_utf8_lossy(CONTAINER_PAGE_MAGIC_V2)
    );
    assert_eq!(1, differing, "the two magics differ in {differing} bytes, not one");
}

/// rust-internal: operates on the page codec directly
#[test]
fn a_removal_that_cannot_be_framed_is_counted_and_writes_no_page() {
    crate::engine::container_pages::reset_unframed_container_removal_count();

    // A kind with no element-key spelling: its page is its whole object, so there is no element to
    // tombstone. `string` is such a kind and is not one of the four containers.
    let none = crate::engine::container_pages::tombstone_page("string", "anything");
    assert!(
        none.is_none(),
        "a kind whose pages name no element produced a tombstone page, which would be a page nothing \
         can interpret"
    );
    // A component the kind's spelling cannot produce: a set's components are hex, and this is not.
    let not_hex = crate::engine::container_pages::tombstone_page("set", "zz-not-hex");
    assert!(
        not_hex.is_none(),
        "a component a set's spelling cannot name produced a tombstone page"
    );

    let counted = crate::engine::container_pages::unframed_container_removal_count();
    println!("unframable removals counted: {counted}");
    assert_eq!(
        2, counted,
        "both unframable removals must be COUNTED and not silently dropped -- a removal the pages do \
         not record is a hole, and this counter is how it announces itself instead of turning up \
         later as a resurrected member"
    );

    // AND THE FOUR REAL KINDS ALL FRAME, so the counter above is a defect signal and not a tolerance.
    crate::engine::container_pages::reset_unframed_container_removal_count();
    let components = [
        ("hash", "some-field".to_string()),
        ("set", hex::encode(b"a-member")),
        ("zset", format!("{:016x}{}", 42u64, hex::encode(b"a-member"))),
        ("list", format!("{:016x}", 7u64)),
    ];
    for (kind, component) in &components {
        let page = crate::engine::container_pages::tombstone_page(kind, component);
        assert!(page.is_some(), "{kind} could not frame a removal of {component}");
        match decode_container_page(page.as_ref().expect("framed")) {
            ContainerPageDecode::Framed { items, shape, .. } => {
                assert_eq!(ContainerPageShape::WithRemovals, shape, "{kind}: wrong shape");
                assert_eq!(1, items.len(), "{kind}: wrong item count");
                assert!(items[0].deleted, "{kind}: the tombstone item is not marked removed");
            }
            other => panic!("{kind} tombstone did not decode: {other:?}"),
        }
    }
    assert_eq!(
        4,
        components.len(),
        "DENOMINATOR: all four container kinds must be covered"
    );
    assert_eq!(
        0,
        crate::engine::container_pages::unframed_container_removal_count(),
        "one of the four real kinds could not frame a removal"
    );
}

/// rust-internal: drives the page-derived membership fold
#[test]
fn an_older_page_is_not_outranked_by_a_page_that_merely_does_not_mention_the_element() {
    // THE DISTINCTION THE WHOLE DESIGN TURNS ON. A later page that SAYS NOTHING about an element must
    // leave the earlier page's statement standing; only a page that says REMOVED may overrule it. Get
    // this wrong in the permissive direction and every fold silently empties the containers it did
    // not touch; get it wrong in the strict direction and no removal ever takes effect.
    let member = b"m".to_vec();
    let component = hex::encode(&member);
    let unrelated = b"other".to_vec();

    let older = encode_container_page_items(
        ElementKeySpelling::Hex,
        &[ContainerPageWrite::live(&member, &member)],
    );
    // A LATER page that is perfectly well formed and simply holds a different element.
    let silent = encode_container_page_items(
        ElementKeySpelling::Hex,
        &[ContainerPageWrite::live(&unrelated, &unrelated)],
    );
    let tomb = encode_container_page_items(
        ElementKeySpelling::Hex,
        &[ContainerPageWrite::removed(&member)],
    );

    let older_at = at(1, 100, older.len() as u32);
    let silent_at = at(1, 200, silent.len() as u32);
    let tomb_at = at(1, 300, tomb.len() as u32);
    let bytes = vec![
        (older_at.clone(), older.clone()),
        (silent_at.clone(), silent.clone()),
        (tomb_at.clone(), tomb.clone()),
    ];
    let read = |set: Vec<(BlockAddress, Vec<u8>)>| {
        move |address: &BlockAddress| -> Option<Vec<u8>> {
            set.iter()
                .find(|(candidate, _)| append_position(candidate) == append_position(address))
                .map(|(_, page)| page.clone())
        }
    };

    // SILENCE DOES NOT REMOVE.
    let with_silence = derive_membership(
        "set",
        vec![older_at.clone(), silent_at.clone()],
        read(bytes.clone()),
    );
    println!(
        "older + a later page that does not mention it: {} live, {} removed",
        with_silence.live.len(),
        with_silence.removed.len()
    );
    assert_eq!(2, with_silence.pages_read, "DENOMINATOR: two pages must be read");
    assert!(with_silence.is_complete());
    assert!(
        with_silence.live.contains_key(&component),
        "A LATER PAGE THAT DOES NOT MENTION THE ELEMENT REMOVED IT. Silence is not a removal: a fold \
         writes pages that hold only the elements it batched, so treating an unmentioned element as \
         gone would empty every container a round did not fully rewrite."
    );
    assert!(with_silence.removed.is_empty());

    // A REMOVAL DOES.
    let with_removal = derive_membership(
        "set",
        vec![older_at, silent_at, tomb_at],
        read(bytes.clone()),
    );
    println!(
        "older + silent + a removal: {} live, {} removed",
        with_removal.live.len(),
        with_removal.removed.len()
    );
    assert_eq!(3, with_removal.pages_read, "DENOMINATOR: three pages must be read");
    assert!(with_removal.is_complete());
    assert!(
        with_removal.removed.contains(&component),
        "a page that states the removal did not remove the element"
    );
    assert!(!with_removal.live.contains_key(&component));
    // The unrelated element is untouched by either -- a control, so that "removed" above is not the
    // fold simply losing everything.
    let unrelated_component = hex::encode(&unrelated);
    assert!(
        with_removal.live.contains_key(&unrelated_component),
        "CONTROL: the element nothing removed is gone too, so the fold is losing items rather than \
         applying a tombstone"
    );
}

/// rust-internal: operates on the page codec directly
#[test]
fn an_empty_value_and_a_removal_of_the_same_element_are_different_bytes() {
    let key = b"field-with-no-value".to_vec();

    let empty = encode_container_page_items(
        ElementKeySpelling::Utf8,
        &[ContainerPageWrite::live(&key, b"")],
    );
    let removed = encode_container_page_items(
        ElementKeySpelling::Utf8,
        &[ContainerPageWrite::removed(&key)],
    );

    println!("empty-valued page:  {} bytes {:?}", empty.len(), &empty[7..]);
    println!("tombstone page:     {} bytes {:?}", removed.len(), &removed[7..]);

    // DENOMINATOR: both must actually be frames, or "they differ" could be two failures.
    assert!(
        empty.starts_with(CONTAINER_PAGE_MAGIC_V2) && removed.starts_with(CONTAINER_PAGE_MAGIC_V2),
        "one of the two is not a frame of the second shape at all"
    );
    assert_ne!(
        empty, removed,
        "AN EMPTY VALUE AND A REMOVAL ENCODE THE SAME. That is the one thing that cannot be true: \
         the removal codepoint is free only because the encoder never emits an empty INLINE value, \
         and if it does then every empty-valued hash field in the store decodes as a removal."
    );

    // And each says what it is, which is the round trip the byte comparison above does not cover.
    for (bytes, expect_deleted, label) in
        [(&empty, false, "empty value"), (&removed, true, "removal")]
    {
        match decode_container_page(bytes) {
            ContainerPageDecode::Framed { items, shape, .. } => {
                assert_eq!(ContainerPageShape::WithRemovals, shape, "{label}: wrong shape");
                assert_eq!(1, items.len(), "{label}: wrong item count");
                assert_eq!(key, items[0].key, "{label}: wrong key");
                assert_eq!(
                    expect_deleted, items[0].deleted,
                    "{label} decoded with deleted = {}",
                    items[0].deleted
                );
                assert!(
                    items[0].value.is_empty(),
                    "{label}: both carry no value bytes, and this one carried {}",
                    items[0].value.len()
                );
            }
            other => panic!("{label} did not decode: {other:?}"),
        }
    }
}

/// rust-internal: operates on the page codec directly
#[test]
fn a_live_suffix_page_is_the_same_bytes_under_either_shape() {
    // A set member: the element key IS the member and so is the value, which is the suffix case --
    // and the case that covers the two kinds whose pages dominate the store.
    let member = b"member-0007".to_vec();
    let page = encode_container_page_items(
        ElementKeySpelling::Hex,
        &[ContainerPageWrite::live(&member, &member)],
    );
    assert!(page.starts_with(CONTAINER_PAGE_MAGIC_V2));

    // The SAME BYTES with the magic rewritten. If the two shapes disagreed anywhere in the body for
    // a live suffix item, this would not decode, or would decode to different items.
    let mut as_first_shape = page.clone();
    as_first_shape[..CONTAINER_PAGE_MAGIC.len()].copy_from_slice(CONTAINER_PAGE_MAGIC);
    assert_eq!(
        page.len(),
        as_first_shape.len(),
        "rewriting the magic changed the length, which it cannot"
    );

    let second = decode_container_page(&page);
    let first = decode_container_page(&as_first_shape);
    println!("second shape: {second:?}");
    println!("first shape:  {first:?}");

    match (second, first) {
        (
            ContainerPageDecode::Framed {
                items: new_items,
                shape: new_shape,
                spelling: new_spelling,
            },
            ContainerPageDecode::Framed {
                items: old_items,
                shape: old_shape,
                spelling: old_spelling,
            },
        ) => {
            assert_eq!(ContainerPageShape::WithRemovals, new_shape);
            assert_eq!(ContainerPageShape::LiveOnly, old_shape);
            assert_eq!(new_spelling, old_spelling, "the spelling byte moved");
            assert_eq!(
                new_items, old_items,
                "a live suffix item does not decode the same under the two shapes, so the second \
                 shape is not free for the kinds whose values are their keys' tails"
            );
            assert_eq!(1, new_items.len());
            assert!(!new_items[0].deleted);
        }
        (second, first) => panic!("one shape did not decode: second={second:?} first={first:?}"),
    }
}

/// rust-internal: drives the page-derived membership fold
#[test]
fn the_fold_orders_pages_by_append_position_and_not_by_the_order_it_was_handed_them() {
    let member = b"m".to_vec();
    let live = encode_container_page_items(
        ElementKeySpelling::Hex,
        &[ContainerPageWrite::live(&member, &member)],
    );
    let tomb = encode_container_page_items(
        ElementKeySpelling::Hex,
        &[ContainerPageWrite::removed(&member)],
    );

    let earlier = at(1, 100, live.len() as u32);
    let later = at(1, 200, tomb.len() as u32);
    // THE ORDER IS A FACT ABOUT THE ADDRESSES, asserted before it is relied on.
    assert!(
        append_position(&earlier) < append_position(&later),
        "the later page does not compare greater, so nothing below is testing ordering"
    );
    // AND ACROSS A SLAB ROLL, which is the case a single-slab fixture would not cover and the one a
    // scalar packing of the pair would get wrong. The offset is the HIGHEST one an address can name,
    // so the comparison is made at the boundary rather than somewhere comfortably inside it --
    // `from_parts` panics above it, which is itself the proof that this is the extreme case.
    assert!(
        append_position(&at(1, crate::block_store::MAX_ADDRESSABLE_BLOCK_OFFSET, 1))
            < append_position(&at(2, 0, 1)),
        "a page in a later slab does not compare greater than one at the highest offset of an \
         earlier slab, so the pair is not ordering slab before offset"
    );

    // The reader is keyed by POSITION rather than by a captured address, so it borrows nothing the
    // two calls below need to own. Built fresh per call for the same reason.
    let earlier_position = append_position(&earlier);
    let read = || {
        let live = live.clone();
        let tomb = tomb.clone();
        move |address: &BlockAddress| -> Option<Vec<u8>> {
            if append_position(address) == earlier_position {
                Some(live.clone())
            } else {
                Some(tomb.clone())
            }
        }
    };

    // HANDED IN THE WRONG ORDER ON PURPOSE. A fold that trusted its input would answer "live".
    let reversed = derive_membership("set", vec![later.clone(), earlier.clone()], read());
    let forwards = derive_membership("set", vec![earlier, later], read());

    let component = hex::encode(&member);
    println!(
        "reversed input: {} live, {} removed; forwards: {} live, {} removed",
        reversed.live.len(),
        reversed.removed.len(),
        forwards.live.len(),
        forwards.removed.len()
    );
    assert_eq!(2, reversed.pages_read, "DENOMINATOR: both pages must be read");
    assert!(reversed.is_complete() && forwards.is_complete());
    assert!(
        reversed.removed.contains(&component),
        "handed the tombstone FIRST, the fold kept the member -- so it applies items in the order it \
         receives them rather than in append order"
    );
    assert!(!reversed.live.contains_key(&component));
    assert_eq!(
        forwards.removed, reversed.removed,
        "the two input orders disagree, which is the only thing the sort exists to prevent"
    );
}

/// rust-internal: drives the page-derived membership fold
#[test]
fn a_tombstone_dropped_while_an_older_page_survives_resurrects_its_element() {
    let member = b"m".to_vec();
    let component = hex::encode(&member);
    let other = b"n".to_vec();

    let older = encode_container_page_items(
        ElementKeySpelling::Hex,
        &[ContainerPageWrite::live(&member, &member)],
    );
    let tomb = encode_container_page_items(
        ElementKeySpelling::Hex,
        &[ContainerPageWrite::removed(&member)],
    );
    // What a fold writes when it rewrites the tombstone's page and DROPS the tombstone: a page
    // holding the round's other element and saying nothing about `m`.
    let folded_without_tombstone = encode_container_page_items(
        ElementKeySpelling::Hex,
        &[ContainerPageWrite::live(&other, &other)],
    );

    let older_at = at(1, 100, older.len() as u32);
    let tomb_at = at(1, 200, tomb.len() as u32);
    let folded_at = at(1, 300, folded_without_tombstone.len() as u32);

    let pages = vec![
        (older_at.clone(), older.clone()),
        (tomb_at.clone(), tomb.clone()),
        (folded_at.clone(), folded_without_tombstone.clone()),
    ];
    let read_from = |set: Vec<(BlockAddress, Vec<u8>)>| {
        move |address: &BlockAddress| -> Option<Vec<u8>> {
            set.iter()
                .find(|(candidate, _)| append_position(candidate) == append_position(address))
                .map(|(_, bytes)| bytes.clone())
        }
    };

    // BEFORE: the tombstone is there and the member is gone. This is the state the rule protects.
    let before = derive_membership(
        "set",
        vec![older_at.clone(), tomb_at.clone()],
        read_from(pages.clone()),
    );
    println!(
        "with the tombstone:    {} live, {} removed, {} pages",
        before.live.len(),
        before.removed.len(),
        before.pages_read
    );
    assert_eq!(2, before.pages_read, "DENOMINATOR: two pages must be read");
    assert!(before.is_complete());
    assert!(
        before.removed.contains(&component),
        "the member is not removed even with its tombstone present, so the fixture is wrong"
    );

    // AFTER THE NAIVE RULE: the round rewrote the tombstone's page and dropped the tombstone, while
    // the OLDER page was not relocated and still names the member.
    let after = derive_membership(
        "set",
        vec![older_at.clone(), folded_at.clone()],
        read_from(pages.clone()),
    );
    println!(
        "tombstone dropped:     {} live, {} removed, {} pages; member live again = {}",
        after.live.len(),
        after.removed.len(),
        after.pages_read,
        after.live.contains_key(&component)
    );
    assert_eq!(2, after.pages_read, "DENOMINATOR: two pages must be read");
    assert!(after.is_complete());
    assert!(
        after.live.contains_key(&component),
        "the resurrection did not happen, so this guard is not demonstrating the defect it exists \
         for -- and a rule that forbids a harmless thing is not evidence of anything"
    );
    assert!(
        after.removed.is_empty(),
        "the pages still record a removal after the tombstone was dropped, which they cannot"
    );

    // AND THE RULE DECLINES EXACTLY THAT ROUND. Three pages in the container, one rewritten.
    assert!(
        !may_drop_tombstones(3, 1, 1),
        "the rule permits dropping a tombstone in a round that rewrote one of three pages, which is \
         the round just shown to resurrect"
    );
    // Nor when every page is rewritten but into SEVERAL pages: a tombstone dropped into the first
    // batch is not seen by the second.
    assert!(
        !may_drop_tombstones(3, 3, 2),
        "the rule permits dropping a tombstone in a round that split the container across two pages"
    );
    // And it PERMITS the total rewrite, which is the comparison design's own answer: the tombstone
    // goes once nothing older survives to name the element.
    assert!(
        may_drop_tombstones(3, 3, 1),
        "the rule forbids dropping a tombstone even when the whole container was rewritten into one \
         page, which would mean tombstones are never collectable at all"
    );
    // The denominator case: a container with no pages must not read as a total rewrite.
    assert!(
        !may_drop_tombstones(0, 0, 1),
        "a round over a container with no pages reads as a total rewrite"
    );
}
