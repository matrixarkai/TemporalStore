// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT IT COSTS FOR A PAGE TO STATE WHICH ELEMENT IT HOLDS, MEASURED PER KIND.
//!
//! # THE CLAIM THIS MODULE WAS OPENED ON
//!
//! A container page used to carry a value and nothing else. `container_pages` puts a frame round it
//! that names the element, which costs bytes on every container write and changes no answer. This
//! module is where the cost is measured and where "changes no answer" is asserted rather than
//! assumed.
//!
//! THE COST IS NOT ONE NUMBER, and that is the finding. The element key's relationship to the value
//! is different for each of the four kinds:
//!
//! ```text
//!     hash   key = the field name      value = the field value    unrelated
//!     set    key = the member          value = the member         value IS the key
//!     zset   key = score ++ member     value = the member         value is the key's tail
//!     list   key = the sequence word   value = the element        unrelated
//! ```
//!
//! So a set page can name its element for the price of the framing alone, a zset page for the
//! framing plus its eight-byte score, and only `hash` and `list` pay for a second copy of anything.
//! `the_framing_cost_of_naming_an_element_differs_by_kind` prints the ladder that shows it, per kind
//! and per width, with the denominator on every row.
//!
//! # THE THREE THINGS THAT COULD GO WRONG, AND THE CONTROL FOR EACH
//!
//! ## The frame could be unreadable, and a reader would serve framing bytes as data
//!
//! That is #2016's shape exactly -- an unreadable name becoming a real one -- and it is why
//! `decode_container_page` has a `Corrupt` arm distinct from `NotFramed` rather than falling back.
//! `a_frame_that_cannot_be_walked_is_not_mistaken_for_a_value` drives eleven separate mutilations of
//! a real frame and asserts every one of them is refused.
//!
//! ## The selection could be vacuous, answering correctly for the wrong reason
//!
//! A selector that ignored the component and returned the first item would pass every happy-path
//! test in this file, because at one item per page the first item IS the one asked for. So the
//! controls are a frame holding TWO elements -- asked for each in turn, and asked for a third that
//! is not there -- and a frame whose key spelling is deliberately wrong for the component being
//! looked up. #2016's first mutation run had the defect SURVIVE on a one-key fixture; this module
//! does not own a one-key fixture.
//!
//! ## An old page could stop reading
//!
//! No version stamp moves in this stage, and the only thing standing behind that is the magic
//! discriminating. `a_page_written_without_a_frame_still_reads_through_the_funnel` installs bare
//! bytes into a slab, files an index entry that names them, and reads them back through the product
//! path -- so the compatibility claim is exercised rather than argued.
//!
//! # WHAT THIS MODULE DOES NOT COVER
//!
//! Batching. Every frame written by the product today holds exactly ONE item, because the write
//! sites append one element at a time; the two-item frames here are built directly. The per-item
//! marginal cost measured below is therefore an arithmetic projection of what batching would save
//! and is labelled as one, not a measurement of a batched write.
//!
//! THE STORE PATH LENGTH is held equal across every arm of the end-to-end test and asserted equal:
//! it moves allocation bytes at about six bytes a character, so an arm on a longer temporary
//! directory would read as a heavier representation.
#![allow(clippy::all)]
use super::*;
use crate::engine::container_pages::{
    component_from_element_key, decode_container_page, element_key_from_component,
    encode_container_page, select_container_element, varint_len, ContainerElementRead,
    ContainerPageDecode, ContainerPageItem, ElementKeySpelling, CONTAINER_PAGE_MAGIC,
};

/// Every spelling, so a test over "all of them" cannot silently stop covering one that is added.
const EVERY_SPELLING: [ElementKeySpelling; 4] = [
    ElementKeySpelling::Utf8,
    ElementKeySpelling::Hex,
    ElementKeySpelling::ScoreThenMember,
    ElementKeySpelling::BiasedWord,
];

/// Every kind that frames its pages, beside the spelling it uses.
const EVERY_FRAMED_KIND: [(&str, ElementKeySpelling); 4] = [
    ("hash", ElementKeySpelling::Utf8),
    ("set", ElementKeySpelling::Hex),
    ("zset", ElementKeySpelling::ScoreThenMember),
    ("list", ElementKeySpelling::BiasedWord),
];

fn engine_on(dir: &std::path::Path) -> TemporalEngine {
    TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("pages"),
        dir.join("indexes"),
    )
}

fn load_on(engine: &TemporalEngine) {
    let response = engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 1,
        table_name: "container-page-element-key".to_string(),
        shard_uri: "local://container-page-element-key/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket: 1023,
        readonly: false,
        load_version: 1,
        local_node_id: Some(1),
    });
    assert!(
        response.status.ok,
        "the fixture could not load shard 1: {:?}",
        response.status
    );
}

fn write(engine: &TemporalEngine, command: Command) {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(response.status.ok, "the fixture write failed: {response:?}");
}

fn read(engine: &TemporalEngine, command: Command) -> crate::types::CommandResponse {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(response.status.ok, "a read failed: {response:?}");
    response.response
}

/// A value of exactly `width` bytes, distinct per index.
fn bytes_of(width: usize, index: usize) -> Vec<u8> {
    let mut bytes = vec![0u8; width];
    let stamp = format!("{index:08}");
    for (slot, byte) in stamp.as_bytes().iter().enumerate() {
        if slot < width {
            bytes[slot] = *byte;
        }
    }
    for slot in stamp.len()..width {
        bytes[slot] = (index as u8).wrapping_mul(31).wrapping_add(slot as u8);
    }
    bytes
}

/// A component this spelling can produce, built from a member of the given width.
///
/// Built THROUGH the spelling rather than spelled by hand: a hand-written component would be a
/// second implementation of the thing under test, and #1863's lesson about hand-written subject
/// lists applies to hand-written fixtures of an encoding just as well.
fn component_for(spelling: ElementKeySpelling, width: usize, index: usize) -> String {
    let key = match spelling {
        ElementKeySpelling::Utf8 => format!("field-{index:04}").into_bytes(),
        ElementKeySpelling::Hex => bytes_of(width, index),
        ElementKeySpelling::ScoreThenMember => {
            let mut key = Vec::new();
            key.extend_from_slice(&(0x8000_0000_0000_0000_u64 + index as u64).to_be_bytes());
            key.extend_from_slice(&bytes_of(width, index));
            key
        }
        ElementKeySpelling::BiasedWord => (index as u64).to_be_bytes().to_vec(),
    };
    component_from_element_key(spelling, &key)
        .expect("a key built by this fixture spells a component")
}

// =================================================================================================
// 1. THE SPELLINGS ARE INVERSES, WHICH IS THE WHOLE SAFETY ARGUMENT
// =================================================================================================

/// A COMPONENT SURVIVES BEING TURNED INTO A PAGE KEY AND BACK, FOR EVERY SPELLING.
///
/// This is the property the rest of the program rests on. If the page key and the index component
/// are two spellings of one value then either can be derived from the other, and a later stage may
/// stop storing whichever it prefers. If they were merely written consistently at the same instant
/// by one call site -- which is how a set's component and its payload agree TODAY, with nothing
/// re-checking them -- then the second copy would be an unverified duplicate and deleting the first
/// would be unsafe.
///
/// THE DENOMINATOR IS PRINTED AND FLOORED. A loop over an empty ladder asserts nothing and reports
/// success, which is how two threads were nearly defeated in one day.
///
/// rust-internal: exercises the page framing directly
#[test]
fn a_component_survives_the_round_trip_to_a_page_key_for_every_spelling() {
    let widths = [0usize, 1, 8, 16, 64, 255, 256];
    let mut checked = 0usize;
    println!("\n=== component -> key -> component, every spelling ===");
    println!(
        "  {:>18}  {:>6}  {:>9}  {:>9}",
        "spelling", "width", "comp len", "key len"
    );
    for spelling in EVERY_SPELLING {
        for width in widths {
            // A biased word is eight bytes by definition, so the width ladder does not apply to it;
            // one row is all there is and the fixture says so rather than skipping in silence.
            if matches!(spelling, ElementKeySpelling::BiasedWord) && width != 8 {
                continue;
            }
            for index in [0usize, 1, 7] {
                let component = component_for(spelling, width, index);
                let key = element_key_from_component(spelling, &component)
                    .expect("a component this spelling produced is one it can read back");
                let again = component_from_element_key(spelling, &key)
                    .expect("a key this spelling produced spells a component");
                assert_eq!(
                    component, again,
                    "{spelling:?} did not round trip a component of width {width}"
                );
                if index == 0 {
                    println!(
                        "  {:>18}  {:>6}  {:>9}  {:>9}",
                        format!("{spelling:?}"),
                        width,
                        component.len(),
                        key.len()
                    );
                }
                checked += 1;
            }
        }
    }
    assert!(
        checked >= 4 * 3,
        "the round trip exercised {checked} components, which is too few to cover four spellings"
    );
    println!("  round trips checked: {checked}");
}

/// SIXTEEN CHARACTERS IS A WHOLE ZSET COMPONENT AND NOT A TRUNCATED ONE.
///
/// A member of zero bytes spells exactly sixteen characters, and `hex::decode("")` is
/// `Ok(vec![])`. Asking `<= 16` instead of `< 16` makes that member unaddressable -- the defect
/// #2016 fixed on the reconcile's own zset arm, recorded here because this module introduces a
/// SECOND reader of the same encoding and two readers of one encoding disagreeing about its
/// shortest legal form is the shape of that defect.
///
/// rust-internal: exercises the page framing directly
#[test]
fn a_zset_component_of_exactly_sixteen_characters_names_an_empty_member() {
    let component = format!("{:016x}", 0x8000_0000_0000_0000_u64);
    assert_eq!(16, component.len());
    let key = element_key_from_component(ElementKeySpelling::ScoreThenMember, &component)
        .expect("sixteen characters is a complete score with an empty member after it");
    assert_eq!(8, key.len(), "the key is the score word and nothing else");
    assert_eq!(
        Some(component.clone()),
        component_from_element_key(ElementKeySpelling::ScoreThenMember, &key)
    );

    // And fifteen is genuinely short, so the boundary is tested from both sides rather than
    // asserted from one.
    let short = &component[..15];
    assert_eq!(
        None,
        element_key_from_component(ElementKeySpelling::ScoreThenMember, short),
        "fifteen characters cannot be a score"
    );
}

/// A COMPONENT A SPELLING CANNOT PRODUCE IS REFUSED RATHER THAN COERCED.
///
/// The control on the round trip above: it would pass just as well if every spelling accepted
/// everything, so the refusals have to be asserted too. Each row below is a component that is
/// well formed for SOME spelling and not for the one being asked.
///
/// rust-internal: exercises the page framing directly
#[test]
fn a_component_the_spelling_cannot_produce_is_refused() {
    // Not hex at all.
    assert_eq!(
        None,
        element_key_from_component(ElementKeySpelling::Hex, "field-0001")
    );
    // Hex of odd length.
    assert_eq!(
        None,
        element_key_from_component(ElementKeySpelling::Hex, "abc")
    );
    // A list word is exactly sixteen characters.
    assert_eq!(
        None,
        element_key_from_component(ElementKeySpelling::BiasedWord, "8000000000000000ff")
    );
    assert_eq!(
        None,
        element_key_from_component(ElementKeySpelling::BiasedWord, "800000000000000")
    );
    // A zset component's score half has to be hex.
    assert_eq!(
        None,
        element_key_from_component(ElementKeySpelling::ScoreThenMember, "zzzzzzzzzzzzzzzz")
    );
    // A key that is not eight bytes cannot spell a list word.
    assert_eq!(
        None,
        component_from_element_key(ElementKeySpelling::BiasedWord, &[0u8; 7])
    );
    // A hash field key that is not valid UTF-8 cannot spell a component.
    assert_eq!(
        None,
        component_from_element_key(ElementKeySpelling::Utf8, &[0xff, 0xfe])
    );
    // And a UTF-8 field name CAN be empty, which is legal and must not be refused -- #2016's
    // finding was that an ABSENT name must not become the EMPTY one, not that the empty one is
    // illegal.
    assert_eq!(
        Some(String::new()),
        component_from_element_key(ElementKeySpelling::Utf8, b"")
    );
}

// =================================================================================================
// 2. THE FRAME ROUND TRIPS, AND THE SUFFIX ELISION IS MEASURED RATHER THAN ASSUMED
// =================================================================================================

/// A FRAME DECODES TO THE ITEMS IT WAS BUILT FROM, INCLUDING A VALUE THAT IS THE KEY'S TAIL.
///
/// Two items and not one, deliberately: at one item per page a decoder that returned only the first
/// would pass, and that is the shape #2016's first mutation run survived on.
///
/// rust-internal: exercises the page framing directly
#[test]
fn a_frame_decodes_to_the_items_it_was_built_from() {
    let mut cases = 0usize;
    for spelling in EVERY_SPELLING {
        let first_key = element_key_from_component(spelling, &component_for(spelling, 16, 1))
            .expect("fixture component");
        let second_key = element_key_from_component(spelling, &component_for(spelling, 16, 2))
            .expect("fixture component");
        // The second item's value is its key's tail, so both branches of the value encoding are
        // exercised inside ONE frame.
        let first_value = bytes_of(24, 9);
        let second_value = second_key.clone();
        let page = encode_container_page(
            spelling,
            &[
                (&first_key, first_value.as_slice()),
                (&second_key, second_value.as_slice()),
            ],
        );
        match decode_container_page(&page) {
            ContainerPageDecode::Framed {
                spelling: seen,
                items,
            } => {
                assert_eq!(spelling, seen, "the frame reports the spelling it was written with");
                assert_eq!(
                    vec![
                        ContainerPageItem {
                            key: first_key.clone(),
                            value: first_value.clone()
                        },
                        ContainerPageItem {
                            key: second_key.clone(),
                            value: second_value.clone()
                        },
                    ],
                    items,
                    "{spelling:?} did not round trip its items"
                );
            }
            other => panic!("{spelling:?} frame did not decode: {other:?}"),
        }
        cases += 1;
    }
    assert_eq!(4, cases, "every spelling must be covered");
}

/// A VALUE THAT IS THE KEY'S TAIL IS NOT STORED TWICE, AND ONE THAT IS NOT IS.
///
/// Measured in BYTES off the encoder rather than read off the encoder's source. The saving is the
/// whole reason the frame is affordable for `set`, so it is the one property of the format that has
/// to be a measurement.
///
/// rust-internal: exercises the page framing directly
#[test]
fn a_value_that_is_the_keys_tail_is_stored_once() {
    let key = bytes_of(32, 3);

    // The value IS the key.
    let shared = encode_container_page(ElementKeySpelling::Hex, &[(&key, key.as_slice())]);
    // The value is UNRELATED to the key, and the same length.
    let distinct_value = bytes_of(32, 4);
    assert_ne!(key, distinct_value, "the fixture's two values must differ");
    let separate =
        encode_container_page(ElementKeySpelling::Hex, &[(&key, distinct_value.as_slice())]);

    println!("\n=== the suffix elision, in bytes ===");
    println!("  key 32 bytes, value 32 bytes");
    println!("  value is the key      : {} bytes", shared.len());
    println!("  value is its own      : {} bytes", separate.len());
    println!("  saved                 : {} bytes", separate.len() - shared.len());
    // THE SAVING IS THE VALUE MINUS THE OFFSET VARINT, and it is stated from those two numbers
    // rather than as a round figure: the elided form spends one varint saying where in the key the
    // value starts, so eliding thirty-two bytes saves thirty-one and not thirty-two. A test that
    // asserted the round figure would be off by exactly the byte the format costs, which is the
    // byte most worth being able to see move.
    let expected_saving = 32 - varint_len(0);
    assert_eq!(
        expected_saving,
        separate.len() - shared.len(),
        "eliding a 32-byte value that equals its key should save the value less the offset varint"
    );
    // And the elided form still decodes to the value, so the saving is not a loss.
    assert_eq!(
        ContainerElementRead::Found(key.clone()),
        select_container_element(&shared, &hex::encode(&key))
    );
    assert_eq!(
        ContainerElementRead::Found(distinct_value),
        select_container_element(&separate, &hex::encode(&key))
    );

    // An EMPTY value is the tail beginning at the key's end, so it needs no case of its own.
    let empty = encode_container_page(ElementKeySpelling::Hex, &[(&key, b"")]);
    assert_eq!(
        ContainerElementRead::Found(Vec::new()),
        select_container_element(&empty, &hex::encode(&key)),
        "an empty value must decode back to empty and not to the key"
    );
}

/// THE FRAMING COST OF NAMING AN ELEMENT, PER KIND AND PER WIDTH.
///
/// The headline measurement of this stage, and the number that decides whether the rest of the
/// program is worth building. Printed as a ladder with the denominator on every row, because a mean
/// over four kinds whose keys relate to their values in four different ways would say nothing about
/// any of them.
///
/// THE PER-ITEM MARGINAL FIGURE IS AN ARITHMETIC PROJECTION AND IS LABELLED ONE. Nothing in the
/// product writes a frame of more than one item yet; the two-item frame here is built directly, and
/// the difference between it and the one-item frame is what a second element would cost a batched
/// page. That is a fact about the FORMAT, which is what this stage ships.
///
/// rust-internal: exercises the page framing directly
#[test]
fn the_framing_cost_of_naming_an_element_differs_by_kind() {
    let widths = [8usize, 16, 32, 64, 256];
    println!("\n=== bytes a page pays to name its own element ===");
    println!(
        "  {:>6}  {:>6}  {:>9}  {:>7}  {:>7}  {:>8}  {:>10}  {:>10}",
        "kind", "width", "bare page", "framed", "cost", "cost %", "2nd item", "at 128"
    );
    let mut rows = 0usize;
    let mut worst_share = 0.0_f64;
    for (kind, spelling) in EVERY_FRAMED_KIND {
        for width in widths {
            // What the product writes as the value for this kind, and what it names the element.
            let component = component_for(spelling, width, 1);
            let key = element_key_from_component(spelling, &component).expect("fixture component");
            let value: Vec<u8> = match kind {
                // A set's page holds the member; a zset's holds the member too.
                "set" | "zset" => bytes_of(width, 1),
                // A hash's page holds the field value and a list's holds the element; neither is
                // related to the key, so both are their own bytes.
                _ => bytes_of(width, 1),
            };
            // For set and zset the value the product writes IS the member, which is the key or its
            // tail, so the fixture must use the same bytes or the elision would not fire and the
            // measured cost would be the cost of a shape the product never writes.
            let value: Vec<u8> = match spelling {
                ElementKeySpelling::Hex => key.clone(),
                ElementKeySpelling::ScoreThenMember => key[8..].to_vec(),
                _ => value,
            };

            let bare = value.len();
            let framed = encode_container_page(spelling, &[(&key, value.as_slice())]).len();
            let second_key = element_key_from_component(spelling, &component_for(spelling, width, 2))
                .expect("fixture component");
            let second_value: Vec<u8> = match spelling {
                ElementKeySpelling::Hex => second_key.clone(),
                ElementKeySpelling::ScoreThenMember => second_key[8..].to_vec(),
                _ => bytes_of(width, 2),
            };
            let two = encode_container_page(
                spelling,
                &[
                    (&key, value.as_slice()),
                    (&second_key, second_value.as_slice()),
                ],
            )
            .len();
            let marginal = two - framed;
            // What a 128-item page would average per element, projected from the marginal item.
            let at_128 = (framed + 127 * marginal) as f64 / 128.0;
            let cost = framed as f64 - bare as f64;
            let share = cost / bare as f64 * 100.0;
            worst_share = worst_share.max(share);
            println!(
                "  {kind:>6}  {width:>6}  {bare:>9}  {framed:>7}  {:>7}  {:>7.1}%  {marginal:>10}  {at_128:>10.1}",
                framed as i64 - bare as i64,
                share
            );
            assert!(
                framed > bare,
                "a frame cannot be smaller than the value it wraps"
            );
            // The marginal item must be cheaper than the first, or batching buys nothing.
            assert!(
                marginal < framed,
                "{kind} at width {width}: a second item cost {marginal} against a first at {framed}"
            );
            rows += 1;
        }
    }
    assert_eq!(
        EVERY_FRAMED_KIND.len() * widths.len(),
        rows,
        "the ladder must cover every kind at every width"
    );
    println!("  rows measured: {rows}, worst single-item cost share {worst_share:.1}%");
}

/// WHAT THE FIXED HEADER IS, STATED FROM THE DATA RATHER THAN FROM A CONSTANT.
///
/// A test that cannot say what a width should be from the values that went in cannot notice when it
/// is wrong -- the reason `block_record_varint_len` exists beside the record header.
///
/// rust-internal: exercises the page framing directly
#[test]
fn a_one_item_frame_is_exactly_its_parts() {
    let key = b"field-0001";
    let value = bytes_of(40, 1);
    let page = encode_container_page(ElementKeySpelling::Utf8, &[(key.as_slice(), &value)]);
    let expected = CONTAINER_PAGE_MAGIC.len()
        + 1
        + varint_len(1)
        + varint_len(key.len() as u64)
        + key.len()
        + varint_len(value.len() as u64 + 1)
        + value.len();
    assert_eq!(
        expected,
        page.len(),
        "a one-item frame is the magic, the spelling byte, the count, the key and the value"
    );
    println!(
        "\n=== a one-item hash frame ===\n  magic {} + spelling 1 + count {} + key len {} + key {} + value tag {} + value {} = {}",
        CONTAINER_PAGE_MAGIC.len(),
        varint_len(1),
        varint_len(key.len() as u64),
        key.len(),
        varint_len(value.len() as u64 + 1),
        value.len(),
        page.len()
    );
}

// =================================================================================================
// 3. SELECTION IS REAL, AND ITS FAILURES ARE TOLD APART
// =================================================================================================

/// A FRAME HOLDING TWO ELEMENTS ANSWERS FOR EACH OF THEM AND FOR NEITHER OF THE OTHERS.
///
/// The control that catches a selector which ignores the component. Asked for the first item it must
/// answer the first value, asked for the second the second, and asked for an element that is not
/// there it must answer `Absent` -- which is a different answer from `Corrupt`, because the caller
/// treats one as a miss and the other as a page it must not trust.
///
/// rust-internal: exercises the page framing directly
#[test]
fn a_two_element_frame_answers_for_each_element_and_absent_for_a_third() {
    let mut covered = 0usize;
    for spelling in EVERY_SPELLING {
        let first_component = component_for(spelling, 16, 1);
        let second_component = component_for(spelling, 16, 2);
        let absent_component = component_for(spelling, 16, 3);
        assert_ne!(first_component, second_component);
        assert_ne!(first_component, absent_component);

        let first_key =
            element_key_from_component(spelling, &first_component).expect("fixture component");
        let second_key =
            element_key_from_component(spelling, &second_component).expect("fixture component");
        let first_value = bytes_of(12, 1);
        let second_value = bytes_of(20, 2);
        assert_ne!(first_value, second_value);

        let page = encode_container_page(
            spelling,
            &[
                (&first_key, first_value.as_slice()),
                (&second_key, second_value.as_slice()),
            ],
        );
        assert_eq!(
            ContainerElementRead::Found(first_value.clone()),
            select_container_element(&page, &first_component),
            "{spelling:?} did not answer for its first element"
        );
        assert_eq!(
            ContainerElementRead::Found(second_value.clone()),
            select_container_element(&page, &second_component),
            "{spelling:?} did not answer for its second element -- a selector that returns the \
             first item would pass every one-item test in this file"
        );
        assert_eq!(
            ContainerElementRead::Absent,
            select_container_element(&page, &absent_component),
            "{spelling:?} answered for an element it does not hold"
        );
        covered += 1;
    }
    assert_eq!(4, covered, "every spelling must be covered");
}

/// A FRAME WHOSE SPELLING CANNOT NAME THE COMPONENT ASKED FOR ANSWERS ABSENT, NOT THE FIRST ITEM.
///
/// The second control on vacuous selection, from the other side: here the frame is well formed and
/// the component is well formed, and they belong to different spellings. An implementation that
/// compared strings loosely, or that fell back to the first item when it could not build a key,
/// would serve one element's bytes for another's name -- which is the precise failure #2013 measured
/// on the carried-page path ("the page that loses is handed the FIRST page's BYTES rather than
/// answering missing").
///
/// rust-internal: exercises the page framing directly
#[test]
fn a_frame_asked_for_a_component_its_spelling_cannot_name_answers_absent() {
    let member = bytes_of(16, 1);
    // A set page: its keys are raw members and its components are hex.
    let page = encode_container_page(ElementKeySpelling::Hex, &[(&member, member.as_slice())]);
    // A hash-shaped component, which is not hex.
    assert_eq!(
        ContainerElementRead::Absent,
        select_container_element(&page, "field-0001"),
        "a hex-spelled page answered for a component that is not hex"
    );
    // And the hex component of a DIFFERENT member, which is well formed and not present.
    let other = bytes_of(16, 2);
    assert_eq!(
        ContainerElementRead::Absent,
        select_container_element(&page, &hex::encode(&other))
    );
    // The control that this fixture is not simply always-absent.
    assert_eq!(
        ContainerElementRead::Found(member.clone()),
        select_container_element(&page, &hex::encode(&member))
    );
}

/// A FRAME THAT CANNOT BE WALKED IS NOT MISTAKEN FOR A VALUE.
///
/// Eleven mutilations of one real frame. Every one of them must be refused, because the alternative
/// -- answering `NotFramed` and handing the bytes back -- serves framing bytes as data, which is the
/// #2016 shape one level down.
///
/// THE MUTATIONS ARE COUNTED AND FLOORED. A loop that built no mutants would assert nothing.
///
/// rust-internal: exercises the page framing directly
#[test]
fn a_frame_that_cannot_be_walked_is_not_mistaken_for_a_value() {
    let key = b"field-0001".to_vec();
    let value = bytes_of(24, 1);
    let good = encode_container_page(ElementKeySpelling::Utf8, &[(&key, value.as_slice())]);
    // The control: unmutilated, it is a frame and it answers.
    assert!(matches!(
        decode_container_page(&good),
        ContainerPageDecode::Framed { .. }
    ));

    let mut mutants: Vec<(&str, Vec<u8>)> = Vec::new();
    // The spelling byte names a spelling that does not exist.
    let mut bad_spelling = good.clone();
    bad_spelling[CONTAINER_PAGE_MAGIC.len()] = 99;
    mutants.push(("unknown key spelling", bad_spelling));
    // Truncated at every interesting boundary.
    for cut in [
        CONTAINER_PAGE_MAGIC.len(),
        CONTAINER_PAGE_MAGIC.len() + 1,
        CONTAINER_PAGE_MAGIC.len() + 2,
        CONTAINER_PAGE_MAGIC.len() + 3,
        good.len() - 1,
        good.len() / 2,
    ] {
        mutants.push(("truncated", good[..cut].to_vec()));
    }
    // A trailing byte the walk does not account for.
    let mut trailing = good.clone();
    trailing.push(0);
    mutants.push(("trailing byte", trailing));
    // An item count far larger than the payload could hold.
    let mut huge_count = good.clone();
    huge_count[CONTAINER_PAGE_MAGIC.len() + 1] = 0x7f;
    mutants.push(("item count beyond the payload", huge_count));
    // A key length that runs past the end.
    let mut huge_key = good.clone();
    huge_key[CONTAINER_PAGE_MAGIC.len() + 2] = 0x7f;
    mutants.push(("key length beyond the payload", huge_key));
    // A value offset past the end of its key.
    let offset_page = encode_container_page(ElementKeySpelling::Utf8, &[(&key, b"")]);
    let mut bad_offset = offset_page.clone();
    let last = bad_offset.len() - 1;
    bad_offset[last] = 0x7f;
    mutants.push(("value offset past the key", bad_offset));

    let planted = mutants.len();
    assert!(
        planted >= 10,
        "only {planted} mutants were planted, which is too few to claim the walk is guarded"
    );
    let mut refused = 0usize;
    println!("\n=== frames that must be refused ===");
    for (what, mutant) in mutants {
        let verdict = decode_container_page(&mutant);
        let is_refused = matches!(verdict, ContainerPageDecode::Corrupt(_));
        println!(
            "  {:<30} {} bytes -> {}",
            what,
            mutant.len(),
            if is_refused { "refused" } else { "ACCEPTED" }
        );
        assert!(
            is_refused,
            "{what} ({} bytes) was not refused: {verdict:?}",
            mutant.len()
        );
        refused += 1;
    }
    assert_eq!(planted, refused, "every planted mutant must be refused");
    println!("  planted {planted}, refused {refused}");
}

/// BYTES WITH NO MAGIC ARE NOT A FRAME, AND ARE NOT CORRUPT EITHER.
///
/// The compatibility hinge in isolation: a page written before this stage has no magic, and the
/// answer has to be `NotFramed` so the read funnel hands the payload back. `Corrupt` here would
/// break every stored page.
///
/// rust-internal: exercises the page framing directly
#[test]
fn bytes_with_no_magic_are_not_a_frame() {
    let mut checked = 0usize;
    for payload in [
        b"".to_vec(),
        b"a".to_vec(),
        b"TSCPG".to_vec(),
        b"TSFPB1\n".to_vec(),
        bytes_of(128, 1),
        serde_json::to_vec(&serde_json::json!({"any": "json"})).expect("json"),
    ] {
        assert_eq!(
            ContainerPageDecode::NotFramed,
            decode_container_page(&payload),
            "a payload of {} bytes was taken for a frame",
            payload.len()
        );
        assert_eq!(
            ContainerElementRead::NotFramed,
            select_container_element(&payload, "field-0001")
        );
        checked += 1;
    }
    assert_eq!(6, checked);
}

// =================================================================================================
// 4. AND THE PRODUCT STILL ANSWERS EXACTLY WHAT IT DID
// =================================================================================================

/// EVERY CONTAINER KIND WRITES A FRAMED PAGE AND READS BACK THE VALUE IT WROTE.
///
/// End to end through the engine's own command surface, which is the only arm that proves the write
/// sites and the read funnel agree. The two counters are floored at zero: no write fell back to a
/// bare page, and no read met a frame it could not walk.
///
/// THE STORE PATH LENGTH is printed and is one directory for the whole test, so no arm is measured
/// against a longer path than another.
///
/// rust-internal: drives the engine's own command surface
#[test]
fn every_container_kind_reads_back_the_value_it_wrote_through_a_framed_page() {
    let dir = tempfile::tempdir().expect("tempdir");
    println!(
        "\n=== container kinds through the product path ===\n  store path {} characters",
        dir.path().as_os_str().len()
    );
    let engine = engine_on(dir.path());
    load_on(&engine);
    crate::engine::container_pages::reset_unframed_container_write_count();
    crate::engine::reset_corrupt_container_page_count();

    let fields: Vec<(String, Vec<u8>)> = (0..4)
        .map(|index| (format!("field-{index:04}"), bytes_of(48, index)))
        .collect();
    for (field, value) in &fields {
        write(
            &engine,
            Command::HashSet {
                key: "hash-key".to_string(),
                field: field.clone(),
                value: value.clone(),
            },
        );
    }
    for (field, value) in &fields {
        let response = read(
            &engine,
            Command::HashGet {
                key: "hash-key".to_string(),
                field: field.clone(),
            },
        );
        match response {
            crate::types::CommandResponse::Bytes { value: got } => assert_eq!(
                Some(value.clone()),
                got,
                "hash field {field} did not read back the bytes it was written with"
            ),
            other => panic!("expected Bytes, got {other:?}"),
        }
    }
    // The whole-object door, which walks the page index and reads a page per element: the arm #2014
    // guards, and the one that would return framing bytes if selection were skipped.
    let entries = match read(
        &engine,
        Command::HashGetAll {
            key: "hash-key".to_string(),
        },
    ) {
        crate::types::CommandResponse::HashEntries { entries } => entries,
        other => panic!("expected HashEntries, got {other:?}"),
    };
    assert_eq!(
        fields.len(),
        entries.len(),
        "the listing answered {} entries for {} fields",
        entries.len(),
        fields.len()
    );
    for (field, value) in &fields {
        let found = entries
            .iter()
            .find(|(name, _)| name == field)
            .map(|(_, bytes)| bytes.clone());
        assert_eq!(Some(value.clone()), found, "the listing lost field {field}");
    }

    let members: Vec<Vec<u8>> = (0..4).map(|index| bytes_of(32, index + 10)).collect();
    for member in &members {
        write(
            &engine,
            Command::SetAdd {
                key: "set-key".to_string(),
                member: member.clone(),
            },
        );
    }
    let listed = match read(
        &engine,
        Command::SetMembers {
            key: "set-key".to_string(),
        },
    ) {
        crate::types::CommandResponse::Members { members } => members,
        other => panic!("expected Members, got {other:?}"),
    };
    assert_eq!(
        members.len(),
        listed.len(),
        "the set listing answered {} members for {}",
        listed.len(),
        members.len()
    );
    for member in &members {
        assert!(
            listed.contains(member),
            "the set listing lost a member of {} bytes",
            member.len()
        );
    }

    for (index, member) in members.iter().enumerate() {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ZSetAdd {
                key: "zset-key".to_string(),
                member: member.clone(),
                score: index as f64,
            },
        });
        assert!(response.status.ok, "a zset write failed: {response:?}");
    }
    let zset_members = match read(
        &engine,
        Command::ZSetRange {
            key: "zset-key".to_string(),
            start: 0,
            stop: -1,
            rev: false,
        },
    ) {
        crate::types::CommandResponse::Members { members } => members,
        other => panic!("expected Members, got {other:?}"),
    };
    // A ZSET LISTING ANSWERS INTERLEAVED MEMBER AND SCORE, so four members are eight entries. And
    // it reads NO PAGE AT ALL -- it is served out of `shard.zsets`, which #2017 measured and used
    // as its control. So this arm proves the zset WRITE path did not break the resident map; the
    // zset PAGE is exercised by the reload test below, which is the only door that reads it.
    assert_eq!(
        members.len() * 2,
        zset_members.len(),
        "the zset listing answered {} entries for {} members, and it answers member and score",
        zset_members.len(),
        members.len()
    );
    for member in &members {
        assert!(
            zset_members.contains(member),
            "the zset listing lost a member of {} bytes",
            member.len()
        );
    }

    let elements: Vec<Vec<u8>> = (0..4).map(|index| bytes_of(40, index + 20)).collect();
    for element in &elements {
        write(
            &engine,
            Command::ListPush {
                key: "list-key".to_string(),
                member: element.clone(),
                left: false,
            },
        );
    }
    let ranged = match read(
        &engine,
        Command::ListRange {
            key: "list-key".to_string(),
            start: 0,
            stop: -1,
        },
    ) {
        crate::types::CommandResponse::Members { members } => members,
        other => panic!("expected Members, got {other:?}"),
    };
    assert_eq!(
        elements, ranged,
        "the list did not read back the elements it was pushed, in order"
    );

    assert_eq!(
        0,
        crate::engine::container_pages::unframed_container_write_count(),
        "a container write stored a bare value instead of a frame"
    );
    assert_eq!(
        0,
        crate::engine::corrupt_container_page_count(),
        "a read met a container page it could not walk"
    );
    println!(
        "  hash {} fields, set {} members, zset {} members ({} listing entries), list {} elements -- all read back",
        fields.len(),
        members.len(),
        members.len(),
        zset_members.len(),
        elements.len()
    );
}

/// A FRAMED PAGE SURVIVES A RELOAD, WHICH IS THE ONLY DOOR THAT READS SOME OF THEM AT ALL.
///
/// The end-to-end test above cannot exercise a zset page, because a zset listing is served out of
/// `shard.zsets` and reads nothing -- #2017's own control. A reload is what reads the pages: it
/// rebuilds the resident maps out of the page index and then every subsequent read resolves an
/// address and goes to storage.
///
/// SO THIS IS ALSO THE ARM THAT MATTERS MOST TO WHAT COMES NEXT. The stage after this one makes the
/// load path rebuild those maps out of the PAGES rather than out of the index, and the page read
/// here is the read it will build on. If the frame did not survive a reload there would be nothing
/// to build on.
///
/// THE DENOMINATOR: the reloaded engine is asserted to hold the elements before anything is read
/// through it, so a reload that came up empty would fail here rather than quietly passing every
/// "the value is missing and so is the expectation" comparison.
///
/// rust-internal: drives the engine's own command surface
#[test]
fn a_framed_page_reads_back_after_a_reload() {
    let dir = tempfile::tempdir().expect("tempdir");
    let pages = dir.path().join("pages");
    let indexes = dir.path().join("indexes");
    println!(
        "\n=== framed pages across a reload ===\n  store path {} characters",
        dir.path().as_os_str().len()
    );

    let fields: Vec<(String, Vec<u8>)> = (0..6)
        .map(|index| (format!("field-{index:04}"), bytes_of(64, index)))
        .collect();
    let members: Vec<Vec<u8>> = (0..6).map(|index| bytes_of(24, index + 100)).collect();
    let elements: Vec<Vec<u8>> = (0..6).map(|index| bytes_of(36, index + 200)).collect();

    {
        let engine =
            TemporalEngine::with_local_dirs(64 * 1024 * 1024, dir.path().join("cache"), &pages, &indexes);
        load_on(&engine);
        crate::engine::container_pages::reset_unframed_container_write_count();
        for (field, value) in &fields {
            write(
                &engine,
                Command::HashSet {
                    key: "reloaded".to_string(),
                    field: field.clone(),
                    value: value.clone(),
                },
            );
        }
        for member in &members {
            write(
                &engine,
                Command::SetAdd {
                    key: "reloaded".to_string(),
                    member: member.clone(),
                },
            );
        }
        for (index, member) in members.iter().enumerate() {
            write(
                &engine,
                Command::ZSetAdd {
                    key: "reloaded".to_string(),
                    member: member.clone(),
                    score: index as f64,
                },
            );
        }
        for element in &elements {
            write(
                &engine,
                Command::ListPush {
                    key: "reloaded".to_string(),
                    member: element.clone(),
                    left: false,
                },
            );
        }
        assert_eq!(
            0,
            crate::engine::container_pages::unframed_container_write_count(),
            "a container write stored a bare value instead of a frame"
        );
        engine.flush_shard_index(1);
    }

    // A SECOND ENGINE ON THE SAME FILES, with its own cache directory so nothing is answered out
    // of a warm page the first engine left behind. Without that the reload would be measured
    // against the cache rather than against storage.
    let reloaded = TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.path().join("cache-reloaded"),
        &pages,
        &indexes,
    );
    crate::engine::reset_corrupt_container_page_count();
    load_on(&reloaded);

    // The denominator, before a single value is compared.
    let hash_len = match read(
        &reloaded,
        Command::HashLen {
            key: "reloaded".to_string(),
        },
    ) {
        crate::types::CommandResponse::Integer { value } => value,
        other => panic!("expected Integer, got {other:?}"),
    };
    assert_eq!(
        fields.len() as i64,
        hash_len,
        "the reloaded shard holds {hash_len} fields where {} were written, so a per-field \
         comparison below would be over the wrong population",
        fields.len()
    );

    for (field, value) in &fields {
        let got = match read(
            &reloaded,
            Command::HashGet {
                key: "reloaded".to_string(),
                field: field.clone(),
            },
        ) {
            crate::types::CommandResponse::Bytes { value } => value,
            other => panic!("expected Bytes, got {other:?}"),
        };
        assert_eq!(
            Some(value.clone()),
            got,
            "hash field {field} did not survive the reload"
        );
    }

    let listed = match read(
        &reloaded,
        Command::SetMembers {
            key: "reloaded".to_string(),
        },
    ) {
        crate::types::CommandResponse::Members { members } => members,
        other => panic!("expected Members, got {other:?}"),
    };
    assert_eq!(
        members.len(),
        listed.len(),
        "the reloaded set listing answered {} members for {}",
        listed.len(),
        members.len()
    );
    for member in &members {
        assert!(
            listed.contains(member),
            "the reloaded set lost a member of {} bytes -- and a set listing reads ONE PAGE PER \
             MEMBER, so this is the arm that reads a framed page off storage",
            member.len()
        );
    }

    let ranged = match read(
        &reloaded,
        Command::ListRange {
            key: "reloaded".to_string(),
            start: 0,
            stop: -1,
        },
    ) {
        crate::types::CommandResponse::Members { members } => members,
        other => panic!("expected Members, got {other:?}"),
    };
    assert_eq!(
        elements, ranged,
        "the reloaded list did not read back its elements in order"
    );

    let zset_entries = match read(
        &reloaded,
        Command::ZSetRange {
            key: "reloaded".to_string(),
            start: 0,
            stop: -1,
            rev: false,
        },
    ) {
        crate::types::CommandResponse::Members { members } => members,
        other => panic!("expected Members, got {other:?}"),
    };
    for member in &members {
        assert!(
            zset_entries.contains(member),
            "the reloaded zset lost a member of {} bytes",
            member.len()
        );
    }

    assert_eq!(
        0,
        crate::engine::corrupt_container_page_count(),
        "the reload met a container page it could not walk"
    );
    println!(
        "  reloaded: {} hash fields, {} set members, {} list elements, {} zset members -- all read back off storage",
        fields.len(),
        listed.len(),
        ranged.len(),
        members.len()
    );
}

/// A PAGE WRITTEN WITHOUT A FRAME STILL READS THROUGH THE FUNNEL.
///
/// The compatibility claim, exercised on the product path rather than argued from the magic. No
/// version stamp moves in this stage, and this is the whole reason that is safe: a stored page from
/// before this change has no magic, so the funnel hands its payload back untouched.
///
/// HOW THE OLD SHAPE IS PRODUCED WITHOUT A TIME MACHINE: the value is appended through the block
/// store directly, with no frame round it, and the page index is told about it the same way a write
/// would. That is byte-for-byte what a page written before this stage looks like.
///
/// rust-internal: drives the engine's own command surface
#[test]
fn a_page_written_without_a_frame_still_reads_through_the_funnel() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine);
    crate::engine::reset_corrupt_container_page_count();

    // A framed write first, so the fixture holds both shapes at once and the unframed read cannot
    // pass merely because nothing in the store is framed.
    write(
        &engine,
        Command::HashSet {
            key: "mixed".to_string(),
            field: "framed".to_string(),
            value: b"framed-value".to_vec(),
        },
    );

    let legacy_value = b"a value with no frame at all".to_vec();
    let installed = engine.install_unframed_hash_page_for_test(1, "mixed", "legacy", &legacy_value);
    assert!(
        installed,
        "the fixture could not install an unframed page, so this test proves nothing"
    );

    let got = match read(
        &engine,
        Command::HashGet {
            key: "mixed".to_string(),
            field: "legacy".to_string(),
        },
    ) {
        crate::types::CommandResponse::Bytes { value } => value,
        other => panic!("expected Bytes, got {other:?}"),
    };
    assert_eq!(
        Some(legacy_value.clone()),
        got,
        "an unframed page did not read back its own bytes"
    );

    // And the framed one beside it still answers, so the two shapes coexist.
    let framed = match read(
        &engine,
        Command::HashGet {
            key: "mixed".to_string(),
            field: "framed".to_string(),
        },
    ) {
        crate::types::CommandResponse::Bytes { value } => value,
        other => panic!("expected Bytes, got {other:?}"),
    };
    assert_eq!(Some(b"framed-value".to_vec()), framed);

    assert_eq!(
        0,
        crate::engine::corrupt_container_page_count(),
        "reading an unframed page was scored as a corrupt frame"
    );
    println!(
        "\n=== both shapes in one store ===\n  unframed page {} bytes read back exactly; framed page beside it still answers",
        legacy_value.len()
    );
}
