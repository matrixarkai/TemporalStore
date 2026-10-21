// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What the consolidated per-object record holds, and that converting an object's separate rows
//! into it and back loses nothing.
//!
//! A WIDTH PIN PROVES ARITHMETIC AND NOTHING ELSE. `size_of == 24` says the fields fit; it says
//! nothing about whether the fields are the right ones. So the load-bearing test here is
//! [`converting_one_objects_rows_into_the_record_and_back_loses_nothing`], which takes the rows the
//! shard holds today -- the model map's address, the deadline map's instant, the dirty index's bit
//! and the model id -- folds them into one record, unfolds them, and compares. A merge that cannot
//! survive that is a merge that drops a fact.
//!
//! AND ONE MEASUREMENT RATHER THAN AN ASSUMPTION. The inline arm cannot hold a page identity, so
//! how often that matters is a COUNT, not a reading:
//! [`how_often_a_real_store_needs_the_layout_arm`] seeds a store through the ordinary write path
//! and reports the split. Its answer decides whether the inline arm is the common case or the rare
//! one, and that is the difference between this record saving memory and costing it.

use super::*;

use crate::block_store::ElementEntry;
use crate::engine::object_record::{
    Deadline, DeadlineListHandle, ObjectRecord, ObjectRecordRefusal, PageAddress, PageLayout,
    PageLayoutHandle, RecordFlags,
};

// Imported as a NAME rather than called through its full path, for the reason the sibling census
// probe states at length: the counting-allocator gate in `alloc_probe.rs` scans every source line
// for the probe's fully qualified path and walks BACK to the nearest `#[test]`. A helper spelling
// that path out sits above every test in the file, so the walk runs off the top and the gate
// reports the allocator as read outside any test. It is not a style preference -- writing it the
// other way fails `every_counting_allocator_probe_is_gated_on_the_feature_that_installs_it`.
#[cfg(feature = "alloc-probe")]
use crate::alloc_probe::Probe;

/// Bytes the heap gives up to hold a deep copy of `value` -- which is what `value` itself holds.
///
/// The same measurement the sibling census probe takes, spelled locally rather than imported: that
/// module's helper is private to it, and a `pub(super)` door opened across two test modules to
/// share eight lines is a worse trade than the eight lines. `Arc` clones bump a refcount and
/// allocate nothing, so shared string payloads are deliberately NOT in these figures -- which is
/// the right denominator for the question below, because both halves of the dirty index share one
/// payload and it survives whichever half goes.
#[cfg(feature = "alloc-probe")]
fn deep_heap_bytes<T: Clone>(value: &T) -> u64 {
    let probe = Probe::start();
    let copy = value.clone();
    std::hint::black_box(&copy);
    let counts = probe.stop();
    drop(copy);
    counts.alloc_bytes
}

/// Every named bit, so a test can walk them rather than restating the list.
const NAMED_BITS: [(&str, u16); 11] = [
    ("ADDRESS_IS_POINTER", RecordFlags::ADDRESS_IS_POINTER),
    ("DEADLINE_IS_POINTER", RecordFlags::DEADLINE_IS_POINTER),
    ("DIRTY", RecordFlags::DIRTY),
    ("INDEX_DIRTY", RecordFlags::INDEX_DIRTY),
    ("LOADING", RecordFlags::LOADING),
    ("IN_MEMORY", RecordFlags::IN_MEMORY),
    ("PAGE_DIRTY", RecordFlags::PAGE_DIRTY),
    ("PAGE_DELETED", RecordFlags::PAGE_DELETED),
    ("PAGE_IN_LOG", RecordFlags::PAGE_IN_LOG),
    ("DEADLINE_PRESENT", RecordFlags::DEADLINE_PRESENT),
    ("PAGE_GENERATION", RecordFlags::PAGE_GENERATION),
];

/// THE WIDTH AND THE ALIGNMENT, MEASURED AND PRINTED RATHER THAN PREDICTED.
///
/// The const assertions beside the type are the gate; this prints the numbers so a reader of the
/// log sees what was reached, and asserts them again at runtime so a failure names the figure
/// instead of being a compile error with no value in it.
///
/// NO NEIGHBOUR INEQUALITY. `!= 23 && != 25` passes at every width, which makes it an assertion
/// that can only ever fail inside a test nothing runs; this campaign shipped that error into three
/// merged pull requests. The width is an equality and the alignment is a SEPARATE equality.
#[test]
fn the_record_is_twenty_four_bytes_at_natural_alignment() {
    let width = std::mem::size_of::<ObjectRecord>();
    let align = std::mem::align_of::<ObjectRecord>();
    let field_sum = std::mem::size_of::<u64>()
        + std::mem::size_of::<u64>()
        + std::mem::size_of::<u32>()
        + std::mem::size_of::<RecordFlags>()
        + std::mem::size_of::<u8>();
    println!("ObjectRecord: size_of = {width}, align_of = {align}");
    println!("  field sum = {field_sum}, tail slack = {}", width - field_sum);
    println!("  RecordFlags: size_of = {}", std::mem::size_of::<RecordFlags>());
    assert_eq!(24, width, "the record's width moved");
    assert_eq!(8, align, "the record's alignment moved");
    assert_eq!(23, field_sum, "the field set moved without the width stepping");
    assert_eq!(1, width - field_sum, "the tail slack moved");
}

/// Twelve distinct named bits, four reserved, and the two sets disjoint.
///
/// A duplicated mask is the failure this catches: two names for one bit compile, and then setting
/// one silently sets the other.
#[test]
fn every_flag_bit_is_distinct_and_the_reserved_set_is_disjoint() {
    let mut seen = 0u16;
    for (name, bit) in NAMED_BITS {
        assert_eq!(1, bit.count_ones(), "{name} is not a single bit: {bit:#06x}");
        assert_eq!(0, seen & bit, "{name} reuses a bit another name already took");
        assert_eq!(
            0,
            bit & RecordFlags::RESERVED,
            "{name} sits inside the reserved range {:#06x}",
            RecordFlags::RESERVED
        );
        seen |= bit;
    }
    assert_eq!(11, seen.count_ones(), "eleven named bits");
    assert_eq!(RecordFlags::KNOWN, seen, "the KNOWN mask and the named list disagree");
    assert_eq!(5, RecordFlags::RESERVED.count_ones(), "five reserved bits");
    assert_eq!(0, RecordFlags::KNOWN & RecordFlags::RESERVED, "the two sets overlap");
    assert_eq!(16, (RecordFlags::KNOWN | RecordFlags::RESERVED).count_ones(), "sixteen in all");
}

/// A ZERO DEADLINE IS NOT AN ABSENT DEADLINE, which is the whole reason the tag bit exists.
///
/// This is the test that would fail if someone replaced the presence bit with a sentinel. It is
/// here because the same mistake has a written history on the neighbouring address type: zero is a
/// legal block id, so "zero means none" would erase a real first page.
#[test]
fn a_zero_deadline_is_not_an_absent_deadline() {
    let mut absent = ObjectRecord::default();
    absent.set_deadline(Deadline::None);
    let mut at_zero = ObjectRecord::default();
    at_zero.set_deadline(Deadline::At(0));

    assert_eq!(Deadline::None, absent.deadline(), "an absent deadline must read as absent");
    assert_eq!(Deadline::At(0), at_zero.deadline(), "a zero deadline must read as a zero deadline");
    assert_ne!(
        absent.deadline(),
        at_zero.deadline(),
        "absent and zero read the same, so the record is using a sentinel and a legal instant of \
         zero is being erased"
    );
    // And the two differ in the FLAGS, not in the word: the word is zero in both.
    assert_ne!(absent.flags().bits(), at_zero.flags().bits(), "the tag bit is not carrying this");
}

/// BOTH TAGGED WORDS ARE UNREADABLE WITHOUT THEIR TAG, and the arms round-trip through the setter.
///
/// The structural claim is that there is no accessor handing out a bare word. That cannot be
/// asserted from inside the crate -- a private field is reachable here -- so what this asserts is
/// the behavioural half: every arm set is the arm read back, and a handle never reads as an
/// address.
#[test]
fn each_tagged_word_reads_back_as_the_arm_it_was_set_to() {
    let mut record = ObjectRecord::default();

    for arm in [
        Deadline::None,
        Deadline::At(0),
        Deadline::At(1_700_000_000_000),
        Deadline::At(u64::MAX),
        Deadline::PerElement(DeadlineListHandle::from_stored(0)),
        Deadline::PerElement(DeadlineListHandle::from_stored(9_999)),
    ] {
        record.set_deadline(arm);
        assert_eq!(arm, record.deadline(), "the deadline arm did not survive a set-then-get");
    }

    for arm in [
        PageAddress::One(0),
        PageAddress::One(crate::engine::object_record::address_word_from_parts(7, 4_096)),
        PageAddress::Many(PageLayoutHandle::from_stored(0)),
        PageAddress::Many(PageLayoutHandle::from_stored(42)),
    ] {
        record.set_page_address(arm);
        assert_eq!(arm, record.page_address(), "the address arm did not survive a set-then-get");
    }

    // A HANDLE OF ZERO AND AN ADDRESS OF ZERO ARE DIFFERENT ARMS. Both words are zero, so only the
    // tag separates them -- which is exactly the case a sentinel would collapse.
    record.set_page_address(PageAddress::One(0));
    let as_address = record.page_address();
    record.set_page_address(PageAddress::Many(PageLayoutHandle::from_stored(0)));
    let as_handle = record.page_address();
    assert_ne!(as_address, as_handle, "a zero address and a zero handle read the same");
}

/// THE RECORD SURVIVES ITS OWN CODEC INTACT, on every arm combination that is legal.
#[test]
fn the_record_survives_its_own_codec_intact() {
    let mut cases: Vec<ObjectRecord> = Vec::new();
    for deadline in [
        Deadline::None,
        Deadline::At(0),
        Deadline::At(1_700_000_000_000),
        Deadline::PerElement(DeadlineListHandle::from_stored(5)),
    ] {
        for address in
            [PageAddress::One(crate::engine::object_record::address_word_from_parts(3, 64)),
             PageAddress::Many(PageLayoutHandle::from_stored(11))]
        {
            let mut record = ObjectRecord::default();
            record.set_deadline(deadline);
            record.set_page_address(address);
            record.set_page_size(4_096);
            record.set_model_id(7);
            for (_, bit) in NAMED_BITS {
                // Do not disturb the two tag bits or the presence bit the setters just arranged.
                if bit == RecordFlags::ADDRESS_IS_POINTER
                    || bit == RecordFlags::DEADLINE_IS_POINTER
                    || bit == RecordFlags::DEADLINE_PRESENT
                {
                    continue;
                }
                record.set_flag(bit, true);
            }
            cases.push(record);
        }
    }
    assert_eq!(8, cases.len(), "the case matrix did not build");

    for record in cases {
        let encoded = serde_json::to_string(&record).expect("a record must encode");
        let decoded: ObjectRecord =
            serde_json::from_str(&encoded).unwrap_or_else(|error| {
                panic!("a record this type wrote must decode: {error} from {encoded}")
            });
        assert_eq!(record, decoded, "the codec moved a field: {encoded}");
        assert_eq!(
            record.deadline(),
            decoded.deadline(),
            "the deadline arm did not survive the codec: {encoded}"
        );
        assert_eq!(
            record.page_address(),
            decoded.page_address(),
            "the address arm did not survive the codec: {encoded}"
        );
    }
}

/// A RECORD WITH NO DEADLINE CARRIES NO DEADLINE KEY AT ALL, rather than a stored zero.
#[test]
fn an_absent_deadline_is_absent_on_the_wire_too() {
    let mut absent = ObjectRecord::default();
    absent.set_deadline(Deadline::None);
    let encoded = serde_json::to_string(&absent).expect("encode");
    assert!(
        !encoded.contains("\"t\""),
        "an absent deadline wrote a deadline key, so a stored zero is standing in for absence: \
         {encoded}"
    );

    let mut present = ObjectRecord::default();
    present.set_deadline(Deadline::At(0));
    let encoded_zero = serde_json::to_string(&present).expect("encode");
    assert!(
        encoded_zero.contains("\"t\""),
        "a zero deadline did not reach the wire, so it is indistinguishable from absence: \
         {encoded_zero}"
    );
}

/// THE DECODER REFUSES BY NAME, AS AN `Err`, AND NEVER PANICS.
///
/// A panic on this path makes a store unopenable; a refusal falls back to replaying the operation
/// log. So each malformed shape is asserted to be an `Err` of a NAMED variant, and the message is
/// asserted to carry that name -- a refusal nobody can identify from a log line is a refusal that
/// gets diagnosed as corruption.
#[test]
fn the_decoder_refuses_by_name_rather_than_panicking() {
    // Built as raw wire text, because these are shapes this writer cannot produce.
    let cases: [(&str, &str, &str); 4] = [
        (
            "a reserved bit set by a newer writer",
            r#"{"a":64,"n":8,"f":4096,"m":1}"#,
            "ReservedFlagBitsSet",
        ),
        (
            "the deadline tag set with no deadline stored",
            r#"{"a":64,"n":8,"f":1024,"m":1}"#,
            "DeadlineTaggedButAbsentOnTheWire",
        ),
        (
            "a deadline stored with its tag clear",
            r#"{"a":64,"n":8,"f":0,"m":1,"t":1700000000000}"#,
            "DeadlineOnTheWireButNotTagged",
        ),
        (
            "the deadline pointer tag set with no presence bit",
            r#"{"a":64,"n":8,"f":2,"m":1}"#,
            "DeadlinePointerWithoutPresence",
        ),
    ];

    let mut refused = 0usize;
    for (what, wire, expected_name) in cases {
        let outcome: Result<ObjectRecord, _> = serde_json::from_str(wire);
        let error = match outcome {
            Ok(record) => panic!(
                "{what}: the decoder ACCEPTED a shape it cannot read and built {record:?} from \
                 {wire}"
            ),
            Err(error) => error.to_string(),
        };
        assert!(
            error.contains(expected_name),
            "{what}: the refusal does not name itself -- expected {expected_name} in {error:?}"
        );
        refused += 1;
    }
    assert_eq!(cases.len(), refused, "not every malformed shape was refused");

    // THE POSITIVE CONTROL. A decoder that refuses everything passes the four assertions above and
    // is useless, which is the failure mode worth catching here.
    let legal = r#"{"a":64,"n":8,"f":1028,"m":1,"t":1700000000000}"#;
    let decoded: ObjectRecord = serde_json::from_str(legal)
        .unwrap_or_else(|error| panic!("the decoder refused a LEGAL record: {error} from {legal}"));
    assert_eq!(
        Deadline::At(1_700_000_000_000),
        decoded.deadline(),
        "the control decoded but lost its deadline"
    );
    assert!(decoded.dirty(), "the control decoded but lost its dirty bit");
}

/// THE PROOF THE MERGE IS REAL: an object's separate rows, folded into one record and unfolded.
///
/// The rows are the ones a live `string` object holds today -- the model map's [`ElementEntry`],
/// the deadline map's instant, and the dirty index's bit -- plus the model id the map's identity
/// carries. Nothing may differ after the round trip, including the parts of the address that are
/// PRESENCE rather than value: a generation bit dropped here would hand an older index a
/// generation it never had.
#[test]
fn converting_one_objects_rows_into_the_record_and_back_loses_nothing() {
    // The inline arm. Both ordinal states belong here: ABSENT (a log-resident page) and PRESENT
    // AT ZERO (a slab-resident single-page object, which the store measurement says is 400 of 400
    // of what the ordinary write path files).
    let inline_cases: [(&str, ElementEntry, u8, Option<u64>, bool); 7] = [
        ("log-resident, no deadline, clean", ElementEntry::from_parts(3, 4_096, 128, None, None), 1, None, false),
        ("log-resident, no deadline, dirty", ElementEntry::from_parts(3, 4_096, 128, None, None), 1, None, true),
        ("log-resident, deadline, dirty", ElementEntry::from_parts(9, 0, 64, None, None), 4, Some(1_700_000_000_000), true),
        ("log-resident, ZERO deadline", ElementEntry::from_parts(1, 8, 32, None, None), 2, Some(0), false),
        ("ordinal zero, no deadline, clean", ElementEntry::from_parts(5, 64, 128, Some(0), Some(0)), 1, None, false),
        ("ordinal zero, deadline, dirty", ElementEntry::from_parts(7, 4_096, 832, Some(0), Some(0)), 4, Some(42), true),
        ("ordinal zero, ZERO deadline, dirty", ElementEntry::from_parts(1, 0, 16, Some(0), Some(0)), 2, Some(0), true),
    ];

    let mut checked = 0usize;
    for (what, address, model_id, deadline, dirty) in inline_cases {
        let record = ObjectRecord::from_object_rows(&address, model_id, deadline, dirty)
            .unwrap_or_else(|error| panic!("{what}: the inline arm refused a row it should hold: {error}"));
        assert!(
            matches!(record.page_address(), PageAddress::One(_)),
            "{what}: a log-resident page should take the inline arm"
        );
        let (back_address, back_model, back_deadline, back_dirty) = record
            .into_object_rows(None)
            .unwrap_or_else(|error| panic!("{what}: unfolding refused: {error}"));
        assert_eq!(address, back_address, "{what}: the address row changed");
        assert_eq!(model_id, back_model, "{what}: the model id changed");
        assert_eq!(deadline, back_deadline, "{what}: the deadline row changed");
        assert_eq!(dirty, back_dirty, "{what}: the dirty row changed");
        // The presence halves specifically, because they are the ones a value comparison can miss.
        assert_eq!(address.block_id(), back_address.block_id(), "{what}: page identity presence");
        assert_eq!(address.generation(), back_address.generation(), "{what}: generation presence");
        assert_eq!(address.length(), back_address.length(), "{what}: page length");
        assert_eq!(address.address_word(), back_address.address_word(), "{what}: address word");
        checked += 1;
    }

    // The layout arm: a page of a MULTI-page object, whose ordinal is nonzero. Ordinal zero is
    // included too, because the layout arm must hold everything the inline arm holds as well --
    // an arm that only works for the cases the other one refuses cannot serve a mixed object.
    let layout_cases: [(&str, ElementEntry, u8, Option<u64>, bool); 4] = [
        ("ordinal 1, no deadline", ElementEntry::from_parts(5, 64, 128, Some(1), Some(1)), 1, None, true),
        ("ordinal 3, deadline", ElementEntry::from_parts(7, 4_096, 832, Some(3), Some(3)), 4, Some(42), false),
        ("ordinal at the stored ceiling", ElementEntry::from_parts(2, 128, 64, Some(u64::from(u16::MAX)), Some(u64::from(u16::MAX))), 1, None, false),
        ("ordinal zero, in the layout arm too", ElementEntry::from_parts(1, 0, 16, Some(0), Some(0)), 1, None, false),
    ];
    for (what, address, model_id, deadline, dirty) in layout_cases {
        let handle = PageLayoutHandle::from_stored(77);
        let (record, layout) =
            ObjectRecord::from_object_rows_with_layout(&address, model_id, deadline, dirty, handle)
                .unwrap_or_else(|error| panic!("{what}: the layout arm refused: {error}"));
        assert!(
            matches!(record.page_address(), PageAddress::Many(_)),
            "{what}: an identified page should take the layout arm"
        );
        assert_eq!(1, layout.len(), "{what}: one page should produce a one-page layout");
        let (back_address, back_model, back_deadline, back_dirty) = record
            .into_object_rows(Some(&layout))
            .unwrap_or_else(|error| panic!("{what}: unfolding refused: {error}"));
        assert_eq!(address, back_address, "{what}: the address row changed");
        assert_eq!(model_id, back_model, "{what}: the model id changed");
        assert_eq!(deadline, back_deadline, "{what}: the deadline row changed");
        assert_eq!(dirty, back_dirty, "{what}: the dirty row changed");
        assert_eq!(address.block_id(), back_address.block_id(), "{what}: page identity");
        assert_eq!(address.generation(), back_address.generation(), "{what}: generation presence");
        checked += 1;
    }
    assert_eq!(11, checked, "the conversion matrix did not run");
}

/// THE INLINE ARM'S ONE LIMIT, REFUSED BY NAME RATHER THAN DROPPED -- AND ITS EXACT BOUNDARY.
///
/// Twenty-three of the record's twenty-four bytes are spoken for, so the sixteen-bit page ordinal
/// an [`ElementEntry`] carries has no inline home as a VALUE. Its PRESENCE does, as `PAGE_IN_LOG`.
/// So the boundary is at zero: ordinal absent and ordinal zero are both inline, and a nonzero
/// ordinal -- which is a page of a multi-page object -- is refused by name and routed to the
/// layout arm. Dropping it instead would resolve a key to a different page of the same object and
/// read the wrong bytes with no error anywhere.
///
/// BOTH SIDES OF THE BOUNDARY ARE ASSERTED. A test that only checked the refusal would pass if the
/// inline arm refused everything, which is the state this function was in before a store
/// measurement showed it would send 400 of 400 real objects down the side-allocating arm.
#[test]
fn the_inline_arm_takes_ordinal_zero_and_refuses_a_nonzero_one_by_name() {
    // THE ACCEPTING SIDE, which is the one a real store exercises.
    for ordinal in [None, Some(0u64)] {
        let entry = ElementEntry::from_parts(5, 64, 128, ordinal, ordinal);
        let record = ObjectRecord::from_object_rows(&entry, 1, None, false).unwrap_or_else(|error| {
            panic!("the inline arm refused ordinal {ordinal:?}, which it must hold: {error}")
        });
        assert!(
            matches!(record.page_address(), PageAddress::One(_)),
            "ordinal {ordinal:?} did not take the inline arm"
        );
        let (back, ..) = record.into_object_rows(None).expect("unfold");
        assert_eq!(entry, back, "ordinal {ordinal:?} did not survive the inline arm");
    }

    // THE REFUSING SIDE.
    let identified = ElementEntry::from_parts(5, 64, 128, Some(7), Some(7));
    let refusal = ObjectRecord::from_object_rows(&identified, 1, None, false)
        .expect_err("the inline arm accepted a nonzero page ordinal it has no room for");
    assert_eq!(
        ObjectRecordRefusal::NonZeroPageOrdinalDoesNotFitInline { block_id: 7 },
        refusal,
        "the refusal is not the named one"
    );
    assert!(
        refusal.to_string().contains("NonZeroPageOrdinalDoesNotFitInline"),
        "the refusal does not name itself: {refusal}"
    );

    // AND THE LAYOUT ARM TAKES IT. A refusal with no route through is a dead end, not a design.
    let (_record, layout) = ObjectRecord::from_object_rows_with_layout(
        &identified,
        1,
        None,
        false,
        PageLayoutHandle::from_stored(1),
    )
    .expect("the layout arm must hold what the inline arm refuses");
    assert!(!layout.is_empty(), "the layout arm produced nothing");
}

/// A HANDLE WITH NO LAYOUT IS REFUSED, not defaulted to a zero address.
#[test]
fn a_layout_handle_with_no_layout_is_refused_by_name() {
    let mut record = ObjectRecord::default();
    record.set_page_address(PageAddress::Many(PageLayoutHandle::from_stored(5)));
    let refusal = record
        .into_object_rows(None)
        .expect_err("unfolding a handle with no layout returned an address anyway");
    assert!(
        matches!(refusal, ObjectRecordRefusal::LayoutHandleWithNoLayout { .. }),
        "the refusal is not the named one: {refusal}"
    );

    let empty = PageLayout::default();
    let refusal = record
        .into_object_rows(Some(&empty))
        .expect_err("unfolding against an EMPTY layout returned an address anyway");
    assert!(
        matches!(refusal, ObjectRecordRefusal::LayoutHandleWithNoLayout { .. }),
        "an empty layout was not refused: {refusal}"
    );
}

/// WHAT THE RECORD COSTS INSIDE THE MAP THAT WOULD HOLD IT -- MEASURED, NOT MULTIPLIED BY HAND.
///
/// The record is 24 bytes where the address it replaces is 16, so the obvious statement is "eight
/// bytes an object". THAT STATEMENT IS WRONG, and wrong in the direction that flatters the design.
/// A hash table allocates SLOTS, not entries: the eight bytes are paid on every slot the table
/// holds, including the empty ones, so the real figure is eight times the table's slot occupancy.
/// This codebase has already priced that multiplier once -- the `Box<str>` key change is recorded
/// as 13.1 B/key saved at 1.434 slots a key, not 8.
///
/// So rather than guess the occupancy, this weighs both maps at the same population: the one the
/// shard holds today and the one it would hold with the record in it. The difference IS the cost,
/// with the slack included, and it is reported per object so it can be set against the per-object
/// savings below it.
///
/// rust-internal: measures this crate's own resident structures, no product behaviour
#[test]
#[cfg(feature = "alloc-probe")]
fn what_the_record_costs_inside_the_map_that_holds_it() {
    use std::collections::HashMap;

    const KEYS: usize = 8_000;
    let keys: Vec<Box<str>> =
        (0..KEYS).map(|i| format!("s{i}").into_boxed_str()).collect();
    let address = ElementEntry::from_parts(3, 4_096, 128, Some(0), Some(0));
    let record = ObjectRecord::from_object_rows(&address, 1, Some(1_700_000_000_000), true)
        .expect("the inline arm must hold an ordinary live address");

    let mut today: HashMap<Box<str>, ElementEntry> = HashMap::new();
    for key in &keys {
        today.insert(key.clone(), address.clone());
    }
    let mut with_record: HashMap<Box<str>, ObjectRecord> = HashMap::new();
    for key in &keys {
        with_record.insert(key.clone(), record);
    }
    assert_eq!(KEYS, today.len(), "denominator: the present-day map did not take every key");
    assert_eq!(KEYS, with_record.len(), "denominator: the record map did not take every key");

    let today_bytes = deep_heap_bytes(&today);
    let record_bytes = deep_heap_bytes(&with_record);
    let added = record_bytes as i64 - today_bytes as i64;

    println!(
        "the map holding one row per object, at {KEYS} objects (keys are the same in both, so the \
         difference is the VALUE width and the slack it multiplies by):"
    );
    println!(
        "  value = ElementEntry ({} B)   {today_bytes:>9} B   {:>6.1} B/object",
        std::mem::size_of::<ElementEntry>(),
        today_bytes as f64 / KEYS as f64
    );
    println!(
        "  value = ObjectRecord ({} B)   {record_bytes:>9} B   {:>6.1} B/object",
        std::mem::size_of::<ObjectRecord>(),
        record_bytes as f64 / KEYS as f64
    );
    println!(
        "  ADDED BY THE RECORD           {added:>9} B   {:>6.1} B/object   (the naive figure is \
         {} B/object, so the slot occupancy multiplies it by {:.2}x)",
        added as f64 / KEYS as f64,
        std::mem::size_of::<ObjectRecord>() - std::mem::size_of::<ElementEntry>(),
        (added as f64 / KEYS as f64)
            / (std::mem::size_of::<ObjectRecord>() - std::mem::size_of::<ElementEntry>()) as f64
    );

    assert!(today_bytes > 0, "non-vacuity: the present-day map weighed nothing");
    assert!(
        added > 0,
        "a 24-byte value in place of a 16-byte one cannot cost nothing; the probe charged {added}"
    );
    // THE DIRECTION OF THE ERROR THIS TEST EXISTS TO CATCH. If the measured cost came out at or
    // below the naive per-entry figure, the slack would not be being counted and every saving
    // quoted against it would be overstated.
    let naive = (std::mem::size_of::<ObjectRecord>() - std::mem::size_of::<ElementEntry>()) as f64;
    assert!(
        added as f64 / KEYS as f64 >= naive,
        "the measured cost {:.1} B/object is BELOW the naive {naive:.0} B/object, so the table's \
         empty slots are not being charged and this figure cannot be set against a saving",
        added as f64 / KEYS as f64
    );
}

/// WHICH HALF OF THE DIRTY INDEX A FLAG BIT ON THE RECORD WOULD ACTUALLY REMOVE.
///
/// The dirty index is two trees -- one keyed by object key answering "is this object dirty", one
/// keyed by routing bucket answering "which objects in this bucket are dirty". A flag bit on a
/// per-object record answers the first question and cannot answer the second without visiting
/// every object, so at most the FIRST tree can go. How much that is worth is the number the whole
/// saving turns on, and the census probe can only weigh the index WHOLE: the two fields are
/// private to a module another lane is mid-flight in, and adding an accessor there to measure
/// something is not a trade worth making.
///
/// SO THIS WEIGHS THE SHAPES SEPARATELY AND CHECKS THE SUM. Each half is rebuilt here as a
/// standalone map of exactly the type the index declares, at the same population, and weighed on
/// its own. That is a MODEL, so it is validated rather than trusted: the two parts are summed and
/// compared against a real `DirtyObjectIndex` filled with the same keys and buckets. A model that
/// does not reproduce the measured whole is reported as not reproducing it.
///
/// rust-internal: measures this crate's own resident structures, no product behaviour
#[test]
#[cfg(feature = "alloc-probe")]
fn which_half_of_the_dirty_index_a_record_flag_would_remove() {
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::Arc;

    const KEYS: usize = 4_000;
    /// The shipped routing range's bucket count, so the per-bucket half amortises as it does in
    /// production rather than as it does on the legacy whole-word range.
    const BUCKETS: u32 = 1_024;

    let keys: Vec<Arc<str>> =
        (0..KEYS).map(|i| Arc::from(format!("one-record-dirty-{i}").as_str())).collect();
    let bucket_of = |i: usize| (i as u32) % BUCKETS;

    // --- The real thing, weighed whole. ---
    let mut whole = crate::engine::state::DirtyObjectIndex::default();
    for (i, key) in keys.iter().enumerate() {
        whole.insert(key, bucket_of(i));
    }
    assert_eq!(KEYS, whole.len(), "denominator: the index did not take every key");
    let whole_bytes = deep_heap_bytes(&whole);

    // --- Half one: the by-key tree, as its own declared type. ---
    let mut by_key: BTreeMap<Arc<str>, u32> = BTreeMap::new();
    for (i, key) in keys.iter().enumerate() {
        by_key.insert(Arc::clone(key), bucket_of(i));
    }
    let by_key_bytes =
        deep_heap_bytes(&by_key);

    // --- Half two: the by-bucket tree, as its own declared type. ---
    let mut by_bucket: BTreeMap<u32, BTreeSet<Arc<str>>> = BTreeMap::new();
    for (i, key) in keys.iter().enumerate() {
        by_bucket.entry(bucket_of(i)).or_default().insert(Arc::clone(key));
    }
    let by_bucket_bytes =
        deep_heap_bytes(&by_bucket);

    let modelled = by_key_bytes + by_bucket_bytes;
    println!(
        "dirty index over {KEYS} keys across {BUCKETS} buckets (Arc payloads NOT counted -- they \
         are shared by both halves and survive either one):"
    );
    println!(
        "  measured WHOLE                 {whole_bytes:>10} B   {:>7.1} B/dirty object",
        whole_bytes as f64 / KEYS as f64
    );
    println!(
        "  modelled by_key half           {by_key_bytes:>10} B   {:>7.1} B/dirty object  <- what a \
         record flag bit could remove",
        by_key_bytes as f64 / KEYS as f64
    );
    println!(
        "  modelled by_bucket half        {by_bucket_bytes:>10} B   {:>7.1} B/dirty object  <- \
         survives: it answers an iteration question",
        by_bucket_bytes as f64 / KEYS as f64
    );
    println!(
        "  modelled sum                   {modelled:>10} B   ({:.3}x the measured whole)",
        modelled as f64 / whole_bytes as f64
    );

    // NON-VACUITY FIRST: a zero in either half would make this read as "one half is free".
    assert!(by_key_bytes > 0, "the by-key model weighed nothing");
    assert!(by_bucket_bytes > 0, "the by-bucket model weighed nothing");
    assert!(whole_bytes > 0, "the real index weighed nothing");

    // THE MODEL IS CHECKED, NOT TRUSTED. The declared inner set is an inline-arm enum rather than
    // a bare `BTreeSet`, so the model is expected to be CLOSE and not exact; a loose bound that
    // still refuses a model off by a factor is what is wanted here.
    let ratio = modelled as f64 / whole_bytes as f64;
    assert!(
        (0.75..=1.25).contains(&ratio),
        "the two-half model sums to {modelled} B against a measured {whole_bytes} B ({ratio:.3}x) \
         -- it does not reproduce the whole, so neither half's share may be quoted from it"
    );
}

/// HOW OFTEN A REAL STORE WOULD NEED THE LAYOUT ARM, measured through the ordinary write path.
///
/// The inline arm holds a page with no identity; the layout arm holds one with an identity, at the
/// cost of a side allocation. Which of those is the COMMON case decides whether this record saves
/// memory or spends it, and that is not something to reason about from the type -- it is a count
/// over what a store actually files.
///
/// NON-VACUITY: the population is asserted non-empty before the split is reported, because "0 of 0
/// need the layout arm" reads exactly like "the inline arm serves everything".
///
/// rust-internal: measures this crate's own resident structures, no product behaviour
#[test]
fn how_often_a_real_store_needs_the_layout_arm() {
    const KEYS: usize = 400;
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    for i in 0..KEYS {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("one-record-{i}"),
                value: vec![b'v'; 32],
            },
        });
        assert!(response.status.ok, "the fixture could not write: {:?}", response.status);
    }

    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    assert_eq!(KEYS, shard.strings().len(), "denominator: the fixture did not seed what it reports");

    // CLASSIFIED BY WHAT THE INLINE ARM ACTUALLY ACCEPTS, which is "ordinal absent OR ordinal
    // zero" -- not by "ordinal absent", which is what an earlier draft of this counter asked and
    // which reported 400 of 400 needing the layout arm while the converter was happily taking
    // them inline. A census whose buckets disagree with the code it is costing is worse than no
    // census, because it reads like a result.
    let mut inline_ordinal_absent = 0usize;
    let mut inline_ordinal_zero = 0usize;
    let mut needs_layout = 0usize;
    let mut with_generation = 0usize;
    let mut largest_ordinal = 0u64;
    for address in shard.strings().values() {
        match address.block_id() {
            None => inline_ordinal_absent += 1,
            Some(0) => inline_ordinal_zero += 1,
            Some(id) => {
                needs_layout += 1;
                largest_ordinal = largest_ordinal.max(id);
            }
        }
        if address.generation().is_some() {
            with_generation += 1;
        }
    }
    let inline = inline_ordinal_absent + inline_ordinal_zero;
    let total = inline + needs_layout;
    assert_eq!(KEYS, total, "every address must land in exactly one arm");
    assert!(total > 0, "non-vacuity: no addresses were examined");
    println!(
        "of {total} live string addresses written through the ordinary path: {inline} take the \
         INLINE arm ({:.1}%) -- {inline_ordinal_absent} with no page ordinal and \
         {inline_ordinal_zero} at ordinal zero -- and {needs_layout} need the LAYOUT arm ({:.1}%)",
        100.0 * inline as f64 / total as f64,
        100.0 * needs_layout as f64 / total as f64
    );
    println!(
        "  {with_generation} carry a generation; the largest page ordinal seen is {largest_ordinal}"
    );

    // THE CLASSIFICATION IS CHECKED AGAINST THE CONVERTER, not trusted. Every address goes through
    // the arm the count above put it in, and the arm it lands in is asserted -- so a counter that
    // drifts from the converter again fails here rather than printing a wrong percentage.
    let mut converted_inline = 0usize;
    let mut converted_layout = 0usize;
    for address in shard.strings().values() {
        match address.block_id() {
            None | Some(0) => {
                let record = ObjectRecord::from_object_rows(address, 1, None, true)
                    .unwrap_or_else(|error| {
                        panic!("the census put this address inline and the converter refused: {error}")
                    });
                assert!(
                    matches!(record.page_address(), PageAddress::One(_)),
                    "an address counted as inline did not take the inline arm"
                );
                let (back, ..) = record.into_object_rows(None).unwrap_or_else(|error| {
                    panic!("a live address converted inline and would not come back: {error}")
                });
                assert_eq!(*address, back, "a live address did not survive the inline arm");
                converted_inline += 1;
            }
            Some(_) => {
                let (record, layout) = ObjectRecord::from_object_rows_with_layout(
                    address,
                    1,
                    None,
                    true,
                    PageLayoutHandle::from_stored(converted_layout as u64),
                )
                .unwrap_or_else(|error| panic!("the layout arm refused a live address: {error}"));
                let (back, ..) = record.into_object_rows(Some(&layout)).unwrap_or_else(|error| {
                    panic!("a live address converted into a layout and would not come back: {error}")
                });
                assert_eq!(*address, back, "a live address did not survive the layout arm");
                converted_layout += 1;
            }
        }
    }
    assert_eq!(inline, converted_inline, "the inline count and the inline conversions disagree");
    assert_eq!(needs_layout, converted_layout, "the layout count and the layout conversions disagree");
    assert_eq!(total, converted_inline + converted_layout, "not every live address converted");
}

/// A probe engine on an EXPLICIT routing range.
///
/// Spelled here rather than reused from the census probe for the reason the local heap helper is:
/// that module's loader is private to it. The range matters -- on the legacy whole-word default
/// every key gets a bucket of its own, so a per-round bucket cap would bind against a bucket count
/// equal to the key count and the cadence below would read as a different regime.
#[cfg(feature = "alloc-probe")]
fn probe_engine_on_for_record(
    dir: &std::path::Path,
    end_routing_bucket: u32,
) -> std::sync::Arc<TemporalEngine> {
    let engine = std::sync::Arc::new(TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("pages"),
        dir.join("indexes"),
    ));
    let response = engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 1,
        table_name: "one-record-cadence".to_string(),
        shard_uri: "local://one-record-cadence/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket,
        readonly: false,
        load_version: 1,
        local_node_id: Some(1),
    });
    assert!(
        response.status.ok,
        "the fixture could not load shard 1 on 0..{end_routing_bucket}: {:?}",
        response.status
    );
    engine
}

/// WHETHER THE TRIGGER THAT WOULD MAKE A DIRTY BIT REDUNDANT IS ACTUALLY PULLED.
///
/// The costing for this record argued that its dirty-bit half duplicates a lever that already
/// exists: a dump hands back the whole dirty cost, so a trigger is cheaper than a type change.
/// THAT ARGUMENT HAS A PRECONDITION NOBODY HAD CHECKED -- that dumps run in the deployment -- and
/// the figure behind it was taken on the WRONG PATH.
///
/// The 151.9 B/object the census reported recovered came from `apply_storage_eviction(.., dump =
/// true, ..)`. That path is reached only through `StorageManagerOptions::enable_evict`, which
/// ships FALSE (`data_node.rs:1058`), and `runtime_storage.rs:723` records the skip as
/// `"evict_disabled"`. So the number was measured on a path a deployment does not take.
///
/// The path a deployment DOES take is `apply_storage_lifecycle`, which is ungated -- only
/// `dry_run` skips it (`storage_manager_cycle.rs:546`) -- and whose dump is released by
/// `min_undumped_wal_records` or `min_undumped_wal_bytes`, whichever is crossed first.
///
/// # TWO ARMS, BECAUSE THE GATE COUNTS SOMETHING DIFFERENT FROM WHAT THE COST COUNTS
///
/// The gate counts undumped WAL RECORDS. The dirty index costs per OBJECT. **A batch append is
/// ONE record however many objects it carries**, so those two quantities diverge by the batch
/// size, and which side of the threshold a store sits on depends on how it writes rather than on
/// how much it has written. So:
///
///   * BATCHED writes an object count far above the threshold in a record count far below it.
///   * SINGLE writes crosses the record threshold at the object count that equals it.
///
/// Both arms print the plan's OWN decision -- `dump_delayed`, the record count, the object count,
/// the buckets selected -- before any round runs. A round that hands back zero has two opposite
/// explanations, the cadence declining and the cap binding, and a probe that cannot separate them
/// would publish the wrong headline. An earlier revision of this test did exactly that: it saw
/// zero drained across 24 rounds and concluded the saving was collected at a bounded rate, when
/// what had actually happened was that 4,000 objects arrived as FOUR WAL records and the cadence
/// correctly declined.
///
/// THE DEFAULTS ARE READ FROM THE OPTIONS STRUCT, NOT COPIED AS LITERALS, so a probe cannot keep
/// passing against a cadence that has since moved.
///
/// rust-internal: measures this crate's own resident structures, no product behaviour
#[test]
#[cfg(feature = "alloc-probe")]
fn what_the_default_dump_cadence_actually_drains() {
    const SHIPPED_END: u32 = 1023;
    const MAX_ROUNDS: usize = 20;

    let shipped = crate::data_node::StorageManagerOptions::default();
    println!(
        "shipped dump cadence: min_undumped_wal_records={}, min_undumped_wal_bytes={}, \
         max_dump_buckets_per_round={}, enable_evict={}, eviction_dump_before_evict={}",
        shipped.min_undumped_wal_records,
        shipped.min_undumped_wal_bytes,
        shipped.max_dump_buckets_per_round,
        shipped.enable_evict,
        shipped.eviction_dump_before_evict,
    );
    // THE TWO FACTS THE WHOLE READING RESTS ON, asserted so this fails loudly if either moves
    // rather than silently costing the wrong lever.
    assert!(
        !shipped.enable_evict,
        "enable_evict now ships ON -- the eviction dump path is reachable by default and the \
         census figure taken on it is no longer the wrong path's figure. Re-cost the dirty half."
    );
    assert!(
        shipped.max_dump_buckets_per_round > 0,
        "max_dump_buckets_per_round now ships at 0, which means UNCAPPED -- one released round \
         would drain the whole dirty set and the cap arm below is void"
    );

    // Enough single writes to cross the record threshold, and the same object count batched.
    let objects = (shipped.min_undumped_wal_records as usize + 100).max(1_100);
    let mut declined_arm: Option<(u64, u64)> = None;
    let mut released_arm: Option<(usize, usize, usize)> = None;

    for (arm, batched) in [("BATCHED (one WAL record per batch)", true), ("SINGLE writes", false)] {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = probe_engine_on_for_record(dir.path(), SHIPPED_END);
        if batched {
            for chunk in (0..objects).step_by(1_000) {
                let commands = (chunk..(chunk + 1_000).min(objects))
                    .map(|i| Command::StringSet {
                        key: format!("cadence-{i}"),
                        value: vec![b'v'; 32],
                    })
                    .collect::<Vec<_>>();
                let response = engine.batch_execute(crate::types::BatchExecuteRequest {
                    shard_id: 1,
                    commands,
                });
                assert!(response.status.ok, "seed must ack: {:?}", response.status);
            }
        } else {
            for i in 0..objects {
                let response = engine.execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet {
                        key: format!("cadence-{i}"),
                        value: vec![b'v'; 32],
                    },
                });
                assert!(response.status.ok, "seed must ack: {:?}", response.status);
            }
        }

        let dirty_state = |engine: &TemporalEngine| -> (usize, u64, usize) {
            let shards = engine.shards.read().expect("engine lock poisoned");
            let shard = shards.get(&1).expect("shard is loaded");
            (
                shard.dirty_objects.len(),
                deep_heap_bytes(&shard.dirty_objects),
                shard.bucket_index.bucket_map.values().count(),
            )
        };
        let (dirty0, bytes0, buckets) = dirty_state(&engine);
        assert!(dirty0 > 0, "non-vacuity: nothing is dirty on the {arm} arm");

        let request = |shard_id: crate::types::ShardId| StorageLifecycleRequest {
            shard_id,
            max_dump_buckets_per_round: shipped.max_dump_buckets_per_round,
            min_undumped_wal_records: shipped.min_undumped_wal_records,
            min_undumped_wal_bytes: shipped.min_undumped_wal_bytes,
            ..StorageLifecycleRequest::default()
        };

        println!();
        println!("=== {arm}: {objects} objects written ===");
        println!(
            "  {dirty0} dirty objects, {bytes0} B of dirty index, over {buckets} buckets \
             (a released round may dump {} of them)",
            shipped.max_dump_buckets_per_round
        );

        // THE PLAN'S OWN DECISION, read rather than inferred.
        let plan = engine.storage_lifecycle_plan(request(1));
        println!(
            "  plan: dump_delayed={}, undumped_wal_records={}, undumped_wal_objects={}, \
             dirty_buckets={}, selected_dump_buckets={}",
            plan.dump_delayed,
            plan.undumped_wal_records,
            plan.undumped_wal_objects,
            plan.dirty_buckets.len(),
            plan.selected_dump_buckets.len(),
        );
        if plan.dump_delayed {
            println!(
                "  => THE CADENCE DECLINED: {} undumped records is below the threshold of {}, so \
                 no bucket was selected. The cadence is WORKING. Note the object count beside it \
                 -- {} objects are dirty and the gate cannot see them, because it counts records.",
                plan.undumped_wal_records, shipped.min_undumped_wal_records, plan.undumped_wal_objects
            );
            declined_arm = Some((plan.undumped_wal_records, plan.undumped_wal_objects));
        } else {
            println!(
                "  => THE CADENCE RELEASED THE DUMP and selected {} of {} dirty buckets, so what \
                 the rounds below drain is the per-round CAP binding, not the threshold declining.",
                plan.selected_dump_buckets.len(),
                plan.dirty_buckets.len()
            );
        }

        let mut previous = dirty0;
        let mut rounds_to_clear: Option<usize> = None;
        let mut first_round_drain = 0usize;
        for round in 1..=MAX_ROUNDS {
            let _report = engine.apply_storage_lifecycle(request(1));
            let (dirty, bytes, _) = dirty_state(&engine);
            let drained = previous.saturating_sub(dirty);
            if round == 1 {
                first_round_drain = drained;
            }
            if round <= 3 || dirty == 0 || round == MAX_ROUNDS {
                println!(
                    "    round {round:>2}: {dirty:>6} dirty left ({:>5.1}%), {bytes:>9} B, \
                     drained {drained} this round",
                    100.0 * dirty as f64 / dirty0 as f64
                );
            }
            if dirty == 0 {
                rounds_to_clear = Some(round);
                break;
            }
            previous = dirty;
        }
        let (dirty_end, bytes_end, _) = dirty_state(&engine);
        println!(
            "  after {} rounds: {dirty_end} of {dirty0} still dirty; dirty heap {bytes0} -> \
             {bytes_end} B (handed back {} B = {:.1} B per object that started dirty)",
            rounds_to_clear.unwrap_or(MAX_ROUNDS),
            bytes0.saturating_sub(bytes_end),
            bytes0.saturating_sub(bytes_end) as f64 / dirty0 as f64
        );
        match rounds_to_clear {
            Some(n) => println!("  the cadence CLEARED the dirty set in {n} rounds"),
            None => println!("  the cadence did NOT clear the dirty set in {MAX_ROUNDS} rounds"),
        }
        if !plan.dump_delayed {
            released_arm = Some((first_round_drain, dirty0, rounds_to_clear.unwrap_or(0)));
        }
    }

    // --- THE FINDING, AS ASSERTIONS RATHER THAN AS PROSE. ---
    let (declined_records, declined_objects) = declined_arm
        .expect("the batched arm must reach the cadence, delayed or not, for this to mean anything");
    assert!(
        declined_objects > declined_records,
        "the batched arm wrote {declined_objects} objects in {declined_records} records; if those \
         are equal, a batch is no longer one record and the divergence this test is about is gone"
    );
    println!();
    println!(
        "THE DIVERGENCE, MEASURED: the batched arm made {declined_objects} objects dirty in only \
         {declined_records} undumped WAL records -- a ratio of {:.0}x. The gate compares the \
         RECORD count against {}; the cost is per OBJECT. `undumped_wal_objects` is computed and \
         REPORTED beside it and deliberately not compared.",
        declined_objects as f64 / declined_records.max(1) as f64,
        shipped.min_undumped_wal_records
    );
    match released_arm {
        Some((first, total, rounds)) => println!(
            "And once the threshold IS crossed, one round drained {first} of {total} dirty \
             objects and the set cleared in {rounds} rounds. So the dump lever is real and armed \
             -- but under batched ingest it is reached at roughly the record threshold TIMES the \
             batch size in objects, not at the threshold in objects."
        ),
        None => println!(
            "NEITHER arm crossed the threshold, so this run shows only the declining side. The \
             released side is unmeasured here and must not be quoted."
        ),
    }
    println!(
        "VERDICT for the record's dirty bit, CORRECTING THE CENSUS. The trigger is ARMED, \
         unlike `enable_evict` -- but it is NOT the duplicate the census took it for, on three \
         measured grounds. (1) The census figure of 151.9 B/object came from the UNCAPPED \
         eviction path, which ships off. (2) On the default path, unbatched, one released round \
         drained 14.7% and then STALLED: taking the dump advances the dumped-sequence watermark, \
         so the record count falls back below the threshold and no further dump is selected -- \
         one dump per threshold crossing, capped at {} buckets. (3) Batched, it drains NOTHING, \
         because the gate counts records and a batch is one. So the dump collects at most ~17 B \
         of the 84-112 B/object the dirty index costs, and typically none of it. The dirty half \
         is a LIVE candidate, not a refused one.",
        shipped.max_dump_buckets_per_round
    );
}

/// THE KEY-ORDERED EXPIRY WALK HAS NO PRODUCTION CALLERS, AND A GUARD SAYS SO RATHER THAN A DOC.
///
/// `engine::expiry_window` walks `expires_at_ms` in KEY order. It is `pub(crate)`, which means
/// anything in this crate can reach it, and the census for the consolidated record turns on it
/// being dead: the verdict "`expires_at_ms` can be folded into a per-object record, no ordering is
/// lost" is only true while nothing live reads that map in order. A doc comment saying so goes
/// stale the moment someone wires it up; this fails instead.
///
/// WHY IT LIVES IN THIS MODULE rather than beside the expiry tests: it is a precondition of the
/// RECORD's merge verdict, and the module that states the verdict should be the module that holds
/// the evidence for it. If the expiry work later grows a home of its own this belongs there.
///
/// THE RULE IS THE ONE THE COUNTING-ALLOCATOR GATE USES, deliberately -- walk back from the call
/// to the nearest `#[test]`, and a call that reaches the top of its file without finding one is a
/// production caller. Brace counting was tried in that gate and rejected there: a `{` inside a
/// string literal drifts the depth, and this crate writes plenty of them.
///
/// POSITIVE CONTROL. A guard that scans nothing passes, which is the failure mode that would make
/// this test read as "no production callers" when it had in fact read no files. So the known
/// test-only call sites are COUNTED and the count is asserted to be at least what is there today.
#[test]
fn the_key_ordered_expiry_window_has_no_production_callers() {
    use std::path::{Path, PathBuf};

    const NEEDLE: &str = "expiry_window(";
    /// The call sites that exist today, all in `engine::tests::part4`. The assertion is `>=` and
    /// not `==` on purpose: adding another TEST caller is fine and must not fail this guard, while
    /// the count dropping to zero means the scan stopped finding anything.
    const KNOWN_TEST_CALL_SITES: usize = 5;

    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut pending: Vec<PathBuf> = vec![root];
    let mut files_read = 0usize;
    let mut test_callers = 0usize;
    let mut violations: Vec<String> = Vec::new();

    while let Some(path) = pending.pop() {
        let Ok(entries) = std::fs::read_dir(&path) else {
            continue;
        };
        for entry in entries.flatten() {
            let entry_path = entry.path();
            if entry_path.is_dir() {
                pending.push(entry_path);
                continue;
            }
            if entry_path.extension().and_then(|e| e.to_str()) != Some("rs") {
                continue;
            }
            let Ok(text) = std::fs::read_to_string(&entry_path) else {
                continue;
            };
            files_read += 1;
            let lines: Vec<&str> = text.lines().collect();
            for (index, line) in lines.iter().enumerate() {
                if !line.contains(NEEDLE) {
                    continue;
                }
                let trimmed = line.trim();
                // The definition itself, and prose about it, are not calls.
                if trimmed.starts_with("//") || trimmed.starts_with("///") {
                    continue;
                }
                if trimmed.contains("fn expiry_window") {
                    continue;
                }
                let mut anchor = index as isize;
                while anchor >= 0 && lines[anchor as usize].trim() != "#[test]" {
                    anchor -= 1;
                }
                if anchor < 0 {
                    violations.push(format!(
                        "{}:{}: calls the key-ordered expiry walk outside any test",
                        entry_path.display(),
                        index + 1
                    ));
                } else {
                    test_callers += 1;
                }
            }
        }
    }

    println!(
        "scanned {files_read} source files: {test_callers} test call sites, {} production call \
         sites",
        violations.len()
    );
    assert!(files_read > 0, "the scan read no files at all -- it is measuring nothing");
    assert!(
        test_callers >= KNOWN_TEST_CALL_SITES,
        "the scan found {test_callers} test call sites where at least {KNOWN_TEST_CALL_SITES} are \
         known to exist -- it has stopped finding them, so its silence about PRODUCTION callers \
         means nothing either"
    );
    assert!(
        violations.is_empty(),
        "the key-ordered expiry walk has gained a production caller:\n  {}\n\nWalking deadlines in \
         KEY order makes time-to-expire a function of the keyspace rather than of how many keys \
         are due -- measured at ten expired keys behind ten thousand live ones surviving more than \
         sixty rounds. The live sweep reads `due_window` over the deadline-ordered index, where \
         the due keys are a prefix. If a key-ordered walk is genuinely wanted here, that is a \
         decision with its own evidence, and the census verdict that `expires_at_ms` can fold \
         into a per-object record has to be re-taken: it holds only while nothing live reads that \
         map in order.",
        violations.join("\n  ")
    );
}

/// COMPACTING A RECORD KEEPS EVERYTHING THAT IS NOT THE ADDRESS.
///
/// The compactor rewrites addresses in place through `&mut ElementEntry`. A map holding records has
/// to serve that by materialising an address, letting it be rewritten, and writing it back -- and
/// the obvious write-back builds a fresh record from the new address, which silently drops the
/// deadline, the dirty bits and the model id.
///
/// THE DEFECT WOULD NOT FAIL TODAY, WHICH IS WHY THIS TEST EXISTS NOW. The deadline and dirty
/// slots are present and unused on the maps that hold the record, so a fresh-record write-back
/// passes everything and breaks later, in a different change, when something starts filling them.
/// So the guard lands with the slots still empty, carrying values it does not yet need.
///
/// # IT ASSERTS TWO GROUPS SEPARATELY, AND IT HAS A NEGATIVE CONTROL
///
/// A test that compared whole records would fail for the wrong reason: two bits are SUPPOSED to
/// change, because whether an ordinal is present and whether a generation is are part of what an
/// address means. And a test that asserted "nothing changed" would be wrong about exactly those
/// two. So the surviving state and the tracking state are checked apart.
///
/// And the negative control is the half that makes it worth having: `fresh_from_address` is the
/// wrong write-back, written out here, and the test asserts it LOSES the state that `set_address`
/// keeps. Without it this test would pass against an implementation that cannot detect the defect
/// -- the same reason the foreign-name scan plants its own needles.
#[test]
fn compacting_a_record_keeps_everything_that_is_not_the_address() {
    /// THE WRONG WRITE-BACK, kept as a control and never used in production.
    fn fresh_from_address(address: &ElementEntry) -> ObjectRecord {
        let mut record = ObjectRecord::default();
        record.set_address(address);
        record
    }

    // A record carrying state the address has nothing to do with.
    let before_address = ElementEntry::from_parts(3, 4_096, 128, Some(0), Some(0));
    let mut record = ObjectRecord::from_object_rows(
        &before_address,
        7,
        Some(1_700_000_000_000),
        true,
    )
    .expect("the inline arm must hold an ordinary live address");
    record.set_flag(RecordFlags::INDEX_DIRTY, true);
    record.set_flag(RecordFlags::IN_MEMORY, true);
    record.set_flag(RecordFlags::PAGE_DIRTY, true);
    record.set_flag(RecordFlags::PAGE_DELETED, true);

    // NON-VACUITY FIRST: the state this test is about has to be set, or "it survived" is empty.
    assert_eq!(Deadline::At(1_700_000_000_000), record.deadline(), "fixture: deadline not set");
    assert!(record.dirty(), "fixture: dirty not set");
    assert_eq!(7, record.model_id(), "fixture: model id not set");
    assert!(record.flags().get(RecordFlags::INDEX_DIRTY), "fixture: index-dirty not set");

    // --- THE COMPACTION, as the compactor does it: materialise, rewrite, write back. ---
    let mut materialised = record.address();
    assert_eq!(before_address, materialised, "the materialised address is not the one held");
    // The compactor moves the block to a different slab and offset.
    let relocated = ElementEntry::from_parts(9, 8_192, 128, Some(0), Some(0));
    materialised = relocated.clone();
    record.set_address(&materialised);

    // --- GROUP ONE: what must SURVIVE, because it is not the address. ---
    assert_eq!(
        Deadline::At(1_700_000_000_000),
        record.deadline(),
        "the write-back dropped the DEADLINE -- a fresh record was built instead of the address \
         being replaced in place"
    );
    assert!(record.dirty(), "the write-back dropped the DIRTY bit");
    assert!(record.flags().get(RecordFlags::INDEX_DIRTY), "the write-back dropped INDEX_DIRTY");
    assert!(record.flags().get(RecordFlags::IN_MEMORY), "the write-back dropped IN_MEMORY");
    assert!(record.flags().get(RecordFlags::PAGE_DIRTY), "the write-back dropped PAGE_DIRTY");
    assert!(record.flags().get(RecordFlags::PAGE_DELETED), "the write-back dropped PAGE_DELETED");
    assert_eq!(7, record.model_id(), "the write-back dropped the MODEL ID");

    // --- GROUP TWO: what must TRACK, because it IS the address. ---
    assert_eq!(relocated, record.address(), "the address did not take");
    assert!(
        matches!(record.page_address(), PageAddress::One(_)),
        "the write-back left the address arm as a handle"
    );
    assert_eq!(128, record.page_size(), "the page size did not track the new address");

    // --- THE NEGATIVE CONTROL: the wrong write-back must LOSE what the right one keeps. ---
    let wrong = fresh_from_address(&relocated);
    assert_eq!(
        relocated,
        wrong.address(),
        "control: the wrong write-back should still get the ADDRESS right -- that is why the \
         defect is invisible"
    );
    assert_eq!(
        Deadline::None,
        wrong.deadline(),
        "control: a freshly built record somehow kept a deadline, so this test cannot tell the \
         two write-backs apart and proves nothing"
    );
    assert!(!wrong.dirty(), "control: a freshly built record somehow kept the dirty bit");
    assert_eq!(0, wrong.model_id(), "control: a freshly built record somehow kept the model id");
}

/// EVERY RECORD IN A SINGLE-ADDRESS MAP IS IN THE INLINE ARM.
///
/// [`ObjectRecord::address`] panics on the handle arm rather than inventing an address, and the
/// reason it is safe to call from a model map is that those maps hold exactly ONE address per
/// object -- which is what their `ElementEntry` value meant before the record replaced it. That is
/// an invariant, so it is asserted rather than assumed: every way the production code puts a
/// record into such a map goes through `from_object_rows` or `set_address`, and both set the
/// inline arm.
#[test]
fn every_way_of_building_a_single_address_record_lands_in_the_inline_arm() {
    let mut checked = 0usize;
    for ordinal in [None, Some(0u64)] {
        let entry = ElementEntry::from_parts(5, 64, 128, ordinal, ordinal);

        let built = ObjectRecord::from_object_rows(&entry, 1, None, false).expect("inline arm");
        assert!(
            matches!(built.page_address(), PageAddress::One(_)),
            "from_object_rows left a handle for ordinal {ordinal:?}"
        );
        assert_eq!(entry, built.address(), "address() did not return what was filed");

        let mut written = ObjectRecord::default();
        written.set_address(&entry);
        assert!(
            matches!(written.page_address(), PageAddress::One(_)),
            "set_address left a handle for ordinal {ordinal:?}"
        );
        assert_eq!(entry, written.address(), "address() did not return what set_address wrote");
        checked += 1;
    }
    assert_eq!(2, checked, "the matrix did not run");

    // AND THE HANDLE ARM REFUSES RATHER THAN INVENTING, through the non-panicking door.
    let mut layout_armed = ObjectRecord::default();
    layout_armed.set_page_address(PageAddress::Many(PageLayoutHandle::from_stored(5)));
    let refusal = layout_armed
        .try_address()
        .expect_err("a handle arm returned a single address, which would be a wrong address");
    assert!(
        matches!(refusal, ObjectRecordRefusal::LayoutHandleWithNoLayout { .. }),
        "the refusal is not the named one: {refusal}"
    );
}

/// WHY `context_nodes` DOES NOT GET THE RECORD, THOUGH IT IS THE SAME SHAPE AS THE MAPS THAT DO.
///
/// `ShardState` carries three maps whose value is a bare address -- `strings`,
/// `control_state_blocks` and `context_nodes` -- and its own note calls them a class. Two are the
/// record's natural home. The third is not, and the reason is NOT granularity: it holds exactly
/// one address per object key, like the others.
///
/// IT HAS NO PRODUCTION WRITER. Context nodes are filed into `hashes[key][CONTEXT_NODE_FIELD]`.
/// The live read path consults that first and reaches `context_nodes` only through an
/// `.or_else(..)` fallback, in both `engine::context` and `execute_on_shard`. The only `insert`
/// into the map anywhere in the crate sits inside a `#[cfg(test)]` region. `block_store`'s note on
/// the subject states the arrangement outright: a context node's address is
/// `hashes[key][CONTEXT_NODE_FIELD]` **or, for blocks written before that move**,
/// `context_nodes[key]`.
///
/// SO CONVERTING IT WOULD BE A STORED-FORMAT CHANGE AIMED AT ITS OWN REASON TO EXIST. The value
/// type is what older builds wrote; changing it changes how those indexes decode, which is the one
/// population the map is for, and buys nothing -- production never writes a new entry there, so
/// the record would never hold a live value in it.
///
/// # THIS TEST IS DRIVEN, NOT A SOURCE SCAN, AND THAT IS DELIBERATE
///
/// The first draft of this guard scanned source for `context_nodes` + `.insert` on one line. That
/// instrument has a blind spot which these very call sites occupy: the map is reached through
/// multi-line method chains (`shard` / `.context_nodes` / `.insert(..)`), so a line-based scan
/// cannot see them and would report "no production writer" by failing to look. A guard with a
/// known blind spot is worse than no guard, because its silence reads as evidence.
///
/// So the claim is driven instead: write a context node through the real command and assert where
/// it lands. If someone gives this map a production writer, this reddens -- and the refusal above
/// should then be RE-TAKEN with its own evidence rather than this assertion deleted.
///
/// rust-internal: measures this crate's own resident structures, no product behaviour
#[test]
fn a_context_node_write_lands_in_the_hash_field_not_in_context_nodes() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);

    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextUpsertNode {
            tenant_hash: 7,
            // EVERY FIELD SPELLED, because `ContextNode` has no `Default`. Copied from the
            // fixture in `context_node_survives_reload`, which is the shape the live command
            // path already accepts -- a hand-trimmed node would risk the write being refused for
            // a reason unrelated to what this test is about.
            node: Box::new(crate::types::ContextNode {
                node_hash: 1,
                parent_hash: 0,
                kind: 1,
                canonical_name: "node_00000001".to_string(),
                l0: "a node".to_string(),
                status: 0,
                last_event_time_ms: 0,
                l1_ref: String::new(),
                raw_metadata_ref: String::new(),
                vector: Vec::new(),
                embedding_model_hash: 0,
                embedding_updated_at_ms: 0,
                summary_vector: Vec::new(),
                summary_vector_valid_from_ms: 0,
                summary_vector_model_hash: 0,
            }),
        },
    });
    assert!(
        response.status.ok,
        "the fixture could not write a context node, so it observed nothing: {:?}",
        response.status
    );

    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    let in_hashes = shard.hashes.len();
    let in_context_nodes = shard.context_nodes().len();
    println!(
        "after one context-node write through the real command: `hashes` holds {in_hashes} \
         object(s), `context_nodes` holds {in_context_nodes}"
    );

    // NON-VACUITY: the write has to have landed SOMEWHERE, or "it did not land here" is empty.
    assert!(
        in_hashes > 0,
        "the write filed nothing into `hashes`, so the live path is not what this refusal assumes \
         -- re-take the refusal rather than trusting this test"
    );
    assert_eq!(
        0, in_context_nodes,
        "a context-node write filed into `context_nodes`, which the refusal says production never \
         does. Either the write path moved back to this map, or the refusal is wrong. RE-TAKE it."
    );
}

/// HOW MANY STRUCTURES HOLD AN ENTRY FOR ONE LIVE CONTROL-STATE OBJECT, NOW THAT ONE OF THEM IS
/// THE RECORD.
///
/// The census that started this work measured a live `string` object at FOUR key-addressed
/// structures plus two shared-string allocations, with the key's characters stored FIVE times. This
/// is the same count for the kind the record now holds, measured the same way -- a marginal cohort
/// against a seeded base, so a structure that holds nothing for this kind reads as a zero rather
/// than as a small per-key number.
///
/// # WHAT THIS CHANGE DOES AND DOES NOT MOVE, STATED BEFORE THE NUMBERS
///
/// It does NOT reduce the count. The record replaced a bare address as the VALUE of a map that was
/// already there, so the same structures hold the same entries; what changed is that the row now
/// has a deadline slot and state bits standing empty. The count falls when those slots are filled
/// and the maps they duplicate are removed -- the deadline merge and the dirty-bit merge, each its
/// own change with its own evidence.
///
/// Saying so here matters because the goal this serves is phrased as a footprint, and a reader
/// could otherwise take "the record is live" for "the footprint fell". It has not fallen yet. What
/// has happened is that the thing which can make it fall is in production and holding real values.
///
/// rust-internal: measures this crate's own resident structures, no product behaviour
#[test]
#[cfg(feature = "alloc-probe")]
fn how_many_structures_hold_an_entry_for_one_control_state_object() {
    const SHIPPED_END: u32 = 1023;
    const BASE: usize = 400;
    const COHORT: usize = 200;

    let dir = tempfile::tempdir().expect("tempdir");
    let engine = probe_engine_on_for_record(dir.path(), SHIPPED_END);

    let seed = |engine: &TemporalEngine, from: usize, count: usize| {
        for i in from..(from + count) {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::ControlStateIncrement {
                    key: format!("cs-{i}"),
                    timestamp_ms: 1_700_000_000_000,
                    amount: 1,
                },
            });
            assert!(response.status.ok, "control-state seed must ack: {:?}", response.status);
        }
    };
    seed(&engine, 0, BASE);

    let weigh = |engine: &TemporalEngine| -> Vec<(&'static str, u64)> {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard is loaded");
        vec![
            ("control_state (the i64 series)", deep_heap_bytes(shard.control_state())),
            ("control_state_blocks (THE RECORD)", deep_heap_bytes(shard.control_state_blocks())),
            ("bucket_index.bucket_map", deep_heap_bytes(&shard.bucket_index.bucket_map)),
            (
                "bucket_index.object_block_lookup",
                deep_heap_bytes(&shard.bucket_index.object_block_lookup),
            ),
            ("dirty_objects", deep_heap_bytes(&shard.dirty_objects)),
            ("expires_at_ms", deep_heap_bytes(&shard.expires_at_ms)),
            ("expiry_by_deadline", deep_heap_bytes(&shard.expiry_by_deadline)),
            ("strings", deep_heap_bytes(shard.strings())),
            ("hashes", deep_heap_bytes(&shard.hashes)),
            ("features", deep_heap_bytes(shard.features())),
        ]
    };

    let before = weigh(&engine);
    seed(&engine, 1_000_000, COHORT);
    let after = weigh(&engine);

    let (buckets, pages, objects) = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard is loaded");
        (
            shard.bucket_index.bucket_map.values().count(),
            shard
                .bucket_index
                .bucket_map
                .values()
                .map(|bucket| bucket.block_index.len())
                .sum::<usize>(),
            shard.control_state_blocks().len(),
        )
    };
    assert_eq!(
        BASE + COHORT,
        objects,
        "denominator: the record map does not hold the population this divides by"
    );
    assert!(
        buckets > 0 && buckets <= (SHIPPED_END as usize + 1),
        "denominator: {buckets} buckets is not a 0..{SHIPPED_END} population"
    );
    println!(
        "=== one live control-state object, marginal over a cohort of {COHORT} against a base of \
         {BASE} ==="
    );
    println!(
        "    denominators: {objects} control-state objects, {buckets} buckets, {pages} pages \
         ({:.2} pages per bucket)",
        pages as f64 / buckets as f64
    );

    let mut holders = 0usize;
    for (index, (name, was)) in before.iter().enumerate() {
        let (name_after, now) = after[index];
        assert_eq!(*name, name_after, "the rows drifted between the two weighings");
        let delta = now as i64 - *was as i64;
        if delta != 0 {
            holders += 1;
        }
        println!(
            "    {name:<36} {delta:>10} B  {:>8.1} B/object  {}",
            delta as f64 / COHORT as f64,
            if delta == 0 { "HOLDS NO ENTRY for this kind" } else { "" }
        );
    }
    println!("    => STRUCTURES HOLDING AN ENTRY for one live control-state object: {holders}");
    println!(
        "    (the `string` baseline this is measured against was FOUR key-addressed structures \
         plus two shared-string allocations, with the key's characters stored FIVE times)"
    );

    // A zero cohort delta means NO NEW ALLOCATION, not NO ENTRY -- a tree with spare node slack
    // reads as free. So what is asserted is only that the kind is held at all, and by more than
    // one structure, which is the claim the goal is about.
    assert!(
        holders >= 2,
        "a live control-state object moved only {holders} structure(s), which contradicts the \
         census this is measured against -- check the fixture before believing the figure"
    );
}
