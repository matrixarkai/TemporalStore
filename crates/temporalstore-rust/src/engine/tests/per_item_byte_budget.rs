// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A BYTE BUDGET for the structures that exist once per stored item.
//!
//! Most of this engine reaches for a 64-bit integer by default and that is the right default: a
//! sequence number, a hash, a timestamp and a byte count all want the full width, and a structure
//! that exists once per shard can be as fat as it likes. `ShardState` is 1,888 bytes and there is
//! one of it. What is different about the handful of structures below is that their count is the
//! CORPUS: one per stored address, one per page-index entry, one per routing bucket. At those
//! counts a byte is a megabyte, and nothing in this tree stopped one of them growing.
//!
//! WHAT THIS MODULE IS FOR. Two things, and the second is the durable one:
//!
//!   * `what_the_per_item_structures_cost_at_two_corpus_sizes` measures `size_of` x COUNT at two
//!     corpus sizes ten times apart, so a structure is ranked by what it actually costs the store
//!     rather than by how wide it looks. A fat struct with one instance is not a finding.
//!   * `every_per_item_structure_states_its_width_and_its_padding` pins each width against a
//!     literal and reports the FIELD SUM beside it. The literals are the budget. A field added to
//!     any of these structures fails this test by name, which is the thing that did not exist
//!     before: the widths below have moved several times and nothing ever failed.
//!
//! WIDTH IS NOT THE SAME PROBLEM AS PADDING, and the two want opposite fixes. Rust reorders
//! fields, so a structure whose size exceeds its field sum is wasting ALIGNMENT, and narrowing a
//! field in it changes nothing at all -- `BlockLookupRef` is 12 bytes of field in 16 bytes of
//! struct and no narrowing of either field moves it. A structure whose fields fill it is wasting
//! WIDTH, and narrowing one helps only if the sum crosses an 8-byte boundary. `BlockAddress` is
//! the only candidate here where it does, and only when TWO fields narrow together: one alone
//! takes 53 bytes of field to 49 and leaves the struct at 56.
//!
//! THERE IS A THIRD THING A FIELD CAN BE WASTING, and `BucketNode` was. An `Option<u64>` is 16
//! bytes for 8 bytes of number, because a `u64` has no value it does not use and the
//! discriminant has nowhere to go but a word of its own. That is neither width nor alignment:
//! the number is exactly as wide as it needs to be and the struct has no slack to reclaim. It is
//! a DISCRIMINANT with no niche to sit in, and the fix is to make one -- `Option<NonZeroU64>` is
//! 8. `every_byte_of_the_bucket_node_is_accounted_for` states the whole of that structure field
//! by field, `what_each_declined_shape_of_the_bucket_node_would_cost` prices the three shapes
//! that were considered for it and declined, and
//! `the_stored_spelling_of_a_bucket_node_did_not_move` drives the constraint that decides which
//! of them can be taken at all.
//!
//! THE COUNTS ARE MEASURED, NOT ASSUMED. Every denominator below is read off a seeded shard and
//! asserted non-zero before anything is divided by it, because a walk over an empty shard reports
//! "0 bytes over 0 items", which reads exactly like a structure that costs nothing.
#![allow(clippy::all)]
use super::*;
use std::mem::{align_of, size_of};
use std::sync::Arc;

use crate::block_store::{BlockAddress, BlockStoreSlabDescriptor};
use crate::engine::state::{
    BlockIndex, BlockIndexMap, BlockLookupRef, BlockRefs, BucketFlags, BucketLayoutState, BucketNode,
    BucketTtl,
    ComponentBlocks, ComponentList, DeletedObjectIndex, DirtyKeySet, ObjectBlockRefs, ObjectIndex,
    WalResidentBlock,
};

// Imported as a NAME rather than spelled out at the call site: the counting-allocator gate in
// `alloc_probe.rs` walks back from every line quoting the probe's full path to the nearest
// `#[test]`, and a helper spelling it out would be reported as reading the probe outside a test.
#[cfg(feature = "alloc-probe")]
use crate::alloc_probe::Probe;
use crate::index_log::{IndexItem, IndexItemKind, SlabCatalogEntry};

/// One structure in the budget: what it costs, and what its own fields add up to.
///
/// `fields` is written out as a sum of `size_of` over the DECLARED field types rather than as a
/// number, so it is a statement about the declaration and moves when the declaration does.
struct Budgeted {
    name: &'static str,
    size: usize,
    align: usize,
    /// For a struct, the sum of its field widths. For an ENUM, the width of its widest arm --
    /// so the slack reported beside it is the discriminant plus any alignment, not padding
    /// alone. `ObjectIndex` reads as 8 bytes of arm in 16 for exactly that reason, and it is not
    /// waste that a narrower field could reclaim.
    fields: usize,
    /// Whether the count of this structure grows with the corpus. A `false` row is a CONTROL:
    /// it is here to be ranked last, and it is the reason "rank by width" is the wrong ranking.
    per_item: bool,
}

impl Budgeted {
    fn padding(&self) -> usize {
        self.size.saturating_sub(self.fields)
    }
}

/// THE BUDGET. Each `size` literal is the ceiling; each `fields` expression is what the
/// declaration adds up to today.
fn budget() -> Vec<Budgeted> {
    let arc_str = size_of::<Arc<str>>();
    let opt_arc_str = size_of::<Option<Arc<str>>>();
    let opt_u64 = size_of::<Option<u64>>();
    let string = size_of::<String>();
    vec![
        Budgeted {
            name: "BlockAddress",
            size: size_of::<BlockAddress>(),
            align: align_of::<BlockAddress>(),
            // address               : u64  (slab id in the high 32 bits, offset in the low 32)
            // object_id             : u64
            // length, block_id      : u32 x 2
            // routing_bucket        : u32
            // present               : u8
            //
            // `generation` was a fourth u64 here until it became derived from
            // `block_id.or(object_id)`; it is not a field any more, so it is not a row here.
            // `block_slab_id` and `offset` were two more u64s until they became the two halves of
            // `address` -- two rows became one for the same reason, and by the same eight bytes.
            fields: 2 * size_of::<u64>() + 3 * size_of::<u32>() + size_of::<u8>(),
            per_item: true,
        },
        Budgeted {
            name: "BlockIndex",
            size: size_of::<BlockIndex>(),
            align: align_of::<BlockIndex>(),
            // object_key, model_id : Arc<str> x 2
            // component            : Option<Arc<str>>
            // address              : BlockAddress
            // dirty/deleted/log_backed : bool x 3
            fields: 2 * arc_str + opt_arc_str + size_of::<BlockAddress>() + 3 * size_of::<bool>(),
            per_item: true,
        },
        Budgeted {
            name: "BlockIndexMap",
            size: size_of::<BlockIndexMap>(),
            align: align_of::<BlockIndexMap>(),
            // Widest arm: One(u64, BlockIndex). The discriminant rides a niche in the page.
            fields: size_of::<u64>() + size_of::<BlockIndex>(),
            per_item: true,
        },
        Budgeted {
            name: "BucketNode",
            size: size_of::<BucketNode>(),
            align: align_of::<BucketNode>(),
            // routing_bucket u32, layout, the packed flag byte, ttl_ms BucketTtl, THREE u64
            // sequences, the live object index, the tombstone index, one BlockIndexMap
            fields: size_of::<u32>()
                + size_of::<BucketLayoutState>()
                + size_of::<BucketFlags>()
                + size_of::<BucketTtl>()
                + 3 * size_of::<u64>()
                + size_of::<ObjectIndex>()
                + size_of::<DeletedObjectIndex>()
                + size_of::<BlockIndexMap>(),
            per_item: true,
        },
        Budgeted {
            name: "BlockLookupRef",
            size: size_of::<BlockLookupRef>(),
            align: align_of::<BlockLookupRef>(),
            fields: size_of::<u32>() + size_of::<u64>(),
            per_item: true,
        },
        Budgeted {
            name: "BlockRefs",
            size: size_of::<BlockRefs>(),
            align: align_of::<BlockRefs>(),
            // Widest arm: Many(Vec<BlockLookupRef>).
            fields: size_of::<Vec<BlockLookupRef>>(),
            per_item: true,
        },
        Budgeted {
            name: "ComponentBlocks",
            size: size_of::<ComponentBlocks>(),
            align: align_of::<ComponentBlocks>(),
            fields: opt_arc_str + size_of::<BlockRefs>(),
            per_item: true,
        },
        Budgeted {
            name: "ComponentList",
            size: size_of::<ComponentList>(),
            align: align_of::<ComponentList>(),
            // Widest arm: One(ComponentBlocks).
            fields: size_of::<ComponentBlocks>(),
            per_item: true,
        },
        Budgeted {
            name: "ObjectBlockRefs",
            size: size_of::<ObjectBlockRefs>(),
            align: align_of::<ObjectBlockRefs>(),
            fields: size_of::<ComponentList>(),
            per_item: true,
        },
        Budgeted {
            name: "ObjectIndex",
            size: size_of::<ObjectIndex>(),
            align: align_of::<ObjectIndex>(),
            // Widest arm: Many(Box<BTreeSet<u64>>), which is one pointer.
            fields: size_of::<Box<std::collections::BTreeSet<u64>>>(),
            per_item: true,
        },
        Budgeted {
            name: "DirtyKeySet",
            size: size_of::<DirtyKeySet>(),
            align: align_of::<DirtyKeySet>(),
            // Widest arm: One(Arc<str>).
            fields: arc_str,
            per_item: true,
        },
        Budgeted {
            name: "WalResidentBlock",
            size: size_of::<WalResidentBlock>(),
            align: align_of::<WalResidentBlock>(),
            fields: 2 * size_of::<u64>(),
            per_item: true,
        },
        Budgeted {
            name: "IndexItem",
            size: size_of::<IndexItem>(),
            align: align_of::<IndexItem>(),
            // kind, routing_bucket, three String handles, Option<String> component,
            // object_id, block_id, Option<BlockAddress>, size, in_log, deleted
            fields: size_of::<IndexItemKind>()
                + size_of::<u32>()
                + 3 * string
                + size_of::<Option<String>>()
                + 2 * size_of::<u64>()
                + size_of::<Option<BlockAddress>>()
                + size_of::<u64>()
                + 2 * size_of::<bool>(),
            per_item: true,
        },
        Budgeted {
            name: "SlabCatalogEntry",
            size: size_of::<SlabCatalogEntry>(),
            align: align_of::<SlabCatalogEntry>(),
            fields: 4 * size_of::<u64>()
                + size_of::<crate::index_log::SlabCatalogState>()
                + 4 * opt_u64,
            per_item: false,
        },
        Budgeted {
            name: "BlockStoreSlabDescriptor",
            size: size_of::<BlockStoreSlabDescriptor>(),
            align: align_of::<BlockStoreSlabDescriptor>(),
            // Two slab ids, two byte counts and the readable prefix; the lifecycle state; six
            // optional u64s (created, updated, first/last block id, verified mtime, first error
            // offset); the corruption flag; and the first error text.
            fields: 5 * size_of::<u64>()
                + size_of::<crate::block_store::BlockStoreSlabState>()
                + 6 * opt_u64
                + size_of::<bool>()
                + size_of::<Option<String>>(),
            per_item: false,
        },
        // CONTROLS. One instance each, per shard, and the widest things in the engine. They are
        // in the table so the ranking below has something to rank them BELOW: if this module
        // ordered by width instead of by width x count, these two would head the list and the
        // whole exercise would point at the wrong structures.
        Budgeted {
            name: "ShardState (control: one per shard)",
            size: size_of::<crate::engine::state::ShardState>(),
            align: align_of::<crate::engine::state::ShardState>(),
            fields: 0,
            per_item: false,
        },
        Budgeted {
            name: "CoreIndex (control: one per shard)",
            size: size_of::<crate::engine::state::CoreIndex>(),
            align: align_of::<crate::engine::state::CoreIndex>(),
            fields: 0,
            per_item: false,
        },
    ]
}

/// THE BUDGET, PINNED. Every per-item width, with its field sum and its padding beside it.
///
/// The literals here and the `const _: () = assert!(size_of::<T>() == N)` beside each declaration
/// say the same thing twice on purpose. The const assertion is what stops a growth landing: it is
/// a BUILD failure, so it cannot be skipped, filtered out or marked ignored. This test is what
/// says WHY each number is what it is, and separates the two different wastes -- a field that is
/// wider than its range needs, and the bytes the aligner inserted between fields -- because they
/// do not have the same fix and one of them has no fix at this layer at all.
#[test]
fn every_per_item_structure_states_its_width_and_its_padding() {
    let rows = budget();
    assert!(
        rows.len() >= 15,
        "the budget lists {} structures; it was written with 17 and a shrinking list is how a \
         structure stops being watched",
        rows.len()
    );

    println!(
        "{:<38} {:>6} {:>6} {:>7} {:>8}  {}",
        "structure", "size", "align", "fields", "slack", "counted per"
    );
    for row in &rows {
        println!(
            "{:<38} {:>6} {:>6} {:>7} {:>8}  {}",
            row.name,
            row.size,
            row.align,
            if row.fields == 0 { 0 } else { row.fields },
            if row.fields == 0 { 0 } else { row.padding() },
            if row.per_item { "stored item" } else { "shard or slab" }
        );
    }

    // --- The pinned widths. ---
    assert_eq!(32, size_of::<BlockAddress>(), "BlockAddress width moved");
    assert_eq!(88, size_of::<BlockIndex>(), "BlockIndex width moved");
    assert_eq!(96, size_of::<BlockIndexMap>(), "BlockIndexMap width moved");
    assert_eq!(160, size_of::<BucketNode>(), "BucketNode width moved");
    assert_eq!(16, size_of::<BlockLookupRef>(), "BlockLookupRef width moved");
    assert_eq!(24, size_of::<BlockRefs>(), "BlockRefs width moved");
    assert_eq!(40, size_of::<ComponentBlocks>(), "ComponentBlocks width moved");
    assert_eq!(40, size_of::<ComponentList>(), "ComponentList width moved");
    assert_eq!(40, size_of::<ObjectBlockRefs>(), "ObjectBlockRefs width moved");
    assert_eq!(16, size_of::<ObjectIndex>(), "ObjectIndex width moved");
    assert_eq!(8, size_of::<DeletedObjectIndex>(), "DeletedObjectIndex width moved");
    assert_eq!(24, size_of::<DirtyKeySet>(), "DirtyKeySet width moved");
    assert_eq!(16, size_of::<WalResidentBlock>(), "WalResidentBlock width moved");
    assert_eq!(168, size_of::<IndexItem>(), "IndexItem width moved");
    assert_eq!(104, size_of::<SlabCatalogEntry>(), "SlabCatalogEntry width moved");
    assert_eq!(
        168,
        size_of::<BlockStoreSlabDescriptor>(),
        "BlockStoreSlabDescriptor width moved"
    );

    // --- The field sums, so a width change says whether it was a field or the aligner. ---
    for row in &rows {
        if row.fields == 0 {
            continue;
        }
        assert!(
            row.fields <= row.size,
            "{}: its fields add up to {} inside a {}-byte struct, which is impossible -- the \
             field list in this module has drifted from the declaration",
            row.name,
            row.fields,
            row.size
        );
    }

    // --- Non-vacuity: the padding arithmetic must discriminate, not report a constant. ---
    let with_padding = rows.iter().filter(|r| r.fields > 0 && r.padding() > 0).count();
    let without_padding = rows.iter().filter(|r| r.fields > 0 && r.padding() == 0).count();
    assert!(
        with_padding > 0 && without_padding > 0,
        "the padding column reports {with_padding} padded and {without_padding} unpadded \
         structures; with either at zero it is a constant and says nothing"
    );

    // --- The two named cases, so the classification itself is guarded. ---
    // WIDTH-BOUND: the fields fill the struct, so narrowing one can move it.
    let address = rows.iter().find(|r| r.name == "BlockAddress").expect("row");
    assert!(
        address.padding() < 8,
        "BlockAddress carries {} bytes of padding; at 8 or more it is alignment-bound and the \
         narrowing this module justifies would not have moved it",
        address.padding()
    );
    // ALIGNMENT-BOUND: a quarter of it is padding and no narrowing of either field helps.
    let lookup = rows.iter().find(|r| r.name == "BlockLookupRef").expect("row");
    assert_eq!(
        4,
        lookup.padding(),
        "BlockLookupRef is the worked example of an alignment problem: 12 bytes of field in 16. \
         If that is no longer true the module's own explanation is stale"
    );
}

// -------------------------------------------------------------------------------------------
// TRUNCATION. The two narrowed fields, at and past their boundary.
// -------------------------------------------------------------------------------------------

/// A LENGTH THAT DOES NOT FIT SATURATES; IT NEVER WRAPS.
///
/// This is the mutant that matters for this change. `length` was a `u64` and is a `u32`; the
/// conversion sits in `BlockAddress::from_parts`, which every construction goes through. A
/// conversion written `as u32` compiles, passes every existing test, and turns a 4 GiB length
/// into a small one -- a read that then returns the wrong bytes with no error anywhere. The
/// saturating form cannot do that: what comes back is larger than any length the encoder will
/// ever accept, so it reads as broken rather than as plausible.
///
/// `BLOCK_RECORD_LENGTH_MASK` is why this is a boundary and not a cliff: `encode_block_record`
/// REFUSES a record above 0x3FFF_FFFF bytes, so a real length is at most 2^30 plus a header --
/// a quarter of what the field now holds.
#[test]
fn a_length_that_does_not_fit_the_field_saturates_rather_than_wrapping() {
    let just_under = BlockAddress::from_parts(1, 0, u64::from(u32::MAX) - 1, None, None, None);
    assert_eq!(
        u64::from(u32::MAX) - 1,
        just_under.length(),
        "a length inside the field must round-trip exactly"
    );

    for over in [
        u64::from(u32::MAX) + 1,
        u64::from(u32::MAX) + 2,
        1u64 << 32,
        (1u64 << 32) + 7,
        u64::MAX,
    ] {
        let address = BlockAddress::from_parts(1, 0, over, None, None, None);
        assert_eq!(
            u64::from(u32::MAX),
            address.length(),
            "a length of {over} must saturate to {}, not wrap to {}",
            u32::MAX,
            over as u32
        );
        // The discriminating half: for every value whose low 32 bits are NOT already the
        // saturation value, truncation and saturation give different answers, and this is the
        // one that fails under `as u32`. `u64::MAX` is excluded because its low bits ARE
        // `u32::MAX`, so it cannot tell the two apart -- a case that passes either way is not a
        // test of which one ran.
        if over as u32 != u32::MAX {
            assert_ne!(
                over as u32 as u64,
                address.length(),
                "a length of {over} came back as its low 32 bits -- that is the truncation this \
                 field was narrowed under the promise of not doing"
            );
        }
    }

    // And the saturated value is outside what the encoder will ever produce, so it cannot be
    // mistaken for a real length.
    assert!(
        u64::from(u32::MAX) > u64::from(crate::block_store::BLOCK_RECORD_LENGTH_MASK),
        "the saturation value must sit above the largest length a record can hold, or a \
         saturated address is indistinguishable from a valid one"
    );
}

/// A BLOCK ID THAT DOES NOT FIT SATURATES TOO, and the field has four billion times the headroom
/// the write path allows: `encode_block_record` refuses a block id above `u16::MAX`.
#[test]
fn a_block_id_that_does_not_fit_the_field_saturates_rather_than_wrapping() {
    let real = BlockAddress::from_parts(1, 0, 64, Some(u64::from(u16::MAX)), None, None);
    assert_eq!(
        Some(u64::from(u16::MAX)),
        real.block_id(),
        "the largest block id the encoder accepts must round-trip exactly"
    );

    for over in [u64::from(u32::MAX) + 1, 1u64 << 33, u64::MAX] {
        let address = BlockAddress::from_parts(1, 0, 64, Some(over), None, None);
        assert_eq!(
            Some(u64::from(u32::MAX)),
            address.block_id(),
            "a block id of {over} must saturate, not wrap to {}",
            over as u32
        );
    }

    // The setter is the second write path into the same field and has to agree with the first.
    //
    // THE VALUE IS CHOSEN SO THE TWO ANSWERS DIFFER. `u64::MAX` cannot test this: its low 32 bits
    // ARE `u32::MAX`, so truncation and saturation agree on it and a setter written `as u32`
    // passes. A mutation run scored exactly that -- the setter truncating survived every test
    // here -- and this is the value that kills it.
    const OVER: u64 = (1u64 << 33) + 5;
    assert_ne!(
        u64::from(OVER as u32),
        u64::from(u32::MAX),
        "the probe value must distinguish truncation from saturation, or this test cannot fail"
    );
    let mut address = BlockAddress::from_parts(1, 0, 64, None, None, None);
    address.set_block_id(Some(OVER));
    assert_eq!(
        Some(u64::from(u32::MAX)),
        address.block_id(),
        "the setter must saturate too; it answered with the low 32 bits"
    );
    address.set_block_id(None);
    assert_eq!(None, address.block_id(), "clearing the field must still clear it");

    // And the constructor, on the same discriminating value.
    let built = BlockAddress::from_parts(1, 0, 64, Some(OVER), None, None);
    assert_eq!(Some(u64::from(u32::MAX)), built.block_id(), "the constructor must saturate");
}

/// `set_length` IS A WRITE PATH, and a setter that quietly drops its write is invisible to every
/// test that only ever reads a length back out of a constructor.
///
/// The slab inspector is its one production caller -- it walks a slab and stamps each record's
/// framed length onto the address it reports -- so a no-op here makes an inspection report every
/// record as zero-length. A mutation run found this uncovered.
#[test]
fn setting_a_length_after_the_fact_writes_it_and_saturates_it() {
    let mut address = BlockAddress::from_parts(1, 0, 0, None, None, None);
    assert_eq!(0, address.length(), "it starts at the length it was built with");

    address.set_length(4_096);
    assert_eq!(4_096, address.length(), "the setter must actually write");

    address.set_length(17);
    assert_eq!(17, address.length(), "and must overwrite what was there");

    // The same discriminating value as above: low bits that are not the saturation value.
    const OVER: u64 = (1u64 << 33) + 5;
    address.set_length(OVER);
    assert_eq!(
        u64::from(u32::MAX),
        address.length(),
        "the setter must saturate, not keep the low 32 bits"
    );
}

/// THE STORED FORM DID NOT MOVE.
///
/// `BlockAddress` serializes through `BlockAddressWire`, which holds `length` as a `u64` and
/// `block_id` as an `Option<u64>` and is untouched by this change. That is what makes the
/// narrowing resident-only: an index written before it reads back identically, and an index
/// written after it is byte-identical to one written before. If this test fails, a stored format
/// has moved and the change is not what its own pull request says it is.
///
/// THE ONE BYTE-LEVEL CHANGE SINCE, AND WHY IT IS NOT THAT. `generation` became DERIVED as
/// `block_id.or(object_id)` rather than stored. The wire still carries `g`, still reads it, and
/// still writes it -- so for every address this engine produces the spelling is unchanged, because
/// every production constructor already passed exactly that expression. What moved is this
/// FIXTURE: it had chosen an independent `0x0123456789ABCDEF` beside a `block_id` of 7, which no
/// writer emits, and the address can no longer represent it. The golden below therefore reads
/// `"g":7`. An index that really did carry a disagreeing generation is now REFUSED at load rather
/// than re-keyed -- see
/// `engine::tests::page_entry_names::an_old_store_whose_generation_disagrees_is_refused_before_the_decode`.
#[test]
fn narrowing_the_resident_fields_did_not_move_the_stored_form() {
    // THE SLAB AND THE OFFSET AT THEIR ADDRESSABLE MAXIMUMS, taken from the constants rather than
    // written as a literal. This fixture used to carry a slab id of 9,876,543,210 -- above 2^32 --
    // to state that the stored form kept 64-bit slab coordinates whatever the resident fields did.
    // That premise is GONE: the two coordinates are now the two halves of one 32/32 word, so a
    // slab id that large is refused rather than stored, and the largest one there is is what this
    // pins instead.
    let slab = crate::block_store::MAX_ADDRESSABLE_BLOCK_SLAB_ID;
    let offset = crate::block_store::MAX_ADDRESSABLE_BLOCK_OFFSET;
    let address = BlockAddress::from_parts(
        slab,
        offset,
        1_048_576,
        Some(7),
        Some(0xDEAD_BEEF_CAFE_F00D),
        Some(4_294_967_290),
    );
    let word = crate::block_store::make_block_address_word(slab as u32, offset as u32);
    let json = serde_json::to_string(&address).expect("an address serializes");
    assert_eq!(
        format!(
            "{{\"a\":{word},\"l\":1048576,\"pi\":7,\"oi\":16045690984503111693,\
             \"rs\":4294967290,\"g\":7,\"h\":null}}"
        ),
        json,
        "the stored spelling of an address moved"
    );
    assert!(
        !json.contains("\"ps\"") && !json.contains("\"o\":"),
        "the split slab id and offset are still being written: {json}"
    );
    let back: BlockAddress = serde_json::from_str(&json).expect("it reads back");
    assert_eq!(address, back, "an address must round-trip through its stored form");
    assert_eq!(
        (back.block_slab_id(), back.offset()),
        (slab, offset),
        "the packed word did not unpack to the two numbers that went into it"
    );

    // A stored length or block id above the resident field is already outside what the encoder
    // can have written, so reading one is reading a corrupt index. It must come back saturated,
    // never truncated: a truncated length is a plausible number and a saturated one is not.
    // NO generation key here, deliberately. Besides the saturation it was written for, this
    // is the shape of an index written before the generation existed: an identity and no
    // generation at all. It must LOAD, and it must not acquire one.
    let wide = "{\"a\":4294967296,\"l\":4294967296,\"pi\":4294967296}";
    let read: BlockAddress = serde_json::from_str(wide).expect("a wide stored value still loads");
    assert_eq!(u64::from(u32::MAX), read.length(), "a wide stored length saturates");
    assert_eq!(Some(u64::from(u32::MAX)), read.block_id(), "a wide stored block id saturates");
    assert_eq!(
        None,
        read.generation(),
        "an index that stored no generation must not acquire one when the field is derived"
    );
}

// -------------------------------------------------------------------------------------------
// WHAT THEY COST, AT TWO CORPUS SIZES.
// -------------------------------------------------------------------------------------------

/// How many of each structure a seeded shard actually holds.
#[derive(Default)]
struct ItemCounts {
    records: usize,
    bucket_nodes: usize,
    block_index_entries: usize,
    model_map_addresses: usize,
    object_block_refs: usize,
    component_blocks: usize,
    block_lookup_refs: usize,
    dirty_key_sets: usize,
    wal_resident_blocks: usize,
}

impl ItemCounts {
    /// Every resident `BlockAddress`: the one each model-map entry holds, plus the one inside
    /// every page-index entry. The two are separate copies of the same address, which is why
    /// this structure's count is roughly twice the record count rather than equal to it.
    fn block_addresses(&self) -> usize {
        self.model_map_addresses + self.block_index_entries
    }
}

fn count_items(shard: &crate::engine::state::ShardState) -> ItemCounts {
    let mut counts = ItemCounts::default();
    counts.model_map_addresses = shard.strings.len()
        + shard.features.values().map(|series| series.len()).sum::<usize>()
        + shard.hashes.values().map(|fields| fields.len()).sum::<usize>()
        + shard.sets.values().map(|members| members.len()).sum::<usize>()
        + shard.zsets.values().map(|members| members.len()).sum::<usize>()
        + shard.lists.values().map(|items| items.len()).sum::<usize>();
    counts.records = shard.strings.len() + shard.features.values().map(|s| s.len()).sum::<usize>();
    counts.bucket_nodes = shard.bucket_index.bucket_map.len();
    counts.block_index_entries = shard
        .bucket_index
        .bucket_map
        .values()
        .map(|bucket| bucket.block_index.len())
        .sum();
    counts.object_block_refs = shard.bucket_index.object_block_lookup.len();
    counts.component_blocks = shard
        .bucket_index
        .object_block_lookup
        .values()
        .map(|entry| entry.by_component.len())
        .sum();
    counts.block_lookup_refs = shard
        .bucket_index
        .object_block_lookup
        .values()
        .map(|entry| entry.total_refs())
        .sum();
    // One `DirtyKeySet` per dirty BUCKET, and the measured distribution is one key per bucket at
    // both corpus sizes -- `every_dirty_bucket_holds_its_one_key_inline` walks the shard and
    // reports it. So the dirty-object count is the set count here, and the comment says which
    // measurement that leans on rather than assuming it.
    counts.dirty_key_sets = shard.dirty_objects.len();
    counts.wal_resident_blocks = shard.wal_resident_blocks.len();
    counts
}

/// THE MEASUREMENT. `size_of` x count, at two corpus sizes ten times apart.
///
/// Ranked by the product, which is the only ranking that answers the question this module asks.
/// `BlockStoreSlabDescriptor` is the reason: it is 168 bytes against `BlockLookupRef`'s 16, and a
/// store holding a gigabyte of pages has ONE of it and eighty thousand of the other.
///
/// A per-item figure that is FLAT across the two corpus sizes is what makes the total multiply
/// out to any scale. One that grows is a finding in itself, and the per-structure rows say which
/// structure grew.
///
/// THE STORE PATH LENGTH IS HELD CONSTANT across the two arms and asserted below: `tempfile`
/// names every directory with the same number of characters, and a path length that moved between
/// arms would move allocation counts with it.
#[test]
#[ignore = "seeds 8,000 then 80,000 records; run by name"]
fn what_the_per_item_structures_cost_at_two_corpus_sizes() {
    let mut per_record: Vec<(&'static str, f64, f64)> = Vec::new();
    let mut path_lengths: Vec<usize> = Vec::new();

    for (label, strings_n, series_keys, series_points) in [
        ("8,000 records", 4_000usize, 4usize, 1_000usize),
        ("80,000 records", 40_000usize, 40usize, 1_000usize),
    ] {
        let dir = tempfile::tempdir().expect("tempdir");
        path_lengths.push(dir.path().as_os_str().len());
        let engine = budget_engine(dir.path());
        budget_seed(&engine, strings_n, series_keys, series_points);

        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard is loaded");
        let counts = count_items(shard);

        // --- Denominators, asserted before anything divides by them. ---
        assert_eq!(
            strings_n + series_keys * series_points,
            counts.records,
            "denominator: the shard must hold every record seeded"
        );
        assert!(
            counts.bucket_nodes > 0,
            "denominator: no routing buckets, so every per-bucket figure below is a zero that \
             means nothing"
        );
        assert!(
            counts.block_index_entries > 0,
            "denominator: no page-index entries, so the structure this module is about is absent"
        );
        assert!(
            counts.object_block_refs > 0 && counts.block_lookup_refs > 0,
            "denominator: the object lookup is empty"
        );

        let rows: Vec<(&'static str, usize, usize)> = vec![
            ("BlockAddress", size_of::<BlockAddress>(), counts.block_addresses()),
            ("BucketNode", size_of::<BucketNode>(), counts.bucket_nodes),
            ("BlockIndex (inside BucketNode)", size_of::<BlockIndex>(), counts.block_index_entries),
            ("BlockLookupRef", size_of::<BlockLookupRef>(), counts.block_lookup_refs),
            ("ComponentBlocks", size_of::<ComponentBlocks>(), counts.component_blocks),
            ("ObjectBlockRefs", size_of::<ObjectBlockRefs>(), counts.object_block_refs),
            ("DirtyKeySet", size_of::<DirtyKeySet>(), counts.dirty_key_sets),
            ("WalResidentBlock", size_of::<WalResidentBlock>(), counts.wal_resident_blocks),
        ];

        let mut ranked = rows.clone();
        ranked.sort_by_key(|(_, width, count)| std::cmp::Reverse(width * count));

        println!("\n=== {label}: per-item structures ranked by size_of x count ===");
        println!("  records={} buckets={} page entries={} model-map addresses={}",
            counts.records, counts.bucket_nodes, counts.block_index_entries,
            counts.model_map_addresses);
        println!(
            "  {:<32} {:>6} {:>10} {:>12} {:>12}",
            "structure", "width", "count", "total bytes", "per record"
        );
        let mut total = 0usize;
        for (name, width, count) in &ranked {
            let bytes = width * count;
            total += bytes;
            println!(
                "  {:<32} {:>6} {:>10} {:>12} {:>12.2}",
                name,
                width,
                count,
                bytes,
                bytes as f64 / counts.records as f64
            );
        }
        println!(
            "  {:<32} {:>6} {:>10} {:>12} {:>12.2}",
            "TOTAL (structure payload only)",
            "",
            "",
            total,
            total as f64 / counts.records as f64
        );

        // BlockIndex is NOT added into the total separately: every page entry lives inside a
        // BucketNode's own `BlockIndexMap`, so its bytes are already inside the BucketNode row.
        // Counting it twice would report a win twice.
        let counted: usize = rows
            .iter()
            .filter(|(name, _, _)| *name != "BlockIndex (inside BucketNode)")
            .map(|(_, width, count)| width * count)
            .sum();
        for (name, width, count) in &rows {
            per_record.push((
                name,
                (width * count) as f64 / counts.records as f64,
                counts.records as f64,
            ));
        }
        println!(
            "  payload without double-counting the nested page entries: {} B, {:.2} B/record",
            counted,
            counted as f64 / counts.records as f64
        );
    }

    assert_eq!(
        2,
        path_lengths.len(),
        "both arms must have run, or the flatness claim below compares one arm with itself"
    );
    assert_eq!(
        path_lengths[0], path_lengths[1],
        "the store path length moved between arms ({} then {}); allocation counts move with it",
        path_lengths[0], path_lengths[1]
    );

    // --- Flatness. Every per-item figure must hold across a tenfold corpus. ---
    println!("\n=== flatness across a tenfold corpus ===");
    let half = per_record.len() / 2;
    for index in 0..half {
        let (name, small, _) = per_record[index];
        let (name_big, big, _) = per_record[index + half];
        assert_eq!(name, name_big, "the two arms reported structures in different orders");
        let ratio = if small > 0.0 { big / small } else { 1.0 };
        println!("  {name:<32} {small:>8.2} -> {big:>8.2} B/record  ({ratio:.3}x)");
        assert!(
            ratio < 1.25,
            "{name} costs {small:.2} B/record at 8,000 records and {big:.2} at 80,000 ({ratio:.3}x) \
             -- it is growing with the corpus, which is a finding and not a budget"
        );
    }
}

fn budget_engine(dir: &std::path::Path) -> Arc<TemporalEngine> {
    let engine = Arc::new(TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("pages"),
        dir.join("indexes"),
    ));
    engine.load_shard(1);
    engine
}

fn budget_seed(engine: &TemporalEngine, strings_n: usize, series_keys: usize, series_points: usize) {
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
            shard_id: 1,
            commands,
        });
        assert!(response.status.ok, "string seed must ack: {:?}", response.status);
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
                shard_id: 1,
                command: Command::FeatureAppend {
                    key: format!("f{k}"),
                    points,
                },
            });
            assert!(response.status.ok, "feature seed must ack: {:?}", response.status);
        }
    }
}

// -------------------------------------------------------------------------------------------
// THE WIDEST PER-ITEM STRUCTURE, BYTE BY BYTE.
//
// `BucketNode` exists once per routing bucket, which is once per key that routes to a bucket of
// its own, and the ranking above puts it first at both corpus sizes. This section says what its
// bytes ARE -- every field with its own width and alignment, the sum beside `size_of`, and the
// slack named separately -- and then prices the three shapes that were considered for it and
// declined, so the decline is a number rather than an opinion.
// -------------------------------------------------------------------------------------------

/// One declared field of `BucketNode`.
///
/// The widths are `size_of` over the DECLARED types, never literals, so this table moves when the
/// declaration does instead of going quietly stale beside it.
struct Field {
    name: &'static str,
    ty: &'static str,
    size: usize,
    align: usize,
}

fn bucket_node_fields() -> Vec<Field> {
    macro_rules! field {
        ($name:literal, $ty:ty) => {
            Field {
                name: $name,
                ty: stringify!($ty),
                size: size_of::<$ty>(),
                align: align_of::<$ty>(),
            }
        };
    }
    vec![
        field!("routing_bucket", u32),
        field!("layout", BucketLayoutState),
        field!("flags", BucketFlags),
        field!("ttl_ms", BucketTtl),
        field!("dirty_generation", u64),
        field!("first_dirty_wal_sequence", u64),
        field!("first_dirty_index_log_sequence", u64),
        field!("object_index", ObjectIndex),
        field!("deleted_object_index", DeletedObjectIndex),
        field!("block_index", BlockIndexMap),
    ]
}

/// EVERY BYTE OF THE BUCKET NODE, ACCOUNTED FOR.
///
/// Ten fields, their widths summed, and the difference against `size_of` named as what it is:
/// the aligner's, not any field's. The sum is the discriminating half. A width that is stated
/// without its field sum cannot tell a structure that is FULL from one that is half padding, and
/// those two want opposite fixes -- one wants a narrower field, the other cannot be helped by any
/// narrowing at all.
///
/// HOW RUST LAYS THIS OUT, and it is the whole explanation of the number. Fields reorder freely,
/// so the layout is two groups: everything of alignment 8 packs solid, and everything smaller
/// fills the tail, which is then rounded up to the struct's own alignment. Here that is 160 bytes
/// of eight-aligned field and 6 bytes of small field rounded to 8.
///
/// THE TAIL USED TO BE TEN BYTES AND THE RULE WRITTEN HERE WAS TOO STRONG. It said NOTHING in the
/// ten-byte tail could be narrowed to any effect, because the tail was already inside a rounding.
/// That is true of narrowing ONE field and it is what the per-field verdict below still says --
/// 9, 6 and 4 all round back to 16 -- but it was read as though it applied to the tail as a
/// whole, and it does not. Five `bool` became five BITS, four bytes left at once, and the tail
/// crossed the step: 10-in-16 became 6-in-8 and the structure lost eight bytes. The rule that
/// survives is the last clause, which was right all along: only a change that takes the tail to 8
/// bytes or fewer, or that takes a whole word out of the eight-aligned group, moves this
/// structure at all. Packing was such a change; narrowing any single tail field is not.
///
/// AND THE OTHER CLAUSE IS WHAT TOOK IT TO 168. `last_dump_sequence` was a whole word of the
/// eight-aligned group, and removing it takes that word out without touching the tail: 160 + 8.
/// Removed bytes LEAVE, where a narrowed field's bytes move into the tail and are handed straight
/// back at six -- which is why a removal crosses the step here and a narrowing does not.
#[test]
fn every_byte_of_the_bucket_node_is_accounted_for() {
    let fields = bucket_node_fields();
    assert_eq!(
        10,
        fields.len(),
        "the field table lists {} fields; `BucketNode` has ten and a table that has drifted \
         from the declaration proves nothing about it",
        fields.len()
    );

    println!("\n=== BucketNode, field by field ===");
    println!("  {:<34} {:<20} {:>5} {:>6}", "field", "declared type", "size", "align");
    let mut eight_aligned = 0usize;
    let mut tail = 0usize;
    for field in &fields {
        println!(
            "  {:<34} {:<20} {:>5} {:>6}",
            field.name, field.ty, field.size, field.align
        );
        if field.align == align_of::<BucketNode>() {
            eight_aligned += field.size;
        } else {
            tail += field.size;
        }
    }
    let sum: usize = fields.iter().map(|field| field.size).sum();
    let size = size_of::<BucketNode>();
    let slack = size - sum;
    println!("  {:<34} {:<20} {:>5}", "SUM OF FIELDS", "", sum);
    println!("  {:<34} {:<20} {:>5}", "size_of::<BucketNode>()", "", size);
    println!(
        "  {:<34} {:<20} {:>5}   <- the aligner, not any field",
        "SLACK", "", slack
    );
    println!(
        "  groups: {eight_aligned} B of eight-aligned field + {tail} B of small field, the tail \
         rounded up to {}",
        size - eight_aligned
    );

    assert_eq!(158, sum, "the fields of BucketNode add up to {sum}, not 158");
    assert_eq!(160, size, "BucketNode is {size} bytes wide, not 160");
    assert_eq!(2, slack, "BucketNode carries {slack} bytes of alignment slack, not 2");

    // The layout rule itself, asserted rather than described: the eight-aligned group packs
    // solid and the rest is one rounding.
    let align = align_of::<BucketNode>();
    assert_eq!(8, align, "BucketNode's alignment moved, and the arithmetic below assumes 8");
    // 152, not 160, 168 or 176. THREE eight-byte changes have come out of THIS group and none
    // out of the tail: the address inside the inline page entry shed a derived `generation`,
    // `last_dump_sequence` left the node, and that same address merged its slab id and its
    // offset into ONE WORD. The tail is six because the five flags became five bits, which is
    // the other half of the structure entirely.
    assert_eq!(152, eight_aligned, "the eight-aligned group is {eight_aligned} B, not 152");
    assert_eq!(6, tail, "the tail group is {tail} B, not 6");
    assert_eq!(
        eight_aligned + tail.div_ceil(align) * align,
        size,
        "the two groups plus one rounding must reconstruct the width exactly, or the layout is \
         not what this test says it is"
    );

    // --- WIDTH OR ALIGNMENT, field by field, and the verdict is arithmetic. ---
    //
    // A field is a WIDTH candidate only if taking bytes off it takes bytes off the struct.
    // Inside the tail that is false until the tail itself drops past a multiple of the
    // alignment, and every field in the tail here is one byte or four.
    println!("\n=== which fields could move this structure, and which could not ===");
    for field in &fields {
        let verdict = if field.align < align {
            // In the rounding. Narrowing it cannot cross the boundary on its own -- but note
            // that REMOVING enough of them at once can, which is how the tail got to six.
            "ALIGNMENT -- inside a 6-in-8 rounding; narrowing it alone moves nothing"
        } else if field.size % align == 0 && field.size > align {
            "WIDTH -- a whole number of words, and every word of it is paid per bucket"
        } else {
            "WIDTH -- one word, and losing it would take a word off the struct"
        };
        println!("  {:<34} {}", field.name, verdict);
    }
    let tail_fields = fields.iter().filter(|field| field.align < align).count();
    assert_eq!(
        3,
        tail_fields,
        "three fields sit in the tail rounding -- the routing bucket, the layout and the packed \
         flag byte -- and this test found {tail_fields}"
    );
    let tail_bytes: usize = fields
        .iter()
        .filter(|field| field.align < align)
        .map(|field| field.size)
        .sum();
    assert!(
        tail_bytes.div_ceil(align) * align > tail_bytes,
        "the tail is {tail_bytes} B and fills its rounding exactly; the claim that narrowing a \
         tail field changes nothing is only true while it does not"
    );

    // --- The biggest single field, because an accounting that does not say so misleads. ---
    let page = fields
        .iter()
        .find(|field| field.name == "block_index")
        .expect("block_index is a field of BucketNode");
    assert!(
        page.size * 2 > size,
        "the inline page entry is {} of {size} bytes; if it is no longer more than half the \
         structure, the accounting above leads with the wrong field",
        page.size
    );
}

// -------------------------------------------------------------------------------------------
// THE THREE SHAPES CONSIDERED AND DECLINED, PRICED.
//
// Each is a MIRROR: a struct built from the same field types, so it is a statement about widths
// and not a guess. `mirror_is_faithful` is the control -- it fails if a mirror stops describing
// the declaration, which is the only way these numbers could quietly become fiction.
// -------------------------------------------------------------------------------------------

/// The node as it stands. If this is not `size_of::<BucketNode>()` every mirror below is fiction.
#[allow(dead_code)]
struct MirrorLive {
    routing_bucket: u32,
    layout: BucketLayoutState,
    flags: BucketFlags,
    ttl_ms: BucketTtl,
    dirty_generation: u64,
    first_dirty_wal_sequence: u64,
    first_dirty_index_log_sequence: u64,
    object_index: ObjectIndex,
    deleted_object_index: DeletedObjectIndex,
    block_index: BlockIndexMap,
}

/// The node as it was before #1958, with the countdown spending a word on its discriminant AND
/// the tombstone index spending sixteen bytes on a case it is almost never in.
#[allow(dead_code)]
struct MirrorWideTtl {
    routing_bucket: u32,
    layout: BucketLayoutState,
    dirty: bool,
    deleted: bool,
    meta_loaded: bool,
    loading: bool,
    in_memory: bool,
    ttl_ms: Option<u64>,
    dirty_generation: u64,
    first_dirty_wal_sequence: u64,
    first_dirty_index_log_sequence: u64,
    object_index: ObjectIndex,
    deleted_object_index: ObjectIndex,
    block_index: BlockIndexMap,
}

/// The node immediately before this change: the live shape with the tombstone index still held
/// as the full enum.
#[allow(dead_code)]
struct MirrorWideTombstone {
    routing_bucket: u32,
    layout: BucketLayoutState,
    dirty: bool,
    deleted: bool,
    meta_loaded: bool,
    loading: bool,
    in_memory: bool,
    ttl_ms: BucketTtl,
    dirty_generation: u64,
    first_dirty_wal_sequence: u64,
    first_dirty_index_log_sequence: u64,
    object_index: ObjectIndex,
    deleted_object_index: ObjectIndex,
    block_index: BlockIndexMap,
}

/// The five flags folded into one byte -- the shape that SHIPPED, held here as a control on the
/// live mirror rather than as a proposal.
#[allow(dead_code)]
struct MirrorPackedFlags {
    routing_bucket: u32,
    layout: BucketLayoutState,
    flags: u8,
    ttl_ms: BucketTtl,
    dirty_generation: u64,
    first_dirty_wal_sequence: u64,
    first_dirty_index_log_sequence: u64,
    object_index: ObjectIndex,
    deleted_object_index: DeletedObjectIndex,
    block_index: BlockIndexMap,
}

/// The node immediately BEFORE this change: the same fields with the five flags as five
/// independent `bool`, which is ten bytes of tail rounded to sixteen.
#[allow(dead_code)]
struct MirrorLooseFlags {
    routing_bucket: u32,
    layout: BucketLayoutState,
    dirty: bool,
    deleted: bool,
    meta_loaded: bool,
    loading: bool,
    in_memory: bool,
    ttl_ms: BucketTtl,
    dirty_generation: u64,
    first_dirty_wal_sequence: u64,
    first_dirty_index_log_sequence: u64,
    object_index: ObjectIndex,
    deleted_object_index: DeletedObjectIndex,
    block_index: BlockIndexMap,
}

/// The two transient log claims moved out of the node into a side map of dirty buckets.
#[allow(dead_code)]
struct MirrorHoistedClaims {
    routing_bucket: u32,
    layout: BucketLayoutState,
    flags: BucketFlags,
    ttl_ms: BucketTtl,
    dirty_generation: u64,
    object_index: ObjectIndex,
    deleted_object_index: DeletedObjectIndex,
    block_index: BlockIndexMap,
}

/// The inline page entry held behind a pointer instead.
#[allow(dead_code)]
enum MirrorBoxedBlockIndexMap {
    Empty,
    One(u64, Box<BlockIndex>),
    Many(std::collections::BTreeMap<u64, BlockIndex>),
}

#[allow(dead_code)]
struct MirrorBoxedPage {
    routing_bucket: u32,
    layout: BucketLayoutState,
    flags: BucketFlags,
    ttl_ms: BucketTtl,
    dirty_generation: u64,
    first_dirty_wal_sequence: u64,
    first_dirty_index_log_sequence: u64,
    object_index: ObjectIndex,
    deleted_object_index: DeletedObjectIndex,
    block_index: MirrorBoxedBlockIndexMap,
}

/// WHAT EACH DECLINED SHAPE WOULD ACTUALLY BUY, IN BYTES PER BUCKET.
///
/// THE CONTROL COMES FIRST. `MirrorLive` is built from the same field types as the declaration
/// and has to agree with it exactly; if it does not, every other row here is describing a
/// structure this engine does not have, and the test says so rather than printing numbers.
///
/// THE ONE THAT WAS TAKEN. `MirrorWideTtl` is the shape before this change and `MirrorLive` is
/// the shape after: eight bytes, and they came off because the countdown stopped needing a word
/// for its discriminant. It is the only one of the four whose stored spelling does not move --
/// the countdown is one JSON key either way, and the serde impls unbias across it.
///
/// THE OTHER ONE THAT WAS TAKEN, AND IT WAS DECLINED HERE FIRST. Folding the five flags into a
/// byte is worth the same eight bytes. The decline was written on blast radius -- `dirty`,
/// `deleted`, `meta_loaded`, `loading` and `in_memory` are five keys of the stored index and 90
/// sites across the engine, and holding the stored spelling still means a hand-written
/// serializer for the node. Every clause of that was accurate and it still cost eight bytes a
/// bucket to believe. What made it payable was doing the rewrite from the COMPILER'S error
/// positions rather than from a grep: the flags became accessors of the same name, so the sites
/// are still `dirty` and `meta_loaded` to anything that reads this tree, and the enumeration ran
/// to zero errors rather than to zero sites of a shape -- which is what caught the four
/// multi-line assignments a line-oriented pass had quietly turned into reads.
///
/// THE ONE THAT WAS DECLINED ON WHERE THE COST WOULD GO. The two `#[serde(skip)]` log claims are
/// sixteen bytes and touch no stored shape at all, which makes them the cheapest bytes here to
/// reach -- but only if they move somewhere cheaper, and a side map keyed by bucket costs its own
/// key, its own pair and its own B-tree node for every bucket that is dirty. The measurement that
/// decides it is the dirty FRACTION, which the corpus probe prints; at the fraction this store
/// runs at it is not a win.
///
/// THE ONE THAT IS A LOSS, AND THE ONLY ONE WHOSE SIGN IS NOT OBVIOUS. Putting the inline page
/// entry behind a pointer takes the most off the struct of anything here -- and then pays it
/// back with interest, because almost every bucket holds exactly one page and would allocate for
/// it. The allocation is rounded up by the allocator, so the pair costs MORE than the inline form
/// it replaced, plus an indirection on every page read and an allocation on every bucket.
#[test]
fn what_each_declined_shape_of_the_bucket_node_would_cost() {
    // --- The control. Nothing below means anything without it. ---
    assert_eq!(
        size_of::<BucketNode>(),
        size_of::<MirrorLive>(),
        "the mirror of the live declaration is {} bytes against the declaration's {}; the mirrors \
         have drifted and every price below is fiction",
        size_of::<MirrorLive>(),
        size_of::<BucketNode>()
    );

    let live = size_of::<BucketNode>();
    let wide_ttl = size_of::<MirrorWideTtl>();
    let wide_tombstone = size_of::<MirrorWideTombstone>();
    let loose = size_of::<MirrorLooseFlags>();
    let packed = size_of::<MirrorPackedFlags>();
    let hoisted = size_of::<MirrorHoistedClaims>();
    let boxed = size_of::<MirrorBoxedPage>();

    println!("\n=== what each shape makes the node ===");
    println!("  {:<44} {:>5} {:>9}", "shape", "bytes", "vs live");
    for (name, bytes) in [
        ("the node as it stands", live),
        ("with the five flags back as five bools (before)", loose),
        ("with the tombstone index back at the full enum as well", wide_tombstone),
        ("with the countdown back at two words as well", wide_ttl),
        ("the packed-flag mirror, which must equal the live shape", packed),
        ("with the two transient log claims hoisted out", hoisted),
        ("with the inline page entry behind a pointer", boxed),
    ] {
        println!(
            "  {:<44} {:>5} {:>+9}",
            name,
            bytes,
            bytes as i64 - live as i64
        );
    }

    // --- The change this module documents. ---
    //
    // EVERY ABSOLUTE FIGURE HERE IS EIGHT BYTES LOWER THAN IT WAS, TWICE OVER, AND NO DIFFERENCE
    // MOVED. Each mirror is the live declaration plus one historical difference, so when the live
    // node loses a word every mirror loses it too -- and it has to, or the differences below would
    // be measured against a shape this engine does not have. `last_dump_sequence` left the
    // eight-aligned group whole, and the address inside the inline page entry merged its two slab
    // coordinates into one word out of that same group, which is the same reason the two earlier
    // eight-byte changes came out of it whole.
    assert_eq!(
        184, wide_ttl,
        "the shape before #1958 was 208 bytes, 200 once the address inside the inline page entry \
         shed its derived generation, 192 once the node stopped carrying a per-bucket \
         last_dump_sequence, and 184 once that address merged its two slab coordinates; it reads \
         as {wide_ttl}, so the mirrors have drifted from the history they claim to price"
    );
    assert_eq!(
        176, wide_tombstone,
        "the shape before #1961 was 200 bytes, 192 once the address shed its derived generation, \
         184 once the node stopped carrying a per-bucket last_dump_sequence, and 176 once that \
         address merged its two slab coordinates; it reads as {wide_tombstone}, so the eight \
         bytes that change claims are not the eight bytes it took"
    );
    assert_eq!(160, live, "the node is {live} bytes, not 160");
    assert_eq!(
        168, loose,
        "the shape before the flags were packed was 184 bytes, 176 once the node stopped \
         carrying a per-bucket last_dump_sequence, and 168 once the address inside the inline \
         page entry merged its two slab coordinates -- this mirror holds that address too, so \
         it moved with the live shape and the EIGHT BYTES BETWEEN THEM is still what packing \
         the flags is worth. It reads as {loose}, so the row that prices this change is not \
         describing the shape it replaced"
    );
    assert_eq!(
        8,
        wide_tombstone - loose,
        "holding the tombstone index as one nullable pointer is priced at eight bytes a bucket, \
         against the loose-flag shape it was measured on; it measured {}",
        wide_tombstone - loose
    );

    // --- WHAT THIS CHANGE TOOK, and the control that says the mirror describes it. ---
    //
    // `MirrorPackedFlags` is built from a bare `u8` where the declaration now has `BucketFlags`.
    // They must come out the same width, or the newtype is costing something a byte does not.
    assert_eq!(
        live, packed,
        "the packed-flag mirror is {packed} B against the declaration's {live}; `BucketFlags` is \
         not laying out as the byte it wraps"
    );
    assert_eq!(
        8,
        loose - live,
        "folding the five flags into one byte is priced at eight bytes a bucket; it measured {}",
        loose - live
    );

    // --- The one still on the table. ---
    assert_eq!(
        16,
        live - hoisted,
        "hoisting the two transient claims is priced at sixteen bytes a bucket; it measured {}",
        live - hoisted
    );

    // --- The loss, and why the struct width alone would have read as a win. ---
    //
    // The pointer takes eighty bytes off the struct and then buys a 104-byte allocation for the
    // entry it moved out -- and it buys one for very nearly every bucket, because the page
    // entries and the buckets are within a tenth of a percent of each other in count. A 104-byte
    // request is served from a 112-byte chunk once the allocator has taken its header and rounded
    // to a class, so the pair is WIDER than the inline form it replaced, before counting the
    // allocation itself or the pointer chase on every page read.
    const ALLOCATOR_CHUNK_FOR_A_PAGE_ENTRY: usize = 112;
    assert!(
        boxed < live,
        "the boxed shape must make the STRUCT smaller, or the point of the row is lost"
    );
    let boxed_pair = boxed + ALLOCATOR_CHUNK_FOR_A_PAGE_ENTRY;
    println!(
        "\n  the boxed shape: {boxed} B of struct + {ALLOCATOR_CHUNK_FOR_A_PAGE_ENTRY} B of \
         allocation = {boxed_pair} B for the bucket that holds one page, against {live} B inline \
         -- {:+} B, one allocation and one indirection",
        boxed_pair as i64 - live as i64
    );
    assert!(
        boxed_pair > live,
        "the boxed shape costs {boxed_pair} B against {live} B inline; if that has become a win \
         the decline recorded here is stale and should be revisited"
    );
    assert!(
        size_of::<BlockIndex>() <= ALLOCATOR_CHUNK_FOR_A_PAGE_ENTRY,
        "the chunk size assumed for a page entry is smaller than the entry ({} B), which would \
         under-price the decline",
        size_of::<BlockIndex>()
    );
}

// -------------------------------------------------------------------------------------------
// THE STORED SHAPE, DRIVEN IN BOTH DIRECTIONS.
// -------------------------------------------------------------------------------------------

/// A bucket node with every field set to something distinguishable, for the wire tests.
fn wire_fixture(ttl_ms: Option<u64>) -> BucketNode {
    BucketNode {
        routing_bucket: 7,
        layout: BucketLayoutState::SingleBlockObject,
        flags: BucketFlags::default().with(BucketFlags::DIRTY, false).with(BucketFlags::DELETED, false).with(BucketFlags::META_LOADED, true).with(BucketFlags::LOADING, false).with(BucketFlags::IN_MEMORY, true),
        ttl_ms: BucketTtl::from_ms(ttl_ms),
        dirty_generation: 3,
        first_dirty_wal_sequence: 41,
        first_dirty_index_log_sequence: 42,
        object_index: [42u64].into_iter().collect(),
        deleted_object_index: DeletedObjectIndex::default(),
        block_index: BlockIndexMap::default(),
    }
}

/// THE STORED SPELLING OF A BUCKET NODE DID NOT MOVE.
///
/// This is the constraint the whole subject runs into, and it is why the accounting above ends
/// where it does: `BucketNode` is written into the shard index, so its field names ARE a stored
/// format and a resident-layout change that alters one of them is not a resident-layout change.
///
/// The countdown is the one field this change touched, and it is held differently in memory and
/// written identically to disk. Both directions are DRIVEN rather than argued:
///
///   * NEW WRITE. The node serializes to the exact bytes below -- `ttl_ms` as a bare number or
///     `null`, in the same position, with no key added and none removed.
///   * OLD READ. The spellings an index written before this change can contain -- a number, a
///     zero, `null`, and the key absent entirely -- all load, and load to the value they meant.
///
/// THE ZERO IS THE CASE THAT MATTERS. A countdown of zero is a bucket whose expiry is due, and
/// the biased representation has to keep it distinct from absent; a version of this change that
/// read zero as absent passes every other assertion in this module.
#[test]
fn the_stored_spelling_of_a_bucket_node_did_not_move() {
    // --- NEW WRITE: the exact bytes. ---
    let with_ttl = serde_json::to_string(&wire_fixture(Some(5_000))).expect("a node serializes");
    assert_eq!(
        "{\"routing_slot\":7,\"layout\":\"SingleBlockObject\",\"dirty\":false,\"deleted\":false,\
         \"meta_loaded\":true,\"loading\":false,\"in_memory\":true,\"ttl_ms\":5000,\
         \"dirty_generation\":3,\"object_index\":[42],\
         \"deleted_object_index\":[],\"page_index\":{}}",
        with_ttl,
        "the stored spelling of a bucket node moved"
    );
    // TWELVE KEYS, NOT THIRTEEN, AND THAT IS THE DELIBERATE PART OF THIS CHANGE.
    // `last_dump_sequence` is not written any more -- the node does not hold it. It is still
    // ACCEPTED on the way in, which is what the OLD READ fixtures below carry it for.
    assert!(
        !with_ttl.contains("last_dump_sequence"),
        "the node still writes a last_dump_sequence it does not hold: {with_ttl}"
    );
    let without = serde_json::to_string(&wire_fixture(None)).expect("a node serializes");
    assert!(
        without.contains("\"ttl_ms\":null"),
        "an absent countdown must still be written as null, not omitted: {without}"
    );
    let at_zero = serde_json::to_string(&wire_fixture(Some(0))).expect("a node serializes");
    assert!(
        at_zero.contains("\"ttl_ms\":0"),
        "a countdown of zero must be written as 0, not as null and not as 1: {at_zero}"
    );

    // --- The two sides must not agree by accident. ---
    assert_ne!(
        without, at_zero,
        "absent and zero must not write the same bytes, or the wire has lost the distinction \
         this representation was built to keep"
    );

    // --- OLD READ: every spelling an index written before this change can hold. ---
    for (stored, expected) in [
        ("5000", Some(5_000u64)),
        ("0", Some(0)),
        ("null", None),
        ("18446744073709551615", Some(u64::MAX - 1)),
    ] {
        let json = format!(
            "{{\"routing_slot\":7,\"layout\":\"SingleBlockObject\",\"dirty\":false,\
             \"deleted\":false,\"meta_loaded\":true,\"loading\":false,\"in_memory\":true,\
             \"ttl_ms\":{stored},\"dirty_generation\":3,\"last_dump_sequence\":11,\
             \"object_ids\":[42],\"page_refs\":{{}}}}"
        );
        let node: BucketNode = serde_json::from_str(&json).expect("a stored node loads");
        assert_eq!(
            expected,
            node.ttl_ms.ms(),
            "a stored countdown of {stored} loaded as {:?}",
            node.ttl_ms.ms()
        );
    }

    // The key absent entirely: an index older than the field.
    let missing = "{\"routing_slot\":7,\"layout\":\"Empty\",\"dirty\":false,\"deleted\":false,\
                   \"meta_loaded\":true,\"loading\":false,\"in_memory\":false,\
                   \"dirty_generation\":0,\"last_dump_sequence\":0}";
    let node: BucketNode = serde_json::from_str(missing).expect("a node without the key loads");
    assert_eq!(None, node.ttl_ms.ms(), "a missing countdown key must load as absent");

    // --- ROUND TRIP, at the values that discriminate. ---
    for ms in [None, Some(0), Some(1), Some(5_000), Some(u64::MAX - 1)] {
        let node = wire_fixture(ms);
        let json = serde_json::to_string(&node).expect("a node serializes");
        let back: BucketNode = serde_json::from_str(&json).expect("a node loads");
        assert_eq!(ms, back.ttl_ms.ms(), "a countdown of {ms:?} did not round-trip");
    }
}

/// THE BIAS SATURATES AT THE TOP, AND NOWHERE ELSE.
///
/// A representation that gains a byte by giving up a value has to say which value, and has to
/// come back wrong in a direction that reads as wrong. Every countdown below `u64::MAX` survives
/// exactly; `u64::MAX` alone comes back one millisecond short, which is 584,542,046 years after
/// the deadline either way.
///
/// THE PROBE VALUES ARE CHOSEN SO TRUNCATION AND SATURATION DISAGREE. A wrapping bias would turn
/// `u64::MAX` into a countdown of zero -- an expiry that is due -- which is both plausible and
/// wrong, and is the mutant this test exists to kill.
#[test]
fn the_biased_countdown_saturates_at_the_top_and_is_exact_below_it() {
    for ms in [0u64, 1, 2, 5_000, 1 << 32, u64::MAX - 2, u64::MAX - 1] {
        assert_eq!(
            Some(ms),
            BucketTtl::from_ms(Some(ms)).ms(),
            "a countdown of {ms} must round-trip through the biased representation exactly"
        );
    }

    let top = BucketTtl::from_ms(Some(u64::MAX));
    assert_eq!(
        Some(u64::MAX - 1),
        top.ms(),
        "the one countdown the bias cannot hold must saturate one below the top"
    );
    assert_ne!(
        Some(0),
        top.ms(),
        "a countdown of u64::MAX came back as zero -- that is a wrapping bias, and zero means an \
         expiry that is due"
    );
    assert!(top.is_some(), "a saturated countdown is still a countdown, not an absence");

    // Absent and zero are two states, not one.
    assert_eq!(None, BucketTtl::ABSENT.ms());
    assert!(!BucketTtl::ABSENT.is_some());
    assert_eq!(Some(0), BucketTtl::from_ms(Some(0)).ms());
    assert!(
        BucketTtl::from_ms(Some(0)).is_some(),
        "a countdown of zero must report as present; a bucket whose expiry is due is exactly \
         what `ttl_bucket_count` is counting"
    );
    assert_ne!(
        BucketTtl::ABSENT,
        BucketTtl::from_ms(Some(0)),
        "absent and zero must not compare equal"
    );
    assert_eq!(BucketTtl::ABSENT, BucketTtl::default(), "a fresh node holds no countdown");
}

/// THE COUNTDOWN REACHES BOTH REPORTS, AND AN EMPTY EXPIRY TABLE CLEARS IT.
///
/// The representation is held by the tests above; this is its other half, and it exists because a
/// mutation run said it did not. Three production readers carry the countdown out of the node --
/// `refresh_bucket_runtime_flags`, which computes it, and the two reports that publish it -- and
/// a mutant in each of the three survived every test in the tree that names a bucket flag:
///
///   * `refresh_bucket_runtime_flags` leaving a countdown of ZERO where it should clear. Zero is
///     "this bucket's expiry is due", so this one does not read as a bug anywhere; it reads as a
///     store whose every bucket is about to expire.
///   * `bucket_store::runtime_report` publishing `ttl_ms: None` on every row. `ttl_bucket_count`
///     is computed from the node and not from the row, so the count stays right while every row
///     says the opposite.
///   * `storage_physical_index_report` doing the same.
///
/// `bucket_store_reports_all_layout_states_and_runtime_flags` builds its nodes by hand and never
/// runs a refresh; `bucket_runtime_flags_match_full_sweep` compares the targeted refresh against
/// a full sweep, so a mutation in the shared computation moves both sides and cancels. Neither
/// could have caught any of the three, which is why the value is asserted here and not the flag.
#[test]
fn the_countdown_reaches_both_reports_and_an_empty_expiry_table_clears_it() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = budget_engine(dir.path());
    for key in ["countdown-a", "countdown-b"] {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: key.to_string(),
                value: vec![b'v'; 16],
            },
        });
        assert!(response.status.ok, "the seed must ack: {:?}", response.status);
    }

    // --- NOTHING EXPIRES. Every bucket must report ABSENT, not a countdown of zero. ---
    {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard is loaded");
        assert!(
            !shard.bucket_index.bucket_map.is_empty(),
            "denominator: no buckets, so every assertion below is vacuous"
        );
        for (routing_bucket, bucket) in &shard.bucket_index.bucket_map {
            assert_eq!(
                None,
                bucket.ttl_ms.ms(),
                "bucket {routing_bucket} holds nothing that expires and reports a countdown of \
                 {:?}; a countdown of zero here means every bucket in the store is due",
                bucket.ttl_ms.ms()
            );
            assert!(!bucket.ttl_ms.is_some(), "bucket {routing_bucket} reports a countdown it does not have");
        }
        let report = crate::engine::bucket_store::runtime_report(shard);
        assert_eq!(
            0, report.ttl_bucket_count,
            "no key has an expiry, so no bucket should be counted as holding one"
        );
        assert!(
            report.buckets.iter().all(|row| row.ttl_ms.is_none()),
            "a published row carries a countdown no bucket holds"
        );
    }

    // --- ONE KEY IS ARMED. The VALUE has to arrive, not just the flag. ---
    const ARMED_MS: u64 = 600_000;
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::CommonExpire {
            key: "countdown-a".to_string(),
            ttl_ms: ARMED_MS,
        },
    });
    assert!(response.status.ok, "arming must ack: {:?}", response.status);

    {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard is loaded");
        let report = crate::engine::bucket_store::runtime_report(shard);
        assert_eq!(1, report.ttl_bucket_count, "exactly one bucket now holds an expiry");
        let armed: Vec<u64> = report.buckets.iter().filter_map(|row| row.ttl_ms).collect();
        assert_eq!(
            1,
            armed.len(),
            "exactly one published row must carry the countdown; {} did",
            armed.len()
        );
        // The BOUNDS are what make this a test of the value rather than of its presence: zero is
        // what a cleared countdown looks like, and ARMED_MS + 1 is what the stored bias looks
        // like if it ever escapes the accessor.
        assert!(
            armed[0] > 0 && armed[0] <= ARMED_MS,
            "the published countdown is {}; it must be a real number of milliseconds, neither \
             zero nor the biased value the node holds internally",
            armed[0]
        );
    }

    let physical = engine.storage_physical_index_report(1);
    let published: Vec<u64> = physical
        .bucket_nodes
        .iter()
        .filter_map(|bucket| bucket.ttl_ms)
        .collect();
    assert_eq!(
        1,
        published.len(),
        "the physical index report must publish the countdown too; {} of its {} bucket rows \
         carried one",
        published.len(),
        physical.bucket_nodes.len()
    );
    assert!(
        published[0] > 0 && published[0] <= ARMED_MS,
        "the physical report published a countdown of {}",
        published[0]
    );
}

// -------------------------------------------------------------------------------------------
// WHAT IT COSTS AT TWO LARGE CORPUS SIZES, AND WHAT AN INSTRUMENT THAT IS NOT `size_of` SEES.
// -------------------------------------------------------------------------------------------

/// The counting allocator, used here as the SECOND instrument.
///
/// `Clone` on a `BTreeMap` rebuilds the tree node for node, so the bytes charged across one
/// `clone()` are the map's own nodes -- the key array, the value array, the node header and every
/// slot a node has not filled. That number is measured by the allocator and owes nothing to
/// `size_of` x count, which is what makes the difference between them a reading rather than an
/// identity. A residual computed from one instrument twice cannot notice anything.
#[cfg(feature = "alloc-probe")]
fn clone_alloc_bytes<T: Clone>(value: &T) -> u64 {
    let probe = Probe::start();
    let copy = value.clone();
    let counts = probe.stop();
    std::hint::black_box(&copy);
    drop(copy);
    counts.alloc_bytes
}

/// THE PLANTED MARKER. Recovered exactly, or every residual below is noise.
///
/// The failure this guards against is the one that reads as good news: an instrument that reports
/// near zero makes a structure look free. A megabyte is planted and has to come back as a
/// megabyte -- not less, which would be blindness, and not much more, which would be the probe
/// charging for something other than the clone.
#[cfg(feature = "alloc-probe")]
#[test]
fn the_clone_instrument_recovers_a_planted_megabyte_exactly() {
    const PLANTED: usize = 1 << 20;
    let marker: Vec<u8> = vec![0xA5; PLANTED];
    let measured = clone_alloc_bytes(&marker);
    println!(
        "planted {PLANTED} B, instrument charged {measured} B ({:.4}x)",
        measured as f64 / PLANTED as f64
    );
    assert_eq!(
        PLANTED as u64, measured,
        "the clone instrument charged {measured} B for a planted {PLANTED} B; a residual taken \
         with it would be measuring the instrument"
    );
}

/// WHAT THE BUCKET NODE COSTS AT TWO LARGE CORPUS SIZES, IN TOTAL AND PER RECORD.
///
/// Two corpora three times apart, both large, because the question this module exists to answer
/// is what the structure costs a STORE and not what it costs one bucket. The per-record figure is
/// the one that multiplies out to any scale, and it is only usable if it is flat -- a per-record
/// cost that grows with the corpus is a finding, not a budget, and the flatness check below says
/// which of the two it is.
///
/// THE STORE PATH LENGTH IS HELD CONSTANT and asserted: `tempfile` names every directory with the
/// same number of characters, and allocation bytes move with the path at about six bytes per
/// character, so an arm whose path was one character longer would report a different residual for
/// a reason that has nothing to do with the structure.
///
/// THE DIRTY FRACTION IS PRINTED because it is what prices the one decline this module could not
/// settle from widths alone: the two `#[serde(skip)]` log claims are sixteen bytes that touch no
/// stored shape, and moving them into a side map keyed by bucket is a win exactly when few
/// buckets are dirty and a loss when most are.
///
/// THE RESIDUAL IS THE POINT OF THE SECOND INSTRUMENT. `size_of` x count is what the node
/// occupies; the map that holds the nodes charges more than that, and the difference -- key
/// arrays, node headers, unfilled slots and whatever each node owns on the heap -- is reported
/// rather than assumed away, so a change in it is visible.
#[test]
#[ignore = "seeds 80,000 then 240,000 records; run by name"]
fn what_the_bucket_node_costs_at_two_large_corpus_sizes() {
    let mut path_lengths: Vec<usize> = Vec::new();
    let mut per_record: Vec<(usize, f64)> = Vec::new();

    for (label, strings_n, series_keys, series_points) in [
        ("80,000 records", 40_000usize, 40usize, 1_000usize),
        ("240,000 records", 120_000usize, 120usize, 1_000usize),
    ] {
        let dir = tempfile::tempdir().expect("tempdir");
        path_lengths.push(dir.path().as_os_str().len());
        let engine = budget_engine(dir.path());
        budget_seed(&engine, strings_n, series_keys, series_points);

        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard is loaded");
        let counts = count_items(shard);

        assert_eq!(
            strings_n + series_keys * series_points,
            counts.records,
            "denominator: the shard must hold every record seeded"
        );
        assert!(
            counts.bucket_nodes > 0,
            "denominator: no routing buckets, so every figure below divides by nothing"
        );

        let width = size_of::<BucketNode>();
        let accounted = width * counts.bucket_nodes;
        // The width this structure carried before the tombstone index stopped spending a
        // sixteen-byte enum on a case it is in 2.32% of the time. A literal, because the shape it
        // names no longer exists to be measured; `what_each_declined_shape_of_the_bucket_node_
        // would_cost` holds a mirror of it to 200, and a mirror of the shape before that to 208,
        // so neither literal can drift away from what it claims.
        const WIDTH_BEFORE: usize = 200;
        let before = WIDTH_BEFORE * counts.bucket_nodes;

        let dirty_buckets = shard
            .bucket_index
            .bucket_map
            .values()
            .filter(|bucket| bucket.dirty())
            .count();

        println!("\n=== {label} ===");
        println!(
            "  records={} buckets={} page entries={}",
            counts.records, counts.bucket_nodes, counts.block_index_entries
        );
        println!(
            "  BucketNode: {width} B x {} = {accounted} B ({:.2} MiB), {:.2} B/record",
            counts.bucket_nodes,
            accounted as f64 / (1024.0 * 1024.0),
            accounted as f64 / counts.records as f64
        );
        println!(
            "  before this change: {WIDTH_BEFORE} B x {} = {before} B ({:.2} MiB), {:.2} B/record \
             -- and 208 B x that before #1958",
            counts.bucket_nodes,
            before as f64 / (1024.0 * 1024.0),
            before as f64 / counts.records as f64
        );
        println!(
            "  SAVED: {} B ({:.2} MiB), {:.2} B/record, {:.1}% of the structure",
            before - accounted,
            (before - accounted) as f64 / (1024.0 * 1024.0),
            (before - accounted) as f64 / counts.records as f64,
            100.0 * (before - accounted) as f64 / before as f64
        );
        println!(
            "  dirty buckets: {dirty_buckets} of {} ({:.1}%)",
            counts.bucket_nodes,
            100.0 * dirty_buckets as f64 / counts.bucket_nodes as f64
        );

        #[cfg(feature = "alloc-probe")]
        {
            let measured = clone_alloc_bytes(&shard.bucket_index.bucket_map);
            let residual = measured as i64 - accounted as i64;
            println!(
                "  the map that holds them, charged by the allocator: {measured} B ({:.2} MiB)",
                measured as f64 / (1024.0 * 1024.0)
            );
            println!(
                "  RESIDUAL outside the node widths: {residual} B, {:.2} B/bucket -- key arrays, \
                 node headers, unfilled slots and what each node owns on the heap",
                residual as f64 / counts.bucket_nodes as f64
            );
            assert!(
                residual > 0,
                "the allocator charged {measured} B for a map of {} nodes whose widths add up to \
                 {accounted} B; a residual at or below zero means the two instruments are not \
                 independent and the subtraction is an identity",
                counts.bucket_nodes
            );
        }

        per_record.push((
            counts.records,
            accounted as f64 / counts.records as f64,
        ));
    }

    assert_eq!(2, path_lengths.len(), "both arms must have run");
    assert_eq!(
        path_lengths[0], path_lengths[1],
        "the store path length moved between arms ({} then {}); allocation bytes move with it at \
         about six bytes a character",
        path_lengths[0], path_lengths[1]
    );

    let (small_records, small) = per_record[0];
    let (big_records, big) = per_record[1];
    assert!(
        big_records > small_records,
        "the second arm must be the larger corpus, or the flatness claim compares nothing"
    );
    let ratio = big / small;
    println!(
        "\n=== flatness: {small:.2} B/record at {small_records} -> {big:.2} B/record at \
         {big_records} ({ratio:.3}x) ==="
    );
    assert!(
        ratio < 1.25,
        "BucketNode costs {small:.2} B/record at {small_records} records and {big:.2} at \
         {big_records} ({ratio:.3}x) -- it is growing with the corpus, which is a finding and not \
         a budget"
    );
}

// -------------------------------------------------------------------------------------------
// THE OBJECT SIDE OF THE NODE: HOW MANY OBJECTS A BUCKET ACTUALLY HOLDS.
//
// `object_index` and `deleted_object_index` are 16 bytes each and both are `ObjectIndex`, which
// already tiers: `Empty` costs nothing beyond the tag, `One` holds the id inline, and only
// `Many` allocates. Whether that tiering is the right shape -- and whether a further tier would
// pay -- is a question about the DISTRIBUTION of objects per bucket, and a mean cannot answer
// it. A mean of 1.02 is consistent with "almost every bucket holds one" and with "most hold one
// and a handful hold thousands", and those have opposite answers.
//
// So this reports a HISTOGRAM, at two corpus sizes, over a seed that produces every model shape
// the engine files into buckets -- and asserts that the multi-object case is reached at all,
// because a fixture that only ever produces one object per bucket cannot tell a correct tiering
// from a constant.
// -------------------------------------------------------------------------------------------

/// The objects-per-bucket distribution of one shard, for one of the two object indexes.
#[derive(Default)]
struct Occupancy {
    /// `buckets[n]` is how many buckets hold exactly `n` objects, for `n` up to 8; everything
    /// above lands in `over_eight` and is described by `max` and `total`.
    buckets: [usize; 9],
    over_eight: usize,
    max: usize,
    total_objects: usize,
    total_buckets: usize,
}

impl Occupancy {
    fn observe(&mut self, len: usize) {
        self.total_buckets += 1;
        self.total_objects += len;
        self.max = self.max.max(len);
        if len <= 8 {
            self.buckets[len] += 1;
        } else {
            self.over_eight += 1;
        }
    }

    fn at_least(&self, n: usize) -> usize {
        let below: usize = self.buckets[..n.min(9)].iter().sum();
        self.total_buckets - below
    }

    fn mean(&self) -> f64 {
        if self.total_buckets == 0 {
            0.0
        } else {
            self.total_objects as f64 / self.total_buckets as f64
        }
    }

    fn report(&self, label: &str) {
        println!("  {label}: {} buckets, {} objects, mean {:.4}, max {}",
            self.total_buckets, self.total_objects, self.mean(), self.max);
        for n in 0..=8 {
            if self.buckets[n] == 0 {
                continue;
            }
            println!(
                "    holds {n:>2}: {:>8} buckets  ({:>6.2}%)",
                self.buckets[n],
                100.0 * self.buckets[n] as f64 / self.total_buckets as f64
            );
        }
        if self.over_eight > 0 {
            println!(
                "    holds >8: {:>8} buckets  ({:>6.2}%)",
                self.over_eight,
                100.0 * self.over_eight as f64 / self.total_buckets as f64
            );
        }
    }
}

fn occupancy_of(shard: &crate::engine::state::ShardState) -> (Occupancy, Occupancy) {
    let mut live = Occupancy::default();
    let mut tombstones = Occupancy::default();
    for bucket in shard.bucket_index.bucket_map.values() {
        live.observe(bucket.object_index.len());
        tombstones.observe(bucket.deleted_object_index.len());
    }
    (live, tombstones)
}

/// A seed that produces every model shape this engine files into a bucket, not just the two the
/// rest of this module uses.
///
/// The shapes matter to the question, because they reach the object index differently. A string
/// and a feature series are one object each: one key, no component, one id. A hash, a set, a
/// zset and a list are one object PER MEMBER, because the object id is hashed over
/// `shard:kind:key:component` and the member name is the component -- while the ROUTING bucket is
/// hashed over the key alone. So a single hash with eight fields files eight distinct object ids
/// into one bucket, and a seed without them cannot produce the multi-object case on purpose.
///
/// `deletes` then removes a fraction of the string keys, which is the only thing that writes
/// `deleted_object_index` at all.
///
/// `collections` selects the mix: with it off the seed is strings and feature series only, which
/// is the shape the rest of this module seeds and the one that produces no multi-object bucket
/// at all.
fn objside_seed(engine: &TemporalEngine, scale: usize, collections: bool) {
    // Strings: one object per key, the shape the rest of the module seeds.
    for chunk_start in (0..scale * 4).step_by(1_000) {
        let commands = (chunk_start..(chunk_start + 1_000).min(scale * 4))
            .map(|i| Command::StringSet {
                key: format!("s{i}"),
                value: vec![b'v'; 32],
            })
            .collect::<Vec<_>>();
        if commands.is_empty() {
            continue;
        }
        let response = engine.batch_execute(crate::types::BatchExecuteRequest {
            shard_id: 1,
            commands,
        });
        assert!(response.status.ok, "string seed must ack: {:?}", response.status);
    }

    // Hashes: eight fields per key, so eight component object ids on one routing key.
    let commands = (0..if collections { scale } else { 0 })
        .flat_map(|i| {
            (0..8).map(move |f| Command::HashSet {
                key: format!("h{i}"),
                field: format!("f{f}"),
                value: vec![b'h'; 24],
            })
        })
        .collect::<Vec<_>>();
    for chunk in commands.chunks(1_000) {
        let response = engine.batch_execute(crate::types::BatchExecuteRequest {
            shard_id: 1,
            commands: chunk.to_vec(),
        });
        assert!(response.status.ok, "hash seed must ack: {:?}", response.status);
    }

    // Sorted sets and lists: four members each, the same one-object-per-member shape.
    let collection_keys = if collections { scale } else { 0 };
    let commands = (0..collection_keys)
        .flat_map(|i| {
            (0..4).map(move |m| Command::ZSetAdd {
                key: format!("z{i}"),
                member: format!("m{m}").into_bytes(),
                score: m as f64,
            })
        })
        .chain((0..collection_keys).flat_map(|i| {
            (0..4).map(move |m| Command::ListPush {
                key: format!("l{i}"),
                member: format!("i{m}").into_bytes(),
                left: false,
            })
        }))
        .chain((0..collection_keys).flat_map(|i| {
            (0..4).map(move |m| Command::SetAdd {
                key: format!("t{i}"),
                member: format!("e{m}").into_bytes(),
            })
        }))
        .collect::<Vec<_>>();
    for chunk in commands.chunks(1_000) {
        let response = engine.batch_execute(crate::types::BatchExecuteRequest {
            shard_id: 1,
            commands: chunk.to_vec(),
        });
        assert!(response.status.ok, "collection seed must ack: {:?}", response.status);
    }

    // Feature series: one object, many points.
    for k in 0..(scale / 100).max(1) {
        for chunk_start in (0..1_000usize).step_by(500) {
            let points = (chunk_start..(chunk_start + 500).min(1_000))
                .map(|t| crate::types::FeaturePoint {
                    timestamp_ms: 1_700_000_000_000 + t as u64,
                    value: vec![b'f'; 32],
                })
                .collect::<Vec<_>>();
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::FeatureAppend {
                    key: format!("f{k}"),
                    points,
                },
            });
            assert!(response.status.ok, "feature seed must ack: {:?}", response.status);
        }
    }

    // Deletes: one string key in twenty. The only writer of `deleted_object_index`.
    let commands = (0..scale * 4)
        .step_by(20)
        .map(|i| Command::CommonDelete { key: format!("s{i}") })
        .collect::<Vec<_>>();
    for chunk in commands.chunks(1_000) {
        let response = engine.batch_execute(crate::types::BatchExecuteRequest {
            shard_id: 1,
            commands: chunk.to_vec(),
        });
        assert!(response.status.ok, "delete seed must ack: {:?}", response.status);
    }
}

/// THE DISTRIBUTION, AS A HISTOGRAM, AT TWO CORPUS SIZES AND TWO SHAPE MIXES.
///
/// This is the measurement the object side of the node turns on, and it is reported before any
/// representation is priced, because the numbers decide which representations are even worth
/// pricing. A MEAN CANNOT ANSWER IT: a mean near one is consistent with "almost every bucket
/// holds one object" and with "most hold one and a few hold hundreds", and those two want
/// opposite representations.
///
/// TWO MIXES, because the answer is not a property of the engine alone. A string or a feature
/// series is ONE object: one key, no component, one id, one bucket. A hash, a set, a sorted set
/// or a list is one object PER MEMBER, because the object id is hashed over
/// `shard:kind:key:component` while the ROUTING bucket is hashed over the key alone -- so a hash
/// with eight fields files eight distinct ids into the one bucket its key routes to. A store of
/// strings and series therefore has NO multi-object buckets at all, and a store with collections
/// in it has as many objects in a bucket as that key has members. Reporting one mix and calling
/// it the distribution would have been reporting the seed.
///
/// THE MULTI-OBJECT CASE IS ASSERTED PRESENT in the mix that is supposed to produce it, and
/// asserted ABSENT in the mix that is not. A fixture that only ever produces one object per
/// bucket would report a perfect histogram for a representation that had thrown the second
/// object away; a fixture whose two mixes could not be told apart would prove that the mix is
/// not what decides this.
///
/// THE STORE PATH LENGTH IS HELD CONSTANT across the arms and asserted, the same way the other
/// corpus tests in this module hold it: allocation bytes move with the path at about six bytes a
/// character. Counts are immune to it, and these are counts -- the assertion is here so the arms
/// stay comparable if a later reader adds an allocator reading to them.
#[test]
#[ignore = "seeds four corpora; run by name"]
fn how_many_objects_a_bucket_holds_at_two_corpus_sizes_and_two_shape_mixes() {
    let mut path_lengths: Vec<usize> = Vec::new();
    // (mix, records, mean, share holding exactly one, share holding two or more, tombstone share)
    let mut summary: Vec<(&'static str, usize, f64, f64, f64, f64)> = Vec::new();

    for (mix, collections) in [("strings and series only", false), ("every model shape", true)] {
        for (size, scale) in [("small corpus", 2_000usize), ("large corpus", 20_000usize)] {
            let dir = tempfile::tempdir().expect("tempdir");
            path_lengths.push(dir.path().as_os_str().len());
            let engine = budget_engine(dir.path());
            objside_seed(&engine, scale, collections);

            let shards = engine.shards.read().expect("engine lock poisoned");
            let shard = shards.get(&1).expect("shard is loaded");
            let (live, tombstones) = occupancy_of(shard);
            let counts = count_items(shard);

            assert!(
                live.total_buckets > 0,
                "denominator: no routing buckets, so every share below divides by nothing"
            );
            assert!(
                live.total_objects > 0,
                "denominator: the shard holds no objects, so the histogram is a row of zeroes"
            );

            println!("\n=== {mix}, {size}: objects per bucket ===");
            println!("  buckets={} records={}", live.total_buckets, counts.records);
            live.report("object_index (live)");
            tombstones.report("deleted_object_index (tombstones)");

            let multi = live.at_least(2);
            // The single case has to be reached in BOTH mixes, or the histogram is a constant.
            assert!(
                live.buckets[1] > 0,
                "{mix}: no bucket holds exactly one object; the inline arm was never built"
            );
            if collections {
                assert!(
                    multi > 0,
                    "{mix}: the fixture produced {} buckets and not one of them holds two \
                     objects; a histogram from a fixture that cannot reach the multi-object case \
                     cannot tell a correct representation from a constant",
                    live.total_buckets
                );
                assert!(
                    live.max >= 2,
                    "{mix}: the widest bucket holds {} object(s); the multi-entry arm was never \
                     constructed",
                    live.max
                );
            } else {
                // The CONTROL on the claim that the mix is what decides this. If a store of
                // strings and series also produced multi-object buckets, the explanation above
                // would be wrong and the two mixes would not be measuring different things.
                assert_eq!(
                    0, multi,
                    "{mix}: {multi} buckets hold two or more objects, but a string and a series \
                     are one object each -- either routing is colliding keys into one bucket or \
                     the explanation this module gives for the multi-object case is wrong"
                );
            }

            let single_share = 100.0 * live.buckets[1] as f64 / live.total_buckets as f64;
            let multi_share = 100.0 * multi as f64 / live.total_buckets as f64;
            let carrying = tombstones.total_buckets - tombstones.buckets[0];
            let tombstone_share = 100.0 * carrying as f64 / tombstones.total_buckets as f64;
            println!(
                "  SHARES: exactly one {single_share:.2}%, two or more {multi_share:.2}%, \
                 buckets carrying any tombstone {tombstone_share:.2}%"
            );
            summary.push((
                mix,
                counts.records,
                live.mean(),
                single_share,
                multi_share,
                tombstone_share,
            ));
        }
    }

    assert_eq!(4, path_lengths.len(), "all four arms must have run");
    let first = path_lengths[0];
    for (at, length) in path_lengths.iter().enumerate() {
        assert_eq!(
            first, *length,
            "the store path length moved at arm {at} ({first} then {length})"
        );
    }

    println!("\n=== the distribution, summarised ===");
    println!(
        "  {:<26} {:>9} {:>8} {:>10} {:>12} {:>12}",
        "mix", "records", "mean", "holds one", "holds two+", "tombstoned"
    );
    for (mix, records, mean, single, multi, tomb) in &summary {
        println!(
            "  {mix:<26} {records:>9} {mean:>8.4} {single:>9.2}% {multi:>11.2}% {tomb:>11.2}%"
        );
    }

    // --- FLATNESS: each mix must report the same distribution at both corpus sizes. ---
    for pair in [(0usize, 1usize), (2usize, 3usize)] {
        let (mix, small_records, small_mean, small_one, _, small_tomb) = summary[pair.0];
        let (mix_big, big_records, big_mean, big_one, _, big_tomb) = summary[pair.1];
        assert_eq!(mix, mix_big, "the summary rows are not paired by mix");
        assert!(big_records > small_records, "the second arm must be the larger corpus");
        assert!(
            (big_one - small_one).abs() < 1.0,
            "{mix}: {small_one:.2}% of buckets hold one object at {small_records} records and \
             {big_one:.2}% at {big_records}; the distribution is moving with the corpus, which \
             is a finding and not a budget"
        );
        assert!(
            (big_tomb - small_tomb).abs() < 1.0,
            "{mix}: the tombstone-carrying share moved from {small_tomb:.2}% to {big_tomb:.2}% \
             across the corpus"
        );
        println!(
            "  {mix}: mean {small_mean:.4} -> {big_mean:.4} across a tenfold corpus"
        );
    }

    // --- THE TWO MIXES MUST DIFFER, or the seed is not what this test says it is. ---
    let simple_multi = summary[1].4;
    let mixed_multi = summary[3].4;
    assert!(
        mixed_multi > simple_multi + 10.0,
        "the two mixes reported {simple_multi:.2}% and {mixed_multi:.2}% of buckets holding two \
         or more objects; if the mix does not change the distribution then one seed was used \
         twice and this test compares an arm with itself"
    );
}

// -------------------------------------------------------------------------------------------
// THE CANDIDATES FOR THE OBJECT SIDE, MEASURED AGAINST EACH OTHER.
//
// Each shape below is a MIRROR built from the same field types as the live declaration, so the
// widths are measurements and not estimates, and the mirror is checked against the declaration
// FIRST -- without that check every row is fiction.
// -------------------------------------------------------------------------------------------

/// The shape BEFORE this change: the same tiering with a search tree in the rare arm.
#[derive(Clone)]
enum MirrorTiered {
    Empty,
    One(u64),
    Many(Box<BTreeSet<u64>>),
}

/// The DECLARATION, restated: the same tiering with the rare arm holding a sorted run.
///
/// Checked against `ObjectIndex` twice below -- once on width, and once on what the allocator
/// charges for a clone of the whole distribution, which is the check that would catch a mirror
/// that had the right width and the wrong arm.
#[derive(Clone)]
enum MirrorSortedRun {
    Empty,
    One(u64),
    Many(Box<Vec<u64>>),
}

/// No tiering at all: one heap set per bucket, always. Eight bytes of field.
#[derive(Clone)]
struct MirrorAlwaysBoxed(Box<BTreeSet<u64>>);

/// No tiering and no box: the set held inline, which is what the field was before the tiering.
#[derive(Clone)]
struct MirrorInline(BTreeSet<u64>);

/// The DECLARATION of the tombstone side, restated: absence costs a pointer and nothing else.
#[derive(Clone)]
struct MirrorAbsentIsFree(Option<Box<ObjectIndex>>);

fn mirror_tiered(ids: &[u64]) -> MirrorTiered {
    match ids.len() {
        0 => MirrorTiered::Empty,
        1 => MirrorTiered::One(ids[0]),
        _ => MirrorTiered::Many(Box::new(ids.iter().copied().collect())),
    }
}

fn mirror_sorted_run(ids: &[u64]) -> MirrorSortedRun {
    match ids.len() {
        0 => MirrorSortedRun::Empty,
        1 => MirrorSortedRun::One(ids[0]),
        _ => {
            let mut run = ids.to_vec();
            run.sort_unstable();
            MirrorSortedRun::Many(Box::new(run))
        }
    }
}

fn mirror_absent_is_free(ids: &[u64]) -> MirrorAbsentIsFree {
    if ids.is_empty() {
        MirrorAbsentIsFree(None)
    } else {
        MirrorAbsentIsFree(Some(Box::new(ids.iter().copied().collect())))
    }
}

/// The ids each bucket's live and tombstone indexes hold, read off a seeded shard.
fn object_id_rows(shard: &crate::engine::state::ShardState) -> (Vec<Vec<u64>>, Vec<Vec<u64>>) {
    let mut live = Vec::new();
    let mut tombstones = Vec::new();
    for bucket in shard.bucket_index.bucket_map.values() {
        live.push(bucket.object_index.iter().copied().collect::<Vec<u64>>());
        tombstones.push(bucket.deleted_object_index.iter().copied().collect::<Vec<u64>>());
    }
    (live, tombstones)
}

/// EVERY CANDIDATE'S WIDTH, AND THE MIRROR CHECKED AGAINST THE DECLARATION FIRST.
///
/// The check is the whole reason these are numbers. A mirror that has drifted from the live type
/// reports whatever it was last written to report, and the table underneath it stays plausible.
#[test]
fn what_each_shape_of_the_object_index_would_cost_in_width() {
    // --- The controls: each mirror of a LIVE shape must be that shape. ---
    assert_eq!(
        size_of::<ObjectIndex>(),
        size_of::<MirrorSortedRun>(),
        "the mirror of the live tiering is {} bytes against the declaration's {}; it has drifted \
         and every row below is fiction",
        size_of::<MirrorSortedRun>(),
        size_of::<ObjectIndex>()
    );
    assert_eq!(
        align_of::<ObjectIndex>(),
        align_of::<MirrorSortedRun>(),
        "the mirror's alignment does not match the declaration's"
    );
    assert_eq!(
        size_of::<DeletedObjectIndex>(),
        size_of::<MirrorAbsentIsFree>(),
        "the mirror of the tombstone side is {} bytes against the declaration's {}",
        size_of::<MirrorAbsentIsFree>(),
        size_of::<DeletedObjectIndex>()
    );

    let rows: Vec<(&str, usize, &str)> = vec![
        ("rare arm a search tree (before)", size_of::<MirrorTiered>(), "Empty / One(u64) / Many(Box<BTreeSet>)"),
        ("rare arm a sorted run (the live shape)", size_of::<MirrorSortedRun>(), "Empty / One(u64) / Many(Box<Vec>)"),
        ("no tiering, always boxed", size_of::<MirrorAlwaysBoxed>(), "Box<BTreeSet> only"),
        ("no tiering, held inline", size_of::<MirrorInline>(), "BTreeSet held in the node"),
        ("absence costs a pointer (the tombstone side)", size_of::<MirrorAbsentIsFree>(), "Option<Box<ObjectIndex>>"),
    ];
    println!("{:<32} {:>6}  {}", "shape", "width", "representation");
    for (name, width, note) in &rows {
        println!("{name:<32} {width:>6}  {note}");
    }

    // --- The widths that decide the accounting. ---
    assert_eq!(16, size_of::<ObjectIndex>(), "ObjectIndex width moved");
    assert_eq!(8, size_of::<DeletedObjectIndex>(), "DeletedObjectIndex width moved");
    assert_eq!(16, size_of::<MirrorTiered>(), "the tree in the rare arm cost the same width");
    assert_eq!(8, size_of::<MirrorAlwaysBoxed>(), "a bare box is one pointer");
    assert_eq!(24, size_of::<MirrorInline>(), "an inline BTreeSet is three words");
    assert_eq!(8, size_of::<MirrorAbsentIsFree>(), "Option<Box<T>> rides the null niche");

    // --- WHY THE TIERING CANNOT REACH EIGHT BYTES, stated as a property and not an opinion. ---
    //
    // The eight-byte form of this shape is a single word that holds either an object id or a
    // pointer, told apart by a bit the id does not use. This engine's object id is
    // `stable_block_object_id`, a 64-bit FNV-1a over `shard:kind:key:component`, and it reserves
    // nothing: the assertion below walks real keys and shows the ids reaching both ends of the
    // range, so there is no bit a tag could take without losing ids.
    let mut low_bit_set = 0usize;
    let mut high_bit_set = 0usize;
    let mut sampled = 0usize;
    for i in 0..4_096u64 {
        let id = crate::engine::hashing::stable_block_object_id(1, "string", &format!("s{i}"), None);
        sampled += 1;
        if id & 1 == 1 {
            low_bit_set += 1;
        }
        if id & (1 << 63) != 0 {
            high_bit_set += 1;
        }
    }
    assert_eq!(4_096, sampled, "the id sample must have run");
    println!(
        "object ids over {sampled} real keys: {low_bit_set} with the low bit set, \
         {high_bit_set} with the high bit set"
    );
    assert!(
        low_bit_set > 0 && high_bit_set > 0,
        "an object id that never set the low bit ({low_bit_set}) or never set the high bit \
         ({high_bit_set}) would leave a tag somewhere to sit; it sets both, so the one-word form \
         of this tiering would drop ids"
    );
    // And it is not merely that both occur: both occur often enough that no bit is a tag.
    assert!(
        low_bit_set > sampled / 4 && high_bit_set > sampled / 4,
        "the id sample must show both bits in general use, not as rare outliers"
    );
}

/// WHAT THE OBJECT SIDE COSTS, EVERY SHAPE, ON THE MEASURED DISTRIBUTION.
///
/// Width is only half of this structure's cost: the rare arm allocates, and at the measured
/// occupancy the rare arm is not rare. This charges every candidate with the counting allocator
/// over the SAME object-id rows read off a seeded shard, so the comparison is between
/// representations of one distribution rather than between two guesses.
///
/// The `Vec` that holds the mirrors allocates too, and its own allocation is subtracted and
/// reported rather than folded in, so a width difference between candidates cannot be mistaken
/// for a heap difference.
///
/// THE STORE PATH LENGTH IS HELD CONSTANT and asserted: allocation bytes move with the path at
/// about six bytes a character.
#[cfg(feature = "alloc-probe")]
#[test]
#[ignore = "seeds two corpora and charges five representations; run by name"]
fn what_the_object_side_of_the_bucket_node_costs() {
    let mut path_lengths: Vec<usize> = Vec::new();
    let mut per_bucket: Vec<(&'static str, f64)> = Vec::new();

    for (label, scale) in [("small corpus", 2_000usize), ("large corpus", 20_000usize)] {
        let dir = tempfile::tempdir().expect("tempdir");
        path_lengths.push(dir.path().as_os_str().len());
        let engine = budget_engine(dir.path());
        objside_seed(&engine, scale, true);

        let (live_rows, tombstone_rows, page_indexes, page_arms) = {
            let shards = engine.shards.read().expect("engine lock poisoned");
            let shard = shards.get(&1).expect("shard is loaded");
            let (live, dead) = object_id_rows(shard);
            // THE NEXT TERM, carried out of the same shard so it is the same distribution.
            let pages: Vec<BlockIndexMap> = shard
                .bucket_index
                .bucket_map
                .values()
                .map(|bucket| bucket.block_index.clone())
                .collect();
            let mut arms = [0usize; 3];
            for bucket in shard.bucket_index.bucket_map.values() {
                let at = match bucket.block_index.len() {
                    0 => 0,
                    1 => 1,
                    _ => 2,
                };
                arms[at] += 1;
            }
            (live, dead, pages, arms)
        };
        let buckets = live_rows.len();
        assert!(buckets > 0, "denominator: no buckets, so every per-bucket figure is nothing");
        let multi = live_rows.iter().filter(|ids| ids.len() >= 2).count();
        assert!(
            multi > 0,
            "the fixture reached no multi-object bucket, so the rare arm was never built and \
             four of the five rows below would be measuring the same empty shape"
        );

        // Built once, outside the probe, so what the probe charges is the CLONE and not the
        // construction.
        let tiered: Vec<MirrorTiered> = live_rows.iter().map(|ids| mirror_tiered(ids)).collect();
        let sorted: Vec<MirrorSortedRun> =
            live_rows.iter().map(|ids| mirror_sorted_run(ids)).collect();
        let always: Vec<MirrorAlwaysBoxed> = live_rows
            .iter()
            .map(|ids| MirrorAlwaysBoxed(Box::new(ids.iter().copied().collect())))
            .collect();
        let inline: Vec<MirrorInline> = live_rows
            .iter()
            .map(|ids| MirrorInline(ids.iter().copied().collect()))
            .collect();

        let tomb_tiered: Vec<MirrorTiered> =
            tombstone_rows.iter().map(|ids| mirror_tiered(ids)).collect();
        let tomb_absent: Vec<MirrorAbsentIsFree> =
            tombstone_rows.iter().map(|ids| mirror_absent_is_free(ids)).collect();

        // THE DECLARATIONS THEMSELVES, charged the same way. These are the controls: a mirror
        // that matched on width and not on arm would pass every width assertion and report a
        // heap figure for a shape the engine does not have.
        let declared_live: Vec<ObjectIndex> = live_rows
            .iter()
            .map(|ids| ids.iter().copied().collect::<ObjectIndex>())
            .collect();
        let declared_tomb: Vec<DeletedObjectIndex> = tombstone_rows
            .iter()
            .map(|ids| ids.iter().copied().collect::<DeletedObjectIndex>())
            .collect();
        let declared_live_bytes = clone_alloc_bytes(&declared_live);
        let declared_tomb_bytes = clone_alloc_bytes(&declared_tomb);

        // The spine each candidate's vector allocates for itself, charged separately so it is
        // not read as heap the representation owns.
        let spine = |width: usize| (width * buckets) as u64;

        let sorted_bytes = clone_alloc_bytes(&sorted);
        assert_eq!(
            declared_live_bytes, sorted_bytes,
            "the declaration charged {declared_live_bytes} B and its mirror {sorted_bytes} B for \
             the same ids; the mirror is not describing the live arm and the table below is \
             fiction"
        );
        let absent_bytes = clone_alloc_bytes(&tomb_absent);
        assert_eq!(
            declared_tomb_bytes, absent_bytes,
            "the tombstone declaration charged {declared_tomb_bytes} B and its mirror \
             {absent_bytes} B for the same ids"
        );

        let rows: Vec<(&'static str, usize, u64)> = vec![
            ("live index: rare arm a search tree (before)", size_of::<MirrorTiered>(), clone_alloc_bytes(&tiered)),
            ("live index: rare arm a sorted run (now)", size_of::<ObjectIndex>(), declared_live_bytes),
            ("live index: no tiering, always boxed", size_of::<MirrorAlwaysBoxed>(), clone_alloc_bytes(&always)),
            ("live index: no tiering, held inline", size_of::<MirrorInline>(), clone_alloc_bytes(&inline)),
        ];

        println!("\n=== {label}: {buckets} buckets, {multi} of them multi-object ===");
        println!(
            "  {:<40} {:>6} {:>14} {:>14} {:>12}",
            "shape", "width", "heap bytes", "field bytes", "total/bucket"
        );
        for (name, width, charged) in &rows {
            let heap = charged.saturating_sub(spine(*width));
            let field = spine(*width);
            println!(
                "  {:<40} {:>6} {:>14} {:>14} {:>12.2}",
                name,
                width,
                heap,
                field,
                (heap + field) as f64 / buckets as f64
            );
            per_bucket.push((name, (heap + field) as f64 / buckets as f64));
        }

        let tomb_rows: Vec<(&'static str, usize, u64)> = vec![
            ("tombstone index: the full enum (before)", size_of::<MirrorTiered>(), clone_alloc_bytes(&tomb_tiered)),
            ("tombstone index: absence costs a pointer (now)", size_of::<DeletedObjectIndex>(), declared_tomb_bytes),
        ];
        for (name, width, charged) in &tomb_rows {
            let heap = charged.saturating_sub(spine(*width));
            let field = spine(*width);
            println!(
                "  {:<40} {:>6} {:>14} {:>14} {:>12.2}",
                name,
                width,
                heap,
                field,
                (heap + field) as f64 / buckets as f64
            );
            per_bucket.push((name, (heap + field) as f64 / buckets as f64));
        }

        // --- THE SAVING, AND THE SHARE THE TOMBSTONE TRADE TURNS ON. ---
        // What the probe charges for a clone of one of these vectors is ALREADY the field
        // bytes plus the heap: the clone reallocates the vector's own spine, which is exactly
        // `width x buckets`. Adding the spine on top would count the field bytes twice -- at the
        // OLD width in the before arm and the NEW width in the after arm, which inflates the
        // saving by exactly the difference being claimed.
        //
        // THE CONTROL FOR THAT, and it is available for free: in the before arm the tombstone
        // side holds nothing at all on the heap, every bucket being in the arm that allocates
        // nothing, so what the probe charges for it must be the spine EXACTLY. If it is not,
        // the charge is not what this arithmetic assumes and both totals are wrong.
        let tomb_before_charge = clone_alloc_bytes(&tomb_tiered);
        assert_eq!(
            spine(size_of::<MirrorTiered>()),
            tomb_before_charge,
            "a vector of {buckets} indexes that allocate nothing was charged \
             {tomb_before_charge} B against a spine of {} B; the probe is charging for \
             something other than the clone and the totals below are not what they say",
            spine(size_of::<MirrorTiered>())
        );

        let before = clone_alloc_bytes(&tiered) + tomb_before_charge;
        let now = declared_live_bytes + declared_tomb_bytes;
        assert!(
            now < before,
            "the object side charges {now} B where the shape before charged {before} B; the \
             change this module documents is not a saving on this distribution"
        );
        let carrying = tombstone_rows.iter().filter(|ids| !ids.is_empty()).count();
        println!(
            "  OBJECT SIDE: {before} B before, {now} B now -- saved {} B ({:.2} MiB), \
             {:.2} B/bucket",
            before - now,
            (before - now) as f64 / (1024.0 * 1024.0),
            (before - now) as f64 / buckets as f64
        );
        println!(
            "  buckets carrying a tombstone: {carrying} of {buckets} ({:.2}%) -- the pointer \
             shape wins below about a quarter and loses above it",
            100.0 * carrying as f64 / buckets as f64
        );
        assert!(
            carrying * 4 < buckets,
            "{carrying} of {buckets} buckets carry a tombstone; above about a quarter the \
             nullable-pointer shape costs more than the enum it replaced and the decision \
             recorded here is stale"
        );

        // --- THE NEXT DOMINANT TERM ON THIS STRUCTURE, PRICED RATHER THAN NAMED. ---
        //
        // With the object side at 24 bytes of field, the page index is 104 of the node's 176 --
        // 56.5% -- and it is the same question one level along: an inline arm for the bucket
        // that holds one page, and a MAP for the bucket that holds several. The object side's
        // answer was that the multi-entry arm is not rare and a tree charges a node sized for
        // eleven slots to hold two. The arm census and the allocator reading below are what the
        // next change should start from, and they are measured here rather than assumed.
        let page_charge = clone_alloc_bytes(&page_indexes);
        let page_spine = spine(size_of::<BlockIndexMap>());
        let page_heap = page_charge.saturating_sub(page_spine);
        let page_multi = page_arms[2];
        assert_eq!(
            buckets,
            page_arms.iter().sum::<usize>(),
            "the page arm census counted {} buckets against {buckets}",
            page_arms.iter().sum::<usize>()
        );
        println!(
            "  NEXT TERM -- the page index: {} B of field x {buckets} = {page_spine} B, plus \
             {page_heap} B on the heap ({:.2} B/bucket), {:.1}% of the node's width",
            size_of::<BlockIndexMap>(),
            page_heap as f64 / buckets as f64,
            100.0 * size_of::<BlockIndexMap>() as f64 / size_of::<BucketNode>() as f64
        );
        println!(
            "    its arms: {} buckets hold no page, {} hold exactly one (inline), {page_multi} \
             hold several (a map) -- {:.2}% on the map arm",
            page_arms[0],
            page_arms[1],
            100.0 * page_multi as f64 / buckets as f64
        );
        if page_multi > 0 {
            println!(
                "    the map arm alone charges {:.1} B a bucket that is on it",
                page_heap as f64 / page_multi as f64
            );
        }

        std::hint::black_box((&tiered, &sorted, &always, &inline, &tomb_tiered, &tomb_absent));
        std::hint::black_box((&declared_live, &declared_tomb, &page_indexes));
    }

    assert_eq!(2, path_lengths.len(), "both arms must have run");
    assert_eq!(
        path_lengths[0], path_lengths[1],
        "the store path length moved between arms ({} then {}); allocation bytes move with it",
        path_lengths[0], path_lengths[1]
    );

    println!("\n=== flatness across the two corpora ===");
    let half = per_bucket.len() / 2;
    for index in 0..half {
        let (name, small) = per_bucket[index];
        let (name_big, big) = per_bucket[index + half];
        assert_eq!(name, name_big, "the two arms reported shapes in different orders");
        let ratio = if small > 0.0 { big / small } else { 1.0 };
        println!("  {name:<44} {small:>9.2} -> {big:>9.2} B/bucket  ({ratio:.3}x)");
        assert!(
            ratio < 1.25,
            "{name} costs {small:.2} B/bucket at the small corpus and {big:.2} at the large \
             ({ratio:.3}x) -- it grows with the corpus, which is a finding and not a budget"
        );
    }
}

/// THE READ PATH, PRICED. A representation that makes the common lookup slower to make the
/// structure smaller is declined here, with the number that declines it.
///
/// `contains` is the read this structure exists for: the object fold asks it once per page, and
/// the delete path asks it once per key. The sweep below asks it over the WHOLE measured
/// distribution rather than over one object, because a microbench on a single index measures the
/// wrong regime -- a bucket holding one id and a bucket holding eight take different paths and
/// the distribution is nearly half and half.
///
/// ABBA ORDER, not interleaving. The two arms run A, B, B, A and each arm's two readings are
/// summed, so a warm-up or a drift that favours whichever ran first cancels instead of being
/// attributed to a representation.
#[test]
#[ignore = "sweeps the whole distribution four times; run by name"]
fn the_read_path_of_each_object_index_shape_is_priced() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = budget_engine(dir.path());
    objside_seed(&engine, 2_000, true);

    let live_rows = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard is loaded");
        object_id_rows(shard).0
    };
    let buckets = live_rows.len();
    assert!(buckets > 0, "denominator: no buckets to sweep");
    let multi = live_rows.iter().filter(|ids| ids.len() >= 2).count();
    assert!(
        multi > 0,
        "the sweep reached no multi-object bucket, so it prices only the arm the two shapes share"
    );

    let tiered: Vec<MirrorTiered> = live_rows.iter().map(|ids| mirror_tiered(ids)).collect();
    // The AFTER arm is the declaration itself, not a mirror of it: a read-path comparison that
    // timed a mirror would be timing something the engine does not run.
    let sorted: Vec<ObjectIndex> = live_rows
        .iter()
        .map(|ids| ids.iter().copied().collect::<ObjectIndex>())
        .collect();

    // Every id that is present, plus one that is not, per bucket. The absent probe is what makes
    // this a search rather than a hit on the first element.
    let probes: Vec<Vec<u64>> = live_rows
        .iter()
        .map(|ids| {
            let mut probe = ids.clone();
            probe.push(0xFFFF_FFFF_FFFF_FFFF);
            probe
        })
        .collect();

    let sweep_tiered = || {
        let mut hits = 0usize;
        for (index, probe) in probes.iter().enumerate() {
            for id in probe {
                let found = match &tiered[index] {
                    MirrorTiered::Empty => false,
                    MirrorTiered::One(held) => held == id,
                    MirrorTiered::Many(set) => set.contains(id),
                };
                hits += usize::from(found);
            }
        }
        hits
    };
    let sweep_sorted = || {
        let mut hits = 0usize;
        for (index, probe) in probes.iter().enumerate() {
            for id in probe {
                hits += usize::from(sorted[index].contains(id));
            }
        }
        hits
    };

    const ROUNDS: usize = 40;
    let mut tree_ns = 0u128;
    let mut run_ns = 0u128;
    let mut tree_hits = 0usize;
    let mut run_hits = 0usize;

    // A, B, B, A -- repeated, so an order effect cancels rather than being attributed.
    for _ in 0..ROUNDS {
        let t0 = std::time::Instant::now();
        tree_hits += sweep_tiered();
        tree_ns += t0.elapsed().as_nanos();

        let t1 = std::time::Instant::now();
        run_hits += sweep_sorted();
        run_ns += t1.elapsed().as_nanos();

        let t2 = std::time::Instant::now();
        run_hits += sweep_sorted();
        run_ns += t2.elapsed().as_nanos();

        let t3 = std::time::Instant::now();
        tree_hits += sweep_tiered();
        tree_ns += t3.elapsed().as_nanos();
    }

    // --- The two arms must agree on the ANSWER, or the faster one is faster at being wrong. ---
    assert_eq!(
        tree_hits, run_hits,
        "the two representations answered {tree_hits} and {run_hits} hits over the same probes; \
         a timing comparison between shapes that disagree is meaningless"
    );
    let probe_count: usize = probes.iter().map(|p| p.len()).sum();
    assert!(
        tree_hits > 0,
        "the sweep found nothing; a `contains` that never hits prices only the miss path"
    );
    assert!(
        tree_hits < probe_count * 2 * ROUNDS,
        "the sweep hit on every probe including the absent one, so the miss path was never taken"
    );

    let lookups = (probe_count * 2 * ROUNDS) as f64;
    println!(
        "{buckets} buckets ({multi} multi-object), {probe_count} probes, {ROUNDS} ABBA rounds"
    );
    println!(
        "  tree in the rare arm : {:>12} ns total, {:>7.2} ns/lookup",
        tree_ns,
        tree_ns as f64 / lookups
    );
    println!(
        "  sorted run in it     : {:>12} ns total, {:>7.2} ns/lookup",
        run_ns,
        run_ns as f64 / lookups
    );
    println!(
        "  the run is {:.3}x the tree's time per lookup",
        run_ns as f64 / tree_ns as f64
    );
}

// -------------------------------------------------------------------------------------------
// DIRECTION DECIDES THE TEST.
//
// An object index that LOSES an entry is silent data loss: the object is still in the store, the
// bucket still routes to it, and nothing finds it -- `object_manager`'s fold stops reporting it
// and the delete path stops retiring its pages. An index that KEEPS too much is merely fat. So
// every assertion below is the strong form: the FULL SET of entries, element by element, against
// a control built with the container the arm used to be, in both the single and the multi case,
// and with the transition driven in both directions -- a bucket that grows from one object to
// many, and one that shrinks back.
// -------------------------------------------------------------------------------------------

/// The control: what a search-tree set makes of the same operations.
///
/// It is the container this arm held before, so a disagreement between the two is exactly the
/// regression this change could cause, and the comparison is against something that was already
/// correct rather than against a second copy of the code under test.
fn control_set(ids: &[u64]) -> BTreeSet<u64> {
    ids.iter().copied().collect()
}

fn entries(index: &ObjectIndex) -> Vec<u64> {
    index.iter().copied().collect()
}

/// THE SORTED RUN IS SORTED AND DEDUPLICATED AFTER EVERY MUTATION.
///
/// Those two are not decoration: the stored spelling is the iteration order, so a run that lost
/// its order would move the bytes on disk, and `contains` answers by bisection, so a run that
/// lost its order would start answering "absent" for ids it holds. A duplicate would inflate
/// `len`, which is what `classify_bucket_layout` and the object count both read.
#[test]
fn the_sorted_run_stays_sorted_and_deduplicated_through_every_mutation() {
    // Inserted in an order chosen so an implementation that appends rather than placing would
    // pass `len` and fail here: every id but the first belongs BEFORE something already held.
    let inserted = [900u64, 5, 700, 1, 800, 0, u64::MAX, 400];
    let mut index = ObjectIndex::default();
    let mut control = BTreeSet::new();

    for (step, id) in inserted.iter().enumerate() {
        let index_said = index.insert(*id);
        let control_said = control.insert(*id);
        assert_eq!(
            control_said, index_said,
            "step {step}: inserting {id} answered {index_said}, the control answered \
             {control_said}"
        );
        assert_eq!(
            control.iter().copied().collect::<Vec<u64>>(),
            entries(&index),
            "step {step}: after inserting {id} the run and the control hold different entries"
        );
        // Sorted, stated directly rather than inferred from the comparison above.
        let held = entries(&index);
        assert!(
            held.windows(2).all(|pair| pair[0] < pair[1]),
            "step {step}: the run is not strictly ascending: {held:?}"
        );
    }

    // Re-inserting every id must change nothing and must answer false.
    for id in &inserted {
        assert!(!index.insert(*id), "re-inserting {id} reported a new entry");
    }
    assert_eq!(
        control.iter().copied().collect::<Vec<u64>>(),
        entries(&index),
        "re-inserting every held id changed the set"
    );
    assert_eq!(inserted.len(), index.len(), "a duplicate reached the run");

    // Removing, in a different order, and an id that was never held.
    assert!(!index.remove(&123_456), "removing an absent id reported a removal");
    for id in [700u64, 0, u64::MAX, 900] {
        assert!(index.remove(&id), "removing held id {id} reported nothing");
        control.remove(&id);
        assert_eq!(
            control.iter().copied().collect::<Vec<u64>>(),
            entries(&index),
            "after removing {id} the run and the control disagree"
        );
    }

    // And the fixture reached both arms, or it proved one of them and not the other.
    assert!(
        matches!(index, ObjectIndex::Many(_)),
        "the fixture never built the multi-entry arm, so it cannot tell it from a constant"
    );
}

/// THE TRANSITION, DRIVEN IN BOTH DIRECTIONS.
///
/// One object becomes many and many becomes one again, and at every step the whole set is
/// compared element by element against the control. The shrink direction is the one that can lose
/// an entry silently: it replaces the run with an inline id, and an implementation that took the
/// wrong one -- the first rather than the only, or the last rather than the first -- keeps the
/// count right and the contents wrong.
#[test]
fn an_object_set_survives_growing_past_one_and_shrinking_back() {
    // Chosen so "keep the first" and "keep the last" give different answers at every shrink.
    let ids = [500u64, 100, 900, 300];

    let mut index = ObjectIndex::default();
    let mut control: BTreeSet<u64> = BTreeSet::new();

    assert!(index.is_empty(), "a fresh index must be empty");
    assert_eq!(0, index.len(), "a fresh index must hold nothing");

    // --- GROW: Empty -> One -> Many. ---
    index.insert(ids[0]);
    control.insert(ids[0]);
    assert!(
        matches!(index, ObjectIndex::One(held) if held == ids[0]),
        "one id must be held inline"
    );
    assert_eq!(control_set(&ids[..1]).into_iter().collect::<Vec<u64>>(), entries(&index));

    for (at, id) in ids.iter().enumerate().skip(1) {
        index.insert(*id);
        control.insert(*id);
        assert!(
            matches!(index, ObjectIndex::Many(_)),
            "at {at} ids the index must have taken the multi-entry arm"
        );
        assert_eq!(
            control.iter().copied().collect::<Vec<u64>>(),
            entries(&index),
            "growing to {} ids lost or gained an entry",
            at + 1
        );
        for held in &control {
            assert!(index.contains(held), "the index stopped finding {held} after growing");
        }
    }

    // --- SHRINK: Many -> One -> Empty, removing back down. ---
    for id in ids.iter().rev() {
        index.remove(id);
        control.remove(id);
        assert_eq!(
            control.iter().copied().collect::<Vec<u64>>(),
            entries(&index),
            "shrinking past {id} lost or kept the wrong entry"
        );
        for held in &control {
            assert!(index.contains(held), "the index stopped finding {held} after shrinking");
        }
        assert_eq!(control.len(), index.len(), "the count disagrees with the control");
    }

    assert!(index.is_empty(), "removing every id must leave the index empty");
    assert!(
        matches!(index, ObjectIndex::Empty),
        "an index that has been emptied must be back in the arm that costs nothing, or a bucket \
         that briefly held two objects keeps an allocation for the rest of its life"
    );

    // --- And the single case is reached on the way down, with the RIGHT id in it. ---
    //
    // Driven separately, because the loop above passes straight through it. The id left behind is
    // the one the control holds, which is what an implementation that took the wrong end of the
    // run would get wrong while keeping the count right.
    let mut two: ObjectIndex = [500u64, 100].into_iter().collect();
    assert!(two.remove(&500), "removing the larger of two must report a removal");
    assert!(
        matches!(two, ObjectIndex::One(100)),
        "after shrinking to one the remaining id must be 100, held inline; it is {two:?}"
    );
    let mut two: ObjectIndex = [500u64, 100].into_iter().collect();
    assert!(two.remove(&100), "removing the smaller of two must report a removal");
    assert!(
        matches!(two, ObjectIndex::One(500)),
        "after shrinking to one the remaining id must be 500, held inline; it is {two:?}"
    );
}

/// THE TOMBSTONE SIDE HAS EXACTLY ONE SPELLING FOR "NOTHING", AND GIVES THE ALLOCATION BACK.
///
/// The shape only pays while a bucket that carries no tombstone costs one null pointer, so a
/// bucket that carried one and had it cleared has to return to that state rather than keeping an
/// empty box for the rest of its life. Two buckets holding no tombstone must also compare equal,
/// which they cannot if "nothing" has two spellings.
#[test]
fn the_tombstone_index_gives_its_allocation_back_when_it_empties() {
    let mut tombstones = DeletedObjectIndex::default();
    assert!(tombstones.is_empty(), "a fresh tombstone index holds nothing");
    assert_eq!(0, tombstones.len());
    assert!(!tombstones.contains(&7), "a fresh index cannot contain anything");
    assert!(!tombstones.remove(&7), "removing from a fresh index reports nothing");
    assert_eq!(
        DeletedObjectIndex::default(),
        tombstones,
        "an index that has never held anything must equal a fresh one"
    );

    tombstones.extend([9u64, 4, 9]);
    assert_eq!(vec![4u64, 9], tombstones.iter().copied().collect::<Vec<u64>>());
    assert_eq!(2, tombstones.len(), "the repeated id must have collapsed");
    assert!(tombstones.contains(&9) && tombstones.contains(&4));
    assert_ne!(
        DeletedObjectIndex::default(),
        tombstones,
        "an index holding two ids must not equal an empty one"
    );

    assert!(tombstones.remove(&4));
    assert_eq!(vec![9u64], tombstones.iter().copied().collect::<Vec<u64>>());
    assert!(tombstones.remove(&9));
    assert!(tombstones.is_empty(), "removing the last id must leave it empty");
    assert_eq!(
        DeletedObjectIndex::default(),
        tombstones,
        "an index emptied by removal must compare EQUAL to a fresh one; it does not, so \
         `nothing` has two spellings and the empty state is still carrying an allocation"
    );

    // The round trip: emptied, it writes and loads as the empty sequence it always did.
    let json = serde_json::to_string(&tombstones).expect("serializes");
    assert_eq!("[]", json, "an emptied tombstone index must still spell itself []");
    let loaded: DeletedObjectIndex = serde_json::from_str("[]").expect("loads");
    assert_eq!(DeletedObjectIndex::default(), loaded, "an empty sequence must load as nothing");
    let loaded: DeletedObjectIndex = serde_json::from_str("[5,5,2]").expect("loads");
    assert_eq!(
        vec![2u64, 5],
        loaded.iter().copied().collect::<Vec<u64>>(),
        "a loaded sequence must arrive ordered and deduplicated, as the tree form did"
    );
}

/// THE STORED SPELLING OF THE OBJECT SIDE DID NOT MOVE, DRIVEN IN BOTH DIRECTIONS.
///
/// The strings below are CAPTURED BYTES, not expectations written by hand: each one is what a
/// binary built at `5f86d420f` wrote for the same fixture, read out of that binary's own output.
/// Driving it that way is the point -- the two representations are different containers holding
/// the same ids, and whether they write the same sequence is a question about two binaries and
/// not about one reading of the code.
///
///   * NEW WRITE, OLD BYTES. Every fixture serializes to exactly the string the older binary
///     wrote for it, key for key and element for element.
///   * OLD WRITE, NEW READ. Every one of those strings loads here and recovers the object sets
///     ELEMENT BY ELEMENT, compared against a control built from the container the arm used to
///     hold -- so a run that arrived out of order, short an id, or carrying a duplicate fails.
///
/// The sets span the cases that can differ: nothing, one, several, and the extremes of the id
/// range -- `0` and `u64::MAX`, which are where a representation that reserved a bit for a tag
/// would have lost an id.
#[test]
fn the_stored_spelling_of_the_object_side_did_not_move() {
    // (name, live ids, tombstone ids, the bytes `5f86d420f` wrote)
    let fixtures: Vec<(&str, Vec<u64>, Vec<u64>, &str)> = vec![
        (
            "nothing on either side",
            vec![],
            vec![],
            "{\"routing_slot\":7,\"layout\":\"MultiObject\",\"dirty\":true,\"deleted\":false,\
             \"meta_loaded\":true,\"loading\":false,\"in_memory\":true,\"ttl_ms\":5000,\
             \"dirty_generation\":3,\"last_dump_sequence\":11,\"object_index\":[],\
             \"deleted_object_index\":[],\"page_index\":{}}",
        ),
        (
            "one object, no tombstone",
            vec![42],
            vec![],
            "{\"routing_slot\":7,\"layout\":\"MultiObject\",\"dirty\":true,\"deleted\":false,\
             \"meta_loaded\":true,\"loading\":false,\"in_memory\":true,\"ttl_ms\":5000,\
             \"dirty_generation\":3,\"last_dump_sequence\":11,\"object_index\":[42],\
             \"deleted_object_index\":[],\"page_index\":{}}",
        ),
        (
            "one object, one tombstone",
            vec![42],
            vec![42],
            "{\"routing_slot\":7,\"layout\":\"MultiObject\",\"dirty\":true,\"deleted\":false,\
             \"meta_loaded\":true,\"loading\":false,\"in_memory\":true,\"ttl_ms\":5000,\
             \"dirty_generation\":3,\"last_dump_sequence\":11,\"object_index\":[42],\
             \"deleted_object_index\":[42],\"page_index\":{}}",
        ),
        (
            "several objects and several tombstones, both ends of the id range",
            vec![9, 1, u64::MAX, 0, 7],
            vec![u64::MAX, 0],
            "{\"routing_slot\":7,\"layout\":\"MultiObject\",\"dirty\":true,\"deleted\":false,\
             \"meta_loaded\":true,\"loading\":false,\"in_memory\":true,\"ttl_ms\":5000,\
             \"dirty_generation\":3,\"last_dump_sequence\":11,\
             \"object_index\":[0,1,7,9,18446744073709551615],\
             \"deleted_object_index\":[0,18446744073709551615],\"page_index\":{}}",
        ),
        (
            "several objects, no tombstone",
            vec![300, 200, 100],
            vec![],
            "{\"routing_slot\":7,\"layout\":\"MultiObject\",\"dirty\":true,\"deleted\":false,\
             \"meta_loaded\":true,\"loading\":false,\"in_memory\":true,\"ttl_ms\":5000,\
             \"dirty_generation\":3,\"last_dump_sequence\":11,\"object_index\":[100,200,300],\
             \"deleted_object_index\":[],\"page_index\":{}}",
        ),
    ];

    assert_eq!(5, fixtures.len(), "all five captured spellings must be driven");
    let mut reached_multi_live = 0usize;
    let mut reached_multi_dead = 0usize;

    for (name, live, dead, captured) in &fixtures {
        let node = BucketNode {
            routing_bucket: 7,
            layout: BucketLayoutState::MultiObject,
            flags: BucketFlags::default().with(BucketFlags::DIRTY, true).with(BucketFlags::DELETED, false).with(BucketFlags::META_LOADED, true).with(BucketFlags::LOADING, false).with(BucketFlags::IN_MEMORY, true),
            ttl_ms: BucketTtl::from_ms(Some(5_000)),
            dirty_generation: 3,
            first_dirty_wal_sequence: 41,
            first_dirty_index_log_sequence: 42,
            object_index: live.iter().copied().collect(),
            deleted_object_index: dead.iter().copied().collect(),
            block_index: BlockIndexMap::default(),
        };
        if live.len() >= 2 {
            reached_multi_live += 1;
        }
        if dead.len() >= 2 {
            reached_multi_dead += 1;
        }

        // --- NEW WRITE, against the bytes the older binary produced, MINUS ONE KEY. ---
        //
        // `last_dump_sequence` is the only difference, and it is DERIVED from the captured string
        // rather than written out again by hand: the expectation is the older binary's own bytes
        // with that one key deleted, so every other byte is still being compared against what
        // `5f86d420f` wrote. The assertion below that the deletion changed something is what stops
        // this reading as an equality against itself.
        let expected_now = captured.replace("\"last_dump_sequence\":11,", "");
        assert_ne!(
            *captured, expected_now,
            "{name}: the captured spelling does not contain the key this change removes, so the \
             expectation below is the captured string unmodified and proves nothing about the \
             removal"
        );
        let written = serde_json::to_string(&node).expect("a node serializes");
        assert_eq!(
            expected_now, written,
            "{name}: the stored spelling moved against what 5f86d420f wrote, in some way other \
             than dropping last_dump_sequence"
        );

        // --- OLD WRITE, NEW READ, element by element against the control. ---
        let loaded: BucketNode = serde_json::from_str(captured).expect("the older bytes must load");
        let control_live: Vec<u64> = control_set(live).into_iter().collect();
        let control_dead: Vec<u64> = control_set(dead).into_iter().collect();
        assert_eq!(
            control_live,
            loaded.object_index.iter().copied().collect::<Vec<u64>>(),
            "{name}: the live object set did not come back element for element"
        );
        assert_eq!(
            control_dead,
            loaded.deleted_object_index.iter().copied().collect::<Vec<u64>>(),
            "{name}: the tombstone set did not come back element for element"
        );
        assert_eq!(control_live.len(), loaded.object_index.len(), "{name}: live count moved");
        assert_eq!(
            control_dead.len(),
            loaded.deleted_object_index.len(),
            "{name}: tombstone count moved"
        );
        for id in &control_live {
            assert!(loaded.object_index.contains(id), "{name}: {id} loaded but cannot be found");
        }
        for id in &control_dead {
            assert!(
                loaded.deleted_object_index.contains(id),
                "{name}: tombstone {id} loaded but cannot be found"
            );
        }

        // And what it loaded writes back to the same bytes but for the one key, so a load is not a
        // slow rewrite of anything else.
        let round = serde_json::to_string(&loaded).expect("a loaded node re-serializes");
        assert_eq!(expected_now, round, "{name}: a load-then-write did not round-trip");
    }

    // --- NON-VACUITY: the fixtures must reach the arms this change touched. ---
    assert!(
        reached_multi_live >= 2,
        "only {reached_multi_live} fixtures hold two or more objects; the arm this change \
         rewrote would barely be driven"
    );
    assert!(
        reached_multi_dead >= 1,
        "no fixture holds two or more tombstones, so the tombstone side's boxed arm is untested"
    );

    // --- A SPELLING THAT DIFFERS MUST BE SEEN TO DIFFER. ---
    //
    // The control for the comparison above: every assertion in this test is an equality against
    // a captured string, and an equality that cannot fail proves nothing. One byte is changed in
    // a captured spelling and the same comparison must reject it.
    let (_, _, _, captured) = &fixtures[3];
    let injected = captured.replace("\"object_index\":[0,1,7,9", "\"object_index\":[1,7,9");
    assert_ne!(*captured, injected.as_str(), "the injection must change the string");
    let short: BucketNode = serde_json::from_str(&injected).expect("the injected bytes load");
    assert_ne!(
        vec![0u64, 1, 7, 9, u64::MAX],
        short.object_index.iter().copied().collect::<Vec<u64>>(),
        "a spelling with an id removed compared EQUAL to the full one; the element-by-element \
         comparison above cannot report a difference and proves nothing"
    );
    assert_eq!(4, short.object_index.len(), "the injected spelling must hold one id fewer");
}
