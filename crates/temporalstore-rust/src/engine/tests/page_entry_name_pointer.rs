// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! HOW WIDE IS THE POINTER TO A NAME -- the last unexamined term on `BlockIndex`.
//!
//! THIS IS NOT THE QUESTION THE THREE MERGED REFUTATIONS ANSWERED, and saying so is the first job
//! of this module because the next reader will otherwise close it unread.
//!
//!   * #1986 asked whether the entry needs `object_key` AT ALL and found it does: 47 production
//!     lines, twelve of them class 2 -- they need the CHARACTERS -- and class 3 empty.
//!   * #1976 / #1985 asked whether `component` could be an ordinal and found it cannot: the
//!     ordinal does not exist for `zset`, `set`, `list` or `hash`.
//!   * #1986 also priced an interning HANDLE and declined it on arithmetic: a table entry at
//!     54.02 B resident, break-even 3.38 pages an object against a mixed store's 1.98.
//!
//! All three are about WHAT THE FIELD MEANS. This module is about HOW WIDE THE POINTER TO IT IS.
//! `Arc<str>` is a FAT pointer -- a data pointer and a length, side by side -- so each of the two
//! name fields costs two words to point at one allocation. A THIN pointer keeps the characters at
//! the same address, reachable by every one of those twelve class-2 readers with no table, no
//! handle, no map and no ordinal; only the length moves, from beside the pointer into the
//! allocation it points at. Nothing any of the three refutations established is reopened.
//!
//! WHAT THIS MODULE DECIDES, as six verdicts, because four of them are DEPENDENT and reporting them
//! as parallel options would overstate every one.
//!
//! AND IT IS THE STRIDE THAT IS REPORTED, NOT THE STRUCT. `state.rs` asserts that `BlockIndexMap`
//! IS its list -- `size_of::<BlockIndexMap>() == size_of::<Vec<(u64, BlockIndex)>>()` -- so what a
//! page costs in memory is `size_of::<(u64, BlockIndex)>()`, and that re-aligns to eight whatever
//! the entry's own alignment is. Measuring the struct instead of the stride is exactly how a step
//! worth nothing reads as a win:
//!
//!                                                              entry  stride
//!   today                                                         64      72
//!   step 1a  two one-word name slots (thin pointer)               48      56   measured, DECLINED
//!   step 1b  the key moved to the bucket's object list            48      56   measured, REFUTED
//!   step 2   an eight-bit object ordinal, needs 1b                40      48   does not follow
//!   step 3   an element ordinal for the component (another tree's) 24     32   does not follow
//!   step 4   the handle folded out of the list                    24      24   does not follow
//!   step 5   `repr(packed)`, needs 3                              22      32   WORTH NOTHING
//!
//! STEP 5 IS WORTH NOTHING AND THAT IS A MEASURED CORRECTION, not a judgement. An 18-byte entry in a
//! tuple with a `u64` still strides at 32, because the tuple's alignment is the `u64`'s. Packing
//! would spend an unaligned read on every address load, and undefined behaviour on every existing
//! `&entry.address`, to save nothing at all in the list.
//!
//! STEP 1a REACHES 48 AND WINS ON EVERY ARM MEASURED: -16.00 B a page on six of the eight and
//! -16.61 and -21.59 on the two where the list arm dominates. It wins because the name allocation
//! does not grow -- `Arc<str>` already spends two header words on a strong count and a weak count,
//! and a thin string spends two on a strong count and the LENGTH, and this tree uses `Weak` nowhere.
//! That is the whole mechanism, and the measurement carries the row that proves it: a thin string
//! KEEPING a weak count is only -6.33 to +0.00 a page, so the free ride is the weak count and
//! nothing else.
//!
//! IT IS DECLINED ON SOUNDNESS, NOT ON BYTES. `Arc<str>` cannot be made thin, and the only thin
//! shape that needs no unsafe is `Arc<String>` -- one word in the entry, two allocations per name --
//! which is +17.00 to +32.00 B a page on ALL EIGHT arms. A hand-rolled one would be the first
//! self-managed heap allocation in this crate, whose entire production unsafe surface is four
//! single-expression FFI calls, with no Miri available for the pinned toolchain, no sanitizer job
//! among the five workflows, and a Rust workflow whose test step is `continue-on-error`.
//!
//! STEP 1b REACHES THE SAME 48 AND LOSES ANYWAY, on two separate grounds. On BYTES it is two
//! populations with opposite signs: -15.76 to -15.84 a page on containers, but **+8.11 to +9.10 on
//! routed keys at the shipped 0..=1023 range** and 0.00 at the wide range, because the object list
//! grows by a key for every object and a routed bucket holds forty of them. That is #1994's shape
//! exactly. And on IDENTITY it is refuted outright: with the key off the entry, an entry's only
//! handle on its object is `BlockAddress::object_id`, which is an `Option` answered as ZERO when
//! absent and a 64-bit hash when present. Step 2 needs exactly the id-to-key map 1b would have
//! created, so it falls with it; steps 3 and 5 fall behind step 2.
//!
//! STEP 4 -- the list's `u64` handle -- is the one term that needs no step above it and no format
//! change at all. `block_index_handle` is a `DefaultHasher` over the entry's own `model_id`,
//! `object_key`, `component` and five address fields, so it is a CACHED DERIVATION OF THE ENTRY
//! BESIDE THE ENTRY -- the shape `generation` (#1966) and the component name (#1978) both had when
//! they came out. Nothing persists it: the page index's stored form is a map keyed by
//! `block_index_written_key`'s string, and `CoreIndex::object_block_lookup`, which holds the handle
//! in `BlockLookupRef`, is `#[serde(default, skip_serializing)]` and rebuilt on load. So there is no
//! stored shape to move.
//!
//! IT IS DECLINED ON TWO NUMBERS, both measured rather than argued.
//!
//!   * IT IS THE LIST ARM ONLY, and that is two populations, not one. The single-page arm holds its
//!     handle INSIDE a 24-byte enum whose width is set by the list arm either way, so taking it out
//!     of `One` reclaims nothing. MEASURED: 8.00 B a page on the six arms whose pages sit in lists,
//!     and **0.00 B a page on the two routed arms at 0..=u32::MAX, where all 40,000 buckets hold one
//!     page**. Charging it over every page would credit it with eight bytes where it is worth none --
//!     the same error #1994 caught from the other side, where a change was -324.44 B a bucket at one
//!     range and +0.70 at the other.
//!   * WHAT IT COSTS IS A HASH PER PROBE, counted and not timed. The list is sorted by the handle and
//!     `find_page` bisects it, so a recomputed key hashes `model_id`, the key, the component and five
//!     address fields at every probe of every warm page lookup, against zero today. MEASURED probe
//!     depth: 6 to 7 at the median bucket and up to 9 at the widest, which is **48 to 72 field hashes
//!     a warm page lookup**, and one of the eight is a 24-character string. The ORDER is safe -- the
//!     same function of the same fields gives the same order, so #1964's five readers taking the
//!     ascending walk as given keep it and no explicit sort appears -- and nothing persists the
//!     handle, so there is no stored shape to move. It is the probe count that declines it.
//!
//! SO 64 STANDS, and the reason is written down here rather than left to be re-derived.
//!
//! THE HONEST CEILING. The design that prompted this work holds a 17-byte entry with NO key and NO
//! component: names resolve to small integer ids at its API boundary. Three merged refutations
//! establish our readers need the characters somewhere, so 17 is not reachable without that object
//! model. Our own floor, with every step above taken and the alignment given up, is 22 -- and the
//! 18-byte figure quoted for it omits `model_id` and the three flag bytes, which are 4 of our 22.
#![allow(clippy::all)]
use super::*;
use std::collections::{BTreeMap, BTreeSet};
use std::mem::{align_of, offset_of, size_of};
use std::ptr::NonNull;
use std::sync::Arc;

use crate::alloc_probe::documented_glibc_chunk;
use crate::block_store::BlockAddress;
use crate::engine::state::{BlockIndex, BucketNode, ObjectIndex};
use crate::engine::storage_bucket_internals::StoredModelKind;

use super::page_entry_names::{
    load_shard_over, page_fixture, probe_engine, seed_container_keys, seed_routed_keys,
};

// Imported as a NAME rather than spelled out at the call site: the counting-allocator gate in
// `alloc_probe.rs` walks back from every line quoting the probe's full path to the nearest
// #[test], and a helper spelling it out would be reported as reading the probe outside a test.
#[cfg(feature = "alloc-probe")]
use crate::alloc_probe::Probe;

// =================================================================================================
// THE INSTRUMENTS.
// =================================================================================================

/// The width of a field, inferred from the FIELD rather than named at the call site.
///
/// #1986 landed this and the reason is live in this tree: `pages_per_bucket.rs` carried a field
/// table that called `model_id` an `Arc<str>` and charged the group 32 bytes for 17, and rustc said
/// nothing, because `size_of::<Arc<str>>()` compiles whatever the field became. Every width below is
/// taken through this.
fn field_width<T>(_field: &T) -> usize {
    size_of::<T>()
}

/// The chunk glibc is documented to serve a request from, checked as a FLOOR rather than trusted as
/// an equality.
///
/// #1969 read a 104-byte request served out of 128. The rule is `max(32, round_up(request + 8, 16))`
/// and `alloc_probe::documented_glibc_chunk` is this tree's statement of it; this wrapper asserts the
/// three properties that make it a floor at every call, so a platform whose rule differs fails here
/// rather than inside a byte total.
fn chunk(request: usize) -> usize {
    let served = documented_glibc_chunk(request);
    assert!(
        served >= request,
        "the chunk rule served {served} B for a {request} B request, which is less than was asked for"
    );
    assert!(
        served > request,
        "the chunk rule served exactly {served} B for a {request} B request; the header word means a \
         served chunk is STRICTLY larger than the request, and an equality here means this is not the \
         rule being modelled"
    );
    assert_eq!(
        0,
        served % 16,
        "the chunk rule served {served} B, which is not a multiple of sixteen"
    );
    served
}

/// A histogram's quantile, read off the counts rather than a materialised list.
fn quantile(hist: &BTreeMap<usize, usize>, q: f64) -> usize {
    let total: usize = hist.values().copied().sum();
    if total == 0 {
        return 0;
    }
    let target = ((total as f64) * q).ceil().max(1.0) as usize;
    let mut seen = 0usize;
    for (value, count) in hist {
        seen += count;
        if seen >= target {
            return *value;
        }
    }
    hist.keys().copied().next_back().unwrap_or_default()
}

fn widest(hist: &BTreeMap<usize, usize>) -> usize {
    hist.keys().copied().next_back().unwrap_or_default()
}

fn samples(hist: &BTreeMap<usize, usize>) -> usize {
    hist.values().copied().sum()
}

// =================================================================================================
// THE MIRRORS. Declared shapes this engine does not have, so a width can be quoted without
// asserting anything about a structure that does not exist.
// =================================================================================================

/// The page entry with its two name fields held as ONE WORD each.
///
/// A `NonNull<u8>` is exactly what a thin pointer is -- one word, no length beside it -- and
/// `Option<NonNull<u8>>` is still one word because the null value is the niche. Both are asserted
/// rather than assumed: if `Option<NonNull<u8>>` were two words this mirror would be measuring a
/// different change.
#[allow(dead_code)]
struct MirrorEntryThinNames {
    object_key: NonNull<u8>,
    model_id: StoredModelKind,
    component: Option<NonNull<u8>>,
    address: BlockAddress,
    dirty: bool,
    deleted: bool,
    log_backed: bool,
}

/// The page entry with `object_key` GONE -- step 1b, the key held on the bucket's object list.
#[allow(dead_code)]
struct MirrorEntryNoKey {
    model_id: StoredModelKind,
    component: Option<Arc<str>>,
    address: BlockAddress,
    dirty: bool,
    deleted: bool,
    log_backed: bool,
}

/// The object list carrying `(id, key)` instead of a bare id, as step 1b needs.
#[allow(dead_code)]
enum MirrorObjectIndexWithKeys {
    Empty,
    One(u64, Arc<str>),
    Many(Box<Vec<(u64, Arc<str>)>>),
}

/// The same, with the key as a thin pointer -- the cheaper of the two ways to carry it.
#[allow(dead_code)]
enum MirrorObjectIndexWithThinKeys {
    Empty,
    One(u64, NonNull<u8>),
    Many(Box<Vec<(u64, NonNull<u8>)>>),
}

/// The address with an EIGHT-BIT object ordinal in place of the 64-bit hash -- step 2.
///
/// `Copy` on purpose: inside a packed entry a field may not be borrowed, so every read has to take a
/// copy, and `a_packed_entry_reads_its_address_by_value_because_a_borrow_of_it_is_undefined` drives
/// that discipline rather than describing it.
#[allow(dead_code)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct MirrorAddressEightBitObject {
    address_word: u64,
    length: u32,
    block_id: u16,
    present: u8,
    object_ordinal: u8,
}

/// The same with a SIXTEEN-bit ordinal, which #1994 measured as worth nothing. Mirrored so the claim
/// is reproduced from declarations here rather than quoted.
#[allow(dead_code)]
struct MirrorAddressSixteenBitObject {
    address_word: u64,
    length: u32,
    block_id: u16,
    present: u8,
    object_ordinal: u16,
}

/// Step 1b + step 2: no key, and an eight-bit ordinal inside the address.
#[allow(dead_code)]
struct MirrorEntryOrdinalObject {
    model_id: StoredModelKind,
    component: Option<Arc<str>>,
    address: MirrorAddressEightBitObject,
    dirty: bool,
    deleted: bool,
    log_backed: bool,
}

/// Step 1b + 2 with a SIXTEEN-bit ordinal, to show the step does not land at that width.
#[allow(dead_code)]
struct MirrorEntryWideOrdinalObject {
    model_id: StoredModelKind,
    component: Option<Arc<str>>,
    address: MirrorAddressSixteenBitObject,
    dirty: bool,
    deleted: bool,
    log_backed: bool,
}

/// Step 3: the component as an element ordinal too. Another tree's step, not this one's -- mirrored
/// only so the combined width can be stated.
#[allow(dead_code)]
struct MirrorEntryBothOrdinals {
    model_id: StoredModelKind,
    component: u16,
    address: MirrorAddressEightBitObject,
    dirty: bool,
    deleted: bool,
    log_backed: bool,
}

/// Step 5: the same shape with the alignment given up.
#[allow(dead_code)]
#[repr(packed)]
struct MirrorEntryBothOrdinalsPacked {
    model_id: StoredModelKind,
    component: u16,
    address: MirrorAddressEightBitObject,
    dirty: bool,
    deleted: bool,
    log_backed: bool,
}

/// Step 5 with the three flag bytes folded into one, which is the only remaining field difference
/// between our packed floor and theirs apart from the block id and the address presence byte.
///
/// `pages_per_bucket::MirrorPageOneFlagByte` prices the same fold on the unpacked entry and finds it
/// worth nothing there, because the three bytes already sit inside alignment slack. Packed there is
/// no slack, so the fold is worth exactly its two bytes -- which is the whole reason it appears here
/// and nowhere above.
#[allow(dead_code)]
#[repr(packed)]
struct MirrorEntryBothOrdinalsPackedOneFlagByte {
    model_id: StoredModelKind,
    component: u16,
    address: MirrorAddressEightBitObject,
    flags: u8,
}

// =================================================================================================
// 1. THE RECONSTRUCTION. Does a one-word name slot reach 48?
// =================================================================================================

/// THE ENTRY IS 64 WITH 60 BYTES OF FIELD, AND TWO ONE-WORD NAME SLOTS TAKE IT TO 48.
///
/// A RECONSTRUCTION AND NOT A LITERAL, on both sides. `60 - 16 = 44` rounds to 48 on paper and paper
/// is not what is asserted: the live entry is reconstructed from `offset_of!` with every width read
/// off the field itself, and the projected entry is a DECLARED mirror read the same way. A literal 48
/// would still pass if `Option<NonNull<u8>>` stopped riding its niche, and that is precisely how this
/// arithmetic can be wrong.
///
/// ORDER-INDEPENDENT, which #1986 had to be corrected to be: `repr(Rust)` places fields by alignment
/// and not by declaration, and `object_key` sits at offset 48 despite being declared first. Nothing
/// but `offset_of!` says so.
///
/// AND THE MIRROR IS THE ENTRY WITH TWO SLOTS SWAPPED, not a different structure. Every field the
/// change does not touch is asserted to have the same width on both sides, so a mirror that had
/// drifted from the declaration fails here rather than flattering the projection.
///
/// rust-internal: measures declarations, no product behaviour
#[test]
fn two_one_word_name_slots_take_the_page_entry_from_sixty_four_to_forty_eight() {
    // --- THE CONTROL FIRST. A one-word `Option` is the whole reason 44 is reachable. ---
    assert_eq!(
        size_of::<usize>(),
        size_of::<NonNull<u8>>(),
        "a thin pointer is supposed to be one word; `NonNull<u8>` is {} B",
        size_of::<NonNull<u8>>()
    );
    assert_eq!(
        size_of::<usize>(),
        size_of::<Option<NonNull<u8>>>(),
        "`Option<NonNull<u8>>` is {} B, so the null niche is not being used and the projected entry \
         below would be measuring a two-word optional slot rather than a thin pointer",
        size_of::<Option<NonNull<u8>>>()
    );

    // --- THE LIVE ENTRY, FIELD BY FIELD AT ITS REAL OFFSET. ---
    let live = page_fixture(Some("f0"), 3, (true, false, true));
    let live_fields = [
        (
            "object_key",
            field_width(&live.object_key),
            offset_of!(BlockIndex, object_key),
        ),
        (
            "model_id",
            field_width(&live.model_id),
            offset_of!(BlockIndex, model_id),
        ),
        (
            "component",
            field_width(&live.component),
            offset_of!(BlockIndex, component),
        ),
        (
            "address",
            field_width(&live.address),
            offset_of!(BlockIndex, address),
        ),
        ("dirty", field_width(&live.dirty), offset_of!(BlockIndex, dirty)),
        (
            "deleted",
            field_width(&live.deleted),
            offset_of!(BlockIndex, deleted),
        ),
        (
            "log_backed",
            field_width(&live.log_backed),
            offset_of!(BlockIndex, log_backed),
        ),
    ];

    // --- THE PROJECTED ENTRY, THE SAME WAY, OFF THE MIRROR'S OWN FIELDS. ---
    let projected = MirrorEntryThinNames {
        object_key: NonNull::dangling(),
        model_id: live.model_id,
        component: Some(NonNull::dangling()),
        address: live.address.clone(),
        dirty: true,
        deleted: false,
        log_backed: true,
    };
    let projected_fields = [
        (
            "object_key",
            field_width(&projected.object_key),
            offset_of!(MirrorEntryThinNames, object_key),
        ),
        (
            "model_id",
            field_width(&projected.model_id),
            offset_of!(MirrorEntryThinNames, model_id),
        ),
        (
            "component",
            field_width(&projected.component),
            offset_of!(MirrorEntryThinNames, component),
        ),
        (
            "address",
            field_width(&projected.address),
            offset_of!(MirrorEntryThinNames, address),
        ),
        (
            "dirty",
            field_width(&projected.dirty),
            offset_of!(MirrorEntryThinNames, dirty),
        ),
        (
            "deleted",
            field_width(&projected.deleted),
            offset_of!(MirrorEntryThinNames, deleted),
        ),
        (
            "log_backed",
            field_width(&projected.log_backed),
            offset_of!(MirrorEntryThinNames, log_backed),
        ),
    ];

    for (label, width, fields) in [
        ("the page entry as declared", size_of::<BlockIndex>(), &live_fields),
        (
            "with two one-word name slots",
            size_of::<MirrorEntryThinNames>(),
            &projected_fields,
        ),
    ] {
        println!("\n=== {label}: {width} B ===");
        for (name, field, offset) in fields.iter() {
            println!("  {name:<12} {field:>3} B at offset {offset:>3}");
        }
        let sum: usize = fields.iter().map(|(_, field, _)| field).sum();
        println!("  {width} B = {sum} B of field + {} B padding", width - sum);

        // NO FIELD OVERLAPS OR OVERRUNS, which is what makes a sum a reconstruction.
        for (name, field, offset) in fields.iter() {
            assert!(
                offset + field <= width,
                "{label}: {name} occupies {offset}..{} of a {width} B structure",
                offset + field
            );
            for (other, other_field, other_offset) in fields.iter() {
                if name == other {
                    continue;
                }
                assert!(
                    offset + field <= *other_offset || other_offset + other_field <= *offset,
                    "{label}: {name} at {offset}..{} overlaps {other} at {other_offset}..{}; the \
                     field sum is counting one byte twice",
                    offset + field,
                    other_offset + other_field
                );
            }
        }
    }

    let live_sum: usize = live_fields.iter().map(|(_, width, _)| width).sum();
    let projected_sum: usize = projected_fields.iter().map(|(_, width, _)| width).sum();

    // --- THE MIRROR IS THE ENTRY WITH TWO SLOTS SWAPPED. ---
    let untouched = ["model_id", "address", "dirty", "deleted", "log_backed"];
    let mut compared = 0usize;
    for name in untouched {
        let here = live_fields
            .iter()
            .find(|(field, _, _)| *field == name)
            .expect("the live entry declares this field");
        let there = projected_fields
            .iter()
            .find(|(field, _, _)| *field == name)
            .expect("the mirror declares this field");
        assert_eq!(
            here.1, there.1,
            "{name} is {} B on the entry and {} B on the mirror; the mirror is supposed to differ \
             from the declaration in the two NAME slots and nothing else",
            here.1, there.1
        );
        compared += 1;
    }
    assert_eq!(
        untouched.len(),
        compared,
        "{compared} of {} untouched fields were compared",
        untouched.len()
    );
    assert_eq!(
        align_of::<BlockIndex>(),
        align_of::<MirrorEntryThinNames>(),
        "the mirror is {}-aligned against the entry's {}, so its rounding is not the entry's",
        align_of::<MirrorEntryThinNames>(),
        align_of::<BlockIndex>()
    );

    // --- AND BOTH NAMES ARE FAT POINTERS TODAY, which is what the step is about. ---
    assert_eq!(
        2 * size_of::<usize>(),
        field_width(&live.object_key),
        "`object_key` is {} B. Every figure in this module is about that field being a data pointer \
         and a length side by side",
        field_width(&live.object_key)
    );
    assert_eq!(
        2 * size_of::<usize>(),
        field_width(&live.component),
        "`component` is {} B, not the two words of a fat optional pointer",
        field_width(&live.component)
    );

    // --- THE STEP. ---
    println!(
        "\n=== the step ===\n  {} B of field in {} B  ->  {} B of field in {} B   ({:+} B a page)",
        live_sum,
        size_of::<BlockIndex>(),
        projected_sum,
        size_of::<MirrorEntryThinNames>(),
        size_of::<MirrorEntryThinNames>() as isize - size_of::<BlockIndex>() as isize
    );
    assert_eq!(
        live_sum - 2 * size_of::<usize>(),
        projected_sum,
        "swapping two fat pointers for two thin ones is supposed to take {live_sum} B of field to \
         {}; the mirror holds {projected_sum}",
        live_sum - 2 * size_of::<usize>()
    );
    assert_eq!(
        48,
        size_of::<MirrorEntryThinNames>(),
        "two one-word name slots land the entry at {} B, not 48. The whole premise of this module is \
         that {projected_sum} B of field rounds to 48",
        size_of::<MirrorEntryThinNames>()
    );
    assert_eq!(
        16,
        size_of::<BlockIndex>() - size_of::<MirrorEntryThinNames>(),
        "the step is {} B, not the 16 the rest of this module prices",
        size_of::<BlockIndex>() - size_of::<MirrorEntryThinNames>()
    );

    // --- AND THE CHUNK CLASS MOVES WITH IT, which is the part a struct width cannot say. Since
    //     #1975 the single-page arm holds the entry behind a BOX, so what a bucket pays for one page
    //     is the CHUNK and not the width -- and a sibling found 64 + 8 and 72 + 8 landing in the
    //     same 80-byte class, so a width step can round away entirely. This one does not. ---
    let chunk_now = chunk(size_of::<BlockIndex>());
    let chunk_thin = chunk(size_of::<MirrorEntryThinNames>());
    println!(
        "  boxed single page: chunk {chunk_now} B -> {chunk_thin} B ({:+} B)",
        chunk_thin as isize - chunk_now as isize
    );
    assert!(
        chunk_thin < chunk_now,
        "the entry narrows from {} B to {} B and the allocator serves both out of a {chunk_now} B \
         chunk, so the step rounds away for every single-page bucket",
        size_of::<BlockIndex>(),
        size_of::<MirrorEntryThinNames>()
    );
}

// =================================================================================================
// 2. THE HEAP. What a one-word name slot is worth on the chunk column, at both populations.
// =================================================================================================

/// One store's page and name census, by POINTER wherever sharing is the question.
#[derive(Debug, Default)]
struct PageCensus {
    buckets: usize,
    pages: usize,
    objects: usize,
    /// Buckets keyed by pages held.
    pages_per_bucket: BTreeMap<usize, usize>,
    /// Objects keyed by pages held -- the population that decides everything here.
    pages_per_object: BTreeMap<usize, usize>,
    /// Buckets keyed by distinct objects held.
    objects_per_bucket: BTreeMap<usize, usize>,
    /// Lengths of the DISTINCT object-key allocations, one entry per allocation.
    object_key_lengths: Vec<usize>,
    /// Lengths of the DISTINCT component allocations, one entry per allocation.
    component_lengths: Vec<usize>,
    /// Pages whose component is `None`, so the optional slot's population is visible.
    pages_without_component: usize,
    single_page_buckets: usize,
    multi_page_buckets: usize,
}

fn page_census(engine: &TemporalEngine) -> PageCensus {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    let mut census = PageCensus::default();
    // Distinct allocations across the WHOLE store, by address, with the length each holds. Counted
    // by pointer because contents cannot tell one shared allocation from two equal ones.
    let mut key_allocations: BTreeMap<usize, usize> = BTreeMap::new();
    let mut component_allocations: BTreeMap<usize, usize> = BTreeMap::new();
    let mut pages_by_object: BTreeMap<(StoredModelKind, String), usize> = BTreeMap::new();

    for bucket in shard.bucket_index.bucket_map.values() {
        let mut held = 0usize;
        let mut objects_here: BTreeSet<String> = BTreeSet::new();
        for (_, page) in bucket.block_index.iter() {
            held += 1;
            key_allocations.insert(page.object_key.as_ptr() as usize, page.object_key.len());
            match page.component.as_deref() {
                Some(name) => {
                    component_allocations.insert(name.as_ptr() as usize, name.len());
                }
                None => census.pages_without_component += 1,
            }
            objects_here.insert(page.object_key.to_string());
            *pages_by_object
                .entry((page.model_id, page.object_key.to_string()))
                .or_default() += 1;
        }
        census.buckets += 1;
        census.pages += held;
        *census.pages_per_bucket.entry(held).or_default() += 1;
        *census.objects_per_bucket.entry(objects_here.len()).or_default() += 1;
        match held {
            0 => {}
            1 => census.single_page_buckets += 1,
            _ => census.multi_page_buckets += 1,
        }
    }
    census.objects = pages_by_object.len();
    for held in pages_by_object.values() {
        *census.pages_per_object.entry(*held).or_default() += 1;
    }
    census.object_key_lengths = key_allocations.values().copied().collect();
    census.component_lengths = component_allocations.values().copied().collect();
    census
}

/// What one store charges for its page entries and its name allocations, under one counterfactual.
#[derive(Debug, Default, Clone, Copy)]
struct HeapBytes {
    entries: usize,
    names: usize,
}

impl HeapBytes {
    fn total(&self) -> usize {
        self.entries + self.names
    }
}

/// The heap the page index occupies: one chunk per single-page bucket's BOX, one per multi-page
/// bucket's LIST.
///
/// THE LIST IS CHARGED AT ITS CAPACITY AND NOT ITS LENGTH, because this tree's growth is knowable:
/// `reserve_one_more` calls `reserve_exact` in steps of `PAGE_LIST_GROWTH_STEP`, so a list of `n`
/// holds `n.div_ceil(STEP) * STEP` slots. Charging at the length would understate every row by up to
/// three entries a bucket, which at the shipped range is up to three entries in thirty-nine.
///
/// STILL A FLOOR. `Vec::reserve_exact` guarantees at least what was asked for and may serve more, and
/// the chunk rule is itself a floor (#1969). Every counterfactual is charged identically, so the
/// floor flatters none of them against another.
fn page_index_heap(census: &PageCensus, entry_width: usize, element_width: usize) -> usize {
    let step = crate::engine::state::PAGE_LIST_GROWTH_STEP;
    let mut bytes = 0usize;
    for (held, buckets) in &census.pages_per_bucket {
        let per_bucket = match held {
            0 => 0,
            1 => chunk(entry_width),
            n => chunk(n.div_ceil(step) * step * element_width),
        };
        bytes += per_bucket * buckets;
    }
    bytes
}

/// What the name allocations cost, one chunk per DISTINCT allocation, for a header of `header_words`
/// words in front of the characters.
///
/// `Arc<str>`'s allocation is a strong count, a weak count and the characters: two words. A thin
/// refcounted string needs a strong count, the LENGTH and the characters: also two. A thin string
/// that KEPT a weak count would need three, and that row is priced beside the other two below,
/// because it is the one thing that would make step 1a cost something.
fn name_heap(lengths: &[usize], header_words: usize) -> usize {
    lengths
        .iter()
        .map(|len| chunk(header_words * size_of::<usize>() + len))
        .sum()
}

/// The only THIN shape that needs no unsafe: `Arc<String>`. One word in the entry, and TWO
/// allocations per name -- the `Arc` holding a `String`, and the `String`'s own buffer.
fn arc_string_name_heap(lengths: &[usize]) -> usize {
    lengths
        .iter()
        .map(|len| {
            chunk(2 * size_of::<usize>() + size_of::<String>())
                + if *len == 0 { 0 } else { chunk(*len) }
        })
        .sum()
}

/// What the object list costs on the heap, for a run element of `element_width`.
///
/// `ObjectIndex` is `Empty | One(u64) | Many(Box<Vec<u64>>)`: a bucket holding one object pays the
/// enum and nothing else, a bucket holding several pays a chunk for the run.
fn object_list_heap(census: &PageCensus, element_width: usize) -> usize {
    let mut bytes = 0usize;
    for (objects, buckets) in &census.objects_per_bucket {
        let per_bucket = match objects {
            0 | 1 => 0,
            n => chunk(n * element_width),
        };
        bytes += per_bucket * buckets;
    }
    bytes
}

/// BOTH BYTE COLUMNS, BOTH ROUTING RANGES, TWO CORPUS SIZES, AND TWO POPULATIONS THAT ARE NEVER
/// AVERAGED.
///
/// #1986 found the mixed pages-per-object histogram holding ONLY 1 and 100, with zero objects at
/// two, so a mean of 1.98 describes neither store. Every row below is one population; its
/// percentiles and MAX are printed, its sample count is printed, its denominator is printed, and the
/// arm is ASSERTED to reach the population it claims to measure.
///
/// THE CHUNK RULE IS A FLOOR, not an equality (#1969: a 104-byte request served out of 128), and
/// `chunk` asserts that at every call.
///
/// THE STORE PATH LENGTH is held constant across all eight arms and asserted: allocation bytes move
/// with it at about six bytes a character, and a confound of exactly that shape has been published
/// from this tree once already.
#[test]
#[ignore = "seeds eight stores up to 40,000 records each; run by name"]
fn what_a_one_word_name_slot_is_worth_on_the_chunk_column() {
    let entry_now = size_of::<BlockIndex>();
    let entry_thin = size_of::<MirrorEntryThinNames>();
    let entry_no_key = size_of::<MirrorEntryNoKey>();
    let element_now = size_of::<(u64, BlockIndex)>();
    let element_thin = size_of::<(u64, MirrorEntryThinNames)>();
    let element_no_key = size_of::<(u64, MirrorEntryNoKey)>();

    println!("\n=== the widths every row below is built from ===");
    println!("  entry today            {entry_now} B, list element {element_now} B");
    println!("  entry with thin names  {entry_thin} B, list element {element_thin} B");
    println!("  entry with no key      {entry_no_key} B, list element {element_no_key} B");
    println!(
        "  object list: bare id {} B, with a shared key {} B, with a thin key {} B",
        size_of::<ObjectIndex>(),
        size_of::<MirrorObjectIndexWithKeys>(),
        size_of::<MirrorObjectIndexWithThinKeys>()
    );
    assert_eq!(48, entry_thin, "the thin mirror is not 48 B; every row below is fiction");
    assert_eq!(48, entry_no_key, "the keyless mirror is not 48 B; every row below is fiction");

    let mut path_lengths: Vec<usize> = Vec::new();
    let mut rows: Vec<(String, PageCensus)> = Vec::new();

    for (size_label, records) in [("4,000 records", 4_000usize), ("40,000 records", 40_000usize)] {
        for (range_label, end_routing_bucket) in [
            ("range 0..=1023 (shipped)", 1023u32),
            ("range 0..=u32::MAX (load_shard)", u32::MAX),
        ] {
            {
                let dir = tempfile::tempdir().expect("tempdir");
                path_lengths.push(dir.path().as_os_str().len());
                let engine = probe_engine(dir.path());
                load_shard_over(&engine, end_routing_bucket);
                seed_container_keys(&engine, records / 100, 100);
                rows.push((
                    format!("containers, {size_label}, {range_label}"),
                    page_census(&engine),
                ));
            }
            {
                let dir = tempfile::tempdir().expect("tempdir");
                path_lengths.push(dir.path().as_os_str().len());
                let engine = probe_engine(dir.path());
                load_shard_over(&engine, end_routing_bucket);
                seed_routed_keys(&engine, records);
                rows.push((
                    format!("routed keys, {size_label}, {range_label}"),
                    page_census(&engine),
                ));
            }
        }
    }

    assert_eq!(8, path_lengths.len(), "all eight arms must have run");
    for length in &path_lengths {
        assert_eq!(
            path_lengths[0], *length,
            "the store path length moved between arms: {} against {length}",
            path_lengths[0]
        );
    }
    println!(
        "\n  store path held at {} characters across all eight arms",
        path_lengths[0]
    );

    let mut container_arms = 0usize;
    let mut routed_arms = 0usize;
    let mut thin_wins = 0usize;

    for (label, census) in &rows {
        assert!(census.pages > 0, "{label}: denominator is zero, nothing was censused");
        assert!(census.objects > 0, "{label}: no objects censused");

        // --- THE TWO POPULATIONS, printed as percentiles and never as a mean. ---
        println!("\n=== {label} ===");
        println!(
            "  buckets={} pages={} objects={}  (single-page buckets={} multi-page buckets={})",
            census.buckets,
            census.pages,
            census.objects,
            census.single_page_buckets,
            census.multi_page_buckets
        );
        println!(
            "  pages per object   : p50 {} p90 {} p99 {} MAX {}   over {} objects",
            quantile(&census.pages_per_object, 0.50),
            quantile(&census.pages_per_object, 0.90),
            quantile(&census.pages_per_object, 0.99),
            widest(&census.pages_per_object),
            samples(&census.pages_per_object)
        );
        println!(
            "  objects per bucket : p50 {} p90 {} p99 {} MAX {}   over {} buckets",
            quantile(&census.objects_per_bucket, 0.50),
            quantile(&census.objects_per_bucket, 0.90),
            quantile(&census.objects_per_bucket, 0.99),
            widest(&census.objects_per_bucket),
            samples(&census.objects_per_bucket)
        );
        println!(
            "  distinct name allocations: {} object keys, {} components ({} pages carry none)",
            census.object_key_lengths.len(),
            census.component_lengths.len(),
            census.pages_without_component
        );

        // --- THE COUNTERFACTUALS, side by side, on the chunk column. ---
        let today = HeapBytes {
            entries: page_index_heap(census, entry_now, element_now),
            names: name_heap(&census.object_key_lengths, 2)
                + name_heap(&census.component_lengths, 2),
        };
        // Strong count + LENGTH + characters: two header words, the same two `Arc<str>` spends on
        // strong + weak.
        let thin = HeapBytes {
            entries: page_index_heap(census, entry_thin, element_thin),
            names: name_heap(&census.object_key_lengths, 2)
                + name_heap(&census.component_lengths, 2),
        };
        // The same, but KEEPING a weak count: strong + weak + length. The row that shows what the
        // free ride actually depends on.
        let thin_with_weak = HeapBytes {
            entries: page_index_heap(census, entry_thin, element_thin),
            names: name_heap(&census.object_key_lengths, 3)
                + name_heap(&census.component_lengths, 3),
        };
        // The safe shape: `Arc<String>`, one word in the entry and two allocations per name.
        let arc_string = HeapBytes {
            entries: page_index_heap(census, entry_thin, element_thin),
            names: arc_string_name_heap(&census.object_key_lengths)
                + arc_string_name_heap(&census.component_lengths),
        };
        // Step 1b: the key on the object list. The entry loses it; the list grows to carry it, on
        // the heap for a bucket holding several objects and in the NODE for every bucket.
        let object_list_now = object_list_heap(census, size_of::<u64>());
        let object_list_with_keys = object_list_heap(census, size_of::<(u64, Arc<str>)>());
        let node_delta = (size_of::<MirrorObjectIndexWithKeys>() - size_of::<ObjectIndex>())
            * census.buckets;
        let on_object_list = HeapBytes {
            entries: page_index_heap(census, entry_no_key, element_no_key)
                + object_list_with_keys
                + node_delta,
            names: name_heap(&census.object_key_lengths, 2)
                + name_heap(&census.component_lengths, 2),
        };

        let denominator = census.pages as f64;
        println!(
            "  {:<46} {:>13} {:>13} {:>13} {:>12}",
            "counterfactual", "entry bytes", "name bytes", "total", "B a page"
        );
        let counterfactuals = [
            ("today: two fat pointers", today),
            ("1a thin pointer, no weak count (unsafe)", thin),
            ("1a thin pointer, weak count kept (unsafe)", thin_with_weak),
            ("1a thin pointer as Arc<String> (safe)", arc_string),
            ("1b key on the object list", on_object_list),
        ];
        for (name, bytes) in counterfactuals {
            println!(
                "  {:<46} {:>13} {:>13} {:>13} {:>12.2}",
                name,
                bytes.entries,
                bytes.names,
                bytes.total(),
                bytes.total() as f64 / denominator
            );
        }
        println!(
            "  object list today {object_list_now} B on the heap; carrying keys \
             {object_list_with_keys} B + {node_delta} B resident across {} buckets",
            census.buckets
        );
        println!("  --- against today, per page ---");
        for (name, bytes) in counterfactuals.iter().skip(1) {
            println!(
                "  {:<46} {:>+12.2} B a page",
                name,
                (bytes.total() as f64 - today.total() as f64) / denominator
            );
        }

        // --- STEP 4, ON THE SAME POPULATION. The list's `u64` is a cached derivation of the entry
        //     beside the entry, so folding it out is 8 bytes of stride a page with no ABI change and
        //     no step above it. What it costs is a HASH PER PROBE, because the list is sorted by the
        //     handle and bisected -- and the probe depth is a property of this store's
        //     pages-per-bucket population, which is why it is printed here rather than reasoned
        //     about. COUNTED, not timed: a timing ratio on this box has read 485x idle against 11x
        //     busy off identical code.
        // AND IT IS THE `Many` ARM ONLY. `One` holds the handle INSIDE the enum beside a boxed
        // entry, and the enum is as wide as `Many`'s list either way -- 24 bytes -- so taking the
        // handle out of the single-page arm reclaims nothing at all. Charging the saving over every
        // page would credit step 4 with eight bytes for buckets where it is worth zero, which at the
        // legacy wide range is nearly all of them. TWO POPULATIONS AGAIN.
        let pages_in_multi_page_buckets: usize = census
            .pages_per_bucket
            .iter()
            .filter(|(held, _)| **held >= 2)
            .map(|(held, buckets)| held * buckets)
            .sum();
        let handle_stride_saving = size_of::<u64>() * pages_in_multi_page_buckets;
        let probes_p50 = (quantile(&census.pages_per_bucket, 0.50) as f64)
            .max(1.0)
            .log2()
            .ceil() as usize;
        let probes_max = (widest(&census.pages_per_bucket) as f64).max(1.0).log2().ceil() as usize;
        // `block_index_handle` hashes model_id, object_key, component and five address fields.
        const HANDLE_FIELDS_HASHED: usize = 8;
        println!(
            "  step 4 (handle out of the LIST arm): {handle_stride_saving} B of stride over \
             {pages_in_multi_page_buckets} of {} pages = {:.2} B a page, against {probes_p50} \
             probes at the median bucket and {probes_max} at the widest -- {} and {} field hashes a \
             warm page lookup, where today it is 0",
            census.pages,
            handle_stride_saving as f64 / denominator,
            probes_p50 * HANDLE_FIELDS_HASHED,
            probes_max * HANDLE_FIELDS_HASHED
        );
        assert!(
            probes_max >= probes_p50,
            "{label}: the widest bucket bisects in {probes_max} probes and the median in \
             {probes_p50}; a widest below the median means the histogram is being read wrongly"
        );
        assert_eq!(
            (element_now - size_of::<BlockIndex>()) * pages_in_multi_page_buckets,
            handle_stride_saving,
            "{label}: the list element is {element_now} B around a {} B entry, so the handle is {} \
             B for each of the {pages_in_multi_page_buckets} pages in a list arm. The saving is \
             read off the tuple stride rather than assumed",
            size_of::<BlockIndex>(),
            element_now - size_of::<BlockIndex>()
        );
        assert_eq!(
            census.pages - census.single_page_buckets,
            pages_in_multi_page_buckets,
            "{label}: {} pages sit in list arms by one count and {pages_in_multi_page_buckets} by \
             the other. The two are the same number -- every page is either alone in its bucket or \
             in a list -- and a disagreement means the histogram and the bucket tallies are \
             counting different things",
            census.pages - census.single_page_buckets
        );

        // --- AND WHAT AN EIGHT-BIT OBJECT ORDINAL WOULD BE BOUNDED BY, off the same population.
        //     Step 2 needs the object list step 1b is refuted for, so it does not follow -- but the
        //     ceiling is the number an operator would hit and it is cheaper to state it here than to
        //     have it re-derived by division. THE CAP BINDS AT THE MAX BUCKET AND NOT THE MEAN: hash
        //     spread puts the busiest bucket above n/buckets, so the ratio below is what converts a
        //     corpus size into a ceiling, and it is MEASURED rather than assumed to be one.
        let mean_objects_per_bucket = census.objects as f64 / census.buckets as f64;
        let max_objects_per_bucket = widest(&census.objects_per_bucket);
        let spread = if mean_objects_per_bucket > 0.0 {
            max_objects_per_bucket as f64 / mean_objects_per_bucket
        } else {
            0.0
        };
        // 256 objects in the widest bucket is where a `u8` ordinal runs out.
        let objects_at_the_cap = if spread > 0.0 {
            (256.0 * census.buckets as f64 / spread) as u64
        } else {
            0
        };
        println!(
            "  a u8 object ordinal: MAX {max_objects_per_bucket} objects a bucket against a mean of \
             {mean_objects_per_bucket:.2} -- spread {spread:.2}x, so the widest bucket reaches 256 at \
             about {objects_at_the_cap} objects a shard on this range. A u16 caps at 65,536 a bucket, \
             which is {}x further out",
            256u64 * 256
        );
        assert!(
            max_objects_per_bucket as f64 >= mean_objects_per_bucket,
            "{label}: the widest bucket holds {max_objects_per_bucket} objects against a mean of \
             {mean_objects_per_bucket:.2}. A MAX below the mean is arithmetically impossible, so the \
             histogram and the totals are counting different things"
        );
        assert!(
            max_objects_per_bucket < 256,
            "{label}: the widest bucket already holds {max_objects_per_bucket} objects, so a u8 \
             ordinal would ALREADY overflow on this fixture and the ceiling projected above is not a \
             projection"
        );

        // --- THE ENTRY COLUMN MOVES AND IT IS THE SAME MOVE FOR 1a AND 1b, which is why the
        //     tie-break is the heap beside it and not the struct width. ---
        assert!(
            thin.entries < today.entries,
            "{label}: the page index costs {} B with 48-byte entries against {} B with 64-byte \
             ones. If the entry column does not move, the chunk class is swallowing the step and \
             this module's premise is wrong",
            thin.entries,
            today.entries
        );

        // --- AND THE FREE RIDE IS THE WEAK COUNT, which this row is what proves. ---
        assert!(
            thin_with_weak.names > today.names,
            "{label}: a thin string that kept a weak count is charged {} B of name allocation \
             against `Arc<str>`'s {} B. Three header words must cost more than two; an equality \
             here means the name column is not responding to the header at all, and the equality \
             asserted for the two-word row below would then be vacuous",
            thin_with_weak.names,
            today.names
        );
        assert_eq!(
            today.names, thin.names,
            "{label}: the two-word thin allocation is charged {} B against `Arc<str>`'s {} B. They \
             are supposed to be identical -- the length takes the weak count's word -- and the \
             whole arithmetic for step 1a rests on it",
            thin.names, today.names
        );
        assert!(
            arc_string.names > today.names,
            "{label}: `Arc<String>` is charged {} B of name allocation against `Arc<str>`'s {} B. \
             It holds a whole `String` behind the counts AND the `String`'s own buffer, so an \
             equality here means this row is not measuring two allocations",
            arc_string.names,
            today.names
        );
        assert!(
            thin.total() < today.total(),
            "{label}: the hand-rolled thin pointer costs {} B against today's {}. Every verdict in \
             this module rests on step 1a winning on bytes and being declined on soundness",
            thin.total(),
            today.total()
        );
        thin_wins += 1;

        if label.starts_with("containers") {
            container_arms += 1;
            assert!(
                widest(&census.pages_per_object) >= 100,
                "{label}: the widest object holds {} pages. This arm exists to hold objects of a \
                 hundred pages and it does not reach them, so its row says nothing about the \
                 container population",
                widest(&census.pages_per_object)
            );
        } else {
            routed_arms += 1;
            assert_eq!(
                1,
                quantile(&census.pages_per_object, 0.50),
                "{label}: the median object holds {} pages. This arm exists to hold the \
                 one-page-per-object population",
                quantile(&census.pages_per_object, 0.50)
            );
        }
    }

    assert_eq!(4, container_arms, "{container_arms} container arms ran, not four");
    assert_eq!(4, routed_arms, "{routed_arms} routed arms ran, not four");
    assert_eq!(
        8, thin_wins,
        "step 1a was asserted a win on {thin_wins} of the eight arms; a verdict resting on fewer \
         than all of them is not the verdict reported"
    );
}

/// WHAT AN `Arc<str>` ACTUALLY ASKS THE ALLOCATOR FOR, read back from the allocator rather than
/// modelled.
///
/// The whole of step 1a's free ride is the claim that `Arc<str>`'s allocation is TWO header words
/// and the characters, so a thin string that replaces the weak count with the length asks for the
/// same number of bytes. `name_heap` above takes the header width as a parameter precisely so this
/// test can be the thing that fixes it, and the comparison against the three-word row in
/// `what_a_one_word_name_slot_is_worth_on_the_chunk_column` is what stops the two-word equality
/// being vacuous.
///
/// SINGLE-THREADED. The counters are process-wide, so this must be run with `--test-threads=1`; a
/// concurrent allocation from another test would land in this span. That is why it asserts the
/// allocation COUNT as well as the byte total -- a perturbed span shows up as a count above one.
#[cfg(feature = "alloc-probe")]
#[test]
#[ignore = "counting allocator; run by name under --features alloc-probe with --test-threads=1"]
fn an_arc_str_allocation_is_two_header_words_and_the_characters() {
    let text = "tenant/1/object/00000006";
    assert_eq!(
        24,
        text.len(),
        "the fixture key is {} characters, and the expected byte total below is written in terms of \
         24",
        text.len()
    );
    // Warm anything the first call allocates on the side, so the measured span holds one `Arc`.
    let warm: Arc<str> = Arc::from(text);
    std::hint::black_box(&warm);

    let probe = Probe::start();
    let held: Arc<str> = Arc::from(text);
    let counts = probe.stop();
    std::hint::black_box(&held);

    let expected_request = 2 * size_of::<usize>() + text.len();
    println!(
        "\n=== Arc<str>::from({} chars) ===\n  {} alloc call(s), {} B requested, {} B of chunk\n  \
         modelled: {expected_request} B requested, {} B of chunk",
        text.len(),
        counts.allocs,
        counts.alloc_bytes,
        counts.chunk_bytes,
        chunk(expected_request)
    );
    assert_eq!(
        1, counts.allocs,
        "the span held {} allocations, not one. Either `Arc<str>` no longer allocates once or \
         another thread allocated inside this span -- run with --test-threads=1",
        counts.allocs
    );
    assert_eq!(
        expected_request as u64,
        counts.alloc_bytes,
        "`Arc<str>` asked for {} B to hold {} characters. Two header words and the characters is \
         {expected_request} B, and if that is wrong then a thin string replacing the weak count \
         with a length is NOT the same size and step 1a is not free on the allocation",
        counts.alloc_bytes,
        text.len()
    );
    assert_eq!(
        chunk(expected_request) as u64,
        counts.chunk_bytes,
        "the allocator set aside {} B for a {expected_request} B request; the documented rule says \
         {}. The reading and the rule are supposed to agree on this platform",
        counts.chunk_bytes,
        chunk(expected_request)
    );
}

// =================================================================================================
// 3. STEP 1b, REFUTED ON IDENTITY. The entry cannot find its object without the key.
// =================================================================================================

/// AN ENTRY WHOSE ADDRESS CARRIES NO OBJECT ID HAS NO ROUTE TO ITS OBJECT, and `object_id()` answers
/// ZERO for it rather than refusing.
///
/// This is the refutation of step 1b and it is DRIVEN, not argued. Moving the key onto the bucket's
/// object list makes every class-2 reader reach the characters through the object, and the only
/// handle an entry holds on its object is `BlockAddress::object_id` -- which is `Option<u64>` under
/// `#[serde(default)]`, so an index written before that field existed decodes into pages that carry
/// none, and `BlockIndex::object_id` resolves the absence with `unwrap_or_default()`. A page out of
/// such a decode would look up object 0.
///
/// AND THE PRESENT CASE IS NO BETTER IN KIND: the id is `stable_block_object_id`, an FNV-1a hash over
/// the key, so an id is not a unique handle on an object either -- it is a value two keys may share.
/// #1994 confirmed that is not hypothetical: `object_manager::runtime_report` already folds two
/// buckets holding one id into one object, recording the bucket it saw first.
///
/// SO STEP 1b IS REFUTED AT THE TYPE rather than on bytes -- and step 2's eight-bit object ordinal
/// needs exactly the id-to-key map step 1b would have built, so it falls with it.
///
/// rust-internal: drives the decode and the accessor, changes no product behaviour
#[test]
fn an_entry_whose_address_carries_no_object_id_cannot_name_its_object() {
    // --- A page filed with no object id, which is what a pre-field index decodes into. ---
    let unrouted = BlockIndex {
        object_key: Arc::from("tenant/1/object/00000006"),
        model_id: StoredModelKind::String,
        component: None,
        address: BlockAddress::from_parts(1, 2, 3, Some(4), None),
        dirty: false,
        deleted: false,
        log_backed: true,
    };
    assert!(
        unrouted.address.object_id().is_none(),
        "the fixture is supposed to carry NO object id; it carries {:?}",
        unrouted.address.object_id()
    );
    assert_eq!(
        0,
        unrouted.object_id(),
        "an entry with no object id in its address answers {} rather than 0, so the silent zero this \
         refutation is about is no longer the behaviour",
        unrouted.object_id()
    );
    assert!(
        !unrouted.object_key.is_empty(),
        "the control: the entry still knows its object BY NAME, which is the field step 1b would \
         remove. If the key were empty this test would be about nothing"
    );

    // --- AND IT SURVIVES THE STORED FORM, so this is not an in-memory-only shape. ---
    let written = serde_json::to_string(&unrouted).expect("a page entry serializes");
    let read_back: BlockIndex = serde_json::from_str(&written).expect("a page entry deserializes");
    assert!(
        read_back.address.object_id().is_none(),
        "a page written with no object id came back carrying {:?}; this refutation is about what a \
         DECODE produces and this fixture would not reach it",
        read_back.address.object_id()
    );
    assert_eq!(0, read_back.object_id());
    println!("\n=== step 1b, refuted ===");
    println!("  stored spelling of an entry with no object id: {written}");
    println!(
        "  its only handle on its object is object_id() = {}, and it holds the key \"{}\"",
        read_back.object_id(),
        read_back.object_key
    );

    // --- A CONTROL: an entry that DOES carry one answers it, so the zero above is the absence and
    //     not a broken accessor. ---
    let routed = BlockIndex {
        object_key: Arc::from("tenant/1/object/00000006"),
        model_id: StoredModelKind::String,
        component: None,
        address: BlockAddress::from_parts(1, 2, 3, Some(4), Some(42)),
        dirty: false,
        deleted: false,
        log_backed: true,
    };
    assert_eq!(
        42,
        routed.object_id(),
        "an entry carrying object id 42 answers {}; the accessor is not reading the field and the \
         zero above proves nothing",
        routed.object_id()
    );

    // --- WHAT THE OBJECT LIST WOULD HAVE TO BECOME, and what that costs the node. ---
    println!("\n=== what step 1b would cost the object list ===");
    println!(
        "  ObjectIndex today       {:>3} B   (Empty | One(u64) | Many(Box<Vec<u64>>)) -- bare ids, \
         no keys",
        size_of::<ObjectIndex>()
    );
    println!(
        "  carrying a shared key   {:>3} B",
        size_of::<MirrorObjectIndexWithKeys>()
    );
    println!(
        "  carrying a thin key     {:>3} B",
        size_of::<MirrorObjectIndexWithThinKeys>()
    );
    println!(
        "  BucketNode today        {:>3} B   -> {} B / {} B with the two shapes above",
        size_of::<BucketNode>(),
        size_of::<BucketNode>() + size_of::<MirrorObjectIndexWithKeys>() - size_of::<ObjectIndex>(),
        size_of::<BucketNode>() + size_of::<MirrorObjectIndexWithThinKeys>()
            - size_of::<ObjectIndex>()
    );
    assert!(
        size_of::<MirrorObjectIndexWithKeys>() > size_of::<ObjectIndex>(),
        "carrying a key alongside the id is supposed to WIDEN the object list; both shapes are {} B",
        size_of::<ObjectIndex>()
    );
    // `BucketNode` IS NOT EDITED HERE. Another tree owns its fields, and this module's verdict is
    // that the change step 1b would need does not pay -- so the numbers are handed over rather than
    // spent.
    assert_eq!(
        88,
        size_of::<BucketNode>(),
        "the node is {} B, so the two projected widths printed above are not the ones this \
         refutation hands to whoever owns the node's fields",
        size_of::<BucketNode>()
    );
}

// =================================================================================================
// 4. THE DEPENDENT STEPS, priced so nobody re-derives them, and declared as not following.
// =================================================================================================

/// WHAT STEPS 2, 3 AND 4 WOULD HAVE REACHED, AND WHY NONE OF THEM FOLLOWS.
///
/// Every width here is read off a DECLARED mirror rather than computed, so the chain is reproduced
/// from this tree's own types instead of quoted from the pull requests that measured each link.
///
/// STEP 2 -- an object ordinal inside the address -- IS A CHOICE BETWEEN A WIDTH THAT BUYS NOTHING
/// AND A WIDTH THAT BUYS A CEILING, and both halves are asserted below from declarations rather than
/// quoted.
///
///   * AT SIXTEEN BITS IT BUYS NOTHING. The address's payload rounds straight back to 24 and the
///     entry does not move at all -- #1994's finding, reproduced here. So "take the `u16`, it keeps
///     98% of the saving" is not available: on this structure the `u16` keeps none of it.
///   * AT EIGHT BITS IT BUYS A HARD CEILING OF 255 OBJECTS A BUCKET where there is none today.
///     Measured on this module's own population at the shipped 0..=1023 range: the widest bucket
///     holds 54 objects against a mean of 39, a spread of 1.38x, so the widest reaches 256 at
///     roughly 190,000 objects a shard -- and #1995 put the same ceiling at about 261,000 records
///     from the other direction. Worse than either figure suggests, because a deleted object keeps
///     its id in `object_index` while its pages leave `block_index`, so occupancy includes ids no
///     page belongs to. And the bucket count is a knob: #1973 made 1023 the default, and an operator
///     who lowers it moves the ceiling proportionally closer.
///
/// Either way the step needs an id-to-key map, which is what step 1b would have created. Step 1b is
/// refuted, so step 2 does not follow -- and if it ever does, exhaustion has to be a loud refusal
/// rather than a wrap or a saturation. #1985's verdict on the same shape: saturating is the
/// arithmetic that looks safest and is exactly the violation of the uniqueness the function exists
/// for.
///
/// STEP 3 -- an element ordinal for the component -- belongs to another tree and is not built here.
/// It is mirrored only so the combined width can be stated.
///
/// STEP 4 -- folding the list's `u64` handle out -- needs NO step above it and no ABI change, so it
/// is reported as available today and declined on its own count rather than as a link in this chain.
///
/// STEP 5 -- giving up the alignment -- IS WORTH NOTHING, and that is measured here rather than
/// judged. The entry does live in a tuple with the handle, and a tuple re-aligns to eight whatever
/// its members do, so an 18-byte entry strides at the same 32 a 24-byte one does. And the honest
/// packed floor is 22, not the 18 quoted for it: that figure omits `model_id` and the three flag
/// bytes, which are 4 of our 22.
///
/// rust-internal: measures declarations, no product behaviour
#[test]
fn the_object_ordinal_and_the_packing_do_not_follow_and_here_is_what_they_would_have_been() {
    // --- STEP 2 EXISTS AT EIGHT BITS AND NOT AT SIXTEEN. ---
    assert_eq!(
        size_of::<BlockAddress>(),
        size_of::<MirrorAddressSixteenBitObject>(),
        "a sixteen-bit object ordinal is supposed to leave the address exactly as wide as it is \
         today ({} B); the mirror is {} B, so #1994's arithmetic is not being reproduced",
        size_of::<BlockAddress>(),
        size_of::<MirrorAddressSixteenBitObject>()
    );
    assert!(
        size_of::<MirrorAddressEightBitObject>() < size_of::<BlockAddress>(),
        "an eight-bit ordinal is supposed to narrow the address below its {} B; the mirror is {} B",
        size_of::<BlockAddress>(),
        size_of::<MirrorAddressEightBitObject>()
    );

    // --- THE CONTAINER IS THE LIST, which is what makes the STRIDE the number that multiplies by
    //     the page count. `state.rs` asserts this too; it is repeated here because every stride
    //     below is meaningless if the page index is not exactly its list. ---
    assert_eq!(
        size_of::<crate::engine::state::BlockIndexMap>(),
        size_of::<Vec<(u64, BlockIndex)>>(),
        "the page index is {} B and its list is {} B, so the index is no longer exactly its list \
         and a tuple stride is not what a page costs",
        size_of::<crate::engine::state::BlockIndexMap>(),
        size_of::<Vec<(u64, BlockIndex)>>()
    );

    let chain: [(&str, usize, usize, &str); 11] = [
        ("entry today", size_of::<BlockIndex>(), size_of::<(u64, BlockIndex)>(), "shipped"),
        (
            "step 1a  two one-word name slots",
            size_of::<MirrorEntryThinNames>(),
            size_of::<(u64, MirrorEntryThinNames)>(),
            "measured, DECLINED on soundness",
        ),
        (
            "step 1b  key on the bucket's object list",
            size_of::<MirrorEntryNoKey>(),
            size_of::<(u64, MirrorEntryNoKey)>(),
            "measured, REFUTED on identity",
        ),
        (
            "step 2   + sixteen-bit object ordinal",
            size_of::<MirrorEntryWideOrdinalObject>(),
            size_of::<(u64, MirrorEntryWideOrdinalObject)>(),
            "worth nothing at this width",
        ),
        (
            "step 2   + eight-bit object ordinal",
            size_of::<MirrorEntryOrdinalObject>(),
            size_of::<(u64, MirrorEntryOrdinalObject)>(),
            "does not follow: needs 1b",
        ),
        (
            "step 3   + element ordinal for component",
            size_of::<MirrorEntryBothOrdinals>(),
            size_of::<(u64, MirrorEntryBothOrdinals)>(),
            "another tree's; does not follow",
        ),
        (
            "step 4   handle folded out of the list",
            size_of::<MirrorEntryBothOrdinals>(),
            size_of::<MirrorEntryBothOrdinals>(),
            "declined on a probe count; needs no step above it",
        ),
        (
            "step 4   handle folded out, entry as shipped",
            size_of::<BlockIndex>(),
            size_of::<BlockIndex>(),
            "8 B of stride available TODAY, declined",
        ),
        (
            "step 5   + repr(packed), WHILE the handle stays",
            size_of::<MirrorEntryBothOrdinalsPacked>(),
            size_of::<(u64, MirrorEntryBothOrdinalsPacked)>(),
            "worth NOTHING: the tuple re-aligns to eight",
        ),
        (
            "step 5   + repr(packed), AFTER step 4",
            size_of::<MirrorEntryBothOrdinalsPacked>(),
            size_of::<MirrorEntryBothOrdinalsPacked>(),
            "worth 2 B, not 6: our packed floor is 22",
        ),
        (
            "step 5   + packed and three flags in one byte",
            size_of::<MirrorEntryBothOrdinalsPackedOneFlagByte>(),
            size_of::<MirrorEntryBothOrdinalsPackedOneFlagByte>(),
            "the last field difference from theirs",
        ),
    ];
    println!("\n=== the whole chain, each link a declared mirror ===");
    println!("  {:<46} {:>6} {:>8}   verdict", "link", "entry", "stride");
    for (name, width, stride, verdict) in &chain {
        println!("  {name:<46} {width:>4} B {stride:>6} B   {verdict}");
    }

    // --- PACKING IS WORTH NOTHING WHILE THE HANDLE STAYS, AND TWO BYTES AFTER IT GOES. Both halves
    //     are asserted, because the first is the trap -- an 18-byte entry in a tuple with a `u64`
    //     strides at the same 32 a 24-byte one does, and measuring `size_of::<BlockIndex>()` instead
    //     of the stride is how that reads as six bytes a page. ---
    assert!(
        size_of::<MirrorEntryBothOrdinalsPacked>() < size_of::<MirrorEntryBothOrdinals>(),
        "packing is supposed to narrow the STRUCT -- {} B against {} B -- which is the part of the \
         claim that is true",
        size_of::<MirrorEntryBothOrdinalsPacked>(),
        size_of::<MirrorEntryBothOrdinals>()
    );
    assert_eq!(
        size_of::<(u64, MirrorEntryBothOrdinals)>(),
        size_of::<(u64, MirrorEntryBothOrdinalsPacked)>(),
        "packing takes the TUPLE stride from {} B to {} B. While the handle is stored it is supposed \
         to take it nowhere, which is why step 5 is ordered behind step 4 and not beside it",
        size_of::<(u64, MirrorEntryBothOrdinals)>(),
        size_of::<(u64, MirrorEntryBothOrdinalsPacked)>()
    );
    assert!(
        size_of::<MirrorEntryBothOrdinalsPacked>() < size_of::<MirrorEntryBothOrdinals>(),
        "with the handle gone the stride IS the entry, so packing has to narrow it or step 5 is \
         worth nothing in either order"
    );
    assert_eq!(
        2,
        size_of::<MirrorEntryBothOrdinals>() - size_of::<MirrorEntryBothOrdinalsPacked>(),
        "packing behind step 4 is worth {} B a page, not the 2 this module reports. The six bytes it \
         was projected at assumed an 18-byte packed floor; ours is {} B because `model_id` and the \
         three flag bytes are not in that decomposition",
        size_of::<MirrorEntryBothOrdinals>() - size_of::<MirrorEntryBothOrdinalsPacked>(),
        size_of::<MirrorEntryBothOrdinalsPacked>()
    );

    // --- AND WHAT IS LEFT BETWEEN OUR PACKED FLOOR AND THEIR 17 IS FIELD CONTENT, NOT ALIGNMENT.
    //     Their entry is object id 1 + model id 1 + page id 2 + flags 1 + page size 4 + address 8.
    //     Ours, packed and with the flags folded, is the same list plus a two-byte `block_id` and
    //     the address's one-byte presence flag. ---
    const THEIR_PACKED_ENTRY: usize = 1 + 1 + 2 + 1 + 4 + 8;
    let ours_folded = size_of::<MirrorEntryBothOrdinalsPackedOneFlagByte>();
    println!(
        "\n=== the ceiling, field by field rather than as alignment ===\n  our packed floor {} B, \
         with three flags folded into one {ours_folded} B, theirs {THEIR_PACKED_ENTRY} B\n  the \
         remaining {} B are a two-byte block id and the address's presence byte -- content we carry \
         and they do not, not rounding",
        size_of::<MirrorEntryBothOrdinalsPacked>(),
        ours_folded - THEIR_PACKED_ENTRY
    );
    assert_eq!(
        17, THEIR_PACKED_ENTRY,
        "the compared entry reconstructs to {THEIR_PACKED_ENTRY} B, not the 17 it is quoted at, so \
         the ceiling stated here is against a different structure"
    );
    assert_eq!(
        3,
        ours_folded - THEIR_PACKED_ENTRY,
        "with every step taken and the flags folded we are {} B above their {THEIR_PACKED_ENTRY}, \
         not 3. The claim is that the residue is a two-byte block id plus a one-byte address \
         presence flag, and a different number means the residue is something else",
        ours_folded - THEIR_PACKED_ENTRY
    );

    // --- AND STEP 4 IS EIGHT BYTES OF STRIDE WITH NO STEP ABOVE IT, which is why it is reported as
    //     available today rather than as the last link. ---
    assert_eq!(
        size_of::<u64>(),
        size_of::<(u64, BlockIndex)>() - size_of::<BlockIndex>(),
        "the list's handle is worth {} B of stride, not one word. Every step-4 figure is about the \
         tuple's `u64` and nothing else",
        size_of::<(u64, BlockIndex)>() - size_of::<BlockIndex>()
    );

    assert_eq!(
        48,
        size_of::<MirrorEntryNoKey>(),
        "step 1b lands the entry at {} B, not 48",
        size_of::<MirrorEntryNoKey>()
    );
    assert_eq!(
        size_of::<MirrorEntryNoKey>(),
        size_of::<MirrorEntryWideOrdinalObject>(),
        "a sixteen-bit ordinal takes the entry from {} B to {} B. It is supposed to take it \
         nowhere, which is the whole reason only the eight-bit width is a step",
        size_of::<MirrorEntryNoKey>(),
        size_of::<MirrorEntryWideOrdinalObject>()
    );
    assert_eq!(
        40,
        size_of::<MirrorEntryOrdinalObject>(),
        "step 2 lands the entry at {} B, not 40",
        size_of::<MirrorEntryOrdinalObject>()
    );
    assert_eq!(
        24,
        size_of::<MirrorEntryBothOrdinals>(),
        "step 3 lands the entry at {} B, not 24",
        size_of::<MirrorEntryBothOrdinals>()
    );
    assert_eq!(
        22,
        size_of::<MirrorEntryBothOrdinalsPacked>(),
        "the packed floor is {} B, not 22",
        size_of::<MirrorEntryBothOrdinalsPacked>()
    );

    // --- AND THE 18 QUOTED FOR OUR PACKED FLOOR IS NOT OUR FLOOR. Their 17 bytes of field occupy
    //     17 bytes because the structure is packed; the 18-byte decomposition offered for ours --
    //     merged address 8, length 4, block id 2, present 1, object ordinal 1, component ordinal 2
    //     -- leaves out `model_id` and the three flags. With them it is 22, which is what the packed
    //     mirror above measures. ---
    let quoted_floor = 8 + 4 + 2 + 1 + 1 + 2;
    let ours_on_top = size_of::<StoredModelKind>() + 3 * size_of::<bool>();
    println!(
        "\n  quoted floor {quoted_floor} B + model_id and three flags {ours_on_top} B = {} B, and \
         the packed mirror measures {} B",
        quoted_floor + ours_on_top,
        size_of::<MirrorEntryBothOrdinalsPacked>()
    );
    assert_eq!(
        quoted_floor + ours_on_top,
        size_of::<MirrorEntryBothOrdinalsPacked>(),
        "the packed mirror is {} B and the decomposition reconstructs to {} B; one of the two is \
         describing a different structure",
        size_of::<MirrorEntryBothOrdinalsPacked>(),
        quoted_floor + ours_on_top
    );

    // --- THE CEILING. Their entry is 17 and carries NO key and NO component. Ours cannot reach it
    //     without that object model, and three merged refutations say why. ---
    assert!(
        size_of::<MirrorEntryBothOrdinalsPacked>() > 17,
        "our packed floor is {} B, which would reach their 17 -- and three merged refutations say \
         our readers need the characters somewhere, so reaching it would mean one of them is wrong",
        size_of::<MirrorEntryBothOrdinalsPacked>()
    );
}

/// A PACKED ENTRY READS ITS ADDRESS BY VALUE, BECAUSE A BORROW OF IT IS UNDEFINED BEHAVIOUR.
///
/// This is the cost of step 5 that is not bytes, and it is the class no zero-error pass reaches:
/// `&entry.address` on a `repr(packed)` structure is a misaligned borrow, which is undefined
/// behaviour, and it COMPILES: rustc's misaligned-borrow lint fires on a borrow written out in
/// full, and there is no diagnostic for the same borrow reached through a method, a macro or a
/// generic. The same shape as the width assertions #1994 had to sweep separately -- a claim that
/// compiles is a claim no error count sees.
///
/// So the discipline a packed entry would require is that every field is read BY VALUE through an
/// accessor that copies, exactly as the compared design does -- its address is a plain 64-bit word
/// copied out, never borrowed. This test drives that: the address is read out of a packed mirror as a
/// copy, and the copy is asserted equal to what was written, so the accessor shape is exercised
/// rather than described.
///
/// rust-internal: measures declarations, no product behaviour
#[test]
fn a_packed_entry_reads_its_address_by_value_because_a_borrow_of_it_is_undefined() {
    let written = MirrorAddressEightBitObject {
        address_word: 0x0123_4567_89ab_cdef,
        length: 4_096,
        block_id: 7,
        present: 1,
        object_ordinal: 42,
    };
    let entry = MirrorEntryBothOrdinalsPacked {
        model_id: StoredModelKind::String,
        component: 9,
        address: written,
        dirty: true,
        deleted: false,
        log_backed: true,
    };
    // BY VALUE. `let read = entry.address;` copies out of the packed layout; `&entry.address` would
    // be a misaligned borrow and is deliberately not written anywhere in this module.
    let read = entry.address;
    assert_eq!(
        written, read,
        "the address copied out of a packed entry is {read:?} against the {written:?} that went in; \
         if a by-value read does not round-trip then the accessor discipline step 5 needs is not \
         this one"
    );
    // And the flags are read the same way, so the test covers the one-byte members too.
    let dirty = entry.dirty;
    let log_backed = entry.log_backed;
    assert!(dirty && log_backed, "the flag members did not survive a by-value read");

    println!(
        "\n=== step 5's non-byte cost ===\n  a packed entry is {} B and {}-aligned; every field read \
         must copy, and `&entry.address` compiles while being undefined",
        size_of::<MirrorEntryBothOrdinalsPacked>(),
        align_of::<MirrorEntryBothOrdinalsPacked>()
    );
    assert_eq!(
        1,
        align_of::<MirrorEntryBothOrdinalsPacked>(),
        "the packed mirror is {}-aligned, so it is not packed and this test is about a structure \
         whose field reads are perfectly safe",
        align_of::<MirrorEntryBothOrdinalsPacked>()
    );
    assert!(
        align_of::<MirrorEntryBothOrdinals>() > align_of::<MirrorEntryBothOrdinalsPacked>(),
        "the unpacked mirror is {}-aligned and the packed one {}. If they agree, packing changed \
         nothing and there is no unaligned read to pay for",
        align_of::<MirrorEntryBothOrdinals>(),
        align_of::<MirrorEntryBothOrdinalsPacked>()
    );
}

// =================================================================================================
// 5. WHY STEP 1a IS DECLINED. The only thin pointer that is safe to build here loses.
// =================================================================================================

/// THERE IS NO SAFE THIN REFCOUNTED STRING IN THIS TREE, AND THE ONE SHAPE THAT NEEDS NO UNSAFE
/// COSTS MORE THAN THE ENTRY SAVES.
///
/// `Arc<str>` is fat because `str` is unsized; the length rides beside the pointer. The safe way to
/// get one word is `Arc<String>` -- `String` is sized, so the pointer is thin -- and it pays for it
/// with a SECOND allocation per name: the `Arc` holding a 24-byte `String`, and the `String`'s own
/// buffer. This test prices that per distinct name against what the entry saves per page, at both
/// populations, and the two answers have opposite signs -- which is why a mean would name neither.
///
/// PRICED HERE ON THE OBJECT KEY ALONE, because that is the name whose allocation is shared across an
/// object's pages, so its cost DIVIDES by the pages of its object: +16.00 B a page where an object
/// has one page and -15.68 where it has a hundred.
///
/// AND THE WHOLE-STORE FIGURE HAS NO SECOND SIGN, which is why the verdict is not "loses on the
/// routed population". The component is interned only up to `KIND_POOL_CAP`, so a container's
/// components allocate PER PAGE -- 33,763 distinct component allocations across 40,000 container
/// pages, measured -- and their cost does not divide at all.
/// `what_a_one_word_name_slot_is_worth_on_the_chunk_column` therefore reads +17.00 to +32.00 B a page
/// on ALL EIGHT arms, containers included.
///
/// THE OTHER WAY IS HAND-ROLLED UNSAFE, and it is declined rather than attempted. A thin refcounted
/// string is an allocation this crate would own outright: a strong count, a length and the
/// characters behind one `Layout`, a `Deref` through `from_utf8_unchecked`, `unsafe impl Send` and
/// `Sync`, and a `Drop` whose release-then-acquire ordering is the only thing standing between a
/// shared page entry and a use-after-free. It would also have to spread: `object_key` is shared by
/// POINTER with `ObjectBlockLookup`'s inner key through `shared_object_key`, and `component` with
/// `kind_pool` through `intern_shared` and with `ComponentBlocks` through a clone, so all of those
/// would have to hold the same type or the sharing `page_entry_names` asserts would break.
///
/// AND NOTHING IN THIS REPOSITORY WOULD CHECK IT. The entire production unsafe surface of this crate
/// is four single-expression FFI calls -- `flock`, `malloc_trim`, an SSE4.2 CRC intrinsic and
/// `std::env::set_var`. There is no self-managed allocation anywhere in it. Miri is not installed and
/// is not available for the toolchain this tree pins, there is no sanitizer job among the five
/// workflows, and the Rust workflow's test step is `continue-on-error`, so a green run is a COMPILE.
/// Sixteen bytes a page does not buy that.
///
/// rust-internal: measures declarations, no product behaviour
#[test]
fn the_only_thin_pointer_that_is_safe_to_build_here_costs_more_than_the_entry_saves() {
    // --- `Arc<String>` is thin, and that is the whole of its appeal. ---
    assert_eq!(
        size_of::<usize>(),
        size_of::<Arc<String>>(),
        "`Arc<String>` is {} B; if it were fat there would be no safe thin shape at all and this \
         test is about the wrong type",
        size_of::<Arc<String>>()
    );
    assert_eq!(
        2 * size_of::<usize>(),
        size_of::<Arc<str>>(),
        "`Arc<str>` is {} B, not the two words this whole module is about",
        size_of::<Arc<str>>()
    );

    // --- What each shape charges for ONE name, by length, and what the entry saves per page. ---
    let entry_saving = size_of::<BlockIndex>() - size_of::<MirrorEntryThinNames>();
    println!("\n=== one name allocation, by key length ===");
    println!(
        "  {:<6} {:>12} {:>16} {:>16} {:>18} {:>12}",
        "len", "Arc<str>", "thin, no weak", "thin, weak kept", "Arc<String> (safe)", "safe cost"
    );
    let mut safe_costs: Vec<(usize, isize)> = Vec::new();
    let mut weak_costs: Vec<(usize, isize)> = Vec::new();
    for len in [1usize, 2, 8, 16, 24, 32, 64] {
        let fat = chunk(2 * size_of::<usize>() + len);
        let thin = chunk(2 * size_of::<usize>() + len);
        let thin_weak = chunk(3 * size_of::<usize>() + len);
        let safe = chunk(2 * size_of::<usize>() + size_of::<String>()) + chunk(len);
        println!(
            "  {len:<6} {fat:>12} {thin:>16} {thin_weak:>16} {safe:>18} {:>+12}",
            safe as isize - fat as isize
        );
        safe_costs.push((len, safe as isize - fat as isize));
        weak_costs.push((len, thin_weak as isize - fat as isize));
    }
    assert_eq!(7, safe_costs.len(), "the sweep must price seven lengths");

    // --- THE SAFE SHAPE IS NOT FREE AT ANY LENGTH IN THE SWEEP. ---
    for (len, cost) in &safe_costs {
        assert!(
            *cost > 0,
            "`Arc<String>` is charged {cost} B against `Arc<str>` at length {len}. It holds a whole \
             `String` behind the counts plus the `String`'s own buffer, so a non-positive cost here \
             means this row is not measuring two allocations"
        );
    }
    // --- AND KEEPING A WEAK COUNT IS NOT FREE EITHER, at some length: three header words cannot
    //     round into two at every size, and if they did the "free ride" claim would not depend on
    //     dropping `Weak` and this module's reasoning would be wrong. ---
    assert!(
        weak_costs.iter().any(|(_, cost)| *cost > 0),
        "a thin string keeping a weak count is charged nothing extra at any of the seven lengths. \
         The free ride step 1a claims is supposed to DEPEND on dropping `Weak`; if a third header \
         word costs nothing anywhere then the reasoning behind it is not this one"
    );

    // --- THE TWO POPULATIONS, AND THEY HAVE OPPOSITE SIGNS. A page's share of an object-key
    //     allocation is the allocation divided by the pages of its object: 1 for a routed key, 100
    //     for a container. #1986 found the mixed histogram holding only those two values. ---
    let safe_at_24 = safe_costs
        .iter()
        .find(|(len, _)| *len == 24)
        .expect("24 is in the sweep")
        .1;
    println!(
        "\n=== the safe shape at the two populations, the object key alone, per page ===\n  entry \
         saving {entry_saving} B a page; at a 24-character key the safe shape costs {safe_at_24} B \
         an allocation"
    );
    let mut verdicts = 0usize;
    for (population, pages_per_object) in [("routed keys", 1usize), ("containers", 100usize)] {
        let per_page = safe_at_24 as f64 / pages_per_object as f64 - entry_saving as f64;
        println!(
            "  {population:<14} {pages_per_object:>4} pages an object   {per_page:>+9.2} B a page"
        );
        verdicts += 1;
        if pages_per_object == 1 {
            assert!(
                per_page > 0.0,
                "the safe shape is {per_page:+.2} B a page on the one-page-per-object population. It \
                 is declined because it LOSES there, and if it wins this module's verdict on step 1a \
                 is the wrong one"
            );
        } else {
            assert!(
                per_page < 0.0,
                "the safe shape is {per_page:+.2} B a page on the container population, where it is \
                 supposed to win -- a single sign across both populations would mean there is only \
                 one population and the two-population framing is wrong"
            );
        }
    }
    assert_eq!(2, verdicts, "both populations must be priced");

    // --- THE UNSAFE SURFACE THIS TREE CARRIES is stated in the doc above and deliberately not
    //     asserted: a test cannot read its own workflow files, and a count of `unsafe` tokens would
    //     pass under any mutation that added one. ---
    println!(
        "\n=== the verdict ===\n  step 1a saves {entry_saving} B a page on BOTH populations and \
         nothing on the name allocation, and is declined because the only way to build it here is an \
         allocation this crate would own outright, with no Miri, no sanitizer job, and a Rust \
         workflow whose test step is continue-on-error."
    );
}
