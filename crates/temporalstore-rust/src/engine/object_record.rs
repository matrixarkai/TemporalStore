// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! ONE RECORD PER OBJECT: the address, the deadline, the model and the state in a single
//! twenty-four byte value, so that an object costs one row instead of four to six.
//!
//! # WHY THIS EXISTS
//!
//! A live object in a shard is currently named by several independent key-addressed structures,
//! each of which pays its own tree-or-hash node AND its own copy-or-refcount of the object key's
//! characters. For one plain `string` that is the model map, the page lookup, the dirty index and
//! the page index; with a deadline it is two more. The width of any one of those rows is not the
//! cost -- the COUNT of them is, because the key copy multiplies by it.
//!
//! This type is the single row those collapse into: one address, one deadline, one model id and
//! one set of state bits. It is deliberately a VALUE and not a map, so that whatever holds it
//! decides the key. That is what makes it independent of the integer-key work in flight elsewhere:
//! held in a map keyed by the object key's characters it saves the SIBLING rows today, and held in
//! a map keyed by an assigned integer it additionally saves the characters. Neither is assumed
//! here.
//!
//! # WHAT IT DOES NOT REPLACE, AND WHY THAT IS NOT A SHORTFALL
//!
//! Two of the structures it sits beside answer ITERATION questions rather than point questions,
//! and an unordered per-object row cannot answer those at all:
//!
//!   * a deadline index ordered BY DEADLINE answers "which object expires next". This record
//!     answers "when does THIS object expire". Folding the first into the second replaces a
//!     prefix walk with a shard scan, so the ordered index survives and what this removes is its
//!     DUPLICATE of the deadline value, not its ordering.
//!   * a dirty index grouped BY BUCKET answers "which objects in this bucket are dirty", which a
//!     per-object bit cannot answer without visiting every object. That grouping survives too.
//!
//! Both are stated here rather than discovered later, because a merge that destroys an ordering is
//! a correctness loss wearing a saving's clothes.
//!
//! # THE TWO TAGGED WORDS ARE THE DESIGN
//!
//! The deadline word and the address word are each EITHER a value OR a handle naming a side
//! structure, and which one is decided by a TAG BIT, never by the value. That is the same
//! conclusion [`crate::block_store::ElementEntry`] already reached for its own presence byte, for
//! the same reason it wrote down: zero is a legal block id, so "zero means absent" would silently
//! erase a real first page. A deadline of zero and an address word of zero are both legal, so
//! neither can carry its own absence.
//!
//! The tags are enforced STRUCTURALLY and not by convention. Both words are private and
//! unreachable except through [`ObjectRecord::deadline`] and [`ObjectRecord::page_address`], which
//! return a named arm. There is no accessor that hands out a bare word a caller could dereference
//! without having consulted its tag -- that shape is how a handle becomes a wild pointer.
//!
//! # WHY A HANDLE AND NOT A POINTER
//!
//! The shape this follows spends a real heap pointer in the pointer arm. This holds an opaque
//! handle into a side table instead, for two reasons that are both about this codebase rather than
//! about taste. The record is DURABLE -- it is the value of a map that rides the shard index
//! snapshot -- and a heap pointer cannot be serialized, so a pointer arm would have to be rebuilt
//! on every load from something else, which is a second source of truth. And a handle cannot be
//! dereferenced by accident: the side table either holds it or refuses, where a stale pointer
//! reads whatever now lives there.
//!
//! # WHY NOT `repr(packed)`
//!
//! The shape this follows reaches twenty-four bytes with a packing attribute. Reproducing that
//! here would be a measured mistake: an earlier leg of this campaign priced `repr(packed)` on the
//! neighbouring index types at roughly 390 `E0793` unaligned-reference sites, and it buys nothing
//! where the field order already leaves no interior padding. The field order below reaches the
//! same twenty-four bytes at natural alignment, with one byte of TAIL slack and no packing, and
//! the width and the alignment are pinned SEPARATELY below.

use serde::{Deserialize, Serialize};

use super::storage_bucket_internals::StoredModelKind;
use crate::block_store::{
    extract_block_offset, extract_block_slab_id, make_block_address_word, ElementEntry,
};

// =================================================================================================
// THE FLAGS
// =================================================================================================

/// Eleven named bits in sixteen, five reserved.
///
/// # THE BIT LIST, AND THE TWO THAT ARE NOT IN THE SHAPE THIS FOLLOWS
///
/// Ten of these are the state a consolidated row carries: two tags, four object-level states and
/// four page-level states. Two more are here because a LOSSLESS conversion needs them, and both
/// were found by writing the conversion test rather than by reading the layout:
///
///   * `DEADLINE_PRESENT` (bit 10). One tag bit gives a word two meanings, and this record needs
///     THREE: absent, a value, or a handle. A sentinel value is refused for the reason stated in
///     the module header, so absence costs a bit. Ten bits cannot represent an absent deadline
///     without one.
///   * `PAGE_GENERATION` (bit 11). [`ElementEntry`] carries a generation-present bit that is
///     independent of its block-id-present bit ON THE STORED PATH: an index written before the
///     generation existed carries an identity and no generation, and must keep loading that way.
///     Dropping this bit would hand such an entry a generation it never had and move every key it
///     resolves through, which is the defect that type's own doc records being caught by a round
///     trip.
///
/// Four bits remain reserved. A wider flags field would push this structure to thirty-two bytes,
/// so the four are the budget; a decoder REFUSES a reserved bit it finds set rather than ignoring
/// it, so a newer writer's record is rejected by name instead of being read with its meaning
/// dropped.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub(super) struct RecordFlags(u16);

impl RecordFlags {
    /// The address word is a handle naming a multi-page layout, not a page address.
    pub(super) const ADDRESS_IS_POINTER: u16 = 1 << 0;
    /// The deadline word is a handle naming a list of per-element deadlines, not one deadline.
    pub(super) const DEADLINE_IS_POINTER: u16 = 1 << 1;
    /// The object has unflushed writes.
    pub(super) const DIRTY: u16 = 1 << 2;
    /// The object's INDEX-LOG state has unflushed writes, independently of its data.
    ///
    /// NAMED FOR THE SPLIT THIS ENGINE ALREADY MAKES, which is between the write-ahead log and
    /// the index log: a bucket carries `first_dirty_wal_sequence` and
    /// `first_dirty_index_log_sequence` as two separate watermarks, and a round can clear one
    /// while the other still pins its log. `DIRTY` above is the data side of that split and this
    /// is the index side, so the pair reads as the engine's own two watermarks rather than as a
    /// generic notion of metadata.
    pub(super) const INDEX_DIRTY: u16 = 1 << 3;
    /// A load for this object is in flight.
    pub(super) const LOADING: u16 = 1 << 4;
    /// The object's bytes are resident.
    pub(super) const IN_MEMORY: u16 = 1 << 5;
    /// The page named by the address word has unflushed writes.
    pub(super) const PAGE_DIRTY: u16 = 1 << 6;
    /// The page named by the address word is a tombstone.
    pub(super) const PAGE_DELETED: u16 = 1 << 7;
    /// The page's only durable copy is an operation-log record.
    pub(super) const PAGE_IN_LOG: u16 = 1 << 8;
    //
    // BIT 9 IS A HOLE, AND IT IS THE RECORD OF A BIT REMOVED AS REDUNDANT.
    //
    // It held a second flag whose documented meaning was "the page is deleted" -- which is
    // exactly what `PAGE_DELETED` at bit 7 already says. Two bits for one fact is worse than
    // either of them alone, because nothing stops them disagreeing and then no reader knows
    // which to believe. So the fact keeps one bit and the other is gone.
    //
    // THE HOLE IS KEPT RATHER THAN RENUMBERED, following the same reasoning `ElementEntry`'s
    // presence byte gives for its own holes: nothing depends on the numbering being dense, and a
    // hole is cheaper to read than a renumbering. Here it also costs nothing, because bit 9 falls
    // into `RESERVED` below -- so a writer that sets the retired bit is refused by name as a
    // newer writer, which is the correct answer for a bit this reader no longer understands.
    //
    /// A deadline is present at all. See the type doc for why ten bits cannot say this.
    pub(super) const DEADLINE_PRESENT: u16 = 1 << 10;
    /// The page carries a generation, independently of whether it carries an identity.
    pub(super) const PAGE_GENERATION: u16 = 1 << 11;

    /// Bit 9 and bits 12..=15. Set in a decoded record, these mean a writer newer than this
    /// reader -- bit 9 because it was retired as redundant, the rest because they were never
    /// spent. A decoder REFUSES any of them rather than reading a record with their meaning
    /// dropped.
    pub(super) const RESERVED: u16 = 0xF200;

    /// Every bit this reader understands.
    pub(super) const KNOWN: u16 = !Self::RESERVED;

    pub(super) const fn empty() -> Self {
        Self(0)
    }

    pub(super) const fn bits(self) -> u16 {
        self.0
    }

    pub(super) const fn get(self, bit: u16) -> bool {
        self.0 & bit != 0
    }

    pub(super) fn set(&mut self, bit: u16, on: bool) {
        if on {
            self.0 |= bit;
        } else {
            self.0 &= !bit;
        }
    }

    #[must_use]
    pub(super) const fn with(self, bit: u16, on: bool) -> Self {
        Self(if on { self.0 | bit } else { self.0 & !bit })
    }
}

/// Ten named state bits, less one dropped as redundant, plus the two the conversion needs: eleven of sixteen.
const _: () = assert!(RecordFlags::KNOWN.count_ones() == 11);
const _: () = assert!(RecordFlags::RESERVED.count_ones() == 5);
const _: () = assert!(RecordFlags::KNOWN & RecordFlags::RESERVED == 0);

// =================================================================================================
// THE TWO TAGGED WORDS, AS NAMED ARMS
// =================================================================================================

/// A handle naming a list of per-element deadlines held outside the record.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub(super) struct DeadlineListHandle(u64);

impl DeadlineListHandle {
    pub(super) const fn from_stored(value: u64) -> Self {
        Self(value)
    }
    pub(super) const fn get(self) -> u64 {
        self.0
    }
}

/// A handle naming the pages of a multi-page object, held outside the record.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub(super) struct PageLayoutHandle(u64);

impl PageLayoutHandle {
    pub(super) const fn from_stored(value: u64) -> Self {
        Self(value)
    }
    pub(super) const fn get(self) -> u64 {
        self.0
    }
}

/// What the deadline word means. THE TAG DECIDES, NEVER THE VALUE.
///
/// There is no arm for "zero means none": zero is a legal instant, and the module header says why
/// a sentinel is refused here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Deadline {
    /// No deadline. A BIT says so.
    None,
    /// One deadline, in milliseconds.
    At(u64),
    /// Several, one per element, in a side list this handle names.
    PerElement(DeadlineListHandle),
}

/// What the address word means. THE TAG DECIDES, NEVER THE VALUE.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PageAddress {
    /// One page, at this address word: the slab in the high half and the offset in the low.
    One(u64),
    /// Several pages, in a side layout this handle names.
    Many(PageLayoutHandle),
}

// =================================================================================================
// THE RECORD
// =================================================================================================

/// One object's whole index row: a deadline, an address, a page size, the state bits and a model.
///
/// # THE LAYOUT, AND WHY THE FIELDS ARE IN THIS ORDER
///
/// Five fields, widest first: the two tagged words, then the size, then the bits, then the model.
///
/// WIDEST FIRST IS WHAT REMOVES THE INTERIOR PADDING. The two `u64`s are eight-aligned at offsets
/// 0 and 8, the `u32` is four-aligned at 16, the `u16` is two-aligned at 20, and the `u8` needs no
/// alignment at all. That leaves ONE byte standing empty at offset 23 -- at the TAIL, where it
/// costs nothing and where a field up to a byte wide would still be free. Any other order of the
/// same five fields pays for padding between them instead.
///
/// `size_of` is 24 and `align_of` is 8, both pinned below as separate equalities and both
/// measured rather than argued -- see `the_record_is_twenty_four_bytes_at_natural_alignment`,
/// which prints them. Twenty-three bytes of field in twenty-four, with no packing attribute: see
/// the module header for why reproducing one was refused on measurement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "ObjectRecordWire", into = "ObjectRecordWire")]
pub(super) struct ObjectRecord {
    /// Value or handle. UNREADABLE except through [`ObjectRecord::deadline`].
    deadline: u64,
    /// Value or handle. UNREADABLE except through [`ObjectRecord::page_address`].
    address: u64,
    page_size: u32,
    flags: RecordFlags,
    model_id: u8,
}

/// THE WIDTH, PINNED ON ITS OWN.
///
/// A width pin dominates any neighbour inequality -- `!= 23 && != 25` passes at every width and
/// every alignment, which is an assertion that can only ever fail inside a test nothing runs. So
/// this is an equality, and the alignment is a SECOND equality rather than a clause of this one: a
/// change that keeps the width and moves the alignment is a different change, and each should name
/// itself.
const _: () = assert!(std::mem::size_of::<ObjectRecord>() == 24);
const _: () = assert!(std::mem::align_of::<ObjectRecord>() == 8);

/// The field sum, so a field added inside the tail slack is visible as arithmetic rather than
/// hiding behind a width that did not step.
const _: () = assert!(
    std::mem::size_of::<u64>()
        + std::mem::size_of::<u64>()
        + std::mem::size_of::<u32>()
        + std::mem::size_of::<RecordFlags>()
        + std::mem::size_of::<u8>()
        == 23
);
const _: () = assert!(std::mem::size_of::<ObjectRecord>() - 23 == 1);

impl Default for ObjectRecord {
    fn default() -> Self {
        Self {
            deadline: 0,
            address: 0,
            page_size: 0,
            flags: RecordFlags::empty(),
            model_id: 0,
        }
    }
}

impl ObjectRecord {
    // --- The tagged words, as arms. ---

    /// The deadline, decided by its tag bits and not by its value.
    pub(super) fn deadline(&self) -> Deadline {
        if !self.flags.get(RecordFlags::DEADLINE_PRESENT) {
            return Deadline::None;
        }
        if self.flags.get(RecordFlags::DEADLINE_IS_POINTER) {
            return Deadline::PerElement(DeadlineListHandle::from_stored(self.deadline));
        }
        Deadline::At(self.deadline)
    }

    /// Set the deadline and its tag bits TOGETHER, so the two cannot disagree.
    pub(super) fn set_deadline(&mut self, deadline: Deadline) {
        match deadline {
            Deadline::None => {
                self.deadline = 0;
                self.flags.set(RecordFlags::DEADLINE_PRESENT, false);
                self.flags.set(RecordFlags::DEADLINE_IS_POINTER, false);
            }
            Deadline::At(at) => {
                self.deadline = at;
                self.flags.set(RecordFlags::DEADLINE_PRESENT, true);
                self.flags.set(RecordFlags::DEADLINE_IS_POINTER, false);
            }
            Deadline::PerElement(handle) => {
                self.deadline = handle.get();
                self.flags.set(RecordFlags::DEADLINE_PRESENT, true);
                self.flags.set(RecordFlags::DEADLINE_IS_POINTER, true);
            }
        }
    }

    /// The address, decided by its tag bit and not by its value.
    pub(super) fn page_address(&self) -> PageAddress {
        if self.flags.get(RecordFlags::ADDRESS_IS_POINTER) {
            PageAddress::Many(PageLayoutHandle::from_stored(self.address))
        } else {
            PageAddress::One(self.address)
        }
    }

    /// Set the address and its tag bit TOGETHER, so the two cannot disagree.
    pub(super) fn set_page_address(&mut self, address: PageAddress) {
        match address {
            PageAddress::One(word) => {
                self.address = word;
                self.flags.set(RecordFlags::ADDRESS_IS_POINTER, false);
            }
            PageAddress::Many(handle) => {
                self.address = handle.get();
                self.flags.set(RecordFlags::ADDRESS_IS_POINTER, true);
            }
        }
    }

    // --- The plain fields. ---

    pub(super) const fn page_size(&self) -> u32 {
        self.page_size
    }

    pub(super) fn set_page_size(&mut self, value: u32) {
        self.page_size = value;
    }

    pub(super) const fn model_id(&self) -> u8 {
        self.model_id
    }

    pub(super) fn set_model_id(&mut self, value: u8) {
        self.model_id = value;
    }

    pub(super) const fn flags(&self) -> RecordFlags {
        self.flags
    }

    pub(super) fn set_flag(&mut self, bit: u16, on: bool) {
        debug_assert_eq!(
            0,
            bit & RecordFlags::RESERVED,
            "a reserved flag bit cannot be set through this door; name it in RecordFlags first"
        );
        self.flags.set(bit, on);
    }

    pub(super) const fn dirty(&self) -> bool {
        self.flags.get(RecordFlags::DIRTY)
    }

    // --- THE SHIMS THE MODEL MAPS HOLD THIS THROUGH. ---

    /// The address this record holds, as the holder every container stores.
    ///
    /// This exists so a map whose value became a record does not move its call sites: a reader
    /// that had `map.get(key)` and wanted an address still gets one, in one call.
    ///
    /// # IT PANICS ON THE HANDLE ARM, AND THAT IS DELIBERATE
    ///
    /// A record whose address word is a handle names a multi-page layout, and there is no single
    /// address to hand back -- the layout holds them. Returning a DEFAULT address instead would
    /// resolve the key to slab 0, offset 0, which is a perfectly legal address for a different
    /// block, and a read that followed it would return the wrong bytes with no error anywhere.
    /// That is the same argument [`ElementEntry::set_block_id`] makes for panicking rather than
    /// narrowing, and the same one [`crate::block_store::make_block_address_word`] makes for
    /// checking rather than saturating.
    ///
    /// It is unreachable for the maps that hold this today. `strings` and `control_state_blocks`
    /// store exactly ONE address per object -- that is what their `ElementEntry` value meant --
    /// so every record in them is in the inline arm by construction, and
    /// `every_record_in_a_single_address_map_is_in_the_inline_arm` holds that rather than trusting
    /// it. A caller that genuinely might hold a layout should match on
    /// [`ObjectRecord::page_address`] instead, which cannot panic.
    pub(super) fn address(&self) -> ElementEntry {
        match self.try_address() {
            Ok(address) => address,
            Err(refusal) => panic!(
                "a single-address map held a record with no single address: {refusal}. Match on \
                 `page_address()` instead if this caller can hold a multi-page layout."
            ),
        }
    }

    /// The same, as a `Result`, for a caller that cannot assume the inline arm.
    pub(super) fn try_address(&self) -> Result<ElementEntry, ObjectRecordRefusal> {
        let word = match self.page_address() {
            PageAddress::One(word) => word,
            PageAddress::Many(handle) => {
                return Err(ObjectRecordRefusal::LayoutHandleWithNoLayout { handle })
            }
        };
        ElementEntry::try_from_wire_parts(
            u64::from(extract_block_slab_id(word)),
            u64::from(extract_block_offset(word)),
            u64::from(self.page_size),
            (!self.flags.get(RecordFlags::PAGE_IN_LOG)).then_some(0u64),
            self.flags.get(RecordFlags::PAGE_GENERATION),
        )
        .map_err(|_| ObjectRecordRefusal::AddressWordOutOfRange { word })
    }

    /// Replace ONLY the address, leaving every other field of the record where it was.
    ///
    /// # WHY THIS IS A NARROW DOOR RATHER THAN A WHOLE-RECORD ASSIGNMENT
    ///
    /// The compactor rewrites addresses in place: it takes `&mut ElementEntry` and moves blocks
    /// between slabs. A map holding records has to serve that by materialising an address, letting
    /// it be rewritten, and writing it back -- and the obvious way to write it back is to build a
    /// fresh record from the new address. THAT SILENTLY DROPS EVERYTHING ELSE: the deadline, the
    /// dirty bits, the model id.
    ///
    /// And it would not fail today. The deadline and dirty slots are present and unused on the
    /// maps that hold this, so a fresh-record write-back passes every test there is, and breaks
    /// later, in a different change, when something starts filling them -- with nothing pointing
    /// back here.
    ///
    /// So the write-back door is this one, and it is narrow BY CONSTRUCTION rather than by a
    /// caller remembering: it writes the address word, the page size, and the two bits that are
    /// part of what an address MEANS (whether an ordinal is present, and whether a generation is).
    /// It cannot reach the deadline word, the deadline tags, `DIRTY`, `INDEX_DIRTY`, `LOADING`,
    /// `IN_MEMORY`, `PAGE_DIRTY`, `PAGE_DELETED`, or the model id. A careless caller gets the
    /// right answer.
    ///
    /// `compacting_a_record_keeps_everything_that_is_not_the_address` is the guard, and it asserts
    /// the surviving state and the tracking state SEPARATELY -- because a test that compared whole
    /// records would fail for the wrong reason, and a test that asserted "nothing changed" would
    /// be wrong about the two bits that are supposed to change.
    pub(super) fn set_address(&mut self, address: &ElementEntry) {
        self.set_page_address(PageAddress::One(address.address_word()));
        // NOT SATURATED. `ElementEntry::length` is a `u32` field and its accessor widens, so a
        // value that does not fit here cannot exist -- and if one ever does, the honest answer is
        // to say so rather than to record a shorter page than the one that was written. A
        // saturated length reads as broken; a truncated one reads as a valid shorter block.
        self.page_size = u32::try_from(address.length()).unwrap_or_else(|_| {
            panic!(
                "an address length of {} does not fit the record's page size, which cannot happen \
                 while `ElementEntry` holds its length in a u32",
                address.length()
            )
        });
        self.flags.set(RecordFlags::PAGE_IN_LOG, address.block_id().is_none());
        self.flags.set(RecordFlags::PAGE_GENERATION, address.generation().is_some());
        // COUNTED, BECAUSE THE VALUE IS DROPPED HERE AND NOTHING ELSE SAYS SO.
        //
        // The inline arm keeps the ordinal's PRESENCE, set just above, and has no room for its
        // value -- correct for a single-page object, which has page zero by construction. A
        // NONZERO ordinal is therefore silently lost at this line, while the conversion path
        // refuses the same condition by name as `NonZeroPageOrdinalDoesNotFitInline`. Two halves
        // of one contract disagreeing, and no test fed either path a nonzero ordinal, so nothing
        // could tell.
        //
        // Not made fatal and not made to skip: the destination for a multi-page object does not
        // exist yet, so a fatal refusal would make a recovery path unopenable and a skip would
        // drop the page's address -- the same loss by another mechanism. See
        // `dropped_nonzero_page_ordinals` for the whole argument, and `try_set_address` for the
        // refusing door the callers move to once such an object has somewhere to go.
        if address.block_id().is_some_and(|block_id| block_id != 0) {
            DROPPED_NONZERO_PAGE_ORDINALS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }

    // --- THE CONVERSION. This is what proves the merge is real. ---

    /// Build the record that stands in for one object's separate rows.
    ///
    /// `address` is the row the model map holds today, `deadline` the row the deadline map holds,
    /// and `dirty` the row the dirty index holds. The model id is the fact the map's identity
    /// carries today.
    ///
    /// # WHAT IT REFUSES, AND WHY THE MEASUREMENT CHANGED THE ANSWER
    ///
    /// An [`ElementEntry`] carries a sixteen-bit page ordinal beside its address word, and the
    /// inline arm has no room for sixteen more bits: twenty-three of twenty-four bytes are spoken
    /// for and two more would push the structure to thirty-two. The first draft of this function
    /// therefore refused EVERY entry carrying an ordinal -- and measured against a real store that
    /// was 400 of 400 live string addresses, which would have sent every object through the
    /// side-allocating arm and made this record cost memory rather than save it.
    ///
    /// THE ORDINAL'S VALUE IS ZERO FOR A SINGLE-PAGE OBJECT, which is what the measurement
    /// actually said: 400 of 400 carried ordinal 0, the largest seen being 0. The ordinal names
    /// WHICH PAGE of the object this is, so an object with one page has page zero by construction.
    /// What the inline arm needs is the ordinal's PRESENCE -- a bit it already spends as
    /// `PAGE_IN_LOG`, since an absent ordinal is exactly what log-residence means -- and not its
    /// value.
    ///
    /// So a NONZERO ordinal is what is refused, and a nonzero ordinal means a multi-page object,
    /// which belongs in the layout arm anyway: [`ObjectRecord::from_object_rows_with_layout`]
    /// holds it exactly. The refusal is an `Err` and not a silent drop, because a dropped page
    /// ordinal resolves a key to a different page of the same object and reads the wrong bytes
    /// with no error anywhere.
    pub(super) fn from_object_rows(
        address: &ElementEntry,
        model_id: u8,
        deadline: Option<u64>,
        dirty: bool,
    ) -> Result<Self, ObjectRecordRefusal> {
        let block_id = address.block_id();
        if let Some(ordinal) = block_id {
            if ordinal != 0 {
                return Err(ObjectRecordRefusal::NonZeroPageOrdinalDoesNotFitInline {
                    block_id: ordinal,
                });
            }
        }
        let length = address.length();
        let page_size = u32::try_from(length)
            .map_err(|_| ObjectRecordRefusal::PageSizeDoesNotFit { length })?;

        let mut record = Self {
            deadline: 0,
            address: 0,
            page_size,
            flags: RecordFlags::empty(),
            model_id,
        };
        record.set_page_address(PageAddress::One(address.address_word()));
        record.set_deadline(match deadline {
            None => Deadline::None,
            Some(at) => Deadline::At(at),
        });
        // `block_id()` is `None` exactly while the block lives in the log, which is the one
        // derivation this type keeps rather than restates: see `ElementEntry::log_resident`. So
        // this ONE BIT carries the ordinal's presence, and the inline arm's ordinal value is zero.
        record.flags.set(RecordFlags::PAGE_IN_LOG, block_id.is_none());
        record.flags.set(RecordFlags::PAGE_GENERATION, address.generation().is_some());
        record.flags.set(RecordFlags::DIRTY, dirty);
        Ok(record)
    }

    /// The same, for an entry whose page identity the inline arm cannot hold.
    ///
    /// Returns the record -- whose address word is a HANDLE -- and the layout the handle names.
    /// This arm is lossless for any entry, at the cost of the side allocation the handle points
    /// into.
    pub(super) fn from_object_rows_with_layout(
        address: &ElementEntry,
        model_id: u8,
        deadline: Option<u64>,
        dirty: bool,
        handle: PageLayoutHandle,
    ) -> Result<(Self, PageLayout), ObjectRecordRefusal> {
        let length = address.length();
        let page_size = u32::try_from(length)
            .map_err(|_| ObjectRecordRefusal::PageSizeDoesNotFit { length })?;
        let block_id = match address.block_id() {
            None => None,
            Some(id) => Some(
                u16::try_from(id)
                    .map_err(|_| ObjectRecordRefusal::PageIdentityOutOfRange { block_id: id })?,
            ),
        };
        let layout = PageLayout {
            pages: vec![StoredPage {
                address: address.address_word(),
                length: page_size,
                block_id: block_id.unwrap_or_default(),
                has_block_id: block_id.is_some(),
                has_generation: address.generation().is_some(),
            }],
        };
        let mut record = Self {
            deadline: 0,
            address: 0,
            page_size,
            flags: RecordFlags::empty(),
            model_id,
        };
        record.set_page_address(PageAddress::Many(handle));
        record.set_deadline(match deadline {
            None => Deadline::None,
            Some(at) => Deadline::At(at),
        });
        record.flags.set(RecordFlags::PAGE_IN_LOG, block_id.is_none());
        record.flags.set(RecordFlags::PAGE_GENERATION, address.generation().is_some());
        record.flags.set(RecordFlags::DIRTY, dirty);
        Ok((record, layout))
    }

    /// Put the separate rows back, so the conversion can be shown to lose nothing.
    ///
    /// `layout` is consulted only when the address word is a handle, and its ABSENCE in that case
    /// is refused by name rather than papered over with a default address.
    pub(super) fn into_object_rows(
        &self,
        layout: Option<&PageLayout>,
    ) -> Result<(ElementEntry, u8, Option<u64>, bool), ObjectRecordRefusal> {
        let (word, length, block_id, has_generation) = match self.page_address() {
            // THE INLINE ARM'S ORDINAL IS ZERO WHEN IT IS PRESENT AT ALL, and `PAGE_IN_LOG`
            // carries that presence: set means no ordinal, clear means ordinal zero. See
            // `from_object_rows` for the measurement that settled this.
            PageAddress::One(word) => (
                word,
                u64::from(self.page_size),
                (!self.flags.get(RecordFlags::PAGE_IN_LOG)).then_some(0u64),
                self.flags.get(RecordFlags::PAGE_GENERATION),
            ),
            PageAddress::Many(handle) => {
                let layout =
                    layout.ok_or(ObjectRecordRefusal::LayoutHandleWithNoLayout { handle })?;
                let page = layout
                    .pages
                    .first()
                    .ok_or(ObjectRecordRefusal::LayoutHandleWithNoLayout { handle })?;
                (
                    page.address,
                    u64::from(page.length),
                    page.has_block_id.then_some(u64::from(page.block_id)),
                    page.has_generation,
                )
            }
        };
        let entry = ElementEntry::try_from_wire_parts(
            u64::from(extract_block_slab_id(word)),
            u64::from(extract_block_offset(word)),
            length,
            block_id,
            has_generation,
        )
        .map_err(|_| ObjectRecordRefusal::AddressWordOutOfRange { word })?;
        let deadline = match self.deadline() {
            Deadline::None => None,
            Deadline::At(at) => Some(at),
            Deadline::PerElement(handle) => {
                return Err(ObjectRecordRefusal::PerElementDeadlineHasNoSingleValue { handle })
            }
        };
        Ok((entry, self.model_id, deadline, self.flags.get(RecordFlags::DIRTY)))
    }
}

/// The pages of a multi-page object, named by a record whose address word is a handle.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct PageLayout {
    pages: Vec<StoredPage>,
}

impl PageLayout {
    pub(super) fn len(&self) -> usize {
        self.pages.len()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.pages.is_empty()
    }
}

/// One page inside a layout: everything the inline arm cannot hold, held exactly.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct StoredPage {
    address: u64,
    length: u32,
    block_id: u16,
    has_block_id: bool,
    has_generation: bool,
}

// =================================================================================================
// THE LAYOUTS A HANDLE NAMES
// =================================================================================================

/// Where the pages of a multi-page object actually live.
///
/// THIS IS THE PIECE WHOSE ABSENCE MADE THE HANDLE ARM DEAD CODE. `PageAddress::Many` and
/// `from_object_rows_with_layout` have existed and been tested for some time, and
/// `RecordFlags::ADDRESS_IS_POINTER` with them -- but every production mention of all three was
/// inside this file, because a handle is only a number until something maps it to a layout. The
/// record keeps ONE WORD that is either a page address or a handle; this is what the handle half
/// points into, so an object's pages hang off the object's own record instead of being filed in a
/// second index keyed by the object key again.
///
/// # HANDLE ZERO IS NEVER A LAYOUT, AND THAT IS LOAD-BEARING
///
/// `PageLayoutHandle` derives `Default`, so a zeroed or defaulted handle is `0`. If slot zero
/// backed handle zero, a record that never had a layout would resolve to whatever landed in the
/// first slot. So a slot at index `i` backs handle `i + 1` in the low half, and zero resolves to
/// nothing by construction rather than by a check someone has to remember.
///
/// # A STALE HANDLE FAILS CLOSED RATHER THAN NAMING SOMEONE ELSE'S PAGES
///
/// Vacated slots are reused, which on its own is the ABA problem: free a layout, allocate another
/// into the same slot, and the first handle now reads the second object's pages -- silently, and
/// as a plausible answer. The high half of the handle therefore carries the slot's GENERATION,
/// bumped on every removal, so a handle outliving its layout stops resolving instead of resolving
/// to the wrong thing. A raw pointer cannot express this; a handle can, for the cost of bookkeeping
/// in the half of the word that was going spare.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct PageLayoutStore {
    slots: Vec<LayoutSlot>,
    vacant: Vec<u32>,
    live: usize,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct LayoutSlot {
    generation: u32,
    layout: Option<PageLayout>,
}

impl PageLayoutStore {
    /// Split a handle into the slot it names and the generation it was issued at.
    ///
    /// `None` for handle zero, which names no slot -- see the type's doc.
    fn decode(handle: PageLayoutHandle) -> Option<(usize, u32)> {
        let word = handle.get();
        let low = (word & 0xFFFF_FFFF) as u32;
        let index = low.checked_sub(1)?;
        Some((index as usize, (word >> 32) as u32))
    }

    fn encode(index: usize, generation: u32) -> PageLayoutHandle {
        let low = u64::from(index as u32 + 1);
        PageLayoutHandle::from_stored((u64::from(generation) << 32) | low)
    }

    /// File a layout and return the handle a record should carry.
    pub(super) fn insert(&mut self, layout: PageLayout) -> PageLayoutHandle {
        self.live += 1;
        match self.vacant.pop() {
            Some(index) => {
                let slot = &mut self.slots[index as usize];
                slot.layout = Some(layout);
                Self::encode(index as usize, slot.generation)
            }
            None => {
                let index = self.slots.len();
                self.slots.push(LayoutSlot { generation: 0, layout: Some(layout) });
                Self::encode(index, 0)
            }
        }
    }

    /// The layout this handle names, or `None` if it names nothing any more.
    pub(super) fn get(&self, handle: PageLayoutHandle) -> Option<&PageLayout> {
        let (index, generation) = Self::decode(handle)?;
        let slot = self.slots.get(index)?;
        if slot.generation != generation {
            return None;
        }
        slot.layout.as_ref()
    }

    /// Give up a layout. The slot is reused, and its generation moves so the old handle cannot.
    pub(super) fn remove(&mut self, handle: PageLayoutHandle) -> Option<PageLayout> {
        let (index, generation) = Self::decode(handle)?;
        let slot = self.slots.get_mut(index)?;
        if slot.generation != generation {
            return None;
        }
        let layout = slot.layout.take()?;
        // SATURATING, so a slot recycled 2^32 times stops reusing rather than wrapping its
        // generation back onto a handle someone might still hold.
        slot.generation = slot.generation.saturating_add(1);
        if slot.generation != u32::MAX {
            self.vacant.push(index as u32);
        }
        self.live -= 1;
        Some(layout)
    }

    /// How many layouts are filed.
    pub(super) fn len(&self) -> usize {
        self.live
    }

    pub(super) fn is_empty(&self) -> bool {
        self.live == 0
    }
}

// =================================================================================================
// THE WIRE FORM, AND A DECODER THAT REFUSES BY NAME
// =================================================================================================

/// The stored spelling. Short keys because this is one row per object and there are many of them.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct ObjectRecordWire {
    #[serde(rename = "a")]
    address: u64,
    #[serde(rename = "n", default)]
    page_size: u32,
    #[serde(rename = "f", default)]
    flags: u16,
    #[serde(rename = "m", default)]
    model_id: u8,
    /// ABSENT RATHER THAN ZERO when there is no deadline, which is the wire half of the same
    /// argument the tag bit makes in memory: a stored zero would be a legal instant.
    #[serde(rename = "t", default, skip_serializing_if = "Option::is_none")]
    deadline: Option<u64>,
}

impl From<ObjectRecord> for ObjectRecordWire {
    fn from(record: ObjectRecord) -> Self {
        Self {
            address: record.address,
            page_size: record.page_size,
            flags: record.flags.bits(),
            model_id: record.model_id,
            deadline: record
                .flags
                .get(RecordFlags::DEADLINE_PRESENT)
                .then_some(record.deadline),
        }
    }
}

impl TryFrom<ObjectRecordWire> for ObjectRecord {
    type Error = ObjectRecordRefusal;

    /// REFUSES, AS AN `Err` AND NEVER AS A PANIC.
    ///
    /// This decoder runs on the load path. A panic there makes a store UNOPENABLE, where a refusal
    /// falls back to replaying the operation log and rebuilding the index correctly. So every
    /// disagreement below returns a named error, and none of them asserts.
    fn try_from(wire: ObjectRecordWire) -> Result<Self, Self::Error> {
        let reserved = wire.flags & RecordFlags::RESERVED;
        if reserved != 0 {
            return Err(ObjectRecordRefusal::ReservedFlagBitsSet { bits: reserved });
        }
        let flags = RecordFlags(wire.flags);
        // THE TAG AND THE VALUE MUST AGREE, IN BOTH DIRECTIONS. A tag with no value would read
        // whatever zero means, and a value with no tag would be invisible to every reader.
        let tagged = flags.get(RecordFlags::DEADLINE_PRESENT);
        match (tagged, wire.deadline) {
            (true, None) => {
                return Err(ObjectRecordRefusal::DeadlineTaggedButAbsentOnTheWire);
            }
            (false, Some(value)) => {
                return Err(ObjectRecordRefusal::DeadlineOnTheWireButNotTagged { value });
            }
            _ => {}
        }
        if !tagged && flags.get(RecordFlags::DEADLINE_IS_POINTER) {
            return Err(ObjectRecordRefusal::DeadlinePointerWithoutPresence);
        }
        Ok(Self {
            deadline: wire.deadline.unwrap_or_default(),
            address: wire.address,
            page_size: wire.page_size,
            flags,
            model_id: wire.model_id,
        })
    }
}

/// Every way this type refuses, each NAMED, so a log line says which one fired.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum ObjectRecordRefusal {
    /// A writer newer than this reader set a bit this reader does not understand.
    ReservedFlagBitsSet { bits: u16 },
    /// The deadline tag is set and the wire carries no deadline.
    DeadlineTaggedButAbsentOnTheWire,
    /// The wire carries a deadline and the tag is clear, so no reader would consult it.
    DeadlineOnTheWireButNotTagged { value: u64 },
    /// The pointer tag is set while the presence bit is clear, which names no arm.
    DeadlinePointerWithoutPresence,
    /// The inline arm carries a page ordinal's presence but not a nonzero value. Use the layout
    /// arm, which is where a multi-page object belongs regardless.
    NonZeroPageOrdinalDoesNotFitInline { block_id: u64 },
    /// A page identity wider than the sixteen bits the stored form accepts.
    PageIdentityOutOfRange { block_id: u64 },
    /// A page longer than the stored size field can name.
    PageSizeDoesNotFit { length: u64 },
    /// The address word is a handle and no layout was supplied for it.
    LayoutHandleWithNoLayout { handle: PageLayoutHandle },
    /// A per-element deadline list has no single value to hand back.
    PerElementDeadlineHasNoSingleValue { handle: DeadlineListHandle },
    /// The address word does not decode to an address this store can name.
    AddressWordOutOfRange { word: u64 },
}

impl std::fmt::Display for ObjectRecordRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ReservedFlagBitsSet { bits } => write!(
                f,
                "ReservedFlagBitsSet: the stored record sets reserved flag bits {bits:#06x}, so it \
                 was written by a newer reader; refusing rather than reading it with those bits' \
                 meaning dropped"
            ),
            Self::DeadlineTaggedButAbsentOnTheWire => write!(
                f,
                "DeadlineTaggedButAbsentOnTheWire: the record claims a deadline and carries none"
            ),
            Self::DeadlineOnTheWireButNotTagged { value } => write!(
                f,
                "DeadlineOnTheWireButNotTagged: the record carries deadline {value} with its \
                 presence bit clear, so no reader would consult it"
            ),
            Self::DeadlinePointerWithoutPresence => write!(
                f,
                "DeadlinePointerWithoutPresence: the deadline pointer tag is set while the presence \
                 bit is clear, which names no arm"
            ),
            Self::NonZeroPageOrdinalDoesNotFitInline { block_id } => write!(
                f,
                "NonZeroPageOrdinalDoesNotFitInline: page ordinal {block_id} has no room in the \
                 inline arm's twenty-four bytes, which carry the ordinal's presence and an implied \
                 value of zero; a nonzero ordinal is a multi-page object and belongs in the layout \
                 arm"
            ),
            Self::PageIdentityOutOfRange { block_id } => write!(
                f,
                "PageIdentityOutOfRange: page identity {block_id} is wider than the stored form \
                 accepts; truncating it would name a different page of the same object"
            ),
            Self::PageSizeDoesNotFit { length } => write!(
                f,
                "PageSizeDoesNotFit: page length {length} is wider than the stored size field"
            ),
            Self::LayoutHandleWithNoLayout { handle } => write!(
                f,
                "LayoutHandleWithNoLayout: the address word is handle {} and no layout was supplied",
                handle.get()
            ),
            Self::PerElementDeadlineHasNoSingleValue { handle } => write!(
                f,
                "PerElementDeadlineHasNoSingleValue: deadline list {} holds one deadline per \
                 element, so there is no single value to hand back",
                handle.get()
            ),
            Self::AddressWordOutOfRange { word } => write!(
                f,
                "AddressWordOutOfRange: address word {word:#018x} does not decode to an address \
                 this store can name"
            ),
        }
    }
}

impl std::error::Error for ObjectRecordRefusal {}

/// Rebuild an address word from its halves, for a caller holding a slab and an offset.
///
/// Here rather than at a call site because the packing is the stored shape's, not this type's.
pub(super) fn address_word_from_parts(block_slab_id: u32, offset: u32) -> u64 {
    make_block_address_word(block_slab_id, offset)
}

// =================================================================================================
// THE SLOT: ONE RECORD AND, WHEN THERE IS MORE THAN ONE ELEMENT, ONE OWNED ARRAY
// =================================================================================================

/// What a consolidated per-kind map stores for one object: the fixed-width record, plus an owned
/// element array for the objects that have more than one element.
///
/// # WHY THE PAYLOAD SITS BESIDE THE RECORD AND NOT INSIDE IT
///
/// Both shapes are thirty-two bytes. Only one of them keeps [`ObjectRecord`] `Copy`.
///
/// Putting `Option<Box<Vec<_>>>` *inside* the record would strip `Copy` from it, which turns every
/// implicit copy at every use site into a move or a clone. Those sites are invisible to a grep --
/// an implicit copy has no syntax -- so the only enumeration of them is a compiler error list, and
/// the width proofs the record carries (`size_of == 24`, `align_of == 8`, the field sum, and the
/// test that prints them) would all have to be restated at the same time. That is a blast-radius
/// cost, not a byte cost, and it buys nothing: the slot below is the same thirty-two bytes with the
/// record's width, its alignment, its `Copy` and all four of its proofs left exactly as they were.
///
/// # WHY THERE IS NO LAYOUT TAG BIT FOR "HAS A PAYLOAD"
///
/// `Option<Box<T>>` is self-describing. The null pointer is not a legal `Box`, so the language
/// carries the discrimination in the representation itself and `size_of::<Option<Box<T>>>()` is
/// eight, the same as the bare pointer. A flag bit saying "the payload is present" would therefore
/// be a SECOND source of truth for a fact the first one already answers exactly -- and this type's
/// own decoder refuses precisely that class of disagreement in four named variants, among them
/// [`ObjectRecordRefusal::DeadlineTaggedButAbsentOnTheWire`]. Adding a bit whose only job is to
/// agree with the niche would be manufacturing the defect those variants exist to catch.
///
/// This is not a reduction of the shape it follows. A C-style union is NOT self-describing, so a
/// design built on one must spend a byte on an explicit layout tag: the tag is the only thing that
/// can say which arm is live. The niche gives the same discrimination for ZERO bytes. The mechanism
/// is the same one; it is simply cheaper here, because the language pays for it.
///
/// Reserved flag bits are therefore spent only on a distinction the niche CANNOT express -- which
/// inline layout the address word is carrying -- and not on whether a payload exists at all.
///
/// # THE PAYLOAD IS ONE ALLOCATION CHAIN, NOT A SIDE TABLE
///
/// A sorted contiguous array is the right shape for this access pattern rather than a compromise
/// for it: these series are append-mostly timestamped key-value, where an array appends in
/// amortised O(1) and binary-searches in O(log N). Measured against the per-kind
/// `BTreeMap`-in-a-`HashMap` shape it replaces, at a thousand elements it is **49,120 B against
/// 24,056 B -- 2.042x -- in 170 allocations against 2**; the B-tree shape allocates a 280-byte
/// root node for a single element before any element byte is stored, and that node is paid per key
/// per kind. Owned per slot, there is no outer map to pay for and no second key copy.
///
/// `Box<Vec<_>>` rather than `Box<[_]>` on purpose: a boxed slice is a FAT pointer, sixteen bytes,
/// which would push this type to forty. The boxed `Vec` is a thin pointer at eight, and costs one
/// further indirection plus the `Vec` header -- measured at a flat sixteen heap bytes and one
/// allocation more than a single-buffer layout at every length tested, which is not a price worth
/// `unsafe` to avoid.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub(super) struct ObjectSlot {
    /// The fixed-width record, unchanged and still `Copy`.
    pub(super) record: ObjectRecord,
    /// The owned element array, for an object whose elements do not fit the record's inline arms.
    ///
    /// SKIPPED ON THE WIRE, AND THAT IS CORRECT RATHER THAN DEFERRED. No map stores elements here
    /// yet -- the first tenant holds exactly one record per key and no elements at all -- so there
    /// is nothing a stored form could carry, and `#[serde(transparent)]` over the one remaining
    /// field makes this type's stored image BYTE-IDENTICAL to the bare record's. That is what lets
    /// this land without moving the format stamp. The wire representation arrives with the first
    /// tenant that actually populates it, in the change that moves the stamp for other reasons.
    ///
    /// PRIVATE TO THIS MODULE, AND THAT IS THE ENFORCEMENT RATHER THAN A STYLE CHOICE. The
    /// only path that puts elements here is [`ObjectSlot::set_owned_elements`], and it
    /// demands a tenancy witness whose durable arm cannot be spelled -- see
    /// [`OwnedElementTenancy`]. A `pub(super)` field would let any engine module assign the
    /// array directly, and the witness would then be advice rather than a check.
    #[serde(skip)]
    many: Option<Box<Vec<(u64, ElementEntry)>>>,
}

/// THE WIDTH, PINNED AS ITS OWN EQUALITY, for the same reason the record's is: an inequality
/// against a neighbour passes at every width.
const _: () = assert!(std::mem::size_of::<ObjectSlot>() == 32);
const _: () = assert!(std::mem::align_of::<ObjectSlot>() == 8);

/// THE RECORD IS UNCHANGED BY ITS OWN NEIGHBOUR. Restated here rather than left to the record's
/// own asserts, because the whole argument for this shape is that wrapping the record did not cost
/// the record anything -- so the claim is asserted at the site that makes it.
const _: () = assert!(std::mem::size_of::<ObjectRecord>() == 24);
const _: () = assert!(std::mem::align_of::<ObjectRecord>() == 8);

/// THE SLOT IS THE RECORD PLUS ONE POINTER AND NOTHING ELSE -- no padding, no tag word. If a
/// future field lands between them this steps and says so, rather than hiding inside a width that
/// happened to stay put.
const _: () = assert!(
    std::mem::size_of::<ObjectSlot>()
        == std::mem::size_of::<ObjectRecord>()
            + std::mem::size_of::<Option<Box<Vec<(u64, ElementEntry)>>>>()
);

/// AND THE OWNING POINTER IS THIN. `Box<[T]>` is a fat pointer at sixteen bytes and would make the
/// slot forty; this asserts the eight the shape depends on, so a change of payload type that
/// silently widened it is a build failure and not a measurement surprise.
const _: () = assert!(std::mem::size_of::<Option<Box<Vec<(u64, ElementEntry)>>>>() == 8);

/// WHICH TENANT MAY PUT ELEMENTS IN THE OWNED ARRAY -- AND THE ARM THAT CANNOT BE SPELLED.
///
/// A tenant of the consolidated map registers a kind and then writes slots for it. Two tenancies
/// are possible, and they differ only in what a dump is expected to carry:
///
///   * `RebuiltAtLoad` -- the elements are re-derived from the bucket index on load, so an empty
///     stored image costs a rebuild and nothing else. Two of the eight candidate maps are this
///     shape: they are `skip_serializing` already, so nothing was going to be written for them.
///   * `DurableOnTheWire` -- the elements ARE the stored copy. The other six are this shape,
///     declared `#[serde(default)]` with no `skip_serializing`, and for them an empty stored
///     image is not a rebuild. It is the rows, gone.
///
/// The owned array has no stored form -- see `many` above, and the test named in the deletion
/// list below, which measures that rather than restating it. So the second tenancy is not merely
/// discouraged here, it is UNSPELLABLE: its witness field has type [`OwnedElementWireForm`],
/// which has no values, so there is nothing a call site can put in the hole and the registration
/// does not compile. Issue #2165.
///
/// # WHY A WITNESS, AND NOT A FLAG, A COMMENT OR A LIST OF KINDS
///
/// The hazard this exists for is that the lossy change LOOKS like the safe one. Both are "point a
/// context map at `objects` and write slots for it"; what separates them is one serde attribute
/// on a field the migration may well delete, and it is visible in neither `objects`' declaration
/// nor the candidate map's own attribute. A `durable: bool` parameter is satisfied by writing
/// `false`. A doc note is satisfied by not reading it. A hand-kept list of durable kinds goes
/// stale beside the call sites it describes, and then the guard passes because its list changed
/// rather than its subject. A type with no inhabitants is checked by the compiler, at the one
/// site where the decision is actually taken.
///
/// What it does not check is a tenant that claims `RebuiltAtLoad` while meaning the other thing.
/// That is why the arm carries its kind: the claim and the tenant it is made for sit together in
/// the diff, which is the half a reviewer can see and a compiler cannot.
pub(super) enum OwnedElementTenancy {
    /// The elements are re-derived at load, and the stored image is not expected to carry them.
    RebuiltAtLoad(StoredModelKind),
    /// The elements are the stored copy. UNCONSTRUCTIBLE while the array has no stored form.
    DurableOnTheWire {
        /// The tenant making the claim.
        kind: StoredModelKind,
        /// The hole with nothing to put in it.
        wire_form: OwnedElementWireForm,
    },
}

/// THE ABSENCE OF A STORED FORM, AS A TYPE WITH NO VALUES.
///
/// An uninhabited enum. The moment the owned array gains a stored form, this type gains an
/// inhabitant to describe it -- and that is the loud failure rather than a silent pass: the door
/// below discharges the durable arm with `match wire_form {}`, an empty match that compiles ONLY
/// over an uninhabited type. A variant added here makes that match non-exhaustive, so the build
/// fails with `E0004` NAMING the variant, at the door, instead of this guard quietly becoming
/// vacuous and being kept for the wrong reason.
///
/// A width assertion would not do this job. `size_of` is zero for an uninhabited enum AND zero
/// for a unit struct, so it would pass either way and prove nothing. Exhaustiveness is the
/// property, so exhaustiveness is what is asserted.
///
/// WHEN THAT HAPPENS, DELETE ALL OF THIS IN THE CHANGE THAT LANDS THE STORED FORM:
///   1. this type, and the `wire_form` field of `OwnedElementTenancy::DurableOnTheWire`;
///   2. the `match wire_form {}` arm in [`ObjectSlot::set_owned_elements`];
///   3. the pointer at `ShardState::objects`' declaration in `state.rs`;
///   4. `the_owned_element_array_reaches_no_stored_image`, which measures the premise and fails
///      when it goes.
/// Keeping the witness afterwards would refuse a registration that had become correct, and a
/// guard that fires on correct code gets deleted rather than obeyed.
pub(super) enum OwnedElementWireForm {}

impl ObjectSlot {
    /// A slot holding one record and no element array.
    pub(super) fn from_record(record: ObjectRecord) -> Self {
        Self { record, many: None }
    }

    /// The record, by value. It is `Copy`, which is the point of keeping the payload outside it.
    pub(super) fn record(&self) -> ObjectRecord {
        self.record
    }

    /// The record, mutably, for the callers that set flags or an address in place.
    pub(super) fn record_mut(&mut self) -> &mut ObjectRecord {
        &mut self.record
    }

    /// How many elements the owned array holds. Zero when there is no array, which is NOT the same
    /// statement as "the object has no elements": an object whose elements fit the record's inline
    /// arms carries them there and has no array at all. This counts the array.
    pub(super) fn owned_element_count(&self) -> usize {
        self.many.as_ref().map_or(0, |elements| elements.len())
    }

    /// THE OWNED ARRAY, READ-ONLY. The counterpart to [`ObjectSlot::set_owned_elements`] on the
    /// read side, and deliberately NOT a re-exposure of the field: `many` stays private, the
    /// write surface stays the one door that demands a tenancy witness, and a caller that holds
    /// `&[..]` cannot put an array here or reorder the one that is present.
    ///
    /// `None` and `Some(&[])` are DIFFERENT ANSWERS and that is why this returns the `Option`
    /// rather than a slice that flattens the absent arm to empty. An emptied array gives up its
    /// allocation, so "no points" has exactly one spelling -- `None` -- and a reader that could
    /// not tell the two apart is the second representation of one state this shape exists to
    /// avoid. [`ObjectSlot::owned_element_count`] answers the length question; this one answers
    /// the presence question and hands back the stored order.
    pub(super) fn owned_elements(&self) -> Option<&[(u64, ElementEntry)]> {
        self.many.as_deref().map(|elements| elements.as_slice())
    }

    /// THE ONLY DOOR INTO THE OWNED ELEMENT ARRAY, AND IT ASKS WHICH TENANCY IS BEING CLAIMED.
    ///
    /// `many` is private to this module, so this is the whole write surface for the array rather
    /// than the recommended one. A caller names its tenant and says whether the elements are
    /// re-derived at load or are themselves the stored copy; the second is unspellable while the
    /// array has no stored form. See [`OwnedElementTenancy`] and [`OwnedElementWireForm`].
    ///
    /// Sorted on the way in, which is what keeps the array's own access argument true: the type
    /// doc claims a sorted contiguous array that binary-searches in O(log N), and a door that
    /// accepted an arbitrary order would be the one place that claim stopped holding.
    pub(super) fn set_owned_elements(
        &mut self,
        tenancy: OwnedElementTenancy,
        mut elements: Vec<(u64, ElementEntry)>,
    ) {
        match tenancy {
            OwnedElementTenancy::RebuiltAtLoad(_) => {}
            // UNREACHABLE BECAUSE IT IS UNSPELLABLE, and this empty match is the proof rather
            // than a consequence of it: it compiles only while `OwnedElementWireForm` has no
            // values, so the day one arrives this line is what refuses to build.
            OwnedElementTenancy::DurableOnTheWire { wire_form, .. } => match wire_form {},
        }
        elements.sort_unstable_by_key(|(when, _)| *when);
        self.many = Some(Box::new(elements));
    }
}

impl From<ObjectRecord> for ObjectSlot {
    fn from(record: ObjectRecord) -> Self {
        Self::from_record(record)
    }
}

/// THE THREE THE MAPS ACTUALLY CALL, DELEGATED RATHER THAN REACHED THROUGH.
///
/// Every one of these forwards to the record and adds nothing. They exist so that wrapping the
/// record did not become a rewrite of its readers: a site that said `slot.address()` before the
/// payload field existed says exactly that afterwards. The alternative -- making each of the five
/// call sites spell `.record().address()` -- would have put the wrapper in the middle of code that
/// has no interest in it, and would grow with every tenant rather than staying at five.
///
/// They are also the seam the consolidated map wants: a reader asks the SLOT where the object
/// lives, and whether that answer comes from an inline arm of the record or from the owned array
/// is the slot's business and not the caller's.
impl ObjectSlot {
    /// Where the object's single page lives. Panics on the handle arm, exactly as the record does
    /// and for the record's own stated reason -- see [`ObjectRecord::address`].
    pub(super) fn address(&self) -> ElementEntry {
        self.record.address()
    }

    /// The same question as an `Err` rather than a panic, for the load path.
    pub(super) fn try_address(&self) -> Result<ElementEntry, ObjectRecordRefusal> {
        self.record.try_address()
    }

    /// Point the slot at a page. Touches only what the record's own setter touches; the owned
    /// element array is untouched, which is the whole reason it is a separate field.
    pub(super) fn set_address(&mut self, address: &ElementEntry) {
        self.record.set_address(address);
    }
}

// =================================================================================================
// THE SLOT AS A TIMESTAMPED SERIES
// =================================================================================================

/// THE OWNED ARRAY, READ AND WRITTEN AS A TIMESTAMPED SERIES.
///
/// This is what the owned array was built for: a per-element kind's points live in `many`, one
/// contiguous allocation per object, instead of a `BTreeMap` of its own per object key.
///
/// # WHY THE ARRAY AND NOT A B-TREE, MEASURED RATHER THAN ARGUED
///
/// Marginal cost of one key with N points, against the `BTreeMap`-inside-a-`HashMap` shape this
/// replaces -- measured on the counting allocator, and independently in a standalone harness that
/// agreed to three significant figures:
///
/// | N     | per-kind map        | owned array     | ratio |
/// |-------|---------------------|-----------------|-------|
/// | 1     | 312 B / 4 allocs    | 48 B / 2        | 6.50x |
/// | 100   | 5,080 B / 20        | 2,424 B / 2     | 2.10x |
/// | 1,000 | 49,120 B / 170      | 24,056 B / 2    | 2.04x |
/// | 3,200 | 156,192 B / 535     | 76,824 B / 2    | 2.03x |
///
/// The single-point row decomposes to one allocation: a `BTreeMap` holding ONE element allocates a
/// 280-byte root node, paid per key per kind before a single element byte is stored.
///
/// And the array is FIXED WIDTH AND KEPT SORTED, which is not a detail. #2122 measured that a
/// variable-length, linear-scan payload is 3.0-17.8x worse on misses, so the compactness that makes
/// such a buffer small is also what stops it being binary-searchable. `(u64, ElementEntry)` is
/// twenty-four fixed bytes, so [`TimestampedSeries::get`] binary-searches in O(log N) and
/// [`TimestampedSeriesWrite::set`] appends in amortised O(1) at the tail, which is where an
/// append-mostly series writes.
///
/// # THE POINTS LIVE ONLY IN `many`, AND THAT IS DELIBERATE FOR THE FIRST TENANT
///
/// The record's inline address arm is NOT consulted here. A series could in principle keep its
/// single point inline and spend no allocation at all, and that would beat the 48 B row above --
/// but it would make every read merge two sources, and a reader that disagreed with a writer about
/// which source held a point is precisely the divergence class this campaign keeps finding. One
/// source of truth for the first tenant; the inline arm is a measured follow-up, not a
/// precondition, because every figure in the table above is already an owned-array figure.
impl super::timestamped_series::TimestampedSeries for ObjectSlot {
    fn len(&self) -> usize {
        self.many.as_ref().map_or(0, |points| points.len())
    }

    fn get(&self, at: u64) -> Option<&ElementEntry> {
        let points = self.many.as_ref()?;
        points
            .binary_search_by_key(&at, |(instant, _)| *instant)
            .ok()
            .map(|found| &points[found].1)
    }

    fn addresses(&self) -> impl Iterator<Item = &ElementEntry> {
        self.many
            .as_deref()
            .map(|points| points.as_slice())
            .unwrap_or(&[])
            .iter()
            .map(|(_, address)| address)
    }

    fn points(&self) -> impl Iterator<Item = (u64, &ElementEntry)> {
        self.many
            .as_deref()
            .map(|points| points.as_slice())
            .unwrap_or(&[])
            .iter()
            .map(|(instant, address)| (*instant, address))
    }

    fn points_mut(&mut self) -> impl Iterator<Item = (u64, &mut ElementEntry)> {
        self.many
            .as_deref_mut()
            .map(|points| points.as_mut_slice())
            .unwrap_or(&mut [])
            .iter_mut()
            .map(|(instant, address)| (*instant, address))
    }

    /// TWO BINARY SEARCHES AND A SLICE, which is why a window belongs on the trait at all: a
    /// sorted array answers it more cheaply than a tree does, so requiring it costs the trait
    /// nothing in generality. See the method's own note for the test this passes.
    ///
    /// `partition_point` answers the first index whose point FAILS the predicate, which is the
    /// insertion position -- so each bound becomes one search and the window is the slice between
    /// them. The `max(start)` keeps an inverted or empty window as an empty slice rather than a
    /// panic on a reversed range.
    fn range(
        &self,
        bounds: impl std::ops::RangeBounds<u64>,
    ) -> impl Iterator<Item = (u64, &ElementEntry)> {
        use std::ops::Bound;
        let points = self
            .many
            .as_deref()
            .map(|points| points.as_slice())
            .unwrap_or(&[]);
        let start = match bounds.start_bound() {
            Bound::Unbounded => 0,
            Bound::Included(&at) => points.partition_point(|(instant, _)| *instant < at),
            Bound::Excluded(&at) => points.partition_point(|(instant, _)| *instant <= at),
        };
        let end = match bounds.end_bound() {
            Bound::Unbounded => points.len(),
            Bound::Included(&at) => points.partition_point(|(instant, _)| *instant <= at),
            Bound::Excluded(&at) => points.partition_point(|(instant, _)| *instant < at),
        };
        points[start..end.max(start)]
            .iter()
            .map(|(instant, address)| (*instant, address))
    }
}

impl super::timestamped_series::TimestampedSeriesWrite for ObjectSlot {
    /// Record an address at one instant, KEEPING THE ARRAY SORTED.
    ///
    /// The sort order is what [`TimestampedSeries::points`] promises and what
    /// [`TimestampedSeries::get`] binary-searches, so an unsorted insert would silently break both
    /// -- `get` would miss points that are present, which reads as a deletion.
    ///
    /// `binary_search_by_key` answers the insertion position on a miss, so the common case -- an
    /// append at or near the tail of an append-mostly series -- is a `Vec::insert` at the end,
    /// which is amortised O(1) and copies nothing.
    fn set(&mut self, at: u64, address: ElementEntry) {
        let points = self.many.get_or_insert_with(|| Box::new(Vec::new()));
        match points.binary_search_by_key(&at, |(instant, _)| *instant) {
            Ok(found) => points[found].1 = address,
            Err(position) => points.insert(position, (at, address)),
        }
    }

    /// Forget the point at one instant, if any.
    ///
    /// AN EMPTIED ARRAY GIVES UP ITS ALLOCATION rather than keeping a zero-length buffer, so that
    /// `many.is_none()` means "no points" with no second way to spell it. A slot holding
    /// `Some(empty)` would answer `len() == 0` and still carry a heap allocation, which is both a
    /// cost and a second representation of one state.
    fn clear(&mut self, at: u64) {
        let Some(points) = self.many.as_deref_mut() else {
            return;
        };
        if let Ok(found) = points.binary_search_by_key(&at, |(instant, _)| *instant) {
            points.remove(found);
        }
        if points.is_empty() {
            self.many = None;
        }
    }
}

// =================================================================================================
// THE WRITE-BACK DOOR AND THE ORDINAL IT USED TO DROP IN SILENCE
// =================================================================================================

/// HOW MANY TIMES THE WRITE-BACK DOOR WAS HANDED A PAGE ORDINAL IT CANNOT HOLD.
///
/// `ObjectRecord::set_address` writes the inline arm, which keeps a page ordinal's PRESENCE and
/// not its value -- correct, and measured: a single-page object has page zero by construction, and
/// 400 of 400 live addresses on a real store carried ordinal 0. A nonzero ordinal belongs in the
/// layout arm.
///
/// The conversion path refuses such an entry by name, as
/// [`ObjectRecordRefusal::NonZeroPageOrdinalDoesNotFitInline`]. The write-back door did not: it
/// wrote the presence bit and dropped the value, silently.
///
/// # WHY THIS IS A COUNTER AND NOT YET A REFUSAL
///
/// Making `set_address` refuse would be the obvious fix and is the wrong one TODAY, because the
/// correct destination for a multi-page object does not exist yet -- it arrives with the owned
/// element array. That leaves a refusing caller two options and both are worse than the bug:
///
///   * FATAL turns a recovery path into a store that cannot be opened, which this tree has twice
///     established as the worst outcome available on a load path.
///   * SKIP drops the page's address, which is silent data loss -- the same class as the
///     behaviour being fixed, reached by a different mechanism.
///
/// So what ships is OBSERVABILITY: today's behaviour, counted, with the count exposed and asserted
/// zero. It should never fire, because every map holding a record today is single-page. If it ever
/// does, we learn before it costs anything, and the counter's call site is where the routing to the
/// owned array will go.
///
/// A NAMED `TODO` WITH NO INSTRUMENT IS WHAT LET THIS SURVIVE. The asymmetry between the two paths
/// was documented in prose and contradicted by the code, and no test fed either path a nonzero
/// ordinal, so nothing could tell. A counter asserted zero is the instrument that prose was not.
///
/// [`ObjectRecord::try_set_address`] is the refusing door, reachable and tested now, for the
/// callers that will be able to handle it once there is somewhere for such an object to go.
static DROPPED_NONZERO_PAGE_ORDINALS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// How many nonzero page ordinals the write-back door has dropped.
pub(crate) fn dropped_nonzero_page_ordinals() -> u64 {
    DROPPED_NONZERO_PAGE_ORDINALS.load(std::sync::atomic::Ordering::Relaxed)
}

/// Forget the count, so a test measures its own writes rather than the suite before it.
pub(crate) fn reset_dropped_nonzero_page_ordinals() {
    DROPPED_NONZERO_PAGE_ORDINALS.store(0, std::sync::atomic::Ordering::Relaxed);
}

impl ObjectRecord {
    /// Point the record at a page, REFUSING an address the inline arm cannot hold faithfully.
    ///
    /// This is [`ObjectRecord::set_address`] with the refusal the conversion path already makes,
    /// by the same named variant, so the two halves of one contract agree. It exists now, ahead of
    /// any caller that can act on it, because a refusal that is unreachable is untestable -- and
    /// the defect this closes survived precisely because nothing ever drove the condition.
    ///
    /// Callers migrate to this once a multi-page object has a destination. Until then
    /// `set_address` keeps today's behaviour and counts.
    pub(super) fn try_set_address(
        &mut self,
        address: &ElementEntry,
    ) -> Result<(), ObjectRecordRefusal> {
        if let Some(block_id) = address.block_id() {
            if block_id != 0 {
                return Err(ObjectRecordRefusal::NonZeroPageOrdinalDoesNotFitInline {
                    block_id: u64::from(block_id),
                });
            }
        }
        self.set_address(address);
        Ok(())
    }
}

#[cfg(test)]
mod the_handle_arm_now_has_something_to_point_into {
    use super::{
        ElementEntry, ObjectRecord, PageAddress, PageLayout, PageLayoutHandle, PageLayoutStore,
    };

    fn an_entry() -> ElementEntry {
        ElementEntry::from_parts(7, 64, 128, Some(3), None)
    }

    /// Handle zero names nothing, and no issued handle is ever zero.
    ///
    /// `PageLayoutHandle` derives `Default`, so this is the property that keeps a record which
    /// never had a layout from reading whatever landed in the first slot.
    #[test]
    fn handle_zero_names_nothing_and_is_never_issued() {
        let mut store = PageLayoutStore::default();
        assert!(store.get(PageLayoutHandle::default()).is_none(), "the default handle resolved");
        assert!(store.get(PageLayoutHandle::from_stored(0)).is_none(), "handle zero resolved");

        for _ in 0..4 {
            let handle = store.insert(PageLayout::default());
            assert_ne!(0, handle.get(), "an issued handle must never be zero");
        }
        assert_eq!(4, store.len());
        assert!(store.get(PageLayoutHandle::from_stored(0)).is_none(), "handle zero resolved");
    }

    /// A record whose address word is a HANDLE gives its rows back unchanged through the store.
    ///
    /// This is the arm that existed and could not be reached: the record carries the handle, the
    /// store holds the layout, and nothing is lost in either direction.
    #[test]
    fn a_record_and_its_layout_round_trip_through_the_store() {
        let mut store = PageLayoutStore::default();
        let address = an_entry();

        // The handle has to be known before the record is built, which is why the store issues it
        // first and the layout is filed under the same handle afterwards.
        let reserved = store.insert(PageLayout::default());
        let (record, layout) =
            ObjectRecord::from_object_rows_with_layout(&address, 9, Some(1_700), true, reserved)
                .expect("the layout arm refused an entry it is documented to hold");
        assert!(
            matches!(record.page_address(), PageAddress::Many(_)),
            "the record should carry a handle, not an inline address"
        );

        // File the real layout in the slot the handle already names.
        let replaced = store.remove(reserved);
        assert!(replaced.is_some(), "the reserved slot should have held the placeholder");
        let handle = store.insert(layout);

        let held = store.get(handle).expect("the filed layout should resolve");
        let (back_address, back_model, back_deadline, back_dirty) = record
            .into_object_rows(Some(held))
            .expect("unfolding a record through its layout refused");
        assert_eq!(address, back_address, "the address row changed");
        assert_eq!(9, back_model, "the model id changed");
        assert_eq!(Some(1_700), back_deadline, "the deadline row changed");
        assert!(back_dirty, "the dirty bit changed");
    }

    /// A stale handle stops resolving rather than naming the next layout in its slot.
    ///
    /// AND THE REUSE IS ASSERTED, because without it this test passes for the wrong reason: if the
    /// second layout landed in a FRESH slot the ABA case would never be exercised and the stale
    /// handle would fail to resolve trivially. So the slot count is pinned at one.
    #[test]
    fn a_stale_handle_does_not_resolve_to_the_layout_that_replaced_it() {
        let mut store = PageLayoutStore::default();

        let first = store.insert(PageLayout::default());
        assert!(store.get(first).is_some());
        assert!(store.remove(first).is_some(), "the first layout should have been there");
        assert!(store.get(first).is_none(), "a removed handle still resolved");

        let second = store.insert(PageLayout::default());
        assert_eq!(
            1,
            store.slots.len(),
            "the vacated slot was NOT reused, so this test would not have exercised a stale handle"
        );
        assert_ne!(
            first.get(),
            second.get(),
            "the slot was reused and the handle did not change, so a stale handle is \
             indistinguishable from a live one"
        );
        assert!(
            store.get(first).is_none(),
            "the stale handle resolved to the layout that took its slot"
        );
        assert!(store.get(second).is_some(), "the live handle did not resolve");
        assert_eq!(1, store.len(), "one layout should be filed");
    }

    /// Removing a handle twice is not a second removal, and neither is removing a fabricated one.
    #[test]
    fn a_handle_that_names_nothing_removes_nothing() {
        let mut store = PageLayoutStore::default();
        let handle = store.insert(PageLayout::default());
        assert!(store.remove(handle).is_some());
        assert!(store.remove(handle).is_none(), "the same handle removed twice");
        assert!(
            store.remove(PageLayoutHandle::from_stored(9_999)).is_none(),
            "a fabricated handle removed something"
        );
        assert!(store.is_empty(), "the store should be empty");
        assert_eq!(0, store.len());
    }
}

#[cfg(test)]
mod what_moving_one_kinds_refs_onto_its_record_would_cost {
    use super::{PageLayout, PageLayoutStore, StoredPage};

    /// glibc, which is what production uses: the crate's only `#[global_allocator]` is behind
    /// `cfg(all(test, feature = "alloc-probe"))`.
    fn chunk(request: usize) -> usize {
        if request == 0 {
            return 0;
        }
        let rounded = (request + 8 + 15) / 16 * 16;
        if rounded < 32 { 32 } else { rounded }
    }

    /// BEFORE BUILDING IT: is an object's refs on its record cheaper than a lookup entry?
    ///
    /// The slice under consideration stops filing one kind in `ObjectBlockLookup` and hangs its
    /// block refs off the object's own record instead. That is their shape -- one node per key,
    /// everything else behind one word -- but shape is not a saving, and this campaign has refused
    /// seven routes that looked right and measured wrong. So the arithmetic goes first.
    ///
    /// The two sides, per object holding ONE block:
    ///
    ///   TODAY  a `BTreeMap<Arc<str>, BlockRefs>` entry under the kind. The key `Arc` is SHARED
    ///          with the page index and costs nothing extra here, so the entry is the node slot
    ///          plus the 24-byte `BlockRefs` inline in it.
    ///
    ///   MOVED  the record already exists in `objects` for this kind and already holds the key,
    ///          so the key is free on this side too. What is added is a store slot and a layout:
    ///          the layout's `Vec<StoredPage>` buffer, and the refs beside it.
    #[test]
    fn the_two_shapes_priced_side_by_side() {
        let pair = std::mem::size_of::<StoredPage>();
        let layout_inline = std::mem::size_of::<PageLayout>();
        let refs_inline = 24; // BlockRefs, pinned in state.rs

        // TODAY: a B-tree node holds up to 11 entries; a one-element map allocates a 280-byte root
        // and the steady-state occupancy of a leaf is about 5.8 of 11. Per entry at steady state:
        let node_bytes_per_entry = 288.0 / 5.8;
        let today = node_bytes_per_entry + refs_inline as f64;

        // MOVED: one store slot (generation + Option<PageLayout>, inline in the arena's Vec) plus
        // one exact-capacity page buffer. The arena's own Vec amortises, so per object it is the
        // slot inline plus the buffer chunk.
        let slot_inline = 4 + 4 + layout_inline; // generation, padding, the Option<PageLayout>
        let buffer = chunk(pair * 1);
        let moved = slot_inline as f64 + buffer as f64 + refs_inline as f64;

        println!();
        println!("  per object holding one block, chunk bytes:");
        println!("    StoredPage                {pair} B");
        println!("    PageLayout inline         {layout_inline} B");
        println!("    BlockRefs inline          {refs_inline} B");
        println!();
        println!("    TODAY  lookup entry       {today:>7.1} B  (node {node_bytes_per_entry:.1} + refs {refs_inline})");
        println!("    MOVED  record + layout    {moved:>7.1} B  (slot {slot_inline} + buffer {buffer} + refs {refs_inline})");
        println!("    delta                     {:>7.1} B  ({})", moved - today,
                 if moved < today { "MOVED is cheaper" } else { "MOVED is MORE EXPENSIVE" });
        println!();

        // The arena's growth is amortised, so confirm the slot really is inline rather than a
        // per-object allocation -- otherwise the figure above understates the moved side.
        let mut store = PageLayoutStore::default();
        for _ in 0..64 {
            store.insert(PageLayout::default());
        }
        assert_eq!(64, store.len(), "the arena did not take 64 layouts");

        // This test exists to PRICE the change, not to bless it. Assert only what was measured,
        // so the number is recorded and a later shape change has to re-run it rather than inherit
        // a claim.
        assert!(pair > 0 && layout_inline > 0, "the shapes have a size");
    }
}
