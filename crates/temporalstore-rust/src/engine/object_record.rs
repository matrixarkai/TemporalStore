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

// DEAD CODE IS THE POINT, FOR EXACTLY ONE COMMIT. Nothing in production holds an `ObjectRecord`
// yet: this type is built and tested standalone, ahead of the graft, so that the layout and the
// conversion can be proved before anything is rewired. Every item here is exercised by
// `engine::tests::one_record_per_object`, which is `cfg(test)`, so a non-test build sees eleven
// items with no caller and says so eleven times.
//
// The alternative to this attribute is eleven unexplained warnings on a branch, which is worse:
// a reader cannot tell them from the real thing. THE GRAFT COMMIT REMOVES THIS LINE -- once the
// record is the value of a live map, every item below has a production caller and a genuine dead
// item here should be reported again.
#![allow(dead_code)]

use serde::{Deserialize, Serialize};

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
