// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A CONTAINER BLOCK STATES WHICH ELEMENT IT HOLDS.
//!
//! Until now a container block carried a VALUE and nothing else. Which element of which object that
//! value belonged to was known only to the block index entry that named it -- `BlockIndex::element_name`
//! -- and to the object id folded into the address. The block itself said nothing, so the bytes were
//! only interpretable beside the entry that pointed at them.
//!
//! That is the one fact underneath four separate refutations, and it is worth stating plainly
//! because each of those looked like an independent dead end:
//!
//!   * the element name cannot leave the block entry, because for a hash the name IS the element and
//!     for a hash the entry is the only place it is written (#2009);
//!   * the resident maps cannot be rebuilt from anything but the block index, because the index is
//!     the only thing that names elements (#2016's arms skip an unnamed block for exactly this
//!     reason);
//!   * blocks cannot be batched usefully while the element name is folded into the address the
//!     write returns (#1999);
//!   * and the resident map cannot become authoritative while it is merged from a snapshot the
//!     index outranks (#2017).
//!
//! All four are downstream of the block not naming its own element. This module is where it starts
//! to, and on its own it BUYS NOTHING: it costs bytes on every container write and every reader
//! keeps answering exactly as it did. What it buys is that the element name now exists in a second
//! place, which is the precondition for it leaving the first.
//!
//! # THE SHAPE
//!
//! ```text
//!     magic            7 bytes   b"TSCPG1\n"
//!     key spelling     1 byte    how a key in this page spells its index element name
//!     item count       varint
//!     per item:
//!       key length     varint
//!       key            that many bytes
//!       value tag      varint    0 -> the value is a SUFFIX of the key, offset follows
//!                                n -> the value is the next n-1 bytes
//!       (offset)       varint    present only when the tag is 0
//!       (value)        bytes     present only when the tag is >= 1
//! ```
//!
//! ## WHY THE VALUE MAY BE A SUFFIX OF THE KEY, WHICH IS NOT A TRICK
//!
//! For a `set` the element key IS the member and the value IS the member -- #2017 measured that
//! `hex::decode(element_name)` equalled the block bytes on every one of sixty entries. Writing both
//! would make a set block carry its member twice, and a stage whose whole justification is that it
//! is a cheap foundation cannot afford to double the one kind where the redundancy was already
//! measured. For a `zset` the key is an eight-byte score followed by the member, so the value is
//! the key from byte eight. Both fall out of one rule, which the encoder detects rather than being
//! told: if the value equals the key's tail, store where the tail starts instead of storing it.
//!
//! An EMPTY value needs no case of its own -- the empty slice is the tail starting at the key's
//! end, so it encodes in two varints and decodes back to empty.
//!
//! ## WHY THE KEY SPELLING IS IN THE PAGE AND NOT IN THE SIGNATURE
//!
//! The obvious alternative was to key each item by the element name STRING, so that selecting an
//! element is a string compare and nothing has to know the kind. Measured on the shapes this store
//! actually writes, that costs a `set` block 42 bytes over its member and a `zset` block 58, because
//! an element name is hex and hex is two characters a byte. Carrying the key in its own bytes and one
//! byte saying how it spells costs 11 and 19. So the spelling byte is not generality for its own
//! sake; it is what keeps the foundation affordable for the two kinds whose keys are hex.
//!
//! It also means `read_block_bytes` needs no new argument. A block asked for one of its elements is
//! handed the element name it was looked up by, and this module turns that back into key bytes without
//! being told the kind -- so the twenty-odd call sites of the read funnel are untouched, and no
//! mechanical signature rewrite goes past the guards that read those call sites.
//!
//! ## AND THE SPELLING IS A ROUND TRIP, ASSERTED BOTH WAYS
//!
//! [`element_name_from_element_key`] and [`element_key_from_element_name`] are inverses, and that is the
//! whole safety argument for this module: the block's key and the index's element name are two
//! spellings of one value, so either can be derived from the other and a later stage may delete
//! whichever it no longer wants to store. If they were merely written consistently at the same
//! moment by the same call site -- which is how the element name and the set payload agree TODAY, and
//! nothing re-checks them -- then the second spelling would be an unverified copy rather than a
//! derivation. `container_block_element_key` drives the round trip over every spelling and over the
//! edge cases each one has (an empty member, a score of exactly sixteen characters with nothing
//! after it, a field name that is not valid UTF-8).
//!
//! EXCEPT ONE DIRECTION OF ONE SPELLING, SINCE THE ELEMENT NAME STOPPED SPELLING A ZSET'S SCORE.
//! `ScoreThenMember`'s `element_name_from_element_key` now drops a key's first eight bytes rather
//! than rendering them, so `element_key_from_element_name` cannot get them back and returns a
//! PLACEHOLDER there instead of the true score. The two are no longer inverses for this one
//! spelling -- `container_block_element_key` asserts the loss explicitly rather than asserting a
//! round trip that does not hold -- and the reason it is still safe is that nothing resolves a
//! `ScoreThenMember` key through `element_key_from_element_name` and then trusts its first eight
//! bytes: see `select_container_element`'s member-suffix comparison for that spelling.
//!
//! # WHAT DOES NOT MOVE
//!
//! No stored format is retired and no version stamp is spent. The magic DISCRIMINATES: a page
//! written before this module has no magic, [`decode_container_block`] answers `NotFramed`, and the
//! read funnel hands the bytes back exactly as it always did. That is the same trade
//! `encode_feature_block` already makes with `TSFPB1\n` against the JSON blocks that came before it,
//! and it is why this stage does not move `SHARD_INDEX_FORMAT_VERSION` at all: the SHARD INDEX
//! does not change shape here, and the block payload is self-describing rather than versioned.
//! The stamp stood at 2 when this was written and is 5 now, moved by later stages; this stage
//! is not one of them, and the number above is the one it had then rather than the one to read
//! off the constant today.
//!
//! A payload that legitimately begins with these seven bytes would be mistaken for a frame. That is
//! the same exposure the two feature magics carry, on the same kind of bytes, and it is noted here
//! rather than guarded because a guard would have to be a length or checksum the block record
//! already provides.

/// How a key inside a container page spells the element name its index entry is filed under.
///
/// One byte in the block, because the block has to be interpretable WITHOUT the entry that names it
/// -- that is the entire point of the module -- and the kind is not recoverable from the bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ElementKeySpelling {
    /// The element name is the key's bytes as UTF-8. A hash field.
    Utf8,
    /// The element name is `hex::encode(key)`. A set member.
    Hex,
    /// The element name is sixteen hex characters of the key's first EIGHT BYTES read big-endian,
    /// followed by `hex::encode` of everything after them. A zset's biased score and member.
    ScoreThenMember,
    /// The element name is sixteen hex characters of the key's eight bytes read big-endian, and the
    /// key is exactly eight bytes. A list sequence.
    BiasedWord,
    /// The element name is the key spelled by [`member_element_name`]. A set member, on a block written
    /// after the spelling moved off hex.
    ///
    /// WHY A NEW VARIANT RATHER THAN A REDEFINITION OF `Hex`: this byte is written INTO the block,
    /// so that the block is interpretable without the entry that names it. Redefining `Hex` would
    /// silently reinterpret every block already on disk. A new number leaves them alone and lets
    /// one store hold both spellings at once, which is what reading across the change requires.
    Name,
    /// As [`ElementKeySpelling::Name`] for the element name and as
    /// [`ElementKeySpelling::ScoreThenMember`] for the key: eight bytes of biased score then the
    /// member, with only the member spelled into the element name. A zset member.
    ScoreThenMemberName,
}

impl ElementKeySpelling {
    fn to_byte(self) -> u8 {
        match self {
            ElementKeySpelling::Utf8 => 0,
            ElementKeySpelling::Hex => 1,
            ElementKeySpelling::ScoreThenMember => 2,
            ElementKeySpelling::BiasedWord => 3,
            ElementKeySpelling::Name => 4,
            ElementKeySpelling::ScoreThenMemberName => 5,
        }
    }

    fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            0 => Some(ElementKeySpelling::Utf8),
            1 => Some(ElementKeySpelling::Hex),
            2 => Some(ElementKeySpelling::ScoreThenMember),
            3 => Some(ElementKeySpelling::BiasedWord),
            4 => Some(ElementKeySpelling::Name),
            5 => Some(ElementKeySpelling::ScoreThenMemberName),
            _ => None,
        }
    }

    /// The spelling the given stored kind uses, or `None` for a kind whose page is its whole
    /// object and carries no element key.
    ///
    /// Derived from the kind string the write sites already hold, in ONE place, so a fifth
    /// container kind cannot be given a block format by accident at a new call site.
    ///
    /// SET AND ZSET ANSWER THE TAGGED SPELLINGS NOW, and this one-line change in this one
    /// function is what moved every set and zset writer off hex at once -- which is the whole
    /// return on deriving the spelling here instead of at each site.
    ///
    /// HASH AND LIST ARE DELIBERATELY NOT MOVED, and not out of caution: neither has a doubling
    /// to remove. A hash field element name is the field text at one byte a byte already, and a list
    /// element name is sixteen characters for an eight-byte word whatever the element holds. Moving
    /// either would change bytes on disk and buy nothing, so they are left exactly as they are
    /// and the guard below asserts it.
    ///
    /// BLOCKS ALREADY ON DISK KEEP READING: they carry byte 1 or 2, `from_byte` still accepts
    /// both, and the `Hex | Name` arm of [`element_key_from_element_name`] takes either element name
    /// spelling. The other direction does NOT hold -- a binary from before the reader half reads
    /// a byte 4 or 5 block as `Corrupt` rather than as an absence -- which is why that half
    /// landed on its own first, and why this one cannot be backported without it.
    pub(super) fn for_kind(kind: &str) -> Option<Self> {
        match kind {
            "hash" => Some(ElementKeySpelling::Utf8),
            "set" => Some(ElementKeySpelling::Name),
            "zset" => Some(ElementKeySpelling::ScoreThenMemberName),
            "list" => Some(ElementKeySpelling::BiasedWord),
            _ => None,
        }
    }
}

/// Seven bytes that say a block payload holds items rather than one value.
///
/// THE FIRST SHAPE, WHICH STILL READS. Every item in it is present: it has no way to say an element
/// was removed, which is the whole reason there is a second shape. Kept because stores written by
/// #2022 and #2027 hold these blocks and a reader that could not walk them would lose their
/// elements -- see [`CONTAINER_BLOCK_MAGIC_V2`] for what changed and what it costs.
pub(super) const CONTAINER_BLOCK_MAGIC: &[u8] = b"TSCPG1\n";

/// Seven bytes that say a block payload holds items SOME OF WHICH MAY BE REMOVALS.
///
/// # WHY A SECOND MAGIC AND NOT A FLAG BYTE PER ITEM
///
/// A removal has to be a WRITTEN ITEM rather than an absence, because a block is never rewritten by
/// the removal path: #2027 folds blocks only when a round was already rewriting them, and a folded
/// block stays live for its other elements. #2028 drove what that costs -- twelve members folded to
/// one block, one removed, the index naming eleven and the block holding twelve -- so a membership
/// derived from blocks resurrected the removed member. This magic is where the block starts to be
/// able to say otherwise.
///
/// The obvious encoding was a `flags` varint beside every item, which costs ONE BYTE PER ITEM on
/// every block whether or not anything was ever removed. It does not have to: the item's VALUE TAG
/// already has an unused codepoint, and widening its meaning is free.
///
/// ```text
///     v1 tag   0 -> value is a suffix of the key, offset follows
///              n -> value is the next n-1 bytes          (so tag 1 is an empty inline value)
///
///     v2 tag   0 -> LIVE, value is a suffix of the key, offset follows
///              1 -> REMOVED. No value bytes at all, which is the point: a tombstone carries the
///                   key and nothing else, exactly as the comparison design clears the value
///                   before it logs the item.
///              n -> LIVE, value is the next n-2 bytes
/// ```
///
/// TAG 1 WAS ALREADY UNREACHABLE FROM THE ENCODER, which is what makes this free rather than a
/// widening. An empty inline value would be tag 1, and `suffix_offset_of(key, b"")` always answers
/// `Some(key.len())` -- the empty slice is the tail that starts at the key's end -- so the encoder
/// has never emitted it and `an_empty_value_and_a_removal_of_the_same_element_are_different_bytes` has driven that
/// since #2022. So a live suffix item is byte-identical between the two shapes, and a live inline
/// item is the same width except where `n - 1` and `n - 2` fall either side of a varint boundary.
/// `a_live_suffix_block_is_the_same_bytes_under_either_shape` prices both against the first.
///
/// # WHY EVERY NEW BLOCK TAKES IT, INCLUDING ONE WITH NO REMOVAL IN IT
///
/// The cheaper-looking alternative is to write v1 while a block has no tombstone and v2 only when it
/// does. That makes "a block is v1 if and only if it holds no removal" an invariant, and nothing
/// would enforce it -- a writer that took the wrong shape for its contents would produce a block
/// whose tombstone decodes as an empty live value, which is the resurrection this whole stage
/// exists to close, reintroduced by a branch. One writer, one shape, and the cost is priced above.
pub(super) const CONTAINER_BLOCK_MAGIC_V2: &[u8] = b"TSCPG2\n";

/// The magic, the spelling byte, and the smallest possible item count.
const CONTAINER_BLOCK_HEADER_BYTES: usize = CONTAINER_BLOCK_MAGIC_V2.len() + 1 + 1;

/// Which shape a frame's magic named, because the value tag means different things in each.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ContainerBlockShape {
    /// `TSCPG1\n`. Every item is live; the shape cannot say otherwise.
    LiveOnly,
    /// `TSCPG2\n`. An item may be a removal.
    WithRemovals,
}

/// One element of a container block: the key that names it, the value it holds, and whether the item
/// is the RECORD OF ITS REMOVAL rather than its value.
///
/// A removed item's `value` is EMPTY and carries no information -- the encoder writes no value bytes
/// for it and the decoder does not invent any. Reading the value of an item whose `deleted` is set
/// is therefore always a mistake, and `deleted` is checked before `value` at every use.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ContainerBlockItem {
    pub(super) key: Vec<u8>,
    pub(super) value: Vec<u8>,
    /// This item says its element is GONE, not what it holds.
    pub(super) deleted: bool,
}

/// What a payload turned out to be.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum ContainerBlockDecode {
    /// No magic. The payload is one value and the index entry beside it names the element -- which
    /// is every block written before this module, and every block of a kind that is its whole object.
    NotFramed,
    /// A frame, and these are its items in written order.
    Framed {
        spelling: ElementKeySpelling,
        /// Which magic it carried. A `LiveOnly` block cannot hold a removal, so a derivation reading
        /// one knows its silence about an element means nothing rather than meaning present.
        shape: ContainerBlockShape,
        items: Vec<ContainerBlockItem>,
    },
    /// The magic is there and what follows it is not a frame. NEVER treated as `NotFramed`: the
    /// bytes claim to be a frame, so handing them back as a value would serve framing bytes as
    /// data.
    Corrupt(String),
}

/// What asking a payload for one of its elements turned out to be.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum ContainerElementRead {
    /// No magic, so the whole payload is the value the caller asked for.
    NotFramed,
    /// The frame holds this element and this is its value.
    Found(Vec<u8>),
    /// The frame is well formed and does NOT hold this element. Distinct from `Corrupt` on purpose:
    /// this is an answer, and a missing element is a legitimate one.
    Absent,
    /// The frame holds this element's REMOVAL. Distinct from `Absent` because the two are different
    /// facts and only one of them is durable: `Absent` says this block never mentioned the element,
    /// so an older block still may; `Removed` says this block states it is gone, which is what makes a
    /// block-derived membership possible at all. A reader that only wants a value treats both as no
    /// value, and the read funnel does; a derivation must not, and
    /// `an_older_block_is_not_outranked_by_a_block_that_merely_does_not_mention_the_element` drives
    /// exactly that difference.
    Removed,
    /// The frame could not be walked.
    Corrupt(String),
}

/// What projecting several UTF-8 named elements from one fetched block produced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Utf8ContainerProjection {
    /// No container magic; the caller retains legacy bare-value behavior.
    NotFramed,
    /// The frame uses another container spelling; the caller retains the generic selector.
    OtherSpelling,
    /// One positional value per requested element name.
    Values(Vec<Option<Vec<u8>>>),
    /// The frame could not be walked.
    Corrupt(String),
}

/// Byte ranges for requested UTF-8 fields in one retained packed frame.
pub(super) enum Utf8ContainerRangeProjection {
    /// No container magic; the caller retains legacy bare-value behavior.
    NotFramed,
    /// The frame uses another container spelling; the caller retains the generic selector.
    OtherSpelling,
    /// One positional value range per requested element name.
    Values(Vec<Option<std::ops::Range<usize>>>),
    /// The frame could not be walked.
    Corrupt(String),
}

/// Immutable field names and membership shared by a homogeneous write batch.
pub(super) struct SharedHashFields {
    names: Vec<std::sync::Arc<str>>,
    sorted_names: Vec<std::sync::Arc<str>>,
    membership: std::collections::HashSet<std::sync::Arc<str>>,
}

impl SharedHashFields {
    pub(super) fn new<'a>(fields: impl Iterator<Item = &'a str>) -> Option<Self> {
        let names: Vec<std::sync::Arc<str>> = fields.map(std::sync::Arc::from).collect();
        let membership: std::collections::HashSet<_> = names.iter().cloned().collect();
        if membership.len() != names.len() {
            return None;
        }
        let mut sorted_names = names.clone();
        sorted_names.sort_unstable();
        Some(Self { names, sorted_names, membership })
    }

    pub(super) fn names(&self) -> &[std::sync::Arc<str>] {
        &self.names
    }

    pub(super) fn sorted_names(&self) -> &[std::sync::Arc<str>] {
        &self.sorted_names
    }

    pub(super) fn matches<'a>(&self, fields: impl Iterator<Item = &'a str>) -> bool {
        self.names.iter().map(|name| name.as_ref()).eq(fields)
    }

    pub(super) fn contains(&self, field: &str) -> bool {
        self.membership.contains(field)
    }
}

/// The requested-name index shared by every row in one hash projection batch.
pub(super) struct Utf8ProjectionPlan<'a> {
    element_names: &'a [String],
    first_position: std::collections::HashMap<&'a [u8], usize>,
    positions_by_name: Vec<usize>,
}

impl<'a> Utf8ProjectionPlan<'a> {
    pub(super) fn new(element_names: &'a [String]) -> Self {
        let mut first_position =
            std::collections::HashMap::<&[u8], usize>::with_capacity(element_names.len());
        for (index, element_name) in element_names.iter().enumerate() {
            first_position.entry(element_name.as_bytes()).or_insert(index);
        }
        let mut positions_by_name = (0..element_names.len()).collect::<Vec<_>>();
        positions_by_name.sort_unstable_by(|left, right| {
            element_names[*left]
                .cmp(&element_names[*right])
                .then_with(|| left.cmp(right))
        });
        Self {
            element_names,
            first_position,
            positions_by_name,
        }
    }

    pub(super) fn positions_by_name(&self) -> &[usize] {
        &self.positions_by_name
    }
}

/// Reusable per-row state for one shared-field projection batch.
pub(super) struct Utf8ProjectionScratch {
    found: Vec<bool>,
    present_in_index: Vec<bool>,
}

#[cfg(test)]
thread_local! {
    static INDEX_PRESENCE_RESETS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn reset_index_presence_reset_count_for_test() {
    INDEX_PRESENCE_RESETS.with(|count| count.set(0));
}

#[cfg(test)]
pub(crate) fn index_presence_reset_count_for_test() -> u64 {
    INDEX_PRESENCE_RESETS.with(std::cell::Cell::get)
}

impl Utf8ProjectionScratch {
    pub(super) fn new() -> Self {
        Self {
            found: Vec::new(),
            present_in_index: Vec::new(),
        }
    }

    fn reset(&mut self, field_count: usize) {
        self.found.resize(field_count, false);
        self.found.fill(false);
    }

    pub(super) fn reset_index_presence(&mut self, field_count: usize) {
        #[cfg(test)]
        INDEX_PRESENCE_RESETS.with(|count| count.set(count.get().saturating_add(1)));
        self.present_in_index.resize(field_count, false);
        self.present_in_index.fill(false);
    }

    pub(super) fn mark_present_in_index(&mut self, index: usize) {
        self.present_in_index[index] = true;
    }

    pub(super) fn is_present_in_index(&self, index: usize) -> bool {
        self.present_in_index[index]
    }
}

fn put_varint(out: &mut Vec<u8>, mut value: u64) {
    loop {
        let byte = (value & 0x7f) as u8;
        value >>= 7;
        if value == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

/// How many bytes `value` occupies as a varint, so a test can state a width from the data.
pub(super) fn varint_len(mut value: u64) -> usize {
    let mut len = 1;
    while value >= 0x80 {
        value >>= 7;
        len += 1;
    }
    len
}

fn take_varint(bytes: &[u8], cursor: &mut usize, what: &str) -> Result<u64, String> {
    let mut value = 0_u64;
    let mut shift = 0_u32;
    loop {
        let byte = *bytes
            .get(*cursor)
            .ok_or_else(|| format!("container block truncated reading {what}"))?;
        *cursor += 1;
        if shift >= 64 {
            return Err(format!("container block {what} varint is wider than 64 bits"));
        }
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
        shift += 7;
    }
}

fn take_slice<'a>(
    bytes: &'a [u8],
    cursor: &mut usize,
    len: usize,
    what: &str,
) -> Result<&'a [u8], String> {
    let end = cursor
        .checked_add(len)
        .ok_or_else(|| format!("container block {what} length overflows"))?;
    let slice = bytes
        .get(*cursor..end)
        .ok_or_else(|| format!("container block truncated reading {what}"))?;
    *cursor = end;
    Ok(slice)
}

/// One item on its way INTO a block: what to write, and whether it is a removal.
///
/// Borrowed rather than owned because every caller already holds both halves, and a removal holds
/// no value at all -- `TOMBSTONE_VALUE` is what a caller passes for it, so that "a removal carries
/// no value" is a fact about this type rather than a convention each site remembers.
#[derive(Debug, Clone, Copy)]
pub(super) struct ContainerBlockWrite<'a> {
    pub(super) key: &'a [u8],
    pub(super) value: &'a [u8],
    pub(super) deleted: bool,
}

impl<'a> ContainerBlockWrite<'a> {
    /// A live element and the value it holds.
    pub(super) fn live(key: &'a [u8], value: &'a [u8]) -> Self {
        Self {
            key,
            value,
            deleted: false,
        }
    }

    /// The RECORD THAT THIS ELEMENT IS GONE. No value, because the comparison design's own answer to
    /// this is that the value is cleared before the item is logged: a tombstone that carried the
    /// last value would be a second copy of data the store has already been told to forget.
    pub(super) fn removed(key: &'a [u8]) -> Self {
        Self {
            key,
            value: &[],
            deleted: true,
        }
    }
}

/// Write items as a block payload, every one of them live.
///
/// The shape the four write sites and the fold use: neither produces a removal, so neither should
/// have to say `false` per item. A removal is written through
/// [`encode_container_block_items`], which is the only way to produce one.
pub(super) fn encode_container_block(
    spelling: ElementKeySpelling,
    items: &[(&[u8], &[u8])],
) -> Vec<u8> {
    encode_live_container_block(
        spelling,
        items.len(),
        items.iter().copied(),
        items
            .iter()
            .map(|(key, value)| key.len() + value.len() + 6)
            .sum(),
    )
}

/// Encode an owned UTF-8 row without first materializing borrowed pair and write vectors.
pub(super) fn encode_utf8_container_block<K, V>(items: &[(K, V)]) -> Vec<u8>
where
    K: AsRef<[u8]>,
    V: AsRef<[u8]>,
{
    encode_utf8_container_iter(items.iter().map(|(key, value)| (key.as_ref(), value.as_ref())))
}

pub(super) fn encode_utf8_container_iter<'a>(
    items: impl Iterator<Item = (&'a [u8], &'a [u8])> + Clone + ExactSizeIterator,
) -> Vec<u8> {
    encode_live_container_block(
        ElementKeySpelling::Utf8,
        items.len(),
        items.clone(),
        items.map(|(key, value)| key.len() + value.len() + 6).sum(),
    )
}

fn encode_live_container_block<'a>(
    spelling: ElementKeySpelling,
    item_count: usize,
    items: impl IntoIterator<Item = (&'a [u8], &'a [u8])>,
    payload_capacity: usize,
) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(CONTAINER_BLOCK_HEADER_BYTES + payload_capacity);
    bytes.extend_from_slice(CONTAINER_BLOCK_MAGIC_V2);
    bytes.push(spelling.to_byte());
    put_varint(&mut bytes, item_count as u64);
    for (key, value) in items {
        put_varint(&mut bytes, key.len() as u64);
        bytes.extend_from_slice(key);
        match suffix_offset_of(key, value) {
            Some(offset) => {
                put_varint(&mut bytes, TAG_SUFFIX);
                put_varint(&mut bytes, offset as u64);
            }
            None => {
                put_varint(&mut bytes, value.len() as u64 + TAG_INLINE_BIAS_V2);
                bytes.extend_from_slice(value);
            }
        }
    }
    bytes
}

/// Write items as a block payload, each either live or a removal.
///
/// The suffix detection is ONE candidate and not a search: the only offset at which the value can
/// be the key's tail is `key.len() - value.len()`, so the check is a single slice comparison. It is
/// not asked at all for a removal -- a removal has no value to place.
pub(super) fn encode_container_block_items(
    spelling: ElementKeySpelling,
    items: &[ContainerBlockWrite<'_>],
) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(
        CONTAINER_BLOCK_HEADER_BYTES
            + items
                .iter()
                .map(|item| item.key.len() + item.value.len() + 6)
                .sum::<usize>(),
    );
    bytes.extend_from_slice(CONTAINER_BLOCK_MAGIC_V2);
    bytes.push(spelling.to_byte());
    put_varint(&mut bytes, items.len() as u64);
    for item in items {
        put_varint(&mut bytes, item.key.len() as u64);
        bytes.extend_from_slice(item.key);
        if item.deleted {
            put_varint(&mut bytes, TAG_REMOVED);
            continue;
        }
        match suffix_offset_of(item.key, item.value) {
            Some(offset) => {
                put_varint(&mut bytes, TAG_SUFFIX);
                put_varint(&mut bytes, offset as u64);
            }
            None => {
                put_varint(&mut bytes, item.value.len() as u64 + TAG_INLINE_BIAS_V2);
                bytes.extend_from_slice(item.value);
            }
        }
    }
    bytes
}

/// The value is the key's tail, and the offset it starts at follows. The SAME NUMBER IN BOTH SHAPES,
/// which is why a live suffix item does not change width.
const TAG_SUFFIX: u64 = 0;

/// The item is a removal. Only meaningful under [`CONTAINER_BLOCK_MAGIC_V2`]; under the first shape
/// this number is an empty inline value, which the encoder never produced.
const TAG_REMOVED: u64 = 1;

/// What an inline value's length is raised by so it cannot collide with the two tags above. One
/// under the first shape, two under the second.
const TAG_INLINE_BIAS_V1: u64 = 1;
const TAG_INLINE_BIAS_V2: u64 = 2;

/// Where `value` starts inside `key`, when it is `key`'s tail.
fn suffix_offset_of(key: &[u8], value: &[u8]) -> Option<usize> {
    let offset = key.len().checked_sub(value.len())?;
    if &key[offset..] == value {
        Some(offset)
    } else {
        None
    }
}

/// Walk a payload, if it is a frame.
pub(super) fn decode_container_block(bytes: &[u8]) -> ContainerBlockDecode {
    let Some(header) = frame_header(bytes) else {
        return ContainerBlockDecode::NotFramed;
    };
    let (shape, spelling) = match header {
        Ok(header) => header,
        Err(error) => return ContainerBlockDecode::Corrupt(error),
    };
    let mut cursor = CONTAINER_BLOCK_MAGIC.len() + 1;
    let count = match take_varint(bytes, &mut cursor, "item count") {
        Ok(count) => count,
        Err(error) => return ContainerBlockDecode::Corrupt(error),
    };
    // A count wider than the payload can hold items would otherwise reserve on a corrupt number.
    if count > bytes.len() as u64 {
        return ContainerBlockDecode::Corrupt(format!(
            "container block claims {count} items in {} bytes",
            bytes.len()
        ));
    }
    let mut items = Vec::with_capacity(count as usize);
    for index in 0..count {
        match take_item(bytes, &mut cursor, index, shape) {
            Ok((key, value, deleted)) => items.push(ContainerBlockItem {
                key: key.to_vec(),
                value,
                deleted,
            }),
            Err(error) => return ContainerBlockDecode::Corrupt(error),
        }
    }
    if cursor != bytes.len() {
        return ContainerBlockDecode::Corrupt(format!(
            "container block has {} bytes after its last item",
            bytes.len() - cursor
        ));
    }
    ContainerBlockDecode::Framed {
        spelling,
        shape,
        items,
    }
}

/// The frame's shape and spelling byte, or `None` when these bytes are not a frame at all.
///
/// BOTH MAGICS ARE SEVEN BYTES AND DIFFER IN ONE, so the spelling byte is at the same offset in
/// either and every cursor in this module still starts at `MAGIC.len() + 1`. That is not a
/// coincidence to rely on silently -- `the_two_shapes_put_their_spelling_byte_at_one_offset` asserts
/// the two lengths are equal, so a third shape spelled differently is a build-time argument rather
/// than a cursor that reads one byte into the wrong field.
///
/// A PAYLOAD THAT IS EXACTLY THE MAGIC IS A TRUNCATED FRAME, NOT AN UNFRAMED BLOCK. This returned
/// `None` for it -- `bytes.get(..)?` propagating out of the whole function -- so seven bytes that
/// claim to be a frame were handed back to the caller as a value. It is the same one-character
/// shape as every other defect this module cites: an answer that cannot be produced being turned
/// into one that can. Caught by `a_frame_that_cannot_be_walked_is_not_mistaken_for_a_value`, whose
/// truncation ladder cuts at exactly this boundary.
fn frame_header(
    bytes: &[u8],
) -> Option<Result<(ContainerBlockShape, ElementKeySpelling), String>> {
    const _: () = assert!(CONTAINER_BLOCK_MAGIC.len() == CONTAINER_BLOCK_MAGIC_V2.len());
    let shape = if bytes.starts_with(CONTAINER_BLOCK_MAGIC_V2) {
        ContainerBlockShape::WithRemovals
    } else if bytes.starts_with(CONTAINER_BLOCK_MAGIC) {
        ContainerBlockShape::LiveOnly
    } else {
        return None;
    };
    let Some(byte) = bytes.get(CONTAINER_BLOCK_MAGIC.len()).copied() else {
        return Some(Err(String::from(
            "container page is the magic and nothing else, so it names no key spelling",
        )));
    };
    Some(
        ElementKeySpelling::from_byte(byte)
            .ok_or_else(|| format!("container page names key spelling {byte}, which is not one"))
            .map(|spelling| (shape, spelling)),
    )
}

/// One item, and whether it says its element is gone.
///
/// THE TAG IS READ AGAINST THE SHAPE THE MAGIC NAMED, never against a guess. Under the first shape
/// `1` is an empty inline value and under the second it is a removal, and those are the same three
/// bytes on disk -- so a decoder that took one meaning for both would either resurrect a removed
/// element in a new block or invent a removal in an old one. The shape comes from the magic, which is
/// the only thing on the block that can say.
fn take_item_bytes<'a>(
    bytes: &'a [u8],
    cursor: &mut usize,
    index: u64,
    shape: ContainerBlockShape,
) -> Result<(&'a [u8], &'a [u8], bool), String> {
    let key_len = take_varint(bytes, cursor, "key length")? as usize;
    let key = take_slice(bytes, cursor, key_len, "key")?;
    let tag = take_varint(bytes, cursor, "value tag")?;
    if tag == TAG_SUFFIX {
        let offset = take_varint(bytes, cursor, "value offset")? as usize;
        let tail = key.get(offset..).ok_or_else(|| {
            format!(
                "container block item {index} names value offset {offset} into a {key_len}-byte key"
            )
        })?;
        return Ok((key, tail, false));
    }
    if shape == ContainerBlockShape::WithRemovals && tag == TAG_REMOVED {
        // NO VALUE BYTES FOLLOW. The cursor stands after the tag, which is what keeps the
        // consume-the-payload-exactly property true for a block whose last item is a removal.
        return Ok((key, &[], true));
    }
    let bias = match shape {
        ContainerBlockShape::LiveOnly => TAG_INLINE_BIAS_V1,
        ContainerBlockShape::WithRemovals => TAG_INLINE_BIAS_V2,
    };
    let value = take_slice(bytes, cursor, (tag - bias) as usize, "value")?;
    Ok((key, value, false))
}

fn take_item<'a>(
    bytes: &'a [u8],
    cursor: &mut usize,
    index: u64,
    shape: ContainerBlockShape,
) -> Result<(&'a [u8], Vec<u8>, bool), String> {
    take_item_bytes(bytes, cursor, index, shape)
        .map(|(key, value, deleted)| (key, value.to_vec(), deleted))
}

/// Match a complete UTF-8 field projection without allocating keys or values.
pub(super) fn matches_utf8_projection(bytes: &[u8], fields: &[std::sync::Arc<str>]) -> bool {
    let Some(Ok((shape, ElementKeySpelling::Utf8))) = frame_header(bytes) else {
        return false;
    };
    let mut cursor = CONTAINER_BLOCK_MAGIC.len() + 1;
    let Ok(count) = take_varint(bytes, &mut cursor, "item count") else { return false };
    if count != fields.len() as u64 {
        return false;
    }
    for (index, field) in fields.iter().enumerate() {
        match take_item_bytes(bytes, &mut cursor, index as u64, shape) {
            Ok((key, _, false)) if key == field.as_bytes() => {}
            _ => return false,
        }
    }
    cursor == bytes.len()
}

/// Ask a payload for one element's value, without building every other item.
///
/// `element_name` is the index's spelling, which is what every reader has in hand: the read funnel is
/// handed the element name the address was looked up by.
pub(super) fn select_container_element(bytes: &[u8], element_name: &str) -> ContainerElementRead {
    let Some(header) = frame_header(bytes) else {
        return ContainerElementRead::NotFramed;
    };
    let (shape, spelling) = match header {
        Ok(header) => header,
        Err(error) => return ContainerElementRead::Corrupt(error),
    };
    // A ScoreThenMember ELEMENT NAME NO LONGER NAMES A KEY EXACTLY -- the score is gone from it, so
    // `element_key_from_element_name` can only hand back a key with a PLACEHOLDER in its first eight
    // bytes, which would miss every block a previous binary wrote with its true score there. So
    // this spelling is matched by member suffix, decoded once, rather than by key equality: a
    // block's own first eight bytes, real score or placeholder alike, are never examined. Every
    // other spelling keeps the exact-key comparison, which stays correct because none of them
    // dropped anything from what the element name spells.
    enum Wanted {
        Key(Vec<u8>),
        Member(Vec<u8>),
    }
    let wanted = if matches!(
        spelling,
        ElementKeySpelling::ScoreThenMember | ElementKeySpelling::ScoreThenMemberName
    ) {
        match member_from_element_name(element_name) {
            Some(member) => Wanted::Member(member),
            // Not an element name in either spelling, so no item's member suffix can equal it. An
            // answer, not a corruption -- the same shape the exact-key arm below answers in.
            None => return ContainerElementRead::Absent,
        }
    } else {
        match element_key_from_element_name(spelling, element_name) {
            Some(key) => Wanted::Key(key),
            // The frame is fine and this element name is not one its spelling can name, so no item
            // in it can be the one asked for. An answer, not a corruption.
            None => return ContainerElementRead::Absent,
        }
    };
    let mut cursor = CONTAINER_BLOCK_MAGIC.len() + 1;
    let count = match take_varint(bytes, &mut cursor, "item count") {
        Ok(count) => count,
        Err(error) => return ContainerElementRead::Corrupt(error),
    };
    for index in 0..count {
        match take_item_bytes(bytes, &mut cursor, index, shape) {
            Ok((key, value, deleted)) => {
                let matched = match &wanted {
                    Wanted::Key(key_wanted) => key == key_wanted.as_slice(),
                    // THE MEMBER SUFFIX ONLY -- never the first eight bytes, which this spelling's
                    // element name no longer names.
                    Wanted::Member(member) => key.get(8..) == Some(member.as_slice()),
                };
                if matched {
                    return if deleted {
                        ContainerElementRead::Removed
                    } else {
                        ContainerElementRead::Found(value.to_vec())
                    };
                }
            }
            Err(error) => return ContainerElementRead::Corrupt(error),
        }
    }
    ContainerElementRead::Absent
}

/// Project UTF-8 named fields in one pass over a packed hash block.
///
/// The generic point selector starts at the frame header for every field. A feature projection
/// asks for many fields from the same packed row, so indexing requested names once and walking the
/// frame once keeps decode work linear in the row width. Skipped values remain borrowed; only
/// values that the caller requested are copied into the response.
pub(super) fn project_utf8_container_elements(
    bytes: &[u8],
    element_names: &[String],
) -> Utf8ContainerProjection {
    let plan = Utf8ProjectionPlan::new(element_names);
    let mut scratch = Utf8ProjectionScratch::new();
    project_utf8_container_elements_with_plan(bytes, &plan, &mut scratch)
}

pub(super) fn project_utf8_container_elements_with_plan(
    bytes: &[u8],
    plan: &Utf8ProjectionPlan<'_>,
    scratch: &mut Utf8ProjectionScratch,
) -> Utf8ContainerProjection {
    match project_utf8_container_element_ranges_with_plan(bytes, plan, scratch) {
        Utf8ContainerRangeProjection::NotFramed => Utf8ContainerProjection::NotFramed,
        Utf8ContainerRangeProjection::OtherSpelling => Utf8ContainerProjection::OtherSpelling,
        Utf8ContainerRangeProjection::Corrupt(error) => Utf8ContainerProjection::Corrupt(error),
        Utf8ContainerRangeProjection::Values(ranges) => Utf8ContainerProjection::Values(
            ranges
                .into_iter()
                .map(|range| range.map(|range| bytes[range].to_vec()))
                .collect(),
        ),
    }
}

pub(super) fn project_utf8_container_element_ranges_with_plan(
    bytes: &[u8],
    plan: &Utf8ProjectionPlan<'_>,
    scratch: &mut Utf8ProjectionScratch,
) -> Utf8ContainerRangeProjection {
    let Some(header) = frame_header(bytes) else {
        return Utf8ContainerRangeProjection::NotFramed;
    };
    let (shape, spelling) = match header {
        Ok(header) => header,
        Err(error) => return Utf8ContainerRangeProjection::Corrupt(error),
    };
    if spelling != ElementKeySpelling::Utf8 {
        return Utf8ContainerRangeProjection::OtherSpelling;
    }

    scratch.reset(plan.element_names.len());
    let mut values = vec![None; plan.element_names.len()];
    let mut cursor = CONTAINER_BLOCK_MAGIC.len() + 1;
    let count = match take_varint(bytes, &mut cursor, "item count") {
        Ok(count) => count,
        Err(error) => return Utf8ContainerRangeProjection::Corrupt(error),
    };
    if count > bytes.len() as u64 {
        return Utf8ContainerRangeProjection::Corrupt(format!(
            "container block claims {count} items in {} bytes",
            bytes.len()
        ));
    }
    for index in 0..count {
        let (key, value, deleted) = match take_item_bytes(bytes, &mut cursor, index, shape) {
            Ok(item) => item,
            Err(error) => return Utf8ContainerRangeProjection::Corrupt(error),
        };
        let Some(&position) = plan.first_position.get(key) else {
            continue;
        };
        if !scratch.found[position] {
            scratch.found[position] = true;
            if !deleted {
                let start = value.as_ptr() as usize - bytes.as_ptr() as usize;
                values[position] = Some(start..start + value.len());
            }
        }
    }
    for (index, element_name) in plan.element_names.iter().enumerate() {
        let first = plan.first_position[element_name.as_bytes()];
        if index != first && scratch.found[first] {
            values[index] = values[first].clone();
        }
    }
    Utf8ContainerRangeProjection::Values(values)
}

/// Spell a container member as an index element name, without doubling it.
///
/// `hex::encode` charges TWO bytes for every member byte. This charges one byte plus the member's
/// own text when the member is text, and one byte plus the binary-safe name encoding otherwise --
/// so a text member travels at 1.0x plus a constant instead of 2.0x. The element name is the term
/// that scales with member width, so this moves the RATE, not a fixed overhead.
///
/// # THE LEADING TAG IS WHAT MAKES THE TWO SPELLINGS TELLABLE APART, AND IT IS NOT DECORATION
///
/// [`crate::binary_safe_name::encode_name`] returns text UNCHANGED when the member is text that
/// does not already begin with the tag. That is what makes it cheap, and it is also exactly why it
/// cannot be used bare here: the member `b"abcd"` would spell `"abcd"`, which is ALSO VALID HEX,
/// for the two bytes `[0xab, 0xcd]`. An index element name carries no spelling byte -- unlike a
/// block, which does -- so a reader holding only the string could not tell which of those two a
/// stored element name meant, and would hand back the wrong member without failing.
///
/// One unconditional tag in front settles it, because `hex::encode` emits only `[0-9a-f]` and can
/// never produce a leading tag. [`member_from_element_name`] dispatches on precisely that, and
/// `encoding_is_injective_over_every_two_byte_name` is what holds up the half inside the tag.
pub(super) fn member_element_name(member: &[u8]) -> String {
    let encoded = crate::binary_safe_name::encode_name(member);
    let mut out = String::with_capacity(1 + encoded.len());
    out.push(crate::binary_safe_name::BINARY_TAG as char);
    out.push_str(&encoded);
    out
}

/// A container member from its index element name, in EITHER spelling.
///
/// This is the reader half of the move off hex, and it is the half that has to land first. A
/// element name written by [`member_element_name`] and an element name written by `hex::encode` both decode
/// here, which is what lets one store hold blocks from either side of the change. `None` where the
/// element name is neither, which is the same answer every arm of the derived view already gives an
/// unreadable name: it names nothing, so nothing is found.
///
/// BOTH SPELLINGS ARE LIVE NOW. [`ElementKeySpelling::for_kind`] answers `Name` and
/// `ScoreThenMemberName`, so every set and zset write from here on files the tagged spelling and
/// that branch is the hot one.
///
/// THE HEX BRANCH IS NOT DEAD BOOKKEEPING AND MUST NOT BE READ AS SUCH. Every set and zset block
/// written before the writers flipped is filed under a hex element name, and this function is the
/// only reason any of them still answers. Deleting the branch because no writer emits it would
/// strand the members of every store that predates the flip: a zero-writer branch here is load
/// bearing, not unreachable.
pub(super) fn member_from_element_name(element_name: &str) -> Option<Vec<u8>> {
    match element_name.strip_prefix(crate::binary_safe_name::BINARY_TAG as char) {
        Some(tail) => crate::binary_safe_name::decode_name(tail).ok(),
        None => hex::decode(element_name).ok(),
    }
}

/// The index element name a page key spells.
///
/// Inverse of [`element_key_from_element_name`]. `None` where the key cannot spell one at all -- a
/// hash field that is not valid UTF-8, a list word that is not eight bytes -- which is a corrupt
/// block rather than an absent element, and is reported as such by the caller.
pub(super) fn element_name_from_element_key(
    spelling: ElementKeySpelling,
    key: &[u8],
) -> Option<String> {
    match spelling {
        ElementKeySpelling::Utf8 => String::from_utf8(key.to_vec()).ok(),
        ElementKeySpelling::Hex => Some(hex::encode(key)),
        ElementKeySpelling::ScoreThenMember => {
            // THE SCORE IS NO LONGER SPELLED HERE. `BlockIndex.element_name` is what this render
            // feeds, and the score term was 16 of its bytes with the member carrying the rest --
            // the one thing stopping zset from collapsing to one index entry a block the way set
            // and list already have. The resident map (`shard.bucket_index.zsets`) is the score's only copy
            // now; a reader that wants it reads that map, not this string. See
            // `element_key_from_element_name`'s mirror of this cut for what it costs on the way
            // back, and `select_container_element` for why nothing here needed to change to stay
            // correct against it.
            Some(hex::encode(key.get(8..)?))
        }
        ElementKeySpelling::BiasedWord => {
            if key.len() != 8 {
                return None;
            }
            let biased = u64::from_be_bytes(key.try_into().ok()?);
            Some(format!("{biased:016x}"))
        }
        // A SET MEMBER IS UNORDERED, so nothing reads this element name for its order and the choice
        // of spelling is free: the only readers of a set element name decode it back to the member.
        ElementKeySpelling::Name => Some(member_element_name(key)),
        // The score is not spelled here, for the same reason the hex pair above stopped spelling
        // it: the resident map is its only copy, and the ordered readers sort from that map. So
        // this element name, like its hex predecessor, carries the member and nothing else -- which
        // is why there is no element name ORDER question for zset either.
        ElementKeySpelling::ScoreThenMemberName => Some(member_element_name(key.get(8..)?)),
    }
}

/// The page key an element name spells.
///
/// Inverse of [`element_name_from_element_key`]. `None` where the element name is not one this spelling
/// can produce -- which is how a reader asking a set block for an element name that is not hex gets
/// `Absent` rather than a walk.
pub(super) fn element_key_from_element_name(
    spelling: ElementKeySpelling,
    element_name: &str,
) -> Option<Vec<u8>> {
    match spelling {
        ElementKeySpelling::Utf8 => Some(element_name.as_bytes().to_vec()),
        // BOTH SPELLINGS, for `Hex` as well as for `Name`. A hex element name is what every block on
        // disk today is filed under; a tagged one is what a later writer will file. Accepting both
        // in the one arm is what makes the two interchangeable for a reader, and it cannot widen
        // what `Hex` accepts by accident, because a hex string never begins with the tag.
        ElementKeySpelling::Hex | ElementKeySpelling::Name => member_from_element_name(element_name),
        ElementKeySpelling::ScoreThenMember | ElementKeySpelling::ScoreThenMemberName => {
            // LOSSY BY DESIGN, IN THIS ONE ARM ONLY. The element name used to be the score and the
            // member both; it is the member alone now, so this direction cannot recover the score
            // that named a PRE-EXISTING block -- there is no score left in the input to recover.
            // What it returns is a validly-shaped key (eight bytes then the member) with a
            // PLACEHOLDER in the first eight, not the true score.
            //
            // THAT IS SAFE, AND HERE IS THE WHOLE ARGUMENT WHY: every caller of this arm either
            // builds a brand-new block (`encode_single_element_block`, `encode_tombstone_block` --
            // the placeholder is all a fresh write needs, since nothing downstream reads it back)
            // or does not call it at all. `select_container_element` is the one caller that used
            // to call this and compare the result for EXACT equality against a stored key -- which
            // would have compared this placeholder against a pre-existing block's TRUE score and
            // missed every one of them after an upgrade -- so it no longer calls this arm: it
            // decodes the member once and compares the key's member suffix only, blind to
            // whatever sits in the first eight bytes, old real score or new placeholder alike.
            // `element_name_from_element_key`, the other direction, never reads them back either.
            let member = member_from_element_name(element_name)?;
            let mut key = Vec::with_capacity(8 + member.len());
            key.extend_from_slice(&[0u8; 8]);
            key.extend_from_slice(&member);
            Some(key)
        }
        ElementKeySpelling::BiasedWord => {
            if element_name.len() != 16 {
                return None;
            }
            let biased = u64::from_str_radix(element_name, 16).ok()?;
            Some(biased.to_be_bytes().to_vec())
        }
    }
}

/// Frame ONE element's value as a block payload, which is what every container write does today.
///
/// Named rather than spelled at each of the seven write sites: the sites differ in which kind they
/// are and in nothing else, and a site that built the frame itself could pick the wrong spelling
/// for its kind without anything failing until a reload.
///
/// `None` where this kind has no element key, or where the element name is not one the kind's spelling
/// can produce. Neither is reachable from the write path -- every container element name is built by
/// the very function this reverses -- and [`single_element_block`] is what the write sites call, so
/// the unreachable case is counted rather than expressed as an `unwrap`.
pub(super) fn encode_single_element_block(
    kind: &str,
    element_name: &str,
    value: &[u8],
) -> Option<Vec<u8>> {
    let spelling = ElementKeySpelling::for_kind(kind)?;
    let key = element_key_from_element_name(spelling, element_name)?;
    Some(encode_container_block(spelling, &[(&key, value)]))
}

/// Writes whose block could not be framed, and which therefore stored a bare value.
///
/// A bare value still READS correctly -- `decode_container_block` answers `NotFramed` and the read
/// funnel hands the payload back -- so this is degradation and not data loss, which is exactly why
/// it needs a counter. An unframed block is invisible to every reader today and will be invisible to
/// the load path that later rebuilds from blocks, at which point its element is simply not there.
/// A guard floors this at zero over a real exercise of all four kinds.
static UNFRAMED_CONTAINER_WRITES: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// How many container writes stored a bare value because their block could not be framed.
pub fn unframed_container_write_count() -> u64 {
    UNFRAMED_CONTAINER_WRITES.load(std::sync::atomic::Ordering::Relaxed)
}

/// Forget the count, so a test measures its own exercise.
pub fn reset_unframed_container_write_count() {
    UNFRAMED_CONTAINER_WRITES.store(0, std::sync::atomic::Ordering::Relaxed);
}

/// The payload a container write stores: the framed block, or the bare value with the miss counted.
///
/// Owned rather than borrowed because the framed form is a new allocation either way, and a write
/// site that had to branch on whether it got one would reintroduce the per-site decision this
/// function exists to remove.
pub(super) fn single_element_block(kind: &str, element_name: &str, value: &[u8]) -> Vec<u8> {
    match encode_single_element_block(kind, element_name, value) {
        Some(block) => block,
        None => {
            UNFRAMED_CONTAINER_WRITES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            value.to_vec()
        }
    }
}

/// Frame ONE element's REMOVAL as a block payload.
///
/// # WHY A REMOVAL NEEDS A BLOCK OF ITS OWN
///
/// The block holding the element cannot be amended: that is a read-modify-write on the removal path,
/// which is the cost #2027 exists to avoid and the first of the three ways forward #2028 recorded.
/// So the removal is written the way every other container change is written -- as a new block,
/// appended -- and it is the LAST block to mention its element, which is what makes it win.
///
/// # THERE IS NO FALLBACK, AND THAT IS THE DIFFERENCE FROM [`single_element_block`]
///
/// A write whose block cannot be framed stores a bare value and counts the miss: the element's value
/// is still served, because an unframed block IS its element's value, so the degradation costs
/// nothing a reader can see. A REMOVAL HAS NO SUCH FORM. Bare bytes cannot say "gone" -- they would
/// read as a value, and the element would be served its own tombstone -- so a removal that cannot be
/// framed must write NOTHING and say so, rather than write something that decodes as a resurrection.
///
/// `None` therefore means the caller must not treat the removal as durable in the blocks. Every
/// caller counts it; `a_removal_that_cannot_be_framed_is_counted_and_writes_no_block` drives the
/// count, and the four kinds all frame, so a nonzero count is a defect and not a tolerance.
pub(super) fn encode_tombstone_block(kind: &str, element_name: &str) -> Option<Vec<u8>> {
    let spelling = ElementKeySpelling::for_kind(kind)?;
    let key = element_key_from_element_name(spelling, element_name)?;
    Some(encode_container_block_items(
        spelling,
        &[ContainerBlockWrite::removed(&key)],
    ))
}

/// Removals whose block could not be framed, and which therefore recorded NOTHING in the blocks.
///
/// Counted and not tolerated: unlike an unframed value write this is a real hole -- the element is
/// gone from the resident map and the live index and the blocks do not say so, so a membership
/// derived from blocks would put it back. A guard floors this at zero over a real exercise of all
/// four kinds' removals.
static UNFRAMED_CONTAINER_REMOVALS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// How many removals stored no tombstone because their block could not be framed.
pub fn unframed_container_removal_count() -> u64 {
    UNFRAMED_CONTAINER_REMOVALS.load(std::sync::atomic::Ordering::Relaxed)
}

/// Forget the count, so a test measures its own exercise.
pub fn reset_unframed_container_removal_count() {
    UNFRAMED_CONTAINER_REMOVALS.store(0, std::sync::atomic::Ordering::Relaxed);
}

/// The payload a container removal stores, or `None` with the miss counted.
///
/// The counting twin of [`single_element_block`], and it returns an `Option` where that one returns
/// bytes for exactly the reason in [`encode_tombstone_block`]: there is no degraded form of "gone".
pub(super) fn tombstone_block(kind: &str, element_name: &str) -> Option<Vec<u8>> {
    match encode_tombstone_block(kind, element_name) {
        Some(block) => Some(block),
        None => {
            UNFRAMED_CONTAINER_REMOVALS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            None
        }
    }
}

#[cfg(test)]
mod member_element_name_tests {
    use super::*;

    #[test]
    fn shared_registration_projection_requires_a_complete_live_frame() {
        let fields = [std::sync::Arc::from("first"), std::sync::Arc::from("second")];
        let payload = vec![b'x'; 16 * 1024];
        let frame = encode_container_block(ElementKeySpelling::Utf8,
            &[(b"first".as_slice(), payload.as_slice()), (b"second", b"second")]);
        assert!(matches_utf8_projection(&frame, &fields));
        assert!(!matches_utf8_projection(&frame, &[fields[1].clone(), fields[0].clone()]));
        assert!(!matches_utf8_projection(&frame, &fields[..1]));
        assert!(!matches_utf8_projection(&frame, &[]));
        for end in 0..frame.len() {
            assert!(!matches_utf8_projection(&frame[..end], &fields), "cut at {end}");
        }
        let mut trailing = frame.clone();
        trailing.push(0);
        assert!(!matches_utf8_projection(&trailing, &fields));
        let removed = encode_container_block_items(ElementKeySpelling::Utf8,
            &[ContainerBlockWrite::live(b"first", b"one"), ContainerBlockWrite::removed(b"second")]);
        assert!(!matches_utf8_projection(&removed, &fields));
        let other = encode_container_block(ElementKeySpelling::Hex,
            &[(b"first".as_slice(), b"one".as_slice()), (b"second", b"two")]);
        assert!(!matches_utf8_projection(&other, &fields));
        assert!(!matches_utf8_projection(b"plain bytes", &fields));
    }

    #[test]
    fn owned_utf8_rows_encode_identically_without_pair_vectors() {
        let owned = vec![
            ("first".to_string(), b"one".to_vec()),
            ("second".to_string(), b"second".to_vec()),
        ];
        let borrowed = owned
            .iter()
            .map(|(key, value)| (key.as_bytes(), value.as_slice()))
            .collect::<Vec<_>>();

        assert_eq!(
            encode_utf8_container_block(&owned),
            encode_container_block(ElementKeySpelling::Utf8, &borrowed),
        );
    }

    #[test]
    #[cfg(feature = "alloc-probe")]
    fn warm_projection_reuses_its_match_bitmap() {
        let fields = (0..64)
            .map(|index| format!("wanted-{index}"))
            .collect::<Vec<_>>();
        let rows = vec![("other".to_string(), b"value".to_vec())];
        let block = encode_utf8_container_block(&rows);
        let plan = Utf8ProjectionPlan::new(&fields);
        let mut scratch = Utf8ProjectionScratch::new();

        let warm = project_utf8_container_elements_with_plan(&block, &plan, &mut scratch);
        assert_eq!(
            warm,
            Utf8ContainerProjection::Values(vec![None; fields.len()])
        );

        let probe = crate::alloc_probe::Probe::start();
        let projected = project_utf8_container_elements_with_plan(&block, &plan, &mut scratch);
        let counted = probe.stop();

        assert_eq!(
            projected,
            Utf8ContainerProjection::Values(vec![None; fields.len()])
        );
        assert_eq!(
            counted.allocs, 1,
            "a warm projection should allocate only its returned value slots: {counted:?}"
        );
    }

    /// Every element name on disk today is hex, so the reader has to keep meaning what it meant.
    #[test]
    fn a_hex_element_name_still_decodes_to_the_member_it_always_did() {
        for member in [
            Vec::new(),
            b"a".to_vec(),
            b"member".to_vec(),
            vec![0x00, 0xff, crate::binary_safe_name::BINARY_TAG],
        ] {
            assert_eq!(
                member_from_element_name(&hex::encode(&member)),
                Some(member.clone()),
                "the hex spelling of {member:?} stopped decoding"
            );
        }
        assert_eq!(member_from_element_name("not hex at all"), None);
    }

    /// THE AMBIGUITY THE TAG EXISTS FOR. `encode_name` leaves plain text alone, so an UNTAGGED
    /// element name for the member `b"abcd"` would be the string `"abcd"` -- which is also valid hex
    /// for two entirely different bytes. Both readings have to stay reachable and distinct.
    #[test]
    fn a_member_whose_text_is_valid_hex_is_not_confused_with_the_bytes_that_hex_spells() {
        let member = b"abcd".to_vec();
        let element_name = member_element_name(&member);
        assert_ne!(
            element_name, "abcd",
            "an untagged element name would be indistinguishable from hex"
        );
        assert_eq!(member_from_element_name(&element_name), Some(member));
        assert_eq!(member_from_element_name("abcd"), Some(vec![0xab, 0xcd]));
    }

    #[test]
    fn the_new_spelling_round_trips_a_text_a_tag_leading_and_a_non_utf8_member() {
        for member in [
            b"plain".to_vec(),
            vec![crate::binary_safe_name::BINARY_TAG, b'x'],
            vec![0xff, 0xfe, 0x00],
            Vec::new(),
        ] {
            let element_name = member_element_name(&member);
            assert!(element_name.starts_with(crate::binary_safe_name::BINARY_TAG as char));
            assert_eq!(
                member_from_element_name(&element_name),
                Some(member.clone()),
                "{member:?} did not survive the tagged spelling"
            );
        }
    }

    /// THE RATE, not the fixed term: the element name is the only term that scales with member width,
    /// and hex charges two of its bytes for one of the member's.
    #[test]
    fn the_new_spelling_charges_one_byte_a_member_byte_where_hex_charges_two() {
        let member = vec![b'm'; 64];
        assert_eq!(hex::encode(&member).len(), 128);
        assert_eq!(member_element_name(&member).len(), 65);
    }

    #[test]
    fn the_two_new_spelling_bytes_are_four_and_five_and_round_trip() {
        for spelling in [
            ElementKeySpelling::Utf8,
            ElementKeySpelling::Hex,
            ElementKeySpelling::ScoreThenMember,
            ElementKeySpelling::BiasedWord,
            ElementKeySpelling::Name,
            ElementKeySpelling::ScoreThenMemberName,
        ] {
            assert_eq!(
                ElementKeySpelling::from_byte(spelling.to_byte()),
                Some(spelling),
                "{spelling:?} does not survive its own byte"
            );
        }
        assert_eq!(ElementKeySpelling::Name.to_byte(), 4);
        assert_eq!(ElementKeySpelling::ScoreThenMemberName.to_byte(), 5);
        assert_eq!(ElementKeySpelling::from_byte(6), None);
    }

    #[test]
    fn the_new_variants_render_an_element_name_the_same_pair_parses_back() {
        let member = b"a member spelled in characters".to_vec();

        let rendered = element_name_from_element_key(ElementKeySpelling::Name, &member)
            .expect("a set member always spells an element name");
        assert_eq!(
            element_key_from_element_name(ElementKeySpelling::Name, &rendered),
            Some(member.clone())
        );

        let mut key = 7u64.to_be_bytes().to_vec();
        key.extend_from_slice(&member);
        let rendered = element_name_from_element_key(ElementKeySpelling::ScoreThenMemberName, &key)
            .expect("a zset member always spells an element name");
        // The score comes back as the PLACEHOLDER, exactly as the hex pair's does, because the
        // element name never carried it. See `element_key_from_element_name`'s own argument for why.
        let mut placeholder = vec![0u8; 8];
        placeholder.extend_from_slice(&member);
        assert_eq!(
            element_key_from_element_name(ElementKeySpelling::ScoreThenMemberName, &rendered),
            Some(placeholder)
        );
    }

    /// WHICH SPELLING EACH KIND'S WRITERS EMIT, WHICH IS THE ONE DECISION THE FLIP CONSISTS OF.
    ///
    /// This replaces `no_kind_maps_to_a_new_spelling_yet_so_no_writer_can_emit_one`, which pinned
    /// the reader half by asserting that NOTHING answered byte 4 or 5. That guard existed to make
    /// flipping a writer a deliberate act rather than an accident, and it did its job: moving the
    /// writers meant coming here first. It is RETARGETED rather than deleted, because the question
    /// it asks is still the live one -- what the writers emit -- and only the answer has moved.
    ///
    /// THE HASH AND LIST ARMS ARE THE POINT OF THE TEST, NOT PADDING. Set and zset moving shows up
    /// in any byte measurement; hash or list moving BY ACCIDENT does not, and would rewrite a
    /// element name shape that has no doubling to remove, for nothing.
    #[test]
    fn every_kind_maps_to_the_spelling_its_writers_emit() {
        assert_eq!(
            ElementKeySpelling::for_kind("set"),
            Some(ElementKeySpelling::Name),
            "a set member files the tagged spelling"
        );
        assert_eq!(
            ElementKeySpelling::for_kind("zset"),
            Some(ElementKeySpelling::ScoreThenMemberName),
            "a zset member files the tagged spelling"
        );
        assert_eq!(
            ElementKeySpelling::for_kind("hash"),
            Some(ElementKeySpelling::Utf8),
            "hash DID NOT MOVE: a field element name is its own text at one byte a byte already"
        );
        assert_eq!(
            ElementKeySpelling::for_kind("list"),
            Some(ElementKeySpelling::BiasedWord),
            "list DID NOT MOVE: a sequence element name is sixteen characters whatever it holds"
        );
        assert_eq!(ElementKeySpelling::for_kind("feature"), None);
        assert_eq!(ElementKeySpelling::for_kind("string"), None);
    }

    /// A BLOCK WRITTEN BEFORE THE WRITERS FLIPPED IS STILL FOUND, ASKED FOR IN EITHER SPELLING.
    ///
    /// This is the compatibility promise the change is split in two halves to keep, and it is the
    /// half no byte measurement can see: a store that loses its set and zset members in exchange
    /// for a smaller element name has not got cheaper, it has broken.
    ///
    /// THE FIXTURE BUILDS THE OLD BYTES DELIBERATELY, and must keep doing so. It used to go
    /// through `encode_single_element_block`, which asks [`ElementKeySpelling::for_kind`] -- and
    /// that now answers `Name` and `ScoreThenMemberName`. Routing through it would write a byte-4
    /// block and leave this test asserting that today's writer agrees with today's reader, which
    /// is the one thing that cannot break. Old bytes have to be constructed on purpose or they are
    /// not under test at all. The `assert_ne!` and the decoded-spelling check below are what stop
    /// the fixture from silently becoming a post-flip block again.
    #[test]
    fn a_block_written_before_the_flip_is_found_by_an_element_name_in_either_spelling() {
        // `b"abcd"` on purpose: a member whose own TEXT is also valid hex, which is the case the
        // two spellings are confusable in if the tag is dropped. Note the two element names below
        // genuinely name DIFFERENT members as strings -- "61626364" and the tagged "abcd" -- and
        // both have to find this one block.
        let member = b"abcd".to_vec();
        let value = b"v".to_vec();
        for (kind, old) in [
            ("set", ElementKeySpelling::Hex),
            ("zset", ElementKeySpelling::ScoreThenMember),
        ] {
            let new = ElementKeySpelling::for_kind(kind).expect("the kind has an element key");
            assert_ne!(
                new, old,
                "{kind} still maps to its pre-flip spelling, so this test is vacuous"
            );

            let hex_element_name = hex::encode(&member);
            let key = element_key_from_element_name(old, &hex_element_name)
                .expect("the old spelling parses the element name it used to write");
            let pre_flip = encode_container_block(old, &[(&key[..], &value[..])]);
            match decode_container_block(&pre_flip) {
                ContainerBlockDecode::Framed { spelling, .. } => assert_eq!(
                    spelling, old,
                    "the fixture is not a pre-flip block, so it tests nothing about one"
                ),
                other => panic!("the {kind} fixture did not frame at all: {other:?}"),
            }

            for element_name in [hex_element_name.clone(), member_element_name(&member)] {
                match select_container_element(&pre_flip, &element_name) {
                    ContainerElementRead::Found(found) => assert_eq!(found, value),
                    other => panic!("{kind} pre-flip block missed {element_name:?}: {other:?}"),
                }
            }
        }
    }

    /// AND A BLOCK WRITTEN AFTER THE FLIP ROUND TRIPS THROUGH THE WRITE PATH THE WRITERS USE.
    ///
    /// Separate from the test above because it asks the opposite question. That one builds old
    /// bytes by hand to prove they still read; this one goes through `single_element_block`, the
    /// function every container write site actually calls, and proves the block it produces is
    /// FRAMED rather than falling back to a bare value.
    ///
    /// THE FALLBACK IS WHY THIS TEST EXISTS. `single_element_block` answers a bare value and bumps
    /// a counter when it cannot frame, which READS BACK CORRECTLY -- so a spelling flip that broke
    /// framing would cost bytes and lose nothing a read-back check could see, while every element
    /// written in that window became invisible to the load path that rebuilds from blocks. The
    /// counter is asserted, not just the block.
    #[test]
    fn a_write_after_the_flip_frames_rather_than_falling_back_to_a_bare_value() {
        let value = b"v".to_vec();
        // A text member, a binary one, and the empty member: the three shapes `encode_name`
        // branches on, so a flip that framed only the pass-through case shows up here.
        for member in [b"abcd".to_vec(), vec![0xff, 0xfe, 0x00], Vec::new()] {
            for (kind, expected) in [
                ("set", ElementKeySpelling::Name),
                ("zset", ElementKeySpelling::ScoreThenMemberName),
            ] {
                let element_name = member_element_name(&member);
                reset_unframed_container_write_count();
                let block = single_element_block(kind, &element_name, &value);
                assert_eq!(
                    unframed_container_write_count(),
                    0,
                    "{kind} fell back to a bare value for member {member:?}: the element name the \
                     writers file is one its own block spelling cannot parse"
                );
                match decode_container_block(&block) {
                    ContainerBlockDecode::Framed { spelling, .. } => assert_eq!(
                        spelling, expected,
                        "{kind} framed as {spelling:?} rather than the spelling its kind maps to"
                    ),
                    other => panic!("{kind} did not frame for member {member:?}: {other:?}"),
                }
                match select_container_element(&block, &element_name) {
                    ContainerElementRead::Found(found) => assert_eq!(found, value),
                    other => panic!("{kind} block missed its own element name: {other:?}"),
                }
            }
        }
    }
}
