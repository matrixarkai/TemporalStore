// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A CONTAINER PAGE STATES WHICH ELEMENT IT HOLDS.
//!
//! Until now a container page carried a VALUE and nothing else. Which element of which object that
//! value belonged to was known only to the page index entry that named it -- `BlockIndex::component`
//! -- and to the object id folded into the address. The page itself said nothing, so the bytes were
//! only interpretable beside the entry that pointed at them.
//!
//! That is the one fact underneath four separate refutations, and it is worth stating plainly
//! because each of those looked like an independent dead end:
//!
//!   * the element name cannot leave the page entry, because for a hash the name IS the element and
//!     for a hash the entry is the only place it is written (#2009);
//!   * the resident maps cannot be rebuilt from anything but the page index, because the index is
//!     the only thing that names elements (#2016's arms skip an unnamed page for exactly this
//!     reason);
//!   * pages cannot be batched usefully while the element name is folded into the address the
//!     write returns (#1999);
//!   * and the resident map cannot become authoritative while it is merged from a snapshot the
//!     index outranks (#2017).
//!
//! All four are downstream of the page not naming its own element. This module is where it starts
//! to, and on its own it BUYS NOTHING: it costs bytes on every container write and every reader
//! keeps answering exactly as it did. What it buys is that the element name now exists in a second
//! place, which is the precondition for it leaving the first.
//!
//! # THE SHAPE
//!
//! ```text
//!     magic            7 bytes   b"TSCPG1\n"
//!     key spelling     1 byte    how a key in this page spells its index component
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
//! `hex::decode(component)` equalled the page bytes on every one of sixty entries. Writing both
//! would make a set page carry its member twice, and a stage whose whole justification is that it
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
//! The obvious alternative was to key each item by the component STRING, so that selecting an
//! element is a string compare and nothing has to know the kind. Measured on the shapes this store
//! actually writes, that costs a `set` page 42 bytes over its member and a `zset` page 58, because
//! a component is hex and hex is two characters a byte. Carrying the key in its own bytes and one
//! byte saying how it spells costs 11 and 19. So the spelling byte is not generality for its own
//! sake; it is what keeps the foundation affordable for the two kinds whose keys are hex.
//!
//! It also means `read_block_bytes` needs no new argument. A page asked for one of its elements is
//! handed the component it was looked up by, and this module turns that back into key bytes without
//! being told the kind -- so the twenty-odd call sites of the read funnel are untouched, and no
//! mechanical signature rewrite goes past the guards that read those call sites.
//!
//! ## AND THE SPELLING IS A ROUND TRIP, ASSERTED BOTH WAYS
//!
//! [`component_from_element_key`] and [`element_key_from_component`] are inverses, and that is the
//! whole safety argument for this module: the page's key and the index's component are two
//! spellings of one value, so either can be derived from the other and a later stage may delete
//! whichever it no longer wants to store. If they were merely written consistently at the same
//! moment by the same call site -- which is how the component and the set payload agree TODAY, and
//! nothing re-checks them -- then the second spelling would be an unverified copy rather than a
//! derivation. `container_page_element_key` drives the round trip over every spelling and over the
//! edge cases each one has (an empty member, a score of exactly sixteen characters with nothing
//! after it, a field name that is not valid UTF-8).
//!
//! # WHAT DOES NOT MOVE
//!
//! No stored format is retired and no version stamp is spent. The magic DISCRIMINATES: a page
//! written before this module has no magic, [`decode_container_page`] answers `NotFramed`, and the
//! read funnel hands the bytes back exactly as it always did. That is the same trade
//! `encode_feature_block` already makes with `TSFPB1\n` against the JSON pages that came before it,
//! and it is why `SHARD_INDEX_FORMAT_VERSION` stays at 2 here: the SHARD INDEX does not change
//! shape in this stage at all, and the page payload is self-describing rather than versioned.
//!
//! A payload that legitimately begins with these seven bytes would be mistaken for a frame. That is
//! the same exposure the two feature magics carry, on the same kind of bytes, and it is noted here
//! rather than guarded because a guard would have to be a length or checksum the block record
//! already provides.

/// How a key inside a container page spells the component its index entry is filed under.
///
/// One byte in the page, because the page has to be interpretable WITHOUT the entry that names it
/// -- that is the entire point of the module -- and the kind is not recoverable from the bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ElementKeySpelling {
    /// The component is the key's bytes as UTF-8. A hash field.
    Utf8,
    /// The component is `hex::encode(key)`. A set member.
    Hex,
    /// The component is sixteen hex characters of the key's first EIGHT BYTES read big-endian,
    /// followed by `hex::encode` of everything after them. A zset's biased score and member.
    ScoreThenMember,
    /// The component is sixteen hex characters of the key's eight bytes read big-endian, and the
    /// key is exactly eight bytes. A list sequence.
    BiasedWord,
}

impl ElementKeySpelling {
    fn to_byte(self) -> u8 {
        match self {
            ElementKeySpelling::Utf8 => 0,
            ElementKeySpelling::Hex => 1,
            ElementKeySpelling::ScoreThenMember => 2,
            ElementKeySpelling::BiasedWord => 3,
        }
    }

    fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            0 => Some(ElementKeySpelling::Utf8),
            1 => Some(ElementKeySpelling::Hex),
            2 => Some(ElementKeySpelling::ScoreThenMember),
            3 => Some(ElementKeySpelling::BiasedWord),
            _ => None,
        }
    }

    /// The spelling the given stored kind uses, or `None` for a kind whose page is its whole
    /// object and carries no element key.
    ///
    /// Derived from the kind string the write sites already hold, in ONE place, so a fifth
    /// container kind cannot be given a page format by accident at a new call site.
    pub(super) fn for_kind(kind: &str) -> Option<Self> {
        match kind {
            "hash" => Some(ElementKeySpelling::Utf8),
            "set" => Some(ElementKeySpelling::Hex),
            "zset" => Some(ElementKeySpelling::ScoreThenMember),
            "list" => Some(ElementKeySpelling::BiasedWord),
            _ => None,
        }
    }
}

/// Seven bytes that say a page payload holds items rather than one value.
///
/// THE FIRST SHAPE, WHICH STILL READS. Every item in it is present: it has no way to say an element
/// was removed, which is the whole reason there is a second shape. Kept because stores written by
/// #2022 and #2027 hold these pages and a reader that could not walk them would lose their
/// elements -- see [`CONTAINER_PAGE_MAGIC_V2`] for what changed and what it costs.
pub(super) const CONTAINER_PAGE_MAGIC: &[u8] = b"TSCPG1\n";

/// Seven bytes that say a page payload holds items SOME OF WHICH MAY BE REMOVALS.
///
/// # WHY A SECOND MAGIC AND NOT A FLAG BYTE PER ITEM
///
/// A removal has to be a WRITTEN ITEM rather than an absence, because a page is never rewritten by
/// the removal path: #2027 folds pages only when a round was already rewriting them, and a folded
/// page stays live for its other elements. #2028 drove what that costs -- twelve members folded to
/// one page, one removed, the index naming eleven and the page holding twelve -- so a membership
/// derived from pages resurrected the removed member. This magic is where the page starts to be
/// able to say otherwise.
///
/// The obvious encoding was a `flags` varint beside every item, which costs ONE BYTE PER ITEM on
/// every page whether or not anything was ever removed. It does not have to: the item's VALUE TAG
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
/// `a_live_suffix_page_is_the_same_bytes_under_either_shape` prices both against the first.
///
/// # WHY EVERY NEW PAGE TAKES IT, INCLUDING ONE WITH NO REMOVAL IN IT
///
/// The cheaper-looking alternative is to write v1 while a page has no tombstone and v2 only when it
/// does. That makes "a page is v1 if and only if it holds no removal" an invariant, and nothing
/// would enforce it -- a writer that took the wrong shape for its contents would produce a page
/// whose tombstone decodes as an empty live value, which is the resurrection this whole stage
/// exists to close, reintroduced by a branch. One writer, one shape, and the cost is priced above.
pub(super) const CONTAINER_PAGE_MAGIC_V2: &[u8] = b"TSCPG2\n";

/// The magic, the spelling byte, and the smallest possible item count.
const CONTAINER_PAGE_HEADER_BYTES: usize = CONTAINER_PAGE_MAGIC_V2.len() + 1 + 1;

/// Which shape a frame's magic named, because the value tag means different things in each.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ContainerPageShape {
    /// `TSCPG1\n`. Every item is live; the shape cannot say otherwise.
    LiveOnly,
    /// `TSCPG2\n`. An item may be a removal.
    WithRemovals,
}

/// One element of a container page: the key that names it, the value it holds, and whether the item
/// is the RECORD OF ITS REMOVAL rather than its value.
///
/// A removed item's `value` is EMPTY and carries no information -- the encoder writes no value bytes
/// for it and the decoder does not invent any. Reading the value of an item whose `deleted` is set
/// is therefore always a mistake, and `deleted` is checked before `value` at every use.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ContainerPageItem {
    pub(super) key: Vec<u8>,
    pub(super) value: Vec<u8>,
    /// This item says its element is GONE, not what it holds.
    pub(super) deleted: bool,
}

/// What a payload turned out to be.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum ContainerPageDecode {
    /// No magic. The payload is one value and the index entry beside it names the element -- which
    /// is every page written before this module, and every page of a kind that is its whole object.
    NotFramed,
    /// A frame, and these are its items in written order.
    Framed {
        spelling: ElementKeySpelling,
        /// Which magic it carried. A `LiveOnly` page cannot hold a removal, so a derivation reading
        /// one knows its silence about an element means nothing rather than meaning present.
        shape: ContainerPageShape,
        items: Vec<ContainerPageItem>,
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
    /// facts and only one of them is durable: `Absent` says this page never mentioned the element,
    /// so an older page still may; `Removed` says this page states it is gone, which is what makes a
    /// page-derived membership possible at all. A reader that only wants a value treats both as no
    /// value, and the read funnel does; a derivation must not, and
    /// `an_older_page_is_not_outranked_by_a_page_that_merely_does_not_mention_the_element` drives
    /// exactly that difference.
    Removed,
    /// The frame could not be walked.
    Corrupt(String),
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
            .ok_or_else(|| format!("container page truncated reading {what}"))?;
        *cursor += 1;
        if shift >= 64 {
            return Err(format!("container page {what} varint is wider than 64 bits"));
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
        .ok_or_else(|| format!("container page {what} length overflows"))?;
    let slice = bytes
        .get(*cursor..end)
        .ok_or_else(|| format!("container page truncated reading {what}"))?;
    *cursor = end;
    Ok(slice)
}

/// One item on its way INTO a page: what to write, and whether it is a removal.
///
/// Borrowed rather than owned because every caller already holds both halves, and a removal holds
/// no value at all -- `TOMBSTONE_VALUE` is what a caller passes for it, so that "a removal carries
/// no value" is a fact about this type rather than a convention each site remembers.
#[derive(Debug, Clone, Copy)]
pub(super) struct ContainerPageWrite<'a> {
    pub(super) key: &'a [u8],
    pub(super) value: &'a [u8],
    pub(super) deleted: bool,
}

impl<'a> ContainerPageWrite<'a> {
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

/// Write items as a page payload, every one of them live.
///
/// The shape the four write sites and the fold use: neither produces a removal, so neither should
/// have to say `false` per item. A removal is written through
/// [`encode_container_page_items`], which is the only way to produce one.
pub(super) fn encode_container_page(
    spelling: ElementKeySpelling,
    items: &[(&[u8], &[u8])],
) -> Vec<u8> {
    let writes: Vec<ContainerPageWrite<'_>> = items
        .iter()
        .map(|(key, value)| ContainerPageWrite::live(key, value))
        .collect();
    encode_container_page_items(spelling, &writes)
}

/// Write items as a page payload, each either live or a removal.
///
/// The suffix detection is ONE candidate and not a search: the only offset at which the value can
/// be the key's tail is `key.len() - value.len()`, so the check is a single slice comparison. It is
/// not asked at all for a removal -- a removal has no value to place.
pub(super) fn encode_container_page_items(
    spelling: ElementKeySpelling,
    items: &[ContainerPageWrite<'_>],
) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(
        CONTAINER_PAGE_HEADER_BYTES
            + items
                .iter()
                .map(|item| item.key.len() + item.value.len() + 6)
                .sum::<usize>(),
    );
    bytes.extend_from_slice(CONTAINER_PAGE_MAGIC_V2);
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

/// The item is a removal. Only meaningful under [`CONTAINER_PAGE_MAGIC_V2`]; under the first shape
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
pub(super) fn decode_container_page(bytes: &[u8]) -> ContainerPageDecode {
    let Some(header) = frame_header(bytes) else {
        return ContainerPageDecode::NotFramed;
    };
    let (shape, spelling) = match header {
        Ok(header) => header,
        Err(error) => return ContainerPageDecode::Corrupt(error),
    };
    let mut cursor = CONTAINER_PAGE_MAGIC.len() + 1;
    let count = match take_varint(bytes, &mut cursor, "item count") {
        Ok(count) => count,
        Err(error) => return ContainerPageDecode::Corrupt(error),
    };
    // A count wider than the payload can hold items would otherwise reserve on a corrupt number.
    if count > bytes.len() as u64 {
        return ContainerPageDecode::Corrupt(format!(
            "container page claims {count} items in {} bytes",
            bytes.len()
        ));
    }
    let mut items = Vec::with_capacity(count as usize);
    for index in 0..count {
        match take_item(bytes, &mut cursor, index, shape) {
            Ok((key, value, deleted)) => items.push(ContainerPageItem {
                key: key.to_vec(),
                value,
                deleted,
            }),
            Err(error) => return ContainerPageDecode::Corrupt(error),
        }
    }
    if cursor != bytes.len() {
        return ContainerPageDecode::Corrupt(format!(
            "container page has {} bytes after its last item",
            bytes.len() - cursor
        ));
    }
    ContainerPageDecode::Framed {
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
/// A PAYLOAD THAT IS EXACTLY THE MAGIC IS A TRUNCATED FRAME, NOT AN UNFRAMED PAGE. This returned
/// `None` for it -- `bytes.get(..)?` propagating out of the whole function -- so seven bytes that
/// claim to be a frame were handed back to the caller as a value. It is the same one-character
/// shape as every other defect this module cites: an answer that cannot be produced being turned
/// into one that can. Caught by `a_frame_that_cannot_be_walked_is_not_mistaken_for_a_value`, whose
/// truncation ladder cuts at exactly this boundary.
fn frame_header(
    bytes: &[u8],
) -> Option<Result<(ContainerPageShape, ElementKeySpelling), String>> {
    const _: () = assert!(CONTAINER_PAGE_MAGIC.len() == CONTAINER_PAGE_MAGIC_V2.len());
    let shape = if bytes.starts_with(CONTAINER_PAGE_MAGIC_V2) {
        ContainerPageShape::WithRemovals
    } else if bytes.starts_with(CONTAINER_PAGE_MAGIC) {
        ContainerPageShape::LiveOnly
    } else {
        return None;
    };
    let Some(byte) = bytes.get(CONTAINER_PAGE_MAGIC.len()).copied() else {
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
/// element in a new page or invent a removal in an old one. The shape comes from the magic, which is
/// the only thing on the page that can say.
fn take_item<'a>(
    bytes: &'a [u8],
    cursor: &mut usize,
    index: u64,
    shape: ContainerPageShape,
) -> Result<(&'a [u8], Vec<u8>, bool), String> {
    let key_len = take_varint(bytes, cursor, "key length")? as usize;
    let key = take_slice(bytes, cursor, key_len, "key")?;
    let tag = take_varint(bytes, cursor, "value tag")?;
    if tag == TAG_SUFFIX {
        let offset = take_varint(bytes, cursor, "value offset")? as usize;
        let tail = key.get(offset..).ok_or_else(|| {
            format!(
                "container page item {index} names value offset {offset} into a {key_len}-byte key"
            )
        })?;
        return Ok((key, tail.to_vec(), false));
    }
    if shape == ContainerPageShape::WithRemovals && tag == TAG_REMOVED {
        // NO VALUE BYTES FOLLOW. The cursor stands after the tag, which is what keeps the
        // consume-the-payload-exactly property true for a page whose last item is a removal.
        return Ok((key, Vec::new(), true));
    }
    let bias = match shape {
        ContainerPageShape::LiveOnly => TAG_INLINE_BIAS_V1,
        ContainerPageShape::WithRemovals => TAG_INLINE_BIAS_V2,
    };
    let value = take_slice(bytes, cursor, (tag - bias) as usize, "value")?;
    Ok((key, value.to_vec(), false))
}

/// Ask a payload for one element's value, without building every other item.
///
/// `component` is the index's spelling, which is what every reader has in hand: the read funnel is
/// handed the component the address was looked up by.
pub(super) fn select_container_element(bytes: &[u8], component: &str) -> ContainerElementRead {
    let Some(header) = frame_header(bytes) else {
        return ContainerElementRead::NotFramed;
    };
    let (shape, spelling) = match header {
        Ok(header) => header,
        Err(error) => return ContainerElementRead::Corrupt(error),
    };
    let Some(wanted) = element_key_from_component(spelling, component) else {
        // The frame is fine and this component is not one its spelling can name, so no item in it
        // can be the one asked for. An answer, not a corruption.
        return ContainerElementRead::Absent;
    };
    let mut cursor = CONTAINER_PAGE_MAGIC.len() + 1;
    let count = match take_varint(bytes, &mut cursor, "item count") {
        Ok(count) => count,
        Err(error) => return ContainerElementRead::Corrupt(error),
    };
    for index in 0..count {
        match take_item(bytes, &mut cursor, index, shape) {
            Ok((key, value, deleted)) => {
                if key == wanted.as_slice() {
                    return if deleted {
                        ContainerElementRead::Removed
                    } else {
                        ContainerElementRead::Found(value)
                    };
                }
            }
            Err(error) => return ContainerElementRead::Corrupt(error),
        }
    }
    ContainerElementRead::Absent
}

/// The index component a page key spells.
///
/// Inverse of [`element_key_from_component`]. `None` where the key cannot spell one at all -- a
/// hash field that is not valid UTF-8, a list word that is not eight bytes -- which is a corrupt
/// page rather than an absent element, and is reported as such by the caller.
pub(super) fn component_from_element_key(
    spelling: ElementKeySpelling,
    key: &[u8],
) -> Option<String> {
    match spelling {
        ElementKeySpelling::Utf8 => String::from_utf8(key.to_vec()).ok(),
        ElementKeySpelling::Hex => Some(hex::encode(key)),
        ElementKeySpelling::ScoreThenMember => {
            let word = key.get(..8)?;
            let biased = u64::from_be_bytes(word.try_into().ok()?);
            Some(format!("{biased:016x}{}", hex::encode(&key[8..])))
        }
        ElementKeySpelling::BiasedWord => {
            if key.len() != 8 {
                return None;
            }
            let biased = u64::from_be_bytes(key.try_into().ok()?);
            Some(format!("{biased:016x}"))
        }
    }
}

/// The page key a component spells.
///
/// Inverse of [`component_from_element_key`]. `None` where the component is not one this spelling
/// can produce -- which is how a reader asking a set page for a component that is not hex gets
/// `Absent` rather than a walk.
pub(super) fn element_key_from_component(
    spelling: ElementKeySpelling,
    component: &str,
) -> Option<Vec<u8>> {
    match spelling {
        ElementKeySpelling::Utf8 => Some(component.as_bytes().to_vec()),
        ElementKeySpelling::Hex => hex::decode(component).ok(),
        ElementKeySpelling::ScoreThenMember => {
            // SIXTEEN CHARACTERS IS A WHOLE COMPONENT, NOT A TRUNCATED ONE -- the same boundary
            // `reconcile_secondary_views_from_bucket_index`'s zset arm spells, and for the same
            // reason: a member of zero bytes spells exactly sixteen characters and `hex::decode("")`
            // is `Ok(vec![])`. Asking `<= 16` here would make an empty member unaddressable.
            if component.len() < 16 {
                return None;
            }
            let biased = u64::from_str_radix(&component[..16], 16).ok()?;
            let member = hex::decode(&component[16..]).ok()?;
            let mut key = Vec::with_capacity(8 + member.len());
            key.extend_from_slice(&biased.to_be_bytes());
            key.extend_from_slice(&member);
            Some(key)
        }
        ElementKeySpelling::BiasedWord => {
            if component.len() != 16 {
                return None;
            }
            let biased = u64::from_str_radix(component, 16).ok()?;
            Some(biased.to_be_bytes().to_vec())
        }
    }
}

/// Frame ONE element's value as a page payload, which is what every container write does today.
///
/// Named rather than spelled at each of the seven write sites: the sites differ in which kind they
/// are and in nothing else, and a site that built the frame itself could pick the wrong spelling
/// for its kind without anything failing until a reload.
///
/// `None` where this kind has no element key, or where the component is not one the kind's spelling
/// can produce. Neither is reachable from the write path -- every container component is built by
/// the very function this reverses -- and [`single_element_page`] is what the write sites call, so
/// the unreachable case is counted rather than expressed as an `unwrap`.
pub(super) fn encode_single_element_page(
    kind: &str,
    component: &str,
    value: &[u8],
) -> Option<Vec<u8>> {
    let spelling = ElementKeySpelling::for_kind(kind)?;
    let key = element_key_from_component(spelling, component)?;
    Some(encode_container_page(spelling, &[(&key, value)]))
}

/// Writes whose page could not be framed, and which therefore stored a bare value.
///
/// A bare value still READS correctly -- `decode_container_page` answers `NotFramed` and the read
/// funnel hands the payload back -- so this is degradation and not data loss, which is exactly why
/// it needs a counter. An unframed page is invisible to every reader today and will be invisible to
/// the load path that later rebuilds from pages, at which point its element is simply not there.
/// A guard floors this at zero over a real exercise of all four kinds.
static UNFRAMED_CONTAINER_WRITES: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// How many container writes stored a bare value because their page could not be framed.
pub fn unframed_container_write_count() -> u64 {
    UNFRAMED_CONTAINER_WRITES.load(std::sync::atomic::Ordering::Relaxed)
}

/// Forget the count, so a test measures its own exercise.
pub fn reset_unframed_container_write_count() {
    UNFRAMED_CONTAINER_WRITES.store(0, std::sync::atomic::Ordering::Relaxed);
}

/// The payload a container write stores: the framed page, or the bare value with the miss counted.
///
/// Owned rather than borrowed because the framed form is a new allocation either way, and a write
/// site that had to branch on whether it got one would reintroduce the per-site decision this
/// function exists to remove.
pub(super) fn single_element_page(kind: &str, component: &str, value: &[u8]) -> Vec<u8> {
    match encode_single_element_page(kind, component, value) {
        Some(page) => page,
        None => {
            UNFRAMED_CONTAINER_WRITES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            value.to_vec()
        }
    }
}

/// Frame ONE element's REMOVAL as a page payload.
///
/// # WHY A REMOVAL NEEDS A PAGE OF ITS OWN
///
/// The page holding the element cannot be amended: that is a read-modify-write on the removal path,
/// which is the cost #2027 exists to avoid and the first of the three ways forward #2028 recorded.
/// So the removal is written the way every other container change is written -- as a new page,
/// appended -- and it is the LAST page to mention its element, which is what makes it win.
///
/// # THERE IS NO FALLBACK, AND THAT IS THE DIFFERENCE FROM [`single_element_page`]
///
/// A write whose page cannot be framed stores a bare value and counts the miss: the element's value
/// is still served, because an unframed page IS its element's value, so the degradation costs
/// nothing a reader can see. A REMOVAL HAS NO SUCH FORM. Bare bytes cannot say "gone" -- they would
/// read as a value, and the element would be served its own tombstone -- so a removal that cannot be
/// framed must write NOTHING and say so, rather than write something that decodes as a resurrection.
///
/// `None` therefore means the caller must not treat the removal as durable in the pages. Every
/// caller counts it; `a_removal_that_cannot_be_framed_is_counted_and_writes_no_page` drives the
/// count, and the four kinds all frame, so a nonzero count is a defect and not a tolerance.
pub(super) fn encode_tombstone_page(kind: &str, component: &str) -> Option<Vec<u8>> {
    let spelling = ElementKeySpelling::for_kind(kind)?;
    let key = element_key_from_component(spelling, component)?;
    Some(encode_container_page_items(
        spelling,
        &[ContainerPageWrite::removed(&key)],
    ))
}

/// Removals whose page could not be framed, and which therefore recorded NOTHING in the pages.
///
/// Counted and not tolerated: unlike an unframed value write this is a real hole -- the element is
/// gone from the resident map and the live index and the pages do not say so, so a membership
/// derived from pages would put it back. A guard floors this at zero over a real exercise of all
/// four kinds' removals.
static UNFRAMED_CONTAINER_REMOVALS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// How many removals stored no tombstone because their page could not be framed.
pub fn unframed_container_removal_count() -> u64 {
    UNFRAMED_CONTAINER_REMOVALS.load(std::sync::atomic::Ordering::Relaxed)
}

/// Forget the count, so a test measures its own exercise.
pub fn reset_unframed_container_removal_count() {
    UNFRAMED_CONTAINER_REMOVALS.store(0, std::sync::atomic::Ordering::Relaxed);
}

/// The payload a container removal stores, or `None` with the miss counted.
///
/// The counting twin of [`single_element_page`], and it returns an `Option` where that one returns
/// bytes for exactly the reason in [`encode_tombstone_page`]: there is no degraded form of "gone".
pub(super) fn tombstone_page(kind: &str, component: &str) -> Option<Vec<u8>> {
    match encode_tombstone_page(kind, component) {
        Some(page) => Some(page),
        None => {
            UNFRAMED_CONTAINER_REMOVALS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            None
        }
    }
}
