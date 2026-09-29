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
pub(super) const CONTAINER_PAGE_MAGIC: &[u8] = b"TSCPG1\n";

/// The magic, the spelling byte, and the smallest possible item count.
const CONTAINER_PAGE_HEADER_BYTES: usize = CONTAINER_PAGE_MAGIC.len() + 1 + 1;

/// One element of a container page: the key that names it and the value it holds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ContainerPageItem {
    pub(super) key: Vec<u8>,
    pub(super) value: Vec<u8>,
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

/// Write items as a page payload.
///
/// The suffix detection is ONE candidate and not a search: the only offset at which the value can
/// be the key's tail is `key.len() - value.len()`, so the check is a single slice comparison.
pub(super) fn encode_container_page(
    spelling: ElementKeySpelling,
    items: &[(&[u8], &[u8])],
) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(
        CONTAINER_PAGE_HEADER_BYTES
            + items
                .iter()
                .map(|(key, value)| key.len() + value.len() + 6)
                .sum::<usize>(),
    );
    bytes.extend_from_slice(CONTAINER_PAGE_MAGIC);
    bytes.push(spelling.to_byte());
    put_varint(&mut bytes, items.len() as u64);
    for (key, value) in items {
        put_varint(&mut bytes, key.len() as u64);
        bytes.extend_from_slice(key);
        match suffix_offset_of(key, value) {
            Some(offset) => {
                put_varint(&mut bytes, 0);
                put_varint(&mut bytes, offset as u64);
            }
            None => {
                put_varint(&mut bytes, value.len() as u64 + 1);
                bytes.extend_from_slice(value);
            }
        }
    }
    bytes
}

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
    let Some(spelling) = frame_spelling(bytes) else {
        return ContainerPageDecode::NotFramed;
    };
    let spelling = match spelling {
        Ok(spelling) => spelling,
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
        match take_item(bytes, &mut cursor, index) {
            Ok((key, value)) => items.push(ContainerPageItem {
                key: key.to_vec(),
                value,
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
    ContainerPageDecode::Framed { spelling, items }
}

/// The frame's spelling byte, or `None` when these bytes are not a frame at all.
fn frame_spelling(bytes: &[u8]) -> Option<Result<ElementKeySpelling, String>> {
    if !bytes.starts_with(CONTAINER_PAGE_MAGIC) {
        return None;
    }
    let byte = *bytes.get(CONTAINER_PAGE_MAGIC.len())?;
    Some(
        ElementKeySpelling::from_byte(byte)
            .ok_or_else(|| format!("container page names key spelling {byte}, which is not one")),
    )
}

fn take_item<'a>(
    bytes: &'a [u8],
    cursor: &mut usize,
    index: u64,
) -> Result<(&'a [u8], Vec<u8>), String> {
    let key_len = take_varint(bytes, cursor, "key length")? as usize;
    let key = take_slice(bytes, cursor, key_len, "key")?;
    let tag = take_varint(bytes, cursor, "value tag")?;
    if tag == 0 {
        let offset = take_varint(bytes, cursor, "value offset")? as usize;
        let tail = key.get(offset..).ok_or_else(|| {
            format!(
                "container page item {index} names value offset {offset} into a {key_len}-byte key"
            )
        })?;
        return Ok((key, tail.to_vec()));
    }
    let value = take_slice(bytes, cursor, (tag - 1) as usize, "value")?;
    Ok((key, value.to_vec()))
}

/// Ask a payload for one element's value, without building every other item.
///
/// `component` is the index's spelling, which is what every reader has in hand: the read funnel is
/// handed the component the address was looked up by.
pub(super) fn select_container_element(bytes: &[u8], component: &str) -> ContainerElementRead {
    let Some(spelling) = frame_spelling(bytes) else {
        return ContainerElementRead::NotFramed;
    };
    let spelling = match spelling {
        Ok(spelling) => spelling,
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
        match take_item(bytes, &mut cursor, index) {
            Ok((key, value)) => {
                if key == wanted.as_slice() {
                    return ContainerElementRead::Found(value);
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
