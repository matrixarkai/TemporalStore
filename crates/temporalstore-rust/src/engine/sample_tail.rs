// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Sample tails: a merge write into a stored sample kept as a tail beside the sample instead of
//! rewriting it. See `docs/WINDOW_REDUCE_SAMPLE_TAILS.md` for the whole design.
//!
//! A tail is the ordered list of merge writes a stored sample has not absorbed yet. Each write is
//! kept exactly as the request carried it (its packed cells, no re-encoding). The value a reader
//! sees is the FOLD: the stored sample with every write merged into it, in order, by the same
//! [`FlatCells::merge`] a whole-sample write uses. A write into a sample is a merge of the
//! request into the sample as it stands, so replaying the writes one after another IS what the
//! whole-sample path has always done, and the folded bytes are the bytes it would have stored.
//!
//! This module is pure: it decides nothing about where the tails live. It owns
//!
//! * [`Tail`] and [`SampleTails`]: the writes of each stamp of one key, bounded, with a canonical
//!   byte form (the index image) and the form of one write as a log item ([`TailItem`]);
//! * [`fold_value`]: base plus tail to the value a whole-sample write would have stored;
//! * [`plan_tail_write`]: whether a request can ride as one more tail write, with the counts the
//!   write's receipt needs, or must be stored whole;
//! * the in-memory memo of a hot sample's member set, which keeps the cost of a tail write
//!   independent of the size of the sample. The memo is a cache: it is never stored and never
//!   compared, and a tail without one rebuilds it from the stored sample on the next write.
//!
//! # BOUNDS
//!
//! At most [`MAX_TAIL_STAMPS`] stamps of a key hold a tail, a tail holds at most
//! [`MAX_TAIL_WRITES`] writes and at most `max(`[`MIN_TAIL_CELLS`]`, base cells / 8)` cells as the
//! requests carried them. A write that would pass a bound is stored whole, which folds the tail.

// Nothing calls into this module until the engine wires it in; the allowance goes with that change.
#![allow(dead_code)]

use std::collections::BTreeSet;

use super::cell_set::{
    split_value_raw, validate_cells_in_request, CellHeader, FlatCells, Member, MemberRef, MAX_CELLS_PER_SAMPLE,
};
use crate::types::{decode_varint, encode_bytes_field, encode_varint_field};

/// Stamps of one key that may hold a tail at once.
pub(crate) const MAX_TAIL_STAMPS: usize = 4;
/// Writes one tail may hold.
pub(crate) const MAX_TAIL_WRITES: usize = 256;
/// The least number of cells a tail may hold, whatever the size of its sample.
pub(crate) const MIN_TAIL_CELLS: usize = 64;

/// Cells (as the requests carried them) a tail over a sample of `base_cells` may hold.
pub(crate) fn tail_cell_room(base_cells: usize) -> usize {
    MIN_TAIL_CELLS.max(base_cells / 8)
}

fn owned(member: MemberRef<'_>) -> Member {
    match member {
        MemberRef::U64(v) => Member::U64(v),
        MemberRef::Bytes(b) => Member::Bytes(b.to_vec()),
    }
}

/// One merge write of a tail: the request's packed cells, and how many cells that is.
#[derive(Clone, Debug, PartialEq, Eq)]
struct TailWrite {
    cells: usize,
    packed: Vec<u8>,
}

/// The writes of one stamp not yet folded into its stored sample.
#[derive(Clone, Debug, Default)]
pub(crate) struct Tail {
    /// Cells of the stored sample the first write merged into: sizes the room of the tail.
    base_cells: usize,
    writes: Vec<TailWrite>,
    /// Cells of all writes, as the requests carried them.
    cells: usize,
    /// The members of the folded sample, when known. A cache.
    members: Option<BTreeSet<Member>>,
}

impl PartialEq for Tail {
    fn eq(&self, other: &Self) -> bool {
        self.base_cells == other.base_cells && self.writes == other.writes
    }
}
impl Eq for Tail {}

impl Tail {
    pub(crate) fn new(base_cells: usize) -> Tail {
        Tail { base_cells, writes: Vec::new(), cells: 0, members: None }
    }

    pub(crate) fn write_count(&self) -> usize {
        self.writes.len()
    }

    pub(crate) fn cells(&self) -> usize {
        self.cells
    }

    pub(crate) fn base_cells(&self) -> usize {
        self.base_cells
    }

    /// Whether the member set is held, so a write needs no decode of the stored sample.
    pub(crate) fn has_memo(&self) -> bool {
        self.members.is_some()
    }

    /// Bytes the tail holds in memory for its writes.
    pub(crate) fn bytes(&self) -> usize {
        self.writes.iter().map(|w| w.packed.len()).sum()
    }

    fn has_room_for(&self, cells: usize) -> bool {
        self.writes.len() < MAX_TAIL_WRITES && self.cells + cells <= tail_cell_room(self.base_cells)
    }

    /// Adds one write. `new_members` are the members the write created, to keep the memo (when
    /// held) equal to the folded sample.
    pub(crate) fn push(&mut self, write: &PlannedWrite) {
        self.writes.push(TailWrite { cells: write.cells, packed: write.packed.clone() });
        self.cells += write.cells;
        if let Some(members) = self.members.as_mut() {
            members.extend(write.new_members.iter().cloned());
        }
    }

    /// Installs a member set built by [`plan_tail_write`].
    pub(crate) fn install_memo(&mut self, members: BTreeSet<Member>) {
        self.members = Some(members);
    }
}

/// The tails of one key, by the stamp of the sample each belongs to.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct SampleTails {
    stamps: std::collections::BTreeMap<u64, Tail>,
}

impl SampleTails {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.stamps.is_empty()
    }

    pub(crate) fn len(&self) -> usize {
        self.stamps.len()
    }

    pub(crate) fn tail(&self, stamp: u64) -> Option<&Tail> {
        self.stamps.get(&stamp)
    }

    pub(crate) fn tail_mut(&mut self, stamp: u64) -> Option<&mut Tail> {
        self.stamps.get_mut(&stamp)
    }

    /// Whether a sample at `stamp` may start a tail: it has one already, or there is a free slot.
    pub(crate) fn can_hold(&self, stamp: u64) -> bool {
        self.stamps.contains_key(&stamp) || self.stamps.len() < MAX_TAIL_STAMPS
    }

    /// The tail of `stamp`, started over a base of `base_cells` when it has none.
    pub(crate) fn tail_or_start(&mut self, stamp: u64, base_cells: usize) -> Result<&mut Tail, String> {
        if !self.can_hold(stamp) {
            return Err(format!("{MAX_TAIL_STAMPS} samples of the key already hold a tail"));
        }
        Ok(self.stamps.entry(stamp).or_insert_with(|| Tail::new(base_cells)))
    }

    /// The stamps whose tails should be folded to make room for a tail at `stamp`: none while
    /// there is room, else the oldest tails (never `stamp`), enough to leave one free slot.
    pub(crate) fn fold_to_make_room(&self, stamp: u64) -> Vec<u64> {
        if self.can_hold(stamp) {
            return Vec::new();
        }
        let surplus = self.stamps.len() + 1 - MAX_TAIL_STAMPS;
        self.stamps.keys().copied().filter(|s| *s != stamp).take(surplus).collect()
    }

    /// Forgets the tail of `stamp`: its sample was rewritten whole, replaced or removed.
    pub(crate) fn clear(&mut self, stamp: u64) -> bool {
        self.stamps.remove(&stamp).is_some()
    }

    /// Forgets every tail whose stamp `live` does not name: the series no longer holds the sample.
    pub(crate) fn retain_live(&mut self, live: impl Fn(u64) -> bool) -> usize {
        let before = self.stamps.len();
        self.stamps.retain(|stamp, _| live(*stamp));
        before - self.stamps.len()
    }

    /// The stamps that hold a tail, ascending.
    pub(crate) fn stamps(&self) -> impl Iterator<Item = u64> + '_ {
        self.stamps.keys().copied()
    }

    /// Applies one logged write: appends it to the stamp's tail, starting one when there is none.
    /// A write that does not fit is an error: the leader never logs one, so a replay that meets it
    /// has diverged.
    pub(crate) fn apply_item(&mut self, item: &TailItem) -> Result<(), String> {
        let tail = self.tail_or_start(item.stamp, item.base_cells)?;
        if !tail.has_room_for(item.cells) {
            return Err("a logged tail write does not fit the tail it belongs to".to_string());
        }
        tail.writes.push(TailWrite { cells: item.cells, packed: item.packed.clone() });
        tail.cells += item.cells;
        // The cache cannot say which members the write created without the header: drop it.
        tail.members = None;
        Ok(())
    }

    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        for (stamp, tail) in &self.stamps {
            let mut record = Vec::new();
            encode_varint_field(&mut record, 1, *stamp);
            encode_varint_field(&mut record, 2, tail.base_cells as u64);
            for write in &tail.writes {
                let mut inner = Vec::new();
                encode_varint_field(&mut inner, 1, write.cells as u64);
                encode_bytes_field(&mut inner, 2, &write.packed);
                encode_bytes_field(&mut record, 3, &inner);
            }
            encode_bytes_field(&mut out, 1, &record);
        }
        out
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<SampleTails, String> {
        let mut tails = SampleTails::new();
        let mut cursor = 0;
        let mut previous: Option<u64> = None;
        while cursor < bytes.len() {
            expect_tag(bytes, &mut cursor, 1, 2, "sample tail")?;
            let record = read_bytes(bytes, &mut cursor)?;
            let mut at = 0;
            expect_tag(record, &mut at, 1, 0, "sample tail stamp")?;
            let stamp = read_varint(record, &mut at)?;
            if previous.is_some_and(|p| p >= stamp) {
                return Err("sample tails are not in ascending stamp order".to_string());
            }
            previous = Some(stamp);
            expect_tag(record, &mut at, 2, 0, "sample tail base cells")?;
            let base_cells = usize::try_from(read_varint(record, &mut at)?).map_err(|_| "cell count overflows")?;
            let tail = tails.tail_or_start(stamp, base_cells)?;
            while at < record.len() {
                expect_tag(record, &mut at, 3, 2, "sample tail write")?;
                let inner = read_bytes(record, &mut at)?;
                let mut i = 0;
                expect_tag(inner, &mut i, 1, 0, "sample tail write cells")?;
                let cells = usize::try_from(read_varint(inner, &mut i)?).map_err(|_| "cell count overflows")?;
                expect_tag(inner, &mut i, 2, 2, "sample tail write bytes")?;
                let packed = read_bytes(inner, &mut i)?.to_vec();
                if i != inner.len() {
                    return Err("sample tail write has bytes after its last field".to_string());
                }
                if !tail.has_room_for(cells) {
                    return Err("a stored tail is over its bounds".to_string());
                }
                tail.writes.push(TailWrite { cells, packed });
                tail.cells += cells;
            }
            if tail.writes.is_empty() {
                return Err("a stored tail holds no write".to_string());
            }
        }
        if tails.encode() != bytes {
            return Err("sample tails are not in their canonical spelling".to_string());
        }
        Ok(tails)
    }
}

/// Stored as the one byte string [`SampleTails::encode`] writes, as the retry ring is.
impl serde::Serialize for SampleTails {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(&self.encode())
    }
}

impl<'de> serde::Deserialize<'de> for SampleTails {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = SampleTails;

            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("sample tails in their stored byte spelling")
            }

            fn visit_bytes<E: serde::de::Error>(self, bytes: &[u8]) -> Result<SampleTails, E> {
                SampleTails::decode(bytes).map_err(E::custom)
            }

            fn visit_byte_buf<E: serde::de::Error>(self, bytes: Vec<u8>) -> Result<SampleTails, E> {
                self.visit_bytes(&bytes)
            }

            fn visit_seq<A: serde::de::SeqAccess<'de>>(self, mut seq: A) -> Result<SampleTails, A::Error> {
                let mut bytes = Vec::new();
                while let Some(byte) = seq.next_element::<u8>()? {
                    bytes.push(byte);
                }
                self.visit_bytes(&bytes)
            }
        }
        deserializer.deserialize_bytes(Visitor)
    }
}

fn read_varint(bytes: &[u8], cursor: &mut usize) -> Result<u64, String> {
    decode_varint(bytes, cursor).ok_or_else(|| "truncated or overlong varint".to_string())
}

fn read_bytes<'a>(bytes: &'a [u8], cursor: &mut usize) -> Result<&'a [u8], String> {
    let len = usize::try_from(read_varint(bytes, cursor)?).map_err(|_| "length overflows".to_string())?;
    let end = cursor.checked_add(len).ok_or("length overflows")?;
    let out = bytes.get(*cursor..end).ok_or("field runs past the end of the value")?;
    *cursor = end;
    Ok(out)
}

fn expect_tag(bytes: &[u8], cursor: &mut usize, field: u64, wire: u64, what: &str) -> Result<(), String> {
    let tag = read_varint(bytes, cursor)?;
    if tag != (field << 3) | wire {
        return Err(format!("{what}: expected field {field}/{wire}, found {}/{}", tag >> 3, tag & 7));
    }
    Ok(())
}

/// One tail write as a log item carries it: which sample, how big that sample was when the tail
/// started, and the request's packed cells.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TailItem {
    pub(crate) stamp: u64,
    pub(crate) base_cells: usize,
    pub(crate) cells: usize,
    pub(crate) packed: Vec<u8>,
}

impl TailItem {
    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.packed.len() + 16);
        encode_varint_field(&mut out, 1, self.stamp);
        encode_varint_field(&mut out, 2, self.base_cells as u64);
        encode_varint_field(&mut out, 3, self.cells as u64);
        encode_bytes_field(&mut out, 4, &self.packed);
        out
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<TailItem, String> {
        let mut at = 0;
        expect_tag(bytes, &mut at, 1, 0, "tail item stamp")?;
        let stamp = read_varint(bytes, &mut at)?;
        expect_tag(bytes, &mut at, 2, 0, "tail item base cells")?;
        let base_cells = usize::try_from(read_varint(bytes, &mut at)?).map_err(|_| "cell count overflows")?;
        expect_tag(bytes, &mut at, 3, 0, "tail item cells")?;
        let cells = usize::try_from(read_varint(bytes, &mut at)?).map_err(|_| "cell count overflows")?;
        expect_tag(bytes, &mut at, 4, 2, "tail item bytes")?;
        let packed = read_bytes(bytes, &mut at)?.to_vec();
        if at != bytes.len() {
            return Err("tail item has bytes after its last field".to_string());
        }
        let item = TailItem { stamp, base_cells, cells, packed };
        if item.encode() != bytes {
            return Err("tail item is not in its canonical spelling".to_string());
        }
        Ok(item)
    }
}

/// A tail write a request can ride as, with what the write's receipt needs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PlannedWrite {
    /// The request's packed cells, kept as sent.
    pub(crate) packed: Vec<u8>,
    /// Cells the request carried.
    pub(crate) cells: usize,
    /// Cells of the stored sample the tail is over (the first write's base).
    pub(crate) base_cells: usize,
    /// Cells of the request whose member was not in the sample.
    pub(crate) created: usize,
    /// Cells of the request whose member was already there, or earlier in the request.
    pub(crate) merged: usize,
    /// The members the write adds, ascending.
    pub(crate) new_members: Vec<Member>,
}

impl PlannedWrite {
    /// The log item that carries this write for `stamp`.
    pub(crate) fn item(&self, stamp: u64) -> TailItem {
        TailItem { stamp, base_cells: self.base_cells, cells: self.cells, packed: self.packed.clone() }
    }
}

/// What a request becomes at a stamp that already holds a stored sample.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum TailPlan {
    /// One more tail write. `memo` is the member set built to plan it, for the caller to install
    /// when the tail had none (planning is pure and never stores it itself).
    Tail { write: PlannedWrite, memo: Option<BTreeSet<Member>> },
    /// Store the folded and merged sample whole, and clear the tail.
    Whole,
}

/// Every cell of the stored sample with the tail merged in, in the flat form.
fn folded_cells(header: &CellHeader, base_packed: &[u8], tail: Option<&Tail>) -> Result<FlatCells, String> {
    let mut cells = FlatCells::decode(header, base_packed, 0)?;
    if let Some(tail) = tail {
        for write in &tail.writes {
            let incoming = FlatCells::decode(header, &write.packed, 0)?;
            validate_cells_in_request(incoming.len())?;
            cells.merge(header, &incoming)?;
        }
    }
    Ok(cells)
}

/// The value of a sample with a tail: what a whole-sample write of the same requests, in the same
/// order, would have stored, byte for byte.
pub(crate) fn fold_value(base: &[u8], tail: &Tail) -> Result<Vec<u8>, String> {
    let (head, packed) = split_value_raw(base)?;
    let header = CellHeader::decode(head)?;
    folded_cells(&header, packed, Some(tail))?.encode_value(&header)
}

/// The member set of a stored sample with its tail.
fn member_set(cells: &FlatCells) -> BTreeSet<Member> {
    (0..cells.len()).map(|i| owned(cells.cell(i).member)).collect()
}

/// Decides how `request` (a whole value: header, then packed cells) is written into the sample
/// stored as `base`, whose tail is `tail`.
///
/// It rides as a tail write only when the request declares exactly the stored header (the same
/// header bytes: a request that extends the header is stored whole, which upgrades the sample) and
/// the tail has room for it; otherwise [`TailPlan::Whole`]. Refuses what the whole-sample merge
/// refuses (a sample that would pass its cell limit, an invalid cell), so a request is accepted
/// by the same rule on either path.
///
/// The cost is the request's own size when the tail holds its member set, and one decode of the
/// stored sample when it does not.
pub(crate) fn plan_tail_write(base: &[u8], tail: Option<&Tail>, request: &[u8]) -> Result<TailPlan, String> {
    let (head, base_packed) = split_value_raw(base)?;
    let (request_head, request_packed) = split_value_raw(request)?;
    if request_head != head {
        return Ok(TailPlan::Whole);
    }
    let header = CellHeader::decode(head)?;
    let incoming = FlatCells::decode(&header, request_packed, 0)?;
    validate_cells_in_request(incoming.len())?;

    let mut built: Option<BTreeSet<Member>> = None;
    let mut base_cells = tail.map(Tail::base_cells);
    let known: &BTreeSet<Member> = match tail.and_then(|t| t.members.as_ref()) {
        Some(set) => set,
        None => {
            let cells = folded_cells(&header, base_packed, tail)?;
            if base_cells.is_none() {
                // No tail yet: the folded sample is the stored one.
                base_cells = Some(cells.len());
            }
            built = Some(member_set(&cells));
            built.as_ref().expect("just built")
        }
    };
    let base_cells = base_cells.expect("a tail knows its base, and a missing one was just decoded");

    let room_ok = match tail {
        Some(t) => t.has_room_for(incoming.len()),
        None => incoming.len() <= tail_cell_room(base_cells),
    };
    if !room_ok {
        return Ok(TailPlan::Whole);
    }

    let (mut created, mut merged) = (0usize, 0usize);
    let mut added: BTreeSet<Member> = BTreeSet::new();
    for i in 0..incoming.len() {
        let member = owned(incoming.cell(i).member);
        if known.contains(&member) || added.contains(&member) {
            merged += 1;
        } else {
            created += 1;
            added.insert(member);
        }
    }
    if known.len() + added.len() > MAX_CELLS_PER_SAMPLE {
        return Err(format!(
            "a sample holds at most {MAX_CELLS_PER_SAMPLE} cells; this write would make it {}",
            known.len() + added.len()
        ));
    }
    // The memo installed is the set BEFORE this write; the write's own members join it when the
    // write is pushed, so a replan of the same request over it cannot double count.
    let memo = built;
    Ok(TailPlan::Tail {
        write: PlannedWrite {
            packed: request_packed.to_vec(),
            cells: incoming.len(),
            base_cells,
            created,
            merged,
            new_members: added.into_iter().collect(),
        },
        memo,
    })
}

#[cfg(test)]
#[path = "sample_tail_tests.rs"]
mod tests;
