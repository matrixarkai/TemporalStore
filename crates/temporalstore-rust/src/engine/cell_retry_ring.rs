// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The retry ring of a merge-reduce append: a bounded, per-key memory of the last few retry tokens
//! and what each one's first application landed, as a pure type.
//!
//! A merge-reduce append is not idempotent: a client that does not know whether its request
//! landed and sends it again counts every `Sum` and `Count` lane twice. A request may carry a
//! retry token, a caller-chosen `u64`. The first request with a token is applied and its receipt
//! is filed here; the same token again is answered with that receipt and writes nothing.
//!
//! # THE RULES
//!
//! * One ring per key, holding at most [`RETRY_RING_CAPACITY`] tokens. A token is remembered for
//!   the next `RETRY_RING_CAPACITY - 1` distinct tokens after it and is evicted by the one after
//!   that. **A retry that arrives after `RETRY_RING_CAPACITY` newer tokens have been applied is
//!   NOT protected**: it is applied again as a new request. The window is a count of appends, never
//!   a time, so it is the same on a leader, on a follower and on a replay.
//! * Entries are ordered oldest first. Recording at capacity evicts the oldest and nothing else.
//!   A token already in the ring is never recorded again: asking is [`RetryRing::admit`], and
//!   recording a token that is present is refused, so the order cannot be perturbed by a retry.
//! * A ring is a pure function of the requests applied to it. There is no clock, no randomness and
//!   no hashing whose order could vary, so two engines applying the same log hold the same bytes.
//! * An empty ring costs no allocation and, stored, no bytes: the owner skips it. The first record
//!   reserves exactly the capacity, so a ring that is full never allocates again.
//!
//! # BYTES
//!
//! One field-1 length-delimited record per entry, oldest first, each holding fields 1 to 4 in order
//! (token, snapshots written, cells created, cells merged) as varints. The encoding is canonical:
//! [`RetryRing::decode`] refuses a value that is not exactly what [`RetryRing::encode`] writes
//! (unknown, missing, repeated or out-of-order fields, trailing bytes, more entries than the
//! capacity, a token twice), and never panics on a malformed or truncated value.

use crate::types::{decode_varint, encode_bytes_field, encode_varint_field};
use std::collections::VecDeque;

/// Tokens a key remembers. Raising it is a one-line change and costs 32 bytes of memory per slot
/// for every key that uses tokens, because the first record reserves the whole ring.
pub(crate) const RETRY_RING_CAPACITY: usize = 16;

/// What the first application of a token landed: the three counts of the merge receipt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RetryReceipt {
    pub(crate) snapshots_written: u64,
    pub(crate) cells_created: u64,
    pub(crate) cells_merged: u64,
}

/// What a token asks of the ring.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Admission {
    /// Not remembered: apply the request and [`RetryRing::record`] its receipt.
    Fresh,
    /// Remembered: answer this receipt and write nothing.
    Replay(RetryReceipt),
}

/// The last [`RETRY_RING_CAPACITY`] tokens of one key, oldest first.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct RetryRing {
    entries: VecDeque<(u64, RetryReceipt)>,
}

impl RetryRing {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// The tokens held, oldest first.
    #[cfg(test)]
    pub(crate) fn tokens(&self) -> Vec<u64> {
        self.entries.iter().map(|(token, _)| *token).collect()
    }

    /// Whether `token` is remembered, and the receipt of its first application if so. Allocates
    /// nothing.
    pub(crate) fn admit(&self, token: u64) -> Admission {
        match self.entries.iter().find(|(held, _)| *held == token) {
            Some((_, receipt)) => Admission::Replay(*receipt),
            None => Admission::Fresh,
        }
    }

    /// Files the receipt of a token's first application as the newest entry. At capacity the
    /// oldest entry is evicted and its token returned. Refused, and nothing changed, for a token
    /// the ring already holds: that request was a replay and had nothing to record.
    pub(crate) fn record(&mut self, token: u64, receipt: RetryReceipt) -> Result<Option<u64>, String> {
        if self.entries.iter().any(|(held, _)| *held == token) {
            return Err(format!("retry token {token} is already recorded"));
        }
        if self.entries.capacity() < RETRY_RING_CAPACITY {
            self.entries.reserve_exact(RETRY_RING_CAPACITY);
        }
        let evicted = if self.entries.len() >= RETRY_RING_CAPACITY {
            self.entries.pop_front().map(|(token, _)| token)
        } else {
            None
        };
        self.entries.push_back((token, receipt));
        Ok(evicted)
    }

    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.entries.len() * 8);
        let mut record = Vec::new();
        for (token, receipt) in &self.entries {
            record.clear();
            encode_varint_field(&mut record, 1, *token);
            encode_varint_field(&mut record, 2, receipt.snapshots_written);
            encode_varint_field(&mut record, 3, receipt.cells_created);
            encode_varint_field(&mut record, 4, receipt.cells_merged);
            encode_bytes_field(&mut out, 1, &record);
        }
        out
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<RetryRing, String> {
        let mut ring = RetryRing::new();
        let mut cursor = 0;
        while cursor < bytes.len() {
            let tag = read_varint(bytes, &mut cursor)?;
            if tag != (1 << 3) | 2 {
                return Err(format!("unknown retry ring field {}/{}", tag >> 3, tag & 7));
            }
            let len = usize::try_from(read_varint(bytes, &mut cursor)?).map_err(|_| "length overflows".to_string())?;
            let end = cursor.checked_add(len).ok_or("length overflows")?;
            let record = bytes.get(cursor..end).ok_or("entry runs past the end of the value")?;
            cursor = end;
            if ring.entries.len() >= RETRY_RING_CAPACITY {
                return Err(format!("more than {RETRY_RING_CAPACITY} retry tokens"));
            }
            let (token, receipt) = decode_entry(record)?;
            ring.record(token, receipt)?;
        }
        // A varint may be spelled with padding and still read back to the same number; the stored
        // spelling is the one `encode` writes and no other.
        if ring.encode() != bytes {
            return Err("retry ring is not in its canonical spelling".to_string());
        }
        Ok(ring)
    }
}

/// A ring is stored as the one byte string `encode` writes, wherever a serde format carries it:
/// a byte string in the index image, an array of numbers where a format has no byte type. The
/// image is stamped, so there is no second spelling to read.
impl serde::Serialize for RetryRing {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(&self.encode())
    }
}

impl<'de> serde::Deserialize<'de> for RetryRing {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = RetryRing;

            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("a retry ring in its stored byte spelling")
            }

            fn visit_bytes<E: serde::de::Error>(self, bytes: &[u8]) -> Result<RetryRing, E> {
                RetryRing::decode(bytes).map_err(E::custom)
            }

            fn visit_byte_buf<E: serde::de::Error>(self, bytes: Vec<u8>) -> Result<RetryRing, E> {
                self.visit_bytes(&bytes)
            }

            fn visit_seq<A: serde::de::SeqAccess<'de>>(self, mut seq: A) -> Result<RetryRing, A::Error> {
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

/// One entry: fields 1 to 4, each once, in order, nothing after.
fn decode_entry(record: &[u8]) -> Result<(u64, RetryReceipt), String> {
    let mut cursor = 0;
    let mut values = [0u64; 4];
    for (slot, value) in values.iter_mut().enumerate() {
        let tag = read_varint(record, &mut cursor)?;
        if tag != (((slot as u64) + 1) << 3) {
            return Err(format!("retry entry field {}/{} is not field {} as a varint", tag >> 3, tag & 7, slot + 1));
        }
        *value = read_varint(record, &mut cursor)?;
    }
    if cursor != record.len() {
        return Err("retry entry has bytes after its last field".to_string());
    }
    let [token, snapshots_written, cells_created, cells_merged] = values;
    Ok((token, RetryReceipt { snapshots_written, cells_created, cells_merged }))
}

#[cfg(test)]
#[path = "cell_retry_ring_tests.rs"]
mod tests;
