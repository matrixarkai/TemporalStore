// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Stable object/page hashing + routing-bucket helpers, split from engine.rs.
use super::*;

pub(super) fn routing_bucket_count(start_routing_bucket: u32, end_routing_bucket: u32) -> u32 {
    if end_routing_bucket < start_routing_bucket {
        return 0;
    }
    end_routing_bucket
        .saturating_sub(start_routing_bucket)
        .saturating_add(1)
}

pub(super) fn bucket_for_object(key: &str, start_routing_bucket: u32, routing_bucket_count: u32) -> u32 {
    note_routing_bucket_derivation(key);
    if routing_bucket_count == 0 {
        return start_routing_bucket;
    }
    start_routing_bucket + (stable_object_hash(key) % routing_bucket_count as u64) as u32
}

/// WHAT DERIVING A PAGE'S BUCKET COSTS, COUNTED, AND ONLY UNDER TEST.
///
/// The read path used to read a page's bucket off its `BlockAddress` and now derives it from the
/// object key, so what that costs is a number this tree owes rather than an argument. It is a COUNT
/// and not a timing, because a timing on a shared box moves with whatever is building next door.
///
/// `#[cfg(test)]` DELIBERATELY, which is the one place these counters differ from
/// `PAGE_LOOKUP_ENTRIES_EXAMINED`. That one is charged in production because a page lookup is a walk
/// whose length an operator may need to see. This is one FNV-1a pass over a short borrowed string,
/// and two relaxed atomics per read to measure it would cost more than the thing being measured --
/// which is itself the answer the counters exist to report. A count does not vary with the build
/// profile, so measuring it in a test build measures the shipped number.
#[cfg(test)]
pub(super) static ROUTING_BUCKET_DERIVATIONS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
/// The bytes FNV-1a was walked over, summed. This is the work: the hash is one xor and one multiply
/// per byte of key, so the byte total IS the instruction count up to a constant.
#[cfg(test)]
pub(super) static ROUTING_BUCKET_KEY_BYTES: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

#[cfg(test)]
fn note_routing_bucket_derivation(key: &str) {
    ROUTING_BUCKET_DERIVATIONS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    ROUTING_BUCKET_KEY_BYTES.fetch_add(key.len() as u64, std::sync::atomic::Ordering::Relaxed);
}

#[cfg(not(test))]
#[inline(always)]
fn note_routing_bucket_derivation(_key: &str) {}

#[cfg(test)]
pub(super) fn reset_routing_bucket_derivations() {
    ROUTING_BUCKET_DERIVATIONS.store(0, std::sync::atomic::Ordering::Relaxed);
    ROUTING_BUCKET_KEY_BYTES.store(0, std::sync::atomic::Ordering::Relaxed);
}

#[cfg(test)]
pub(super) fn routing_bucket_derivations() -> (u64, u64) {
    (
        ROUTING_BUCKET_DERIVATIONS.load(std::sync::atomic::Ordering::Relaxed),
        ROUTING_BUCKET_KEY_BYTES.load(std::sync::atomic::Ordering::Relaxed),
    )
}

/// THE SAME TWO NUMBERS FOR THE OBJECT IDENTITY, WHICH IS THE OTHER HASH A READ PAYS FOR.
///
/// A read already walks its key once to place it in a bucket, and the counters above are what that
/// costs. Deriving the object identity rather than reading it off the page entry walks the SAME key
/// a second time, and these are what THAT costs -- so the pair of them is the whole price of the
/// entry no longer carrying the id, stated as a count of passes and a count of bytes.
///
/// A COUNT AND NOT A TIMING, for the reason the routing-bucket counters state and this tree has
/// measured: an instrument on this path read 485x idle against 11x busy off identical code. A count
/// does not move with whatever is building next door.
///
/// `#[cfg(test)]` for the same reason as well. Two relaxed atomics per derivation would cost more
/// than one FNV-1a pass over a short borrowed string, which is itself the answer these report.
#[cfg(test)]
pub(super) static OBJECT_ID_DERIVATIONS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
/// The bytes the identity hash was walked over, summed. One xor and one multiply a byte, so this
/// is the instruction count up to a constant -- the same accounting as `ROUTING_BUCKET_KEY_BYTES`.
///
/// COUNTED OVER KIND AND KEY BOTH, because both are terms of the hash. The routing bucket walks the
/// key alone, so these two byte totals are NOT comparable as a ratio without saying so, and the
/// probe that reads them prints each with its own denominator.
#[cfg(test)]
pub(super) static OBJECT_ID_KEY_BYTES: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

#[cfg(test)]
fn note_object_id_derivation(kind: &str, key: &str) {
    OBJECT_ID_DERIVATIONS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    OBJECT_ID_KEY_BYTES.fetch_add((kind.len() + key.len()) as u64, std::sync::atomic::Ordering::Relaxed);
}

#[cfg(not(test))]
#[inline(always)]
fn note_object_id_derivation(_kind: &str, _key: &str) {}

#[cfg(test)]
pub(super) fn reset_object_id_derivations() {
    OBJECT_ID_DERIVATIONS.store(0, std::sync::atomic::Ordering::Relaxed);
    OBJECT_ID_KEY_BYTES.store(0, std::sync::atomic::Ordering::Relaxed);
}

#[cfg(test)]
pub(super) fn object_id_derivations() -> (u64, u64) {
    (
        OBJECT_ID_DERIVATIONS.load(std::sync::atomic::Ordering::Relaxed),
        OBJECT_ID_KEY_BYTES.load(std::sync::atomic::Ordering::Relaxed),
    )
}

const FNV1A64_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
const FNV1A64_PRIME: u64 = 0x0000_0100_0000_01b3;

pub(super) fn stable_object_hash(key: &str) -> u64 {
    stable_object_hash_bytes(key.as_bytes())
}

pub(super) fn stable_object_hash_bytes(bytes: &[u8]) -> u64 {
    let mut hash = FNV1A64_OFFSET_BASIS;
    stable_object_hash_update(&mut hash, bytes);
    hash
}

/// Initial value for a streaming stable_object_hash_update sequence.
pub(super) fn stable_object_hash_begin() -> u64 {
    FNV1A64_OFFSET_BASIS
}

pub(super) fn stable_object_hash_update(hash: &mut u64, bytes: &[u8]) {
    for byte in bytes {
        *hash ^= *byte as u64;
        *hash = hash.wrapping_mul(FNV1A64_PRIME);
    }
}

pub(super) fn stable_object_hash_update_u64_decimal(hash: &mut u64, mut value: u64) {
    let mut buf = [0_u8; 20];
    let mut pos = buf.len();
    if value == 0 {
        pos -= 1;
        buf[pos] = b'0';
    } else {
        while value > 0 {
            pos -= 1;
            buf[pos] = b'0' + (value % 10) as u8;
            value /= 10;
        }
    }
    stable_object_hash_update(hash, &buf[pos..]);
}

/// THE IDENTITY OF AN OBJECT, WHICH IS ITS KIND AND ITS KEY AND NOTHING ELSE.
///
/// The component USED to be folded in here, and the consequence was that an object held exactly
/// one page: a hash of twenty-five fields was twenty-five objects that happened to share a key.
/// #1986 recorded the result as "the object id names a page's triple, not an object" -- a
/// description of this defect, not a property to preserve.
///
/// THE PARAMETER IS REMOVED RATHER THAN IGNORED. Keeping `component: Option<&str>` and dropping
/// its value on the floor would leave all twenty-six call sites compiling while the meaning
/// changed underneath them, and the fifteen that pass a real component are exactly the sites
/// whose identity MOVES. Removing it makes the compiler name every one.
///
/// The element has not gone anywhere: `BlockIndex::component` is its own serialized field, the
/// registry key is `(u64, Option<Arc<str>>)` since #2013, and `StagedBlock` carries it. What
/// changes is only what the ID means -- the object, not the page.
pub(crate) fn stable_block_object_id(shard_id: ShardId, kind: &str, key: &str) -> u64 {
    note_object_id_derivation(kind, key);
    let mut hash = FNV1A64_OFFSET_BASIS;
    stable_object_hash_update_u64_decimal(&mut hash, shard_id as u64);
    stable_object_hash_update(&mut hash, b":");
    stable_object_hash_update(&mut hash, kind.as_bytes());
    stable_object_hash_update(&mut hash, b":");
    stable_object_hash_update(&mut hash, key.as_bytes());
    hash
}

/// WHICH PAGE A READ WANTS, STATED BY THE CALLER AND NOT READ OFF THE ADDRESS.
///
/// THIS TYPE EXISTS TO MAKE "I CANNOT SAY" A BUILD FAILURE. `engine::read_block_bytes` used to take
/// the element alone and recover the object from `address.object_id()`, an `Option` it consumed with
/// `and_then`. That shape is safe for the routing bucket, where an absent value only weakens a cache
/// key, and it is NOT safe here: `block_in_wal::read_block` serves a page whose only durable copy is
/// its WAL record, so a reader that cannot name the object does not read a stale page -- it reads
/// MISSING for a durably acknowledged write, which is the exact hole `block_in_wal` exists to close.
/// An `Option<u64>` threaded to the same sites compiles with `None` and loses a page in silence.
///
/// SO THERE IS NO CONSTRUCTOR THAT TAKES AN ID. [`PageIdentity::of`] takes the TERMS -- the shard,
/// the kind and the key -- and derives the identity here. A caller cannot pass an id it computed
/// somewhere else, cannot pass a stale one it found on an address, and cannot pass none at all: the
/// compiler names every site that has no key in hand. That is the same doctrine
/// `compaction::compact_block_addresses` states for the routing bucket, where
/// `Item = (u32, &mut BlockAddress)` "makes the caller say it, and the compiler name any caller
/// that cannot" -- one level stricter, because the terms rather than the answer are what crosses.
///
/// THE ELEMENT IS PART OF THE IDENTITY, NOT AN OPTION BESIDE IT. `None` means the page IS its whole
/// object -- a string, a control state -- and `Some` names the element within it, exactly as
/// `BlockIndex::component` beside the address does. One WAL record carries many pages and picks one
/// out of itself by identity, so a read handed only the object answers with the FIRST page of that
/// object: another element's bytes rather than a miss. Keeping the two halves in one value is what
/// stops a site passing the object and forgetting the element.
///
/// AND THE COMPONENT IS NOT A TERM OF THE HASH, which is the part that moved under #2019. The
/// object id is `(shard, kind, key)` and names the OBJECT; the component names the element and is
/// carried beside the id rather than folded into it. Both fields are here for that reason: they
/// answer two different questions and a single number cannot answer both.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct PageIdentity<'a> {
    object_id: u64,
    component: Option<&'a str>,
}

impl<'a> PageIdentity<'a> {
    /// THE ONLY CONSTRUCTOR, AND IT TAKES THE TERMS RATHER THAN THE ANSWER.
    ///
    /// Deriving here rather than accepting a `u64` is the whole point of the type: a caller that
    /// holds the key can always produce an identity, and a caller that does not cannot produce one
    /// at all. There is deliberately no `from_object_id`, for the reason
    /// [`BlockAddress::from_parts`] has no `generation` parameter -- a constructor that accepted a
    /// precomputed value would invite a caller to pass one that disagrees with the terms, which is
    /// the single thing a derivation cannot honour.
    pub(super) fn of(shard_id: ShardId, kind: &str, key: &str, component: Option<&'a str>) -> Self {
        Self {
            object_id: stable_block_object_id(shard_id, kind, key),
            component,
        }
    }

    /// The object this page belongs to.
    pub(super) fn object_id(self) -> u64 {
        self.object_id
    }

    /// Which element of that object, or `None` when the page is the whole of it.
    pub(super) fn component(self) -> Option<&'a str> {
        self.component
    }
}

pub(super) fn block_routing_bucket(key: &str, start_routing_bucket: u32, end_routing_bucket: u32) -> u32 {
    bucket_for_object(
        key,
        start_routing_bucket,
        routing_bucket_count(start_routing_bucket, end_routing_bucket),
    )
}

#[cfg(test)]
mod derived_bucket_range {
    use super::block_routing_bucket;

    /// A DERIVED BUCKET IS INSIDE THE RANGE IT WAS DERIVED ON, and two places rely on that rather
    /// than filtering for it.
    ///
    /// `rebuild_bucket_block_ownership` and `bucket_object_block_ownership_report_from_entries` both
    /// used to carry `if routing_bucket < start || routing_bucket > end { continue; }` after this
    /// call. That filter existed for a bucket carried EXPLICITLY on a `BlockAddress`, which no
    /// address carries any more -- so its only remaining input would be this function, and this is
    /// the statement that says it has nothing to catch. Asserted over many keys and several ranges,
    /// because "start + hash % count" is obviously in range right up until `count` is zero.
    #[test]
    fn a_derived_bucket_is_always_inside_the_range_it_was_derived_on() {
        // The degenerate range is the interesting one: `end < start` makes the count zero, and a
        // modulo by zero would panic rather than return anything at all.
        let ranges = [
            (0u32, u32::MAX),
            (0, 1023),
            (1024, 2047),
            (7, 7),
            (u32::MAX - 1, u32::MAX),
            (5, 4),
        ];
        let mut checked = 0usize;
        for (start, end) in ranges {
            for i in 0..2_000u32 {
                let key = format!("k{i}:{start}:{end}");
                let bucket = block_routing_bucket(&key, start, end);
                checked += 1;
                if end < start {
                    assert_eq!(
                        bucket, start,
                        "a degenerate range must answer its start, not panic or wrap"
                    );
                    continue;
                }
                assert!(
                    bucket >= start && bucket <= end,
                    "key {key} routed to {bucket}, outside {start}..={end}"
                );
            }
        }
        assert_eq!(
            checked,
            ranges.len() * 2_000,
            "denominator: every key of every range has to be checked, or this passes by not looking"
        );
    }

    /// THE CONTROL, because a range check that cannot fail is not a range check.
    ///
    /// The same assertion against a range the bucket was NOT derived on has to break -- otherwise
    /// the loop above would pass for a function that ignored its arguments entirely.
    #[test]
    fn the_range_assertion_fails_against_a_range_the_bucket_was_not_derived_on() {
        let mut outside = 0usize;
        for i in 0..2_000u32 {
            let key = format!("control{i}");
            let bucket = block_routing_bucket(&key, 0, u32::MAX);
            if bucket > 1023 {
                outside += 1;
            }
        }
        assert!(
            outside > 1_900,
            "only {outside} of 2,000 whole-keyspace buckets landed above 1023, so the check above \
             is not discriminating between ranges"
        );
    }
}
