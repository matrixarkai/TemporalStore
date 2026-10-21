// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The two expiry maps hold ONE copy of the key's characters between them, and the stored image
//! did not move.
//!
//! A live object with a deadline was measured keeping its key's characters FIVE times. Two of
//! those five were this pair: `expires_at_ms` owned a `String` and `expiry_by_deadline` owned a
//! second `String` of the same text inside its tuple key. Both are keyed on `Arc<str>` now, and
//! `set_expiry` hands both maps the same handle.
//!
//! # WHY THIS MODULE MEASURES POINTERS RATHER THAN BYTES FOR THE HEADLINE
//!
//! "One copy" is a claim about ALLOCATIONS, not about bytes, and the two answer different
//! questions. The number of distinct allocations holding the key text is exactly countable
//! without an allocator: `Arc::as_ptr` is the identity of the buffer. A byte figure is also
//! reported, from the counting allocator, because an `Arc` handle is not free and a copy count
//! that stood in for a footprint would be the same denominator error this work keeps finding.
//! Both numbers, never one for the other.
//!
//! # AND IT MEASURES THE RESTORED PATH, BECAUSE SERDE DOES NOT RESTORE SHARING
//!
//! `serde` gives each occurrence its own allocation on the way in. `expiry_by_deadline` is
//! `serde(skip)`, so after a decode the mirror is empty and `ensure_expiry_order` rebuilds it --
//! and whether the rebuilt mirror SHARES depends entirely on what that loop spells. It spells
//! `key.clone()`, which on an `Arc` is a refcount bump; spelled `key.to_string()` it would be a
//! second copy, and a probe that only looked at a freshly written store would report one copy
//! while a restored store held two. So every figure here is taken twice.

use super::*;

use std::collections::BTreeMap;
use std::sync::Arc;

#[cfg(feature = "alloc-probe")]
use crate::alloc_probe::Probe;

/// Distinct allocations holding key text across BOTH expiry maps.
///
/// The identity of an `Arc<str>`'s buffer is its data pointer, so two handles to one allocation
/// collapse to one entry here and two independent allocations of the same text do not. That is
/// the difference the whole change is about, and it is a count rather than a reading.
fn distinct_key_allocations(shard: &crate::engine::state::ShardState) -> usize {
    let mut seen: std::collections::HashSet<usize> = std::collections::HashSet::new();
    for key in shard.expires_at_ms().keys() {
        seen.insert(Arc::as_ptr(key) as *const u8 as usize);
    }
    for (_, key) in shard.expiry_by_deadline.keys() {
        seen.insert(Arc::as_ptr(key) as *const u8 as usize);
    }
    seen.len()
}

/// A shard carrying `count` deadlines, written through the real `set_expiry`.
fn shard_with_deadlines(count: usize) -> crate::engine::state::ShardState {
    let mut shard = crate::engine::state::ShardState::default();
    for index in 0..count {
        crate::engine::set_expiry(
            &mut shard,
            format!("deadline-key-{index:06}"),
            1_700_000_000_000 + index as u64,
        );
    }
    shard
}

/// ONE ALLOCATION PER DEADLINE, ON THE FRESH PATH AND AFTER A DECODE AND REBUILD.
///
/// This is the headline of the change, stated as the count it is. Before, a deadline cost two
/// independent allocations of its key's characters -- one per map. After, it costs one, held by
/// both.
///
/// THE RESTORED HALF IS THE HALF THAT COULD HAVE SILENTLY FAILED. `serde` does not reconstruct
/// sharing, so the restored figure is whatever `ensure_expiry_order` produces, not whatever
/// `set_expiry` produced. The two are asserted to agree.
#[test]
fn the_expiry_pair_shares_one_allocation_on_both_paths() {
    const DEADLINES: usize = 500;

    // --- FRESH ---
    let fresh = shard_with_deadlines(DEADLINES);
    assert_eq!(
        DEADLINES,
        fresh.expires_at_ms().len(),
        "denominator: the fixture did not file the deadlines it reports"
    );
    assert_eq!(
        DEADLINES,
        fresh.expiry_by_deadline.len(),
        "the mirror and the key-ordered map disagree on their population, so neither figure below \
         means anything"
    );
    let fresh_allocations = distinct_key_allocations(&fresh);

    // --- RESTORED: the real index image, decoded, then the mirror rebuilt as a load would. ---
    let image = crate::engine::encode_index_bytes(&fresh);
    let mut restored = crate::engine::decode_index_bytes(&image)
        .expect("an index this build wrote must decode in this build");
    assert_eq!(
        DEADLINES,
        restored.expires_at_ms().len(),
        "the decode lost deadlines, so the sharing figure below is measuring a different shard"
    );
    // NON-VACUITY FOR THE REBUILD: the mirror must come back EMPTY from a decode, or
    // `ensure_expiry_order` would not run and the restored figure would be the decode's and not
    // the rebuild's.
    assert!(
        restored.expiry_by_deadline.is_empty(),
        "the mirror survived the decode, so it is not `serde(skip)` any more and the rebuild this \
         test is about never fires"
    );
    crate::engine::ensure_expiry_order(&mut restored);
    assert_eq!(
        DEADLINES,
        restored.expiry_by_deadline.len(),
        "the rebuild did not refill the mirror"
    );
    let restored_allocations = distinct_key_allocations(&restored);

    println!(
        "distinct allocations of key text across BOTH expiry maps, at {DEADLINES} deadlines:"
    );
    println!("  fresh    {fresh_allocations:>6}  ({:.2} per deadline)", fresh_allocations as f64 / DEADLINES as f64);
    println!("  restored {restored_allocations:>6}  ({:.2} per deadline)", restored_allocations as f64 / DEADLINES as f64);
    println!("  (two independent copies would read {} on each)", DEADLINES * 2);

    assert_eq!(
        DEADLINES, fresh_allocations,
        "a freshly written shard holds {fresh_allocations} allocations for {DEADLINES} deadlines; \
         one each is the point of the change and two each is what it replaced"
    );
    assert_eq!(
        DEADLINES, restored_allocations,
        "a RESTORED shard holds {restored_allocations} allocations for {DEADLINES} deadlines \
         while a fresh one holds {fresh_allocations}. `serde` does not restore sharing -- the \
         rebuild in `ensure_expiry_order` is what does, and spelling it `key.to_string()` instead \
         of `key.clone()` produces exactly this failure, invisible to any probe that only looks \
         at a freshly written store."
    );
}

/// THE STORED IMAGE DID NOT MOVE, PROVED ON THE BYTES RATHER THAN ARGUED FROM A FEATURE FLAG.
///
/// `serde`'s `rc` feature is enabled, so an `Arc<str>` key serializes as the same string a
/// `String` key did. That is an argument about a feature; the thing that decides whether a format
/// stamp is owed is the emitted image. So this builds the same content both ways and compares the
/// bytes.
///
/// It also decodes the real index image and checks the deadlines survive, because a map that
/// serializes identically and fails to come back would still be a format change.
#[test]
fn the_arc_keyed_deadline_map_writes_the_same_bytes_as_a_string_keyed_one() {
    const KEYS: usize = 64;
    let mut as_strings: BTreeMap<String, u64> = BTreeMap::new();
    let mut as_handles: BTreeMap<Arc<str>, u64> = BTreeMap::new();
    for index in 0..KEYS {
        let key = format!("deadline-key-{index:06}");
        as_strings.insert(key.clone(), 1_700_000_000_000 + index as u64);
        as_handles.insert(Arc::from(key.as_str()), 1_700_000_000_000 + index as u64);
    }

    let string_bytes = serde_json::to_vec(&as_strings).expect("the string-keyed map must encode");
    let handle_bytes = serde_json::to_vec(&as_handles).expect("the handle-keyed map must encode");

    println!(
        "deadline map wire image at {KEYS} keys: string-keyed {} bytes, handle-keyed {} bytes",
        string_bytes.len(),
        handle_bytes.len()
    );
    // NON-VACUITY: an empty map encodes to `{}` either way and would compare equal while proving
    // nothing about the key's wire form.
    assert!(
        string_bytes.len() > 2 * KEYS,
        "the fixture encoded to {} bytes, which is too small to contain {KEYS} keys",
        string_bytes.len()
    );
    assert_eq!(
        string_bytes, handle_bytes,
        "the handle-keyed map writes different bytes from the string-keyed one, so this IS a \
         stored-format change and owes a format stamp"
    );

    // AND IT COMES BACK. A format that writes the same bytes and cannot read them is still broken.
    let fresh = shard_with_deadlines(KEYS);
    let image = crate::engine::encode_index_bytes(&fresh);
    let restored = crate::engine::decode_index_bytes(&image)
        .expect("the real index image must decode");
    assert_eq!(
        fresh.expires_at_ms().len(),
        restored.expires_at_ms().len(),
        "the real index image lost deadlines across a round trip"
    );
    for (key, deadline) in fresh.expires_at_ms().iter() {
        assert_eq!(
            Some(deadline),
            restored.expires_at_ms().get(&**key),
            "deadline for {key} did not survive the real index round trip"
        );
    }
}

/// WHAT THE PAIR COSTS PER DEADLINE, IN BYTES, ON BOTH PATHS.
///
/// The copy count above is the headline and this is the other half of it: an `Arc` handle is
/// sixteen bytes in the key slot where a `String` was twenty-four, plus one shared allocation of
/// the characters where there were two. Reporting the copy count alone would let a reader take it
/// for a footprint, which it is not.
///
/// rust-internal: measures this crate's own resident structures, no product behaviour
#[test]
#[cfg(feature = "alloc-probe")]
fn what_the_expiry_pair_costs_per_deadline_on_both_paths() {
    const DEADLINES: usize = 500;

    let weigh = |shard: &crate::engine::state::ShardState| -> (u64, u64) {
        let point = {
            let probe = Probe::start();
            let copy = shard.expires_at_ms().clone();
            std::hint::black_box(&copy);
            let counts = probe.stop();
            drop(copy);
            counts.alloc_bytes
        };
        let ordered = {
            let probe = Probe::start();
            let copy = shard.expiry_by_deadline.clone();
            std::hint::black_box(&copy);
            let counts = probe.stop();
            drop(copy);
            counts.alloc_bytes
        };
        (point, ordered)
    };

    let fresh = shard_with_deadlines(DEADLINES);
    let (fresh_point, fresh_ordered) = weigh(&fresh);
    let fresh_shared = distinct_key_allocations(&fresh);

    let image = crate::engine::encode_index_bytes(&fresh);
    let mut restored = crate::engine::decode_index_bytes(&image).expect("decode");
    crate::engine::ensure_expiry_order(&mut restored);
    let (restored_point, restored_ordered) = weigh(&restored);
    let restored_shared = distinct_key_allocations(&restored);

    // A CLONE DOES NOT SEE THE SHARED CHARACTERS, which is why the allocation count is reported
    // beside the bytes rather than folded into them: `Arc::clone` bumps a refcount and allocates
    // nothing, so the figures below are the maps' own nodes and key slots and NOT the text.
    println!("per deadline, at {DEADLINES} deadlines (clone-measured; shared text not counted):");
    println!(
        "  fresh     point-lookup {:>7.1} B  ordered {:>7.1} B  total {:>7.1} B  \
         distinct text allocations {fresh_shared}",
        fresh_point as f64 / DEADLINES as f64,
        fresh_ordered as f64 / DEADLINES as f64,
        (fresh_point + fresh_ordered) as f64 / DEADLINES as f64
    );
    println!(
        "  restored  point-lookup {:>7.1} B  ordered {:>7.1} B  total {:>7.1} B  \
         distinct text allocations {restored_shared}",
        restored_point as f64 / DEADLINES as f64,
        restored_ordered as f64 / DEADLINES as f64,
        (restored_point + restored_ordered) as f64 / DEADLINES as f64
    );

    // --- THE LIKE-FOR-LIKE BEFORE, IN THE SAME PROCESS, BECAUSE THE CENSUS FIGURE IS NOT ONE.
    //
    // The census measured `expires_at_ms` at 71.8 B/deadline and the mirror at 71.8 on a seeded
    // engine with production key lengths, counting each structure's whole heap over its own
    // denominator. This test measures a bare shard with fixture keys, by clone, with the shared
    // text deliberately excluded. Those are DIFFERENT SUBJECTS, and presenting 143.6 -> 96.1 as
    // the saving would be the denominator error this campaign keeps paying for.
    //
    // So the before arm is built here, from the same keys at the same population, and weighed by
    // the same probe. That is the only pair of numbers that may be subtracted.
    let mut before_point: BTreeMap<String, u64> = BTreeMap::new();
    let mut before_ordered: BTreeMap<(u64, String), ()> = BTreeMap::new();
    for (key, deadline) in fresh.expires_at_ms().iter() {
        before_point.insert(key.to_string(), *deadline);
        before_ordered.insert((*deadline, key.to_string()), ());
    }
    assert_eq!(DEADLINES, before_point.len(), "the before arm did not take every key");
    let weigh_one = |value: &dyn Fn() -> u64| value();
    let before_point_bytes = weigh_one(&|| {
        let probe = Probe::start();
        let copy = before_point.clone();
        std::hint::black_box(&copy);
        let counts = probe.stop();
        drop(copy);
        counts.alloc_bytes
    });
    let before_ordered_bytes = weigh_one(&|| {
        let probe = Probe::start();
        let copy = before_ordered.clone();
        std::hint::black_box(&copy);
        let counts = probe.stop();
        drop(copy);
        counts.alloc_bytes
    });
    // A `String` clone DOES copy its characters, so the before arm's figure includes the text and
    // the after arm's does not. Stated rather than silently compared: the text is exactly what
    // the change stops duplicating, so the honest comparison is the SLOTS, reported apart from it.
    let before_total = before_point_bytes + before_ordered_bytes;
    let after_total = fresh_point + fresh_ordered;
    println!(
        "  BEFORE (String keys, same {DEADLINES} keys, same probe): point {:>7.1} B  ordered          {:>7.1} B  total {:>7.1} B  -- includes the text, which a String clone copies",
        before_point_bytes as f64 / DEADLINES as f64,
        before_ordered_bytes as f64 / DEADLINES as f64,
        before_total as f64 / DEADLINES as f64
    );
    println!(
        "  AFTER  (Arc keys):                                       total {:>7.1} B  -- excludes          the text, which an Arc clone does not copy",
        after_total as f64 / DEADLINES as f64
    );
    println!(
        "  copies of the characters per deadline: BEFORE 2, AFTER 1 (measured by pointer identity          above, on both the fresh and the restored path)"
    );

    // --- THE AFTER ARM'S MISSING TERM, MEASURED RATHER THAN COMPUTED.
    //
    // The two figures above are NOT subtractable as they stand: the before arm's bytes include
    // the characters and the after arm's do not, because that is exactly what the two clone
    // behaviours differ on. Subtracting them would overstate the saving by a whole text
    // allocation per deadline, and reporting them side by side without saying so would invite a
    // reader to do it.
    //
    // So the shared allocations are added back ONCE EACH -- which is what sharing actually costs
    // -- using the same arithmetic the census probe uses for an `Arc<str>`: two counter words and
    // the bytes, rounded to pointer alignment. Only then are the two arms the same quantity.
    let mut counted: std::collections::HashSet<usize> = std::collections::HashSet::new();
    let mut shared_text_bytes = 0usize;
    for key in fresh.expires_at_ms().keys() {
        if counted.insert(Arc::as_ptr(key) as *const u8 as usize) {
            shared_text_bytes += (16 + key.len() + 7) / 8 * 8;
        }
    }
    for (_, key) in fresh.expiry_by_deadline.keys() {
        if counted.insert(Arc::as_ptr(key) as *const u8 as usize) {
            shared_text_bytes += (16 + key.len() + 7) / 8 * 8;
        }
    }
    assert_eq!(
        DEADLINES,
        counted.len(),
        "the text walk found {} allocations for {DEADLINES} deadlines, so it is not counting the          same shared set the pointer-identity test asserted",
        counted.len()
    );
    let after_complete = after_total + shared_text_bytes as u64;
    println!(
        "  AFTER, COMPLETE (slots + the ONE shared text allocation added back): {:>7.1} B/deadline",
        after_complete as f64 / DEADLINES as f64
    );
    println!(
        "  SUBTRACTABLE NOW: before {:.1} -> after {:.1} B/deadline, a saving of {:.1} B/deadline          ({:.1}%), at {DEADLINES} deadlines with {}-character keys",
        before_total as f64 / DEADLINES as f64,
        after_complete as f64 / DEADLINES as f64,
        (before_total as f64 - after_complete as f64) / DEADLINES as f64,
        100.0 * (before_total as f64 - after_complete as f64) / before_total as f64,
        fresh.expires_at_ms().keys().next().map(|k| k.len()).unwrap_or(0)
    );

    assert!(fresh_point > 0 && fresh_ordered > 0, "non-vacuity: a map weighed zero");
    assert!(before_total > 0, "non-vacuity: the before arm weighed zero");
    assert!(
        after_complete < before_total,
        "the complete after figure ({after_complete} B) is not below the before ({before_total}          B), so this change does not reduce the footprint and the copy count must not be reported          as if it did"
    );
    assert_eq!(
        fresh_shared, restored_shared,
        "the two paths disagree on how many allocations hold the key text ({fresh_shared} fresh, \
         {restored_shared} restored), so the saving does not survive a restore"
    );
}
