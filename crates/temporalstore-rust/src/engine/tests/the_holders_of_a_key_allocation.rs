// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! HOW MANY RESIDENT STRUCTURES HOLD ONE KEY ALLOCATION, COUNTED ON A WALK THAT REACHES THE
//! CANONICAL HOLDER.
//!
//! Two levers on the 32.0 B/key key-character row are priced on this count, and they move in
//! OPPOSITE directions as it changes, so the count is a premise and not a detail:
//!
//!   * `Box<str>` -- drop the sixteen-byte refcount header. Worth taking at ONE holder, where an
//!     `Arc` pays for sharing that never happens. At TWO it is a wash on the published model
//!     (`32` for one shared allocation against `16` twice for two owned ones) and a LOSS of
//!     16 B/key on what the allocator sets aside, because glibc serves nothing under a 32-byte
//!     chunk.
//!   * the key-character ARENA -- an eight-byte span into a contiguous buffer. Its payload saving
//!     does not turn on the count, but its holder-slot term does.
//!
//! `every_resident_key_allocation_has_exactly_the_holders_the_header_is_priced_for` asserts the
//! count is ONE and concludes from that number, in words, that "the refusal this number used to
//! support has expired". This file measures the same count on a walk that also reaches
//! `CoreIndex::objects`, and reports TWO.
//!
//! # WHY THE EXISTING WALK CANNOT SEE THE SECOND HOLDER
//!
//! It walks `object_block_lookup`, `strings` and `dirty_objects`. The allocation it is counting is
//! minted by none of them: `CoreIndex::any_shared_object_key` resolves the canonical handle by
//! walking `self.objects` FIRST and cloning the `Arc` it finds in the slot row, and
//! `adopt_object_key_allocations` is what points every other `ModelKey`-keyed map at that handle.
//! So the slot row is the holder the others adopt FROM, and it is the one structure the walk omits.
//!
//! That omission has already cost this campaign one figure. `walk_bucket_index_strings` carries the
//! note: the census walked the lookup alone, the refs fold moved each object's refs into its
//! `ObjectSlot` so a shard of strings has an EMPTY lookup, and "the key-character row read 0.0
//! B/key while every key was still allocated and held". The census was fixed by adding the
//! `objects` walk. The two HOLDER arms were not, and they are what the header price rests on.
//!
//! An `Arc` is immune to a clone-based byte probe -- cloning one is a refcount bump -- so a missing
//! holder is invisible to every bytes figure in this group. Pointer identity is the only instrument
//! that answers it, which is what `adopt_object_key_allocations` says in its own note.
//!
//! rust-internal: measures this crate's own resident structures, no product behaviour

use crate::types::{Command, ExecuteRequest};
use crate::TemporalEngine;
use std::collections::HashMap as Counter;
use std::sync::Arc;

/// The published amplification fixture's engine, on the shipped routing range.
fn holder_engine_on(dir: &std::path::Path, end_routing_bucket: u32) -> Arc<TemporalEngine> {
    let engine = Arc::new(TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    ));
    let response = engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 1,
        table_name: "holders-of-a-key-allocation".to_string(),
        shard_uri: "local://holders-of-a-key-allocation/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket,
        readonly: false,
        load_version: 1,
        local_node_id: Some(1),
    });
    assert!(
        response.status.ok,
        "the fixture could not load shard 1 on 0..{end_routing_bucket}: {:?}",
        response.status
    );
    engine
}

/// THE PRICED HOLDER WALK CANNOT SEE THE SLOT ROW THAT OWNS THE KEY.
///
/// # WHAT IS MEASURED, AND WHY BOTH WALKS RUN IN ONE ARM
///
/// The same corpus, at the same steady state, counted twice: once over exactly the structures the
/// priced arm walks, and once over those PLUS `bucket_index.objects`. Running both here is what
/// makes this a correction rather than a competing figure -- a separate arm reporting two could be
/// a different fixture, a different phase or a different dedupe rule, and the reader would have no
/// way to tell which walk was wrong.
///
/// The corpus is the published amplification arm's own: 1,792 `amp-{index:06}` strings with
/// 256-byte values, dumped through the production lifecycle, so the count is the one the published
/// 32.0 B/key row is taken at and not a count at some other phase.
///
/// # THE ASSERTIONS ARE THE PREMISES
///
/// Each step of the chain fails on its own, so a future change says which link moved:
///
///   1. the fixture seeded the corpus it claims (`strings` holds every key);
///   2. the slot row is POPULATED -- an empty structure contributes no holders, which lowers the
///      count in the direction that flatters `Box<str>`, so an unexercised slot row is reported as
///      unexercised rather than counted as a zero;
///   3. the slot row's key is the SAME ALLOCATION as `strings`'s, by data pointer, so it is a
///      holder and not a second equal copy;
///   4. the blind walk reports one holder -- which reproduces the priced arm's figure here, rather
///      than taking it on trust from another file;
///   5. the complete walk reports two.
///
/// If the refs fold or a later change really does leave one holder, step 3 or step 5 fails and says
/// to re-price rather than silently restoring the `Box<str>` lever.
///
///   cargo test -p temporalstore-rust --lib engine::tests::the_holders_of_a_key_allocation::the_priced_holder_walk_cannot_see_the_slot_row_that_owns_the_key -- --exact --ignored --nocapture
#[test]
#[ignore = "seeds a corpus and dumps it through the production lifecycle; run by name"]
fn the_priced_holder_walk_cannot_see_the_slot_row_that_owns_the_key() {
    /// The published arm's 1x phase: 7/8 of 2,048, so the hash tables sit at exactly full load.
    const KEYS: usize = 1_792;
    const SHIPPED_END: u32 = 1023;
    /// What the priced arm asserts today, as a number this arm can disagree with.
    const PRICED_FOR: usize = 1;

    let dir = tempfile::tempdir().expect("tempdir");
    let engine = holder_engine_on(dir.path(), SHIPPED_END);
    for index in 0..KEYS {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet { key: format!("amp-{index:06}"), value: vec![b'v'; 256] },
        });
        assert!(response.status.ok, "write {index}: {:?}", response.status);
    }

    let candidates: Vec<u32> = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard is loaded");
        shard.bucket_index.bucket_map.keys().copied().collect()
    };
    assert!(!candidates.is_empty(), "{KEYS} keys produced no bucket to dump");
    let lifecycle =
        engine.apply_storage_lifecycle(crate::engine::reports::StorageLifecycleRequest {
            shard_id: 1,
            selected_dump_buckets: candidates.clone(),
            max_dump_buckets_per_round: candidates.len(),
            ..Default::default()
        });
    std::hint::black_box(&lifecycle);

    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    assert_eq!(
        0,
        shard.dirty_objects.len(),
        "the dump left the shard dirty, so this is not the steady state the published row is taken \
         at and the dirty index would count as a holder it is not at rest"
    );

    // --- (1) THE FIXTURE FLOOR.
    assert_eq!(
        KEYS,
        shard.strings().len(),
        "the string map holds {} of {KEYS} keys, so every count below is about a corpus this arm \
         did not seed",
        shard.strings().len()
    );

    // --- (2) THE SLOT ROW'S POPULATION, per kind, reported rather than assumed.
    let mut slot_rows: Vec<(&'static str, usize)> = Vec::new();
    for (kind, table) in &shard.bucket_index.objects {
        slot_rows.push((kind.as_str(), table.len()));
    }
    slot_rows.sort_unstable();
    let slot_row_keys: usize = slot_rows.iter().map(|(_, n)| *n).sum();

    // --- THE TWO WALKS. `blind` is exactly the structure list the priced arm uses; `complete`
    // --- adds the canonical holder. Both dedupe by DATA POINTER, the identity `arc_str_bytes`
    // --- uses, so "the same allocation" means here what it means in the published row.
    let mut blind: Counter<usize, Vec<&'static str>> = Counter::new();
    let mut complete: Counter<usize, Vec<&'static str>> = Counter::new();
    for (_model_id, object_key, _refs) in shard.bucket_index.object_block_lookup.iter() {
        blind.entry(object_key.as_ptr() as usize).or_default().push("object_block_lookup");
        complete.entry(object_key.as_ptr() as usize).or_default().push("object_block_lookup");
    }
    for key in shard.strings().keys() {
        blind.entry(key.as_ptr() as usize).or_default().push("strings");
        complete.entry(key.as_ptr() as usize).or_default().push("strings");
    }
    for key in shard.dirty_objects.iter() {
        blind.entry(key.as_ptr() as usize).or_default().push("dirty_objects");
        complete.entry(key.as_ptr() as usize).or_default().push("dirty_objects");
    }
    // The one structure the priced walk omits, and the one `any_shared_object_key` reads first.
    for table in shard.bucket_index.objects.values() {
        for key in table.keys() {
            complete.entry(key.as_ptr() as usize).or_default().push("bucket_index.objects");
        }
    }

    let spread = |walk: &Counter<usize, Vec<&'static str>>| -> Vec<(usize, usize)> {
        let mut by_count: Counter<usize, usize> = Counter::new();
        for who in walk.values() {
            *by_count.entry(who.len()).or_insert(0) += 1;
        }
        let mut out: Vec<(usize, usize)> = by_count.into_iter().collect();
        out.sort_unstable();
        out
    };
    let blind_spread = spread(&blind);
    let complete_spread = spread(&complete);

    // --- (3) THE SLOT ROW SHARES THE ALLOCATION, by pointer. Contents cannot tell a shared
    // --- allocation from a second equal copy; pointers can.
    let string_ptrs: std::collections::HashSet<usize> =
        shard.strings().keys().map(|k| k.as_ptr() as usize).collect();
    let mut slot_ptrs_shared_with_strings = 0usize;
    let mut slot_ptrs_own = 0usize;
    for table in shard.bucket_index.objects.values() {
        for key in table.keys() {
            if string_ptrs.contains(&(key.as_ptr() as usize)) {
                slot_ptrs_shared_with_strings += 1;
            } else {
                slot_ptrs_own += 1;
            }
        }
    }

    println!();
    println!("  {KEYS} keys at the steady state, counted two ways");
    println!();
    println!("    slot rows in bucket_index.objects, by kind");
    for (kind, len) in &slot_rows {
        println!("      {kind:<28} {len:>6} keys");
    }
    println!("      {:<28} {slot_row_keys:>6} keys", "TOTAL");
    println!();
    println!("    the walk the priced arm uses (object_block_lookup + strings + dirty_objects)");
    for (count, allocations) in &blind_spread {
        println!("      {count} holder(s)   {allocations:>7} allocations");
    }
    println!("      distinct key allocations {}", blind.len());
    println!();
    println!("    the same corpus, walking bucket_index.objects as well");
    for (count, allocations) in &complete_spread {
        println!("      {count} holder(s)   {allocations:>7} allocations");
    }
    println!("      distinct key allocations {}", complete.len());
    println!();
    println!(
        "    slot-row keys sharing the string map's allocation {slot_ptrs_shared_with_strings}, \
         holding their own {slot_ptrs_own}"
    );
    println!();
    println!("  WHAT THIS DECIDES. At TWO holders one shared allocation replaces two owned ones,");
    println!("  so the refcount header is exactly paid for on the published model and the");
    println!("  allocator charges 16 B/key MORE for the header-free shape. The `Box<str>` lever");
    println!("  is CLOSED, not expired.");
    println!();

    assert!(
        slot_row_keys > 0,
        "bucket_index.objects holds no key at all, so this arm's second walk is NOT EXERCISED \
         rather than measuring one holder. An empty structure contributes no holders and lowers \
         the count in the direction that flatters the header-free lever"
    );
    assert_eq!(
        KEYS, slot_ptrs_shared_with_strings,
        "{slot_ptrs_shared_with_strings} of {KEYS} slot-row keys are the same allocation as the \
         string map's, and {slot_ptrs_own} hold their own copy. The slot row is where \
         `any_shared_object_key` reads the canonical handle, so every other map should have \
         adopted it -- if these are separate allocations the sharing invariant has regressed and \
         `which_resident_maps_hold_their_own_key_characters` should have caught it first"
    );
    assert_eq!(
        vec![(PRICED_FOR, KEYS)],
        blind_spread,
        "the blind walk no longer reproduces the priced arm's spread of `{PRICED_FOR} holder for \
         every one of {KEYS} allocations`. This arm exists to correct that figure, so it must \
         first MEASURE it here rather than quote it from another file -- if the blind walk has \
         moved, re-read what the priced arm now reports before trusting the correction below"
    );
    assert_eq!(
        vec![(2usize, KEYS)],
        complete_spread,
        "walking the slot row as well gives a spread of {complete_spread:?} rather than `2 \
         holders for every one of {KEYS} allocations`. TWO is what makes the sixteen-byte \
         refcount header paid for: one shared allocation replaces two owned ones, a wash on the \
         published model and a 16 B/key LOSS on what the allocator sets aside. If this reads ONE, \
         the slot row has genuinely stopped holding the key and the header-free lever is open \
         again -- re-price it and say so. If it reads more, the header is better than paid for. \
         Either way re-price rather than loosening this assertion"
    );
}

/// AN ORDER-BOUND LEDGER ADOPTS THE KEY ALLOCATION, AND A SPAN CANNOT ORDER.
///
/// # THE GATE THIS ARM RECORDS
///
/// The arena replaces each holder's `Arc<str>` with an eight-byte span into a contiguous buffer.
/// The 32.0 B/key payload row is ONE allocation shared by every holder, so it is emptied only when
/// EVERY holder takes a span -- a holder that keeps its `Arc` keeps the allocation alive, and the
/// arena then holds a second copy of the characters instead of replacing the first.
///
/// `hashbrown::HashTable` serves a `HashMap` holder, because it takes a caller-supplied hash and so
/// does not need the key to `Borrow<str>`. It does nothing for an ORDERED holder. A `BTreeMap` has
/// no raw-entry API and no comparator hook, so a span-keyed one would order by arena OFFSET, and
/// there is nowhere to hand it the arena so it could order by characters instead.
///
/// And ordered holders DO adopt the canonical allocation, deliberately: `adopt_key_in_ordered`
/// exists for exactly that and is called on `control_state_uuid` and
/// `context_compression_watermark`, both `BTreeMap<ModelKey, u64>`. Their container is ordered for
/// a stated reason: `SideTableShape::U64s` holds that a hash order "would make those walks depend
/// on a per-process seed". Arena-offset order IS insertion order, which is exactly the per-process
/// order that reason rules out.
///
/// THE REASON IS CITED WITHOUT ITS COUNT, WHICH IS STALE. That doc says three production sites
/// iterate the expiry table; recounted on 957d6ad9d the order-dependent production sites are TWO,
/// `engine_lifecycle.rs:1457` and `engine.rs:5708`. The gate does not turn on how many there are --
/// one order-dependent walk is enough to make the order load-bearing -- so this arm asserts the
/// adoption and the ordering rather than a walk census that would go stale the same way.
///
/// So the arena's remaining gate is not the dependency, which is free
/// (`the_span_keyed_table_the_arena_lever_needs_is_not_a_dependency` records that), and it is not
/// confined to the slot saving `object_block_lookup` blocks. For any key an ordered ledger holds,
/// it blocks the PAYLOAD row too, and the lever inverts: the arena's grown capacity is added to a
/// 32.0 row that does not go away.
///
/// # WHAT IS ASSERTED
///
/// The two premises, so this fails if either moves rather than when a conclusion stops being
/// fashionable: that ordered `ModelKey` ledgers adopt the canonical allocation at all, read off the
/// adoption site; and that a span's natural order is NOT its characters' order, demonstrated on a
/// corpus whose insertion order differs from its sorted order.
///
/// Carries both controls, because a scan that silently matches nothing is indistinguishable from
/// one that found nothing.
///
/// rust-internal: reads this crate's own source and layout, no product behaviour
///
///   cargo test -p temporalstore-rust --lib engine::tests::the_holders_of_a_key_allocation::an_order_bound_ledger_adopts_the_key_allocation_a_span_cannot_order -- --exact --nocapture
#[test]
fn an_order_bound_ledger_adopts_the_key_allocation_a_span_cannot_order() {
    // --- PREMISE ONE: ordered ModelKey ledgers adopt the canonical key allocation.
    let source = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join("engine")
        .join("engine_storage_state.rs");
    let text = std::fs::read_to_string(&source)
        .unwrap_or_else(|error| panic!("reading {}: {error}", source.display()));

    // POSITIVE CONTROL: the file read really is the one that declares the alias and the adoption
    // door. Without it a moved declaration would read as "nothing adopts" forever.
    assert!(
        text.contains("pub(super) type ModelKey = Arc<str>;"),
        "{} does not declare `ModelKey`, so whatever it says about adoption is about some other \
         file",
        source.display()
    );
    assert!(
        text.contains("fn adopt_object_key_allocations"),
        "{} does not hold the adoption door, so the call census below is counting nothing",
        source.display()
    );

    // A CALL AND NOT A MENTION, and comments are excluded -- a doc line naming the function is not
    // a map adopting anything. The declaration itself is excluded for the same reason.
    let count_calls = |body: &str| -> usize {
        body.lines()
            .map(str::trim)
            .filter(|line| !line.starts_with("//"))
            .filter(|line| line.contains("adopt_key_in_ordered("))
            .filter(|line| !line.contains("fn adopt_key_in_ordered("))
            .count()
    };
    let ordered_adoptions = count_calls(&text);

    // NEGATIVE CONTROL: the predicate catches a planted call, and ignores a planted COMMENT.
    let planted =
        format!("{text}\n        moved |= adopt_key_in_ordered(self.planted_mut(), k, &c);\n");
    assert_eq!(
        ordered_adoptions + 1,
        count_calls(&planted),
        "the predicate cannot see a planted `adopt_key_in_ordered` call, so its count on the real \
         file means nothing"
    );
    let planted_comment = format!("{text}\n        // moved |= adopt_key_in_ordered(a, b, &c);\n");
    assert_eq!(
        ordered_adoptions,
        count_calls(&planted_comment),
        "the predicate counted a COMMENTED-OUT call, so its count includes prose and not just \
         adoptions"
    );

    // --- PREMISE TWO: a span's natural order is not its characters' order.
    #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
    struct Span {
        at: u32,
        len: u16,
    }
    const _: () = assert!(std::mem::size_of::<Span>() == 8);
    const _: () = assert!(std::mem::size_of::<std::sync::Arc<str>>() == 16);

    // A corpus whose insertion order is deliberately NOT its sorted order, which is the only shape
    // that can tell the two orders apart.
    let inserted = ["zeta", "alpha", "mid"];
    let mut arena = String::new();
    let mut spans: Vec<(Span, &str)> = Vec::new();
    for one in inserted {
        let at = arena.len() as u32;
        arena.push_str(one);
        spans.push((Span { at, len: one.len() as u16 }, one));
    }
    let mut by_span = spans.clone();
    by_span.sort_by_key(|(span, _)| *span);
    let span_order: Vec<&str> = by_span.iter().map(|(_, t)| *t).collect();
    let mut by_chars = spans.clone();
    by_chars.sort_by_key(|(_, t)| *t);
    let char_order: Vec<&str> = by_chars.iter().map(|(_, t)| *t).collect();

    println!();
    println!("  ordered `ModelKey` ledgers adopting the canonical allocation: {ordered_adoptions}");
    println!("  inserted        {inserted:?}");
    println!("  span order      {span_order:?}   (arena offset = insertion order)");
    println!("  character order {char_order:?}");
    println!();
    println!("  So a span-keyed `BTreeMap` iterates in insertion order. `SideTableShape::U64s` is");
    println!("  ordered precisely so its production walks do not depend on a per-process order,");
    println!("  and there is no comparator hook to hand it the arena. For any key an ordered");
    println!("  ledger holds, the 32.0 B/key payload row cannot be emptied, and the arena's grown");
    println!("  capacity is ADDED to it rather than replacing it.");
    println!();

    assert!(
        ordered_adoptions > 0,
        "no ordered `ModelKey` ledger adopts the canonical key allocation any more. That is the \
         premise this gate rests on: if every ordered holder now mints its own copy, an arena \
         span in the hashed holders no longer leaves a live `Arc` behind, and the payload row is \
         reachable -- re-price the arena rather than loosening this assertion"
    );
    assert_ne!(
        span_order, char_order,
        "a span sorted by offset gave the same order as the characters, so this corpus cannot \
         tell the two orders apart and the ordering premise is untested rather than established"
    );
    assert_eq!(
        inserted.to_vec(),
        span_order,
        "span order is not insertion order, so arena offsets are not being assigned in the order \
         keys arrive and the argument above does not describe this arena"
    );
}

/// WHERE A KEY WITH A DEADLINE IS HELD, AND WHICH OF THOSE HOLDERS ARE ORDERED.
///
/// # WHY THE PUBLISHED CORPUS CANNOT ANSWER THIS
///
/// `the_priced_holder_walk_cannot_see_the_slot_row_that_owns_the_key` counts holders on the
/// amplification arm's own corpus, which is strings with NO deadline. That corpus never arms
/// `expires_at_ms` or `expiry_by_deadline`, so every ordered structure in the arena's gate is empty
/// there and reads as a map with nothing to share -- the unexercised-structure reading this file
/// exists to refuse. This arm writes the same keys with a deadline and walks the same census.
///
/// # THE TWO ORDERED HOLDERS A DEADLINE ADDS
///
/// `set_expiry` files each key in TWO ordered maps and says so: `expires_at_ms`, a
/// `BTreeMap<ModelKey, u64>`, and `expiry_by_deadline`, a `BTreeMap<(u64, Arc<str>), ()>` ordered by
/// deadline and then by the key's characters -- the order an expiry sweep walks. A span orders by
/// arena offset, so neither can hold one; and unlike the two adopting ledgers
/// `an_order_bound_ledger_adopts_the_key_allocation_a_span_cannot_order` records, these are on the
/// ordinary write path of every key given a TTL.
///
/// WHAT THIS DOES NOT CLAIM. It does not say the arena is refused: a key with no deadline holds
/// neither map, and the published row is taken on such keys. It says the arena would need two key
/// representations side by side, or the expiry indexes moved off key-ordered containers, and it
/// measures how many allocations and holders a deadline key has so that cost is a number.
///
/// rust-internal: measures this crate's own resident structures, no product behaviour
///
///   cargo test -p temporalstore-rust --lib engine::tests::the_holders_of_a_key_allocation::a_key_with_a_deadline_is_held_by_two_ordered_maps -- --exact --ignored --nocapture
#[test]
#[ignore = "seeds a corpus and dumps it through the production lifecycle; run by name"]
fn a_key_with_a_deadline_is_held_by_two_ordered_maps() {
    const KEYS: usize = 1_792;
    const SHIPPED_END: u32 = 1023;
    /// An hour: far past the test's own runtime, so no key expires under the measurement.
    const TTL_MS: u64 = 3_600_000;

    let dir = tempfile::tempdir().expect("tempdir");
    let engine = holder_engine_on(dir.path(), SHIPPED_END);
    for index in 0..KEYS {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSetEx {
                key: format!("amp-{index:06}"),
                value: vec![b'v'; 256],
                ttl_ms: TTL_MS,
            },
        });
        assert!(response.status.ok, "write {index}: {:?}", response.status);
    }
    let candidates: Vec<u32> = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard is loaded");
        shard.bucket_index.bucket_map.keys().copied().collect()
    };
    assert!(!candidates.is_empty(), "{KEYS} keys produced no bucket to dump");
    let lifecycle =
        engine.apply_storage_lifecycle(crate::engine::reports::StorageLifecycleRequest {
            shard_id: 1,
            selected_dump_buckets: candidates.clone(),
            max_dump_buckets_per_round: candidates.len(),
            ..Default::default()
        });
    std::hint::black_box(&lifecycle);

    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");

    // --- THE FIXTURE FLOORS. A structure that was never populated contributes no holders, which
    // --- is the direction that flatters the arena, so each ordered map is asserted POPULATED.
    assert_eq!(KEYS, shard.strings().len(), "the string map does not hold the seeded corpus");
    assert_eq!(
        KEYS,
        shard.expires_at_ms().len(),
        "expires_at_ms holds {} of {KEYS} keys, so the deadline never armed and every ordered \
         holder below is NOT EXERCISED rather than empty",
        shard.expires_at_ms().len()
    );
    assert_eq!(
        KEYS,
        shard.expiry_by_deadline.len(),
        "expiry_by_deadline holds {} of {KEYS} entries, so its holder row below is unexercised",
        shard.expiry_by_deadline.len()
    );

    let mut holders: Counter<usize, Vec<&'static str>> = Counter::new();
    for table in shard.bucket_index.objects.values() {
        for key in table.keys() {
            holders.entry(key.as_ptr() as usize).or_default().push("bucket_index.objects");
        }
    }
    for key in shard.strings().keys() {
        holders.entry(key.as_ptr() as usize).or_default().push("strings");
    }
    for key in shard.expires_at_ms().keys() {
        holders.entry(key.as_ptr() as usize).or_default().push("expires_at_ms");
    }
    for (_deadline, key) in shard.expiry_by_deadline.keys() {
        holders.entry(key.as_ptr() as usize).or_default().push("expiry_by_deadline");
    }
    for key in shard.control_state_uuid().keys() {
        holders.entry(key.as_ptr() as usize).or_default().push("control_state_uuid");
    }
    for key in shard.context_compression_watermark().keys() {
        holders.entry(key.as_ptr() as usize).or_default().push("context_compression_watermark");
    }

    let mut by_shape: std::collections::BTreeMap<String, usize> = std::collections::BTreeMap::new();
    for who in holders.values() {
        let mut names = who.clone();
        names.sort_unstable();
        *by_shape.entry(names.join(" + ")).or_insert(0) += 1;
    }

    println!();
    println!("  {KEYS} keys written with a deadline, at the steady state");
    println!();
    println!("    {:<86} {:>12}", "which structures hold the allocation", "allocations");
    for (shape, count) in &by_shape {
        println!("    {shape:<86} {count:>12}");
    }
    println!();
    println!("    distinct key allocations {}", holders.len());
    println!(
        "    strings {} / objects {} / expires_at_ms {} / expiry_by_deadline {}",
        shard.strings().len(),
        shard.bucket_index.objects.values().map(|t| t.len()).sum::<usize>(),
        shard.expires_at_ms().len(),
        shard.expiry_by_deadline.len()
    );
    println!("  MEASURED: one allocation, four holders, two of them ordered. A span cannot order,");
    println!("  so a key given a deadline keeps its `Arc` for as long as either ordered map holds it,");
    println!("  and the arena would hold a second copy of its characters instead of replacing them.");
    println!();

    // --- THE SHAPE, ASSERTED AS ITS PREMISES.
    // One allocation per key: a deadline does not mint a copy of the key in either ordered map.
    // If this read 2 per key, the ordered maps would own their own text and a span could replace
    // the shared one -- the arena's gate would be a different gate.
    assert_eq!(
        KEYS,
        holders.len(),
        "{KEYS} keys with a deadline produced {} distinct key allocations. One each is the premise: \
         `set_expiry` takes the handle the block index already holds, so the ordered maps share it. \
         More means a deadline now mints its own copy of the key",
        holders.len()
    );
    let expected: Vec<&str> =
        vec!["bucket_index.objects", "expires_at_ms", "expiry_by_deadline", "strings"];
    for who in holders.values() {
        let mut names = who.clone();
        names.sort_unstable();
        assert_eq!(
            expected, names,
            "a key allocation is held by {names:?}, not by exactly {expected:?}. The two ordered \
             maps are what stop a span replacing the allocation: if either has stopped holding \
             it, a deadline key is no longer order-bound on that side and the arena's gate has \
             moved -- re-price it rather than loosening this assertion"
        );
    }
    // Neither adopting ledger is armed by this corpus, so they read empty here. That is stated
    // rather than assumed: an empty ordered ledger is not evidence a key cannot be in one.
    assert_eq!(0, shard.control_state_uuid().len(), "control_state_uuid is armed by this corpus");
    assert_eq!(
        0,
        shard.context_compression_watermark().len(),
        "context_compression_watermark is armed by this corpus"
    );
}
