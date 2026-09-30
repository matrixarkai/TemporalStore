// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! #2019 CHANGED THE INPUT TO STEP 1b'S REFUTATION, AND THE VERDICT DOES NOT MOVE.
//!
//! THIS ROUTE IS ALREADY REFUTED AND SAYING SO IS THE FIRST JOB OF THIS MODULE, because the next
//! reader will otherwise re-derive it. `page_entry_name_pointer` scores it as two of six dependent
//! verdicts:
//!
//!   * STEP 1b -- the key moved to the bucket's object list -- entry 48, stride 56, REFUTED. On
//!     bytes it is two populations with opposite signs: -15.76 to -15.84 B a page on containers but
//!     +8.11 to +9.10 on routed keys at the shipped `0..=1023` range, because the object list grows
//!     by a key for every object and a routed bucket holds forty of them. On identity it is refuted
//!     outright: with the key off the entry, an entry's only handle on its object is
//!     `BlockAddress::object_id`, an `Option` answered as ZERO when absent.
//!   * STEP 2 -- an object ordinal -- "needs exactly the id-to-key map 1b would have created, so it
//!     falls with it".
//!
//! SO WHY THIS MODULE EXISTS. Step 1b was measured while `bucket.object_index` held ONE ROW PER
//! PAGE -- #2007 measured 0.9983. #2019 collapsed that to one row per `(kind, key)`, measured 0.0400
//! per page on the container arm. The row count is the term step 1b's byte arithmetic divides by, so
//! the single number the refutation rested on changed by a factor of twenty-five two hours before
//! this module was written. That is a real reason to reopen it, and reopening it is what this module
//! does. THE VERDICT DOES NOT MOVE, and the reasons it does not are worth pinning because the next
//! collapse of the row count will raise the same question again.
//!
//! WHAT IS RE-MEASURED, AND WHAT IS NEW.
//!
//!   1. THE ARITHMETIC IS STILL SOUND, AND THE `u8` STILL BUYS NOTHING. Replacing `object_key`
//!      alone takes the entry 64 -> 48 and the stride 72 -> 56: sixteen bytes a page, reconstructed
//!      with `offset_of!` on mirror structures rather than asserted. A `u8` ordinal lands on the
//!      SAME 48, so its ceiling of 255 objects a bucket -- reachable, since a bucket is a hash
//!      bucket that fills with the corpus -- buys precisely zero additional bytes. Nothing should be
//!      spent on it. (`page_entry_name_pointer`'s step 2 row reads 40/48 because it composes an
//!      ordinal with step 1a's thin `component` as well; this row substitutes `object_key` only.)
//!
//!   2. #2019'S COLLAPSE DOES NOT REOPEN STEP 1b, and the reason is the arm it did not touch. The
//!      trade is `saving * pages - name_row * rows`, and it is positive only where a key holds more
//!      than one page. Measured per arm and never averaged: the container arm is where #2019's
//!      collapse lands and it pays, and THE ROUTED ARM STILL HOLDS ONE ROW PER PAGE -- its component
//!      was already absent, so #2019 was the identity on it -- which makes its trade a CEILING of
//!      zero before the list's own allocation is charged at all. The standing +8.11 to +9.10 is that
//!      ceiling with the allocation charged. #2019 published the distribution that decides which arm
//!      a real store is and it is repeated here rather than buried: ACROSS A REAL CORPUS, pages per
//!      `(kind, key)` is p50 1, p90 1, p99 1. This module's fixture is deliberately NOT that
//!      distribution -- 8 containers beside 200 routed keys puts a container at p99 -- because a
//!      sample matching the corpus would hold no visible container effect to measure the ceiling on.
//!      The fixture's own histogram is printed with its sample count so the two are not confused.
//!
//!   3. THE ORDINAL IS NOT STABLE, AND THIS LEG IS NEW. Step 2 falls as a DEPENDENT of 1b -- it
//!      needs a map 1b would have built. That leaves unexamined whether the ordinal would work even
//!      if the map existed, and it would not. `ObjectIndex::Many` is a SORTED RUN: `insert` places a
//!      new id at its bisection position, so filing an unrelated key whose id sorts below an
//!      existing one shifts every later ordinal; `remove` closes the hole the same way; and `shrink`
//!      collapses the arm to `One` at length one, so ordinal 1 stops existing. An ordinal stored in a
//!      page entry is therefore invalidated by an ORDINARY INSERT -- the most common operation this
//!      engine performs, with no delete anywhere near it -- and every entry past the insertion point
//!      silently names a different object. Stable slots would need an append-only list with
//!      tombstones, which costs the bisection `ObjectIndex::contains` answers membership by, on the
//!      one door every membership question goes through. Driven on twenty-five elements, because with
//!      one the first insert is also the last.
//!
//! WHAT THE LIST HOLDS, WHICH IS WHY 1b NEEDED A MAP AT ALL. `ObjectIndex` is
//! `Empty | One(u64) | Many(Box<Vec<u64>>)` -- bare ids, sorted, no characters. An ordinal into it
//! resolves to an id the entry can ALREADY answer for nothing, out of its address. The readers that
//! matter want the characters, and the list has never held any.
//!
//! THE READER COUNT HAS MOVED SINCE #1986 AND THE MOVEMENT IS REAL, not a counting error. #1986
//! recorded 47 production lines, twelve of them class 2. Walked at `7b049cb06`, the tree holds 41
//! production READS of this field across 26 functions plus 6 writes; 24 of the 41 want the
//! characters, to key `dirty_objects` and `expires_at_ms`, to probe the character-keyed
//! `ObjectBlockLookup`, to render `block_index_written_key`, or to report the key. Four have no
//! `BucketNode` in scope at all -- `block_index_handle` and `block_index_written_key` take a bare
//! `&BlockIndex`, and `block_index_handle` is the hottest reader in the tree, one call per page
//! insert -- so for those an ordinal has no bucket to resolve against. Six more sit inside `retain`
//! or `blocks_mut_unaccounted` closures where the node is MUTABLY borrowed. Those counts are stated
//! as a dated measurement, not as a list this module maintains.
//!
//! NO PRODUCT CODE MOVES HERE, so `SHARD_INDEX_FORMAT_VERSION` stays at 3. The bump to 4 would have
//! been the first thing step 1b needed -- the entry's stored shape moves -- and #2019's own bump
//! records the shape of the hazard: an old index decodes CLEANLY because both sides of the
//! generation check come off the wire, and the disagreement appears later, on a recovery path.
#![allow(clippy::all)]
use super::*;
use std::collections::BTreeSet;
use std::sync::Arc;

use crate::block_store::BlockAddress;
use crate::engine::hashing::stable_block_object_id;
use crate::engine::state::{BlockIndex, ObjectIndex};
use crate::engine::storage_bucket_internals::StoredModelKind;

const OPERATOR_END: u32 = 1023;
/// The population #2019 measured on, so the rows here are comparable to its line for line.
const CONTAINER_KEYS: usize = 8;
const MEMBERS_PER_KEY: usize = 25;
/// The CONTROL population: a string page's component is already `None`, one page per key, so #2019
/// was the identity on it and its trade has a ceiling of zero. Both facts are asserted, not assumed.
const ROUTED_KEYS: usize = 200;

/// What the entry gives up per page if `object_key` becomes a `u16` ordinal. Derived below, never
/// asserted as this literal.
const CLAIMED_SAVING_PER_PAGE: usize = 16;

// =================================================================================================
// HARNESS
// =================================================================================================

fn engine_on(dir: &std::path::Path) -> TemporalEngine {
    TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("pages"),
        dir.join("indexes"),
    )
}

fn load_on(engine: &TemporalEngine) {
    let response = engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 1,
        table_name: "entry-points-at-object-list".to_string(),
        shard_uri: "local://entry-points-at-object-list/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket: OPERATOR_END,
        readonly: false,
        load_version: 1,
        local_node_id: Some(1),
    });
    assert!(
        response.status.ok,
        "the fixture could not load shard 1: {:?}",
        response.status
    );
}

fn ack_batch(engine: &TemporalEngine, commands: Vec<Command>) {
    assert!(
        !commands.is_empty(),
        "an empty seed would make every row below vacuous"
    );
    for chunk in commands.chunks(1_000) {
        let response = engine.batch_execute(crate::types::BatchExecuteRequest {
            shard_id: 1,
            commands: chunk.to_vec(),
        });
        assert!(response.status.ok, "seed must ack: {:?}", response.status);
    }
}

fn seed_hash_containers(engine: &TemporalEngine) {
    let mut commands = Vec::with_capacity(CONTAINER_KEYS * MEMBERS_PER_KEY);
    for k in 0..CONTAINER_KEYS {
        for f in 0..MEMBERS_PER_KEY {
            commands.push(Command::HashSet {
                key: format!("h{k}"),
                field: format!("f{f}"),
                value: format!("h{k}-f{f}-value").into_bytes(),
            });
        }
    }
    ack_batch(engine, commands);
}

fn seed_routed_strings(engine: &TemporalEngine) {
    ack_batch(
        engine,
        (0..ROUTED_KEYS)
            .map(|i| Command::StringSet {
                key: format!("s{i}"),
                value: format!("s{i}-value").into_bytes(),
            })
            .collect(),
    );
}

/// `(live pages, distinct object-index rows)` for one kind, read off the buckets themselves. The
/// same shape #2019's `pages_and_index_rows` uses, so the two modules' rows are comparable.
fn pages_and_index_rows(
    engine: &TemporalEngine,
    kind: &str,
    keys: &BTreeSet<String>,
) -> (usize, usize) {
    let wanted: BTreeSet<u64> = keys
        .iter()
        .map(|key| stable_block_object_id(1, kind, key))
        .collect();
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 is loaded");
    let mut pages = 0usize;
    let mut rows: BTreeSet<u64> = BTreeSet::new();
    for bucket in shard.bucket_index.bucket_map.values() {
        for page in bucket.block_index.values() {
            if page.deleted || page.model_id.as_str() != kind {
                continue;
            }
            if !keys.contains(&*page.object_key) {
                continue;
            }
            pages += 1;
        }
        for object_id in &bucket.object_index {
            if wanted.contains(object_id) {
                rows.insert(*object_id);
            }
        }
    }
    (pages, rows.len())
}

fn percentile(sorted: &[usize], p: f64) -> usize {
    assert!(!sorted.is_empty(), "no sample to take a percentile of");
    let rank = ((p / 100.0) * sorted.len() as f64).ceil().max(1.0) as usize;
    sorted[rank.min(sorted.len()) - 1]
}

/// The pages one `(kind, key)` holds, counted off the bucket index.
fn pages_of_key(engine: &TemporalEngine, kind: &str, key: &str) -> usize {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 is loaded");
    let mut held = 0usize;
    for bucket in shard.bucket_index.bucket_map.values() {
        for page in bucket.block_index.values() {
            if !page.deleted && page.model_id.as_str() == kind && &*page.object_key == key {
                held += 1;
            }
        }
    }
    held
}

// =================================================================================================
// THE MIRROR STRUCTURES
//
// The product entry with `object_key` replaced by an ordinal, and nothing else touched. Never
// constructed -- only its layout is read -- so the fields are dead by design.
// =================================================================================================

#[allow(dead_code)]
struct OrdinalEntryU16 {
    object_ordinal: u16,
    model_id: StoredModelKind,
    component: Option<Arc<str>>,
    address: BlockAddress,
    dirty: bool,
    deleted: bool,
    log_backed: bool,
}

#[allow(dead_code)]
struct OrdinalEntryU8 {
    object_ordinal: u8,
    model_id: StoredModelKind,
    component: Option<Arc<str>>,
    address: BlockAddress,
    dirty: bool,
    deleted: bool,
    log_backed: bool,
}

/// `(field bytes, slack)` for a hand-written field list against a measured total.
fn account(fields: &[(&'static str, usize, usize)], total: usize) -> (usize, usize) {
    let covered: usize = fields.iter().map(|(_, _, width)| *width).sum();
    (covered, total.saturating_sub(covered))
}

/// The width a `repr(Rust)` struct takes, reconstructed from a field list: every field whose width
/// is a whole number of words, plus the rest rounded up to the alignment.
fn reconstruct(fields: &[(&'static str, usize, usize)], word: usize) -> usize {
    let aligned: usize = fields
        .iter()
        .map(|(_, _, width)| *width)
        .filter(|width| width % word == 0)
        .sum();
    let tail: usize = fields
        .iter()
        .map(|(_, _, width)| *width)
        .filter(|width| width % word != 0)
        .sum();
    aligned + tail.div_ceil(word) * word
}

// =================================================================================================
// 1. THE ARITHMETIC, WHICH IS SOUND. SIXTEEN BYTES A PAGE, AND A `u8` BUYS NOTHING OVER A `u16`.
// =================================================================================================

/// THE ENTRY AND THE STRIDE, BEFORE AND AFTER, RECONSTRUCTED RATHER THAN ASSERTED AS LITERALS.
///
/// The stride is the number that matters and it is not the struct width: the page list is
/// `Vec<(u64, BlockIndex)>`, so a page costs its handle plus its entry. 64 + 8 = 72 today.
///
/// THE `u8` IS MEASURED AND DISMISSED IN THE SAME BREATH. Both ordinals leave a six- and a
/// five-byte tail, both round to eight, and both land the struct on 48 -- so the `u8`'s ceiling of
/// 255 objects a bucket buys precisely zero additional bytes. A bucket is a hash bucket that fills
/// with the corpus rather than a design with a bounded object count, so that ceiling is reachable:
/// 1,024 buckets times 255 is roughly 261,000 objects a shard. Nothing here should be spent on it.
///
/// rust-internal: reads this crate's own type layout, no product behaviour
#[test]
fn a_u16_ordinal_would_take_the_entry_from_sixty_four_to_forty_eight_and_a_u8_adds_nothing() {
    use std::mem::{align_of, offset_of, size_of};

    let entry_total = size_of::<BlockIndex>();
    let word = align_of::<BlockIndex>();

    // The product entry. One `offset_of!` per named field; `size_of` of the WRONG type still
    // compiles, which is what the reconstruction assertion below is for.
    let entry_fields: Vec<(&'static str, usize, usize)> = vec![
        ("object_key", offset_of!(BlockIndex, object_key), size_of::<Arc<str>>()),
        ("model_id", offset_of!(BlockIndex, model_id), size_of::<StoredModelKind>()),
        ("component", offset_of!(BlockIndex, component), size_of::<Option<Arc<str>>>()),
        ("address", offset_of!(BlockIndex, address), size_of::<BlockAddress>()),
        ("dirty", offset_of!(BlockIndex, dirty), size_of::<bool>()),
        ("deleted", offset_of!(BlockIndex, deleted), size_of::<bool>()),
        ("log_backed", offset_of!(BlockIndex, log_backed), size_of::<bool>()),
    ];
    let (covered, slack) = account(&entry_fields, entry_total);

    let mut sorted = entry_fields.clone();
    sorted.sort_by_key(|(_, offset, _)| *offset);
    println!("\n--- BlockIndex, {entry_total} bytes, align {word} ---");
    let mut cursor = 0usize;
    for (name, offset, width) in &sorted {
        println!(
            "  +{offset:>3}  {name:<12} width {width:>2}  padding before {}",
            offset - cursor
        );
        cursor = offset + width;
    }
    println!("  tail padding {}", entry_total - cursor);
    println!("  field bytes {covered}, slack {slack}, total {entry_total}");

    assert_eq!(
        reconstruct(&entry_fields, word),
        entry_total,
        "the reconstruction of BlockIndex no longer adds up to its width. If a field's TYPE \
         changed, the field list above is what needs editing"
    );
    assert_eq!(covered + slack, entry_total);

    // THE COUNTERFACTUALS, measured off real mirror structures by this compiler.
    let u16_total = size_of::<OrdinalEntryU16>();
    let u8_total = size_of::<OrdinalEntryU8>();
    let u16_fields: Vec<(&'static str, usize, usize)> = vec![
        ("object_ordinal", offset_of!(OrdinalEntryU16, object_ordinal), size_of::<u16>()),
        ("model_id", offset_of!(OrdinalEntryU16, model_id), size_of::<StoredModelKind>()),
        ("component", offset_of!(OrdinalEntryU16, component), size_of::<Option<Arc<str>>>()),
        ("address", offset_of!(OrdinalEntryU16, address), size_of::<BlockAddress>()),
        ("dirty", offset_of!(OrdinalEntryU16, dirty), size_of::<bool>()),
        ("deleted", offset_of!(OrdinalEntryU16, deleted), size_of::<bool>()),
        ("log_backed", offset_of!(OrdinalEntryU16, log_backed), size_of::<bool>()),
    ];
    assert_eq!(
        reconstruct(&u16_fields, align_of::<OrdinalEntryU16>()),
        u16_total,
        "the mirror structure's reconstruction does not add up, so its width is not evidence"
    );

    // The stride: a handle plus an entry, which is what the page list actually holds an element of.
    let stride_now = size_of::<(u64, BlockIndex)>();
    let stride_u16 = size_of::<(u64, OrdinalEntryU16)>();
    let stride_u8 = size_of::<(u64, OrdinalEntryU8)>();
    let saving = stride_now - stride_u16;

    println!("\n=== entry and stride, before and after ===");
    println!("  shape          entry  stride");
    println!("  object_key     {entry_total:>5}  {stride_now:>6}");
    println!("  u16 ordinal    {u16_total:>5}  {stride_u16:>6}");
    println!("  u8  ordinal    {u8_total:>5}  {stride_u8:>6}");
    println!("  saving per page: {saving} bytes of stride");

    assert_eq!(
        stride_now,
        size_of::<u64>() + entry_total,
        "the stride is not a handle plus an entry, so the page list is not the shape this \
         measurement prices"
    );
    assert!(
        u16_total < entry_total,
        "a u16 ordinal does not narrow the entry at all: {u16_total} against {entry_total}"
    );
    assert_eq!(
        u16_total, u8_total,
        "a u8 ordinal lands on {u8_total} where the u16 lands on {u16_total}. The whole reason to \
         refuse the u8 is that it buys NOTHING over the u16 while capping a bucket at 255 objects; \
         if that has stopped being true this advice needs rewriting"
    );
    assert_eq!(
        stride_u16, stride_u8,
        "the two ordinal widths no longer share a stride"
    );
    assert_eq!(
        saving, CLAIMED_SAVING_PER_PAGE,
        "the arithmetic this module refutes on OTHER grounds has itself moved: the stride saving \
         is {saving}, not {CLAIMED_SAVING_PER_PAGE}. The per-arm trade is quoted against \
         this number and must be recomputed"
    );
    println!(
        "  VERDICT: the arithmetic is SOUND -- {saving} bytes a page, and the u8 adds nothing. \
         What follows is why the route is still refused."
    );
}

// =================================================================================================
// 2. WHY 1b NEEDED A MAP: THE LIST HOLDS IDS, AND THE ENTRY ALREADY HAS THE ID FOR FREE.
// =================================================================================================

/// `object_index` IS A LIST OF `u64` OBJECT IDS. AN ORDINAL INTO IT CANNOT ANSWER A NAME.
///
/// This is the reason the proposal's "level one is built" is not true. The list that exists holds
/// ids; the readers that need the CHARACTERS -- 24 of the 41 walked at `7b049cb06` -- want the
/// KEY. And the id an ordinal would
/// resolve to is one the entry can already answer for nothing: `BlockIndex::object_id()` reads it
/// out of the address, which is why #2019 could delete the entry's own copy of it.
///
/// So the ordinal would buy a second route to something already free, and still leave every reader
/// of the name unserved. Asserted through the product's own accessor against the product's own
/// hash rather than by inspecting the type, so the claim is about behaviour and not declaration.
///
/// rust-internal: reads the engine's own bucket index, no external surface
#[test]
fn the_object_list_holds_ids_and_the_entry_already_answers_the_id_without_one() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine);
    seed_hash_containers(&engine);

    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 is loaded");

    let mut rows_seen = 0usize;
    let mut pages_seen = 0usize;
    let mut id_already_answered = 0usize;
    // Every row of every object list, and what an ordinal into it would resolve TO.
    for bucket in shard.bucket_index.bucket_map.values() {
        for _id in &bucket.object_index {
            // The item type of this iterator IS `&u64`. Binding it as one is the assertion: if the
            // list ever starts holding a name, this stops compiling and this module is what says
            // the refutation needs revisiting.
            let _: &u64 = _id;
            rows_seen += 1;
        }
        for page in bucket.block_index.values() {
            // Only the kind this fixture seeded, so an incidental page of another kind cannot
            // decide the assertion below.
            if page.deleted || page.model_id.as_str() != "hash" {
                continue;
            }
            pages_seen += 1;
            // What the entry can answer WITHOUT an ordinal, from the address alone.
            let from_address = page.object_id();
            let recomputed = stable_block_object_id(1, page.model_id.as_str(), &page.object_key);
            if from_address == recomputed {
                id_already_answered += 1;
            }
        }
    }

    println!("\n=== what an ordinal into the object list would resolve to ===");
    println!("  object-index rows walked : {rows_seen}");
    println!("  live pages walked        : {pages_seen}");
    println!("  pages whose id the entry already answers from its address: {id_already_answered}");

    assert!(
        pages_seen > 0 && rows_seen > 0,
        "DENOMINATOR: {pages_seen} pages and {rows_seen} rows walked. A store that filed nothing \
         would satisfy every assertion below while observing nothing"
    );
    assert_eq!(
        id_already_answered, pages_seen,
        "only {id_already_answered} of {pages_seen} pages answer their own object id from the \
         address. The refutation's first reason is that an ordinal buys a second route to \
         something already free; if the address has stopped answering, that reason weakens"
    );
    println!(
        "  VERDICT: the list is {rows_seen} rows of u64 id. The id is ALREADY free from the \
         address on {id_already_answered}/{pages_seen} pages, and the NAME is not in the list at \
         all -- so the ordinal serves no reader of `object_key`."
    );
}

// =================================================================================================
// 3. THE NEW LEG: THE ORDINAL IS INVALIDATED BY AN ORDINARY INSERT, EVEN IF THE MAP EXISTED.
// =================================================================================================

/// AN ORDINAL INTO A SORTED RUN IS INVALIDATED BY AN INSERT, NOT ONLY BY A DELETE.
///
/// The proposal's own risk note asks whether the slot is stable across DELETES. It is not, but
/// that is the lesser half: `ObjectIndex::insert` puts a new id at its BISECTION POSITION, so
/// filing an unrelated key whose id sorts below an existing one shifts every later ordinal. An
/// insert is the most common thing this engine does.
///
/// Driven on TWENTY-FIVE elements, not one: with a single element the first insert is also the
/// last and neither shift can be observed. Each of the three mutations is checked separately --
/// insert below, remove below, and the `shrink` collapse -- because any one of them alone would be
/// enough to lose data and a fixture that only exercised deletes would have reported the insert as
/// safe.
///
/// rust-internal: drives this crate's own `ObjectIndex`, no product surface
#[test]
fn an_ordinal_into_the_object_list_is_invalidated_by_an_ordinary_insert_not_only_a_delete() {
    const ELEMENTS: usize = 25;

    // Ids spaced so there is always room to insert strictly below an existing one.
    let seeded: Vec<u64> = (1..=ELEMENTS as u64).map(|i| i * 100).collect();
    let mut list = ObjectIndex::default();
    for id in &seeded {
        assert!(list.insert(*id), "the seed must file {id}");
    }
    assert_eq!(
        list.len(),
        ELEMENTS,
        "the fixture holds {} rows, not {ELEMENTS}, so nothing below is being observed",
        list.len()
    );
    assert!(
        ELEMENTS > 1,
        "a one-element fixture cannot observe a shift: the first insert would also be the last"
    );

    let ordinals_of = |list: &ObjectIndex| -> Vec<u64> { list.iter().copied().collect() };

    let before = ordinals_of(&list);
    // The entry we imagine having stored an ordinal in. Not the first and not the last, so a shift
    // at either end is visible.
    let watched_ordinal = 12usize;
    let watched_id_before = before[watched_ordinal];

    println!("\n=== an ordinal's stability across the three mutations ===");
    println!("  rows {ELEMENTS}, watching ordinal {watched_ordinal}");
    println!("  before          : ordinal {watched_ordinal} -> id {watched_id_before}");

    // (a) INSERT BELOW. An unrelated key, no delete anywhere.
    let mut inserted = list.clone();
    assert!(inserted.insert(1), "the fixture must file the low id");
    let after_insert = ordinals_of(&inserted);
    let watched_id_after_insert = after_insert[watched_ordinal];
    println!(
        "  after insert(1) : ordinal {watched_ordinal} -> id {watched_id_after_insert}  \
         (rows {})",
        after_insert.len()
    );
    assert_ne!(
        watched_id_before, watched_id_after_insert,
        "inserting an id BELOW the watched one left ordinal {watched_ordinal} naming the same \
         object. If the list has stopped being a sorted run, this module's third reason no longer \
         holds and the route may be reopenable"
    );
    assert_eq!(
        watched_id_after_insert, watched_id_before - 100,
        "the shift is not the one-slot shift an ordered insert produces"
    );

    // (b) REMOVE BELOW, which is the half the proposal already suspected.
    let mut removed = list.clone();
    assert!(removed.remove(&100), "the fixture must remove the low id");
    let after_remove = ordinals_of(&removed);
    let watched_id_after_remove = after_remove[watched_ordinal];
    println!(
        "  after remove(100): ordinal {watched_ordinal} -> id {watched_id_after_remove}  \
         (rows {})",
        after_remove.len()
    );
    assert_ne!(
        watched_id_before, watched_id_after_remove,
        "removing an id BELOW the watched one left ordinal {watched_ordinal} unchanged"
    );

    // (c) THE ARM COLLAPSE. `shrink` takes `Many` to `One` at length one, so ordinal 1 ceases to
    // exist rather than merely moving -- a stored ordinal of 1 then indexes nothing.
    let mut collapsing = ObjectIndex::default();
    assert!(collapsing.insert(100));
    assert!(collapsing.insert(200));
    assert_eq!(collapsing.len(), 2, "the collapse fixture must hold two");
    let two_wide = ordinals_of(&collapsing);
    assert_eq!(two_wide.len(), 2);
    assert!(collapsing.remove(&100), "remove the first of two");
    let one_wide = ordinals_of(&collapsing);
    println!(
        "  arm collapse    : 2 rows {two_wide:?} -> {} rows {one_wide:?}",
        one_wide.len()
    );
    assert_eq!(
        one_wide.len(),
        1,
        "the arm did not collapse, so this leg is not being observed"
    );
    assert_eq!(
        one_wide[0], 200,
        "the surviving row is not the one that should have survived"
    );
    assert!(
        one_wide.get(1).is_none(),
        "ordinal 1 still resolves after the collapse, so a stored 1 would not dangle"
    );

    println!(
        "  VERDICT: an ordinal is invalidated by an INSERT of an unrelated key, by a REMOVE, and \
         by the arm collapse. Stable slots need an append-only list with tombstones, which costs \
         the bisection `ObjectIndex::contains` is built on."
    );
}

// =================================================================================================
// 4. #2019'S ROW COLLAPSE DOES NOT REOPEN STEP 1b, BECAUSE IT DID NOT TOUCH THE ROUTED ARM.
//    REPORTED AS A CEILING ON THE PRIZE, AND THE CEILING ON THE ROUTED ARM IS ZERO.
// =================================================================================================

/// THE ROW COUNT #2019 COLLAPSED IS THE TERM STEP 1b DIVIDES BY, AND THE ROUTED ARM DID NOT MOVE.
///
/// A name the entry stops holding has to be held somewhere, once per row, so the trade is
/// `saving * pages - name_row * rows`. DIVIDED PER ARM AND NEVER AVERAGED: that division is what
/// #2007's routed arms failed, and an average across the two arms would report a win.
///
/// THIS IS A CEILING ON THE PRIZE, NOT THE PRIZE. Only the name pointer is charged here -- one
/// `Arc<str>` a row. The list's own allocation is NOT charged: a `Box<Vec<u64>>` widened to carry
/// names pays a vector header once a bucket, allocator rounding, and growth slack, none of which
/// appears below. So the real figure is WORSE than this on every arm, and the standing measurement
/// says how much worse: `page_entry_name_pointer` scores step 1b at -15.76 to -15.84 B a page on
/// containers and +8.11 to +9.10 on routed keys at the shipped range, where the sign has flipped
/// because the list grows by a key for every object and a routed bucket holds forty.
///
/// A CEILING IS THE RIGHT SHAPE FOR THIS CLAIM. If even the ceiling is zero on the routed arm, no
/// accounting of the list can make that arm pay, and the arm is where a real store's keys are.
///
/// THE CONTROL IS THE ROUTED ARM, whose component was already absent -- so #2019 was the IDENTITY
/// on it and its rows-per-page cannot have moved off 1.0000. Its page count and its row count are
/// both asserted BEFORE its result: a control that exercised no pages reports no movement for the
/// wrong reason, and a control over zero rows reports success whatever the trade.
///
/// rust-internal: reads the engine's own bucket index, no external surface
#[test]
fn the_row_collapse_does_not_reopen_the_route_because_the_routed_arm_did_not_move() {
    use std::mem::size_of;

    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine);
    seed_hash_containers(&engine);
    seed_routed_strings(&engine);

    let hash_keys: BTreeSet<String> = (0..CONTAINER_KEYS).map(|k| format!("h{k}")).collect();
    let string_keys: BTreeSet<String> = (0..ROUTED_KEYS).map(|i| format!("s{i}")).collect();

    let (hash_pages, hash_rows) = pages_and_index_rows(&engine, "hash", &hash_keys);
    let (string_pages, string_rows) = pages_and_index_rows(&engine, "string", &string_keys);

    // The saving is measured, not assumed, so this arithmetic moves with a sibling width step.
    let saving = size_of::<(u64, BlockIndex)>() - size_of::<(u64, OrdinalEntryU16)>();
    // What one row of the list that would have to exist costs: a shared pointer to the name.
    let row_cost = size_of::<Arc<str>>();

    let net_per_page = |pages: usize, rows: usize| -> f64 {
        if pages == 0 {
            return 0.0;
        }
        (saving as f64 * pages as f64 - row_cost as f64 * rows as f64) / pages as f64
    };

    let hash_net = net_per_page(hash_pages, hash_rows);
    let string_net = net_per_page(string_pages, string_rows);

    println!("\n=== the trade, per arm, never averaged -- A CEILING ON THE PRIZE ===");
    println!("  saving {saving} B/page of stride, name row {row_cost} B");
    println!("  the list's own allocation is NOT charged, so every row below is optimistic");
    println!("  arm       keys  pages  rows  rows/page  ceiling B/page");
    for (arm, keys, pages, rows, net) in [
        ("hash", hash_keys.len(), hash_pages, hash_rows, hash_net),
        ("string", string_keys.len(), string_pages, string_rows, string_net),
    ] {
        println!(
            "  {arm:<8} {keys:>5} {pages:>6} {rows:>5}   {:>8.4}  {net:>+10.2}",
            rows as f64 / pages as f64
        );
    }

    // DENOMINATORS FIRST, BOTH ARMS.
    assert_eq!(
        hash_pages,
        CONTAINER_KEYS * MEMBERS_PER_KEY,
        "the container arm exercised {hash_pages} pages, not {}",
        CONTAINER_KEYS * MEMBERS_PER_KEY
    );
    assert_eq!(
        string_pages, ROUTED_KEYS,
        "CONTROL NOT EXERCISED: {string_pages} string pages, not {ROUTED_KEYS}. A control that \
         read nothing reports no movement for the wrong reason"
    );
    assert!(
        string_rows > 0,
        "CONTROL OVER ZERO ROWS: a control whose list is empty reports success whatever the trade"
    );
    assert!(
        hash_pages > hash_rows,
        "DENOMINATOR: {hash_pages} pages and {hash_rows} rows on the container arm. A fixture \
         whose keys held one page each would measure nothing"
    );

    // THE CONTROL. #2019 was the IDENTITY on this arm -- a string page's component was already
    // absent -- so its rows-per-page cannot have moved off 1.0000, and that is the whole reason the
    // collapse does not reopen the route.
    assert_eq!(
        string_rows, string_pages,
        "the control holds {string_rows} rows for {string_pages} pages. #2019 was the identity on \
         this arm, so one row per page is what it must still hold; if this has moved, the route IS \
         reopened and this module's verdict needs remeasuring"
    );
    assert!(
        string_net <= 0.0,
        "the control's CEILING is {string_net:+.4} B/page, above zero. Only the name pointer is \
         charged here, so a positive ceiling would mean the arm could pay once the list's \
         allocation is charged too -- which is the opposite of what the standing +8.11 to +9.10 \
         measures"
    );

    // THE CONTAINER ARM, where #2019's collapse actually lands.
    assert!(
        hash_net > 0.0,
        "the container arm's ceiling is {hash_net:+.4} B/page. If containers have stopped paying, \
         nothing pays and the route is refuted even more cheaply"
    );
    assert!(
        hash_net < saving as f64,
        "the container arm's ceiling is {hash_net:+.4} B/page against a {saving} B/page stride \
         saving. The list's own rows cannot cost zero -- if they read as zero the row cost is not \
         being charged and this ceiling is not one"
    );

    println!(
        "  VERDICT: container ceiling {hash_net:+.2} B/page, CONTROL routed ceiling \
         {string_net:+.2} B/page over {string_pages} pages and {string_rows} rows. #2019 collapsed \
         the container arm's rows/page by 25x and did NOT move the routed arm, whose ceiling is \
         zero before the list's allocation is charged at all -- so step 1b stays refuted."
    );

    // AND THE DISTRIBUTION THAT DECIDES WHICH ARM A REAL STORE IS, as #2019 published it. A
    // histogram rather than a mean, because the whole question is the tail.
    let mut per_key: Vec<usize> = Vec::new();
    for key in &hash_keys {
        per_key.push(pages_of_key(&engine, "hash", key));
    }
    for key in &string_keys {
        per_key.push(pages_of_key(&engine, "string", key));
    }
    per_key.sort();
    let single: usize = per_key.iter().filter(|pages| **pages <= 1).count();
    println!(
        "  pages per (kind,key) over {} keys: p50={} p90={} p99={} MAX={}",
        per_key.len(),
        percentile(&per_key, 50.0),
        percentile(&per_key, 90.0),
        percentile(&per_key, 99.0),
        per_key.last().copied().unwrap_or(0)
    );
    println!(
        "  keys holding ONE page: {single}/{} ({:.2}%) -- each of them nets exactly zero",
        per_key.len(),
        100.0 * single as f64 / per_key.len() as f64
    );
    assert_eq!(
        percentile(&per_key, 50.0),
        1,
        "p50 pages per key is not 1 on a fixture that is {ROUTED_KEYS} single-page keys beside \
         {CONTAINER_KEYS} containers, so the caveat this module repeats would be wrong"
    );
    assert_eq!(
        per_key.last().copied().unwrap_or(0),
        MEMBERS_PER_KEY,
        "MAX pages per key is not {MEMBERS_PER_KEY}: the container arm is what makes the effect \
         visible and it is not in this sample"
    );
    assert!(
        single > 0,
        "no key in the sample holds a single page, so the zero-net population is unobserved"
    );
}

// =================================================================================================
// 5. THE JOIN, PRICED IN PROBES RATHER THAN IN TIME.
// =================================================================================================

/// WHAT A READER OF THE NAME WOULD PAY, COUNTED IN EXAMINED ENTRIES.
///
/// A timing instrument is useless here -- on this box it has read 485x idle against 11x busy off
/// identical code -- so the join is priced in the engine's own probe counters instead.
///
/// `ObjectIndex::contains` charges `floor(log2(n)) + 1` examined entries for a bisection over the
/// sorted run, and that is the FLOOR a stable-slot design would have to beat: an append-only list
/// with tombstones cannot bisect, so its membership answer becomes a walk of the whole run. Both
/// numbers are printed against the run lengths this store actually holds.
///
/// AND AT THESE RUN LENGTHS THE TWO COINCIDE, which is reported rather than hidden: the measured
/// runs are p50 1 and MAX 2, and `floor(log2(n)) + 1 == n` for n of 1 and 2. So this module
/// establishes the join's SIZE today and the DIRECTION a stable-slot design moves it, and does NOT
/// establish that the move is large -- that would need a store whose buckets hold long object
/// lists, which the shipped routing range does not produce. The inequality is what is asserted.
///
/// rust-internal: reads this crate's own probe counters, no product surface
#[test]
fn the_join_a_name_reader_would_pay_is_logarithmic_today_and_linear_with_stable_slots() {
    use crate::engine::state::{
        entries_a_bisection_examines, object_index_entries_examined,
        reset_object_index_entries_examined,
    };

    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine);
    seed_hash_containers(&engine);
    seed_routed_strings(&engine);

    // The run lengths the object lists actually hold, off the buckets.
    let mut run_lengths: Vec<usize> = Vec::new();
    {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard 1 is loaded");
        for bucket in shard.bucket_index.bucket_map.values() {
            let len = bucket.object_index.len();
            if len > 0 {
                run_lengths.push(len);
            }
        }
    }
    assert!(
        !run_lengths.is_empty(),
        "DENOMINATOR: no bucket holds an object list, so no join is being priced"
    );
    run_lengths.sort();

    // One membership question per bucket, charged through the product's own door.
    reset_object_index_entries_examined();
    let probed = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard 1 is loaded");
        let mut asked = 0usize;
        for bucket in shard.bucket_index.bucket_map.values() {
            if let Some(first) = bucket.object_index.iter().next().copied() {
                assert!(
                    bucket.object_index.contains(&first),
                    "a bucket's own first id is not in its own list"
                );
                asked += 1;
            }
        }
        asked
    };
    let examined = object_index_entries_examined();

    // What a stable-slot, append-only list would cost for the same questions: no bisection, so the
    // whole run.
    let linear: usize = run_lengths.iter().sum();
    let bisecting: u64 = run_lengths
        .iter()
        .map(|len| entries_a_bisection_examines(*len))
        .sum();

    println!("\n=== the join, in examined entries ===");
    println!("  buckets with a list : {}", run_lengths.len());
    println!(
        "  run length          : p50={} p90={} p99={} MAX={}",
        percentile(&run_lengths, 50.0),
        percentile(&run_lengths, 90.0),
        percentile(&run_lengths, 99.0),
        run_lengths.last().copied().unwrap_or(0)
    );
    println!("  membership questions asked : {probed}");
    println!("  entries examined, measured : {examined}");
    println!("  entries a bisection predicts: {bisecting}");
    println!("  entries an append-only walk would examine: {linear}");
    if linear as u64 == examined {
        println!(
            "  NOTE: at these run lengths (MAX {}) a bisection and a walk COINCIDE, so the \
             divergence is asserted below as an inequality and is NOT demonstrated by this \
             fixture. floor(log2(n))+1 == n for n in 1..=2, which is where this store sits.",
            run_lengths.last().copied().unwrap_or(0)
        );
    }

    assert!(
        probed > 0 && examined > 0,
        "DENOMINATOR: {probed} questions and {examined} entries examined. A run that charged \
         nothing would report the join as free"
    );
    assert_eq!(
        examined, bisecting,
        "the measured examined count {examined} is not the {bisecting} a bisection predicts over \
         these run lengths, so the probe and the model disagree and neither can price the join"
    );
    assert!(
        linear as u64 >= examined,
        "an append-only walk examines {linear} entries against the bisection's {examined}, which \
         cannot be cheaper"
    );
    println!(
        "  VERDICT: the join is {examined} examined entries today. Stable slots cost the \
         bisection and take the same questions to {linear} -- against a prize capped at the \
         container arm's share of pages."
    );
}
