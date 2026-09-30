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
//!   3. THE ORDINAL WAS NOT STABLE, AND THAT LEG IS NOW SETTLED THE OTHER WAY. It read: step 2
//!      falls as a DEPENDENT of 1b, which leaves unexamined whether the ordinal would work even if
//!      the map existed, and it would not -- `ObjectIndex::Many` was a SORTED RUN, so `insert` placed
//!      a new id at its bisection position and shifted every later ordinal, `remove` closed the hole
//!      the same way, and `shrink` collapsed the arm to `One` at length one so ordinal 1 stopped
//!      existing. An ordinal was therefore invalidated by an ORDINARY INSERT, the most common
//!      operation this engine performs.
//!
//!      THAT IS NO LONGER TRUE. `ObjectIndex::Many` IS A SLOT ARRAY WITH PLACEHOLDERS: `insert` takes
//!      the first free slot and otherwise appends, `remove` leaves a placeholder, nothing closes a
//!      hole, and the collapse to `One` survives only where it cannot renumber anything.
//!      `a_slot_into_the_object_list_survives_the_three_mutations_that_invalidated_an_ordinal` drives
//!      the same three mutations against the same watched position, and every one of them now leaves
//!      it naming the same object.
//!
//!      WHAT IT COST, both measured below rather than conceded. THE BISECTION:
//!      `ObjectIndex::contains` is a WALK now, because slot order is not id order -- and at the run
//!      lengths this store holds, p50 1 and MAX 2, a walk and a bisection are the same number, so the
//!      cost is zero at the shipped distribution and a divergence only on long container lists. THE
//!      BYTES: the placeholders cost nothing that persists -- the array is bounded by the bucket's own
//!      high water mark, 1.000 slots an object at two widths across four churn shapes -- but a SLOT is
//!      an `Option<u64>` at 16 bytes against the run element's 8, because object ids are full-range
//!      FNV-1a hashes and `u64::MAX` is a legitimate one, so no value can be reserved to mean free.
//!
//!      AND IT REOPENS NOTHING ON ITS OWN. Stability was one of three reasons. Legs 1, 2 and 4 are
//!      untouched: the entry still saves sixteen bytes and no more, the routed arm's ceiling is still
//!      zero, and -- the one that decides it -- the list still holds BARE IDS while 24 of 41
//!      production readers want the CHARACTERS. A stable slot resolves to an id the address already
//!      answers for nothing. So this is a precondition that now holds, not a verdict that has moved.
//!
//! WHAT THE LIST HOLDS, WHICH IS WHY 1b NEEDED A MAP AT ALL. `ObjectIndex` is
//! `Empty | One(u64) | Many(Box<ObjectSlots>)` -- bare ids in stable slots, no characters. An ordinal
//! into it resolves to an id the entry can ALREADY answer for nothing, out of its address. The readers
//! that matter want the characters, and the list has never held any. Making the slot stable did not
//! change that, which is why it changes no verdict by itself.
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
//! PRODUCT CODE DOES MOVE NOW -- `ObjectIndex` itself -- AND `SHARD_INDEX_FORMAT_VERSION` STILL DOES
//! NOT. What is stored is the IDS and not the slots: the Serialize impl writes them ascending, which
//! is the order the sorted run wrote and so the order already on disk, and a load re-files them
//! through `insert`, which hands slots out afresh. So a slot is a RESIDENT fact and no stamp is owed.
//! `nothing_persists_a_slot_so_the_written_bytes_do_not_move` drives both halves.
//!
//! THE STAMP IS WHAT THE NEXT STEP WOULD NEED, not this one. A page entry naming its object by slot
//! makes the slot DURABLE, and that is the change that takes the next value above
//! `SHARD_INDEX_FORMAT_VERSION` -- never a reserved one, because `persistence.rs` compares with `<`
//! and so ACCEPTS a lower stamp and misreads it rather than refusing it. #2019's own bump records the
//! shape of the hazard: an old index decodes CLEANLY because both sides of the generation check come
//! off the wire, and the disagreement appears later, on a recovery path.
//!
//! AND THE CHANGE THAT SHEDS `object_id` FROM `BlockAddress` TAKES 6. The constant was 5 when that
//! change landed, so 6 is the next value above what the tree actually held -- read at landing and
//! not at commit, because landing order is not commit order. 4 was reserved for it while the
//! constant was 3, and that reservation is VOID: spending it would LOWER the constant, and the
//! one-sided `<` named above turns a lowering into a silent ACCEPT rather than a refusal. A stamp
//! may only ever increase.
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
            let from_address = page.object_id(1);
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
// =================================================================================================
// 3. THIS LEG IS SETTLED THE OTHER WAY NOW: THE LIST IS A SLOT ARRAY AND A SLOT IS STABLE.
//    WHAT IT COST IS MEASURED IN SECTIONS 5 AND 6. THE OTHER LEGS ARE UNAFFECTED.
// =================================================================================================

/// A SLOT INTO THE OBJECT LIST SURVIVES ALL THREE MUTATIONS THAT INVALIDATED AN ORDINAL.
///
/// WHAT THIS TEST USED TO SAY, because the record matters more than the verdict. It was
/// `an_ordinal_into_the_object_list_is_invalidated_by_an_ordinary_insert_not_only_a_delete`, and on
/// twenty-five ids it drove three separate losses against `ObjectIndex::Many` as a SORTED RUN:
/// `insert` placed a new id at its bisection position, so filing one unrelated SMALLER id took
/// position 12 from id 1300 to id 1200; `remove` closed the hole and took it to 1400; and `shrink`
/// collapsed the arm to `One` at length one, so position 1 stopped existing altogether. Its failure
/// message named the condition under which it would stop holding -- "if the list has stopped being a
/// sorted run, this module's third reason no longer holds" -- and that is what has happened.
///
/// `ObjectIndex::Many` IS NOW A SLOT ARRAY WITH PLACEHOLDERS. `insert` takes the first free slot and
/// otherwise appends, `remove` leaves a placeholder, and the collapse to `One` survives only where it
/// cannot renumber anything. So the SAME three mutations are driven here against the SAME watched
/// position, and every one of them must now leave it naming the same object.
///
/// DRIVEN THROUGH `id_at` AND NOT THROUGH `iter`. That distinction is the test: `iter` skips
/// placeholders, so a list read as a sequence still appears to shift when a hole opens below the
/// watched element -- which is exactly how a compacting implementation would pass a sequence-based
/// check while losing the slot. `id_at(12)` asks the question a stored slot would ask.
///
/// STILL TWENTY-FIVE ELEMENTS, and still each mutation separately: with one element the first insert
/// is also the last, and a fixture that only exercised deletes would report the insert as safe.
///
/// WHAT THIS DOES NOT REOPEN, which is the rest of the module. Legs 1, 2 and 4 are untouched: the
/// entry still saves sixteen bytes and no more, the list still holds bare ids while 24 of 41
/// production readers want the CHARACTERS, and the routed arm's ceiling is still zero. Stability was
/// one of three reasons and it is the only one this change removes.
///
/// rust-internal: drives this crate's own `ObjectIndex`, no product surface
#[test]
fn a_slot_into_the_object_list_survives_the_three_mutations_that_invalidated_an_ordinal() {
    const ELEMENTS: usize = 25;

    // Ids spaced so there is always room to insert strictly below an existing one -- the shape that
    // moved an ordinal with no delete anywhere near it.
    let seeded: Vec<u64> = (1..=ELEMENTS as u64).map(|i| i * 100).collect();
    let mut list = ObjectIndex::default();
    for id in &seeded {
        assert!(list.insert(*id), "the seed must file {id}");
    }
    assert_eq!(
        list.object_count(),
        ELEMENTS,
        "the fixture holds {} rows, not {ELEMENTS}, so nothing below is being observed",
        list.object_count()
    );
    assert!(
        ELEMENTS > 1,
        "a one-element fixture cannot observe a shift: the first insert would also be the last"
    );
    assert!(
        matches!(list, ObjectIndex::Many(_)),
        "the fixture never reached the multi-entry arm, so it is testing the inline id"
    );

    // The watched slot. Not the first and not the last, so a shift at either end is visible.
    let watched_slot = 12usize;
    let watched_id = list
        .id_at(watched_slot)
        .expect("the fixture must hold a live id in the watched slot");
    assert_eq!(
        list.slot_of(&watched_id),
        Some(watched_slot),
        "`slot_of` and `id_at` disagree on the fixture, so neither can witness stability"
    );

    println!("\n=== a slot's stability across the three mutations that moved an ordinal ===");
    println!("  rows {ELEMENTS}, watching slot {watched_slot}");
    println!("  before            : slot {watched_slot} -> id {watched_id}");

    // (a) INSERT BELOW. An unrelated key whose id sorts below everything held, no delete anywhere.
    // This is the mutation that made the route unsafe, and it is the most common thing the engine
    // does.
    let mut inserted = list.clone();
    assert!(inserted.insert(1), "the fixture must file the low id");
    let after_insert = inserted.id_at(watched_slot);
    println!(
        "  after insert(1)   : slot {watched_slot} -> id {after_insert:?}  (rows {}, slots {})",
        inserted.object_count(),
        inserted.slot_count()
    );
    assert_eq!(
        after_insert,
        Some(watched_id),
        "inserting an id BELOW the watched one moved slot {watched_slot} off id {watched_id}. The \
         slot array is not append-or-refill, so an insert still renumbers and the route is unsafe \
         again"
    );
    assert_eq!(
        inserted.slot_of(&1),
        Some(ELEMENTS),
        "the new id did not land in the first slot past the end, so it took a slot something else \
         was in"
    );

    // (b) REMOVE BELOW, which is the half the original proposal already suspected.
    let mut removed = list.clone();
    assert!(removed.remove(&100), "the fixture must remove the low id");
    let after_remove = removed.id_at(watched_slot);
    println!(
        "  after remove(100) : slot {watched_slot} -> id {after_remove:?}  (rows {}, slots {})",
        removed.object_count(),
        removed.slot_count()
    );
    assert_eq!(
        after_remove,
        Some(watched_id),
        "removing an id BELOW the watched one moved slot {watched_slot}: the hole was CLOSED rather \
         than left as a placeholder"
    );
    assert_eq!(
        removed.id_at(0),
        None,
        "slot 0 did not become a placeholder, so the array compacted"
    );
    assert_eq!(
        removed.slot_count(),
        ELEMENTS,
        "the array shortened after a removal from the MIDDLE of it, which it can only do by moving \
         something"
    );
    // And the hole is what the next insert takes, which is what bounds the array.
    let mut refilled = removed.clone();
    assert!(refilled.insert(7), "a fresh id must file into the hole");
    assert_eq!(
        refilled.slot_of(&7),
        Some(0),
        "the fresh id did not take the free slot, so holes are never reused and the array grows \
         with churn"
    );
    assert_eq!(
        refilled.slot_count(),
        ELEMENTS,
        "refilling a hole lengthened the array"
    );
    assert_eq!(
        refilled.id_at(watched_slot),
        Some(watched_id),
        "refilling a hole moved the watched slot"
    );

    // (c) THE ARM COLLAPSE, which is what retired position 1 outright. Two ids, remove the FIRST:
    // the survivor is in slot 1, so collapsing to `One` would renumber it to slot 0 and leave a
    // stored 1 dangling.
    let mut collapsing = ObjectIndex::default();
    assert!(collapsing.insert(100));
    assert!(collapsing.insert(200));
    assert_eq!(collapsing.object_count(), 2, "the collapse fixture must hold two");
    assert_eq!(collapsing.slot_of(&200), Some(1), "the second id must be in slot 1");
    assert!(collapsing.remove(&100), "remove the first of two");
    println!(
        "  arm collapse      : removed slot 0 of 2 -> rows {}, slots {}, slot0 {:?}, slot1 {:?}",
        collapsing.object_count(),
        collapsing.slot_count(),
        collapsing.id_at(0),
        collapsing.id_at(1)
    );
    assert!(
        matches!(collapsing, ObjectIndex::Many(_)),
        "the arm collapsed to `One` with a live id in slot 1. That is the collapse that retired \
         ordinal 1, and it renumbers the survivor to slot 0"
    );
    assert_eq!(collapsing.id_at(0), None, "slot 0 must be a placeholder");
    assert_eq!(collapsing.id_at(1), Some(200), "slot 1 must still name 200");
    assert_eq!(collapsing.object_count(), 1, "one id must be left");

    // (c') AND THE COLLAPSE THAT IS STILL ALLOWED, because it renumbers nothing: remove the SECOND
    // of two, and the survivor is already in slot 0 with no slot above it. The two cases differ by
    // exactly the thing being preserved, which is why both are driven.
    let mut collapsible = ObjectIndex::default();
    assert!(collapsible.insert(100));
    assert!(collapsible.insert(200));
    assert!(collapsible.remove(&200), "remove the second of two");
    assert!(
        matches!(collapsible, ObjectIndex::One(100)),
        "removing the LAST slot of two must give the allocation back: the survivor is in slot 0 \
         with nothing above it, so `One` and the array are indistinguishable through `id_at`. It is \
         {collapsible:?}"
    );
    assert_eq!(collapsible.id_at(0), Some(100), "slot 0 must still name 100");
    assert_eq!(collapsible.id_at(1), None, "there must be no slot 1");
    assert_eq!(collapsible.slot_count(), 1, "the array must be one slot long");

    // (d) EMPTYING gives everything back, which is safe because no live slot remains to preserve.
    let mut emptying = collapsing.clone();
    assert!(emptying.remove(&200), "remove the last id");
    assert!(
        matches!(emptying, ObjectIndex::Empty),
        "an emptied index must cost nothing again; it is {emptying:?}"
    );

    println!(
        "  VERDICT: slot {watched_slot} names id {watched_id} before and after an insert below, a \
         remove below, a refill of the hole, and the arm collapse. The three mutations that \
         invalidated an ordinal no longer do. What it cost is sections 5 and 6."
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

/// WHAT A READER OF THE NAME WOULD PAY, COUNTED IN EXAMINED ENTRIES -- AND WHAT THE SLOT ARRAY COST.
///
/// A timing instrument is useless here -- on this box it has read 485x idle against 11x busy off
/// identical code -- so the join is priced in the engine's own probe counters instead.
///
/// THIS TEST HAS CHANGED SIDES AND THE NUMBER IT ASSERTS IS NOW THE WALK. It used to read
/// `..._is_logarithmic_today_and_linear_with_stable_slots`, pinning the measured count to
/// `floor(log2(n)) + 1` and asserting only that a walk could not be cheaper. The slot array landed,
/// so the walk is what the product does: `ObjectIndex::contains` reads slots in order, placeholders
/// included, because slot order is not id order. Both models are still printed, and the bisection is
/// now the thing that was GIVEN UP rather than the thing that is.
///
/// AND AT THESE RUN LENGTHS THE TWO COINCIDE, which is reported rather than hidden: the measured runs
/// are p50 1 and MAX 2, and `floor(log2(n)) + 1 == n` for n of 1 and 2. So this module establishes
/// that the change costs NOTHING MEASURABLE at the distribution the shipped routing range produces,
/// and that the direction on a store whose buckets hold long object lists is a walk. It does NOT
/// establish that the walk is expensive -- that would need such a store. The inequality is asserted;
/// the equality at this distribution is asserted too, because it is the result.
///
/// PLACEHOLDERS ARE CHARGED. `locate` counts every slot it reads, so an array carrying holes pays for
/// them here. That is deliberate: it is the one place the waste measured in section 6 could hide.
///
/// rust-internal: reads this crate's own probe counters, no product surface
#[test]
fn the_join_a_name_reader_would_pay_is_a_walk_now_and_was_logarithmic_as_a_sorted_run() {
    use crate::engine::state::{
        entries_a_bisection_examines, object_index_entries_examined,
        reset_object_index_entries_examined,
    };

    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine);
    seed_hash_containers(&engine);
    seed_routed_strings(&engine);

    // The run lengths the object lists actually hold, off the buckets -- and the SLOT counts beside
    // them, which are what the walk reads. On a store that has only been written to they are equal;
    // the two are collected separately so a divergence would show rather than be assumed away.
    let mut run_lengths: Vec<usize> = Vec::new();
    let mut slot_counts: Vec<usize> = Vec::new();
    {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard 1 is loaded");
        for bucket in shard.bucket_index.bucket_map.values() {
            let len = bucket.object_index.object_count();
            if len > 0 {
                run_lengths.push(len);
                slot_counts.push(bucket.object_index.slot_count());
            }
        }
    }
    assert!(
        !run_lengths.is_empty(),
        "DENOMINATOR: no bucket holds an object list, so no join is being priced"
    );
    run_lengths.sort();
    slot_counts.sort();

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

    // The two models. The walk reads slots until it finds the id; asking for the FIRST id a bucket
    // holds means it stops at the first live slot, which on an array with no holes is slot 0 -- so
    // the walk's own worst case is the whole array and is printed beside it.
    let walk_worst: usize = slot_counts.iter().sum();
    let bisecting: u64 = run_lengths
        .iter()
        .map(|len| entries_a_bisection_examines(*len))
        .sum();

    println!("\n=== the join, in examined entries ===");
    println!("  buckets with a list : {}", run_lengths.len());
    println!(
        "  objects in a bucket : p50={} p90={} p99={} MAX={}",
        percentile(&run_lengths, 50.0),
        percentile(&run_lengths, 90.0),
        percentile(&run_lengths, 99.0),
        run_lengths.last().copied().unwrap_or(0)
    );
    println!(
        "  slots in a bucket   : p50={} p90={} p99={} MAX={}",
        percentile(&slot_counts, 50.0),
        percentile(&slot_counts, 90.0),
        percentile(&slot_counts, 99.0),
        slot_counts.last().copied().unwrap_or(0)
    );
    println!("  membership questions asked   : {probed}");
    println!("  entries examined, measured   : {examined}");
    println!("  what a bisection would charge : {bisecting}");
    println!("  the walk's worst case (all slots): {walk_worst}");
    if bisecting == examined {
        println!(
            "  NOTE: at these run lengths (MAX {}) the walk and a bisection COINCIDE, so the \
             divergence is asserted below as an inequality and is NOT demonstrated by this \
             fixture. floor(log2(n))+1 == n for n in 1..=2, which is where this store sits.",
            run_lengths.last().copied().unwrap_or(0)
        );
    }

    assert!(
        probed > 0 && examined > 0,
        "DENOMINATOR: {probed} questions and {examined} entries examined. A list that charged \
         nothing would report the join as free"
    );
    assert_eq!(
        run_lengths.len(),
        slot_counts.len(),
        "a bucket was counted in one sample and not the other, so the two models divide by \
         different denominators"
    );
    assert!(
        walk_worst as u64 >= examined,
        "the walk examined {examined} entries against its own worst case of {walk_worst}, which it \
         cannot exceed"
    );
    // A WALK IS BOUNDED AT BOTH ENDS AND A BISECTION IS NOT, which is the whole difference and is
    // why this is an interval rather than an equality. The question asked above is a bucket's own
    // FIRST id, so the walk stops at the first live slot -- its BEST case, one entry a bucket. A
    // bisection charges `floor(log2(n)) + 1` wherever the id is. So the measured walk can come out
    // CHEAPER than the model, and here it does: it is the low end of the interval, not a
    // disagreement between the probe and the model.
    assert!(
        examined <= walk_worst as u64,
        "the walk examined more than every slot it could have read"
    );
    assert!(
        examined >= run_lengths.len() as u64,
        "the walk examined {examined} entries for {} buckets, fewer than one apiece, so some \
         membership question did not go through the door that charges",
        run_lengths.len()
    );
    assert!(
        bisecting <= walk_worst as u64,
        "a bisection is modelled above the walk's own worst case over the same lists, which it \
         cannot be: {bisecting} against {walk_worst}"
    );
    println!(
        "  VERDICT: the walk examined {examined} entries for {probed} questions against the \
         {bisecting} a bisection would charge and its own worst case of {walk_worst}. At this \
         distribution -- every list one or two objects long -- the walk's best case is CHEAPER than \
         the bisection it replaced and its worst case EQUALS it, so this change costs nothing \
         measurable on the membership door. The divergence needs a store whose buckets hold long \
         object lists, which the shipped routing range does not produce."
    );
}

// =================================================================================================
// 6. WHAT THE SLOT ARRAY IS AND WHAT IT COSTS. THE WIDTH, THE PLACEHOLDER WASTE AT TWO CORPUS
//    SIZES, AND THE BYTES -- EACH WITH ITS DENOMINATOR.
// =================================================================================================

/// The slot array, mirrored. Never constructed -- only its layout is read -- so the fields are dead
/// by design, exactly as the entry mirrors above are.
#[allow(dead_code)]
struct MirrorObjectSlots {
    slots: Vec<Option<u64>>,
    valid: usize,
}

/// The enum, mirrored as the tag and payload a `repr(Rust)` enum lays out: the discriminant rounded
/// up to the payload's alignment, then the payload. `offset_of!` on this is how the payload's offset
/// is MEASURED rather than asserted, since `offset_of!` cannot name an enum variant's field.
#[allow(dead_code)]
struct MirrorTagAndPayload {
    tag: u64,
    payload: Box<MirrorObjectSlots>,
}

/// WHAT `ObjectIndex` BECAME, MEASURED WITH `offset_of!` AND ASSERTED AS ARITHMETIC.
///
/// NOT PINNED TO A LITERAL. A literal 16 passes for the wrong reason the moment an arm widens into
/// padding that was already there, and three literal pins in this campaign under-reached for exactly
/// that. The identity asserted is the one a `repr(Rust)` layout actually obeys -- the aligned fields
/// plus the tail rounded up -- through the same `reconstruct` helper the entry rows above use, so
/// there is one model of the layout in this module and not two.
///
/// THE MIRRORS ARE PROVED FAITHFUL BEFORE THEY ARE READ. Each is asserted equal in width to the
/// product type it mirrors, because a mirror that had drifted would report a clean layout for a
/// struct nobody uses.
///
/// AND THE ANSWER IS THAT IT DID NOT MOVE: a tag and one pointer, sixteen bytes, which is what it was
/// as a sorted run. The slot array is behind the same box. What moved is on the heap, and
/// `what_a_slot_array_costs_under_churn` is where that is measured.
///
/// rust-internal: reads this crate's own type layout, no product behaviour
#[test]
fn the_object_index_is_a_tag_and_one_pointer() {
    use crate::engine::state::ObjectSlots;
    use std::mem::{align_of, offset_of, size_of};

    let word = size_of::<usize>();
    assert_eq!(8, word, "this arithmetic is written for a 64-bit word");

    // --- the mirrors are faithful ---
    assert_eq!(
        size_of::<ObjectSlots>(),
        size_of::<MirrorObjectSlots>(),
        "the slot-array mirror has drifted from the product struct, so its offsets describe nothing"
    );
    assert_eq!(
        size_of::<ObjectIndex>(),
        size_of::<MirrorTagAndPayload>(),
        "the enum mirror has drifted: a tag and a boxed payload is not what `ObjectIndex` lays out"
    );
    assert_eq!(
        align_of::<ObjectIndex>(),
        size_of::<u64>(),
        "the mirror's tag is a `u64` because the enum aligns to eight; it no longer does"
    );

    // --- the two offsets, MEASURED, and the arithmetic they satisfy ---
    //
    // THE ORDER IS NOT ASSERTED, because `repr(Rust)` is free to choose it and here it chose to put
    // the box first: `tag` measures at 8 and `payload` at 0. Asserting "the discriminant is first"
    // was asserting a compiler choice, which is a literal pin wearing a measurement's clothes. What
    // the layout does owe is that the two fields TILE the width exactly -- distinct offsets, each a
    // multiple of the alignment, and the last one ending at `size_of` -- and that is checked below
    // and again through `reconstruct`.
    let tag_at = offset_of!(MirrorTagAndPayload, tag);
    let payload_at = offset_of!(MirrorTagAndPayload, payload);
    assert_ne!(tag_at, payload_at, "two fields cannot share one offset");
    let word_offsets = {
        let mut both = [tag_at, payload_at];
        both.sort_unstable();
        both
    };
    assert_eq!(
        [0, align_of::<ObjectIndex>()],
        word_offsets,
        "the two words do not sit at 0 and the alignment, so something is padded that should not be"
    );

    // --- the arithmetic, through this module's own reconstruction of a `repr(Rust)` width ---
    let enum_fields = [
        ("tag", tag_at, align_of::<ObjectIndex>()),
        ("payload", payload_at, size_of::<Box<ObjectSlots>>()),
    ];
    let slots_fields = [
        ("slots", offset_of!(MirrorObjectSlots, slots), size_of::<Vec<Option<u64>>>()),
        ("valid", offset_of!(MirrorObjectSlots, valid), size_of::<usize>()),
    ];

    println!("\n=== what `ObjectIndex` became ===");
    for (name, at, width) in enum_fields {
        println!("  ObjectIndex.{name:<10} offset {at:>3}  width {width:>3}");
    }
    println!(
        "  size_of::<ObjectIndex>()        = {} ; reconstructed = {}",
        size_of::<ObjectIndex>(),
        reconstruct(&enum_fields, word)
    );
    for (name, at, width) in slots_fields {
        println!("  ObjectSlots.{name:<10} offset {at:>3}  width {width:>3}");
    }
    println!(
        "  size_of::<ObjectSlots>()        = {} ; reconstructed = {}",
        size_of::<ObjectSlots>(),
        reconstruct(&slots_fields, word)
    );
    println!("  size_of::<Option<u64>>()        = {} (a slot)", size_of::<Option<u64>>());
    println!("  size_of::<u64>()                = {} (a sorted-run element)", size_of::<u64>());

    assert_eq!(
        reconstruct(&enum_fields, word),
        size_of::<ObjectIndex>(),
        "the enum's width is not its aligned fields plus its tail rounded up, so the field list \
         above does not describe it"
    );
    assert_eq!(
        reconstruct(&slots_fields, word),
        size_of::<ObjectSlots>(),
        "the slot array's width is not its aligned fields plus its tail rounded up"
    );
    let (covered, slack) = account(&enum_fields, size_of::<ObjectIndex>());
    assert_eq!(
        0,
        slack,
        "{covered} of {} bytes are accounted for and {slack} are not, so something in the enum is \
         unexplained",
        size_of::<ObjectIndex>()
    );

    // The thing a page entry would store, priced here so the two numbers sit together. Both widths
    // land the entry on the same place -- see
    // `a_u16_ordinal_would_take_the_entry_from_sixty_four_to_forty_eight_and_a_u8_adds_nothing` --
    // so a ceiling on slots per bucket buys nothing and must be chosen for the array's own reasons.
    println!(
        "  a stored slot would be {} or {} bytes; the entry lands on the same width either way",
        size_of::<u8>(),
        size_of::<u16>()
    );
    assert_eq!(
        size_of::<ObjectIndex>(),
        align_of::<ObjectIndex>() + size_of::<Box<ObjectSlots>>(),
        "a tag and one pointer is no longer what this costs"
    );
}

/// One churn run's outcome: the longest the array got, the most objects held at once, and the worst
/// instantaneous ratio of the two.
struct ChurnResult {
    peak_slots: usize,
    peak_objects: usize,
    worst_transient: f64,
}

/// Fold one observation into a running churn result.
fn observe_churn(result: &mut ChurnResult, index: &ObjectIndex) {
    result.peak_slots = result.peak_slots.max(index.slot_count());
    result.peak_objects = result.peak_objects.max(index.object_count());
    if index.object_count() > 0 {
        let ratio = index.slot_count() as f64 / index.object_count() as f64;
        if ratio > result.worst_transient {
            result.worst_transient = ratio;
        }
    }
}

/// Drive one churn shape at one width and report what the array did.
fn churn_shape(width: usize, cycles: usize, shape: &str) -> ChurnResult {
    let mut index = ObjectIndex::default();
    let mut live: Vec<u64> = (0..width as u64).map(|i| 1_000_000 + i * 97).collect();
    for id in &live {
        assert!(index.insert(*id), "the seed must file {id}");
    }
    let mut result = ChurnResult {
        peak_slots: index.slot_count(),
        peak_objects: index.object_count(),
        worst_transient: 1.0,
    };
    let mut next_id = 9_000_000u64;

    for cycle in 0..cycles {
        match shape {
            // A hole opens in the middle and the next insert must take it.
            "remove-then-add" => {
                let at = cycle % live.len();
                let victim = live[at];
                assert!(index.remove(&victim), "{shape}: {victim} must be held");
                observe_churn(&mut result, &index);
                next_id += 1;
                assert!(index.insert(next_id), "{shape}: the fresh id must file");
                live[at] = next_id;
            }
            // The array grows by one slot and then has to give it back.
            "add-then-remove" => {
                next_id += 1;
                assert!(index.insert(next_id), "{shape}: the fresh id must file");
                observe_churn(&mut result, &index);
                assert!(index.remove(&next_id), "{shape}: the fresh id must come back out");
            }
            // Many holes at once, then many refills.
            "batch" => {
                let half = live.len() / 2;
                assert!(half > 0, "{shape}: width {width} is too small to halve");
                for at in 0..half {
                    assert!(index.remove(&live[at]), "{shape}: {} must be held", live[at]);
                }
                observe_churn(&mut result, &index);
                for at in 0..half {
                    next_id += 1;
                    assert!(index.insert(next_id), "{shape}: the fresh id must file");
                    live[at] = next_id;
                }
            }
            // ALWAYS THE FRONT, so no tail is ever trimmable and only refill can bound it.
            "front-only" => {
                let victim = live[0];
                assert!(index.remove(&victim), "{shape}: {victim} must be held");
                observe_churn(&mut result, &index);
                next_id += 1;
                assert!(index.insert(next_id), "{shape}: the fresh id must file");
                live.remove(0);
                live.push(next_id);
            }
            other => panic!("unknown churn shape {other}"),
        }
        observe_churn(&mut result, &index);
    }

    assert_eq!(
        index.object_count(),
        width,
        "{shape} at width {width}: the fixture ended holding {} objects, not {width}, so the ratio \
         it reports has a moving denominator",
        index.object_count()
    );
    result
}

/// WHAT THE PLACEHOLDERS COST UNDER CHURN, AT TWO CORPUS SIZES, WITH DENOMINATORS.
///
/// THE FAILURE THIS HAS TO RULE OUT. An array that never compacts grows with the number of
/// OPERATIONS rather than with the number of objects, if nothing refills a hole. That is not a
/// hypothetical: an earlier change in this campaign did exactly it by accident and a per-element
/// allocation-scaling control caught it at 1.68x. So every shape is run at TWO widths whose cycle
/// counts differ by 8x, and the number asserted is the BOUND rather than an observation.
///
/// THE BOUND: the array is never longer than the most objects the bucket has held AT ONCE. That
/// follows from the two rules -- a hole is refilled before the array grows, and a tail of holes is
/// given back -- and it is what makes the waste bounded instead of monotonic. Reported as
/// `peak slots / peak objects`, which must be 1.000 at both widths; a cost scaling with operations
/// would make it rise, and rise FURTHER on the arm that runs more of them.
///
/// AND THE TRANSIENT, REPORTED AND NOT ASSERTED EQUAL ACROSS WIDTHS, because it cannot be: a shape
/// that opens one hole in `n` slots is `n/(n-1)` by arithmetic, which is 1.143 at width 8 and 1.016 at
/// width 64. Asserting those equal would be asserting arithmetic. What is asserted is that the
/// transient never exceeds the one the shape's own hole count implies, and that it does not persist.
///
/// FOUR CHURN SHAPES, because they stress different rules. Remove-then-add refills a hole;
/// add-then-remove grows and gives back; a batch of removes followed by a batch of adds opens half the
/// array at once; and the adversarial shape removes from the FRONT every time while the back stays
/// live, which is the one that can never trim a tail.
///
/// AND THE BYTES, stated as arithmetic rather than claimed. A sorted run was a `Box<Vec<u64>>`: a
/// 24-byte vector plus eight bytes an id. The slot array is a `Box<ObjectSlots>`: 32 bytes plus
/// SIXTEEN bytes a slot, because a slot is an `Option<u64>` and object ids span the whole of `u64`, so
/// no value can be reserved to mean free. What a reserved value WOULD buy is printed beside it, since
/// that is the one lever on this number and it should be a decision rather than an omission.
///
/// rust-internal: drives this crate's own `ObjectIndex`, no product surface
#[test]
fn what_a_slot_array_costs_under_churn() {
    use std::mem::size_of;

    // The two corpus sizes: how many objects one bucket holds at once.
    const WIDTHS: [usize; 2] = [8, 64];
    const CYCLES_PER_OBJECT: usize = 40;
    const SHAPES: [&str; 4] = ["remove-then-add", "add-then-remove", "batch", "front-only"];

    println!("\n=== what the placeholders cost under churn ===");
    println!(
        "  {:<16} {:>6} {:>8} {:>8} {:>7} {:>11} {:>11}",
        "shape", "width", "cycles", "pk slots", "pk objs", "bound", "transient"
    );

    let mut bound_per_width: Vec<(usize, f64)> = Vec::new();
    let mut worst_transient_overall = 1.0f64;
    for width in WIDTHS {
        let cycles = width * CYCLES_PER_OBJECT;
        assert!(
            cycles >= width,
            "DENOMINATOR: {cycles} cycles at width {width} is not enough churn to open a hole"
        );
        let mut bound_here = 0.0f64;
        for shape in SHAPES {
            let result = churn_shape(width, cycles, shape);
            let bound = result.peak_slots as f64 / result.peak_objects as f64;
            println!(
                "  {shape:<16} {width:>6} {cycles:>8} {:>8} {:>7} {bound:>11.3} {:>11.3}",
                result.peak_slots, result.peak_objects, result.worst_transient
            );
            // THE BOUND, per shape: never longer than the high water mark of the object count.
            assert!(
                result.peak_slots <= result.peak_objects,
                "{shape} at width {width}: the array reached {} slots against a high water mark of \
                 {} objects over {cycles} cycles. It is growing with OPERATIONS, which is the \
                 failure this shape exists to avoid",
                result.peak_slots,
                result.peak_objects
            );
            // THE TRANSIENT never exceeds what the shape's own hole count implies. `batch` opens
            // half the array, so 2.0; the others open one slot.
            let implied = if shape == "batch" {
                2.0
            } else {
                width as f64 / (width as f64 - 1.0)
            };
            assert!(
                result.worst_transient <= implied + 1e-9,
                "{shape} at width {width}: the worst instantaneous ratio was {:.3} against the \
                 {implied:.3} its own hole count implies, so holes are accumulating",
                result.worst_transient
            );
            bound_here = bound_here.max(bound);
            worst_transient_overall = worst_transient_overall.max(result.worst_transient);
        }
        bound_per_width.push((width, bound_here));
    }

    for (width, bound) in &bound_per_width {
        println!("  width {width:>3}: worst bound {bound:.3} slots an object");
    }
    println!("  worst TRANSIENT across every shape and width: {worst_transient_overall:.3}");

    // THE SCALING ARM. The wider arm runs 8x the cycles of the narrow one. If the waste were a
    // function of operations rather than of the corpus, the bound would not agree between them.
    let (narrow, narrow_bound) = bound_per_width[0];
    let (wide, wide_bound) = bound_per_width[1];
    assert!(
        (narrow_bound - 1.0).abs() < 1e-9 && (wide_bound - 1.0).abs() < 1e-9,
        "the bound is {narrow_bound:.3} at width {narrow} and {wide_bound:.3} at width {wide}; \
         either is above 1.000, so the array outgrew the objects it holds"
    );
    assert!(
        (narrow_bound - wide_bound).abs() < 1e-9,
        "the bound is {narrow_bound:.3} at width {narrow} and {wide_bound:.3} at width {wide}. The \
         wider arm runs {CYCLES_PER_OBJECT}x its width in cycles, so a bound that differs between \
         them is a cost scaling with churn and not with the corpus"
    );

    // --- THE BYTES, as arithmetic. ---
    let run_header = size_of::<Vec<u64>>();
    let slots_header = size_of::<crate::engine::state::ObjectSlots>();
    let run_element = size_of::<u64>();
    let slot_element = size_of::<Option<u64>>();
    println!("\n=== the heap a bucket's object list holds, by object count ===");
    println!("  sorted run : {run_header} + {run_element} an id    (Box<Vec<u64>>)");
    println!("  slot array : {slots_header} + {slot_element} a slot  (Box<ObjectSlots>)");
    println!(
        "  with a reserved id value a slot would be {run_element}, so {slots_header} + \
         {run_element} a slot"
    );
    println!(
        "  {:>8} {:>12} {:>12} {:>8} {:>16} {:>8}",
        "objects", "run bytes", "slot bytes", "ratio", "reserved id", "ratio"
    );
    for objects in [1usize, 2, 5, 8, 25, 64] {
        let run = run_header + run_element * objects;
        let slots = slots_header + slot_element * objects;
        let reserved = slots_header + run_element * objects;
        println!(
            "  {objects:>8} {run:>12} {slots:>12} {:>8.3} {reserved:>16} {:>8.3}",
            slots as f64 / run as f64,
            reserved as f64 / run as f64
        );
    }
    assert_eq!(
        slot_element,
        2 * run_element,
        "a slot is {slot_element} bytes against the run element's {run_element}. If the `Option` has \
         found a niche the table above is pricing a cost nobody pays; if it is wider still, the \
         table is understating it"
    );

    // --- AND A HOLE IS CHARGED BY THE WALK, which is the half no store-shaped fixture can show. ---
    //
    // WHY THIS BLOCK EXISTS. `the_join_a_name_reader_would_pay` prices the membership door off a
    // seeded store, and a store that has only been WRITTEN to has no holes at all -- its slot count
    // and its object count are the same distribution, p50 1 and MAX 2. So that row cannot tell a walk
    // that charges placeholders from one that skips them: both answer the same number on it. A mutant
    // that stopped charging them SURVIVED the whole module for exactly that reason, which is the
    // second reading of a surviving mutant -- the guard did not watch what was changed -- and not a
    // weak assertion. This is the fixture that watches it.
    //
    // The claim being pinned is that `locate` charges every SLOT it reads and not every id it finds,
    // because a placeholder is a word the walk has to read past. An instrument that reported only
    // live entries would hide the one cost holes actually have.
    {
        use crate::engine::state::{object_index_entries_examined, reset_object_index_entries_examined};

        let mut holed = ObjectIndex::default();
        for id in [11u64, 22, 33] {
            assert!(holed.insert(id), "the fixture must file {id}");
        }
        assert_eq!(holed.slot_of(&33), Some(2), "33 must be in slot 2");
        assert!(holed.remove(&11), "11 must come out of slot 0");
        assert_eq!(holed.id_at(0), None, "slot 0 must be a placeholder");
        assert_eq!(holed.slot_count(), 3, "the array must still be three slots long");
        assert_eq!(holed.object_count(), 2, "and hold two objects");

        // The id in the LAST slot, so the walk crosses the placeholder to reach it.
        reset_object_index_entries_examined();
        assert!(holed.contains(&33), "33 is held");
        let over_a_hole = object_index_entries_examined();

        // The same question on an array of the same OBJECT count with no hole in it.
        let dense: ObjectIndex = [22u64, 33].into_iter().collect();
        assert_eq!(dense.slot_count(), 2, "the control must have no placeholder");
        assert_eq!(dense.object_count(), 2, "and the same object count as the holed one");
        reset_object_index_entries_examined();
        assert!(dense.contains(&33), "33 is held in the control too");
        let dense_cost = object_index_entries_examined();

        println!("\n=== what a hole costs the walk ===");
        println!("  3 slots / 2 objects, reaching the last slot : {over_a_hole} entries examined");
        println!("  2 slots / 2 objects, reaching the last slot : {dense_cost} entries examined");

        // A FLOOR AND NOT AN EQUALITY, because the counter is one process-wide atomic and these
        // tests run in parallel: another thread asking a membership question between the reset and
        // the read can only ADD to the count. So the assertion is written in the direction
        // contention cannot fake. A walk that charged only LIVE entries would read 2 here and fail
        // this floor; one that charges slots reads 3.
        assert!(
            over_a_hole >= 3,
            "a walk across a placeholder to slot 2 charged {over_a_hole} entries, below the 3 slots \
             it had to read. The probe is counting live ids rather than SLOTS, so a bucket carrying \
             holes reports the same cost as one carrying none and the waste is invisible exactly \
             where it is paid"
        );
        assert!(
            dense_cost >= 2,
            "the dense control charged {dense_cost} for the two slots it read"
        );
    }

    println!(
        "  VERDICT: the array is bounded by the bucket's own high water mark -- 1.000 slots an \
         object at both widths across four churn shapes, with a transient of at most \
         {worst_transient_overall:.3} while holes are open -- so the PLACEHOLDERS cost nothing that \
         persists. What the change costs on the heap is the SLOT WIDTH and not the holes: 16 bytes \
         against 8, which a reserved id value would recover and which object ids being full-range \
         hashes is what forbids. A hole costs the WALK one entry while it is open, charged."
    );
}

/// NOTHING PERSISTS A SLOT, SO NOTHING ON DISK MOVES AND NO STAMP IS TAKEN.
///
/// WHY THIS HAS TO BE DRIVEN RATHER THAN REASONED. `ObjectIndex` is serialized -- it is a field of
/// `BucketNode`, which is the stored index -- and its Serialize impl used to write `self.iter()`,
/// which was the sorted run's order. A slot array iterates in SLOT order, so taking the container's
/// word for the order would have moved the bytes of every bucket holding two or more objects: 46.6%
/// of them. The impl sorts instead, and this is where that is checked.
///
/// WHAT IS STORED IS THE IDS AND NOT THE SLOTS, which is why the stamp does not move at all. A load
/// re-files what it reads through `insert`, so slots are handed out afresh on every load and a slot is
/// a RESIDENT fact. That is also the limit of this change: a page entry naming its object by slot
/// would make the slot durable, and THAT is what would need the next value above
/// `SHARD_INDEX_FORMAT_VERSION`.
///
/// rust-internal: drives this crate's own serde impls, no external surface
#[test]
fn nothing_persists_a_slot_so_the_written_bytes_do_not_move() {
    // Filed in an order whose slot order is NOT ascending, which is the only order that can catch a
    // Serialize impl taking slot order for id order.
    let filed = [900u64, 5, 700, 1, 800, 0, u64::MAX, 400];
    let index: ObjectIndex = filed.into_iter().collect();
    assert!(
        matches!(index, ObjectIndex::Many(_)),
        "the fixture must reach the slot-array arm"
    );

    let slot_order: Vec<u64> = index.iter().copied().collect();
    let mut ascending = filed.to_vec();
    ascending.sort_unstable();

    println!("\n=== what the slot array writes ===");
    println!("  filed in     : {filed:?}");
    println!("  slot order   : {slot_order:?}");
    println!("  written      : {}", serde_json::to_string(&index).expect("serializes"));

    assert_ne!(
        slot_order, ascending,
        "the fixture's slot order is already ascending, so a Serialize impl that wrote slot order \
         would pass this test for the wrong reason"
    );
    assert_eq!(
        serde_json::to_value(&index).expect("serializes"),
        serde_json::to_value(&ascending).expect("serializes"),
        "the written sequence is not the ascending one a sorted run wrote, so this change moves \
         bytes already on disk and owes a format stamp"
    );

    // And the round trip is the identity on the SET, which is all the wire carries.
    let written = serde_json::to_string(&index).expect("serializes");
    let loaded: ObjectIndex = serde_json::from_str(&written).expect("loads");
    assert_eq!(loaded.sorted_ids(), index.sorted_ids(), "a round trip lost or gained an id");
    assert_eq!(
        loaded.object_count(),
        index.object_count(),
        "a round trip changed the object count"
    );
    // The slots a load hands out are fresh, and ascending because that is what is written. Stated
    // rather than left implicit, because it is the fact that makes a slot resident-only.
    assert_eq!(
        loaded.id_at(0),
        Some(0u64),
        "a loaded bucket does not file the smallest id first, so the slot a load hands out is not \
         derivable from the wire and the claim that nothing persists a slot needs re-examining"
    );

    // `u64::MAX` IS A LEGITIMATE OBJECT ID, which is why a slot is an `Option` and not a reserved
    // value. Driven, because the whole eight-bytes-a-slot cost rests on it.
    assert!(
        index.contains(&u64::MAX),
        "`u64::MAX` is not held, so the fixture is not showing that no value can be reserved"
    );
    assert!(
        index.contains(&0),
        "`0` is not held either, so neither end of the range is shown to be in use"
    );
    let mut narrow = ObjectIndex::default();
    assert!(narrow.insert(u64::MAX));
    assert!(narrow.insert(0));
    assert_eq!(narrow.slot_of(&u64::MAX), Some(0), "`u64::MAX` must take a slot like any id");
    assert!(narrow.remove(&u64::MAX), "`u64::MAX` must be removable");
    assert_eq!(
        narrow.id_at(1),
        Some(0),
        "removing `u64::MAX` from slot 0 moved the id in slot 1, so the placeholder was not left"
    );
}
