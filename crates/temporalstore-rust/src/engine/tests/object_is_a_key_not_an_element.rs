// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! AN OBJECT IS A KEY, NOT ONE OF ITS ELEMENTS.
//!
//! `stable_block_object_id` folded the component into the object's identity, so an object held
//! exactly one page: a hash of twenty-five fields was twenty-five objects that happened to share a
//! key. #1986 recorded the result as "the object id names a page's triple, not an object" -- a
//! DESCRIPTION OF A DEFECT rather than a property to preserve. The parameter is REMOVED here, not
//! merely ignored, because keeping it and discarding its value would leave every call site
//! compiling while the meaning changed underneath it.
//!
//! # WHY THIS IS AVAILABLE ONLY NOW
//!
//! #2011 refuted it, and the mechanism was precise: the object id was the only discriminator
//! between pages INSIDE ONE WAL RECORD, because `StagedBlock` was `{ object_id, bytes }` and
//! `block_in_wal::read_block` picked with `find(|page| page.object_id == object_id)`. One record
//! routinely carries many pages -- `append_batch_as_one_record` writes a whole batch as one -- so a
//! component-free id served the FIRST page's bytes for the second. Measured there: 2/2 today,
//! 1/2 with WRONG BYTES component-free, 96.00% of hash pages unreachable at batch scale.
//!
//! #2013 closed exactly that. `stage()` takes a component, `StagedBlock` carries it, and both
//! in-record `find` sites share one predicate: `page.object_id == object_id &&
//! page.component.as_deref() == component`. The registry key became `(u64, Option<Arc<str>>)`.
//! Its measured result was 96.00% -> 0.00% unreachable with a negative control still at 1/2.
//!
//! So the acceptance test for THIS change is #2011's failing case passing on the product's own
//! read path, and it lives in `carried_page_identity` where the BEFORE numbers were taken. That
//! module's `free` id is no longer a simulation -- it is simply what `stable_block_object_id`
//! returns.
//!
//! # WHAT THIS MODULE HOLDS
//!
//! The consequences that are NOT the in-record read:
//!
//!   1. THE PRIZE, read off `bucket.object_index` itself rather than off a grouping of page
//!      entries, with the distribution that says where the effect exists.
//!   2. THE TOMBSTONE, which is where this most easily becomes data loss. `deleted_object_index`
//!      is filed per removed page's id, and one id now covers every element of a key.
//!   3. HLEN, asserted unmoved beside the listing it must agree with.
//!   4. THE READ PATH, asserted from outside to select on the component at every arm, including
//!      the first-match fallback #1964 enumerates.
//!
//! # THE HANDLE CONSEQUENCE, AND THE STAMP
//!
//! `state::block_index_handle` hashes `address.generation()`, `generation` is
//! `block_id.or(object_id)`, and for a WAL-resident page the block id is absent -- so the
//! generation IS the object id and every such handle moves with it. Handles are written to disk
//! inside the lookup's refs. `engine::SHARD_INDEX_FORMAT_VERSION` is bumped 2 -> 3 for that
//! reason, and the reason is spelled out where the constant is declared: an old index decodes
//! CLEANLY, because `BlockAddress::try_from` compares the stored `g` against the stored
//! `block_id.or(object_id)` and an old row agrees with ITSELF. The disagreement appears later,
//! between a stored `object_index` and a recomputed `stable_block_object_id`, on a recovery path.

#![allow(clippy::all)]
use super::*;
use std::collections::BTreeSet;

use crate::engine::hashing::stable_block_object_id;

const OPERATOR_END: u32 = 1023;
/// Eight keys and twenty-five fields: the population `carried_page_identity` and
/// `entry_object_identity` both use, so the rows here are comparable to theirs line for line.
const CONTAINER_KEYS: usize = 8;
const MEMBERS_PER_KEY: usize = 25;
/// The CONTROL population. A string page's component is already `None`, so this change is the
/// IDENTITY on it and the mechanism predicts no movement whatever.
const ROUTED_KEYS: usize = 200;

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
        table_name: "object-is-a-key".to_string(),
        shard_uri: "local://object-is-a-key/1".to_string(),
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

fn write(engine: &TemporalEngine, command: Command) {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(response.status.ok, "the fixture write failed: {response:?}");
}

fn read(engine: &TemporalEngine, command: Command) -> crate::types::CommandResponse {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(response.status.ok, "the fixture read failed: {response:?}");
    response.response
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

/// `(live pages, distinct object-index rows)` for one kind, read off the buckets themselves.
///
/// `object_index` is the thing this change shrinks, so it is read DIRECTLY rather than inferred
/// from a grouping of page entries. Only rows belonging to the kind's own keys are counted, so one
/// arm's rows cannot leak into the other's.
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

fn tombstone_holds(engine: &TemporalEngine, object_id: u64) -> bool {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 is loaded");
    shard
        .bucket_index
        .bucket_map
        .values()
        .any(|bucket| bucket.deleted_object_index.contains(&object_id))
}

/// The components of one key's live pages, sorted. An absent component reads as the empty string,
/// which is distinguishable here because no fixture in this module writes one.
fn live_components(engine: &TemporalEngine, kind: &str, key: &str) -> Vec<String> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 is loaded");
    let mut held = Vec::new();
    for bucket in shard.bucket_index.bucket_map.values() {
        for page in bucket.block_index.values() {
            if !page.deleted && page.model_id.as_str() == kind && &*page.object_key == key {
                held.push(page.component.as_deref().unwrap_or("").to_string());
            }
        }
    }
    held.sort();
    held
}

fn percentile(sorted: &[usize], p: f64) -> usize {
    assert!(!sorted.is_empty(), "no sample to take a percentile of");
    let rank = ((p / 100.0) * sorted.len() as f64).ceil().max(1.0) as usize;
    sorted[rank.min(sorted.len()) - 1]
}

// =================================================================================================
// 1. THE PRIZE: ONE OBJECT-INDEX ROW PER KEY, READ OFF THE INDEX ITSELF.
// =================================================================================================

/// `bucket.object_index` HOLDS ONE ROW PER KEY INSTEAD OF ONE PER PAGE.
///
/// #2007 measured the row count at 0.9983 per page, and grouping the same pages on `(kind, key)`
/// at MAX 100 per object, saving 6.409-6.496 B/page. THE HONEST CAVEAT FROM THAT WORK IS REPEATED
/// HERE RATHER THAN BURIED: across a real corpus, pages per `(kind, key)` is p50 1, p90 1, p99 1.
/// Ninety-nine percent of keys hold ONE page, so on a general population this saves almost
/// nothing -- it is NOT 30 bytes an object. It is the CONTAINER population -- hashes, sets, zsets,
/// lists -- where a key holds many elements and the row count collapses, and that is the
/// population seeded here.
///
/// So the number below is not a claim about a whole store. It is the size of the effect where the
/// effect exists, printed beside the distribution that says where that is.
///
/// THE CONTROL is the routed string arm, whose component was already `None`: one page per key
/// before and after, so its rows-per-page must be 1.0000. Its page count is asserted FIRST -- a
/// control that read nothing reports no movement for the wrong reason.
///
/// rust-internal: reads the engine's own bucket index, no external surface
#[test]
fn the_object_index_holds_one_row_per_key_rather_than_one_per_page() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine);
    seed_hash_containers(&engine);
    seed_routed_strings(&engine);

    let hash_keys: BTreeSet<String> = (0..CONTAINER_KEYS).map(|k| format!("h{k}")).collect();
    let string_keys: BTreeSet<String> = (0..ROUTED_KEYS).map(|i| format!("s{i}")).collect();

    let (hash_pages, hash_rows) = pages_and_index_rows(&engine, "hash", &hash_keys);
    let (string_pages, string_rows) = pages_and_index_rows(&engine, "string", &string_keys);

    println!("\n=== bucket.object_index rows against live pages ===");
    println!("  kind     keys  pages  rows  rows/page");
    for (kind, keys, pages, rows) in [
        ("hash", hash_keys.len(), hash_pages, hash_rows),
        ("string", string_keys.len(), string_pages, string_rows),
    ] {
        println!(
            "  {kind:<7} {keys:>5} {pages:>6} {rows:>5}   {:>8.4}",
            rows as f64 / pages as f64
        );
    }

    // DENOMINATORS FIRST.
    assert_eq!(
        hash_pages,
        CONTAINER_KEYS * MEMBERS_PER_KEY,
        "the container arm exercised {hash_pages} pages, not {}",
        CONTAINER_KEYS * MEMBERS_PER_KEY
    );
    assert_eq!(
        string_pages, ROUTED_KEYS,
        "CONTROL NOT EXERCISED: {string_pages} string pages, not {ROUTED_KEYS}"
    );

    // THE DELIVERABLE.
    assert_eq!(
        hash_rows, CONTAINER_KEYS,
        "the container arm holds {hash_rows} object-index rows for {CONTAINER_KEYS} keys and \
         {hash_pages} pages. One key is one object now, so the row count is the KEY count -- if it \
         is the PAGE count, the component is still reaching the id"
    );
    assert!(
        hash_pages > hash_rows,
        "DENOMINATOR: {hash_pages} pages and {hash_rows} rows. A fixture whose keys held one page \
         each would satisfy the assertion above while measuring nothing"
    );

    // THE CONTROL, flat by construction.
    assert_eq!(
        string_rows, ROUTED_KEYS,
        "the control holds {string_rows} rows for {ROUTED_KEYS} one-page keys. Its component was \
         already `None`, so this change is the identity on it and the count cannot move"
    );
    println!(
        "  VERDICT: container rows/page {:.4} (was 1.0000, one row per page); CONTROL string \
         rows/page {:.4}, unmoved, {string_pages} pages exercised.",
        hash_rows as f64 / hash_pages as f64,
        string_rows as f64 / string_pages as f64
    );

    // AND THE DISTRIBUTION THAT SAYS WHERE THIS APPLIES, as #2007 measured it. A histogram rather
    // than a mean, because the whole question is the tail.
    let mut per_key: Vec<usize> = Vec::new();
    for key in &hash_keys {
        per_key.push(live_components(&engine, "hash", key).len());
    }
    for key in &string_keys {
        per_key.push(live_components(&engine, "string", key).len());
    }
    per_key.sort();
    println!(
        "  pages per (kind,key) over {} keys: p50={} p90={} p99={} MAX={}",
        per_key.len(),
        percentile(&per_key, 50.0),
        percentile(&per_key, 90.0),
        percentile(&per_key, 99.0),
        per_key.last().copied().unwrap_or(0)
    );
    assert_eq!(
        percentile(&per_key, 50.0),
        1,
        "p50 pages per key is not 1 on a fixture that is {ROUTED_KEYS} single-page keys beside \
         {CONTAINER_KEYS} containers, so the caveat printed above would be wrong"
    );
    assert_eq!(
        per_key.last().copied().unwrap_or(0),
        MEMBERS_PER_KEY,
        "MAX pages per key is not {MEMBERS_PER_KEY}: the container arm is what makes the \
         collapse visible and it is not in this sample"
    );
}

// =================================================================================================
// 2. THE TOMBSTONE: WHERE A SHARED ID MOST EASILY BECOMES DATA LOSS.
// =================================================================================================

/// DELETING ONE ELEMENT DOES NOT TOMBSTONE THE OBJECT; DELETING ITS LAST ELEMENT DOES.
///
/// `mark_bucket_index_block_deleted_with` removes ONE element -- its `retain` matches on
/// `(model_id, object_key, component)` -- and then files the ids of what it removed into
/// `bucket.deleted_object_index`. That was exactly right while the id named a page: every page
/// sharing a removed page's id had the same component and was removed with it, so no survivor
/// could carry it.
///
/// It is WRONG the moment one id covers every element of a key, and the damage is not theoretical.
/// `object_manager::runtime_report` asks `deleted_object_index.contains(page.object_id())` once per
/// page, so dropping one field of a twenty-five-field hash would count all twenty-four survivors
/// as `deleted_block_ref_count` instead of hot or cold. The fix is the same last-element condition
/// `remove_block_entry_from_buckets` already applies to `object_index`.
///
/// THE FIXTURE HOLDS MANY ELEMENTS DELIBERATELY. #2016's first mutation run had its defect SURVIVE
/// because a one-key fixture could not tell the bug from the fix, and a one-ELEMENT fixture cannot
/// tell this one either: with a single element the first delete IS the last, and the guard is
/// unobservable. The two arms below are the same key at two populations for exactly that reason,
/// and the survivors are read back THROUGH THE PRODUCT to show they are not merely untombstoned
/// but still there.
///
/// rust-internal: drives HashSet, HashDelete and HashGet, no external surface
#[test]
fn deleting_one_element_tombstones_the_object_only_at_its_last_element() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine);

    const KEY: &str = "tomb";
    const FIELDS: usize = 25;
    assert!(
        FIELDS > 1,
        "a one-element fixture cannot tell this guard from its absence"
    );
    for f in 0..FIELDS {
        write(
            &engine,
            Command::HashSet {
                key: KEY.to_string(),
                field: format!("f{f}"),
                value: format!("v{f}").into_bytes(),
            },
        );
    }
    let object_id = stable_block_object_id(1, "hash", KEY);
    assert_eq!(
        live_components(&engine, "hash", KEY).len(),
        FIELDS,
        "DENOMINATOR: the seed did not file {FIELDS} pages, so nothing below is being observed"
    );
    assert!(
        !tombstone_holds(&engine, object_id),
        "the object is tombstoned before anything was deleted"
    );

    println!("\n=== one id over {FIELDS} elements, deleted one at a time ===");
    println!("  deleted  live pages  tombstoned");

    // --- ARM 1: ONE ELEMENT OF MANY. ---
    write(
        &engine,
        Command::HashDelete {
            key: KEY.to_string(),
            field: "f0".to_string(),
        },
    );
    let after_one = live_components(&engine, "hash", KEY);
    let tombstoned_after_one = tombstone_holds(&engine, object_id);
    println!(
        "  {:>7}  {:>10}  {:>10}",
        1,
        after_one.len(),
        tombstoned_after_one
    );
    assert_eq!(
        after_one.len(),
        FIELDS - 1,
        "deleting one field left {} pages, not {}",
        after_one.len(),
        FIELDS - 1
    );
    assert!(
        !after_one.iter().any(|component| component == "f0"),
        "the deletion did not find its own row: f0 is still filed. \
         `mark_bucket_index_block_deleted_with` matches on the COMPONENT, which this change does \
         not touch, and this is the arm that says so"
    );
    assert!(
        !tombstoned_after_one,
        "the object was tombstoned after ONE of {FIELDS} elements was deleted, while {} elements \
         are still live. `object_manager::runtime_report` would count every one of them deleted",
        after_one.len()
    );
    // AND THE SURVIVORS STILL READ, through the product rather than off the index.
    for f in 1..FIELDS {
        match read(
            &engine,
            Command::HashGet {
                key: KEY.to_string(),
                field: format!("f{f}"),
            },
        ) {
            crate::types::CommandResponse::Bytes { value: Some(bytes) } => assert_eq!(
                bytes,
                format!("v{f}").into_bytes(),
                "f{f} served the wrong bytes after a sibling was deleted"
            ),
            other => panic!("f{f} did not read back after a sibling was deleted: {other:?}"),
        }
    }

    // --- ARM 2: THE LAST ELEMENT. ---
    for f in 1..FIELDS {
        write(
            &engine,
            Command::HashDelete {
                key: KEY.to_string(),
                field: format!("f{f}"),
            },
        );
    }
    let after_all = live_components(&engine, "hash", KEY);
    let tombstoned_after_all = tombstone_holds(&engine, object_id);
    println!(
        "  {:>7}  {:>10}  {:>10}",
        FIELDS,
        after_all.len(),
        tombstoned_after_all
    );
    assert!(
        after_all.is_empty(),
        "{} pages survived deleting every field",
        after_all.len()
    );
    assert!(
        tombstoned_after_all,
        "the object was NOT tombstoned after its LAST element went. The guard withholds the \
         tombstone until no page carries the id; it does not remove it altogether, and \
         withholding it forever would lose the deletion"
    );
    println!(
        "  VERDICT: tombstone withheld at 1 of {FIELDS} deleted, filed at {FIELDS} of {FIELDS}."
    );
}

// =================================================================================================
// 3. WHAT THIS DOES NOT TOUCH, ASSERTED RATHER THAN ARGUED.
// =================================================================================================

/// HLEN IS UNMOVED, AND SO IS THE LISTING IT MUST AGREE WITH.
///
/// `HashLen` is `bucket_index_component_block_addresses(...).len()`, which filters on `model_id`
/// and `object_key` and never consults the object id; and `upsert_bucket_index_block_inner`'s
/// replacement `retain` matches `(object_key, model_id, component)`, so collapsing the id cannot
/// merge two entries into one. That is an ARGUMENT, and #2008 (7 before, 7 after) and #2016
/// (3->3, 2->2) both chose to assert it anyway. So does this.
///
/// The listing is read beside the length because a length that agrees with itself is not evidence
/// -- #2014 landed exactly that guard after the two agreed by coincidence.
///
/// rust-internal: drives HashSet and HashLen, no external surface
#[test]
fn hash_len_and_its_listing_are_unmoved_by_a_component_free_id() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine);

    const KEY: &str = "hlen";
    const FIELDS: i64 = 7;
    for f in 0..FIELDS {
        write(
            &engine,
            Command::HashSet {
                key: KEY.to_string(),
                field: format!("f{f}"),
                value: format!("v{f}").into_bytes(),
            },
        );
    }
    // A REWRITE MUST NOT ADD AN ENTRY. This is the arm the id could plausibly have moved: two
    // writes of one field are two pages sharing an id, and had the replacement `retain` matched on
    // the id rather than the component, it would now also match every OTHER field of the key.
    write(
        &engine,
        Command::HashSet {
            key: KEY.to_string(),
            field: "f0".to_string(),
            value: b"rewritten".to_vec(),
        },
    );

    let answered = match read(
        &engine,
        Command::HashLen {
            key: KEY.to_string(),
        },
    ) {
        crate::types::CommandResponse::Integer { value } => value,
        other => panic!("HashLen answered {other:?}"),
    };
    let listed = live_components(&engine, "hash", KEY);
    println!(
        "\n=== HLEN === answered {answered}, pages {}, fields written {FIELDS}",
        listed.len()
    );
    assert_eq!(
        answered, FIELDS,
        "HLEN answered {answered} for {FIELDS} distinct fields. The identity change must not move \
         the entry COUNT, which is what this reader returns"
    );
    assert_eq!(
        listed.len() as i64,
        answered,
        "HLEN answered {answered} and the index lists {} pages. A length that does not equal its \
         own listing is the shape #2014 caught",
        listed.len()
    );
    let distinct: BTreeSet<&String> = listed.iter().collect();
    assert_eq!(
        distinct.len(),
        listed.len(),
        "the listing repeats a component, so a rewrite accumulated a second entry for one field"
    );
}

/// THE READ PATH SELECTS ON THE COMPONENT AT EVERY ARM, INCLUDING THE FIRST-MATCH FALLBACK.
///
/// #1964 enumerated five readers that take an ascending walk as given, and the fifth is
/// `bucket_index_block_address`'s first-match fallback -- `.next()` over a filtered walk. A
/// first-match selector keyed on the object id would, after this change, hand back the FIRST page
/// of the object for EVERY element of it: precisely the failure #2011 measured inside a WAL record
/// and #2013 closed there.
///
/// It does not, and this is the arm that says so from OUTSIDE the function. Every element of every
/// key is read back through the product and must answer its own bytes. The values are distinct per
/// `(key, field)`, so serving one page for another is detectable rather than a coincidence of
/// equal bytes. An unload/load cycle runs first, so the answers come through the decode path
/// rather than from a warm cache or a resident map.
///
/// THREE OUTCOMES, NOT TWO. A page that reads MISSING is a read fault; a page that reads ANOTHER
/// PAGE'S BYTES is silent corruption. Folding them into one counter would let this pass at either.
///
/// rust-internal: drives a reload and HashGet, no external surface
#[test]
fn every_element_of_one_object_reads_back_its_own_bytes_after_a_reload() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine);
    seed_hash_containers(&engine);

    engine.unload_shard(1);
    load_on(&engine);

    let mut checked = 0usize;
    let mut wrong = 0usize;
    let mut missing = 0usize;
    for k in 0..CONTAINER_KEYS {
        for f in 0..MEMBERS_PER_KEY {
            checked += 1;
            match read(
                &engine,
                Command::HashGet {
                    key: format!("h{k}"),
                    field: format!("f{f}"),
                },
            ) {
                crate::types::CommandResponse::Bytes { value: Some(bytes) } => {
                    if bytes != format!("h{k}-f{f}-value").into_bytes() {
                        wrong += 1;
                    }
                }
                _ => missing += 1,
            }
        }
    }
    println!(
        "\n=== every element of every key, after an unload/load ===\n  checked={checked} \
         wrong bytes={wrong} missing={missing}"
    );
    assert_eq!(
        checked,
        CONTAINER_KEYS * MEMBERS_PER_KEY,
        "DENOMINATOR: {checked} reads, not {}",
        CONTAINER_KEYS * MEMBERS_PER_KEY
    );
    assert_eq!(
        (wrong, missing),
        (0, 0),
        "{wrong} elements served another element's bytes and {missing} read as absent, over \
         {checked}. Every element of one key now shares an object id, so a selector keyed on the \
         id alone would serve the first page of each key {} times over",
        MEMBERS_PER_KEY - 1
    );
}
