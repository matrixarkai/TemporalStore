// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE SHARD A SERVED STATE IS SERVED UNDER, AND WHY IT HAD TO BE THE ROUTING KEY ITSELF.
//!
//! A page's object id is `stable_block_object_id(shard, kind, key)`. `BlockIndex` carries the kind
//! and the key beside the address, so of the three terms only the FIRST was out of reach of a
//! `&ShardState` -- and four functions that take a shard and never `&self` were stopped by exactly
//! that:
//!
//! ```text
//!   BlockIndex::object_id                       engine/state.rs
//!   object_manager::runtime_report              engine/object_manager.rs
//!   settle_released_bucket_object_delete        engine/storage_bucket_internals.rs
//!   bucket_generation_fingerprints_by_bucket    engine/storage_reporting.rs
//! ```
//!
//! The third one had already written the finding down in its own body -- "recomputing one needs a
//! shard id this path does not carry" -- and skipped the id it could not rebuild. NONE OF THE FOUR
//! IS CHANGED HERE. This module establishes the precondition and nothing else: the term is now
//! reachable from a served shard, and it is the RIGHT term.
//!
//! # WHY A PRESENT-BUT-WRONG ID WOULD BE WORSE THAN THE ABSENCE
//!
//! `stable_block_object_id` is a hash. Handed a zero, or handed the neighbouring shard's id, it
//! returns a perfectly well-formed `u64` that belongs to a different object, and no caller can tell
//! it from the right one -- the absence at least announces itself. So the field is not asserted to
//! EXIST here; it is asserted to EQUAL the id its owner routes it under, and the two
//! `ShardState::shard_id` answers are `Some(id)` or `None`, never a guess.
//!
//! # TWO INDEPENDENT AUTHORITIES, BECAUSE ONE WOULD BE A TAUTOLOGY
//!
//! `install_shard_state` inserts the state into `self.shards` under a key and stamps it from the
//! same argument, so comparing the stamp to the key is nearly -- but not quite -- circular: it
//! still catches an installer that stamps something it derived rather than what it inserted under.
//! The second authority is not circular at all. Every object id ALREADY STORED on a live page entry
//! was computed by the write path from the shard id the engine was routing that write to, long
//! before this field existed. Re-deriving those ids from the shard's OWN stamp and comparing them
//! is a check the stamp cannot pass by being self-consistent:
//! `the_id_a_served_shard_carries_derives_the_object_id_already_stored_on_its_pages`.
//!
//! Both run over a MULTI-SHARD engine, on deliberately non-contiguous ids, and both print their
//! denominator. Shard 0 is in the fixture on purpose and so are four shards that are not 0: a stamp
//! stuck at zero satisfies the first and fails the other four, which is the mutation this module
//! was written to kill.

#![allow(clippy::all)]
use super::*;
use std::collections::BTreeSet;

/// THE FIXTURE'S SHARDS. Non-contiguous, so a stamp that is really the loop index, the count, or
/// the position in the map is a different number from the id at every shard but the first.
///
/// 0 is present because 0 IS a real shard id -- that is the whole reason the carried id needs a
/// flag beside it rather than a sentinel -- and because a stamp stuck at zero has to be caught by
/// the shards that are not zero rather than by luck.
const SHARD_IDS: [crate::types::ShardId; 5] = [0, 1, 7, 41, 1000];

/// The end bucket a production shard is loaded with: `TS_SHARD_END_ROUTING_SLOT=1023`.
const NARROW_END: u32 = 1023;

/// Records per shard. Enough page entries for the derivation floor below to mean something.
const RECORDS: usize = 24;

fn engine_on(dir: &std::path::Path) -> TemporalEngine {
    TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("pages"),
        dir.join("indexes"),
    )
}

fn load(engine: &TemporalEngine, shard_id: crate::types::ShardId) {
    let response = engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id,
        table_name: "shard-carried-identity".to_string(),
        shard_uri: format!("local://shard-carried-identity/{shard_id}"),
        start_routing_bucket: 0,
        end_routing_bucket: NARROW_END,
        readonly: false,
        load_version: 1,
        local_node_id: Some(1),
    });
    assert!(
        response.status.ok,
        "the fixture could not load shard {shard_id}: {:?}",
        response.status
    );
}

/// Writes that produce page entries of two kinds under distinct object keys on one shard.
fn seed(engine: &TemporalEngine, shard_id: crate::types::ShardId) {
    for index in 0..RECORDS {
        let key = format!("ident-{shard_id}-{index:04}");
        let response = engine.execute(ExecuteRequest {
            shard_id,
            command: Command::StringSet {
                key: key.clone(),
                value: vec![b'v'; 48],
            },
        });
        assert!(
            response.status.ok,
            "string write {index} on shard {shard_id}: {:?}",
            response.status
        );
        let response = engine.execute(ExecuteRequest {
            shard_id,
            command: Command::HashSet {
                key: format!("h-{key}"),
                field: "f".to_string(),
                value: vec![b'h'; 48],
            },
        });
        assert!(
            response.status.ok,
            "hash write {index} on shard {shard_id}: {:?}",
            response.status
        );
    }
}

/// A loaded, seeded engine holding every shard in [`SHARD_IDS`].
fn multi_shard_engine(dir: &std::path::Path) -> TemporalEngine {
    let engine = engine_on(dir);
    for shard_id in SHARD_IDS {
        load(&engine, shard_id);
        seed(&engine, shard_id);
    }
    engine
}

/// THE PREDICATE UNDER TEST, named once so the control below can exercise the SAME code the verdict
/// rests on. A state carries `routed_under` when it answers `Some` and answers that exact id.
fn carries(state: &crate::engine::state::ShardState, routed_under: crate::types::ShardId) -> bool {
    state.shard_id() == Some(routed_under)
}

// =================================================================================================
// 1. THE STAMP EQUALS THE KEY THE SERVED MAP ROUTES BY
// =================================================================================================

/// EVERY SHARD THE ENGINE SERVES CARRIES THE ID ITS OWNER ROUTES IT UNDER.
///
/// The engine's shard map IS the routing authority: `self.shards` is keyed by `ShardId` and every
/// read, write and report reaches a shard by looking it up under that key. So the property is an
/// equality between the key and the stamp, over every entry the map holds, on a fixture holding
/// five shards at once.
///
/// THE PREDICATE'S OWN CONTROL RUNS FIRST. A check that cannot fail is indistinguishable from one
/// that passes, and this one compares two numbers that an installer bug would make equal by
/// accident, so `carries` is driven on a right answer, on a wrong-shard answer and on an unstamped
/// state before it is pointed at the engine.
#[test]
fn every_served_shard_carries_the_id_its_owner_routes_it_under() {
    // --- THE CONTROL, before any verdict. ---
    let mut planted = crate::engine::state::ShardState::default();
    assert!(
        !carries(&planted, 7),
        "`carries` reports an UNSTAMPED state as carrying shard 7, so it cannot distinguish a \
         stamped shard from one that never entered the engine and every verdict below is empty"
    );
    planted.set_shard_id(7);
    assert!(
        carries(&planted, 7),
        "`carries` does not report a state stamped with 7 as carrying 7, so it reports nothing"
    );
    assert!(
        !carries(&planted, 41),
        "`carries` reports a state stamped with 7 as carrying 41 -- it is not comparing the id at \
         all, and a shard holding the WRONG shard's id would pass the loop below"
    );
    assert!(
        !carries(&planted, 0),
        "`carries` reports a state stamped with 7 as carrying 0; a stamp stuck at zero is the \
         first mutation this guard exists to kill and it would survive"
    );

    // --- THE TREE. ---
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = multi_shard_engine(dir.path());
    let shards = engine.shards.read().expect("engine lock poisoned");

    let observed: Vec<crate::types::ShardId> = shards.keys().copied().collect();
    let distinct: BTreeSet<crate::types::ShardId> = observed.iter().copied().collect();

    // --- VACUITY FLOORS, BEFORE THE VERDICT. An empty map satisfies a for-loop. ---
    assert_eq!(
        SHARD_IDS.len(),
        shards.len(),
        "the fixture loaded {} shards and the served map holds {}; the loop below would be \
         checking a different set from the one this module reasons about",
        SHARD_IDS.len(),
        shards.len()
    );
    assert!(
        shards.len() >= 5,
        "the served map holds {} shard(s); a single-shard fixture cannot tell an id from a \
         constant and this whole module would pass on a stamp that ignores its argument",
        shards.len()
    );
    assert_eq!(
        shards.len(),
        distinct.len(),
        "the served map holds {} shards under {} distinct ids, so at least two entries share a \
         key and the equality below is not per-shard",
        shards.len(),
        distinct.len()
    );
    assert!(
        distinct.iter().filter(|id| **id != 0).count() >= 4,
        "only {} served shard(s) have a non-zero id; a stamp stuck at 0 needs at least two \
         non-zero shards to be caught and this fixture would let it through",
        distinct.iter().filter(|id| **id != 0).count()
    );
    assert!(
        distinct.contains(&0),
        "shard 0 is not in the served map; the fixture no longer proves that a REAL zero id is \
         distinguishable from an unstamped state"
    );

    let mut verdict_lines = Vec::new();
    let mut unstamped = 0usize;
    let mut mismatched = Vec::new();
    for (routed_under, state) in shards.iter() {
        let carried = state.shard_id();
        verdict_lines.push(format!(
            "    routed under {routed_under:>5}  carries {carried:?}"
        ));
        if carried.is_none() {
            unstamped += 1;
        }
        if !carries(state, *routed_under) {
            mismatched.push(format!("{routed_under} carries {carried:?}"));
        }
    }
    println!(
        "  {} served shards, {} distinct ids, ids {:?}",
        shards.len(),
        distinct.len(),
        distinct
    );
    for line in &verdict_lines {
        println!("{line}");
    }
    assert_eq!(
        0, unstamped,
        "{unstamped} of {} served shards entered the map without a shard id stamped. There is one \
         function that installs a ShardState (`install_shard_state`) and it stamps; if another way \
         in has appeared, route it through that one.",
        shards.len()
    );
    assert!(
        mismatched.is_empty(),
        "{} of {} served shards carry an id that is NOT the key the served map routes them under: \
         {mismatched:?}. A wrong id here is worse than none: \
         `stable_block_object_id(shard, kind, key)` would return a well-formed id belonging to a \
         different shard's object.",
        mismatched.len(),
        shards.len()
    );
}

// =================================================================================================
// 2. THE STAMP DERIVES IDS THE WRITE PATH ALREADY STORED
// =================================================================================================

/// THE ID A SERVED SHARD CARRIES DERIVES THE OBJECT ID ALREADY STORED ON ITS OWN PAGES.
///
/// The check in part 1 compares the stamp to the key it was inserted under, which an installer that
/// stamps the wrong thing could still satisfy if it derived both from one argument. This one cannot
/// be satisfied that way. Every `object_id` on a live page entry was computed by the WRITE PATH from
/// the shard the engine was routing that write to, before this field existed and without consulting
/// it. Re-deriving them from the shard's own stamp is therefore an equality between the new field
/// and the engine's pre-existing routing behaviour, page by page, on five shards at once.
///
/// It is also the payoff stated as a fact: this is `stable_block_object_id(shard, kind, key)`
/// computed from a `&ShardState` and NOTHING ELSE, which is precisely what the four functions named
/// in this module's header could not do.
#[test]
fn the_id_a_served_shard_carries_derives_the_object_id_already_stored_on_its_pages() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = multi_shard_engine(dir.path());
    let shards = engine.shards.read().expect("engine lock poisoned");

    let mut checked = 0usize;
    let mut without_stored_id = 0usize;
    let mut shards_with_pages = 0usize;
    let mut differed = Vec::new();

    for (routed_under, state) in shards.iter() {
        // THE ONLY INPUT. Not the map key -- the shard's own answer, which is the reachability
        // this module is about.
        let carried = state
            .shard_id()
            .expect("a served shard carries its id; part 1 of this module holds that");
        let mut pages_here = 0usize;
        for bucket in state.bucket_index.bucket_map.values() {
            for (_handle, page) in bucket.block_index.iter() {
                // THE INDEPENDENT SIDE IS NOW THE WRITE PATH'S OWN COPY, and it has to be: the
                // address used to hold one and does not any more. `bucket.object_index` was filled
                // by the write path from the id it ACTUALLY USED, before this field existed and
                // without consulting it -- so comparing a fresh derivation against that membership
                // is still an equality between two answers rather than between one and itself.
                let derived = crate::engine::hashing::stable_block_object_id(
                    carried,
                    page.model_id.as_str(),
                    &page.object_key,
                );
                pages_here += 1;
                checked += 1;
                if !bucket.object_index.contains(&derived) {
                    without_stored_id += 1;
                    differed.push(format!(
                        "shard {routed_under} kind {} key {} derived {derived}, which the bucket's \
                         own object index does not hold",
                        page.model_id.as_str(),
                        page.object_key
                    ));
                }
            }
        }
        if pages_here > 0 {
            shards_with_pages += 1;
        }
    }

    println!(
        "  derived from the shard's own id on {checked} live page entries across \
         {shards_with_pages} shards ({without_stored_id} not held by their bucket's object index), \
         {} differed",
        differed.len()
    );

    // --- DENOMINATORS. A zero-entry walk satisfies the emptiness assertion below. ---
    assert!(
        checked >= 100,
        "the walk compared {checked} page entries; the fixture writes {} records of two kinds on \
         {} shards, so below 100 it is not reading the page index and `differed` is empty for the \
         wrong reason",
        RECORDS,
        SHARD_IDS.len()
    );
    assert_eq!(
        SHARD_IDS.len(),
        shards_with_pages,
        "only {shards_with_pages} of {} shards contributed a page entry, so the derivation was \
         never exercised on the other shards' ids -- which is where a wrong id shows up",
        SHARD_IDS.len()
    );

    // --- THE CONTROL: the comparison DOES separate a wrong shard from the right one. ---
    let (right, wrong) = (SHARD_IDS[2], SHARD_IDS[4]);
    assert_ne!(
        crate::engine::hashing::stable_block_object_id(right, "string", "ident-7-0000"),
        crate::engine::hashing::stable_block_object_id(wrong, "string", "ident-7-0000"),
        "`stable_block_object_id` returns the same id for shard {right} and shard {wrong} on one \
         key, so the equality above cannot detect a wrong shard id and this test is vacuous"
    );

    assert!(
        differed.is_empty(),
        "{} of {checked} live page entries derive an object id from the shard's OWN carried id \
         that differs from the one the write path stored: {:?}. Either the carried id is not the \
         shard the engine routed those writes to, or the derivation is no longer the identity the \
         write path uses.",
        differed.len(),
        differed.iter().take(4).collect::<Vec<_>>()
    );
}

// =================================================================================================
// 3. AN UNSTAMPED STATE SAYS SO, AND A REAL ZERO IS NOT THAT
// =================================================================================================

/// A `ShardState` THAT NEVER ENTERED THE ENGINE CARRIES NO ID, AND SHARD 0 IS NOT "NO ID".
///
/// The reason the field is a `u64` and a flag rather than a bare number: shard 0 is a real shard, so
/// there is no value left over to mean "unstamped". Both halves are driven, because a flag that is
/// never read would let `shard_id()` answer `Some(0)` for a decoded image and hand every caller a
/// plausible wrong id for shard 0's objects.
#[test]
fn a_shard_state_that_never_entered_the_engine_carries_no_shard_id() {
    let fresh = crate::engine::state::ShardState::default();
    assert_eq!(
        None,
        fresh.shard_id(),
        "a default ShardState answers a shard id. Whatever it answered, it is a guess: nothing has \
         told this state which shard it belongs to, and a guess here derives a well-formed object \
         id for the wrong shard."
    );

    let mut stamped_zero = crate::engine::state::ShardState::default();
    stamped_zero.set_shard_id(0);
    assert_eq!(
        Some(0),
        stamped_zero.shard_id(),
        "a state stamped with shard 0 answers {:?} instead of Some(0) -- the flag is being used as \
         the value, so shard 0 cannot be served",
        stamped_zero.shard_id()
    );
    assert_ne!(
        fresh.shard_id(),
        stamped_zero.shard_id(),
        "an unstamped state and one stamped with shard 0 answer the same thing, so 0 is being used \
         as a sentinel and shard 0's pages would derive their ids from an unstamped state"
    );
}

// =================================================================================================
// 4. THE STORED SHAPE DOES NOT MOVE
// =================================================================================================

/// THE CARRIED ID CHANGES NO SERIALIZED BYTE.
///
/// `ShardState` IS the serialized shard index. `index_format_version` at the top of that struct
/// records what a change to the stored shape has already cost once, and this field must not be one:
/// it is the id the state is SERVED under, which is a property of the load and not of the file.
/// Driven by serializing the same state stamped and unstamped and comparing the bytes, rather than
/// asserted off the attribute -- `#[serde(skip)]` is exactly the kind of claim a doc comment keeps
/// making after the attribute has moved.
#[test]
fn the_shard_id_field_changes_no_serialized_byte() {
    let unstamped = crate::engine::state::ShardState::default();
    let mut stamped = crate::engine::state::ShardState::default();
    stamped.set_shard_id(1000);
    assert_eq!(
        Some(1000),
        stamped.shard_id(),
        "the fixture failed to stamp, so the two sides below are the same object and the equality \
         is trivially true"
    );

    let before = serde_json::to_vec(&unstamped).expect("an unstamped shard state serializes");
    let after = serde_json::to_vec(&stamped).expect("a stamped shard state serializes");

    // THE DENOMINATOR. Two empty strings compare equal.
    assert!(
        before.len() > 100,
        "the serialized shard state is {} bytes; below 100 it is not the index and the equality \
         below is comparing nothing",
        before.len()
    );
    assert_eq!(
        before, after,
        "stamping the shard id changed the serialized index -- {} bytes against {}. An index \
         written by one build would then decode differently in another, which is the failure \
         `index_format_version` exists to record.",
        before.len(),
        after.len()
    );

    let value = serde_json::to_value(&stamped).expect("a stamped shard state serializes as JSON");
    let object = value
        .as_object()
        .expect("a shard state serializes as a JSON object");
    assert!(
        object.len() > 8,
        "the serialized shard state carried only {} field(s), which is too few for the absence \
         assertion below to mean anything",
        object.len()
    );
    for absent in ["shard_id", "shard_id_known"] {
        assert!(
            !object.contains_key(absent),
            "`{absent}` is written into the shard index. The stored shape has moved, and the id \
             the state is served under is not a property of the file."
        );
    }
    println!(
        "  {} serialized fields, identical at {} bytes stamped and unstamped, no shard_id key",
        object.len(),
        before.len()
    );
}

// =================================================================================================
// 5. THE GUARD: THE ONE WAY IN STAMPS THE ID IT INSERTS UNDER
// =================================================================================================

/// `install_shard_state` STAMPS THE ID, AND NO OTHER PRODUCTION SITE STAMPS ONE.
///
/// `every_shard_the_engine_installs_carries_its_routing_range` already holds, as an equality-compared
/// list with its own matcher control, that `install_shard_state` is the ONLY production site that
/// puts a `ShardState` into the served map. That list is what makes "every served shard is stamped"
/// a property of the code; this adds the other half, read off the source: that the one site stamps,
/// and that the setter is not being called anywhere else in production where it could stamp
/// something other than the key the state is inserted under.
///
/// rust-internal: reads this crate's own source, no product behaviour
#[test]
fn the_only_production_site_that_installs_a_shard_stamps_the_id_it_inserts_under() {
    use std::path::Path;

    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let installer = std::fs::read_to_string(root.join("engine.rs")).expect("engine.rs");
    assert!(
        installer.len() > 100_000,
        "engine.rs read back as {} bytes; below 100,000 this is not the file and the containment \
         checks below pass or fail for the wrong reason",
        installer.len()
    );
    assert!(
        installer.contains("state.set_shard_id(shard_id);"),
        "`install_shard_state` no longer stamps the id it inserts under. Every served shard would \
         answer None, and the four functions named in this module's header go back to being unable \
         to derive an object id."
    );
    // It stamps the ARGUMENT it inserts under, not something re-derived: the insert is keyed on the
    // same name.
    assert!(
        installer.contains(".insert(shard_id, state);"),
        "`install_shard_state` no longer inserts under `shard_id`, so stamping `shard_id` is no \
         longer the same number the served map routes by"
    );

    // No other production file stamps an id. A second setter call site could stamp a shard with
    // something other than the key it is served under, which is the one failure this field must
    // not have.
    let mut pending = vec![root.clone()];
    let mut files_scanned = 0usize;
    let mut excluded = 0usize;
    let mut setter_sites: Vec<String> = Vec::new();
    while let Some(path) = pending.pop() {
        let Ok(entries) = std::fs::read_dir(&path) else {
            continue;
        };
        for entry in entries.flatten() {
            let entry_path = entry.path();
            if entry_path.is_dir() {
                pending.push(entry_path);
                continue;
            }
            if entry_path.extension().and_then(|e| e.to_str()) != Some("rs") {
                continue;
            }
            let display = entry_path.display().to_string();
            if display.contains("/tests/") || display.ends_with("tests.rs") {
                excluded += 1;
                continue;
            }
            let Ok(text) = std::fs::read_to_string(&entry_path) else {
                continue;
            };
            files_scanned += 1;
            let relative = display
                .rsplit_once("/src/")
                .map(|(_, tail)| tail.to_string())
                .unwrap_or(display.clone());
            for line in text.lines() {
                // The definition itself is not a call site.
                if line.contains("fn set_shard_id") {
                    continue;
                }
                if line.contains("set_shard_id(") {
                    setter_sites.push(relative.clone());
                }
            }
        }
    }

    // VACUITY FLOORS on the scan itself.
    assert!(
        files_scanned > 80,
        "the scan read {files_scanned} production .rs files under {}; below 80 it has stopped \
         reading the crate and the list below is empty for the wrong reason",
        root.display()
    );
    assert!(
        excluded > 10,
        "the scan excluded {excluded} test files; this crate has more than ten"
    );
    setter_sites.sort();
    setter_sites.dedup();
    println!(
        "  {files_scanned} production files scanned, {excluded} test files excluded, setter called \
         in {setter_sites:?}"
    );
    assert_eq!(
        vec!["engine.rs".to_string()],
        setter_sites,
        "the set of production files that stamp a shard's id has moved. The stamp must happen where \
         the state is inserted into the served map, so that the id and the routing key are the same \
         number by construction; a second site can stamp a different one."
    );
}
