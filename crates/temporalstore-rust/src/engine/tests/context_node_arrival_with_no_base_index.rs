// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE THIRD DOOR: a load with NO BASE INDEX FILE AT ALL, which reads no stamp.
//!
//! # WHY THIS DOOR IS NOT COVERED BY THE OTHER TWO
//!
//! `persistence::load_index_inner` compares `index_format_version` against the constant on its
//! `Ok` arm, having read the base index file. Its `Err(_)` arm -- the file is absent or unreadable
//! -- takes `ShardState::default()` and **never reaches that comparison**. Then `fold_index_log
//! _deltas` runs with `base_anchor` 0, so it folds EVERY record in the index log rather than a
//! suffix, and each record's `key_states` go through `apply_key_states` ->
//! `apply_key_state_field(&mut shard.context_nodes, ...)`.
//!
//! So a stamp check -- on the base index, where there is one, or on the dump-manifest image,
//! where that is added -- cannot reach this path. Nothing is stamped here, because nothing is
//! read.
//!
//! # WHAT THIS FILE MEASURES, AND WHAT IT DOES NOT CLAIM
//!
//! The paragraph above describes what the FOLD-AWARE loader does with an absent base. It is not a
//! claim that the load driven below takes that arm, and this file must not be read as making one:
//! `load_shard` on the single-barrier default reaches `load_index_base_only`, which passes
//! `fold_deltas: false`. #2152 measures both halves with the fold probe -- the fold, driven
//! directly, DOES deliver the entry, and the shipped path never asks it -- so the door is held
//! shut by the recovery arm rather than by anything about this map.
//!
//! Every assertion here is therefore about an OUTCOME: what the fixture put on disk, which load
//! path was taken (by the presence of a file, not by a counter), and whether the entry is in the
//! map afterwards. None of them rests on the fold having declined the record rather than never
//! having been asked, because from here those two are indistinguishable and #2152 is what tells
//! them apart.
//!
//! # WHY THIS IS DRIVEN AND NOT ARGUED
//!
//! There is a tidy argument that this door cannot carry a `context_nodes` entry: the key-state
//! capture arrived in `49fe2f933` (2026-08-15), two months after the production writer was removed
//! in `0ed2bb7e9` (2026-06-16), so for a record to carry an entry the map must already have been
//! non-empty, which needs an accepted index, which the stamp refuses. That is a CIRCULARITY
//! ARGUMENT ABOUT A POPULATION, and it is inference rather than measurement: it reasons about what
//! could have been written rather than about what the code does with what it is given. The thing
//! it would license -- "guard the stamped doors and the map is removable" -- is load-bearing on a
//! removal, so it is measured here instead.
//!
//! # THE FIXTURE, AND THE THREE CONTROLS THAT MAKE A VERDICT MEAN SOMETHING
//!
//! One fixture, built by [`build_fixture`]:
//!
//!   1. `StringSet` on the key, giving it a live page;
//!   2. a `context_nodes` entry planted under the SAME key, as a clone of that live string entry
//!      -- `ElementEntry` has no public constructor, and an address no slab holds would be
//!      dropped on load for a reason that is not the door;
//!   3. `CommonExpire` on the key. That arms a deadline where there was none, which is one of the
//!      two halves `delta_key_state_change` fires on, so `capture_key_states` runs and writes a
//!      blob for the key -- and the blob carries `context_nodes`, because the map holds it. It is
//!      `CommonExpire` and not `StringSetEx` on purpose: a value-replacing write would relocate
//!      the string's page and leave the planted entry naming a superseded address, which is a
//!      second variable.
//!
//! The controls:
//!
//!   * CONTROL 1, THE FIXTURE REACHED THE LOG. The index-log file on disk contains the literal
//!     `context_nodes`. Without this, a no-arrival verdict is equally well explained by a capture
//!     that never fired, and the two read identically from the far end.
//!   * CONTROL 2, THE PLANT IS DELIVERABLE AND READABLE. A twin directory holding the SAME
//!     fixture, with its base index flushed, is loaded and the entry is there. That is the open
//!     path, and it is what makes a no-arrival through the third door a fact about the door.
//!   * CONTROL 3, THIS REALLY IS THE THIRD DOOR. The base index file is asserted ABSENT at the
//!     moment of the load.
//!
//! CONTROL 3 IS A FILE CHECK AND NOT A COUNTER CHECK, DELIBERATELY. `index_load_path_counts` is
//! process-global, so a second test loading a shard on another thread lands in the same four
//! numbers and nothing can attribute them back. The absence of the file IS the door, it is local
//! to this test, and no other test can perturb it. The counters are printed as a diagnostic and
//! asserted on only in the direction that cannot be faked by someone else's load.

#![allow(clippy::all)]
use super::*;

/// The key the string, the planted entry and the deadline all share.
const KEY: &str = "ctx:node:legacy";

/// Long enough that nothing below can pass by the key having actually expired.
const HOUR_MS: u64 = 3_600_000;

fn engine_at(dir: &std::path::Path) -> TemporalEngine {
    TemporalEngine::with_local_dirs(
        1024,
        dir.join("cache"),
        dir.join("pages"),
        dir.join("indexes"),
    )
}

fn base_index_path(dir: &std::path::Path) -> std::path::PathBuf {
    dir.join("indexes").join("shard-7.index.json")
}

/// Every index-log file for shard 7, whatever suffix and layout this build writes.
///
/// A RECURSIVE WALK OF THE LOG ROOT AND NOT A NAME PATTERN IN THE INDEX DIRECTORY. The first
/// version of this looked for `shard-7.indexlog.*` directly under the index directory and found
/// nothing, because `with_local_dirs` puts the log store at `index_dir/indexlogs` -- and the
/// suffix has moved once already (`jsonl` -> `bin`), with sealed segments carrying a span in the
/// name. So this takes every file under the log root and lets the content check below decide,
/// which cannot go stale on either the directory or the naming.
fn index_log_files(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    fn walk(at: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
        let Ok(entries) = std::fs::read_dir(at) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, out);
            } else {
                out.push(path);
            }
        }
    }
    let mut out = Vec::new();
    walk(&dir.join("indexes").join("indexlogs"), &mut out);
    out.sort();
    out
}

/// Build the fixture in `dir`. `flush_base` decides whether this becomes the twin with a base
/// index (CONTROL 2) or the subject with none (the third door).
fn build_fixture(dir: &std::path::Path, flush_base: bool) {
    let engine = engine_at(dir);
    engine.load_shard(7);

    let wrote = engine.execute(ExecuteRequest {
        shard_id: 7,
        command: Command::StringSet {
            key: KEY.to_string(),
            value: b"v".to_vec(),
        },
    });
    assert!(wrote.status.ok, "the setup write failed: {}", wrote.status.message);

    // THE PLANT. A clone of the live string entry, under the same key.
    {
        let mut shards = engine.shards.write().expect("engine lock poisoned");
        let shard = shards.get_mut(&7).expect("shard 7 is loaded");
        let live = shard
            .strings()
            .get(KEY)
            .expect("the written string is resident")
            .clone();
        shard.context_nodes_mut().insert(KEY.into(), live.address());
    }

    // ARM A DEADLINE, so the capture fires with the planted entry in place.
    let expired = engine.execute(ExecuteRequest {
        shard_id: 7,
        command: Command::CommonExpire {
            key: KEY.to_string(),
            ttl_ms: HOUR_MS,
        },
    });
    assert!(
        expired.status.ok,
        "arming the deadline failed: {}",
        expired.status.message
    );

    if flush_base {
        engine.flush_shard_index(7);
    }
    drop(engine);
}

/// Is the planted entry in shard 7's `context_nodes` map, as this engine now holds it?
fn planted_arrived(engine: &TemporalEngine) -> bool {
    let shards = engine.shards.read().expect("engine lock poisoned");
    shards
        .get(&7)
        .map_or(false, |shard| shard.context_nodes().contains_key(KEY))
}

#[test]
fn no_context_node_entry_arrives_through_a_load_with_no_base_index() {
    // ---- CONTROL 2: the same fixture, with a base index, delivers the entry ----------------
    let twin = tempfile::tempdir().expect("tempdir");
    build_fixture(twin.path(), true);
    assert!(
        base_index_path(twin.path()).exists(),
        "denominator: the twin was asked to flush a base index and has none"
    );
    let twin_engine = engine_at(twin.path());
    twin_engine.load_shard(7);
    assert!(
        planted_arrived(&twin_engine),
        "CONTROL 2 FAILED: the plant is not deliverable even through an accepted base index, so \
         a no-arrival below would be a fact about the fixture and not about the door"
    );
    drop(twin_engine);

    // ---- the subject: the same fixture, and then the base index file is removed ------------
    let dir = tempfile::tempdir().expect("tempdir");
    build_fixture(dir.path(), false);

    // CONTROL 1: the capture reached the log.
    let logs = index_log_files(dir.path());
    assert!(
        !logs.is_empty(),
        "denominator: no file at all under the index-log root, so there is no third door to \
         drive"
    );
    let carried = logs.iter().any(|path| {
        std::fs::read(path).map_or(false, |bytes| {
            bytes
                .windows(b"context_nodes".len())
                .any(|window| window == b"context_nodes")
        })
    });
    assert!(
        carried,
        "CONTROL 1 FAILED: no index-log file contains the literal `context_nodes`, so the \
         capture never wrote the blob and a no-arrival below would say nothing about the fold. \
         Files checked: {logs:?}"
    );

    // CONTROL 3: there is no base index to read a stamp from. Belt and braces -- the fixture was
    // told not to flush one, and any that appeared anyway is removed, so the load below cannot
    // take the stamped arm by accident.
    let base = base_index_path(dir.path());
    let _ = std::fs::remove_file(&base);
    assert!(
        !base.exists(),
        "CONTROL 3 FAILED: a base index file is still present at {base:?}, so the load below is \
         not the third door"
    );

    crate::engine::persistence::reset_index_load_path_counts();
    let reloaded = engine_at(dir.path());
    reloaded.load_shard(7);
    // Diagnostic, and the one assertion that another thread's load cannot fake: a process-global
    // counter can only be pushed UP by someone else, so "absent was counted at least once" is
    // safe to assert while "accepted was counted zero times" is not.
    let (accepted_n, stale_n, absent_n, decode_n) =
        crate::engine::persistence::index_load_path_counts();
    println!(
        "  no base index -> absent {absent_n}, stale {stale_n}, decode {decode_n}, accepted {accepted_n}"
    );
    assert!(
        absent_n > 0,
        "a load with no base index file must count as ABSENT; got absent {absent_n} stale \
         {stale_n} decode {decode_n} accepted {accepted_n}"
    );

    // WHICH HALF OF THE BLOB CAME BACK, printed before the verdict.
    //
    // `expires_at_ms` rides in the SAME key-state blob. The deadline arriving while the entry
    // does not therefore LOOKS like the fold having applied the record and dropped
    // `context_nodes` alone -- and it is not that. `CommonExpire` is a logged command, so replay
    // re-executes it; the arm below removes the index log and shows the deadline and the string
    // arriving anyway. #2152 closes the question with the fold probe: on this path the fold is
    // asked about NO record, so it contributed nothing to either number here.
    //
    // Printed rather than asserted, for exactly that reason: nothing about this map can be
    // concluded from it.
    let (deadline_arrived, string_arrived, anchor) = {
        let shards = reloaded.shards.read().expect("engine lock poisoned");
        shards.get(&7).map_or((false, false, None), |shard| {
            (
                shard.expires_at_ms().contains_key(KEY),
                shard.strings().contains_key(KEY),
                shard.applied_wal_sequence,
            )
        })
    };
    println!(
        "  from the same blob: deadline arrived {deadline_arrived}, string arrived          {string_arrived}, anchor {anchor:?}"
    );

    let arrived = planted_arrived(&reloaded);
    println!("  the planted `context_nodes` entry arrived through the third door: {arrived}");
    assert!(
        !arrived,
        "THE ENTRY NOW ARRIVES THROUGH A LOAD WITH NO BASE INDEX. All three controls hold -- the log carries the blob, the twin delivers the entry through an accepted base index, and no base index file existed at this load -- so this is a change in behaviour and not a broken fixture. It does NOT by itself say the fold is what delivered it: the shipped path does not consult the fold, and `the_third_door_stays_shut` is the file that attributes this. Read that one before concluding anything, and do not relax this assertion to match."
    );
}

/// THE SAME FIXTURE WITH THE INDEX LOG ALSO REMOVED, SO THE DEADLINE STOPS BEING AMBIGUOUS.
///
/// The arm above prints that `expires_at_ms` -- which rides in the SAME key-state blob as the
/// entry that did not arrive -- DID arrive. That looks like proof the fold applied the blob and
/// dropped only `context_nodes`, and it is not: `CommonExpire` is a logged command, so WAL replay
/// re-executing it produces the same deadline. Two explanations, one observation.
///
/// This separates them. Same fixture, base index removed AND the whole index-log root removed, so
/// the fold has nothing to fold and replay is the only source left.
///
/// MEASURED: the deadline and the string still arrive. They came from REPLAY, so their arrival
/// in the arm above says nothing about whether the fold applied that blob -- and #2152 goes
/// further with the fold probe, showing the fold is asked about no record at all on this path.
///
/// The verdict above is unchanged either way -- the entry does not arrive. This is about what the
/// arm above is entitled to CLAIM, which is the part a removal decision would rest on, and the
/// answer is: the outcome, and not the mechanism.
#[test]
fn with_no_index_log_either_the_deadline_shows_which_source_delivered_it() {
    let dir = tempfile::tempdir().expect("tempdir");
    build_fixture(dir.path(), false);

    let base = base_index_path(dir.path());
    let _ = std::fs::remove_file(&base);
    assert!(!base.exists(), "the base index must be gone at {base:?}");
    let log_root = dir.path().join("indexes").join("indexlogs");
    let _ = std::fs::remove_dir_all(&log_root);
    assert!(
        index_log_files(dir.path()).is_empty(),
        "denominator: the index-log root must be empty, or this arm is not the control it claims"
    );

    let reloaded = engine_at(dir.path());
    reloaded.load_shard(7);
    let (deadline, string, planted, anchor) = {
        let shards = reloaded.shards.read().expect("engine lock poisoned");
        shards.get(&7).map_or((false, false, false, None), |shard| {
            (
                shard.expires_at_ms().contains_key(KEY),
                shard.strings().contains_key(KEY),
                shard.context_nodes().contains_key(KEY),
                shard.applied_wal_sequence,
            )
        })
    };
    println!(
        "  no base index AND no index log -> deadline {deadline}, string {string}, planted          {planted}, anchor {anchor:?}"
    );

    // The one thing that is a finding either way, and the reason this arm is safe to assert on:
    // with NEITHER the base nor the log, nothing can put the entry back. Replay cannot, because
    // no command writes this map.
    assert!(
        !planted,
        "with no base index and no index log, the entry still arrived -- so something other than          those two is writing this map, and the door census is incomplete"
    );
}
