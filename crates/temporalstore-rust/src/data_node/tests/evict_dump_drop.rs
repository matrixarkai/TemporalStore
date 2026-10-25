// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT MEMORY RECLAMATION UNDER PRESSURE IS ALLOWED TO DO TO A LIVE RECORD.
//!
//! The rule this module exists to hold: RELIEVING MEMORY PRESSURE NEVER DESTROYS THE ONLY
//! REACHABLE COPY OF LIVE DATA. A read after a reclaiming round loads the value back.
//!
//! WHY THAT IS A RULE AND NOT A PREFERENCE, in this store's own terms. A record's payload lives in
//! the block store and is reached through an ADDRESS held in the shard's model maps
//! (`engine.rs`, the `FastPathRead::String` arm: `shard.string_address(key)` and then
//! `read_block_bytes`). So the resident cost of a record splits in two, and the two halves are not
//! alike:
//!
//!   * THE DERIVED HALF -- a bucket's `block_index` and its object-block lookup entries. This is
//!     droppable, because `reload_released_bucket` rebuilds it FROM the model maps. Dropping it is
//!     reclamation: `release_bucket_blocks` takes it and the next read puts it back.
//!   * THE AUTHORITATIVE HALF -- the model-map entry itself, which holds the address. Nothing in
//!     the engine can rebuild one on demand. There is no per-bucket durable address map to fault
//!     in: the persisted index is written whole, from a clone of the entire shard
//!     (`serialize_index(&snapshot)`), so there is nothing a single read could load.
//!
//! A round that takes the derived half is dump-then-drop. A round that takes the authoritative half
//! is a DELETE, whatever it is called, because the payload bytes it leaves behind in the block store
//! have nothing pointing at them any more.
//!
//! The two tests below are that distinction, measured on matched fixtures, and the third is the
//! guard that keeps the memory-pressure stage on the droppable half.

#![allow(clippy::all)]
use super::*;

/// The shipped routing range, so a released bucket is one that keys SHARE rather than a per-key
/// node that only the bare `load_shard` default produces.
const SHIPPED_END_BUCKET: u32 = 1023;

const VALUE_LEN: usize = 96;

/// A value that is a function of its key, so a read resolved through the WRONG block fails here
/// instead of passing because the length happened to match.
fn keyed_value(index: usize) -> Vec<u8> {
    let mut value = format!("{index:06}:").into_bytes();
    value.resize(VALUE_LEN, b'v');
    value[VALUE_LEN - 1] = (index % 251) as u8;
    value
}

fn key_of(index: usize) -> String {
    format!("dumpdrop-{index:06}")
}

/// A key this fixture never writes. THE NEGATIVE CONTROL: without it, "every key read back" is also
/// what a store that answers every lookup with a value would report.
fn never_written_key(index: usize) -> String {
    format!("dumpdrop-never-written-{index:06}")
}

fn engine_at(dir: &std::path::Path) -> TemporalEngine {
    TemporalEngine::with_local_dirs(
        16 * 1024 * 1024,
        dir.join("cache"),
        dir.join("pages"),
        dir.join("indexes"),
    )
}

fn load_shard_on_shipped_range(engine: &TemporalEngine, load_version: u64) {
    let response = engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 1,
        table_name: "dump-drop".to_string(),
        shard_uri: "local://dump-drop/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket: SHIPPED_END_BUCKET,
        readonly: false,
        load_version,
        local_node_id: Some(1),
    });
    assert!(
        response.status.ok,
        "the fixture could not load shard 1 on 0..{SHIPPED_END_BUCKET}: {:?}",
        response.status
    );
}

/// A shard on the shipped range holding `keys` ACKNOWLEDGED string records -- every write's status
/// is asserted, so "acknowledged" below is a fact about this fixture and not a hope.
fn seeded_engine(dir: &std::path::Path, keys: usize) -> TemporalEngine {
    let engine = engine_at(dir);
    load_shard_on_shipped_range(&engine, 1);
    for index in 0..keys {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: key_of(index),
                value: keyed_value(index),
            },
        });
        assert!(response.status.ok, "write {index}: {:?}", response.status);
    }
    engine
}

/// What one key answered. `Whole` is byte-identical; `Missing` is the store saying the key is not
/// there; `Wrong` is bytes that are not this key's value.
#[derive(Debug, Default)]
struct ReadBack {
    whole: usize,
    missing: Vec<String>,
    wrong: Vec<String>,
}

impl ReadBack {
    fn verified(&self) -> usize {
        self.whole
    }
}

/// Read `keys` keys back and sort the answers into the three outcomes. BOTH DIRECTIONS are recorded
/// -- the count that answered whole AND the lists that did not -- because an empty mismatch list is
/// also what a loop that never ran produces.
fn read_back_all(engine: &TemporalEngine, keys: usize) -> ReadBack {
    let mut outcome = ReadBack::default();
    for index in 0..keys {
        let key = key_of(index);
        let answer = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet { key: key.clone() },
        });
        if !answer.status.ok {
            outcome.wrong.push(format!("{key}: read failed {:?}", answer.status));
            continue;
        }
        match answer.response {
            CommandResponse::Bytes { value: Some(value) } if value == keyed_value(index) => {
                outcome.whole += 1;
            }
            CommandResponse::Bytes { value: Some(value) } => outcome.wrong.push(format!(
                "{key}: {} bytes that are not this key's value",
                value.len()
            )),
            CommandResponse::Bytes { value: None } => outcome.missing.push(key),
            other => outcome
                .wrong
                .push(format!("{key}: a string read answered {other:?}")),
        }
    }
    outcome
}

/// THE NEGATIVE CONTROL, run on the same build at the same moment as the hits it qualifies: keys
/// this fixture never wrote must read ABSENT. Returns how many of them answered with a value.
fn never_written_keys_that_answered(engine: &TemporalEngine, probes: usize) -> usize {
    let mut answered = 0usize;
    for index in 0..probes {
        let answer = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: never_written_key(index),
            },
        });
        if let CommandResponse::Bytes { value: Some(_) } = answer.response {
            answered += 1;
        }
    }
    answered
}

const CONTROL_PROBES: usize = 16;

/// THE FINDING: THE DROP ARM DELETES ACKNOWLEDGED RECORDS, AND THE DELETION SURVIVES A RELOAD.
///
/// This is a characterization, not a proposal. `apply_storage_eviction`'s `delete_drop` arm calls
/// `delete_record` on every live object in its victim buckets -- dirty or clean, dumped or not --
/// which removes the key from every model map, from the bucket index, and from the expiry and
/// seen maps (`engine.rs`, `delete_record_exact`). It then writes a `CommonDelete` tombstone per
/// dropped key and anchors `applied_wal_sequence` past them, so replay observes the deletion
/// rather than the earlier write. BOTH of those are durability work: the arm does not merely drop a
/// resident copy, it makes the removal stick.
///
/// SIX THINGS ASSERTED, IN THIS ORDER, because the later ones say nothing without the earlier:
///
///   1  THE CORPUS READ BACK BEFORE THE ROUND, cold. Keys missing afterwards are then the round's
///      doing and not the fixture's.
///   2  THE ARM FIRED AND THE FIXTURE REACHED THE REGIME: the gate opened, victims were taken, the
///      mode is the drop arm's own, and `dropped_object_count` is positive. A round that declined
///      proves nothing about what the arm does.
///   3  ACKNOWLEDGED KEYS ARE NOW MISSING, and exactly as many as the arm says it dropped. That
///      equality is the accounting: the losses are the arm's deletions and not a read path that
///      stopped answering.
///   4  THE NEGATIVE CONTROL, same build, same moment: keys never written read absent. Without it
///      "missing" is not a reading.
///   5  KEYS OUTSIDE THE VICTIM BUCKETS STILL READ BACK WHOLE, so this is a selective deletion and
///      not a store that has stopped working.
///   6  AND THE MISSING KEYS ARE STILL MISSING AFTER A RELOAD on a fresh engine over the same
///      directories. That is what separates this finding from "a resident copy was dropped": a
///      dropped copy comes back, a deleted record does not.
#[test]
fn the_drop_arm_deletes_acknowledged_records_and_the_deletion_survives_a_reload() {
    const KEYS: usize = 600;
    const BATCH: usize = 16;

    let dir = tempdir().unwrap();
    let (dropped, missing_before_reload, resident_before, resident_after) = {
        let engine = seeded_engine(dir.path(), KEYS);

        // --- 1. THE CORPUS READS BACK BEFORE THE ROUND, COLD. ---
        let _ = engine.cache().invalidate_shard(1);
        let before = read_back_all(&engine, KEYS);
        assert!(
            before.missing.is_empty() && before.wrong.is_empty(),
            "the fixture lost {} and corrupted {} keys before any eviction round ran, so nothing \
             below could be attributed to the round",
            before.missing.len(),
            before.wrong.len()
        );
        assert_eq!(
            before.verified(),
            KEYS,
            "only {} of {KEYS} keys were verified before the round, so the empty lists above are a \
             loop that did not run",
            before.verified()
        );

        let resident_before = engine.bucket_index_resident_bytes(1);
        assert!(
            resident_before > 0,
            "the fixture left no resident index, so there is no memory for a round to reclaim"
        );

        // threshold 0 so the gate admits, dump_before_evict ON so the victims are dumped first --
        // which is the most favourable configuration this arm has, and it still deletes.
        let report = engine.apply_storage_eviction(1, 0, BATCH, true, true);

        // --- 2. THE ARM FIRED, AND THE FIXTURE REACHED THE REGIME. ---
        assert_eq!(
            report.mode, "delete_drop",
            "the round took mode {:?}, so it is not the arm under characterization",
            report.mode
        );
        assert!(
            report.pressure_gate_open,
            "the gate never opened, so this round declined: {report:?}"
        );
        assert!(
            !report.selected_victims.is_empty(),
            "the round took no victims, so it had nothing to act on: {report:?}"
        );
        assert!(
            report.dropped_object_count > 0,
            "the drop arm deleted nothing, so this fixture never entered the regime it is \
             arguing about: {report:?}"
        );

        // --- 3. ACKNOWLEDGED KEYS ARE NOW MISSING, AND THE COUNT ACCOUNTS FOR ITSELF. ---
        let _ = engine.cache().invalidate_shard(1);
        let after = read_back_all(&engine, KEYS);
        assert!(
            after.wrong.is_empty(),
            "{} keys answered with bytes that do not belong to the key read, which is a different \
             failure from deletion: first few {:?}",
            after.wrong.len(),
            &after.wrong[..after.wrong.len().min(5)]
        );
        assert!(
            !after.missing.is_empty(),
            "the arm reported {} records dropped and every key still read back, so this arm does \
             NOT destroy data and the rest of this test is wrong about it",
            report.dropped_object_count
        );
        assert_eq!(
            after.missing.len(),
            report.dropped_object_count,
            "the arm reported {} records dropped and {} acknowledged keys went missing; the two \
             must be the same set of objects",
            report.dropped_object_count,
            after.missing.len()
        );

        // --- 4. THE NEGATIVE CONTROL. ---
        let answered = never_written_keys_that_answered(&engine, CONTROL_PROBES);
        assert_eq!(
            answered, 0,
            "{answered} of {CONTROL_PROBES} keys that were never written answered with a value, so \
             this store's \"absent\" is not a reading and the missing count above means nothing"
        );

        // --- 5. KEYS OUTSIDE THE VICTIM BUCKETS SURVIVED. ---
        assert!(
            after.verified() > 0,
            "every one of {KEYS} keys is gone, so the round did not select victims -- it emptied \
             the shard, and this fixture cannot show a selective deletion"
        );

        let resident_after = engine.bucket_index_resident_bytes(1);
        // The deletion is written out here so the reload below comes back from the state the round
        // left rather than from an index taken before it.
        engine.flush_shard_index(1);
        (
            report.dropped_object_count,
            after.missing.len(),
            resident_before,
            resident_after,
        )
    };

    // --- 6. AND THE DELETION SURVIVED A RELOAD. ---
    //
    // A fresh engine over the same directories, and only after the first is dropped: two handles on
    // one directory is its own failure mode and would make this prove something else.
    let reloaded = engine_at(dir.path());
    load_shard_on_shipped_range(&reloaded, 2);
    let reloaded_resident = reloaded.bucket_index_resident_bytes(1);
    assert!(
        reloaded_resident > 0,
        "the reloaded shard rebuilt no resident index, so its reads are not going through the \
         structure the round changed"
    );
    let after_reload = read_back_all(&reloaded, KEYS);
    assert!(
        !after_reload.missing.is_empty(),
        "{missing_before_reload} keys were missing before the reload and none is missing after, so \
         the arm dropped a resident copy that the reload restored rather than deleting the record"
    );
    assert!(
        after_reload.missing.len() >= missing_before_reload,
        "{missing_before_reload} keys were deleted and only {} are still missing after a reload, \
         so some of the deletions did not survive",
        after_reload.missing.len()
    );
    assert_eq!(
        never_written_keys_that_answered(&reloaded, CONTROL_PROBES),
        0,
        "the reloaded store answered a key that was never written"
    );

    eprintln!(
        "  [drop-arm-characterization] {KEYS} acknowledged keys, resident index \
{resident_before} -> {resident_after} B: the drop arm reported {dropped} records dropped and \
{missing_before_reload} acknowledged keys went MISSING cold; {} still missing after a reload into \
a {reloaded_resident} B index; {} of {KEYS} survived. {CONTROL_PROBES} never-written keys read \
absent in both halves.",
        after_reload.missing.len(),
        after_reload.verified(),
    );
}

/// A runtime over a shard seeded as above, so the STAGE can be run rather than the engine call.
fn seeded_runtime(dir: &std::path::Path, keys: usize) -> DataNodeRuntime {
    let engine = seeded_engine(dir, keys);
    DataNodeRuntime::new_without_workers_with_options(
        engine,
        DataNodeRuntimeOptions {
            worker_threads: 0,
            max_queue_depth: 64,
            max_background_queue_depth: 8,
        },
    )
}

/// THE WIDE ARM, FORCED -- the arm the retired field used to route into a deletion.
///
/// Threshold 1 so this fixture's own pressure clears it, and the shipped 64 MiB bound pinned to 0
/// so it is not the higher bar: the periodic loop takes the HIGHER of the two, and at the shipped
/// bound a fixture of this size declines with `index_resident_below_bound` and would exercise the
/// NARROW arm, which never had a route to the deleting one in the first place.
fn wide_arm_options() -> StorageManagerOptions {
    StorageManagerOptions {
        enable_evict: true,
        eviction_dump_before_evict: true,
        eviction_memory_pressure_threshold: 1,
        index_resident_limit_bytes: 0,
        ..StorageManagerOptions::default()
    }
}

/// THE MEMORY-PRESSURE STAGE CANNOT ROUTE INTO THE ARM THAT DELETES, AND THE ROUND IT TAKES
/// INSTEAD RECLAIMS MEMORY WITHOUT LOSING A KEY.
///
/// This is the gate on the change, and it runs the STAGE -- `run_storage_manager_once` -- not the
/// engine call, because the thing being asserted is about which arm the stage selects.
///
/// It runs the WIDE arm, because that is the arm `eviction_delete_drop` used to route into a
/// deletion; the narrow bound never had such a route.
///
/// WHAT IS ASSERTED, IN THIS ORDER:
///
///   1  THE ROUND REACHED THE REGIME: the stage reported, the gate opened, victims were taken.
///   2  IT WAS NOT THE DELETING ARM, checked on EVERY round rather than the last: the mode is not
///      `delete_drop` and `dropped_object_count` is ZERO. Positive there is the right "it fired"
///      test for the deleting arm and the WRONG one for this round -- asserting it positive would
///      force this fixture back onto the arm that deletes records.
///   3  MEMORY WAS ACTUALLY RECLAIMED: buckets were released and this shard's resident index FELL.
///      That is the "it fired" assertion a release owns.
///   4  AND NOTHING WAS LOST: every acknowledged key reads back byte-identical COLD, in both
///      directions -- the mismatch lists empty AND the verified count equal to the corpus.
///   5  THE NEGATIVE CONTROL, same build, same moment: keys never written read absent.
///   6  AND AGAIN AFTER A RELOAD on a fresh engine over the same directories, both directions and
///      the control again.
///
/// 3 and 4 are both asserted because either alone is satisfiable by a broken fixture: a round that
/// did nothing loses nothing, and a round that emptied the shard reclaims plenty.
///
///   7  AND THE AUTHORITY ARM, last, on the reloaded store this test has just pronounced whole:
///      the deleting arm is invoked DIRECTLY and keys go missing. Without it, "the stage lost
///      nothing" is also what a read-back loop that cannot see a loss reports. This is the inverse
///      of 4 on the same fixture at the same moment, which is what makes 4 an isolation rather
///      than an agreement with nothing.
///
/// WHAT WAS TRIED AND IS NOT HERE. An earlier control ran the same options with the retired field
/// FALSE, expecting a round that refused every candidate for want of a dump. It released 29
/// buckets: `run_storage_manager_once` runs the whole cycle, and the dump stage ahead of eviction
/// has already cleaned the victims by the time the evict stage sees them. So on the STAGE path
/// `eviction_dump_before_evict` does not decide whether a release is possible -- which is also why
/// the retired field could not be given "force the dump" as a safe new meaning. It would have been
/// a setting nothing reads.
#[test]
fn the_memory_pressure_stage_cannot_route_into_the_arm_that_deletes() {
    const KEYS: usize = 1_200;
    const MAX_ROUNDS: usize = 8;

    let dir = tempdir().unwrap();
    let (released_total, dropped_total, resident_before, resident_after) = {
        let runtime = seeded_runtime(dir.path(), KEYS);
        let engine = runtime.engine();
        let _ = engine.cache().invalidate_shard(1);
        let resident_before = engine.bucket_index_resident_bytes(1);
        assert!(
            resident_before > 0,
            "the fixture left no resident index, so there is no memory for the round to reclaim"
        );

        let mut released_total = 0usize;
        let mut dropped_total = 0usize;
        let mut rounds_reported = 0usize;
        let mut saw_open_gate = false;
        let mut saw_victims = false;
        for _ in 0..MAX_ROUNDS {
            let report = runtime.run_storage_manager_once(1, wide_arm_options());
            let Some(eviction) = report.eviction.as_ref() else {
                continue;
            };
            rounds_reported += 1;
            saw_open_gate |= eviction.pressure_gate_open;
            saw_victims |= !eviction.selected_victims.is_empty();

            // --- 2. IT WAS NOT THE DELETING ARM. Checked on EVERY round, not on the last one. ---
            assert_ne!(
                eviction.mode, "delete_drop",
                "the stage took the arm that DELETES records to reclaim memory: {eviction:?}"
            );
            assert_eq!(
                eviction.dropped_object_count, 0,
                "a memory-pressure round deleted {} records",
                eviction.dropped_object_count
            );

            released_total += eviction.bucket_index_buckets_released;
            dropped_total += eviction.dropped_object_count;
            if released_total > 0 {
                break;
            }
        }

        // --- 1. THE ROUND REACHED THE REGIME. ---
        assert!(
            rounds_reported > 0,
            "{MAX_ROUNDS} rounds produced no eviction report at all, so the stage never ran"
        );
        assert!(
            saw_open_gate,
            "no round opened the gate, so this fixture never reached memory pressure and nothing \
             below is a statement about eviction"
        );
        assert!(
            saw_victims,
            "the gate opened and no round took a victim, so there was nothing to release"
        );

        // --- 3. MEMORY WAS ACTUALLY RECLAIMED. ---
        let resident_after = engine.bucket_index_resident_bytes(1);
        assert!(
            released_total > 0,
            "{MAX_ROUNDS} rounds on the wide arm released no bucket, so this round reclaims \
             nothing and \"it lost nothing\" below is vacuous"
        );
        assert!(
            resident_after < resident_before,
            "{released_total} buckets were released and the resident index did not fall \
             ({resident_before} -> {resident_after})"
        );

        // --- 4. AND NOTHING WAS LOST, COLD. ---
        let _ = engine.cache().invalidate_shard(1);
        let read = read_back_all(&engine, KEYS);
        assert!(
            read.missing.is_empty() && read.wrong.is_empty(),
            "{} of {KEYS} acknowledged keys went missing and {} answered wrong after the stage \
             released {released_total} buckets: first few {:?} {:?}",
            read.missing.len(),
            read.wrong.len(),
            &read.missing[..read.missing.len().min(5)],
            &read.wrong[..read.wrong.len().min(5)]
        );
        assert_eq!(
            read.verified(),
            KEYS,
            "only {} of {KEYS} keys were verified, so the empty lists above are a loop that did \
             not run rather than a store that answered",
            read.verified()
        );

        // --- 5. THE NEGATIVE CONTROL. ---
        let answered = never_written_keys_that_answered(&engine, CONTROL_PROBES);
        assert_eq!(
            answered, 0,
            "{answered} of {CONTROL_PROBES} keys that were never written answered with a value, so \
             the hits above are not a reading"
        );

        // Written out while the buckets are released, so the reload comes back from the state the
        // stage left rather than from an index taken before it.
        engine.flush_shard_index(1);
        (released_total, dropped_total, resident_before, resident_after)
    };

    // --- 6. AND AGAIN AFTER A RELOAD. ---
    //
    // A fresh engine over the same directories, and only after the runtime above is dropped: two
    // handles on one directory is its own failure mode and would make this prove something else.
    let reloaded = engine_at(dir.path());
    load_shard_on_shipped_range(&reloaded, 2);
    let reloaded_resident = reloaded.bucket_index_resident_bytes(1);
    assert!(
        reloaded_resident > 0,
        "the reloaded shard rebuilt no resident index, so its reads are not going through the \
         structure the stage released"
    );
    let read = read_back_all(&reloaded, KEYS);
    assert!(
        read.missing.is_empty() && read.wrong.is_empty(),
        "{} of {KEYS} acknowledged keys did not survive a reload taken after the stage released \
         {released_total} buckets, and {} answered wrong: first few {:?} {:?}",
        read.missing.len(),
        read.wrong.len(),
        &read.missing[..read.missing.len().min(5)],
        &read.wrong[..read.wrong.len().min(5)]
    );
    assert_eq!(
        read.verified(),
        KEYS,
        "only {} of {KEYS} keys were verified after the reload, so the empty lists above are a \
         loop that did not run",
        read.verified()
    );
    assert_eq!(
        never_written_keys_that_answered(&reloaded, CONTROL_PROBES),
        0,
        "the reloaded store answered a key that was never written"
    );

    // --- 7. THE AUTHORITY ARM. ---
    //
    // The same store, the same read-back loop, the same moment -- and the deleting arm invoked
    // DIRECTLY, which is the one caller it still has. If this loses nothing either, then the loop
    // that reported "nothing lost" above cannot see a loss and 4 and 6 proved nothing.
    let authority = reloaded.apply_storage_eviction(1, 0, 16, true, true);
    assert_eq!(authority.mode, "delete_drop", "{authority:?}");
    assert!(
        authority.dropped_object_count > 0,
        "the deleting arm, called directly, deleted nothing, so this arm cannot establish that the \
         read-back loop above is able to see a loss: {authority:?}"
    );
    let _ = reloaded.cache().invalidate_shard(1);
    let after_authority = read_back_all(&reloaded, KEYS);
    assert_eq!(
        after_authority.missing.len(),
        authority.dropped_object_count,
        "the deleting arm deleted {} records and the same read-back loop that found {KEYS} of \
         {KEYS} whole a moment ago now finds {} missing; the two must be the same set",
        authority.dropped_object_count,
        after_authority.missing.len()
    );

    eprintln!(
        "  [stage-cannot-delete] {KEYS} acknowledged keys on the wide arm. STAGE: \
{released_total} buckets released, {dropped_total} records deleted, resident index \
{resident_before} -> {resident_after} B; all {KEYS} read back cold and all {KEYS} again after a \
reload into a {reloaded_resident} B index; {CONTROL_PROBES} never-written keys absent in both \
halves. AUTHORITY ARM, deleting arm called directly on that same reloaded store: {} records \
deleted and {} keys immediately missing, so the loop above can see a loss.",
        authority.dropped_object_count,
        after_authority.missing.len(),
    );
}

/// WHAT THE DROPPABLE HALF RECLAIMS, AND WHAT ONLY A DELETION RECLAIMS -- the number the shipping
/// decision rests on.
///
/// Two fixtures of the same corpus at the same size, one round each, one field apart:
///
///   SAFE        dump the dirty victims, clear them, release the block lists. Reclaims the
///               DERIVED half of the resident index. Keeps every record.
///   DESTRUCTIVE delete every live object in the victim buckets. Reclaims the derived half AND the
///               authoritative model-map entries. Loses every record it touched.
///
/// WHAT IS ASSERTED rather than printed: the safe arm reclaimed bytes AND lost nothing; the
/// destructive arm lost records. Both halves on both arms, because "nothing was lost" is satisfied
/// by a round that did nothing and "memory went down" is satisfied by a round that deleted the
/// store.
#[test]
fn the_droppable_half_reclaims_without_loss_and_only_a_deletion_takes_the_rest() {
    const KEYS: usize = 600;
    const BATCH: usize = 16;

    // --- SAFE ARM: dump, then drop the derived half. ---
    let safe_dir = tempdir().unwrap();
    let safe_engine = seeded_engine(safe_dir.path(), KEYS);
    let _ = safe_engine.cache().invalidate_shard(1);
    let safe_before = safe_engine.bucket_index_resident_bytes(1);
    let safe = safe_engine.apply_storage_eviction(1, 0, BATCH, true, false);
    let safe_after = safe_engine.bucket_index_resident_bytes(1);
    assert_eq!(safe.mode, "evict_cache", "{safe:?}");
    assert!(safe.pressure_gate_open, "the safe arm declined: {safe:?}");
    assert!(
        !safe.selected_victims.is_empty(),
        "the safe arm took no victims: {safe:?}"
    );
    assert!(
        safe.bucket_index_buckets_released > 0,
        "the safe arm released no bucket, so it reclaimed nothing and \"it lost nothing\" is \
         vacuous: {} refused",
        safe.bucket_index_release_refused
    );
    assert!(
        safe_after < safe_before,
        "the safe arm released {} buckets and the resident index did not fall ({safe_before} -> \
         {safe_after})",
        safe.bucket_index_buckets_released
    );
    assert_eq!(
        safe.dropped_object_count, 0,
        "the safe arm deleted {} records",
        safe.dropped_object_count
    );
    let _ = safe_engine.cache().invalidate_shard(1);
    let safe_read = read_back_all(&safe_engine, KEYS);
    assert!(
        safe_read.missing.is_empty() && safe_read.wrong.is_empty(),
        "the safe arm lost {} and corrupted {} of {KEYS} keys: first few {:?} {:?}",
        safe_read.missing.len(),
        safe_read.wrong.len(),
        &safe_read.missing[..safe_read.missing.len().min(5)],
        &safe_read.wrong[..safe_read.wrong.len().min(5)]
    );
    assert_eq!(
        safe_read.verified(),
        KEYS,
        "only {} of {KEYS} keys were verified on the safe arm",
        safe_read.verified()
    );
    assert_eq!(
        never_written_keys_that_answered(&safe_engine, CONTROL_PROBES),
        0,
        "the safe arm's store answered a key that was never written"
    );

    // --- DESTRUCTIVE ARM: the same corpus, the same round, one field moved. ---
    let drop_dir = tempdir().unwrap();
    let drop_engine = seeded_engine(drop_dir.path(), KEYS);
    let _ = drop_engine.cache().invalidate_shard(1);
    let drop_before = drop_engine.bucket_index_resident_bytes(1);
    let destructive = drop_engine.apply_storage_eviction(1, 0, BATCH, true, true);
    let drop_after = drop_engine.bucket_index_resident_bytes(1);
    assert_eq!(destructive.mode, "delete_drop", "{destructive:?}");
    assert!(
        destructive.pressure_gate_open,
        "the destructive arm declined: {destructive:?}"
    );
    assert!(
        destructive.dropped_object_count > 0,
        "the destructive arm deleted nothing, so this fixture never entered its regime"
    );
    let _ = drop_engine.cache().invalidate_shard(1);
    let drop_read = read_back_all(&drop_engine, KEYS);
    assert!(
        !drop_read.missing.is_empty(),
        "the destructive arm reported {} records dropped and lost none of {KEYS} keys",
        destructive.dropped_object_count
    );

    // THE COMPARISON THAT DECIDES IT, stated as a property of each arm rather than printed only:
    // the safe arm reclaimed bytes at zero loss; the destructive arm's extra bytes are paid for in
    // records that no read can reach again.
    let safe_reclaimed = safe_before.saturating_sub(safe_after);
    let destructive_reclaimed = drop_before.saturating_sub(drop_after);
    assert!(
        safe_reclaimed > 0,
        "the safe arm reclaimed no bytes, so there is nothing to weigh the destructive arm against"
    );

    eprintln!(
        "  [two-halves] {KEYS} keys, batch {BATCH}. SAFE: {} buckets released ({} refused), \
resident index {safe_before} -> {safe_after} B = {safe_reclaimed} B reclaimed, {} records \
deleted, {} of {KEYS} read back whole. DESTRUCTIVE: {} records deleted, resident index \
{drop_before} -> {drop_after} B = {destructive_reclaimed} B reclaimed, {} acknowledged keys \
UNREACHABLE, {} of {KEYS} read back whole.",
        safe.bucket_index_buckets_released,
        safe.bucket_index_release_refused,
        safe.dropped_object_count,
        safe_read.verified(),
        destructive.dropped_object_count,
        drop_read.missing.len(),
        drop_read.verified(),
    );
}

/// Total bytes on disk under one directory, walked recursively.
///
/// Bracketed around a single `apply_storage_eviction` call, this attributes bytes to that call:
/// the only writer inside it is `create_bucket_dump_manifest`, which is exactly the dump whose
/// cost is in question.
fn bytes_under(path: &std::path::Path) -> u64 {
    fn walk(path: &std::path::Path, total: &mut u64) {
        if let Ok(entries) = std::fs::read_dir(path) {
            for entry in entries.flatten() {
                match entry.file_type() {
                    Ok(kind) if kind.is_dir() => walk(&entry.path(), total),
                    Ok(_) => {
                        if let Ok(meta) = entry.metadata() {
                            *total = total.saturating_add(meta.len());
                        }
                    }
                    Err(_) => {}
                }
            }
        }
    }
    let mut total = 0u64;
    walk(path, &mut total);
    total
}

/// Dump every dirty bucket and clear its dirty state, so what follows runs on a store whose
/// objects are UNCHANGED SINCE THEIR DUMP.
///
/// Driven through `apply_storage_lifecycle`, which is the shipped checkpoint path: it takes the
/// plan's dirty-bucket selection, creates the manifest, and clears the dirty state the manifest
/// covers. `max_dump_buckets_per_round: 0` means no cap, so one call covers the whole shard.
fn dump_everything_and_clear(engine: &TemporalEngine) -> usize {
    let report = engine.apply_storage_lifecycle(crate::engine::reports::StorageLifecycleRequest {
        shard_id: 1,
        selected_dump_buckets: Vec::new(),
        max_dump_buckets_per_round: 0,
        min_undumped_wal_records: 1,
        ..crate::engine::reports::StorageLifecycleRequest::default()
    });
    report
        .dump_manifest
        .as_ref()
        .map(|manifest| manifest.bucket_ids.len())
        .unwrap_or(0)
}

/// WHAT A DUMP COSTS FOR AN OBJECT NOTHING HAS CHANGED SINCE ITS LAST DUMP: measured, and it is
/// ZERO BYTES AND ZERO MANIFESTS.
///
/// The rule this store holds is that an object unchanged since it was dumped is dropped from
/// memory DIRECTLY -- its durable copy already exists, so there is nothing to write first -- while
/// a changed object is dumped and then dropped. The question this answers is whether the shipped
/// round pays a dump for the first case anyway.
///
/// IT DOES NOT, AND THE SELECTION IS WHERE THAT LIVES. `apply_storage_eviction` builds its dump
/// list as the victims with `dirty_object_count > 0`; a clean victim is not in it, and when no
/// victim is dirty no manifest is created at all. The release then takes the clean bucket, because
/// `release_bucket_blocks` REFUSES a dirty one -- so the two cases are not merely distinguished,
/// they are each other's precondition.
///
/// THREE ARMS ON MATCHED CORPORA, each driven by ONE `apply_storage_eviction` call so the bytes
/// are attributable to it:
///
///   A  everything changed       -> a manifest is created, bytes are written, buckets release
///   B  everything dumped first  -> NO manifest, ZERO bytes, buckets still release
///   C  one object changed       -> a manifest is created, and it writes the WHOLE index
///
/// Arm A is what makes arm B a finding: without it, "no bytes written" is also what a round that
/// never reached the dump would report. Arm C is the residual and it is stated rather than
/// removed: a manifest carries the whole-shard index by construction, because install writes the
/// decoded index as THE index for the shard and a manifest narrowed to its own buckets installs a
/// shard missing every bucket it did not name. So the bytes an unchanged object contributes to
/// SOMEONE ELSE's dump are load-bearing, and the only cost this store can decline is the one arm
/// B measures at zero.
///
/// Arm B also carries the correctness proof in both directions: cold read-back, a reload on a
/// fresh engine over the same directories, and the never-written control.
#[test]
fn an_object_unchanged_since_its_dump_is_released_without_being_dumped_again() {
    const KEYS: usize = 1_200;
    const BATCH: usize = 16;

    struct Arm {
        label: &'static str,
        manifests: usize,
        bytes_written: u64,
        released: usize,
        refused: usize,
        dropped: usize,
        round_micros: u128,
    }

    /// One eviction round, with the bytes and the time it spent bracketed around it.
    fn round(engine: &TemporalEngine, dir: &std::path::Path, label: &'static str) -> Arm {
        let before = bytes_under(&dir.join("indexes")) + bytes_under(&dir.join("pages"));
        let clock = std::time::Instant::now();
        let report = engine.apply_storage_eviction(1, 1, BATCH, true, false);
        let round_micros = clock.elapsed().as_micros();
        let after = bytes_under(&dir.join("indexes")) + bytes_under(&dir.join("pages"));
        assert_eq!(
            report.skipped_reason, "",
            "{label}: the gate declined, so this arm measures a refusal: {report:?}"
        );
        Arm {
            label,
            manifests: report.dump_manifest_ids.len(),
            bytes_written: after.saturating_sub(before),
            released: report.bucket_index_buckets_released,
            refused: report.bucket_index_release_refused,
            dropped: report.dropped_object_count,
            round_micros,
        }
    }

    // --- ARM A: everything changed. The dump is needed, and it costs. ---
    let dir_a = tempdir().unwrap();
    let engine_a = seeded_engine(dir_a.path(), KEYS);
    let _ = engine_a.cache().invalidate_shard(1);
    let arm_a = round(&engine_a, dir_a.path(), "A everything changed");

    // --- ARM B: everything dumped first. The same round, with nothing left to dump. ---
    let dir_b = tempdir().unwrap();
    let engine_b = seeded_engine(dir_b.path(), KEYS);
    let dumped_buckets = dump_everything_and_clear(&engine_b);
    assert!(
        dumped_buckets > 0,
        "arm B dumped no bucket, so its objects are not unchanged-since-dump and the arm measures \
         the wrong thing"
    );
    let _ = engine_b.cache().invalidate_shard(1);
    let arm_b = round(&engine_b, dir_b.path(), "B everything already dumped");

    // --- ARM C: one object changed, every other one unchanged. ---
    let dir_c = tempdir().unwrap();
    let engine_c = seeded_engine(dir_c.path(), KEYS);
    assert!(dump_everything_and_clear(&engine_c) > 0, "arm C did not dump");
    let changed = engine_c.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: key_of(0),
            value: keyed_value(0),
        },
    });
    assert!(changed.status.ok, "arm C could not change one record");
    let index_image_c = engine_c
        .export_index_bytes(1)
        .expect("arm C could not export its index")
        .len() as u64;
    let _ = engine_c.cache().invalidate_shard(1);
    // Batch 0 means NO limit here, so the one changed bucket is certainly a victim.
    let before_c = bytes_under(&dir_c.path().join("indexes")) + bytes_under(&dir_c.path().join("pages"));
    let report_c = engine_c.apply_storage_eviction(1, 1, 0, true, false);
    let after_c = bytes_under(&dir_c.path().join("indexes")) + bytes_under(&dir_c.path().join("pages"));
    let arm_c = Arm {
        label: "C one object changed",
        manifests: report_c.dump_manifest_ids.len(),
        bytes_written: after_c.saturating_sub(before_c),
        released: report_c.bucket_index_buckets_released,
        refused: report_c.bucket_index_release_refused,
        dropped: report_c.dropped_object_count,
        round_micros: 0,
    };

    eprintln!("  [unchanged-dump-cost] {KEYS} keys on the shipped 0..{SHIPPED_END_BUCKET} range, batch {BATCH}");
    eprintln!(
        "    {:<30} {:>10} {:>14} {:>9} {:>8} {:>8} {:>12}",
        "arm", "manifests", "bytes_written", "released", "refused", "dropped", "round_us"
    );
    for arm in [&arm_a, &arm_b, &arm_c] {
        eprintln!(
            "    {:<30} {:>10} {:>14} {:>9} {:>8} {:>8} {:>12}",
            arm.label,
            arm.manifests,
            arm.bytes_written,
            arm.released,
            arm.refused,
            arm.dropped,
            arm.round_micros,
        );
    }

    // --- THE FINDING: an unchanged object costs no dump at all. ---
    assert_eq!(
        arm_a.manifests, 1,
        "arm A created {} manifests. With every object changed the round has to dump before it \
         can release, so this arm is what makes arm B's zero a finding",
        arm_a.manifests
    );
    assert!(
        arm_a.bytes_written > 0,
        "arm A wrote no bytes although it created {} manifest(s), so the byte bracket above is \
         not measuring the dump",
        arm_a.manifests
    );
    assert_eq!(
        arm_b.manifests, 0,
        "arm B created {} manifest(s) for victims nothing has changed since their dump. Their \
         durable copy already exists, so the round owes them no write",
        arm_b.manifests
    );
    assert_eq!(
        arm_b.bytes_written, 0,
        "arm B wrote {} bytes dumping objects unchanged since their last dump",
        arm_b.bytes_written
    );
    // AND IT STILL RELEASES. A no-dump path that could not release would be a refusal, not a
    // saving.
    assert!(
        arm_b.released > 0,
        "arm B released nothing ({} refused), so the no-dump path does not actually reclaim",
        arm_b.refused
    );
    assert_eq!(
        arm_b.dropped, 0,
        "arm B deleted {} records. Relieving memory releases pages and never deletes",
        arm_b.dropped
    );

    // --- THE RESIDUAL, stated with its number. ---
    assert_eq!(
        arm_c.manifests, 1,
        "arm C created {} manifests for one changed object",
        arm_c.manifests
    );
    assert!(
        arm_c.bytes_written >= index_image_c,
        "arm C wrote {} bytes for one changed object against a {index_image_c} B whole-shard \
         index image. A manifest carries the whole index by construction, so this is the floor, \
         and if it is now lower the manifest has been narrowed -- which installs a shard missing \
         every bucket it did not name",
        arm_c.bytes_written
    );
    eprintln!(
        "    => an unchanged object costs {} B and {} manifests of its OWN round (arm B). The \
residual is arm C: one changed object out of {KEYS} dumps a {index_image_c} B whole-shard index \
image ({} B written), so about {:.2}% of those bytes describe objects nothing has changed -- and \
that is required, because install writes the decoded index as THE index for the shard",
        arm_b.bytes_written,
        arm_b.manifests,
        arm_c.bytes_written,
        100.0 * (KEYS - 1) as f64 / KEYS as f64,
    );

    // --- CORRECTNESS, BOTH DIRECTIONS, on the arm that took the no-dump path. ---
    let reads = read_back_all(&engine_b, KEYS);
    assert!(
        reads.missing.is_empty() && reads.wrong.is_empty(),
        "arm B lost records to a release that wrote nothing: {} missing, {} wrong, first few \
         {:?} / {:?}",
        reads.missing.len(),
        reads.wrong.len(),
        &reads.missing[..reads.missing.len().min(5)],
        &reads.wrong[..reads.wrong.len().min(5)]
    );
    assert_eq!(
        reads.verified(),
        KEYS,
        "only {} of {KEYS} keys were verified, so the empty lists above are a loop that did not run",
        reads.verified()
    );
    assert_eq!(
        never_written_keys_that_answered(&engine_b, CONTROL_PROBES),
        0,
        "a key this fixture never wrote answered with a value, so the read-back above is not \
         evidence that the store knows what it holds"
    );
    engine_b.flush_shard_index(1);
    drop(engine_b);

    let reloaded = engine_at(dir_b.path());
    load_shard_on_shipped_range(&reloaded, 2);
    let after_reload = read_back_all(&reloaded, KEYS);
    assert!(
        after_reload.missing.is_empty() && after_reload.wrong.is_empty(),
        "arm B lost records across a reload: {} missing, {} wrong, first few {:?} / {:?}",
        after_reload.missing.len(),
        after_reload.wrong.len(),
        &after_reload.missing[..after_reload.missing.len().min(5)],
        &after_reload.wrong[..after_reload.wrong.len().min(5)]
    );
    assert_eq!(
        after_reload.verified(),
        KEYS,
        "only {} of {KEYS} keys were verified after the reload",
        after_reload.verified()
    );
    assert_eq!(
        never_written_keys_that_answered(&reloaded, CONTROL_PROBES),
        0,
        "after the reload a key that was never written answered with a value"
    );
    eprintln!(
        "    [unchanged-correctness] arm B released {} buckets having written {} bytes; all \
{KEYS} keys read back whole cold and all {KEYS} again after a reload, with {CONTROL_PROBES} \
never-written probes absent in both",
        arm_b.released, arm_b.bytes_written
    );
}
