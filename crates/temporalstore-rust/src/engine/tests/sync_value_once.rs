// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A synchronous string write stores its value ONCE, in the log record it acks on.
//!
//! The record is durable before the ack, so the block-store copy bought nothing a read or a
//! recovery used. These tests pin the three halves of that claim:
//!
//! * the placement: no block-store bytes are written on the write path, the value is served from
//!   its record;
//! * crash safety: an acked value reads back after a crash with nothing but the log left, and
//!   after a torn tail every record before the tear survives;
//! * the dump: moving the values into the block store, then reclaiming the log, never loses one.

use super::*;

const SHARD: u64 = 1;

fn dirs(root: &std::path::Path) -> (std::path::PathBuf, std::path::PathBuf, std::path::PathBuf) {
    (root.join("cache"), root.join("blocks"), root.join("indexes"))
}

fn open(root: &std::path::Path) -> TemporalEngine {
    let (cache, blocks, indexes) = dirs(root);
    TemporalEngine::with_local_dirs(1024 * 1024, cache, blocks, indexes)
}

fn set(engine: &TemporalEngine, key: &str, value: &[u8]) {
    let response = engine.execute(ExecuteRequest {
        shard_id: SHARD,
        command: Command::StringSet { key: key.to_string(), value: value.to_vec() },
    });
    assert!(response.status.ok, "write refused: {response:?}");
}

fn get(engine: &TemporalEngine, key: &str) -> Option<Vec<u8>> {
    match engine
        .execute(ExecuteRequest { shard_id: SHARD, command: Command::StringGet { key: key.to_string() } })
        .response
    {
        CommandResponse::Bytes { value } => value,
        other => panic!("unexpected response {other:?}"),
    }
}

fn value_for(index: usize) -> Vec<u8> {
    format!("value-once-{index:04}-").repeat(8).into_bytes()
}

fn block_bytes_on_disk(engine: &TemporalEngine) -> u64 {
    engine.block_store.slab_summary().total_known_physical_bytes
}

/// Sync write, no flush, no unload: the process just stops. Only the files remain.
fn write_then_crash(root: &std::path::Path, keys: usize) {
    let engine = open(root);
    engine.load_shard(SHARD);
    for index in 0..keys {
        set(&engine, &format!("k{index:04}"), &value_for(index));
    }
    // Dropped without `unload_shard`.
}

fn reload(root: &std::path::Path) -> TemporalEngine {
    let engine = open(root);
    let load = engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: SHARD,
        load_version: 0,
        local_node_id: None,
        shard_uri: String::new(),
        start_routing_bucket: 0,
        end_routing_bucket: u32::MAX,
        readonly: false,
        table_name: String::new(),
    });
    assert!(load.status.ok, "reload refused: {:?}", load.status);
    engine
}

/// The write path stores the value once: the block store gains nothing, and the index names the
/// record. Planted fault: `TS_SYNC_VALUE_ONCE=0` (or the `value_once` gate removed) makes the block
/// store grow by a segment record per set and this fails.
#[test]
fn a_synchronous_set_writes_no_block_store_bytes() {
    let _on = value_once_in_test(true);
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    engine.load_shard(SHARD);
    set(&engine, "warm", b"warm");
    let before = block_bytes_on_disk(&engine);
    for index in 0..64 {
        set(&engine, &format!("k{index:04}"), &value_for(index));
    }
    let after = block_bytes_on_disk(&engine);
    assert_eq!(
        before, after,
        "a synchronous set must not write its value into the block store: {before} -> {after}"
    );
    assert!(
        engine.synthetic_address_count_for_test(SHARD) >= 64,
        "the served index must name the log records that hold the values"
    );
    assert!(engine.wal_resident_block_count(SHARD) >= 64);
    // Not vacuous: the value really is readable, with the cache emptied so it comes from the record.
    engine.cache().invalidate_shard(SHARD).unwrap();
    for index in 0..64 {
        assert_eq!(get(&engine, &format!("k{index:04}")), Some(value_for(index)));
    }
}

/// CONTROL for the test above: with the switch off the same writes DO reach the block store, so
/// the assertion above is about the switch and not about a store that never grows.
#[test]
fn the_switch_off_restores_the_block_store_write() {
    let _on = value_once_in_test(false);
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    engine.load_shard(SHARD);
    set(&engine, "warm", b"warm");
    let before = block_bytes_on_disk(&engine);
    for index in 0..64 {
        set(&engine, &format!("k{index:04}"), &value_for(index));
    }
    let after = block_bytes_on_disk(&engine);
    assert!(after > before, "control: the switch off must write blocks: {before} -> {after}");
}

/// An acked synchronous write reads back after a crash that leaves only the log: the block store,
/// the cache and the served index are all removed.
#[test]
fn an_acked_value_survives_a_crash_that_leaves_only_the_log() {
    let _on = value_once_in_test(true);
    let dir = tempfile::tempdir().unwrap();
    write_then_crash(dir.path(), 50);
    let (cache, blocks, indexes) = dirs(dir.path());
    let _ = std::fs::remove_dir_all(&cache);
    let _ = std::fs::remove_dir_all(&blocks);
    for entry in std::fs::read_dir(&indexes).unwrap().flatten() {
        if entry.file_name() != "wals" {
            let path = entry.path();
            if path.is_dir() {
                std::fs::remove_dir_all(path).unwrap();
            } else {
                std::fs::remove_file(path).unwrap();
            }
        }
    }
    let engine = reload(dir.path());
    for index in 0..50 {
        assert_eq!(
            get(&engine, &format!("k{index:04}")),
            Some(value_for(index)),
            "acked key {index} lost: its value was only ever in the log"
        );
    }
}

/// A crash with every file kept (the common case) also reads everything back.
#[test]
fn an_acked_value_survives_a_plain_crash() {
    let _on = value_once_in_test(true);
    let dir = tempfile::tempdir().unwrap();
    write_then_crash(dir.path(), 50);
    let engine = reload(dir.path());
    for index in 0..50 {
        assert_eq!(get(&engine, &format!("k{index:04}")), Some(value_for(index)));
    }
}

/// A torn tail loses at most the record that was being appended, never an earlier acked one.
#[test]
fn a_torn_tail_keeps_every_earlier_acked_value() {
    let _on = value_once_in_test(true);
    let dir = tempfile::tempdir().unwrap();
    write_then_crash(dir.path(), 20);
    let path = dir.path().join("indexes").join("wals").join("shard-1.wal.bin");
    let mut bytes = std::fs::read(&path).expect("the log exists");
    while bytes.last() == Some(&0) {
        bytes.pop();
    }
    bytes.truncate(bytes.len() - 20);
    std::fs::write(&path, &bytes).unwrap();
    let engine = reload(dir.path());
    let present = (0..20).filter(|index| get(&engine, &format!("k{index:04}")).is_some()).count();
    assert!(present >= 19, "a torn tail may lose only the last record; {present} of 20 survive");
    for index in 0..19 {
        assert_eq!(get(&engine, &format!("k{index:04}")), Some(value_for(index)));
    }
}

/// The dump: values move into the block store, the log is reclaimed, and nothing is lost -- on the
/// live engine and after a restart that has only the block store and the dump to go on.
#[test]
fn a_dump_then_reclaim_never_loses_a_log_resident_value() {
    let _on = value_once_in_test(true);
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    engine.load_shard(SHARD);
    for index in 0..80 {
        set(&engine, &format!("k{index:04}"), &value_for(index));
    }
    assert!(engine.synthetic_address_count_for_test(SHARD) >= 80);

    let before = block_bytes_on_disk(&engine);
    let moved = engine.materialize_synthetic_blocks(SHARD);
    assert!(moved >= 80, "the dump must move every log-resident value, moved {moved}");
    assert!(block_bytes_on_disk(&engine) > before, "the dump is where the second write happens");
    assert_eq!(engine.synthetic_address_count_for_test(SHARD), 0);

    engine.create_bucket_dump_manifest(SHARD, Vec::new()).unwrap();
    let plan = engine.storage_wal_reclaim_plan(SHARD, Vec::new(), Vec::new());
    let report = engine.apply_storage_wal_reclaim(plan);
    println!("reclaim applied={} removed={}", report.applied, report.wal_records_removed);

    engine.cache().invalidate_shard(SHARD).unwrap();
    for index in 0..80 {
        assert_eq!(get(&engine, &format!("k{index:04}")), Some(value_for(index)), "live read {index}");
    }
    engine.unload_shard(SHARD);
    drop(engine);
    let engine = reload(dir.path());
    for index in 0..80 {
        assert_eq!(get(&engine, &format!("k{index:04}")), Some(value_for(index)), "restart read {index}");
    }
}

/// Two or more cycles of overwrite churn, a dump and a reclaim between each, a reopen at the end:
/// every acknowledged key reads back with its LATEST value. Plain synchronous commands throughout.
/// Planted fault: let `apply_storage_wal_reclaim` ignore the registered-floor term (it may free a
/// record that is still the only copy of a value) and a key reads back stale or missing.
#[test]
fn churn_across_dump_and_reclaim_cycles_reopens_with_every_latest_value() {
    let _on = value_once_in_test(true);
    let dir = tempfile::tempdir().unwrap();
    let keys = 40usize;
    let mut latest: Vec<Vec<u8>> = (0..keys).map(value_for).collect();
    {
        let engine = open(dir.path());
        engine.load_shard(SHARD);
        for index in 0..keys {
            set(&engine, &format!("k{index:04}"), &latest[index]);
        }
        for cycle in 0..3usize {
            // Overwrite every other key this cycle (some keys are rewritten, some left alone).
            for index in (cycle % 2..keys).step_by(2) {
                latest[index] = format!("cycle-{cycle}-key-{index}-").repeat(6).into_bytes();
                set(&engine, &format!("k{index:04}"), &latest[index]);
            }
            // Cycle 1 dumps the values and the index; the others try to reclaim with some
            // values still log-resident.
            if cycle == 1 {
                engine.materialize_synthetic_blocks(SHARD);
            }
            engine.create_bucket_dump_manifest(SHARD, Vec::new()).unwrap();
            let plan = engine.storage_wal_reclaim_plan(SHARD, Vec::new(), Vec::new());
            let _ = engine.apply_storage_wal_reclaim(plan);
            engine.cache().invalidate_shard(SHARD).unwrap();
            for index in 0..keys {
                assert_eq!(
                    get(&engine, &format!("k{index:04}")),
                    Some(latest[index].clone()),
                    "cycle {cycle}: live read of key {index}"
                );
            }
        }
        // Crash: no unload.
    }
    let engine = reload(dir.path());
    for index in 0..keys {
        assert_eq!(
            get(&engine, &format!("k{index:04}")),
            Some(latest[index].clone()),
            "after reopen: key {index} must hold its latest acknowledged value"
        );
    }
}

/// The shipped storage-manager cycle, run between waves of writes that rewrite only part of the key
/// set: some keys keep their ONLY copy in an early record while others move on. A crash after any
/// round must reload and serve every key. (Found by a 100 s soak: the restart refused to load,
/// "cannot recover WAL-backed block", after a reclaim had freed a record the persisted index named.)
#[test]
fn storage_manager_rounds_between_partial_rewrites_never_strand_a_persisted_placement() {
    let _on = value_once_in_test(true);
    let dir = tempfile::tempdir().unwrap();
    let keys = 300usize;
    let mut latest: Vec<Vec<u8>> = (0..keys).map(value_for).collect();
    {
        let engine = open(dir.path());
        engine.load_shard(SHARD);
        for index in 0..keys {
            set(&engine, &format!("k{index:04}"), &latest[index]);
        }
        for round in 0..4usize {
            // The loop also writes a base served-index image between rounds; a placement the
            // image names can be superseded (and its record reclaimed) after it is written.
            engine.flush_shard_index(SHARD);
            let report = engine.run_storage_manager_cycle(StorageManagerCycleRequest {
                shard_id: SHARD,
                min_undumped_wal_records: 0,
                min_undumped_wal_bytes: 0,
                ..StorageManagerCycleRequest::default()
            });
            println!(
                "round {round}: reclaim applied={:?}",
                report.wal_reclaim_report.as_ref().map(|r| (r.applied, r.wal_records_removed))
            );
            // Rewrite a shifting 2/3 of the keys; the rest keep their older record.
            for index in (0..keys).filter(|index| (index + round) % 3 != 0) {
                latest[index] = format!("round-{round}-key-{index}-").repeat(5).into_bytes();
                set(&engine, &format!("k{index:04}"), &latest[index]);
            }
        }
        // Crash after the last writes: no unload, no further round.
    }
    let engine = reload(dir.path());
    for index in 0..keys {
        assert_eq!(
            get(&engine, &format!("k{index:04}")),
            Some(latest[index].clone()),
            "key {index} lost across rounds + crash"
        );
    }
}

/// A base served-index image written EARLY names placements that later writes supersede. A round
/// that then takes a manifest (naming the newer placements) and reclaims must still keep the
/// records the base image names, because that image is what a restart loads: freeing them makes
/// the load refuse ("cannot recover WAL-backed block"). Found by a 100 s soak through the loop.
/// Planted fault: drop the base-image term from the floor in `apply_storage_wal_reclaim`.
#[test]
fn a_reclaim_keeps_the_records_the_persisted_base_image_names() {
    let _on = value_once_in_test(true);
    let dir = tempfile::tempdir().unwrap();
    let keys = 200usize;
    let mut latest: Vec<Vec<u8>> = (0..keys).map(value_for).collect();
    {
        let engine = open(dir.path());
        engine.load_shard(SHARD);
        for index in 0..keys {
            set(&engine, &format!("k{index:04}"), &latest[index]);
        }
        // The base image names every key's FIRST record.
        engine.flush_shard_index(SHARD);
        // Supersede those placements.
        for index in 0..keys {
            latest[index] = format!("rewritten-{index}-").repeat(6).into_bytes();
            set(&engine, &format!("k{index:04}"), &latest[index]);
        }
        // A round that dumps a manifest and reclaims, WITHOUT writing a new base image.
        for _ in 0..2 {
            engine.create_bucket_dump_manifest(SHARD, Vec::new()).unwrap();
            let plan = engine.storage_wal_reclaim_plan(SHARD, Vec::new(), Vec::new());
            let report = engine.apply_storage_wal_reclaim(plan);
            println!("reclaim applied={} removed={}", report.applied, report.wal_records_removed);
        }
        // Crash.
    }
    let engine = reload(dir.path());
    for index in 0..keys {
        assert_eq!(
            get(&engine, &format!("k{index:04}")),
            Some(latest[index].clone()),
            "key {index}: the load or the read lost it"
        );
    }
}

/// The reclaim floor reads the registrations of the values that live only in the log. After
/// single-copy writes it must name the oldest of them; a `None` here means the floor is cleared
/// and a reclaim frees records that still hold the only copy of a value.
#[test]
fn the_retention_floor_input_names_the_oldest_log_resident_value() {
    let _on = value_once_in_test(true);
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    engine.load_shard(SHARD);
    for index in 0..30 {
        set(&engine, &format!("k{index:04}"), &value_for(index));
    }
    assert!(engine.registration_count_for_test(SHARD) >= 30);
    let floor = block_in_wal::min_registered_sequence(&engine.block_store, SHARD, &engine.wal_store);
    println!("registrations {} floor {:?}", engine.registration_count_for_test(SHARD), floor);
    assert!(floor.is_some(), "30 registrations and no floor: reclaim would free every record");
    // And after a bucket dump, which retires only registrations the dump no longer needs.
    engine.create_bucket_dump_manifest(SHARD, Vec::new()).unwrap();
    let after = block_in_wal::min_registered_sequence(&engine.block_store, SHARD, &engine.wal_store);
    println!("after dump: registrations {} floor {:?}", engine.registration_count_for_test(SHARD), after);
    assert!(
        after.is_some() || engine.registration_count_for_test(SHARD) == 0,
        "registrations remain but the floor input is empty after a dump"
    );
}

/// The batched read a load uses to re-register log-resident values answers exactly what the single
/// read answers, for every id, across several log segments, and says `None` for an id past the end.
#[test]
fn a_batched_log_read_answers_what_the_single_read_answers() {
    let _on = value_once_in_test(true);
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    engine.load_shard(SHARD);
    // Enough records to roll the log over several segments (about 3,000 x 220 B).
    for index in 0..3000 {
        set(&engine, &format!("k{index:05}"), &value_for(index % 50));
    }
    let store = engine.write_ahead_log_store();
    let records = store.scan(SHARD, 0, u64::MAX, u64::MAX).unwrap();
    let mut ids: Vec<u64> = records.iter().map(|(log_id, _)| *log_id).collect();
    assert!(ids.len() >= 3000);
    let segments = std::fs::read_dir(dir.path().join("indexes").join("wals"))
        .unwrap()
        .flatten()
        .filter(|entry| entry.file_name().to_string_lossy().starts_with("shard-1.wal."))
        .count();
    assert!(segments >= 3, "the fixture must span several segments, spans {segments}");
    ids.push(u64::MAX - 1);
    let batched = store.read_frames_at_log_ids(SHARD, &ids).unwrap();
    assert_eq!(batched.len(), ids.len());
    for (id, frame) in ids.iter().zip(&batched) {
        let single = store.read_frame_at_log_id(SHARD, *id).unwrap();
        assert_eq!(frame, &single, "log id {id}: batched and single reads disagree");
    }
    assert!(batched.last().unwrap().is_none(), "an id past the log is None");
    assert!(batched[..batched.len() - 1].iter().all(Option::is_some));
}

/// Reclaim attempted BEFORE the values are dumped must not free the records that hold them.
/// Planted fault: dropping the registered-floor term in `apply_storage_wal_reclaim` lets the log
/// go and the post-crash read below returns None.
#[test]
fn reclaim_before_the_dump_keeps_the_records_that_hold_the_values() {
    let _on = value_once_in_test(true);
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = open(dir.path());
        engine.load_shard(SHARD);
        for index in 0..80 {
            set(&engine, &format!("k{index:04}"), &value_for(index));
        }
        // A served-index dump that does NOT include the log-resident values being materialised.
        engine.create_bucket_dump_manifest(SHARD, Vec::new()).unwrap();
        let plan = engine.storage_wal_reclaim_plan(SHARD, Vec::new(), Vec::new());
        let _ = engine.apply_storage_wal_reclaim(plan);
        assert!(engine.wal_resident_block_count(SHARD) > 0);
    }
    let engine = reload(dir.path());
    for index in 0..80 {
        assert_eq!(
            get(&engine, &format!("k{index:04}")),
            Some(value_for(index)),
            "reclaim before the dump lost key {index}"
        );
    }
}
