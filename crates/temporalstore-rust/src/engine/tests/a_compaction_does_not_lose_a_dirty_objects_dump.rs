// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A COMPACTION ROUND LEAVES NOTHING UNDUMPED THAT A RECOVERY NEEDS.
//!
//! A compaction round re-files every block of the shard and files them clean, so right after one a
//! dump plan selects no bucket even though every object it holds is still in the dirty set -- ten
//! of ten here, measured. That looks like the start of a lost write: nothing dumped, then the log
//! reclaimed past the writes. It is not, and this test pins why. The round ends by serialising the
//! whole shard index and persisting it (`persist_index_bytes` in `compact_shard_blocks_relocating`),
//! so "clean" after a compaction means "durable in the base index", which is what a recovery loads
//! before replaying the log. The index the round persists is the thing the clean flags are true of.
//!
//! The sequence that would lose a value is exactly this one: write, compact, dump what the plan
//! selects, reclaim the log, drop the engine without unloading, reopen, compare every value with
//! what was read before. A compaction that stopped persisting the index turns it red, and so does
//! any later change that lets the dump plan and the log reclaim agree a bucket is clean while no
//! durable copy holds it.
//!
//! It also holds a control (the same sequence with no compaction, which must recover everything and
//! reclaim log, or the sequence does not reach the loss) and the other door onto the same flags: a
//! write that names no object key.
//!
//! rust-internal: measures this crate's own request paths, no product behaviour

#![allow(clippy::all)]
use super::*;

use crate::types::CommandResponse;
use std::collections::BTreeMap;

fn open(dir: &std::path::Path) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        1 << 22,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    );
    engine.load_shard(1);
    engine
}

fn run(engine: &TemporalEngine, command: Command) -> CommandResponse {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(response.status.ok, "{:?}", response.status);
    response.response
}

fn write_everything(engine: &TemporalEngine) -> Vec<String> {
    let mut keys = Vec::new();
    for index in 0..6 {
        let key = format!("s-{index}");
        run(engine, Command::StringSet { key: key.clone(), value: vec![b'a' + index as u8; 20] });
        keys.push(key);
    }
    for index in 0..8 {
        run(engine, Command::HashSet { key: "h".to_string(), field: format!("f{index}"), value: vec![b'h'; 12] });
        run(engine, Command::SetAdd { key: "set".to_string(), member: format!("m{index}").into_bytes() });
        run(engine, Command::ZSetAdd { key: "z".to_string(), member: format!("m{index}").into_bytes(), score: index as f64 });
        run(engine, Command::ListPush { key: "l".to_string(), member: format!("m{index}").into_bytes(), left: false });
    }
    keys.extend(["h", "set", "z", "l"].map(String::from));
    keys
}

fn read_everything(engine: &TemporalEngine, strings: usize) -> BTreeMap<String, Vec<Vec<u8>>> {
    let mut seen = BTreeMap::new();
    for index in 0..strings {
        let key = format!("s-{index}");
        if let CommandResponse::Bytes { value: Some(value) } = run(engine, Command::StringGet { key: key.clone() }) {
            seen.insert(key, vec![value]);
        }
    }
    if let CommandResponse::HashEntries { mut entries } = run(engine, Command::HashGetAll { key: "h".to_string() }) {
        entries.sort();
        seen.insert("h".to_string(), entries.into_iter().flat_map(|(f, v)| [f.into_bytes(), v]).collect());
    }
    if let CommandResponse::Members { mut members } = run(engine, Command::SetMembers { key: "set".to_string() }) {
        members.sort();
        seen.insert("set".to_string(), members);
    }
    if let CommandResponse::Members { members } =
        run(engine, Command::ZSetRange { key: "z".to_string(), start: 0, stop: -1, rev: false })
    {
        seen.insert("z".to_string(), members);
    }
    if let CommandResponse::Members { members } =
        run(engine, Command::ListRange { key: "l".to_string(), start: 0, stop: -1 })
    {
        seen.insert("l".to_string(), members);
    }
    seen
}

struct Flags {
    dirty_objects: usize,
    dirty_objects_with_a_clean_block: Vec<String>,
    dirty_buckets_in_plan: usize,
    buckets_holding_a_dirty_object: usize,
    buckets_the_plan_would_dump_among_those: usize,
}

fn flags(engine: &TemporalEngine, keys: &[String]) -> Flags {
    let plan = engine.storage_lifecycle_plan(crate::engine::reports::StorageLifecycleRequest {
        shard_id: 1,
        min_undumped_wal_records: 0,
        min_undumped_wal_bytes: 0,
        ..crate::engine::reports::StorageLifecycleRequest::default()
    });
    let shards = engine.shards.read().unwrap();
    let shard = shards.get(&1).unwrap();
    let (start, end) = shard.routing_range();
    let entries = crate::engine::storage_bucket_internals::collect_live_block_entries(shard);
    let mut with_clean = Vec::new();
    let mut dirty_buckets = std::collections::BTreeSet::new();
    for key in keys {
        if !shard.dirty_objects.contains(key) {
            continue;
        }
        dirty_buckets.insert(crate::engine::block_routing_bucket(key, start, end));
        if entries.iter().any(|e| e.object_key.as_ref() == key.as_str() && !e.deleted && !e.dirty) {
            with_clean.push(key.clone());
        }
    }
    let planned = dirty_buckets
        .iter()
        .filter(|bucket| plan.selected_dump_buckets.contains(bucket))
        .count();
    Flags {
        dirty_objects: keys.iter().filter(|key| shard.dirty_objects.contains(key)).count(),
        dirty_objects_with_a_clean_block: with_clean,
        dirty_buckets_in_plan: plan.dirty_buckets.len(),
        buckets_holding_a_dirty_object: dirty_buckets.len(),
        buckets_the_plan_would_dump_among_those: planned,
    }
}

fn scenario(mutation: impl Fn(&TemporalEngine)) -> (Flags, bool, BTreeMap<String, Vec<Vec<u8>>>, BTreeMap<String, Vec<Vec<u8>>>, usize) {
    let dir = tempfile::tempdir().unwrap();
    let (before, flags_after, reclaimed) = {
        let engine = open(dir.path());
        let keys = write_everything(&engine);
        let before = read_everything(&engine, 6);
        mutation(&engine);
        let flags_after = flags(&engine, &keys);
        // Dump what the plan selects, then reclaim the log the way the periodic loop does.
        let dumped = engine.apply_storage_lifecycle(crate::engine::reports::StorageLifecycleRequest {
            shard_id: 1,
            min_undumped_wal_records: 0,
            min_undumped_wal_bytes: 0,
            ..crate::engine::reports::StorageLifecycleRequest::default()
        });
        let dumped_buckets = dumped.dump_manifest.as_ref().map(|m| m.bucket_ids.len()).unwrap_or(0);
        let plan = engine.storage_wal_reclaim_plan(1, Vec::new(), Vec::new());
        let reclaim = engine.apply_storage_wal_reclaim(plan);
        let _ = dumped_buckets;
        (before, flags_after, reclaim.wal_records_removed as usize)
    };
    // NO UNLOAD: recovery from the dumps and what is left of the log.
    let engine = open(dir.path());
    let after = read_everything(&engine, 6);
    (flags_after, reclaimed > 0, before, after, reclaimed)
}

#[test]
fn a_compaction_round_does_not_lose_a_dirty_objects_dump() {
    let (control_flags, control_reclaimed, control_before, control_after, control_removed) = scenario(|_| {});
    let (flags, reclaimed, before, after, removed) = scenario(|engine| {
        let report = engine.compact_shard_blocks(1);
        assert!(report.is_ok(), "{:?}", report.err());
    });
    eprintln!(
        "CONTROL (no compaction): dirty objects {} / with a clean block {:?} / plan dirty buckets {} / buckets holding a dirty object {} of which planned {} / log records reclaimed {}",
        control_flags.dirty_objects, control_flags.dirty_objects_with_a_clean_block, control_flags.dirty_buckets_in_plan,
        control_flags.buckets_holding_a_dirty_object, control_flags.buckets_the_plan_would_dump_among_those, control_removed
    );
    eprintln!(
        "COMPACTED: dirty objects {} / with a clean block {:?} / plan dirty buckets {} / buckets holding a dirty object {} of which planned {} / log records reclaimed {}",
        flags.dirty_objects, flags.dirty_objects_with_a_clean_block, flags.dirty_buckets_in_plan,
        flags.buckets_holding_a_dirty_object, flags.buckets_the_plan_would_dump_among_those, removed
    );
    // THE CONTROL IS NOT THE SUBJECT: without a compaction the sequence must recover everything.
    assert_eq!(control_before, control_after, "the control lost a value, so the fixture is not sound");
    assert!(control_reclaimed, "the control reclaimed no log, so the sequence does not reach the loss");
    // THE SUBJECT, loss first: what recovery serves is the question; the plan is the mechanism.
    let lost = before != after;
    eprintln!("LOSS after compaction, dump, log reclaim and recovery: {lost}");
    if lost {
        for (key, value) in &before {
            if after.get(key) != Some(value) {
                eprintln!("LOST OR STALE: {key}: before {} items, after {:?} items", value.len(), after.get(key).map(|v| v.len()));
            }
        }
    }
    assert!(reclaimed, "the compacted run reclaimed no log, so the sequence does not reach the loss");
    assert_eq!(before, after, "a value was lost or stale after compaction, dump, log reclaim and recovery");
    // THE MECHANISM, stated rather than assumed: the round leaves the dirty set populated and the
    // dump plan empty, and it is safe only because it persisted the index. If a change makes the
    // plan select these buckets again that is fine, and this stays green; if a change makes the plan
    // skip them while the round no longer persists, the recovery comparison above is what fails.
    assert!(
        flags.buckets_the_plan_would_dump_among_those == flags.buckets_holding_a_dirty_object
            || flags.buckets_the_plan_would_dump_among_those == 0,
        "the dump plan selected some but not all of the buckets that hold a dirty object"
    );
}

/// The other door onto the same defect: a write that names no object key. The write path answers it
/// by rebuilding the shard's bucket ownership, and the rebuild files every block clean.
#[test]
fn a_write_that_names_no_key_does_not_lose_a_dirty_objects_dump() {
    let (control_flags, _, control_before, control_after, _) = scenario(|_| {});
    assert_eq!(control_before, control_after, "the control lost a value, so the fixture is not sound");
    assert!(control_flags.dirty_objects > 0);
    let (flags, reclaimed, before, after, removed) = scenario(|engine| {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextResourceBlobPut {
                tenant_hash: 7,
                payload: vec![1, 2, 3, 4],
            },
        });
        assert!(response.status.ok, "{:?}", response.status);
    });
    eprintln!(
        "KEYLESS WRITE: dirty objects {} / with a clean block {:?} / plan dirty buckets {} / buckets holding a dirty object {} of which planned {} / log records reclaimed {}",
        flags.dirty_objects, flags.dirty_objects_with_a_clean_block, flags.dirty_buckets_in_plan,
        flags.buckets_holding_a_dirty_object, flags.buckets_the_plan_would_dump_among_those, removed
    );
    eprintln!("LOSS after a keyless write, dump, log reclaim and recovery: {}", before != after);
    assert!(reclaimed, "the run reclaimed no log, so the sequence does not reach the loss");
    assert_eq!(before, after, "a value was lost or stale after a keyless write, dump, log reclaim and recovery");
    assert_eq!(
        flags.buckets_the_plan_would_dump_among_those, flags.buckets_holding_a_dirty_object,
        "after a keyless write the dump plan skips a bucket that holds a dirty object"
    );
}
