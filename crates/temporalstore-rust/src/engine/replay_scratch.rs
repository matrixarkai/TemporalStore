// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

#[derive(Clone, Copy, Debug, Default)]
struct Counts {
    windows: usize,
    window_records: usize,
    applied_records: usize,
    placements_peak: usize,
    placement_capacity_peak: usize,
    outcomes_peak: usize,
    suppressed_outcomes: usize,
}

thread_local! {
    static COUNTS: std::cell::Cell<Counts> = const { std::cell::Cell::new(Counts {
        windows: 0, window_records: 0, applied_records: 0, placements_peak: 0,
        placement_capacity_peak: 0, outcomes_peak: 0, suppressed_outcomes: 0,
    }) };
}

pub(super) fn observe_window(records: usize) {
    COUNTS.with(|cell| {
        let mut counts = cell.get();
        counts.windows += 1;
        counts.applied_records += records;
        counts.window_records = counts.window_records.max(records);
        cell.set(counts);
    });
}

pub(super) fn observe_placements(placements: usize, capacity: usize) {
    COUNTS.with(|cell| {
        let mut counts = cell.get();
        counts.placements_peak = counts.placements_peak.max(placements);
        counts.placement_capacity_peak = counts.placement_capacity_peak.max(capacity);
        cell.set(counts);
    });
}

pub(super) fn observe_staging() {
    COUNTS.with(|cell| {
        let mut counts = cell.get();
        counts.outcomes_peak = counts
            .outcomes_peak
            .max(super::block_in_wal::staged_outcome_count());
        cell.set(counts);
    });
}

pub(super) fn observe_suppressed_outcome() {
    COUNTS.with(|cell| {
        let mut counts = cell.get();
        counts.suppressed_outcomes += 1;
        cell.set(counts);
    });
}

fn engine_at(dir: &std::path::Path, cache: &str) -> TemporalEngine {
    TemporalEngine::with_local_dirs(
        64 << 20,
        dir.join(cache),
        dir.join("blocks"),
        dir.join("indexes"),
    )
}

fn replay_case(records: usize, keys: usize, legacy: bool, window_bytes: u64) -> Counts {
    const SHARD: u64 = 37;
    let dir = tempfile::tempdir().unwrap();
    let (info, log_records) = {
        let writer = engine_at(dir.path(), "writer-cache");
        writer.load_shard(SHARD);
        for index in 0..records {
            assert!(
                writer
                    .execute_replicated(
                        ExecuteRequest {
                            shard_id: SHARD,
                            command: Command::StringSet {
                                key: format!("key-{}", index % keys),
                                value: (index as u64).to_le_bytes().to_vec(),
                            },
                        }
                        .with_async_storage()
                    )
                    .status
                    .ok
            );
        }
        let info = writer.infos.read().unwrap()[&SHARD].clone();
        let records = if legacy {
            writer
                .wal_store
                .scan_decoded(SHARD, 0, u64::MAX, u64::MAX)
                .unwrap()
                .0
        } else {
            Vec::new()
        };
        (info, records)
    };
    if legacy {
        let wal_dir = dir.path().join("indexes").join("wals");
        std::fs::remove_dir_all(&wal_dir).unwrap();
        let wal = crate::wal::LocalWriteAheadLogStore::new(wal_dir);
        for (index, (_, mut record)) in log_records.into_iter().enumerate() {
            assert_eq!(record.sequence, index as u64 + 1);
            record.outcomes.clear();
            record.command = Some(Command::StringSet {
                key: format!("key-{}", index % keys),
                value: (index as u64).to_le_bytes().to_vec(),
            });
            wal.append_replayed_record(record).unwrap();
        }
    }
    let reader = engine_at(dir.path(), "reader-cache");
    reader.infos.write().unwrap().insert(SHARD, info);
    reader.install_shard_state(SHARD, ShardState::default());
    COUNTS.with(|cell| cell.set(Counts::default()));
    reader
        .replay_wal_into_shard_windowed(SHARD, 0, window_bytes)
        .unwrap();
    let counts = COUNTS.with(|cell| cell.get());
    assert_eq!(reader.shards.read().unwrap()[&SHARD].strings().len(), keys);
    assert_eq!(super::block_in_wal::staged_outcome_count(), 0);
    drop(reader);

    let reopened = engine_at(dir.path(), "reopened-cache");
    COUNTS.with(|cell| cell.set(Counts::default()));
    reopened.load_shard(SHARD);
    assert_eq!(
        COUNTS.with(|cell| cell.get()).applied_records,
        0,
        "the second load must not re-execute covered commands"
    );
    assert_eq!(
        reopened
            .replay_installs
            .load(std::sync::atomic::Ordering::Relaxed),
        0,
        "the second load must use the recovered index, not replay over missing placements"
    );
    for key in 0..keys {
        let last = records - 1 - (records - 1 - key) % keys;
        assert_eq!(
            reopened
                .execute(ExecuteRequest {
                    shard_id: SHARD,
                    command: Command::StringGet {
                        key: format!("key-{key}")
                    },
                })
                .response,
            CommandResponse::Bytes {
                value: Some((last as u64).to_le_bytes().to_vec())
            }
        );
    }
    eprintln!("REPLAY_SCRATCH records={records} keys={keys} legacy={legacy} counts={counts:?}");
    assert!(counts.windows > 1 && counts.window_records > 0);
    assert_eq!(counts.applied_records, records);
    assert!(counts.placements_peak > 0 && counts.suppressed_outcomes >= records);
    assert!(
        counts.placements_peak <= counts.window_records,
        "placement scratch retained history beyond one window: {counts:?}"
    );
    assert!(
        counts.placements_peak <= keys,
        "placement scratch retained superseded locations: {counts:?}"
    );
    assert!(
        counts.placement_capacity_peak <= 4 * counts.window_records,
        "placement allocation retained history beyond one window: {counts:?}"
    );
    assert_eq!(
        counts.outcomes_peak, 0,
        "replay staged outgoing outcomes: {counts:?}"
    );
    counts
}

#[test]
fn replay_scratch_is_bounded_by_the_window_not_the_tail() {
    for (records, keys, legacy) in [
        (128usize, 1usize, false),
        (512, 1, false),
        (128, 128, false),
        (512, 128, false),
        (128, 128, true),
    ] {
        replay_case(records, keys, legacy, 256);
    }
}

#[test]
#[ignore = "large WAL recovery measurement"]
fn replay_scratch_history_at_scale() {
    let records = std::env::var("REPLAY_SCRATCH_RECORDS")
        .ok()
        .map(|value| value.parse::<usize>().unwrap())
        .unwrap_or(64_000);
    assert!((1_024..=200_000).contains(&records));
    replay_case(records, 128, false, 512 * 1024);
}

#[test]
fn missing_wal_is_refused_when_a_checkpoint_still_names_wal_blocks() {
    const SHARD: u64 = 37;
    let source = tempfile::tempdir().unwrap();
    let writer = engine_at(source.path(), "writer-cache");
    writer.load_shard(SHARD);
    assert!(writer.execute_replicated(ExecuteRequest {
        shard_id: SHARD,
        command: Command::StringSet {
            key: "wal-backed".to_string(),
            value: b"needs its log".to_vec(),
        },
    }.with_async_storage()).status.ok);
    let state = writer.shards.read().unwrap()[&SHARD].clone();
    assert!(!state.wal_resident_blocks.is_empty());
    assert!(state.bucket_index.bucket_map.values().any(|bucket| {
        bucket.block_index.values().any(|block| {
            crate::wal_record::is_wal_resident(block.address.block_slab_id())
        })
    }));
    let target = tempfile::tempdir().unwrap();
    let reader = engine_at(target.path(), "reader-cache");
    reader.install_shard_state(SHARD, state);
    assert_eq!(
        reader.rehydrate_wal_resident_blocks(SHARD).unwrap_err().code,
        "wal_resident_block_missing"
    );
}

#[test]
fn replay_clears_staging_on_empty_and_refused_tails() {
    const SHARD: u64 = 37;
    for refused in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine_at(dir.path(), "cache");
        engine.load_shard(SHARD);
        if refused {
            assert!(
                engine
                    .execute_replicated(
                        ExecuteRequest {
                            shard_id: SHARD,
                            command: Command::StringSet {
                                key: "good".to_string(),
                                value: vec![7]
                            },
                        }
                        .with_async_storage()
                    )
                    .status
                    .ok
            );
            let mut record = engine
                .wal_store
                .scan_decoded(SHARD, 0, u64::MAX, u64::MAX)
                .unwrap()
                .0
                .pop()
                .unwrap()
                .1;
            let mut invalid = record
                .outcomes
                .iter()
                .find(|item| item.kind == "string")
                .unwrap()
                .clone();
            invalid.kind = "hash".to_string();
            invalid.address = None;
            invalid.value = None;
            invalid.element_name = None;
            record.sequence += 1;
            record.command = None;
            record.outcomes = vec![invalid];
            engine.wal_store.append_replayed_record(record).unwrap();
        }
        super::block_in_wal::begin_write();
        super::block_in_wal::stage(999, None, b"abandoned staging");
        let result = engine.replay_wal_into_shard_windowed(SHARD, 0, 256);
        if refused {
            assert_eq!(result.unwrap_err().code, "wal_replay_outcome_refused");
        } else {
            result.unwrap();
        }
        assert_eq!(super::block_in_wal::staged_outcome_count(), 0);
        assert!(
            super::block_in_wal::take_staged().is_empty(),
            "replay left staged block bytes"
        );
    }
}
