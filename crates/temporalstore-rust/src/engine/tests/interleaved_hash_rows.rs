// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use crate::types::{
    BatchHashMultiSetValue, BatchHashTimestampSetRequest, BatchHashTimestampSetRow,
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

fn open(path: &std::path::Path, asynchronous: bool) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        path.join("cache"),
        path.join("blocks"),
        path.join("indexes"),
    );
    engine.load_shard(1);
    assert!(
        engine
            .set_config(crate::SetConfigRequest {
                shard_id: 1,
                config: Config {
                    version: 2,
                    async_storage: asynchronous,
                    ..Config::default()
                }
            })
            .ok
    );
    engine
}

fn body(rows: usize, width: usize) -> Vec<u8> {
    let request = BatchHashTimestampSetRequest {
        shard_id: 1,
        fields: vec!["value".into(), "timestamp".into()],
        timestamp_field: "timestamp".into(),
        ttl_ms: None,
        rows: (0..rows)
            .map(|row| {
                BatchHashTimestampSetRow(
                    format!("row-{row}"),
                    20,
                    0,
                    vec![
                        BatchHashMultiSetValue(vec![row as u8; width]),
                        BatchHashMultiSetValue(vec![8, 20]),
                    ],
                )
            })
            .collect(),
    };
    rmp_serde::to_vec_named(&request).unwrap()
}

fn apply(engine: &TemporalEngine, body: &[u8]) -> BatchConditionalWriteResponse {
    engine
        .batch_execute_interleaved_hash_rows(rmp_serde::from_slice(body).unwrap(), || Ok(()))
        .0
}

fn read(engine: &TemporalEngine, rows: usize, width: usize) {
    let result = engine.batch_hash_multi_get_compact(BatchHashMultiGetRequest {
        shard_id: 1,
        keys: (0..rows).map(|row| format!("row-{row}")).collect(),
        fields: vec!["value".into()],
    });
    assert!(result.status.ok && result.errors.is_empty(), "{result:?}");
    assert_eq!(result.rows.len(), rows);
    for (index, row) in result.rows.iter().enumerate() {
        assert_eq!(row.0, vec![Some(vec![index as u8; width])]);
    }
}

#[test]
fn interleaved_rows_release_admission_and_register_before_each_slice_returns() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path(), false);
    let observed = Arc::new(AtomicUsize::new(0));
    let visits = observed.clone();
    AFTER_SLICE.with(|slot| {
        *slot.borrow_mut() = Some(Box::new(move |engine, _, count| {
            assert!(matches!(engine.shards.handle(1).expect("shard is loaded").try_write_loaded_for_test(), Ok(true)));
            read(engine, count, 48);
            let future = engine.batch_hash_multi_get_compact(BatchHashMultiGetRequest {
                shard_id: 1,
                keys: vec![format!("row-{count}")],
                fields: vec!["value".into()],
            });
            assert!(future.status.ok);
            assert_eq!(future.rows[0].0, vec![None]);
            visits.fetch_add(1, Ordering::Relaxed);
        }))
    });
    let response = apply(&engine, &body(130, 48));
    AFTER_SLICE.with(|slot| {
        slot.borrow_mut().take();
    });
    assert!(response.status.ok && response.errors.is_empty());
    assert_eq!((response.applied, response.skipped), (130, 0));
    assert_eq!(observed.load(Ordering::Relaxed), 3);
    let names: std::collections::HashSet<_> = engine.shards.with_state(1, |shard| {
        let shard = shard.expect("shard 1 is loaded");
        let mut names = std::collections::HashSet::new();
        for row in 0..130 {
            let fields = shard
                .bucket_index
                .hashes
                .get(&format!("row-{row}"))
                .unwrap();
            for name in fields.keys() {
                names.insert(Arc::as_ptr(name) as *const () as usize);
            }
        }
        names
    });
    assert_eq!(names.len(), 2, "field owners are shared across slices");
    let stats = engine.wal_store.stats_without_rescan(1);
    assert_eq!((stats.writes, stats.syncs), (3, 1));
    assert_eq!(stats.last_sequence, stats.last_flushed_sequence);
    drop(engine);
    read(&open(dir.path(), false), 130, 48);
}

#[test]
fn interleaved_stale_rows_do_not_add_records_or_barriers() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path(), false);
    let body = body(130, 48);
    assert_eq!(apply(&engine, &body).applied, 130);
    let before = engine.wal_store.stats_without_rescan(1);
    let result = apply(&engine, &body);
    assert!(result.status.ok && result.errors.is_empty());
    assert_eq!((result.applied, result.skipped), (0, 130));
    let after = engine.wal_store.stats_without_rescan(1);
    assert_eq!((before.writes, before.syncs), (after.writes, after.syncs));
}

#[test]
fn interleaved_byte_budget_splits_large_rows_without_splitting_a_row() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path(), false);
    let result = apply(&engine, &body(3, MAX_SLICE_BYTES));
    assert!(result.status.ok && result.errors.is_empty());
    let stats = engine.wal_store.stats_without_rescan(1);
    assert_eq!((stats.writes, stats.syncs), (3, 1));
    read(&engine, 3, MAX_SLICE_BYTES);
}

#[test]
fn interleaved_lifecycle_rejection_still_commits_the_appended_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path(), false);
    let mut slices = 0;
    let body = body(130, 48);
    let result = engine
        .batch_execute_interleaved_hash_rows(rmp_serde::from_slice(&body).unwrap(), || {
            slices += 1;
            if slices == 2 {
                Err(Status::error("lifecycle_write_blocked", "injected unload"))
            } else {
                Ok(())
            }
        })
        .0;
    assert!(!result.status.ok);
    assert_eq!(result.status.code, "lifecycle_write_blocked");
    let stats = engine.wal_store.stats_without_rescan(1);
    assert_eq!((stats.writes, stats.syncs), (1, 1));
    assert_eq!(stats.last_sequence, stats.last_flushed_sequence);
    drop(engine);
    read(&open(dir.path(), false), 64, 48);
}

#[test]
fn interleaved_barrier_failure_fences_serving_and_never_acknowledges_rows() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path(), false);
    BEFORE_SHARED_ROW_COMMIT.with(|slot| {
        *slot.borrow_mut() = Some(Box::new(|engine, shard, sequence| {
            assert_eq!(sequence, 3);
            assert!(matches!(engine.shards.handle(shard).expect("shard is loaded").try_write_loaded_for_test(), Ok(true)), "WAL barrier must run outside the shard guard");
            let path = engine.wal_store.info(shard).unwrap().path;
            std::fs::rename(&path, path.with_extension("saved")).unwrap();
            std::fs::create_dir(path).unwrap();
        }))
    });
    let result = apply(&engine, &body(130, 48));
    assert!(!result.status.ok);
    assert_eq!(result.status.code, "wal_commit_failed");
    assert_eq!((result.applied, result.skipped), (0, 0));
    assert!(engine.infos.read().unwrap()[&1].recovering);
    let read = engine.batch_hash_multi_get_compact(BatchHashMultiGetRequest {
        shard_id: 1,
        keys: vec!["row-0".into()],
        fields: vec!["value".into()],
    });
    assert!(!read.status.ok);
}

#[test]
fn interleaved_async_configuration_does_not_gain_a_sync_barrier() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path(), true);
    let result = apply(&engine, &body(130, 48));
    assert!(result.status.ok && result.errors.is_empty());
    let stats = engine.wal_store.stats_without_rescan(1);
    assert_eq!((stats.writes, stats.syncs), (3, 0));
    read(&engine, 130, 48);
}

#[test]
fn interleaved_row_errors_keep_original_request_indices() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path(), false);
    AFTER_SLICE.with(|slot| {
        *slot.borrow_mut() = Some(Box::new(|engine, shard, count| {
            if count == 64 {
                engine
                    .infos
                    .write()
                    .unwrap()
                    .get_mut(&shard)
                    .unwrap()
                    .readonly = true;
            }
        }))
    });
    let result = apply(&engine, &body(130, 48));
    AFTER_SLICE.with(|slot| {
        slot.borrow_mut().take();
    });
    assert!(result.status.ok);
    assert_eq!((result.applied, result.skipped), (64, 0));
    assert_eq!(
        result
            .errors
            .iter()
            .map(|error| error.index)
            .collect::<Vec<_>>(),
        (64..130).collect::<Vec<_>>()
    );
    assert!(result
        .errors
        .iter()
        .all(|error| error.status.code == "readonly_shard"));
    let stats = engine.wal_store.stats_without_rescan(1);
    assert_eq!((stats.writes, stats.syncs), (1, 1));
}

#[test]
fn original_atomic_route_keeps_one_record_for_a_large_batch() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path(), false);
    let body = body(130, 48);
    let result = engine
        .batch_execute_borrowed_hash_rows(rmp_serde::from_slice(&body).unwrap())
        .0;
    assert!(result.status.ok && result.errors.is_empty());
    assert_eq!(result.applied, 130);
    let stats = engine.wal_store.stats_without_rescan(1);
    assert_eq!((stats.writes, stats.syncs), (1, 1));
}
