// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use crate::types::{
    BatchHashMultiSetValue, BatchHashTimestampSetRequest, BatchHashTimestampSetRow,
};
use std::sync::{Arc, Condvar, Mutex};

#[derive(Clone, PartialEq, prost::Message)]
struct Timestamp {
    #[prost(int64, tag = "1")]
    seconds: i64,
    #[prost(int32, tag = "2")]
    nanos: i32,
}

fn open(path: &std::path::Path, asynchronous: bool) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        32 * 1024 * 1024,
        path.join("cache"),
        path.join("blocks"),
        path.join("indexes"),
    );
    engine.load_shard(1);
    assert!(
        engine
            .set_config(crate::control::SetConfigRequest {
                shard_id: 1,
                config: Config {
                    version: 2,
                    async_storage: asynchronous,
                    ..Config::default()
                },
            })
            .ok
    );
    engine
}

fn request(prefix: &str) -> BatchHashTimestampSetRequest {
    use prost::Message;
    let timestamp = Timestamp {
        seconds: 20,
        nanos: 7,
    }
    .encode_to_vec();
    BatchHashTimestampSetRequest {
        shard_id: 1,
        fields: vec!["value".into(), "timestamp".into()],
        timestamp_field: "timestamp".into(),
        ttl_ms: None,
        rows: (0..2)
            .map(|row| {
                BatchHashTimestampSetRow(
                    format!("{prefix}-{row}"),
                    20,
                    7,
                    vec![
                        BatchHashMultiSetValue(vec![row as u8 + 1; 8192]),
                        BatchHashMultiSetValue(timestamp.clone()),
                    ],
                )
            })
            .collect(),
    }
}

fn apply(engine: &TemporalEngine, prefix: &str, borrowed: bool) -> BatchConditionalWriteResponse {
    let request = request(prefix);
    if borrowed {
        let body = rmp_serde::to_vec_named(&request).unwrap();
        let request = rmp_serde::from_slice(&body).unwrap();
        engine.batch_execute_borrowed_hash_rows(request).0
    } else {
        engine.batch_execute_packed_hash_rows(request).0
    }
}

fn verify(engine: &TemporalEngine, prefix: &str) {
    let response = engine.batch_hash_multi_get_compact(BatchHashMultiGetRequest {
        shard_id: 1,
        keys: (0..2).map(|row| format!("{prefix}-{row}")).collect(),
        fields: vec!["value".into()],
    });
    assert!(response.status.ok && response.errors.is_empty());
    for row in 0..2 {
        assert_eq!(response.rows[row].0, vec![Some(vec![row as u8 + 1; 8192])]);
    }
}

#[test]
fn shared_rows_register_payloads_and_release_guard_before_barrier() {
    for borrowed in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let engine = open(dir.path(), false);
        BEFORE_SHARED_ROW_COMMIT.with(|slot| {
            *slot.borrow_mut() = Some(Box::new(|engine, shard, sequence| {
                assert!(engine.shards.try_write().is_ok());
                let stats = engine.wal_store.stats_without_rescan(shard);
                assert_eq!(
                    (stats.writes, stats.syncs, stats.last_flushed_sequence),
                    (1, 0, 0)
                );
                assert_eq!(sequence, 1);
                // Matches single-command concurrent commit: in-flight state is readable,
                // but the write has not yet received a durable acknowledgement.
                verify(engine, "row");
            }))
        });
        let result = apply(&engine, "row", borrowed);
        assert!(result.status.ok && result.errors.is_empty());
        assert_eq!((result.applied, result.skipped), (2, 0));
        assert!(BEFORE_SHARED_ROW_COMMIT.with(|slot| slot.borrow().is_none()));
        let stats = engine.wal_store.stats_without_rescan(1);
        assert_eq!(
            (stats.writes, stats.syncs, stats.last_flushed_sequence),
            (1, 1, 1)
        );
        drop(engine);
        let reopened = open(dir.path(), false);
        verify(&reopened, "row");
    }
}

#[test]
fn concurrent_shared_batches_share_one_barrier_and_recover_both_records() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(open(dir.path(), false));
    let rendezvous = Arc::new((Mutex::new(0usize), Condvar::new()));
    let mut threads = Vec::new();
    for index in 0..2 {
        let engine = engine.clone();
        let rendezvous = rendezvous.clone();
        threads.push(std::thread::spawn(move || {
            BEFORE_SHARED_ROW_COMMIT.with(|slot| {
                *slot.borrow_mut() = Some(Box::new(move |_, _, _| {
                    let (mutex, condition) = &*rendezvous;
                    let mut arrived = mutex.lock().unwrap();
                    *arrived += 1;
                    condition.notify_all();
                    let (arrived, _) = condition
                        .wait_timeout_while(
                            arrived,
                            std::time::Duration::from_secs(10),
                            |arrived| *arrived < 2,
                        )
                        .unwrap();
                    assert_eq!(
                        *arrived, 2,
                        "another writer must append while its peer awaits commit"
                    );
                }))
            });
            let result = apply(&engine, &format!("writer-{index}"), index == 1);
            assert!(result.status.ok && result.errors.is_empty());
            assert_eq!(result.applied, 2);
        }));
    }
    for thread in threads {
        thread.join().unwrap();
    }
    let stats = engine.wal_store.stats_without_rescan(1);
    assert_eq!(
        (stats.writes, stats.syncs, stats.last_flushed_sequence),
        (2, 1, 2)
    );
    drop(engine);
    let reopened = open(dir.path(), false);
    verify(&reopened, "writer-0");
    verify(&reopened, "writer-1");
}

#[test]
fn shared_batch_barrier_failure_never_acknowledges_applied_rows() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path(), false);
    BEFORE_SHARED_ROW_COMMIT.with(|slot| {
        *slot.borrow_mut() = Some(Box::new(|engine, shard, _| {
            let path = engine.wal_store.info(shard).unwrap().path;
            std::fs::rename(&path, path.with_extension("saved")).unwrap();
            std::fs::create_dir(path).unwrap();
        }))
    });
    let result = apply(&engine, "failed", true);
    assert!(!result.status.ok);
    assert_eq!(result.status.code, "wal_commit_failed");
    assert!(result.status.message.contains("commit barrier failed"));
    assert_eq!((result.applied, result.skipped), (0, 0));
    assert_eq!(engine.wal_store.stats_without_rescan(1).syncs, 0);
}

#[test]
fn async_and_disabled_concurrent_commit_keep_their_original_paths() {
    for asynchronous in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let engine = open(dir.path(), asynchronous);
        if !asynchronous {
            engine
                .concurrent_commit
                .store(false, std::sync::atomic::Ordering::Relaxed);
        }
        BEFORE_SHARED_ROW_COMMIT.with(|slot| {
            *slot.borrow_mut() = Some(Box::new(|_, _, _| {
                panic!("this path must not defer a batch barrier");
            }))
        });
        let result = apply(&engine, "row", true);
        assert!(BEFORE_SHARED_ROW_COMMIT.with(|slot| slot.borrow_mut().take().is_some()));
        assert!(result.status.ok && result.errors.is_empty());
        assert_eq!(result.applied, 2);
        let stats = engine.wal_store.stats_without_rescan(1);
        assert_eq!((stats.writes, stats.syncs), (1, u64::from(!asynchronous)));
        verify(&engine, "row");
    }
}

#[test]
fn stale_shared_rows_do_not_add_a_barrier() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path(), false);
    assert!(apply(&engine, "row", false).status.ok);
    BEFORE_SHARED_ROW_COMMIT.with(|slot| {
        *slot.borrow_mut() = Some(Box::new(|_, _, _| {
            panic!("stale rows must not enter the commit queue");
        }))
    });
    let result = apply(&engine, "row", true);
    assert!(BEFORE_SHARED_ROW_COMMIT.with(|slot| slot.borrow_mut().take().is_some()));
    assert!(result.status.ok && result.errors.is_empty());
    assert_eq!((result.applied, result.skipped), (0, 2));
    let stats = engine.wal_store.stats_without_rescan(1);
    assert_eq!((stats.writes, stats.syncs), (1, 1));
}

#[test]
fn index_persisting_shared_rows_keep_the_in_guard_barrier() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path(), false);
    assert!(
        engine
            .set_config(crate::control::SetConfigRequest {
                shard_id: 1,
                config: Config {
                    version: 3,
                    async_storage: false,
                    maxmemory_bytes: Some(1 << 30),
                    ..Config::default()
                },
            })
            .ok
    );
    BEFORE_SHARED_ROW_COMMIT.with(|slot| {
        *slot.borrow_mut() = Some(Box::new(|_, _, _| {
            panic!("index-persisting batches retain their original commit ordering");
        }))
    });
    let result = apply(&engine, "row", false);
    assert!(BEFORE_SHARED_ROW_COMMIT.with(|slot| slot.borrow_mut().take().is_some()));
    assert!(result.status.ok && result.errors.is_empty());
    assert_eq!(result.applied, 2);
    verify(&engine, "row");
}
