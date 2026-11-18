// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A FEATURE RANGE READ SHOULD NOT TAKE THE EXCLUSIVE SHARD LOCK.
//!
//! `FeatureQuery` and `FeatureQueryFiltered` took the exclusive shard table lock, so every range read
//! waited for every writer and for every other reader: measured on one engine, a ten-point range read
//! did 88k, 91k and 96k reads a second at 1, 4 and 16 threads, where a string point read on the shared
//! lock did 581k, 1.04M and 2.32M. They now read under the shared lock like `StringGet`: the window's
//! addresses are taken under the guard and the points are read after it drops, without touching the
//! response cache (a reader that filled it after the guard dropped could store an answer older than a
//! write that had already invalidated the key).
//!
//! TWO ARMS.
//!   * `each_range_read_answers_while_the_shared_lock_is_held` -- the test holds the shard table's
//!     shared lock and each read must answer from another thread.
//!   * `a_range_read_answers_what_was_appended` -- both reads against a model over windows, caps and
//!     filters, after a replaced instant, a trim and a lapsed key.
//!
//! rust-internal: measures this crate's own request paths, no product behaviour

#![allow(clippy::all)]
use super::*;

use crate::types::{FeatureFilter, FeatureFilterOp, FeaturePoint, SequenceFeatureRow};
use std::collections::BTreeMap;
use std::sync::{mpsc, Arc};
use std::time::Duration;

const BASE: u64 = 1_787_270_070_000;

fn engine_on(dir: &std::path::Path, bound: usize) -> Arc<TemporalEngine> {
    let engine = Arc::new(TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    ));
    engine.load_shard(1);
    assert!(
        engine
            .set_config(crate::control::SetConfigRequest {
                shard_id: 1,
                config: crate::control::Config {
                    version: 2,
                    feature_max_size: bound,
                    ..Default::default()
                },
            })
            .ok
    );
    engine
}

fn row_point(index: u64, gid: u64) -> FeaturePoint {
    let row = SequenceFeatureRow {
        timestamp_ms: BASE + index * 1_000,
        gid,
        action_type: (gid % 5) as u32 + 1,
        duration: 3 + (gid % 11) as u32,
        author_id: 9_000 + gid,
    };
    FeaturePoint {
        timestamp_ms: row.timestamp_ms,
        value: row.encode_feature_proto_value(),
    }
}

fn run(engine: &TemporalEngine, command: Command) -> CommandResponse {
    let outcome = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(outcome.status.ok, "{:?}", outcome.response);
    outcome.response
}

fn points_of(response: CommandResponse) -> Vec<FeaturePoint> {
    match response {
        CommandResponse::FeaturePoints { points } => points,
        other => panic!("not points: {other:?}"),
    }
}

fn append(engine: &TemporalEngine, points: Vec<FeaturePoint>) {
    run(
        engine,
        Command::FeatureAppend {
            key: "s".to_string(),
            points,
        },
    );
}

fn reads() -> Vec<(&'static str, Command)> {
    vec![
        (
            "FeatureQuery",
            Command::FeatureQuery {
                key: "s".into(),
                start_ms: 0,
                end_ms: u64::MAX,
                count: None,
            },
        ),
        (
            "FeatureQuery capped",
            Command::FeatureQuery {
                key: "s".into(),
                start_ms: 0,
                end_ms: u64::MAX,
                count: Some(5),
            },
        ),
        (
            "FeatureQueryFiltered",
            Command::FeatureQueryFiltered {
                key: "s".into(),
                start_ms: 0,
                end_ms: u64::MAX,
                count: None,
                filters: vec![FeatureFilter {
                    field: "action_type".into(),
                    op: FeatureFilterOp::Equal,
                    value: 1,
                }],
            },
        ),
        (
            "FeatureQuery of an absent key",
            Command::FeatureQuery {
                key: "nope".into(),
                start_ms: 0,
                end_ms: u64::MAX,
                count: None,
            },
        ),
    ]
}

#[test]
fn each_range_read_answers_while_the_shared_lock_is_held() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path(), 100_000);
    append(&engine, (0..60).map(|i| row_point(i, i)).collect());
    let mut answered = 0usize;
    for (label, command) in reads() {
        let held = engine.shards.read().expect("engine lock poisoned");
        let (sender, receiver) = mpsc::channel();
        let worker = {
            let engine = Arc::clone(&engine);
            std::thread::spawn(move || {
                let _ = sender.send(run(&engine, command));
            })
        };
        let got = receiver.recv_timeout(Duration::from_secs(5));
        drop(held);
        worker.join().expect("the read thread");
        assert!(
            got.is_ok(),
            "{label}: no answer while the shared lock was held"
        );
        answered += 1;
    }
    assert_eq!(reads().len(), answered);
}

#[test]
fn a_captured_feature_window_survives_delete_and_reclaim() {
    for filtered in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine_on(dir.path(), 100_000);
        assert!(
            engine
                .set_config(crate::control::SetConfigRequest {
                    shard_id: 1,
                    config: crate::control::Config {
                        version: 3,
                        async_storage: true,
                        feature_max_size: 100_000,
                        ..Default::default()
                    },
                })
                .ok
        );
        let expected = (0..12)
            .map(|index| row_point(index, index))
            .collect::<Vec<_>>();
        append(&engine, expected.clone());
        {
            let shards = engine.shards.read().unwrap();
            let window = shards[&1]
                .features()
                .get("s")
                .unwrap()
                .range(0..=u64::MAX)
                .collect::<Vec<_>>();
            assert_eq!(window.len(), expected.len());
            assert!(window.iter().all(|(_, address)| {
                crate::wal_record::is_wal_resident(address.block_slab_id())
            }));
        }
        engine.cache.invalidate_shard(1).unwrap();
        let writer = Arc::clone(&engine);
        crate::engine::AFTER_FAST_READ_PLAN.with(|slot| {
            assert!(slot.borrow().is_none());
            *slot.borrow_mut() = Some(Box::new(move || {
                assert!(writer.shards.try_write().is_ok());
                std::thread::spawn(move || {
                    run(&writer, Command::CommonDelete { key: "s".into() });
                    writer.cache.invalidate_shard(1).unwrap();
                    let last = writer.wal_store.stats(1).last_sequence;
                    writer.wal_store.clear_block_retention_floor(1);
                    let reclaimed = writer
                        .wal_store
                        .gc_before_sequence_unchecked(1, last)
                        .unwrap();
                    assert!(reclaimed.clamped_by_block_retention);
                })
                .join()
                .unwrap();
            }));
        });
        let command = if filtered {
            Command::FeatureQueryFiltered {
                key: "s".into(),
                start_ms: 0,
                end_ms: u64::MAX,
                count: None,
                filters: vec![FeatureFilter {
                    field: "action_type".into(),
                    op: FeatureFilterOp::Equal,
                    value: 1,
                }],
            }
        } else {
            Command::FeatureQuery {
                key: "s".into(),
                start_ms: 0,
                end_ms: u64::MAX,
                count: None,
            }
        };
        let wanted = expected
            .into_iter()
            .enumerate()
            .filter(|(index, _)| !filtered || index % 5 == 0)
            .map(|(_, point)| point)
            .collect::<Vec<_>>();
        assert_eq!(points_of(run(&engine, command.clone())), wanted);
        crate::engine::AFTER_FAST_READ_PLAN.with(|slot| assert!(slot.borrow().is_none()));
        assert_eq!(engine.wal_store.block_retention_floor(1), None);
        let released = engine
            .wal_store
            .gc_before_sequence_unchecked(1, engine.wal_store.stats(1).last_sequence)
            .unwrap();
        assert!(!released.clamped_by_block_retention);
        assert!(released.records_after < released.records_before);
        assert!(points_of(run(&engine, command)).is_empty());
    }
}

#[test]
fn a_range_read_answers_what_was_appended() {
    let mut compared = 0usize;
    for bound in [100_000usize, 40] {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = engine_on(dir.path(), bound);
        let mut model: BTreeMap<u64, FeaturePoint> = BTreeMap::new();
        let mut put = |points: Vec<FeaturePoint>, model: &mut BTreeMap<u64, FeaturePoint>| {
            for point in &points {
                model.insert(point.timestamp_ms, point.clone());
            }
            append(&engine, points);
        };
        put((0..60).map(|i| row_point(i, i)).collect(), &mut model);
        for i in 60..70 {
            put(vec![row_point(i, i)], &mut model);
        }
        put(vec![row_point(10, 1_010), row_point(65, 1_065)], &mut model);
        while model.len() > bound {
            let oldest = *model.keys().next().expect("a point");
            model.remove(&oldest);
        }
        let at = |index: u64| BASE + index * 1_000;
        for (start, end, count) in [
            (0u64, u64::MAX, None),
            (at(20), at(40), None),
            (at(60), at(70), Some(4usize)),
            (at(10), at(10), None),
            (at(900), u64::MAX, None),
            (0, u64::MAX, Some(0)),
        ] {
            let expected: Vec<FeaturePoint> = if start > end {
                Vec::new()
            } else {
                model
                    .range(start..=end)
                    .take(count.unwrap_or(bound))
                    .map(|(_, p)| p.clone())
                    .collect()
            };
            let got = points_of(run(
                &engine,
                Command::FeatureQuery {
                    key: "s".into(),
                    start_ms: start,
                    end_ms: end,
                    count,
                },
            ));
            assert_eq!(
                expected, got,
                "bound {bound}: [{start}, {end}] count {count:?}"
            );
            compared += 1;
            // the filtered read caps the window BEFORE it filters
            let limit = count.unwrap_or(bound).min(bound);
            let window: Vec<FeaturePoint> = if start > end {
                Vec::new()
            } else {
                model
                    .range(start..=end)
                    .take(limit)
                    .map(|(_, p)| p.clone())
                    .collect()
            };
            let expected_filtered: Vec<FeaturePoint> = window
                .into_iter()
                .filter(|p| {
                    SequenceFeatureRow::decode_feature_proto_value(p.timestamp_ms, &p.value)
                        .map_or(false, |row| row.action_type == 1)
                })
                .collect();
            let got_filtered = points_of(run(
                &engine,
                Command::FeatureQueryFiltered {
                    key: "s".into(),
                    start_ms: start,
                    end_ms: end,
                    count,
                    filters: vec![FeatureFilter {
                        field: "action_type".into(),
                        op: FeatureFilterOp::Equal,
                        value: 1,
                    }],
                },
            ));
            assert_eq!(
                expected_filtered, got_filtered,
                "bound {bound}: filtered [{start}, {end}] count {count:?}"
            );
            compared += 1;
        }
        // A LAPSED KEY answers empty, through the ordinary path that drops it.
        if bound == 100_000 {
            run(
                &engine,
                Command::CommonExpire {
                    key: "s".into(),
                    ttl_ms: 1,
                },
            );
            std::thread::sleep(Duration::from_millis(30));
            assert!(points_of(run(
                &engine,
                Command::FeatureQuery {
                    key: "s".into(),
                    start_ms: 0,
                    end_ms: u64::MAX,
                    count: None
                },
            ))
            .is_empty());
        }
    }
    assert_eq!(2 * 6 * 2, compared);
}
