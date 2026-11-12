// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

type Hook = Box<dyn FnOnce() -> Result<(), Status>>;
thread_local! {
    static AFTER_CAPTURE: std::cell::RefCell<Option<Hook>> = const { std::cell::RefCell::new(None) };
}

pub(super) fn after_capture() -> Result<(), Status> {
    let hook = AFTER_CAPTURE.with(|slot| slot.borrow_mut().take());
    match hook {
        Some(hook) => hook(),
        None => Ok(()),
    }
}

struct HookGuard;
impl Drop for HookGuard {
    fn drop(&mut self) {
        AFTER_CAPTURE.with(|slot| {
            slot.borrow_mut().take();
        });
    }
}

fn arm(hook: impl FnOnce() -> Result<(), Status> + 'static) -> HookGuard {
    AFTER_CAPTURE.with(|slot| assert!(slot.replace(Some(Box::new(hook))).is_none()));
    HookGuard
}

fn engine_at(root: &std::path::Path, cache: &str) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        2048,
        root.join(cache),
        root.join("blocks"),
        root.join("indexes"),
    );
    engine.load_shard(1);
    assert!(
        engine
            .set_config(SetConfigRequest {
                shard_id: 1,
                config: Config {
                    version: 2,
                    async_storage: true,
                    ..Config::default()
                },
            })
            .ok
    );
    engine
}

fn execute(engine: &TemporalEngine, command: Command) -> CommandResponse {
    let result = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(result.status.ok, "{:?}", result.status);
    result.response
}

#[test]
fn captured_dump_preserves_growth_after_its_image_and_recovers_every_tail_value() {
    for tail_count in [1, 128] {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine_at(dir.path(), "before");
        execute(
            &engine,
            Command::StringSet {
                key: "base".into(),
                value: vec![7; 64],
            },
        );
        execute(
            &engine,
            Command::FeatureAppend {
                key: "series".into(),
                points: vec![crate::types::FeaturePoint {
                    timestamp_ms: 1,
                    value: vec![1],
                }],
            },
        );
        execute(
            &engine,
            Command::ControlStateIncrement {
                key: "counter".into(),
                timestamp_ms: 1,
                amount: 7,
            },
        );
        let anchor = engine.wal_store.stats_without_rescan(1).last_sequence;
        assert!(anchor > 0);
        let appended = std::sync::Arc::new(std::sync::Mutex::new(None));
        let observed = appended.clone();
        let writer = engine.clone();
        let _hook = arm(move || {
            let before_bytes = writer.wal_store.undumped_len_since_dump(1);
            let before_objects = writer.wal_store.undumped_objects_since_dump(1);
            for n in 0..tail_count {
                execute(
                    &writer,
                    Command::StringSet {
                        key: format!("tail-{n:04}"),
                        value: vec![n as u8; 64],
                    },
                );
            }
            execute(
                &writer,
                Command::FeatureAppend {
                    key: "series".into(),
                    points: vec![crate::types::FeaturePoint {
                        timestamp_ms: 2,
                        value: vec![2],
                    }],
                },
            );
            execute(
                &writer,
                Command::ControlStateIncrement {
                    key: "counter".into(),
                    timestamp_ms: 2,
                    amount: 5,
                },
            );
            *observed.lock().unwrap() = Some((
                writer.wal_store.undumped_len_since_dump(1) - before_bytes,
                writer.wal_store.undumped_objects_since_dump(1) - before_objects,
            ));
            Ok(())
        });
        let manifest = engine.create_bucket_dump_manifest(1, []).unwrap();
        let (tail_bytes, tail_objects) = appended.lock().unwrap().unwrap();
        assert!(tail_bytes > 0 && tail_objects > 0);
        assert_eq!(manifest.wal_sequence, anchor);
        let image = decode_index_bytes(&manifest.index_bytes).unwrap();
        assert!(!image.strings().contains_key("tail-0000"));
        assert_eq!(image.features()["series"].len(), 1);
        assert_eq!(engine.wal_store.undumped_len_since_dump(1), tail_bytes);
        assert_eq!(
            engine.wal_store.undumped_objects_since_dump(1),
            tail_objects
        );
        drop(engine);
        let restored = engine_at(dir.path(), "after");
        for n in 0..tail_count {
            assert_eq!(
                execute(
                    &restored,
                    Command::StringGet {
                        key: format!("tail-{n:04}")
                    }
                ),
                CommandResponse::Bytes {
                    value: Some(vec![n as u8; 64])
                }
            );
        }
        assert_eq!(
            execute(
                &restored,
                Command::FeatureQuery {
                    key: "series".into(),
                    start_ms: 0,
                    end_ms: 3,
                    count: None,
                }
            ),
            CommandResponse::FeaturePoints {
                points: vec![
                    crate::types::FeaturePoint {
                        timestamp_ms: 1,
                        value: vec![1]
                    },
                    crate::types::FeaturePoint {
                        timestamp_ms: 2,
                        value: vec![2]
                    },
                ]
            }
        );
        assert_eq!(
            execute(
                &restored,
                Command::ControlStateQuery {
                    key: "counter".into(),
                    start_ms: 0,
                    end_ms: 3,
                    aggregator: "sum".into(),
                }
            ),
            CommandResponse::Integer { value: 12 }
        );
    }
}

#[test]
fn an_aborted_dump_capture_retires_no_growth() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_at(dir.path(), "cache");
    execute(
        &engine,
        Command::StringSet {
            key: "owed".into(),
            value: vec![3; 64],
        },
    );
    let bytes = engine.wal_store.undumped_len_since_dump(1);
    let objects = engine.wal_store.undumped_objects_since_dump(1);
    assert!(bytes > 0 && objects > 0);
    let _hook = arm(|| {
        Err(Status::error(
            "capture_stopped",
            "injected pre-publication failure",
        ))
    });
    assert_eq!(
        engine.create_bucket_dump_manifest(1, []).unwrap_err().code,
        "capture_stopped"
    );
    assert_eq!(engine.wal_store.undumped_len_since_dump(1), bytes);
    assert_eq!(engine.wal_store.undumped_objects_since_dump(1), objects);
    assert!(latest_bucket_dump_manifest_at(&engine.index_dir, 1).is_none());
}

#[test]
fn a_superseded_dump_cannot_clear_the_newer_generations_tail() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_at(dir.path(), "cache");
    execute(
        &engine,
        Command::StringSet {
            key: "base".into(),
            value: vec![1; 64],
        },
    );
    let tail = std::sync::Arc::new(std::sync::Mutex::new(None));
    let observed = tail.clone();
    let writer = engine.clone();
    let _hook = arm(move || {
        execute(
            &writer,
            Command::StringSet {
                key: "middle".into(),
                value: vec![2; 64],
            },
        );
        writer.create_bucket_dump_manifest(1, [])?;
        assert_eq!(writer.wal_store.undumped_len_since_dump(1), 0);
        execute(
            &writer,
            Command::StringSet {
                key: "tail".into(),
                value: vec![3; 64],
            },
        );
        *observed.lock().unwrap() = Some((
            writer.wal_store.undumped_len_since_dump(1),
            writer.wal_store.undumped_objects_since_dump(1),
        ));
        Ok(())
    });
    assert_eq!(
        engine.create_bucket_dump_manifest(1, []).unwrap_err().code,
        "checkpoint_superseded"
    );
    let expected = tail.lock().unwrap().unwrap();
    assert!(expected.0 > 0 && expected.1 > 0);
    assert_eq!(engine.wal_store.undumped_len_since_dump(1), expected.0);
    assert_eq!(engine.wal_store.undumped_objects_since_dump(1), expected.1);
    let latest = latest_bucket_dump_manifest_at(&engine.index_dir, 1).unwrap();
    let image = decode_index_bytes(&latest.index_bytes).unwrap();
    assert!(image.strings().contains_key("middle"));
    assert!(!image.strings().contains_key("tail"));
}

#[test]
#[ignore = "run alone with MATRIXARK_BULK_INGEST=1"]
fn a_frozen_bulk_image_does_not_retire_growth_it_never_captured() {
    assert!(
        bulk_ingest_mode(),
        "this guard requires a fresh bulk-mode process"
    );
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_at(dir.path(), "cache");
    execute(
        &engine,
        Command::StringSet {
            key: "base".into(),
            value: vec![1; 64],
        },
    );
    engine.flush_shard_index(1);
    let first = engine.create_bucket_dump_manifest(1, []).unwrap();
    assert_eq!(engine.wal_store.undumped_len_since_dump(1), 0);
    execute(
        &engine,
        Command::StringSet {
            key: "tail".into(),
            value: vec![2; 64],
        },
    );
    let bytes = engine.wal_store.undumped_len_since_dump(1);
    let objects = engine.wal_store.undumped_objects_since_dump(1);
    assert!(bytes > 0 && objects > 0);
    assert!(engine.wal_store.stats_without_rescan(1).last_sequence > first.wal_sequence);
    let second = engine.create_bucket_dump_manifest(1, []).unwrap();
    assert_eq!(second.wal_sequence, first.wal_sequence);
    assert!(!decode_index_bytes(&second.index_bytes)
        .unwrap()
        .strings()
        .contains_key("tail"));
    assert_eq!(engine.wal_store.undumped_len_since_dump(1), bytes);
    assert_eq!(engine.wal_store.undumped_objects_since_dump(1), objects);
}
