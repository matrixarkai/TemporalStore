// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

fn engine_on(path: &std::path::Path) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        32 * 1024 * 1024,
        path.join("cache"),
        path.join("blocks"),
        path.join("indexes"),
    );
    engine.load_shard(1);
    engine
}

fn write(engine: &TemporalEngine, command: Command) {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(response.status.ok, "{:?}", response.status);
}

fn request(keys: &[&str], fields: &[&str]) -> BatchHashMultiGetRequest {
    BatchHashMultiGetRequest {
        shard_id: 1,
        keys: keys.iter().map(|key| key.to_string()).collect(),
        fields: fields.iter().map(|field| field.to_string()).collect(),
    }
}

#[test]
fn serialization_releases_guard_and_preserves_owned_and_shared_snapshots() {
    for size in [16, 8192] {
        let dir = tempfile::tempdir().unwrap();
        let engine = engine_on(dir.path());
        for key in ["first", "second"] {
            write(
                &engine,
                Command::HashMultiSet {
                    key: key.into(),
                    entries: vec![
                        ("value".into(), vec![7; size]),
                        ("neighbor".into(), b"held".to_vec()),
                    ],
                },
            );
        }
        let mut seen = Vec::new();
        engine
            .visit_batch_hash_multi_get(
                request(&["first", "second"], &["value", "neighbor"]),
                |index, result| {
                    assert!(
                        engine.shards.try_write().is_ok(),
                        "encoding must not hold the shard guard"
                    );
                    let values = result.unwrap();
                    assert_eq!(
                        matches!(values, execute_on_shard::ProjectedHashValues::Shared { .. }),
                        size == 8192
                    );
                    if index == 0 {
                        write(
                            &engine,
                            Command::HashMultiSet {
                                key: "first".into(),
                                entries: vec![("value".into(), b"new".to_vec())],
                            },
                        );
                        write(
                            &engine,
                            Command::CommonDelete {
                                key: "second".into(),
                            },
                        );
                    }
                    seen.push((index, values.into_owned()));
                },
            )
            .unwrap();
        assert_eq!(
            seen,
            vec![
                (0, vec![Some(vec![7; size]), Some(b"held".to_vec())]),
                (1, vec![Some(vec![7; size]), Some(b"held".to_vec())]),
            ]
        );
        let current =
            engine.batch_hash_multi_get_compact(request(&["first", "second"], &["value"]));
        assert_eq!(current.rows[0].0, vec![Some(b"new".to_vec())]);
        assert_eq!(current.rows[1].0, vec![None]);
    }
}

#[test]
fn snapshot_preserves_missing_duplicate_and_empty_projections() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_on(dir.path());
    write(
        &engine,
        Command::HashMultiSet {
            key: "row".into(),
            entries: vec![("large".into(), vec![2; 8192]), ("empty".into(), vec![])],
        },
    );
    for fields in [vec![], vec!["empty", "missing", "large", "empty"]] {
        let req = request(&["row", "missing", "row"], &fields);
        let full = engine.batch_hash_multi_get_compact(req.clone());
        let bytes = engine.batch_hash_multi_get_compact_msgpack(req.clone());
        assert_eq!(bytes, rmp_serde::to_vec_named(&full).unwrap());
        engine
            .visit_batch_hash_multi_get(req, |_, values| {
                assert!(engine.shards.try_write().is_ok());
                assert_eq!(values.unwrap().len(), fields.len());
            })
            .unwrap();
    }
}

#[test]
fn oversized_projection_keeps_under_guard_streaming() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_on(dir.path());
    write(
        &engine,
        Command::HashMultiSet {
            key: "large".into(),
            entries: vec![("value".into(), vec![3; 9 * 1024 * 1024])],
        },
    );
    let mut seen = 0;
    engine
        .visit_batch_hash_multi_get(
            request(&["missing", "large", "missing"], &["value"]),
            |index, row| {
                assert!(
                    engine.shards.try_write().is_err(),
                    "oversized requests must retain one guarded view"
                );
                let row = row.unwrap();
                assert_eq!(index, seen);
                assert_eq!(
                    row.value_at(0).map(<[u8]>::len),
                    (index == 1).then_some(9 * 1024 * 1024)
                );
                seen += 1;
            },
        )
        .unwrap();
    assert_eq!(seen, 3);
}

#[test]
fn unloaded_shard_never_delivers_snapshot_rows() {
    let engine = TemporalEngine::default();
    let result = engine.visit_batch_hash_multi_get(request(&["missing"], &["value"]), |_, _| {
        panic!("an unloaded shard cannot deliver rows");
    });
    assert_eq!(result.unwrap_err().code, "shard_not_loaded");
}
