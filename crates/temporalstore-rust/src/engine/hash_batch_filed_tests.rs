// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

fn open(dir: &std::path::Path) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    );
    engine.load_shard(1);
    engine
}

fn read_fields(engine: &TemporalEngine, fields: &[&str]) -> Vec<Option<Vec<u8>>> {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::HashMultiGet {
            key: "row".into(),
            fields: fields.iter().map(|field| field.to_string()).collect(),
        },
    });
    assert!(response.status.ok);
    let CommandResponse::Values { values } = response.response else {
        panic!("expected hash values");
    };
    values
}

#[test]
fn generic_hash_batch_does_not_repeat_post_write_filing_or_change_layout() {
    for async_storage in [false, true] {
        for count in [0usize, 1, 128] {
            let dir = tempfile::tempdir().unwrap();
            let engine = open(dir.path());
            assert!(
                engine
                    .set_config(crate::control::SetConfigRequest {
                        shard_id: 1,
                        config: Config {
                            version: 2,
                            async_storage,
                            ..Config::default()
                        },
                    })
                    .ok
            );
            let before = crate::engine::storage_bucket_internals::bucket_block_upserts();
            let response = engine.batch_execute(BatchExecuteRequest {
                shard_id: 1,
                commands: vec![Command::HashMultiSet {
                    key: "row".into(),
                    entries: (0..count)
                        .map(|index| {
                            (
                                format!("field-{index:03}"),
                                (index as u64).to_be_bytes().to_vec(),
                            )
                        })
                        .collect(),
                }],
            });
            assert!(response.status.ok, "{:?}", response.status);
            assert!(response.responses.iter().all(|response| response.status.ok));
            let filed = crate::engine::storage_bucket_internals::bucket_block_upserts() - before;
            assert_eq!(
                filed, 0,
                "the writer's filed index needs no post-write filing"
            );
            if count > 0 {
                let shards = engine.shards.read().unwrap();
                let shard = shards.get(&1).unwrap();
                let fields = shard.bucket_index.hashes.get("row").unwrap();
                let addresses = fields
                    .iter()
                    .map(|(_, address)| crate::engine::live_block_key(address))
                    .collect::<std::collections::BTreeSet<_>>();
                assert_eq!(
                    addresses.len(),
                    count,
                    "small fields keep their existing block layout"
                );
                assert_eq!(
                    shard
                        .bucket_index
                        .block_refs_for("hash", "row", None)
                        .unwrap()
                        .len(),
                    count
                );
                drop(shards);
                let fields = (0..count)
                    .map(|index| format!("field-{index:03}"))
                    .collect::<Vec<_>>();
                let before = crate::engine::storage_bucket_internals::bucket_block_upserts();
                let mut shards = engine.shards.write().unwrap();
                assert!(
                    crate::engine::storage_bucket_internals::sync_written_hash_fields(
                        shards.get_mut(&1).unwrap(),
                        1,
                        "row",
                        fields.iter().map(String::as_str),
                    )
                );
                let control =
                    crate::engine::storage_bucket_internals::bucket_block_upserts() - before;
                assert_eq!(
                    control, count as u64,
                    "the counter must see the previous sync path"
                );
                println!("hash-batch fields={count} async={async_storage} post_write_files={filed} control_files={control}");
            }
        }
    }
}

#[test]
fn generic_hash_batch_duplicate_fields_and_readds_survive_restart() {
    for async_storage in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let engine = open(dir.path());
        assert!(
            engine
                .set_config(crate::control::SetConfigRequest {
                    shard_id: 1,
                    config: Config {
                        version: 2,
                        async_storage,
                        ..Config::default()
                    },
                })
                .ok
        );
        let first = engine.batch_execute_durable(BatchExecuteRequest {
            shard_id: 1,
            commands: vec![Command::HashMultiSet {
                key: "row".into(),
                entries: vec![
                    ("a".into(), b"first".to_vec()),
                    ("b".into(), b"held".to_vec()),
                ],
            }],
        });
        assert!(first.status.ok);
        assert!(
            engine
                .execute_durable(ExecuteRequest {
                    shard_id: 1,
                    command: Command::HashDelete {
                        key: "row".into(),
                        field: "a".into()
                    },
                })
                .status
                .ok
        );
        let response = engine.batch_execute_durable(BatchExecuteRequest {
            shard_id: 1,
            commands: vec![
                Command::HashMultiSet {
                    key: "row".into(),
                    entries: vec![
                        ("a".into(), b"middle".to_vec()),
                        ("c".into(), b"added".to_vec()),
                        ("a".into(), b"last".to_vec()),
                    ],
                },
                Command::StringSet {
                    key: "other".into(),
                    value: b"retained payload".to_vec(),
                },
            ],
        });
        assert!(response.status.ok);
        assert!(response.responses.iter().all(|response| response.status.ok));
        let expected = vec![
            Some(b"last".to_vec()),
            Some(b"held".to_vec()),
            Some(b"added".to_vec()),
        ];
        assert_eq!(read_fields(&engine, &["a", "b", "c"]), expected);
        engine.write_ahead_log_store().flush(1).unwrap();
        drop(engine);
        let recovered = open(dir.path());
        assert_eq!(read_fields(&recovered, &["a", "b", "c"]), expected);
        let response = recovered.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: "other".into(),
            },
        });
        assert!(response.status.ok);
        assert!(
            matches!(response.response, CommandResponse::Bytes { value: Some(ref bytes) } if bytes == b"retained payload")
        );
        let shards = recovered.shards.read().unwrap();
        assert_eq!(
            shards
                .get(&1)
                .unwrap()
                .bucket_index
                .block_refs_for("hash", "row", None)
                .unwrap()
                .len(),
            3
        );
    }
}
