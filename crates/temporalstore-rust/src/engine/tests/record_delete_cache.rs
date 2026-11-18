// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

#[test]
fn record_delete_invalidates_each_model_without_evicting_siblings() {
    for batched in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs_block_store_options_and_disk_cache(
            16 << 20,
            dir.path().join("cache"),
            dir.path().join("blocks"),
            dir.path().join("indexes"),
            BlockStoreOptions::default(),
            false,
        );
        engine.load_shard(1);
        engine.load_shard(2);
        let write = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "victim".into(),
                value: b"old".to_vec(),
            },
        });
        assert!(write.status.ok, "{:?}", write.status);

        let mut removed = Vec::new();
        let mut retained = Vec::new();
        for shard in [1, 2] {
            for record in ["victim", "neighbor"] {
                let mut keys = named_record_keys(shard, record).to_vec();
                for namespace in SWEPT_RECORD_NAMESPACES {
                    assert!(
                        engine
                            .cache
                            .keys_for_records(shard, namespace, &[record])
                            .indexed
                    );
                    for selector in ["first", "second"] {
                        keys.push(CacheKey {
                            shard_id: shard,
                            namespace: namespace.into(),
                            record_key: record.into(),
                            selector: selector.into(),
                        });
                    }
                }
                for key in keys {
                    engine.cache.put(key.clone(), vec![7; 16]).unwrap();
                    if shard == 1 && record == "victim" {
                        removed.push(key);
                    } else {
                        retained.push(key);
                    }
                }
            }
        }
        for index in 0..1_024 {
            let page = CacheKey {
                shard_id: 1,
                namespace: "page".into(),
                record_key: format!("page-{index}").into(),
                selector: "data".into(),
            };
            engine.cache.put(page.clone(), vec![3; 16]).unwrap();
            retained.push(page);
        }
        for key in removed.iter().chain(&retained) {
            assert!(engine.cache.peek(key));
        }

        let command = Command::StringDelete {
            key: "victim".into(),
        };
        if batched {
            let response = engine.batch_execute(BatchExecuteRequest {
                shard_id: 1,
                commands: vec![command],
            });
            assert!(response.status.ok, "{:?}", response.status);
            assert_eq!(response.responses.len(), 1);
            assert!(
                response.responses[0].status.ok,
                "{:?}",
                response.responses[0].status
            );
        } else {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command,
            });
            assert!(response.status.ok, "{:?}", response.status);
        }
        for key in &removed {
            assert!(!engine.cache.peek(key), "not invalidated: {key:?}");
        }
        for key in &retained {
            assert!(engine.cache.peek(key), "over-invalidated: {key:?}");
        }
    }
}
