// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use crate::index_log::{IndexDeltaApplyMode, IndexItem};

type ElementName = (&'static str, String, Option<String>);

#[derive(Default)]
pub(super) struct BatchIndexDeltaPlan {
    upserts: Vec<ElementName>,
    snapshot_keys: BTreeSet<String>,
    changed_state_keys: BTreeSet<String>,
    force_legacy_snapshot: bool,
}

fn remember_keys(target: &mut BTreeSet<String>, keys: &[String]) {
    for key in keys {
        if !target.contains(key) {
            target.insert(key.clone());
        }
    }
}

impl BatchIndexDeltaPlan {
    pub(super) fn record_command(
        &mut self,
        keys: &[String],
        elements: Option<Vec<ElementName>>,
        state_changed: bool,
    ) {
        if state_changed {
            remember_keys(&mut self.changed_state_keys, keys);
        }
        match elements {
            Some(elements) => self.upserts.extend(elements),
            None if keys.is_empty() => self.force_legacy_snapshot = true,
            None => remember_keys(&mut self.snapshot_keys, keys),
        }
    }

    pub(super) fn collect(
        mut self,
        shard: &ShardState,
        all_keys: &[String],
        start: u32,
        end: u32,
    ) -> (Vec<IndexItem>, IndexDeltaApplyMode) {
        if self.force_legacy_snapshot {
            return (
                collect_command_index_items(shard, all_keys, start, end),
                false.into(),
            );
        }
        if self.snapshot_keys.is_empty() {
            return (
                collect_upsert_index_items(
                    shard,
                    shard.shard_id().expect("loaded shard"),
                    &self.upserts,
                    start,
                    end,
                ),
                true.into(),
            );
        }
        // The legacy protobuf schema cannot name replacement keys. Preserve its existing
        // whole-batch semantics rather than fail a write or discard replacement information.
        if crate::index_log::index_log_protobuf_enabled() {
            return (
                collect_command_index_items(shard, all_keys, start, end),
                false.into(),
            );
        }
        // A mixed record retains snapshot treatment for trims and deadline changes.
        self.snapshot_keys.extend(self.changed_state_keys);
        self.upserts
            .retain(|(_, key, _)| !self.snapshot_keys.contains(key));
        if self.upserts.is_empty() {
            return (
                collect_command_index_items(shard, all_keys, start, end),
                false.into(),
            );
        }
        let keys = self.snapshot_keys.into_iter().collect::<Vec<_>>();
        let mut items = collect_command_index_items(shard, &keys, start, end);
        items.extend(collect_upsert_index_items(
            shard,
            shard.shard_id().expect("loaded shard"),
            &self.upserts,
            start,
            end,
        ));
        (
            items,
            IndexDeltaApplyMode::keyed_snapshots(keys).expect("mixed record has snapshot keys"),
        )
    }
}

pub(super) fn fold_planned_index_delta(
    index: &mut CoreIndex,
    covered: &BTreeSet<String>,
    items: &[IndexItem],
    mode: &IndexDeltaApplyMode,
) {
    if let Some(upsert) = mode.legacy_upsert() {
        fold_delta_block_items(index, covered, items, upsert);
    } else {
        let replacements = mode
            .replacement_keys()
            .expect("keyed apply mode")
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        // Both phases operate on the unpublished load draft, under one record anchor.
        fold_delta_block_items(index, &replacements, &[], false);
        fold_delta_block_items(index, &BTreeSet::new(), items, true);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::timestamped_series::TimestampedSeries as _;

    // These fixtures use the crate's documented single-threaded test execution.
    struct ProtobufWriter(Option<std::ffi::OsString>);

    impl ProtobufWriter {
        fn set(enabled: bool) -> Self {
            let previous = std::env::var_os("TS_INDEX_LOG_PROTOBUF_ENABLED");
            std::env::set_var(
                "TS_INDEX_LOG_PROTOBUF_ENABLED",
                if enabled { "true" } else { "false" },
            );
            Self(previous)
        }
    }

    impl Drop for ProtobufWriter {
        fn drop(&mut self) {
            match self.0.take() {
                Some(value) => std::env::set_var("TS_INDEX_LOG_PROTOBUF_ENABLED", value),
                None => std::env::remove_var("TS_INDEX_LOG_PROTOBUF_ENABLED"),
            }
        }
    }

    struct FoldProbe;

    impl FoldProbe {
        fn arm() -> Self {
            crate::index_log::probe::arm_decisions();
            Self
        }
    }

    impl Drop for FoldProbe {
        fn drop(&mut self) {
            crate::index_log::probe::disarm_decisions();
        }
    }

    fn run(engine: &TemporalEngine, command: Command) -> CommandResponse {
        let out = engine.execute(ExecuteRequest {
            shard_id: 1,
            command,
        });
        assert!(out.status.ok, "{:?}", out.status);
        out.response
    }

    #[test]
    fn mixed_delta_keeps_catalog_rows_sparse_and_recovers_after_drop() {
        mixed_delta_recovery(false);
    }

    #[test]
    fn mixed_delta_keeps_legacy_protobuf_writes_recoverable() {
        mixed_delta_recovery(true);
    }

    fn mixed_delta_recovery(protobuf: bool) {
        let _writer = ProtobufWriter::set(protobuf);
        for existing in [32, 2_048] {
            let dir = tempfile::tempdir().unwrap();
            let open = || {
                TemporalEngine::with_local_dirs(
                    16 * 1024 * 1024,
                    dir.path().join("cache"),
                    dir.path().join("blocks"),
                    dir.path().join("indexes"),
                )
            };
            let mixed_sequence;
            let base_bytes;
            {
                let engine = open();
                engine.load_shard(1);
                run(
                    &engine,
                    Command::HashMultiSet {
                        key: "catalog".into(),
                        entries: (0..existing)
                            .map(|i| (format!("old-{i}"), vec![7; 64]))
                            .collect(),
                    },
                );
                for member in [b"keep".to_vec(), b"remove".to_vec()] {
                    run(
                        &engine,
                        Command::SetAdd {
                            key: "order".into(),
                            member,
                        },
                    );
                }
                engine.flush_shard_index(1);
                base_bytes = std::fs::read(engine.index_path(1)).unwrap();
                let before = engine
                    .index_log_store
                    .read_delta_records(1, 0)
                    .unwrap()
                    .len();
                let out = engine.batch_execute(BatchExecuteRequest {
                    shard_id: 1,
                    commands: vec![
                        Command::HashSet {
                            key: "catalog".into(),
                            field: "new".into(),
                            value: vec![9; 64],
                        },
                        Command::SetRemove {
                            key: "order".into(),
                            member: b"remove".to_vec(),
                        },
                        Command::SetAdd {
                            key: "order".into(),
                            member: b"remove".to_vec(),
                        },
                        Command::SetRemove {
                            key: "order".into(),
                            member: b"remove".to_vec(),
                        },
                        Command::SetAdd {
                            key: "order".into(),
                            member: b"new".to_vec(),
                        },
                    ],
                });
                assert!(out.status.ok, "{:?}", out.status);
                assert!(out.responses.iter().all(|row| row.status.ok));
                let records = engine.index_log_store.read_delta_records(1, 0).unwrap();
                assert_eq!(
                    records.len(),
                    before + 1,
                    "one batch must have one index anchor"
                );
                let delta = records.last().unwrap();
                mixed_sequence = delta.sequence;
                if protobuf {
                    assert_eq!(delta.upsert.legacy_upsert(), Some(false));
                } else {
                    assert_eq!(
                        delta.upsert.replacement_keys().unwrap(),
                        &["order".to_string()]
                    );
                }
                assert!(delta.applied_wal_sequence.is_some());
                assert_eq!(
                    delta
                        .items
                        .iter()
                        .filter(|item| item.object_key.as_ref() == "catalog")
                        .count(),
                    if protobuf { existing + 1 } else { 1 }
                );
                assert!(
                    protobuf || delta.items.len() < 10,
                    "catalog size {existing}: {} items",
                    delta.items.len()
                );
            }
            // No unload/checkpoint: the surviving catalog and set must be reconstructed on load.
            let recovered = open();
            assert_eq!(
                std::fs::read(recovered.index_path(1)).unwrap(),
                base_bytes,
                "the mixed write must not replace the stale base checkpoint"
            );
            let control = recovered.load_index_base_only(1, false).unwrap();
            assert!(!control
                .bucket_index
                .hashes
                .get("catalog")
                .unwrap()
                .contains_key("new"));
            assert!(control
                .bucket_index
                .sets
                .get("order")
                .unwrap()
                .contains_key(b"remove".as_slice()));
            drop(control);
            {
                let _probe = FoldProbe::arm();
                let folded = recovered.load_index_checked(1, false).unwrap().unwrap();
                assert!(
                    crate::index_log::probe::applied().contains(&(1, mixed_sequence)),
                    "the checked index load must actually apply the mixed delta"
                );
                let catalog = folded.bucket_index.hashes.get("catalog").unwrap();
                assert_eq!(catalog.len(), existing + 1);
                assert!(catalog.contains_key("new"));
                assert!(catalog.contains_key("old-0"));
                let members = folded.bucket_index.sets.get("order").unwrap();
                assert_eq!(members.len(), 2);
                assert!(members.contains_key(b"keep".as_slice()));
                assert!(members.contains_key(b"new".as_slice()));
                assert!(!members.contains_key(b"remove".as_slice()));
            }
            // Also exercise normal restart recovery, which independently uses the WAL tail.
            recovered.load_shard(1);
            for field in ["old-0", &format!("old-{}", existing - 1)] {
                assert_eq!(
                    run(
                        &recovered,
                        Command::HashGet {
                            key: "catalog".into(),
                            field: field.into()
                        }
                    ),
                    CommandResponse::Bytes {
                        value: Some(vec![7; 64])
                    }
                );
            }
            assert_eq!(
                run(
                    &recovered,
                    Command::HashGet {
                        key: "catalog".into(),
                        field: "new".into()
                    }
                ),
                CommandResponse::Bytes {
                    value: Some(vec![9; 64])
                }
            );
            assert_eq!(
                run(
                    &recovered,
                    Command::SetCard {
                        key: "order".into()
                    }
                ),
                CommandResponse::Integer { value: 2 }
            );
            assert_eq!(
                run(
                    &recovered,
                    Command::SetIsMember {
                        key: "order".into(),
                        member: b"remove".to_vec()
                    }
                ),
                CommandResponse::Integer { value: 0 }
            );
        }
    }

    fn feature_times(engine: &TemporalEngine) -> Vec<u64> {
        match run(
            engine,
            Command::FeatureQuery {
                key: "series".into(),
                start_ms: 0,
                end_ms: u64::MAX,
                count: Some(10),
            },
        ) {
            CommandResponse::FeaturePoints { points } => {
                let mut times = points
                    .into_iter()
                    .map(|point| point.timestamp_ms)
                    .collect::<Vec<_>>();
                times.sort_unstable();
                times
            }
            other => panic!("expected feature points, got {other:?}"),
        }
    }

    #[test]
    fn mixed_trim_deadline_and_empty_replacement_survive_reclaim() {
        let _writer = ProtobufWriter::set(false);
        let dir = tempfile::tempdir().unwrap();
        let open = || {
            TemporalEngine::with_local_dirs(
                16 * 1024 * 1024,
                dir.path().join("cache"),
                dir.path().join("blocks"),
                dir.path().join("indexes"),
            )
        };
        let engine = open();
        engine.load_shard(1);
        assert!(
            engine
                .set_config(crate::control::SetConfigRequest {
                    shard_id: 1,
                    config: crate::control::Config {
                        version: 2,
                        feature_max_size: 2,
                        ..Default::default()
                    },
                })
                .ok
        );
        run(
            &engine,
            Command::HashMultiSet {
                key: "catalog".into(),
                entries: (0..32).map(|i| (format!("old-{i}"), vec![7])).collect(),
            },
        );
        run(
            &engine,
            Command::SetAdd {
                key: "order".into(),
                member: b"victim".to_vec(),
            },
        );
        run(
            &engine,
            Command::StringSet {
                key: "deadline".into(),
                value: b"old".to_vec(),
            },
        );
        run(
            &engine,
            Command::CommonExpire {
                key: "deadline".into(),
                ttl_ms: 3_600_000,
            },
        );
        run(
            &engine,
            Command::HashSet {
                key: "armed".into(),
                field: "old".into(),
                value: vec![7],
            },
        );
        run(
            &engine,
            Command::FeatureAppend {
                key: "series".into(),
                points: (1..=2)
                    .map(|timestamp_ms| crate::types::FeaturePoint {
                        timestamp_ms,
                        value: vec![7],
                    })
                    .collect(),
            },
        );
        engine.flush_shard_index(1);
        let stale_base = std::fs::read(engine.index_path(1)).unwrap();
        let out = engine.batch_execute(BatchExecuteRequest {
            shard_id: 1,
            commands: vec![
                Command::SetRemove {
                    key: "order".into(),
                    member: b"victim".to_vec(),
                },
                Command::FeatureAppend {
                    key: "series".into(),
                    points: vec![crate::types::FeaturePoint {
                        timestamp_ms: 3,
                        value: vec![9],
                    }],
                },
                Command::StringSetConditional {
                    key: "deadline".into(),
                    value: b"fresh".to_vec(),
                    ttl_ms: None,
                    condition: crate::types::StringSetCondition::Always,
                    return_old: false,
                    keep_ttl: false,
                },
                Command::HashMultiSetWithTtl {
                    key: "armed".into(),
                    entries: vec![("new".into(), vec![9])],
                    ttl_ms: 3_600_000,
                },
                Command::HashSet {
                    key: "catalog".into(),
                    field: "new".into(),
                    value: vec![9],
                },
            ],
        });
        assert!(out.status.ok, "{:?}", out.status);
        assert!(out.responses.iter().all(|row| row.status.ok));
        let records = engine.index_log_store.read_delta_records(1, 0).unwrap();
        let mixed = records.last().unwrap();
        let sequence = mixed.sequence;
        {
            let state = engine.shards.read().unwrap();
            let shard = state.get(&1).unwrap();
            assert!(
                !shard.expires_at_ms().contains_key("deadline"),
                "the fixture must actually clear the deadline before testing recovery"
            );
            assert!(
                shard.expires_at_ms().contains_key("armed"),
                "the later hash upsert must actually arm a deadline"
            );
        }
        assert_eq!(
            mixed.upsert.replacement_keys().unwrap(),
            &[
                "armed".to_string(),
                "deadline".to_string(),
                "order".to_string(),
                "series".to_string()
            ]
        );
        assert_eq!(
            mixed
                .items
                .iter()
                .filter(|item| item.object_key.as_ref() == "catalog")
                .count(),
            1
        );
        assert_eq!(std::fs::read(engine.index_path(1)).unwrap(), stale_base);
        drop(engine);

        let recovered = open();
        let control = recovered.load_index_base_only(1, false).unwrap();
        assert!(control.expires_at_ms().contains_key("deadline"));
        assert!(!control.expires_at_ms().contains_key("armed"));
        assert!(control
            .bucket_index
            .sets
            .get("order")
            .unwrap()
            .contains_key(b"victim".as_slice()));
        assert_eq!(
            control
                .features()
                .get("series")
                .unwrap()
                .points()
                .map(|(at, _)| at)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        drop(control);
        {
            let _probe = FoldProbe::arm();
            let folded = recovered.load_index_checked(1, false).unwrap().unwrap();
            assert!(crate::index_log::probe::applied().contains(&(1, sequence)));
            assert!(!folded.expires_at_ms().contains_key("deadline"));
            assert!(folded.expires_at_ms().contains_key("armed"));
            let armed = folded.bucket_index.hashes.get("armed").unwrap();
            assert!(armed.contains_key("old"));
            assert!(armed.contains_key("new"));
            assert!(folded
                .bucket_index
                .sets
                .get("order")
                .is_none_or(|members| members.is_empty()));
            assert_eq!(
                folded
                    .features()
                    .get("series")
                    .unwrap()
                    .points()
                    .map(|(at, _)| at)
                    .collect::<Vec<_>>(),
                vec![2, 3]
            );
            assert!(folded
                .bucket_index
                .hashes
                .get("catalog")
                .unwrap()
                .contains_key("old-0"));
            assert!(folded
                .bucket_index
                .hashes
                .get("catalog")
                .unwrap()
                .contains_key("new"));
        }
        recovered.load_shard(1);
        assert_eq!(feature_times(&recovered), vec![2, 3]);
        assert_eq!(
            run(
                &recovered,
                Command::StringGet {
                    key: "deadline".into()
                }
            ),
            CommandResponse::Bytes {
                value: Some(b"fresh".to_vec())
            }
        );
        let report = recovered
            .dump_and_reclaim_index_logs_with_min_reclaimable(1, 0)
            .unwrap();
        assert!(!report.index_log_sweep_failed);
        assert!(
            report.index_log_records_removed > 0,
            "the fixture must actually reclaim index records"
        );
        assert!(
            !recovered
                .index_log_store
                .read_delta_records(1, 0)
                .unwrap()
                .iter()
                .any(|record| record.sequence == sequence),
            "the mixed delta must be removed after its state is checkpointed"
        );
        drop(recovered);

        let reopened = open();
        reopened.load_shard(1);
        assert_eq!(feature_times(&reopened), vec![2, 3]);
        assert_eq!(
            run(
                &reopened,
                Command::SetIsMember {
                    key: "order".into(),
                    member: b"victim".to_vec()
                }
            ),
            CommandResponse::Integer { value: 0 }
        );
        assert_eq!(
            run(
                &reopened,
                Command::StringGet {
                    key: "deadline".into()
                }
            ),
            CommandResponse::Bytes {
                value: Some(b"fresh".to_vec())
            }
        );
        assert_eq!(
            run(
                &reopened,
                Command::HashGet {
                    key: "catalog".into(),
                    field: "old-0".into()
                }
            ),
            CommandResponse::Bytes {
                value: Some(vec![7])
            }
        );
        assert_eq!(
            run(
                &reopened,
                Command::HashGet {
                    key: "catalog".into(),
                    field: "new".into()
                }
            ),
            CommandResponse::Bytes {
                value: Some(vec![9])
            }
        );
        let state = reopened.shards.read().unwrap();
        assert!(state.get(&1).unwrap().expires_at_ms().contains_key("armed"));
        assert!(!state
            .get(&1)
            .unwrap()
            .expires_at_ms()
            .contains_key("deadline"));
    }
}
