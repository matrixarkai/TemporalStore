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
                assert_eq!(
                    delta.upsert.replacement_keys().unwrap(),
                    &["order".to_string()]
                );
                assert!(delta.applied_wal_sequence.is_some());
                assert_eq!(
                    delta
                        .items
                        .iter()
                        .filter(|item| item.object_key.as_ref() == "catalog")
                        .count(),
                    1
                );
                assert!(
                    delta.items.len() < 10,
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
}
