// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

pub(super) const MAX_DELETION_ITEMS: usize = 256;
const TARGET_DELETION_BYTES: usize = 256 << 10;

#[cfg(test)]
#[path = "maintenance_log_tests.rs"]
mod tests;

#[cfg(test)]
thread_local! {
    static FAIL_APPEND_AFTER: std::cell::Cell<Option<usize>> = const { std::cell::Cell::new(None) };
}

pub(super) struct MaintenanceDeletion {
    key: String,
    records: Vec<String>,
}

/// Owned names for an exclusive deletion-only run. Removals cannot add or rename a block,
/// so surviving handles keep these identities across the run's separately logged frames.
pub(super) struct DeletionIdentityBatch {
    names: BTreeMap<u32, HashMap<u64, std::sync::Arc<str>>>,
}

impl DeletionIdentityBatch {
    fn capture(shard: &ShardState, buckets: &BTreeSet<u32>) -> Self {
        let keys = state::BlockKeysByPosition::from_parts_for_buckets(
            &shard.bucket_index.objects,
            &shard.bucket_index.object_block_lookup,
            buckets,
        );
        let names = buckets
            .iter()
            .filter_map(|routing_bucket| {
                let bucket = shard.bucket_index.bucket_map.get(routing_bucket)?;
                Some((*routing_bucket, state::block_identities_in(bucket, &keys)))
            })
            .collect();
        Self { names }
    }

    pub(super) fn bucket(&self, routing_bucket: u32) -> Option<&HashMap<u64, std::sync::Arc<str>>> {
        self.names.get(&routing_bucket)
    }
}

pub(super) struct MaintenanceLogResult {
    pub(super) removed_keys: usize,
    pub(super) apply_ns: u64,
    pub(super) error: Option<Status>,
}

impl MaintenanceDeletion {
    pub(super) fn records(&self) -> &[String] {
        &self.records
    }

    pub(super) fn capture(shard: &ShardState, key: &str) -> Option<Self> {
        let records = associated_record_keys(key)
            .into_iter()
            .filter(|record_key| {
                record_exists_exact(shard, record_key)
                    || shard.expires_at_ms().contains_key(record_key.as_str())
            })
            .collect::<Vec<_>>();
        (!records.is_empty()).then(|| Self {
            key: key.to_string(),
            records,
        })
    }
}

impl TemporalEngine {
    pub(super) fn append_maintenance_deletions(
        &self,
        shard_id: ShardId,
        shard: &mut ShardState,
        deletions: Vec<MaintenanceDeletion>,
        mut note_append: impl FnMut(),
    ) -> MaintenanceLogResult {
        let buckets = deletions
            .iter()
            .flat_map(|deletion| deletion.records.iter())
            .flat_map(|key| bucket_index_target_buckets_for_object_key(shard, key))
            .collect::<BTreeSet<_>>();
        let mut identities = None;
        let mirror = self.maintenance_mirror_sink();
        let (start, end) = shard.routing_range();
        let mut outcomes = Vec::new();
        let mut keys = Vec::new();
        let mut bytes = 0usize;
        let mut removed_keys = 0usize;
        let mut apply_ns = 0u64;
        let mut pending = BTreeSet::new();
        let mut flush = |shard: &mut ShardState,
                         outcomes: &mut Vec<crate::wal::WalOutcomeItem>,
                         keys: &mut Vec<String>| {
            if outcomes.is_empty() {
                return Ok((0usize, 0u64));
            }
            #[cfg(test)]
            if FAIL_APPEND_AFTER.with(|remaining| match remaining.get() {
                Some(0) => {
                    remaining.set(None);
                    true
                }
                Some(count) => {
                    remaining.set(Some(count - 1));
                    false
                }
                None => false,
            }) {
                note_append();
                return Err(Status::error(
                    "maintenance_log_append_failed",
                    "injected append failure",
                ));
            }
            let appended = self.wal_store.append_batch_as_one_record(
                shard_id,
                std::mem::take(outcomes),
                Vec::new(),
                false,
            );
            note_append();
            let record = match appended {
                Ok(record) => record,
                Err(err) => {
                    return Err(Status::error(
                        "maintenance_log_append_failed",
                        err.to_string(),
                    ));
                }
            };
            let apply_started = std::time::Instant::now();
            // Capture only after the first successful append, before any resident deletion.
            let identities =
                identities.get_or_insert_with(|| DeletionIdentityBatch::capture(shard, &buckets));
            for item in &record.outcomes {
                delete_record_exact_with_identity_batch(shard, &item.object_key, Some(identities));
            }
            shard.note_wal_application(record.sequence, record.sequence);
            let applied_ns = apply_started.elapsed().as_nanos() as u64;
            let applied_keys = keys.len();
            if let Some(sink) = mirror.as_ref() {
                for key in keys.drain(..) {
                    sink.record_write(shard_id, &Command::CommonDelete { key });
                }
            } else {
                keys.clear();
            }
            Ok((applied_keys, applied_ns))
        };
        for deletion in deletions {
            let group_bytes = deletion.records.iter().fold(0usize, |sum, key| {
                sum.saturating_add(key.len()).saturating_add(64)
            });
            // A logical deletion and its related records stay in one frame, so a
            // successful frame can be mirrored without exposing a partial family.
            // One oversized group is allowed; the byte target is not a key-size limit.
            if !outcomes.is_empty()
                && (outcomes.len() + deletion.records.len() > MAX_DELETION_ITEMS
                    || bytes.saturating_add(group_bytes) > TARGET_DELETION_BYTES)
            {
                match flush(shard, &mut outcomes, &mut keys) {
                    Ok((count, nanos)) => {
                        removed_keys += count;
                        apply_ns += nanos;
                    }
                    Err(error) => {
                        return MaintenanceLogResult {
                            removed_keys,
                            apply_ns,
                            error: Some(error),
                        }
                    }
                }
                bytes = 0;
                pending.clear();
            }
            bytes = bytes.saturating_add(group_bytes);
            let before = outcomes.len();
            outcomes.extend(
                deletion
                    .records
                    .into_iter()
                    .filter(|key| {
                        (record_exists_exact(shard, key)
                            || shard.expires_at_ms().contains_key(key.as_str()))
                            && pending.insert(key.clone())
                    })
                    .map(|key| crate::wal::WalOutcomeItem {
                        object_id: stable_block_object_id(shard_id, "record", &key),
                        routing_bucket: block_routing_bucket(&key, start, end),
                        kind: "record".to_string(),
                        object_key: key,
                        element_name: None,
                        address: None,
                        value: None,
                        ttl: None,
                        deleted: true,
                        meta: true,
                    }),
            );
            if outcomes.len() > before {
                keys.push(deletion.key);
            }
        }
        let error = match flush(shard, &mut outcomes, &mut keys) {
            Ok((count, nanos)) => {
                removed_keys += count;
                apply_ns += nanos;
                None
            }
            Err(error) => Some(error),
        };
        MaintenanceLogResult {
            removed_keys,
            apply_ns,
            error,
        }
    }
}
