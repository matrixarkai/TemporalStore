// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

#[derive(Default)]
pub(super) struct HashPredecessors {
    by_key: BTreeMap<String, Vec<crate::index_log::IndexItem>>,
}

impl HashPredecessors {
    pub(super) fn capture(
        &mut self,
        shard: &ShardState,
        shard_id: ShardId,
        command: &Command,
        blocked: &mut BTreeSet<String>,
    ) {
        let (key, fields): (&str, Vec<&str>) = match command {
            Command::HashSet { key, field, .. } | Command::HashIncrBy { key, field, .. } => {
                (key, vec![field])
            }
            Command::HashMultiSet { key, entries }
            | Command::HashMultiSetIfTimestampOlder { key, entries, .. } => (
                key,
                entries.iter().map(|(field, _)| field.as_str()).collect(),
            ),
            _ => return,
        };
        let Some(existing) = shard.bucket_index.hashes.get(key) else {
            return;
        };
        let mut wanted: BTreeSet<_> = fields
            .iter()
            .filter_map(|field| existing.get(field))
            .map(live_block_key)
            .collect();
        if wanted.is_empty() {
            return;
        }
        let Some(refs) = shard.bucket_index.block_refs_for("hash", key, None) else {
            blocked.insert(key.to_owned());
            return;
        };
        let rows = self.by_key.entry(key.to_owned()).or_default();
        let mut found = BTreeSet::new();
        for at in refs {
            let Some(block) = shard
                .bucket_index
                .bucket_map
                .get(&at.routing_bucket)
                .and_then(|bucket| bucket.block_index.get(&at.block_ref_key))
            else {
                continue;
            };
            let physical = live_block_key(&block.address);
            if !block.deleted() && wanted.contains(&physical) {
                found.insert(physical);
                rows.push(crate::index_log::IndexItem {
                    kind: crate::index_log::IndexItemKind::Block,
                    routing_bucket: at.routing_bucket,
                    block_ref_key: state::block_index_written_key(key, &block),
                    object_key: Arc::from(key),
                    model_id: "hash".into(),
                    object_id: stable_block_object_id(shard_id, "hash", key),
                    deleted: true,
                    entry: Some(block.address.clone()),
                });
            }
        }
        wanted.retain(|physical| !found.contains(physical));
        if !wanted.is_empty() {
            blocked.insert(key.to_owned());
        }
    }

    fn vacated(
        self,
        shard: &ShardState,
        sparse_keys: &BTreeSet<String>,
    ) -> Vec<crate::index_log::IndexItem> {
        let mut retired = Vec::new();
        for (key, rows) in self.by_key {
            if !sparse_keys.contains(&key) {
                continue;
            }
            let mut vacant: BTreeSet<_> = rows
                .iter()
                .filter_map(|row| row.entry.as_ref())
                .map(live_block_key)
                .collect();
            if let Some(fields) = shard.bucket_index.hashes.get(&key) {
                for address in fields.values() {
                    vacant.remove(&live_block_key(address));
                    if vacant.is_empty() {
                        break;
                    }
                }
            }
            let mut emitted = BTreeSet::new();
            for row in rows {
                let physical = live_block_key(row.entry.as_ref().expect("captured address"));
                if vacant.contains(&physical) && emitted.insert((row.routing_bucket, physical)) {
                    retired.push(row);
                }
            }
        }
        retired
    }
}

pub(super) struct MixedHashBatchDelta {
    pub snapshot_keys: Vec<String>,
    sparse_keys: BTreeSet<String>,
    names: Vec<(&'static str, String, Option<String>)>,
    predecessors: HashPredecessors,
}

impl MixedHashBatchDelta {
    pub(super) fn new(
        keys: &[String],
        touched: &[TouchedContainerElement],
        snapshot_keys: &BTreeSet<String>,
        predecessors: HashPredecessors,
    ) -> Option<Self> {
        let names: Vec<_> = touched
            .iter()
            .filter_map(|element| match element {
                TouchedContainerElement::Hash { key, field } if !snapshot_keys.contains(key) => {
                    Some(("hash", key.clone(), Some(field.clone())))
                }
                _ => None,
            })
            .collect();
        let sparse_keys: BTreeSet<_> = names.iter().map(|(_, key, _)| key.clone()).collect();
        if sparse_keys.is_empty() {
            return None;
        }
        let snapshot_keys: Vec<_> = keys
            .iter()
            .filter(|key| !sparse_keys.contains(*key))
            .cloned()
            .collect();
        if snapshot_keys.is_empty() {
            return None;
        }
        Some(Self {
            snapshot_keys,
            sparse_keys,
            names,
            predecessors,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn append(
        self,
        store: &crate::index_log::LocalIndexLogStore,
        shard_id: ShardId,
        shard: &ShardState,
        start: u32,
        end: u32,
        snapshot_items: Vec<crate::index_log::IndexItem>,
        key_states: Vec<serde_json::Value>,
        applied_wal_sequence: Option<u64>,
        durable: bool,
    ) -> Result<u64, crate::index_log::IndexLogError> {
        let (sparse_states, snapshot_states): (Vec<_>, Vec<_>) =
            key_states.into_iter().partition(|blob| {
                blob.get("key")
                    .and_then(serde_json::Value::as_str)
                    .is_some_and(|key| self.sparse_keys.contains(key))
            });
        // Only the suffix covers the complete WAL batch. A torn prefix must leave replay active.
        let first = store.append_delta(
            shard_id,
            snapshot_items,
            snapshot_states,
            None,
            None,
            false,
            durable,
        )?;
        if first == 0 {
            return Ok(0);
        }
        let mut items = self.predecessors.vacated(shard, &self.sparse_keys);
        items.extend(collect_upsert_index_items(
            shard,
            shard_id,
            &self.names,
            start,
            end,
        ));
        store.append_delta(
            shard_id,
            items,
            sparse_states,
            applied_wal_sequence,
            None,
            true,
            durable,
        )?;
        // Reclaim must retain the prefix as well as the WAL-covering suffix.
        Ok(first)
    }
}

#[cfg(test)]
#[path = "mixed_hash_batch_delta_tests.rs"]
mod tests;
