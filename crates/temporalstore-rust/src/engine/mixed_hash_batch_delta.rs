// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

pub(super) fn can_split(commands: &[Command]) -> bool {
    // Keep other command families on their existing snapshot path.
    commands
        .iter()
        .any(|command| matches!(command, Command::SetRemove { .. }))
        && commands.iter().all(|command| {
            matches!(
                command,
                Command::HashSet { .. }
                    | Command::HashMultiSet { .. }
                    | Command::SetAdd { .. }
                    | Command::SetRemove { .. }
            )
        })
}

pub(super) fn keys_have_no_deadlines(shard: &ShardState, commands: &[Command]) -> bool {
    commands.iter().all(|command| match command {
        Command::HashSet { key, .. }
        | Command::HashMultiSet { key, .. }
        | Command::SetAdd { key, .. }
        | Command::SetRemove { key, .. } => !shard.expires_at_ms().contains_key(key.as_str()),
        _ => false,
    })
}

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

#[derive(Default)]
pub(super) struct SetPredecessors {
    by_key: BTreeMap<String, Vec<crate::index_log::IndexItem>>,
}

impl SetPredecessors {
    pub(super) fn capture(
        &mut self,
        shard: &ShardState,
        shard_id: ShardId,
        command: &Command,
    ) -> bool {
        let Command::SetRemove { key, member } = command else {
            return true;
        };
        let Some(address) = shard
            .bucket_index
            .sets
            .get(key)
            .and_then(|members| members.get(member))
        else {
            return true;
        };
        let physical = live_block_key(address);
        let Some(refs) = shard.bucket_index.block_refs_for("set", key, None) else {
            return false;
        };
        let mut found = false;
        for at in refs {
            let Some(block) = shard
                .bucket_index
                .bucket_map
                .get(&at.routing_bucket)
                .and_then(|bucket| bucket.block_index.get(&at.block_ref_key))
            else {
                continue;
            };
            if !block.deleted()
                && block.model_id() == storage_bucket_internals::stored_model_kind("set")
                && live_block_key(&block.address) == physical
            {
                found = true;
                self.by_key
                    .entry(key.clone())
                    .or_default()
                    .push(crate::index_log::IndexItem {
                        kind: crate::index_log::IndexItemKind::Block,
                        routing_bucket: at.routing_bucket,
                        block_ref_key: state::block_index_written_key(key, &block),
                        object_key: Arc::from(key.as_str()),
                        model_id: "set".into(),
                        object_id: stable_block_object_id(shard_id, "set", key),
                        deleted: true,
                        entry: Some(block.address.clone()),
                    });
            }
        }
        found
    }

    fn fully_vacated(self, shard: &ShardState) -> Option<Vec<crate::index_log::IndexItem>> {
        let mut retired = Vec::new();
        for (key, rows) in self.by_key {
            let wanted: BTreeSet<_> = rows
                .iter()
                .filter_map(|row| row.entry.as_ref())
                .map(live_block_key)
                .collect();
            if shard.bucket_index.sets.get(&key).is_some_and(|members| {
                members
                    .values()
                    .any(|address| wanted.contains(&live_block_key(address)))
            }) {
                // A shared frame needs the existing full-snapshot representation of removal.
                return None;
            }
            let mut emitted = BTreeSet::new();
            for row in rows {
                let physical = live_block_key(row.entry.as_ref().expect("captured set address"));
                if emitted.insert((row.routing_bucket, physical)) {
                    retired.push(row);
                }
            }
        }
        (!retired.is_empty()).then_some(retired)
    }
}

pub(super) struct MixedHashBatchDelta {
    sparse_keys: BTreeSet<String>,
    names: Vec<(&'static str, String, Option<String>)>,
    predecessors: HashPredecessors,
    retired_sets: Vec<crate::index_log::IndexItem>,
}

impl MixedHashBatchDelta {
    pub(super) fn new(
        shard: &ShardState,
        touched: &[TouchedContainerElement],
        predecessors: HashPredecessors,
        sets: SetPredecessors,
    ) -> Option<Self> {
        let names: Vec<_> = touched
            .iter()
            .filter_map(|element| match element {
                TouchedContainerElement::Hash { key, field } => {
                    Some(("hash", key.clone(), Some(field.clone())))
                }
                TouchedContainerElement::Set { key, member } => Some((
                    "set",
                    key.clone(),
                    Some(execute_on_shard::set_element_name(member)),
                )),
                _ => None,
            })
            .collect();
        let sparse_keys: BTreeSet<_> = names
            .iter()
            .filter(|(kind, _, _)| *kind == "hash")
            .map(|(_, key, _)| key.clone())
            .collect();
        if sparse_keys.is_empty() {
            return None;
        }
        Some(Self {
            sparse_keys,
            names,
            predecessors,
            retired_sets: sets.fully_vacated(shard)?,
        })
    }

    pub(super) fn items(
        self,
        shard_id: ShardId,
        shard: &ShardState,
        start: u32,
        end: u32,
    ) -> Vec<crate::index_log::IndexItem> {
        let mut items = self.retired_sets;
        items.extend(self.predecessors.vacated(shard, &self.sparse_keys));
        items.extend(collect_upsert_index_items(
            shard,
            shard_id,
            &self.names,
            start,
            end,
        ));
        items
    }
}

#[cfg(test)]
#[path = "mixed_hash_batch_delta_tests.rs"]
mod tests;
