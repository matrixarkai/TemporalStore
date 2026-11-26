// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Dirty-shard tracking + write-command classification, extracted from data_node.rs.

use super::*;
use std::collections::BTreeSet;
use std::sync::Mutex;
use crate::types::{Command, ExecuteResponse, ShardId};

type DirtyKeys = BTreeMap<Arc<str>, DirtyObjectState>;
type DirtyOwner = Arc<Mutex<Option<DirtyKeys>>>;

#[derive(Debug, Default)]
pub(super) struct DirtyTracker {
    directory: Mutex<DirtyDirectory>,
    next_object_id: std::sync::atomic::AtomicU64,
}

#[derive(Debug, Default)]
struct DirtyDirectory {
    shards: BTreeMap<ShardId, DirtySlot>,
    objects: usize,
    dirty_shards: usize,
}

#[derive(Debug)]
struct DirtySlot {
    owner: DirtyOwner,
    count: usize,
}

impl DirtyTracker {
    fn directory(&self) -> std::sync::MutexGuard<'_, DirtyDirectory> {
        self.directory.lock().expect("dirty directory lock poisoned")
    }

    fn owner(&self, shard_id: ShardId) -> Option<DirtyOwner> {
        self.directory().shards.get(&shard_id).map(|slot| Arc::clone(&slot.owner))
    }

    fn owner_for_mark(&self, shard_id: ShardId) -> DirtyOwner {
        if let Some(owner) = self.owner(shard_id) {
            return owner;
        }
        let fresh = Arc::new(Mutex::new(Some(DirtyKeys::new())));
        let mut directory = self.directory();
        let owner = Arc::clone(&directory.shards.entry(shard_id).or_insert_with(|| DirtySlot {
            owner: Arc::clone(&fresh),
            count: 0,
        }).owner);
        drop(directory);
        owner
    }

    fn mark(&self, shard_id: ShardId, rows: impl IntoIterator<Item = String>, mut timestamp: impl FnMut() -> u64) {
        let mut rows = rows.into_iter().peekable();
        if rows.peek().is_none() {
            return;
        }
        loop {
            let owner = self.owner_for_mark(shard_id);
            let mut state = owner.lock().expect("dirty shard lock poisoned");
            let Some(keys) = state.as_mut() else {
                // A last-key clear retired this owner while we waited.
                continue;
            };
            let before = keys.len();
            for key in rows {
                let now = timestamp();
                if let Some(existing) = keys.get_mut(key.as_str()) {
                    existing.last_dirty_at_ms = now;
                    continue;
                }
                let object_id = self.next_object_id.fetch_update(
                    std::sync::atomic::Ordering::Relaxed,
                    std::sync::atomic::Ordering::Relaxed,
                    |id| id.checked_add(1),
                ).expect("dirty object id exhausted") + 1;
                keys.insert(Arc::from(key), DirtyObjectState {
                    object_id,
                    last_dirty_at_ms: now,
                });
            }
            let added = keys.len() - before;
            if added > 0 {
                let mut directory = self.directory();
                directory.shards.get_mut(&shard_id).expect("live dirty shard").count = keys.len();
                directory.objects += added;
                if before == 0 {
                    directory.dirty_shards += 1;
                }
            }
            return;
        }
    }

    fn clear_where(&self, shard_id: ShardId, mut matches: impl FnMut(&str) -> bool) -> usize {
        self.remove(shard_id, |keys| {
            let selected: Vec<_> = keys.keys().filter(|key| {
                #[cfg(test)]
                DIRTY_REMOVAL_VISITS.with(|visits| visits.set((visits.get().0 + 1, visits.get().1)));
                matches(key)
            }).cloned().collect();
            let removed: Vec<_> = selected.into_iter().filter_map(|key| {
                keys.remove_entry(key.as_ref())
            }).collect();
            let count = removed.len();
            (removed, count)
        })
    }

    pub(super) fn clear_key(&self, shard_id: ShardId, key: &str) -> bool {
        self.remove(shard_id, |keys| {
            #[cfg(test)]
            DIRTY_REMOVAL_VISITS.with(|visits| visits.set((visits.get().0, visits.get().1 + 1)));
            let removed = keys.remove_entry(key);
            let count = usize::from(removed.is_some());
            (removed, count)
        }) != 0
    }

    fn remove<R>(&self, shard_id: ShardId, remove: impl FnOnce(&mut DirtyKeys) -> (R, usize)) -> usize {
        let Some(owner) = self.owner(shard_id) else {
            return 0;
        };
        let mut state = owner.lock().expect("dirty shard lock poisoned");
        let Some(keys) = state.as_mut() else {
            return 0;
        };
        let (removed, count) = remove(keys);
        let remaining = keys.len();
        let mut retired = None;
        if count != 0 {
            let mut directory = self.directory();
            directory.objects -= count;
            if remaining == 0 {
                directory.dirty_shards -= 1;
                let removed_slot = directory.shards.remove(&shard_id).expect("live dirty shard");
                debug_assert!(Arc::ptr_eq(&owner, &removed_slot.owner));
                retired = state.take();
            } else {
                directory.shards.get_mut(&shard_id).expect("live dirty shard").count = remaining;
            }
        }
        drop(state);
        drop(retired);
        drop(removed);
        count
    }

    pub(super) fn counts(&self) -> (usize, usize) {
        let directory = self.directory();
        (directory.objects, directory.dirty_shards)
    }

    pub(super) fn counts_by_shard(&self) -> BTreeMap<ShardId, usize> {
        let counts: Vec<_> = self.directory().shards.iter().filter_map(|(&shard_id, slot)| {
            (slot.count != 0).then_some((shard_id, slot.count))
        }).collect();
        counts.into_iter().collect()
    }

    fn dirty_shards(&self) -> Vec<ShardId> {
        self.directory().shards.iter().filter_map(|(&shard_id, slot)| {
            (slot.count != 0).then_some(shard_id)
        }).collect()
    }

    // Each owner contributes one exact snapshot. Different owners can advance while the
    // listing is built, and no owner or directory guard spans a peer's snapshot.
    pub(super) fn objects(&self) -> Vec<DirtyObjectInfo> {
        let owners: Vec<_> = self.directory().shards.iter().map(|(&shard_id, slot)| {
            (shard_id, Arc::clone(&slot.owner))
        }).collect();
        let mut objects = Vec::new();
        for (shard_id, owner) in owners {
            let rows: Vec<_> = {
                let state = owner.lock().expect("dirty shard lock poisoned");
                state.iter().flatten().map(|(key, state)| {
                    (Arc::clone(key), state.object_id, state.last_dirty_at_ms)
                }).collect()
            };
            objects.extend(rows.into_iter().map(|(key, object_id, last_dirty_at_ms)| DirtyObjectInfo {
                shard_id,
                key: key.to_string(),
                object_id,
                last_dirty_at_ms,
            }));
        }
        objects
    }
}

#[cfg(test)]
thread_local! {
    // Count visiting a candidate during a scan separately from a point map operation.
    static DIRTY_REMOVAL_VISITS: std::cell::Cell<(usize, usize)> = const { std::cell::Cell::new((0, 0)) };
}


pub(super) fn mark_dirty(dirty: &DirtyTracker, shard_id: ShardId, key: Option<&str>) {
    let Some(key) = key else {
        return;
    };
    mark_dirty_owned(dirty, shard_id, key.to_owned());
}

pub(super) fn mark_dirty_owned(
    dirty: &DirtyTracker,
    shard_id: ShardId,
    key: String,
) {
    dirty.mark(shard_id, [key], now_ms);
}

pub(super) fn clear_dirty_shard_buckets(
    dirty: &DirtyTracker,
    engine: &TemporalEngine,
    shard_id: ShardId,
    selected_buckets: &[u32],
) -> usize {
    if selected_buckets.is_empty() {
        return 0;
    }
    let selected_buckets = selected_buckets.iter().copied().collect::<BTreeSet<_>>();
    let bucket_of = engine.routing_bucket_resolver(shard_id);
    dirty.clear_where(shard_id, |key| selected_buckets.contains(&bucket_of(key)))
}

pub(super) fn dirty_shards(dirty: &DirtyTracker) -> Vec<ShardId> {
    dirty.dirty_shards()
}

#[cfg(test)]
#[path = "dirty_count_tests.rs"]
mod dirty_count_tests;

pub(super) fn mark_dirty_for_successful_commands(
    dirty: &DirtyTracker,
    shard_id: ShardId,
    commands: &[Command],
    responses: &[ExecuteResponse],
) {
    for (command, response) in commands.iter().zip(responses.iter()) {
        if command_was_applied(command, response) {
            mark_dirty(dirty, shard_id, command_key(command).as_deref());
        }
    }
}

pub(super) fn write_command_keys(commands: &[Command]) -> Vec<(usize, String, bool)> {
    commands
        .iter()
        .enumerate()
        .filter_map(|(index, command)| {
            if is_write_command(command) {
                command_key(command).map(|key| {
                    (
                        index,
                        key,
                        matches!(command, Command::HashMultiSetIfTimestampOlder { .. }),
                    )
                })
            } else {
                None
            }
        })
        .collect()
}

pub(super) fn mark_dirty_for_successful_keys(
    dirty: &DirtyTracker,
    shard_id: ShardId,
    keys: &[(usize, String, bool)],
    responses: &[ExecuteResponse],
) {
    let applied = keys.iter().filter_map(|(index, key, conditional)| {
        responses.get(*index).filter(|response| {
            response.status.ok
                && (!conditional
                    || matches!(&response.response, CommandResponse::Integer { value } if *value > 0))
        }).map(|_| key.clone())
    });
    dirty.mark(shard_id, applied, now_ms);
}

pub(super) fn mark_dirty_keys(
    dirty: &DirtyTracker,
    shard_id: ShardId,
    keys: Vec<String>,
) {
    let mut timestamp = None;
    dirty.mark(shard_id, keys, || *timestamp.get_or_insert_with(now_ms));
}

pub(super) fn command_was_applied(command: &Command, response: &ExecuteResponse) -> bool {
    response.status.ok
        && (!matches!(command, Command::HashMultiSetIfTimestampOlder { .. })
            || matches!(&response.response, CommandResponse::Integer { value } if *value > 0))
        && is_write_command(command)
}

// Delegate to the engine's canonical object-key extractor (same one used for validation), so
// the dirty tracker records a key for EVERY write command -- including the ones this used to
// omit (context, control-state change/selection, conditional string). Those
// omissions left such writes untracked, so their shard was never scheduled for a dirty-driven
// dump and its WAL grew unbounded. Returns the first (primary) object key.
pub(super) fn command_key(command: &Command) -> Option<String> {
    crate::engine::command_object_keys(command).into_iter().next()
}

// Delegate to the engine's authoritative write-command classifier (the same one that gates WAL
// persistence). The data_node lifecycle write-barrier, dirty tracking and dump scheduling MUST
// use it so they can never drift: a stale subset here mis-classified context / control-state
// (change/selection) / conditional-string writes as READS, letting them
// bypass the lifecycle write gate (execute against a shard being torn down) and never mark the
// shard dirty (so it was never scheduled for a dump and its WAL grew unbounded).
pub(super) fn is_write_command(command: &Command) -> bool {
    crate::engine::is_write_command(command)
}
