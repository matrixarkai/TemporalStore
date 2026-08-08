//! Dirty-shard tracking + write-command classification, extracted from data_node.rs.

use super::*;
use std::collections::BTreeSet;
use std::collections::HashSet;
use std::sync::Mutex;
use crate::types::{Command, ExecuteResponse, ShardId};

pub(super) fn mark_dirty(dirty: &Mutex<DirtyTracker>, shard_id: ShardId, key: Option<&str>) {
    let Some(key) = key else {
        return;
    };
    let mut dirty = dirty.lock().expect("dirty tracker lock poisoned");
    let object_key = (shard_id, key.to_string());
    let now = now_ms();
    if !dirty.by_key.contains_key(&object_key) {
        dirty.next_object_id += 1;
        let object_id = dirty.next_object_id;
        dirty.by_key.insert(
            object_key.clone(),
            DirtyObjectInfo {
                shard_id,
                key: key.to_string(),
                object_id,
                last_dirty_at_ms: now,
            },
        );
    }
    if let Some(entry) = dirty.by_key.get_mut(&object_key) {
        entry.last_dirty_at_ms = now;
    }
}

pub(super) fn clear_dirty_shard_buckets(
    dirty: &Mutex<DirtyTracker>,
    engine: &TemporalEngine,
    shard_id: ShardId,
    selected_buckets: &[u32],
) -> usize {
    if selected_buckets.is_empty() {
        return 0;
    }
    let selected_buckets = selected_buckets.iter().copied().collect::<BTreeSet<_>>();
    let mut dirty = dirty.lock().expect("dirty tracker lock poisoned");
    let before = dirty.by_key.len();
    dirty.by_key.retain(|(dirty_shard_id, key), _| {
        *dirty_shard_id != shard_id
            || !selected_buckets.contains(&engine.routing_bucket_for_key(shard_id, key))
    });
    before - dirty.by_key.len()
}

pub(super) fn dirty_shards(dirty: &Mutex<DirtyTracker>) -> Vec<ShardId> {
    dirty
        .lock()
        .expect("dirty tracker lock poisoned")
        .by_key
        .keys()
        .map(|(shard_id, _)| *shard_id)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

pub(super) fn mark_dirty_for_successful_commands(
    dirty: &Mutex<DirtyTracker>,
    shard_id: ShardId,
    commands: &[Command],
    responses: &[ExecuteResponse],
) {
    for (command, response) in commands.iter().zip(responses.iter()) {
        if response.status.ok && is_write_command(command) {
            mark_dirty(dirty, shard_id, command_key(command).as_deref());
        }
    }
}

// Delegate to the engine's canonical object-key extractor (same one used for validation), so
// the dirty tracker records a key for EVERY write command -- including the ones this used to
// omit (context, control-state change/fol, ips load/remove/delete, conditional string). Those
// omissions left such writes untracked, so their shard was never scheduled for a dirty-driven
// dump and its WAL grew unbounded. Returns the first (primary) object key.
pub(super) fn command_key(command: &Command) -> Option<String> {
    crate::engine::command_object_keys(command).into_iter().next()
}

// Delegate to the engine's authoritative write-command classifier (the same one that gates WAL
// persistence). The data_node lifecycle write-barrier, dirty tracking and dump scheduling MUST
// use it so they can never drift: a stale subset here mis-classified context / control-state
// (change/fol) / ips (load/remove/delete) / conditional-string writes as READS, letting them
// bypass the lifecycle write gate (execute against a shard being torn down) and never mark the
// shard dirty (so it was never scheduled for a dump and its WAL grew unbounded).
pub(super) fn is_write_command(command: &Command) -> bool {
    crate::engine::is_write_command(command)
}
