// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Dirty-shard tracking + write-command classification, extracted from data_node.rs.

use super::*;
use std::collections::BTreeSet;
use std::sync::Mutex;
use crate::types::{Command, ExecuteResponse, ShardId};

pub(super) fn mark_dirty(dirty: &Mutex<DirtyTracker>, shard_id: ShardId, key: Option<&str>) {
    let Some(key) = key else {
        return;
    };
    mark_dirty_owned(dirty, shard_id, key.to_owned());
}

pub(super) fn mark_dirty_owned(
    dirty: &Mutex<DirtyTracker>,
    shard_id: ShardId,
    key: String,
) {
    let mut dirty = dirty.lock().expect("dirty tracker lock poisoned");
    mark_dirty_in(&mut dirty, shard_id, key, now_ms());
}

fn mark_dirty_in(
    dirty: &mut DirtyTracker,
    shard_id: ShardId,
    key: String,
    now: u64,
) {
    let next_object_id = dirty.next_object_id + 1;
    let inserted = match dirty.by_key.entry((shard_id, key)) {
        std::collections::hash_map::Entry::Occupied(mut entry) => {
            entry.get_mut().last_dirty_at_ms = now;
            false
        }
        std::collections::hash_map::Entry::Vacant(entry) => {
            entry.insert(DirtyObjectState {
                object_id: next_object_id,
                last_dirty_at_ms: now,
            });
            true
        }
    };
    if inserted {
        dirty.next_object_id = next_object_id;
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
    dirty: &Mutex<DirtyTracker>,
    shard_id: ShardId,
    keys: &[(usize, String, bool)],
    responses: &[ExecuteResponse],
) {
    let mut dirty = dirty.lock().expect("dirty tracker lock poisoned");
    for (index, key, conditional) in keys {
        if responses
            .get(*index)
            .map(|response| {
                response.status.ok
                    && (!conditional
                        || matches!(
                            &response.response,
                            CommandResponse::Integer { value } if *value > 0
                        ))
            })
            .unwrap_or(false)
        {
            mark_dirty_in(&mut dirty, shard_id, key.clone(), now_ms());
        }
    }
}

pub(super) fn mark_dirty_for_successful_row_keys(
    dirty: &Mutex<DirtyTracker>,
    shard_id: ShardId,
    keys: Vec<String>,
    responses: &[ExecuteResponse],
    conditional: bool,
) {
    let mut dirty = dirty.lock().expect("dirty tracker lock poisoned");
    for (key, response) in keys.into_iter().zip(responses) {
        if response.status.ok
            && (!conditional
                || matches!(
                    &response.response,
                    CommandResponse::Integer { value } if *value > 0
                ))
        {
            mark_dirty_in(&mut dirty, shard_id, key, now_ms());
        }
    }
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
