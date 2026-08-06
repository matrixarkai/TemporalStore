use matrixcache::MultiLayerCache;

use crate::types::ShardId;

use super::records::visit_associated_record_keys;
use super::{
    invalidate_record_all, mark_bucket_index_object_deleted, record_exists, record_exists_exact,
    ShardState,
};

pub(super) fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}

pub(super) fn ttl_ms(
    cache: &MultiLayerCache,
    shard_id: ShardId,
    shard: &mut ShardState,
    key: &str,
) -> i64 {
    if remove_if_expired(cache, shard_id, shard, key) {
        return -2;
    }
    if !record_exists(shard, key) {
        return -2;
    }
    let now = now_ms();
    let mut min_ttl = None;
    visit_associated_record_keys(key, |record_key| {
        if let Some(expires_at) = shard.expires_at_ms.get(record_key).copied() {
            let ttl = expires_at.saturating_sub(now) as i64;
            min_ttl = Some(min_ttl.map_or(ttl, |current: i64| current.min(ttl)));
        }
    });
    min_ttl.unwrap_or(-1)
}

pub(super) fn associated_record_expired(shard: &ShardState, key: &str, now: u64) -> bool {
    let mut expired = false;
    visit_associated_record_keys(key, |record_key| {
        expired |= shard
            .expires_at_ms
            .get(record_key)
            .map(|expires_at| *expires_at <= now)
            .unwrap_or(false);
    });
    expired
}

pub(super) fn read_only_ttl_ms(shard: &ShardState, key: &str) -> Option<i64> {
    let now = now_ms();
    if associated_record_expired(shard, key, now) {
        return None;
    }
    if !record_exists(shard, key) {
        return Some(-2);
    }
    let mut min_ttl = None;
    visit_associated_record_keys(key, |record_key| {
        if let Some(expires_at) = shard.expires_at_ms.get(record_key).copied() {
            let ttl = expires_at.saturating_sub(now) as i64;
            min_ttl = Some(min_ttl.map_or(ttl, |current: i64| current.min(ttl)));
        }
    });
    Some(min_ttl.unwrap_or(-1))
}

pub(super) fn select_expiry_cursor_window(
    keys: Vec<(String, u64)>,
    cursor: Option<&str>,
    limit: usize,
) -> (Vec<(String, u64)>, Option<String>) {
    let start = cursor
        .map(|cursor| keys.partition_point(|(key, _)| key.as_str() <= cursor))
        .unwrap_or_default();
    let mut remaining = keys.into_iter().skip(start);
    if limit == 0 {
        let remaining_len = remaining.size_hint().0;
        let mut selected = Vec::with_capacity(remaining_len);
        selected.extend(remaining);
        return (selected, None);
    }
    let remaining_len = remaining.size_hint().0;
    let mut selected = Vec::with_capacity(limit.min(remaining_len));
    selected.extend(remaining.by_ref().take(limit));
    let next_cursor = remaining
        .next()
        .and_then(|_| selected.last().map(|(key, _)| key.clone()));
    (selected, next_cursor)
}

pub(super) fn remove_if_expired(
    cache: &MultiLayerCache,
    shard_id: ShardId,
    shard: &mut ShardState,
    key: &str,
) -> bool {
    let now = now_ms();
    let mut removed = false;
    visit_associated_record_keys(key, |record_key| {
        if shard
            .expires_at_ms
            .get(record_key)
            .map(|expires_at| *expires_at <= now)
            .unwrap_or(false)
        {
            removed |= delete_record_exact(cache, shard_id, shard, record_key);
        }
    });
    removed
}

pub(super) fn delete_record(
    cache: &MultiLayerCache,
    shard_id: ShardId,
    shard: &mut ShardState,
    key: &str,
) -> bool {
    let mut removed = false;
    visit_associated_record_keys(key, |record_key| {
        removed |= delete_record_exact(cache, shard_id, shard, record_key);
    });
    removed
}

pub(super) fn delete_record_exact(
    cache: &MultiLayerCache,
    shard_id: ShardId,
    shard: &mut ShardState,
    key: &str,
) -> bool {
    if !record_exists_exact(shard, key)
        && !shard.expires_at_ms.contains_key(key)
        && !shard.ips_meta.contains_key(key)
        && !shard.ips_request_ids.contains_key(key)
    {
        return false;
    }
    let mut removed = false;
    removed |= mark_bucket_index_object_deleted(cache, shard_id, shard, key);
    removed |= shard.expires_at_ms.remove(key).is_some();
    removed |= shard.strings.remove(key).is_some();
    removed |= shard.hashes.remove(key).is_some();
    removed |= shard.sets.remove(key).is_some();
    removed |= shard.features.remove(key).is_some();
    removed |= shard.sequences.remove(key).is_some();
    removed |= shard.ips.remove(key).is_some();
    removed |= shard.ips_meta.remove(key).is_some();
    removed |= shard.ips_request_ids.remove(key).is_some();
    removed |= shard.control_state.remove(key).is_some();
    removed |= shard.control_state_pages.remove(key).is_some();
    removed |= shard.control_state_changes.remove(key).is_some();
    removed |= shard.control_state_fol.remove(key).is_some();
    removed |= shard.context_nodes.remove(key).is_some();
    removed |= shard.context_events.remove(key).is_some();
    removed |= shard.context_indexes.remove(key).is_some();
    removed |= shard.context_audits.remove(key).is_some();
    removed |= shard.context_dirty_index.remove(key).is_some();
    removed |= shard.context_entities.remove(key).is_some();
    removed |= shard.context_children.remove(key).is_some();
    removed |= shard.context_embeddings.remove(key).is_some();
    removed |= shard.context_summaries.remove(key).is_some();
    removed |= shard.context_compressions.remove(key).is_some();
    if removed {
        invalidate_record_all(cache, shard_id, key);
    }
    removed
}
