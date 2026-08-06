use matrixcache::MultiLayerCache;

use crate::block_store::LocalBlockStore;
use crate::types::ShardId;

use super::page_reads::read_page_bytes_batch;
use super::slot_store::{
    read_slot_index_component_values, read_slot_index_value, slot_index_component_page_addresses,
};
use super::state::ShardState;

pub(super) fn read_hash_multi_values(
    cache: &MultiLayerCache,
    page_store: &LocalBlockStore,
    shard_id: ShardId,
    shard: &ShardState,
    key: &str,
    fields: &[String],
) -> Vec<Option<Vec<u8>>> {
    if fields.is_empty() {
        return Vec::new();
    }
    if let Some(hash_fields) = shard.hashes.get(key) {
        let addresses = fields
            .iter()
            .map(|field| hash_fields.get(field).cloned())
            .collect::<Vec<_>>();
        return read_page_bytes_batch(cache, page_store, shard_id, &addresses);
    }
    fields
        .iter()
        .map(|field| {
            read_slot_index_value(
                cache,
                page_store,
                shard_id,
                shard,
                "hash",
                key,
                Some(field.as_str()),
            )
        })
        .collect()
}

pub(super) fn read_hash_len(shard: &ShardState, key: &str) -> i64 {
    shard.hashes.get(key).map_or_else(
        || slot_index_component_page_addresses(shard, "hash", key).len() as i64,
        |fields| fields.len() as i64,
    )
}

pub(super) fn read_set_members(
    cache: &MultiLayerCache,
    page_store: &LocalBlockStore,
    shard_id: ShardId,
    shard: &ShardState,
    key: &str,
) -> Vec<Vec<u8>> {
    shard
        .sets
        .get(key)
        .map(|members| members.keys().cloned().collect())
        .unwrap_or_else(|| {
            read_slot_index_component_values(cache, page_store, shard_id, shard, "set", key)
                .into_iter()
                .map(|(_, value)| value)
                .collect()
        })
}
