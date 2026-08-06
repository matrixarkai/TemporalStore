use super::*;

pub(super) fn append_value(
    cache: &MultiLayerCache,
    page_store: &LocalBlockStore,
    shard_id: ShardId,
    bytes: &[u8],
    object_id: Option<u64>,
    routing_bucket: Option<u32>,
    async_storage: bool,
) -> Result<BlockAddress, BlockStoreError> {
    if !async_storage {
        return page_store.append_with_page_metadata(bytes, object_id, routing_bucket);
    }
    let address = BlockAddress {
        page_segment_id: HOT_PAGE_SEGMENT_ID,
        offset: HOT_PAGE_OFFSET.fetch_add(1, Ordering::Relaxed),
        length: bytes.len() as u64,
        page_id: None,
        object_id,
        routing_bucket,
        generation: object_id,
        band_id: None,
        sha256: None,
    };
    let bytes = bytes.to_vec();
    cache.put_memory_only(
        CacheKey::page_with_slot_generation(
            shard_id,
            address.page_segment_id,
            address.offset,
            address.length,
            address.routing_bucket,
            address.generation,
        ),
        bytes,
    );
    Ok(address)
}

pub(super) fn append_timestamped_single_pages_batch(
    cache: &MultiLayerCache,
    page_store: &LocalBlockStore,
    shard_id: ShardId,
    writes: &[TimestampedPageBatchWrite],
    async_storage: bool,
) -> Option<Vec<BlockAddress>> {
    if writes.is_empty() {
        return Some(Vec::new());
    }
    if writes.len() == 1 {
        let write = &writes[0];
        let object_id = stable_page_object_id(shard_id, write.kind, &write.object_key, None);
        let packed = encode_single_feature_page(write.timestamp_ms, &write.value);
        if !async_storage {
            return page_store
                .append_with_page_metadata(&packed, Some(object_id), Some(write.routing_bucket))
                .ok()
                .map(|address| vec![address]);
        }
        let address = BlockAddress {
            page_segment_id: HOT_PAGE_SEGMENT_ID,
            offset: HOT_PAGE_OFFSET.fetch_add(1, Ordering::Relaxed),
            length: packed.len() as u64,
            page_id: None,
            object_id: Some(object_id),
            routing_bucket: Some(write.routing_bucket),
            generation: Some(object_id),
            band_id: None,
            sha256: None,
        };
        cache.put_memory_only(
            CacheKey::page_with_slot_generation(
                shard_id,
                address.page_segment_id,
                address.offset,
                address.length,
                address.routing_bucket,
                address.generation,
            ),
            packed,
        );
        return Some(vec![address]);
    }
    if !async_storage {
        let mut append_records = Vec::with_capacity(writes.len());
        for write in writes {
            let object_id = stable_page_object_id(shard_id, write.kind, &write.object_key, None);
            append_records.push((
                encode_single_feature_page(write.timestamp_ms, &write.value),
                Some(object_id),
                Some(write.routing_bucket),
            ));
        }
        return page_store
            .append_batch_with_page_metadata(append_records)
            .ok();
    }

    let start_offset = HOT_PAGE_OFFSET.fetch_add(writes.len() as u64, Ordering::Relaxed);
    let mut addresses = Vec::with_capacity(writes.len());
    for (index, write) in writes.iter().enumerate() {
        let object_id = stable_page_object_id(shard_id, write.kind, &write.object_key, None);
        let packed = encode_single_feature_page(write.timestamp_ms, &write.value);
        let address = BlockAddress {
            page_segment_id: HOT_PAGE_SEGMENT_ID,
            offset: start_offset.saturating_add(index as u64),
            length: packed.len() as u64,
            page_id: None,
            object_id: Some(object_id),
            routing_bucket: Some(write.routing_bucket),
            generation: Some(object_id),
            band_id: None,
            sha256: None,
        };
        cache.put_memory_only(
            CacheKey::page_with_slot_generation(
                shard_id,
                address.page_segment_id,
                address.offset,
                address.length,
                address.routing_bucket,
                address.generation,
            ),
            packed,
        );
        addresses.push(address);
    }
    Some(addresses)
}

pub(super) fn hash_multiset_batch_memory_put_min() -> usize {
    static MIN: OnceLock<usize> = OnceLock::new();
    *MIN.get_or_init(|| {
        env::var("TEMPORALSTORE_HASH_MULTISET_BATCH_MEMORY_PUT_MIN")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(2)
            .max(2)
    })
}

pub(super) fn persist_control_state_page(
    cache: &MultiLayerCache,
    page_store: &LocalBlockStore,
    shard_id: ShardId,
    shard: &mut ShardState,
    key: &str,
    start_routing_bucket: u32,
    end_routing_bucket: u32,
    async_storage: bool,
) -> bool {
    let Some(series) = shard.control_state.get(key) else {
        shard.control_state_pages.remove(key);
        return false;
    };
    let Ok(bytes) = serde_json::to_vec(series) else {
        return false;
    };
    let object_id = stable_page_object_id(shard_id, "control_state", key, None);
    let routing_bucket = page_routing_bucket(key, start_routing_bucket, end_routing_bucket);
    if let Ok(address) = append_value(
        cache,
        page_store,
        shard_id,
        &bytes,
        Some(object_id),
        Some(routing_bucket),
        async_storage,
    ) {
        upsert_bucket_index_page(
            cache,
            shard,
            shard_id,
            "control_state",
            key,
            None,
            address.clone(),
            true,
            start_routing_bucket,
            end_routing_bucket,
        );
        shard.control_state_pages.insert(key.to_string(), address);
        true
    } else {
        false
    }
}

