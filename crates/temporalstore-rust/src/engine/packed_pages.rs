use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::Ordering;

use crate::block_store::{BlockAddress, BlockStoreError, LocalBlockStore};
use crate::types::{FeaturePoint, ShardId};
use rustmtcache::{CacheKey, MultiLayerCache};

use super::constants::{FEATURE_PAGE_MAGIC, HOT_PAGE_OFFSET, HOT_PAGE_SEGMENT_ID};
use super::state::{PackedFeaturePage, PackedFeaturePageDecode};
use super::{read_page_bytes, read_page_bytes_batch, read_page_bytes_cold, stable_page_object_id};
use crate::storage_config::{cold_scan_no_cache_fill, context_page_target_bytes};

const COLD_SCAN_PACKED_PAGE_CACHE_LIMIT: usize = 128;

pub(super) struct ColdScanPackedPageCache {
    pages: HashMap<BlockAddress, Option<Vec<FeaturePoint>>>,
    order: VecDeque<BlockAddress>,
    limit: usize,
}

impl Default for ColdScanPackedPageCache {
    fn default() -> Self {
        Self {
            pages: HashMap::new(),
            order: VecDeque::new(),
            limit: COLD_SCAN_PACKED_PAGE_CACHE_LIMIT,
        }
    }
}

impl ColdScanPackedPageCache {
    pub(super) fn get(&self, address: &BlockAddress) -> Option<&Option<Vec<FeaturePoint>>> {
        self.pages.get(address)
    }

    pub(super) fn insert(&mut self, address: BlockAddress, points: Option<Vec<FeaturePoint>>) {
        if !self.pages.contains_key(&address) {
            while self.pages.len() >= self.limit {
                let Some(evicted) = self.order.pop_front() else {
                    break;
                };
                self.pages.remove(&evicted);
            }
            self.order.push_back(address.clone());
        }
        self.pages.insert(address, points);
    }
}

pub(super) fn sorted_feature_points(mut points: Vec<FeaturePoint>) -> Vec<FeaturePoint> {
    if points
        .windows(2)
        .all(|window| window[0].timestamp_ms < window[1].timestamp_ms)
    {
        return points;
    }
    let mut by_timestamp = BTreeMap::new();
    for point in points.drain(..) {
        by_timestamp.insert(point.timestamp_ms, point);
    }
    by_timestamp.into_values().collect()
}

pub(super) fn encode_feature_page(points: &[FeaturePoint]) -> Vec<u8> {
    let page = PackedFeaturePage {
        version: 1,
        points: points.to_vec(),
    };
    if let Ok(mut payload) = serde_json::to_vec(&page) {
        let mut bytes = Vec::with_capacity(FEATURE_PAGE_MAGIC.len() + payload.len());
        bytes.extend_from_slice(FEATURE_PAGE_MAGIC);
        bytes.append(&mut payload);
        bytes
    } else {
        FEATURE_PAGE_MAGIC.to_vec()
    }
}

fn empty_feature_page_encoded_len() -> usize {
    FEATURE_PAGE_MAGIC.len()
        + serde_json::to_vec(&PackedFeaturePage {
            version: 1,
            points: Vec::new(),
        })
        .map(|bytes| bytes.len())
        .unwrap_or_default()
}

fn feature_point_encoded_len(point: &FeaturePoint) -> usize {
    serde_json::to_vec(point)
        .map(|bytes| bytes.len())
        .unwrap_or_default()
}

pub(super) fn append_timestamped_kv_pages(
    cache: &MultiLayerCache,
    block_store: &LocalBlockStore,
    shard_id: ShardId,
    kind: &str,
    key: &str,
    points: Vec<FeaturePoint>,
    routing_slot: u32,
    async_storage: bool,
) -> Result<Vec<(u64, BlockAddress)>, BlockStoreError> {
    let object_id = stable_page_object_id(shard_id, kind, key, None);
    let point_count = points.len();
    let mut refs = Vec::with_capacity(point_count);
    let chunks = chunk_timestamped_kv_points(points);
    if !async_storage {
        let mut writes = Vec::with_capacity(chunks.len());
        let mut chunk_points = Vec::with_capacity(chunks.len());
        for chunk in chunks {
            writes.push((
                encode_feature_page(&chunk),
                Some(object_id),
                Some(routing_slot),
            ));
            chunk_points.push(chunk);
        }
        let addresses = block_store.append_batch_with_page_metadata(writes)?;
        if addresses.len() != chunk_points.len() {
            return Err(BlockStoreError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "batch page append returned fewer addresses than chunks",
            )));
        }
        for (chunk, address) in chunk_points.into_iter().zip(addresses) {
            refs.extend(
                chunk
                    .into_iter()
                    .map(|point| (point.timestamp_ms, address.clone())),
            );
        }
        return Ok(refs);
    }

    let start_offset = HOT_PAGE_OFFSET.fetch_add(chunks.len() as u64, Ordering::Relaxed);
    let mut page_cache_entries = Vec::with_capacity(chunks.len());
    for (index, chunk) in chunks.into_iter().enumerate() {
        let packed = encode_feature_page(&chunk);
        let address = BlockAddress {
            page_segment_id: HOT_PAGE_SEGMENT_ID,
            offset: start_offset.saturating_add(index as u64),
            length: packed.len() as u64,
            page_id: None,
            object_id: Some(object_id),
            routing_slot: Some(routing_slot),
            generation: Some(object_id),
            extent_id: None,
            sha256: None,
        };
        page_cache_entries.push((
            CacheKey::page_with_slot_generation(
                shard_id,
                address.page_segment_id,
                address.offset,
                address.length,
                address.routing_slot,
                address.generation,
            ),
            packed,
        ));
        refs.extend(
            chunk
                .into_iter()
                .map(|point| (point.timestamp_ms, address.clone())),
        );
    }
    for (key, bytes) in page_cache_entries {
        cache.put_memory_only(key, bytes);
    }
    Ok(refs)
}

pub(super) fn chunk_timestamped_kv_points(points: Vec<FeaturePoint>) -> Vec<Vec<FeaturePoint>> {
    let point_count = points.len();
    let current_capacity = point_count.min(128);
    let mut chunks =
        Vec::with_capacity(point_count.saturating_sub(1) / current_capacity.max(1) + 1);
    let mut current = Vec::with_capacity(current_capacity);
    let empty_page_len = empty_feature_page_encoded_len();
    let mut current_encoded_len = empty_page_len;
    let page_target_bytes = context_page_target_bytes();

    for point in points {
        let point_encoded_len = feature_point_encoded_len(&point);
        let next_encoded_len = current_encoded_len
            .saturating_add(point_encoded_len)
            .saturating_add(if current.is_empty() { 0 } else { 1 });
        if next_encoded_len > page_target_bytes && !current.is_empty() {
            chunks.push(current);
            current = Vec::with_capacity(current_capacity);
            current_encoded_len = empty_page_len;
        }
        current_encoded_len = current_encoded_len
            .saturating_add(point_encoded_len)
            .saturating_add(if current.is_empty() { 0 } else { 1 });
        current.push(point);
    }

    if !current.is_empty() {
        chunks.push(current);
    }
    chunks
}

#[cfg(test)]
pub(super) fn decode_feature_page(bytes: &[u8]) -> Option<Vec<FeaturePoint>> {
    match decode_feature_page_strict(bytes) {
        PackedFeaturePageDecode::Packed(points) => Some(points),
        PackedFeaturePageDecode::Legacy | PackedFeaturePageDecode::Corrupt(_) => None,
    }
}

pub(super) fn decode_feature_page_strict(bytes: &[u8]) -> PackedFeaturePageDecode {
    let Some(payload) = bytes.strip_prefix(FEATURE_PAGE_MAGIC) else {
        return PackedFeaturePageDecode::Legacy;
    };
    let page = match serde_json::from_slice::<PackedFeaturePage>(payload) {
        Ok(page) => page,
        Err(err) => {
            return PackedFeaturePageDecode::Corrupt(format!(
                "invalid packed feature page payload: {err}"
            ));
        }
    };
    if page.version != 1 {
        return PackedFeaturePageDecode::Corrupt(format!(
            "unsupported packed feature page version {}",
            page.version
        ));
    }
    PackedFeaturePageDecode::Packed(page.points)
}

pub(super) fn read_feature_point_cold_with_cache_policy(
    cache: Option<&MultiLayerCache>,
    block_store: &LocalBlockStore,
    shard_id: ShardId,
    timestamp_ms: u64,
    address: &BlockAddress,
    packed_page_cache: &mut ColdScanPackedPageCache,
) -> Option<FeaturePoint> {
    if cold_scan_no_cache_fill() {
        return read_feature_point_cold_scan_cached(
            block_store,
            timestamp_ms,
            address,
            packed_page_cache,
        );
    }
    let Some(cache) = cache else {
        return read_feature_point_cold_scan_cached(
            block_store,
            timestamp_ms,
            address,
            packed_page_cache,
        );
    };
    read_feature_point_cache_fill_scan_cached(
        cache,
        block_store,
        shard_id,
        timestamp_ms,
        address,
        packed_page_cache,
    )
}

pub(super) fn read_feature_point_cold_scan_cached(
    block_store: &LocalBlockStore,
    timestamp_ms: u64,
    address: &BlockAddress,
    packed_page_cache: &mut ColdScanPackedPageCache,
) -> Option<FeaturePoint> {
    if let Some(points) = packed_page_cache.get(address) {
        return points
            .as_ref()
            .and_then(|points| {
                points
                    .iter()
                    .find(|point| point.timestamp_ms == timestamp_ms)
            })
            .cloned();
    }

    let bytes = read_page_bytes_cold(block_store, address)?;
    match decode_feature_page_strict(&bytes) {
        PackedFeaturePageDecode::Packed(points) => {
            let selected = points
                .iter()
                .find(|point| point.timestamp_ms == timestamp_ms)
                .cloned();
            packed_page_cache.insert(address.clone(), Some(points));
            selected
        }
        PackedFeaturePageDecode::Legacy => Some(FeaturePoint {
            timestamp_ms,
            value: bytes,
        }),
        PackedFeaturePageDecode::Corrupt(_) => {
            packed_page_cache.insert(address.clone(), None);
            None
        }
    }
}

pub(super) fn read_feature_point_cache_fill_scan_cached(
    cache: &MultiLayerCache,
    block_store: &LocalBlockStore,
    shard_id: ShardId,
    timestamp_ms: u64,
    address: &BlockAddress,
    packed_page_cache: &mut ColdScanPackedPageCache,
) -> Option<FeaturePoint> {
    if let Some(points) = packed_page_cache.get(address) {
        return points
            .as_ref()
            .and_then(|points| {
                points
                    .iter()
                    .find(|point| point.timestamp_ms == timestamp_ms)
            })
            .cloned();
    }

    let bytes = read_page_bytes(cache, block_store, shard_id, address)?;
    match decode_feature_page_strict(&bytes) {
        PackedFeaturePageDecode::Packed(points) => {
            let selected = points
                .iter()
                .find(|point| point.timestamp_ms == timestamp_ms)
                .cloned();
            packed_page_cache.insert(address.clone(), Some(points));
            selected
        }
        PackedFeaturePageDecode::Legacy => Some(FeaturePoint {
            timestamp_ms,
            value: bytes,
        }),
        PackedFeaturePageDecode::Corrupt(_) => {
            packed_page_cache.insert(address.clone(), None);
            None
        }
    }
}

pub(super) fn read_feature_points_cached_batch(
    cache: &MultiLayerCache,
    block_store: &LocalBlockStore,
    shard_id: ShardId,
    refs: &[(u64, BlockAddress)],
) -> Vec<FeaturePoint> {
    let mut addresses = Vec::with_capacity(refs.len());
    addresses.extend(refs.iter().map(|(_, address)| Some(address.clone())));
    let page_bytes = read_page_bytes_batch(cache, block_store, shard_id, &addresses);
    let mut decoded_pages =
        HashMap::<BlockAddress, PackedFeaturePageDecode>::with_capacity(refs.len());
    let mut points = Vec::with_capacity(refs.len());

    for ((timestamp_ms, address), bytes) in refs.iter().zip(page_bytes) {
        let Some(bytes) = bytes else {
            continue;
        };
        let decoded = decoded_pages
            .entry(address.clone())
            .or_insert_with(|| decode_feature_page_strict(&bytes));
        match decoded {
            PackedFeaturePageDecode::Packed(page_points) => {
                if let Some(point) = page_points
                    .iter()
                    .find(|point| point.timestamp_ms == *timestamp_ms)
                    .cloned()
                {
                    points.push(point);
                }
            }
            PackedFeaturePageDecode::Legacy => points.push(FeaturePoint {
                timestamp_ms: *timestamp_ms,
                value: bytes,
            }),
            PackedFeaturePageDecode::Corrupt(_) => {}
        }
    }
    points
}
