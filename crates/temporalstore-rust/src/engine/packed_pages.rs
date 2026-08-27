// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::collections::{BTreeMap, HashMap};

use crate::block_store::{BlockAddress, BlockStoreError, LocalBlockStore};
use crate::types::{FeaturePoint, ShardId};
use matrixcache::MultiLayerCache;

use super::constants::FEATURE_PAGE_MAGIC;
use super::state::{PackedFeaturePage, PackedFeaturePageDecode};
use super::{append_value, read_page_bytes, read_page_bytes_cold, stable_page_object_id};
use crate::storage_config::context_page_target_bytes;
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
    let mut bytes = FEATURE_PAGE_MAGIC.to_vec();
    if let Ok(mut payload) = serde_json::to_vec(&page) {
        bytes.append(&mut payload);
    }
    bytes
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

/// State that a timestamped point now lives at an address.
///
/// Emitted from the producer rather than from its callers: eleven handlers append timestamped
/// pages -- feature series, sequences, control state, context events -- and each one having to
/// remember to record the outcome is exactly how a kind ends up silently missing. The series is
/// keyed by timestamp, so that is the component.
/// How a recorded item names the entry a write created.
///
/// Most timestamped series are keyed by the stored timestamp, so the timestamp alone names the
/// entry. A context event is not: its page is timestamp-keyed but its index entry is keyed by the
/// event id, so the timestamp alone names nothing and the identity has to travel with it. Packing
/// both is what list and zset already do with their own keys.
pub(super) fn timestamped_component(stored_key: u64, identity: Option<u64>) -> String {
    match identity {
        Some(identity) => format!("{stored_key:016x}{identity:016x}"),
        None => stored_key.to_string(),
    }
}

fn stage_timestamped_outcomes(
    shard_id: ShardId,
    kind: &str,
    key: &str,
    routing_bucket: u32,
    refs: &[(u64, BlockAddress)],
    identity: Option<u64>,
) {
    if refs.is_empty() || !crate::wal::wal_outcome_items_enabled() {
        return;
    }
    for (timestamp_ms, address) in refs {
        let component = timestamped_component(*timestamp_ms, identity);
        super::block_in_wal::stage_outcome(crate::wal::WalOutcomeItem {
            kind: kind.to_string(),
            object_key: key.to_string(),
            component: Some(component.clone()),
            object_id: stable_page_object_id(shard_id, kind, key, Some(&component)),
            routing_bucket,
            address: Some(address.clone()),
            value: None,
            ttl: None,
            deleted: false,
            meta: false,
        });
    }
}

/// Record that a timestamped point is gone.
///
/// The insert above records where a point landed; without this its removal is invisible, and a
/// shard rebuilt from records alone would keep points the shard itself has dropped.
fn stage_timestamped_removal(shard_id: ShardId, kind: &str, key: &str, routing_bucket: u32, timestamp_ms: u64) {
    if !crate::wal::wal_outcome_items_enabled() {
        return;
    }
    let component = timestamp_ms.to_string();
    super::block_in_wal::stage_outcome(crate::wal::WalOutcomeItem {
        kind: kind.to_string(),
        object_key: key.to_string(),
        component: Some(component.clone()),
        object_id: stable_page_object_id(shard_id, kind, key, Some(&component)),
        routing_bucket,
        address: None,
        value: None,
        ttl: None,
        deleted: true,
        meta: false,
    });
}

/// Drop the named points from a series, recording each one.
///
/// Returns whether the series changed, so a caller can mark the shard dirty without repeating
/// the test.
pub(super) fn drop_timestamped_points(
    shard_id: ShardId,
    kind: &str,
    key: &str,
    routing_bucket: u32,
    series: &mut BTreeMap<u64, BlockAddress>,
    timestamps: &[u64],
) -> bool {
    let mut dropped = false;
    for timestamp_ms in timestamps {
        if series.remove(timestamp_ms).is_some() {
            stage_timestamped_removal(shard_id, kind, key, routing_bucket, *timestamp_ms);
            dropped = true;
        }
    }
    dropped
}

/// Trim a series to its configured bound, oldest first, recording every point that went.
///
/// The bound lives in config rather than in the command, which is why replaying a command
/// reproduces this trim only if the config effective at the time is reproduced with it. Recording
/// the trimmed point instead states the result, and needs no config at all.
pub(super) fn trim_timestamped_series(
    shard_id: ShardId,
    kind: &str,
    key: &str,
    routing_bucket: u32,
    series: &mut BTreeMap<u64, BlockAddress>,
    max_size: usize,
) -> bool {
    let mut trimmed = false;
    while series.len() > max_size {
        let Some(oldest) = series.keys().next().copied() else {
            break;
        };
        series.remove(&oldest);
        stage_timestamped_removal(shard_id, kind, key, routing_bucket, oldest);
        trimmed = true;
    }
    trimmed
}

pub(super) fn append_timestamped_kv_pages(
    cache: &MultiLayerCache,
    block_store: &LocalBlockStore,
    shard_id: ShardId,
    kind: &str,
    key: &str,
    points: Vec<FeaturePoint>,
    routing_bucket: u32,
    async_storage: bool,
) -> Result<Vec<(u64, BlockAddress)>, BlockStoreError> {
    append_timestamped_kv_pages_inner(
        cache,
        block_store,
        shard_id,
        kind,
        key,
        points,
        routing_bucket,
        async_storage,
        None,
    )
}

/// Same, for a series whose index entry is keyed by something other than the stored timestamp.
///
/// The caller passes the key its map will actually use, so the recorded item can name the entry
/// the write created. Without it a record states where a page landed and not what it became.
#[allow(clippy::too_many_arguments)]
pub(super) fn append_timestamped_kv_pages_keyed(
    cache: &MultiLayerCache,
    block_store: &LocalBlockStore,
    shard_id: ShardId,
    kind: &str,
    key: &str,
    points: Vec<FeaturePoint>,
    routing_bucket: u32,
    async_storage: bool,
    identity: u64,
) -> Result<Vec<(u64, BlockAddress)>, BlockStoreError> {
    append_timestamped_kv_pages_inner(
        cache,
        block_store,
        shard_id,
        kind,
        key,
        points,
        routing_bucket,
        async_storage,
        Some(identity),
    )
}

#[allow(clippy::too_many_arguments)]
fn append_timestamped_kv_pages_inner(
    cache: &MultiLayerCache,
    block_store: &LocalBlockStore,
    shard_id: ShardId,
    kind: &str,
    key: &str,
    points: Vec<FeaturePoint>,
    routing_bucket: u32,
    async_storage: bool,
    identity: Option<u64>,
) -> Result<Vec<(u64, BlockAddress)>, BlockStoreError> {
    let object_id = stable_page_object_id(shard_id, kind, key, None);
    let mut refs = Vec::new();
    let chunks = chunk_timestamped_kv_points(points);
    if !async_storage {
        let mut writes = Vec::with_capacity(chunks.len());
        let mut chunk_points = Vec::with_capacity(chunks.len());
        for chunk in chunks {
            writes.push((
                encode_feature_page(&chunk),
                Some(object_id),
                Some(routing_bucket),
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
        stage_timestamped_outcomes(shard_id, kind, key, routing_bucket, &refs, identity);
        return Ok(refs);
    }

    for chunk in chunks {
        let packed = encode_feature_page(&chunk);
        let address = append_value(
            cache,
            block_store,
            shard_id,
            &packed,
            Some(object_id),
            Some(routing_bucket),
            async_storage,
        )?;
        refs.extend(
            chunk
                .into_iter()
                .map(|point| (point.timestamp_ms, address.clone())),
        );
    }
    stage_timestamped_outcomes(shard_id, kind, key, routing_bucket, &refs, identity);
    Ok(refs)
}

pub(super) fn chunk_timestamped_kv_points(points: Vec<FeaturePoint>) -> Vec<Vec<FeaturePoint>> {
    let mut chunks = Vec::new();
    let mut current = Vec::new();
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
            current = Vec::new();
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

pub(super) fn read_feature_point(
    cache: &MultiLayerCache,
    block_store: &LocalBlockStore,
    shard_id: ShardId,
    timestamp_ms: u64,
    address: &BlockAddress,
) -> Option<FeaturePoint> {
    let bytes = read_page_bytes(cache, block_store, shard_id, address)?;
    match decode_feature_page_strict(&bytes) {
        PackedFeaturePageDecode::Packed(points) => points
            .into_iter()
            .find(|point| point.timestamp_ms == timestamp_ms),
        PackedFeaturePageDecode::Legacy => Some(FeaturePoint {
            timestamp_ms,
            value: bytes,
        }),
        PackedFeaturePageDecode::Corrupt(_) => None,
    }
}

pub(super) fn read_feature_point_cold(
    block_store: &LocalBlockStore,
    timestamp_ms: u64,
    address: &BlockAddress,
) -> Option<FeaturePoint> {
    let bytes = read_page_bytes_cold(block_store, address)?;
    match decode_feature_page_strict(&bytes) {
        PackedFeaturePageDecode::Packed(points) => points
            .into_iter()
            .find(|point| point.timestamp_ms == timestamp_ms),
        PackedFeaturePageDecode::Legacy => Some(FeaturePoint {
            timestamp_ms,
            value: bytes,
        }),
        PackedFeaturePageDecode::Corrupt(_) => None,
    }
}

pub(super) fn read_feature_point_cached(
    cache: &MultiLayerCache,
    block_store: &LocalBlockStore,
    shard_id: ShardId,
    timestamp_ms: u64,
    address: &BlockAddress,
    packed_page_cache: &mut HashMap<BlockAddress, Option<Vec<FeaturePoint>>>,
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
