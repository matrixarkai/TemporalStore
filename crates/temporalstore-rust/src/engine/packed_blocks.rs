// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::collections::{BTreeMap, HashMap};

use crate::block_store::{ElementEntry, BlockStoreError, BlockStore};
use crate::types::{FeaturePoint, ShardId};
use matrixcache::{CacheKey, MultiLayerCache};

use super::constants::{FEATURE_BLOCK_BINARY_MAGIC, FEATURE_BLOCK_MAGIC};
use super::state::PackedFeatureBlockDecode;
use super::hashing::BlockIdentity;
use super::{
    append_value, read_block_bytes, read_block_bytes_cold, read_block_shared_keyed,
    stable_block_object_id, BlockKeys,
};
use crate::storage_config::context_block_target_bytes;
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

/// Borrows the points instead of owning them, so the block can be serialised without first
/// copying every value.
#[derive(serde::Serialize)]
struct PackedFeatureBlockRef<'a> {
    version: u8,
    points: &'a [FeaturePoint],
}

/// A block's fixed header: the magic and the point count.
const FEATURE_BLOCK_HEADER_BYTES: usize = FEATURE_BLOCK_BINARY_MAGIC.len() + 4;

/// Per point: an 8-byte timestamp and a 4-byte length in front of the value.
const FEATURE_POINT_HEADER_BYTES: usize = 12;

/// Write a block as bytes.
///
/// A value is a `Vec<u8>`, and JSON writes a byte vector as an array of decimal numbers -- roughly
/// four characters per byte. Measured on the JSON form this replaces, a block was 3.6x its payload,
/// and 4.3x for a small one. The framing and compression work reached the WAL and the index log
/// and never reached the block store, which left this the last place a payload was kept as text.
///
/// The timestamp stays, at 8 bytes rather than the 29 it cost in JSON: it is what picks one point
/// out of a block holding several. Every context write passes a single point, but the feature
/// series path does not, and addressing a point by ordinal instead would change `BlockAddress` and
/// every index that stores one.
pub(super) fn encode_feature_block(points: &[FeaturePoint]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(
        FEATURE_BLOCK_HEADER_BYTES
            + points
                .iter()
                .map(|point| FEATURE_POINT_HEADER_BYTES + point.value.len())
                .sum::<usize>(),
    );
    bytes.extend_from_slice(FEATURE_BLOCK_BINARY_MAGIC);
    // A block with more points than a u32 can count cannot be written, and cannot occur: the
    // chunker splits on a byte target long before this.
    bytes.extend_from_slice(&(points.len() as u32).to_le_bytes());
    for point in points {
        bytes.extend_from_slice(&point.timestamp_ms.to_le_bytes());
        bytes.extend_from_slice(&(point.value.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&point.value);
    }
    bytes
}

fn empty_feature_block_encoded_len() -> usize {
    FEATURE_BLOCK_HEADER_BYTES
}

fn feature_point_encoded_len(point: &FeaturePoint) -> usize {
    FEATURE_POINT_HEADER_BYTES + point.value.len()
}

/// State that a timestamped point now lives at an address.
///
/// Emitted from the producer rather than from its callers: eleven handlers append timestamped
/// blocks -- feature series, sequences, control state, context events -- and each one having to
/// remember to record the outcome is exactly how a kind ends up silently missing. The series is
/// keyed by timestamp, so that is the element name.
/// How a recorded item names the entry a write created.
///
/// Most timestamped series are keyed by the stored timestamp, so the timestamp alone names the
/// entry. A context event is not: its block is timestamp-keyed but its index entry is keyed by the
/// event id, so the timestamp alone names nothing and the identity has to travel with it. Packing
/// both is what list and zset already do with their own keys.
pub(super) fn timestamped_element_name(stored_key: u64, identity: Option<u64>) -> String {
    match identity {
        Some(identity) => format!("{stored_key:016x}{identity:016x}"),
        None => stored_key.to_string(),
    }
}

pub(super) fn stage_timestamped_outcomes(
    kind: &str,
    key: &str,
    object_id: u64,
    routing_bucket: u32,
    refs: &[(u64, ElementEntry)],
    identity: Option<u64>,
) {
    if refs.is_empty() {
        return;
    }
    for (timestamp_ms, address) in refs {
        let element_name = timestamped_element_name(*timestamp_ms, identity);
        super::block_in_wal::stage_outcome(crate::wal::WalOutcomeItem {
            kind: kind.into(),
            object_key: key.to_string(),
            // MOVED, NOT CLONED: the name is built for this item and read by nothing after it.
            element_name: Some(element_name),
            object_id,
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
pub(super) fn stage_timestamped_removal(
    kind: &str,
    key: &str,
    object_id: u64,
    routing_bucket: u32,
    timestamp_ms: u64,
) {
    let element_name = timestamp_ms.to_string();
    super::block_in_wal::stage_outcome(crate::wal::WalOutcomeItem {
        kind: kind.into(),
        object_key: key.to_string(),
        // MOVED, NOT CLONED: the name is built for this item and read by nothing after it.
        element_name: Some(element_name),
        object_id,
        routing_bucket,
        address: None,
        value: None,
        ttl: None,
        deleted: true,
        meta: false,
    });
}

/// The element name spelling for a removal that states a BOUNDARY: every instant up to and
/// including this one is gone.
///
/// Shared by the writer and the apply path on purpose. The spelling IS the contract between them,
/// and two copies of a literal is how a reader comes to disagree with its writer.
pub(super) const TRIM_BOUNDARY_MARK: &str = "<=";

pub(super) fn timestamped_boundary_element_name(through: u64) -> String {
    format!("{TRIM_BOUNDARY_MARK}{through}")
}

/// The boundary an element name states, or `None` when it states a single instant instead.
pub(super) fn timestamped_boundary_through(element_name: Option<&str>) -> Option<u64> {
    element_name?.strip_prefix(TRIM_BOUNDARY_MARK)?.parse::<u64>().ok()
}

/// Record that every timestamped point up to an instant is gone, in ONE item.
///
/// The per-point form below says the same thing once per point, which is what a series at its
/// bound pays on every command: as many removal records as the append was wide.
fn stage_timestamped_boundary_removal(
    shard_id: ShardId,
    kind: &str,
    key: &str,
    routing_bucket: u32,
    through: u64,
) {
    super::block_in_wal::stage_outcome(crate::wal::WalOutcomeItem {
        kind: kind.into(),
        object_key: key.to_string(),
        element_name: Some(timestamped_boundary_element_name(through)),
        object_id: stable_block_object_id(shard_id, kind, key),
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
    series: &mut impl super::timestamped_series::TimestampedSeriesWrite,
    timestamps: &[u64],
) -> bool {
    drop_timestamped_points_naming_what_left(
        shard_id,
        kind,
        key,
        routing_bucket,
        series,
        timestamps,
        None,
    )
}

/// [`drop_timestamped_points`], and every address the dropped points held is pushed onto
/// `vacated` (see [`trim_timestamped_series_naming_what_left`]).
pub(super) fn drop_timestamped_points_naming_what_left(
    shard_id: ShardId,
    kind: &str,
    key: &str,
    routing_bucket: u32,
    series: &mut impl super::timestamped_series::TimestampedSeriesWrite,
    timestamps: &[u64],
    mut vacated: Option<&mut Vec<ElementEntry>>,
) -> bool {
    let mut dropped = false;
    let mut object_id = None;
    for timestamp_ms in timestamps {
        if let Some(address) = series.get(*timestamp_ms) {
            if let Some(vacated) = vacated.as_deref_mut() {
                vacated.push(address.clone());
            }
            series.clear(*timestamp_ms);
            let object_id = *object_id
                .get_or_insert_with(|| stable_block_object_id(shard_id, kind, key));
            stage_timestamped_removal(kind, key, object_id, routing_bucket, *timestamp_ms);
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
    series: &mut impl super::timestamped_series::TimestampedSeriesWrite,
    max_size: usize,
    boundary_record: bool,
) -> bool {
    trim_timestamped_series_naming_what_left(
        shard_id,
        kind,
        key,
        routing_bucket,
        series,
        max_size,
        0,
        boundary_record,
        None,
    )
}

/// [`trim_timestamped_series`], and every address the dropped points held is pushed onto
/// `vacated`.
///
/// A block can be named by several points, so a dropped point does not say its block is gone: the
/// caller asks [`addresses_no_point_names`] which of these no live point still maps to.
#[allow(clippy::too_many_arguments)]
pub(super) fn trim_timestamped_series_naming_what_left(
    shard_id: ShardId,
    kind: &str,
    key: &str,
    routing_bucket: u32,
    series: &mut impl super::timestamped_series::TimestampedSeriesWrite,
    max_size: usize,
    max_age_ms: u64,
    boundary_record: bool,
    mut vacated: Option<&mut Vec<ElementEntry>>,
) -> bool {
    let mut newest_dropped = None;
    let mut object_id = None;
    // THE AGE CUT, when one is configured: points older than `max_age_ms` before the newest the
    // series holds. Measured from the series itself so a replay needs no clock, and read once,
    // because dropping the oldest never moves the newest.
    let age_floor = if max_age_ms == 0 {
        None
    } else {
        series.range(..).next_back().map(|(newest, _)| newest.saturating_sub(max_age_ms))
    };
    while series.len() > max_size
        || age_floor.is_some_and(|floor| series.points().next().is_some_and(|(oldest, _)| oldest < floor))
    {
        // THE OLDEST POINT IS THE FIRST, because `points` promises ascending order. A
        // representation that could not promise it is not a timestamped series.
        let Some((oldest, address)) = series
            .points()
            .next()
            .map(|(instant, address)| (instant, vacated.is_some().then(|| address.clone())))
        else {
            break;
        };
        if let (Some(vacated), Some(address)) = (vacated.as_deref_mut(), address) {
            vacated.push(address);
        }
        series.clear(oldest);
        if !boundary_record {
            let object_id = *object_id
                .get_or_insert_with(|| stable_block_object_id(shard_id, kind, key));
            stage_timestamped_removal(kind, key, object_id, routing_bucket, oldest);
        }
        // The LAST one dropped is the boundary, and the loop takes them oldest first, so this
        // ends up holding the newest instant that went.
        newest_dropped = Some(oldest);
    }
    let Some(through) = newest_dropped else {
        return false;
    };
    if boundary_record {
        // ONE record for the whole prefix. Stated after the loop rather than inside it, because
        // the boundary is not known until the trim has finished deciding what survives.
        stage_timestamped_boundary_removal(shard_id, kind, key, routing_bucket, through);
    }
    true
}

/// Of the addresses a write vacated, those no point of the series maps to any more.
///
/// A POINT DROPPED OR REPLACED DOES NOT SAY ITS BLOCK IS GONE. A block holds every point of the
/// chunk it was written with, so the block outlives the points that leave it for as long as one
/// of its points stays -- and the points of one block are not contiguous once a later write has
/// replaced some of them, so "the oldest block is the dead one" is not a rule the series keeps.
/// The only sound answer is to ask the series, which this does in one allocation-free pass that
/// stops as soon as every candidate has been seen alive. A candidate nobody names costs the whole
/// pass, because absence is only proved by reading to the end.
///
/// The addresses come back sorted by identity and without repeats.
pub(super) fn addresses_no_point_names(
    series: &impl super::timestamped_series::TimestampedSeries,
    mut vacated: Vec<ElementEntry>,
) -> Vec<ElementEntry> {
    use super::storage_bucket_internals::block_physical_identity_key as identity;
    vacated.sort_unstable_by_key(identity);
    vacated.dedup_by_key(|address| identity(address));
    if vacated.is_empty() {
        return vacated;
    }
    let mut seen_alive = vec![false; vacated.len()];
    let mut unseen = vacated.len();
    for address in series.addresses() {
        let held = identity(address);
        if let Ok(at) = vacated.binary_search_by_key(&held, |candidate| identity(candidate)) {
            if !seen_alive[at] {
                seen_alive[at] = true;
                unseen -= 1;
                if unseen == 0 {
                    return Vec::new();
                }
            }
        }
    }
    let mut at = 0;
    vacated.retain(|_| {
        let dead = !seen_alive[at];
        at += 1;
        dead
    });
    vacated
}

pub(super) fn append_timestamped_kv_blocks(
    cache: &MultiLayerCache,
    block_store: &BlockStore,
    shard_id: ShardId,
    kind: &str,
    key: &str,
    points: Vec<FeaturePoint>,
    routing_bucket: u32,
    async_storage: bool,
    promote_sync_writes: bool,
    first_block_ordinal: u32,
) -> Result<Vec<(u64, ElementEntry)>, BlockStoreError> {
    append_timestamped_kv_blocks_inner(
        cache,
        block_store,
        shard_id,
        kind,
        key,
        points,
        routing_bucket,
        async_storage,
        promote_sync_writes,
        None,
        first_block_ordinal,
    )
}

/// Same, for a series whose index entry is keyed by something other than the stored timestamp.
///
/// The caller passes the key its map will actually use, so the recorded item can name the entry
/// the write created. Without it a record states where a block landed and not what it became.
#[allow(clippy::too_many_arguments)]
pub(super) fn append_timestamped_kv_blocks_keyed(
    cache: &MultiLayerCache,
    block_store: &BlockStore,
    shard_id: ShardId,
    kind: &str,
    key: &str,
    points: Vec<FeaturePoint>,
    routing_bucket: u32,
    async_storage: bool,
    promote_sync_writes: bool,
    identity: u64,
    first_block_ordinal: u32,
) -> Result<Vec<(u64, ElementEntry)>, BlockStoreError> {
    append_timestamped_kv_blocks_inner(
        cache,
        block_store,
        shard_id,
        kind,
        key,
        points,
        routing_bucket,
        async_storage,
        promote_sync_writes,
        Some(identity),
        first_block_ordinal,
    )
}

#[allow(clippy::too_many_arguments)]
fn append_timestamped_kv_blocks_inner(
    cache: &MultiLayerCache,
    block_store: &BlockStore,
    shard_id: ShardId,
    kind: &str,
    key: &str,
    points: Vec<FeaturePoint>,
    routing_bucket: u32,
    async_storage: bool,
    promote_sync_writes: bool,
    identity: Option<u64>,
    first_block_ordinal: u32,
) -> Result<Vec<(u64, ElementEntry)>, BlockStoreError> {
    let object_id = stable_block_object_id(shard_id, kind, key);
    let mut refs = Vec::new();
    let chunks = chunk_timestamped_kv_points(points);
    if !async_storage {
        let mut chunk_points = Vec::with_capacity(chunks.len());
        let mut encoded_blocks = Vec::with_capacity(chunks.len());
        for chunk in chunks {
            encoded_blocks.push(encode_feature_block(&chunk));
            chunk_points.push(chunk);
        }
        // Hand the store a slice of each block instead of a clone of it. The block is kept anyway,
        // for the cache put below, so the clone was a second full copy of a JSON block -- and a
        // block is several times its payload, because a value is written as decimal numbers.
        let writes: Vec<crate::block_store::BlockAppendRecord<'_>> = encoded_blocks
            .iter()
            .enumerate()
            .map(|(chunk, packed)| {
                (
                    packed.as_slice(),
                    Some(object_id),
                    Some(routing_bucket),
                    first_block_ordinal.saturating_add(chunk as u32),
                )
            })
            .collect();
        // Carry these blocks in this write's record, the way `append_value` does for a single
        // block. This writer batches straight to the block store, so it never staged anything: a
        // synchronous feature write's record named an address and carried nothing, and because it
        // carried nothing the record kept its operation instead. After a crash that loses the
        // un-fsynced block -- which the single barrier permits -- replay installed the outcomes,
        // the outcomes named a block that was never written, and the read had nothing to fall back
        // to. The whole series came back empty.
        for packed in &encoded_blocks {
            super::block_in_wal::stage(object_id, None, packed.as_slice());
        }
        let addresses = block_store.append_batch_with_block_metadata(writes)?;
        if addresses.len() != chunk_points.len() {
            return Err(BlockStoreError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "batch block append returned fewer addresses than chunks",
            )));
        }
        for ((chunk, packed), address) in chunk_points.into_iter().zip(encoded_blocks).zip(addresses)
        {
            if promote_sync_writes {
                cache.put_memory_only(
                    CacheKey::page_with_slot(
                        shard_id,
                        address.block_slab_id(),
                        address.offset(),
                        address.length(),
                        Some(routing_bucket),
                    ),
                    packed,
                );
            }
            refs.extend(
                chunk
                    .into_iter()
                    .map(|point| (point.timestamp_ms, address.clone())),
            );
        }
        stage_timestamped_outcomes(kind, key, object_id, routing_bucket, &refs, identity);
        return Ok(refs);
    }

    for chunk in chunks {
        let packed = encode_feature_block(&chunk);
        let address = append_value(
            cache,
            block_store,
            shard_id,
            &packed,
            Some(object_id),
            // `None`, matching `stable_block_object_id(shard_id, kind, key)` at the
            // top of this function: a packed series block is not an ELEMENT of anything.
            None,
            Some(routing_bucket),
            async_storage,
        )?;
        refs.extend(
            chunk
                .into_iter()
                .map(|point| (point.timestamp_ms, address.clone())),
        );
    }
    stage_timestamped_outcomes(kind, key, object_id, routing_bucket, &refs, identity);
    Ok(refs)
}

pub(super) fn chunk_timestamped_kv_points(points: Vec<FeaturePoint>) -> Vec<Vec<FeaturePoint>> {
    // One point is always exactly one block, so measuring it is wasted work. The split below fires
    // only when `current` is non-empty, which a lone point never is, so its encoded length is
    // computed and never read -- and computing it is a FULL `serde_json` serialisation of the
    // point, the same work `encode_feature_block` is about to do again on the way to the block.
    //
    // A block carries its value as a `Vec<u8>`, which serde_json writes as an array of decimal
    // numbers, so that measurement costs several times the value it measures. Every context write
    // path -- summary, event, index, audit, child, compression -- passes exactly one point.
    if points.len() == 1 {
        return vec![points];
    }
    let mut chunks = Vec::new();
    let mut current = Vec::new();
    let empty_block_len = empty_feature_block_encoded_len();
    let mut current_encoded_len = empty_block_len;
    let block_target_bytes = context_block_target_bytes();

    for point in points {
        let point_encoded_len = feature_point_encoded_len(&point);
        let next_encoded_len = current_encoded_len
            .saturating_add(point_encoded_len)
            .saturating_add(if current.is_empty() { 0 } else { 1 });
        if next_encoded_len > block_target_bytes && !current.is_empty() {
            chunks.push(current);
            current = Vec::new();
            current_encoded_len = empty_block_len;
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
pub(super) fn decode_feature_block(bytes: &[u8]) -> Option<Vec<FeaturePoint>> {
    match decode_feature_block_strict(bytes) {
        PackedFeatureBlockDecode::Packed(points) => Some(points),
        PackedFeatureBlockDecode::Legacy | PackedFeatureBlockDecode::Corrupt(_) => None,
    }
}

pub(super) fn decode_feature_block_strict(bytes: &[u8]) -> PackedFeatureBlockDecode {
    if let Some(payload) = bytes.strip_prefix(FEATURE_BLOCK_BINARY_MAGIC) {
        return decode_binary_feature_block(payload);
    }
    if bytes.starts_with(FEATURE_BLOCK_MAGIC) {
        // The JSON shape of a packed block. Nothing writes it and nothing reads it: it is refused.
        return PackedFeatureBlockDecode::Corrupt(
            "packed feature block is in a retired JSON shape".to_string(),
        );
    }
    PackedFeatureBlockDecode::Legacy
}

/// Read a byte-format block, checking every declared length against what is actually there.
///
/// A torn block declares a length it does not have. Sizing a buffer from a declared length is what
/// once turned a corrupt tail into an aborted process, so nothing here is allocated until the
/// bytes behind it have been counted.
fn decode_binary_feature_block(payload: &[u8]) -> PackedFeatureBlockDecode {
    let Some((count_bytes, mut rest)) = split_at_checked(payload, 4) else {
        return PackedFeatureBlockDecode::Corrupt(
            "packed feature block ends before its point count".to_string(),
        );
    };
    let count = u32::from_le_bytes([count_bytes[0], count_bytes[1], count_bytes[2], count_bytes[3]])
        as usize;
    // The smallest a point can be is its own header, so a count that could not fit in the
    // remaining bytes is a torn or forged block -- and this is checked BEFORE reserving for it.
    if count.saturating_mul(FEATURE_POINT_HEADER_BYTES) > rest.len() {
        return PackedFeatureBlockDecode::Corrupt(format!(
            "packed feature block declares {count} points but holds {} bytes",
            rest.len()
        ));
    }
    let mut points = Vec::with_capacity(count);
    for index in 0..count {
        let Some((header, tail)) = split_at_checked(rest, FEATURE_POINT_HEADER_BYTES) else {
            return PackedFeatureBlockDecode::Corrupt(format!(
                "packed feature block ends inside the header of point {index}"
            ));
        };
        let timestamp_ms = u64::from_le_bytes([
            header[0], header[1], header[2], header[3], header[4], header[5], header[6], header[7],
        ]);
        let length = u32::from_le_bytes([header[8], header[9], header[10], header[11]]) as usize;
        let Some((value, tail)) = split_at_checked(tail, length) else {
            return PackedFeatureBlockDecode::Corrupt(format!(
                "point {index} declares {length} bytes and the block has {}",
                tail.len()
            ));
        };
        points.push(FeaturePoint {
            timestamp_ms,
            value: value.to_vec(),
        });
        rest = tail;
    }
    if !rest.is_empty() {
        return PackedFeatureBlockDecode::Corrupt(format!(
            "packed feature block has {} bytes after its last point",
            rest.len()
        ));
    }
    PackedFeatureBlockDecode::Packed(points)
}

/// `split_at` that returns `None` instead of panicking when the slice is too short.
fn split_at_checked(bytes: &[u8], at: usize) -> Option<(&[u8], &[u8])> {
    if bytes.len() < at {
        return None;
    }
    Some(bytes.split_at(at))
}

pub(super) fn read_feature_point(
    cache: &MultiLayerCache,
    block_store: &BlockStore,
    shard_id: ShardId,
    timestamp_ms: u64,
    address: &ElementEntry,
    // WHICH BLOCK, stated by the caller. A packed series block holds many points and is not an
    // element of its object, so the element half is `None` -- but the OBJECT half cannot be
    // recovered here: this function is handed an address and a timestamp, and neither names a key.
    // See [`BlockIdentity`] for why it is the terms rather than an id that cross.
    identity: BlockIdentity<'_>,
    routing_bucket: Option<u32>,
) -> Option<FeaturePoint> {
    let bytes = read_block_bytes(cache, block_store, shard_id, address, identity, routing_bucket)?;
    match decode_feature_block_strict(&bytes) {
        PackedFeatureBlockDecode::Packed(points) => points
            .into_iter()
            .find(|point| point.timestamp_ms == timestamp_ms),
        PackedFeatureBlockDecode::Legacy => Some(FeaturePoint {
            timestamp_ms,
            value: bytes,
        }),
        PackedFeatureBlockDecode::Corrupt(_) => None,
    }
}

pub(super) fn read_feature_point_cold(
    block_store: &BlockStore,
    timestamp_ms: u64,
    address: &ElementEntry,
) -> Option<FeaturePoint> {
    let bytes = read_block_bytes_cold(block_store, address)?;
    match decode_feature_block_strict(&bytes) {
        PackedFeatureBlockDecode::Packed(points) => points
            .into_iter()
            .find(|point| point.timestamp_ms == timestamp_ms),
        PackedFeatureBlockDecode::Legacy => Some(FeaturePoint {
            timestamp_ms,
            value: bytes,
        }),
        PackedFeatureBlockDecode::Corrupt(_) => None,
    }
}

pub(super) struct CachedFeatureBlock {
    points: Vec<FeaturePoint>,
    strictly_sorted: bool,
    next_index: usize,
}

impl CachedFeatureBlock {
    fn new(points: Vec<FeaturePoint>) -> Self {
        let strictly_sorted = points
            .windows(2)
            .all(|window| window[0].timestamp_ms < window[1].timestamp_ms);
        Self { points, strictly_sorted, next_index: 0 }
    }

    fn point(&mut self, timestamp_ms: u64) -> Option<FeaturePoint> {
        if !self.strictly_sorted {
            return self.points
                .iter()
                .find(|point| point.timestamp_ms == timestamp_ms)
                .cloned();
        }
        if self.points
            .get(self.next_index)
            .is_some_and(|point| point.timestamp_ms == timestamp_ms)
        {
            let point = self.points[self.next_index].clone();
            self.next_index += 1;
            return Some(point);
        }
        let index = self.points
            .binary_search_by_key(&timestamp_ms, |point| point.timestamp_ms)
            .ok()?;
        self.next_index = index.saturating_add(1);
        Some(self.points[index].clone())
    }
}

pub(super) fn read_feature_point_cached(
    cache: &MultiLayerCache,
    block_store: &BlockStore,
    shard_id: ShardId,
    timestamp_ms: u64,
    address: &ElementEntry,
    // WHICH BLOCK, stated by the caller. A packed series block holds many points and is not an
    // element of its object, so the element half is `None` -- but the OBJECT half cannot be
    // recovered here: this function is handed an address and a timestamp, and neither names a key.
    // See [`BlockIdentity`] for why it is the terms rather than an id that cross.
    identity: BlockIdentity<'_>,
    packed_block_cache: &mut HashMap<ElementEntry, Option<CachedFeatureBlock>>,
    routing_bucket: Option<u32>,
) -> Option<FeaturePoint> {
    if let Some(points) = packed_block_cache.get_mut(address) {
        return points.as_mut().and_then(|points| points.point(timestamp_ms));
    }

    let bytes = read_block_bytes(cache, block_store, shard_id, address, identity, routing_bucket)?;
    match decode_feature_block_strict(&bytes) {
        PackedFeatureBlockDecode::Packed(points) => {
            let mut points = CachedFeatureBlock::new(points);
            let selected = points.point(timestamp_ms);
            packed_block_cache.insert(address.clone(), Some(points));
            selected
        }
        PackedFeatureBlockDecode::Legacy => Some(FeaturePoint {
            timestamp_ms,
            value: bytes,
        }),
        PackedFeatureBlockDecode::Corrupt(_) => {
            packed_block_cache.insert(address.clone(), None);
            None
        }
    }
}

/// Hand `with` the value of the point at `timestamp_ms`, borrowed from the block's own bytes.
///
/// Answers exactly what [`read_feature_point`] answers about the same bytes -- a value for a point
/// that is there, `None` for one that is not, `None` for a block that does not decode -- without
/// building the `Vec<FeaturePoint>` and the value `Vec` that answer used to cost. A sum over a
/// series reads every point and keeps only a number from each, so those two allocations per point
/// were the whole of what it paid beyond the cache lookup.
///
/// THE WHOLE BLOCK IS WALKED even after the point is found. The strict decoder refuses a block with
/// a torn tail or bytes after the last point, and a point read out of such a block must be refused
/// too, or the two readers would disagree about a damaged block. The first point with the
/// timestamp wins, as `find` picks it.
pub(super) fn with_feature_point_value<R>(
    bytes: &[u8],
    timestamp_ms: u64,
    with: impl FnOnce(&[u8]) -> R,
) -> Option<R> {
    match feature_point_in_place(bytes, timestamp_ms, with) {
        PointRead::Found(found) => Some(found),
        PointRead::Absent | PointRead::Corrupt => None,
    }
}

/// How reading one point out of a block's bytes ended.
///
/// `with_feature_point_value` folds the last two into `None`. A reader that must count a damaged
/// block, and say nothing about a point a sound block simply does not hold, needs them apart.
pub(super) enum PointRead<R> {
    /// The point is there; this is what the caller made of its value.
    Found(R),
    /// The block is sound and holds no point at that instant.
    Absent,
    /// The block does not decode: torn, trailing bytes, or a forged count.
    Corrupt,
}

/// [`with_feature_point_value`] with the two reasons for no answer kept apart.
pub(super) fn feature_point_in_place<R>(
    bytes: &[u8],
    timestamp_ms: u64,
    with: impl FnOnce(&[u8]) -> R,
) -> PointRead<R> {
    let Some(payload) = bytes.strip_prefix(FEATURE_BLOCK_BINARY_MAGIC) else {
        // The JSON shape and the unframed legacy value keep the allocating reader: nothing writes
        // the first any more, and the second is the block's own bytes.
        return match decode_feature_block_strict(bytes) {
            PackedFeatureBlockDecode::Packed(points) => {
                match points.into_iter().find(|point| point.timestamp_ms == timestamp_ms) {
                    Some(point) => PointRead::Found(with(&point.value)),
                    None => PointRead::Absent,
                }
            }
            PackedFeatureBlockDecode::Legacy => PointRead::Found(with(bytes)),
            PackedFeatureBlockDecode::Corrupt(_) => PointRead::Corrupt,
        };
    };
    let Some((count_bytes, mut rest)) = split_at_checked(payload, 4) else {
        return PointRead::Corrupt;
    };
    let count = u32::from_le_bytes([count_bytes[0], count_bytes[1], count_bytes[2], count_bytes[3]])
        as usize;
    if count.saturating_mul(FEATURE_POINT_HEADER_BYTES) > rest.len() {
        return PointRead::Corrupt;
    }
    let mut found: Option<&[u8]> = None;
    for _ in 0..count {
        let Some((header, tail)) = split_at_checked(rest, FEATURE_POINT_HEADER_BYTES) else {
            return PointRead::Corrupt;
        };
        let point_timestamp = u64::from_le_bytes([
            header[0], header[1], header[2], header[3], header[4], header[5], header[6], header[7],
        ]);
        let length = u32::from_le_bytes([header[8], header[9], header[10], header[11]]) as usize;
        let Some((value, tail)) = split_at_checked(tail, length) else {
            return PointRead::Corrupt;
        };
        if found.is_none() && point_timestamp == timestamp_ms {
            found = Some(value);
        }
        rest = tail;
    }
    if !rest.is_empty() {
        return PointRead::Corrupt;
    }
    match found {
        Some(value) => PointRead::Found(with(value)),
        None => PointRead::Absent,
    }
}

/// The value of one feature point, read in place: the shared cache bytes are inspected, not copied,
/// and the block key reuses the record key of the slab it shares with the previous point.
///
/// Same answer as [`read_feature_point`], see [`with_feature_point_value`].
#[allow(clippy::too_many_arguments)]
pub(super) fn read_feature_value_with<R>(
    cache: &MultiLayerCache,
    block_store: &BlockStore,
    shard_id: ShardId,
    timestamp_ms: u64,
    address: &ElementEntry,
    identity: BlockIdentity<'_>,
    routing_bucket: Option<u32>,
    keys: &mut BlockKeys,
    with: impl FnOnce(&[u8]) -> R,
) -> Option<R> {
    let cache_key = keys.key(shard_id, address, routing_bucket);
    let bytes = read_block_shared_keyed(cache, block_store, shard_id, address, identity, cache_key)?;
    with_feature_point_value(&bytes, timestamp_ms, with)
}

/// Where a run of ascending reads has got to in one sound binary block.
pub(super) struct AscendingCursor {
    /// Byte offset of the first point header.
    body: usize,
    /// Points the block holds.
    count: usize,
    /// Byte offset of the next header to look at, and how many points precede it.
    offset: usize,
    index: usize,
    /// The instant the last read asked for. A read at or below it starts the block over.
    last: Option<u64>,
}

/// The sound binary block whose instants strictly ascend, as the cursor to walk it with.
///
/// `None` for anything else -- the JSON shape, an unframed value, a torn or forged block, a repeated
/// or descending instant -- and those are read by [`feature_point_in_place`], which answers for all
/// of them. The strict walk here is the decoder's: a block with a torn tail or bytes after the last
/// point is refused, so a point is never served out of a block the strict decoder would reject.
pub(super) fn ascending_cursor(bytes: &[u8]) -> Option<AscendingCursor> {
    let payload = bytes.strip_prefix(FEATURE_BLOCK_BINARY_MAGIC)?;
    let (count_bytes, mut rest) = split_at_checked(payload, 4)?;
    let count = u32::from_le_bytes([count_bytes[0], count_bytes[1], count_bytes[2], count_bytes[3]])
        as usize;
    if count.saturating_mul(FEATURE_POINT_HEADER_BYTES) > rest.len() {
        return None;
    }
    let mut previous: Option<u64> = None;
    for _ in 0..count {
        let (header, tail) = split_at_checked(rest, FEATURE_POINT_HEADER_BYTES)?;
        let instant = u64::from_le_bytes([
            header[0], header[1], header[2], header[3], header[4], header[5], header[6], header[7],
        ]);
        let length = u32::from_le_bytes([header[8], header[9], header[10], header[11]]) as usize;
        let (_, tail) = split_at_checked(tail, length)?;
        if previous.is_some_and(|before| before >= instant) {
            return None;
        }
        previous = Some(instant);
        rest = tail;
    }
    if !rest.is_empty() {
        return None;
    }
    let body = FEATURE_BLOCK_HEADER_BYTES;
    Some(AscendingCursor {
        body,
        count,
        offset: body,
        index: 0,
        last: None,
    })
}

impl AscendingCursor {
    /// The value stored at `instant`, found by stepping forward from where the last read stopped.
    ///
    /// The block was checked whole when the cursor was made, so every header read here is in bounds.
    pub(super) fn value_at<'b>(&mut self, bytes: &'b [u8], instant: u64) -> Option<&'b [u8]> {
        if self.last.is_some_and(|last| instant <= last) {
            self.offset = self.body;
            self.index = 0;
        }
        self.last = Some(instant);
        while self.index < self.count {
            let header = &bytes[self.offset..self.offset + FEATURE_POINT_HEADER_BYTES];
            let stored = u64::from_le_bytes([
                header[0], header[1], header[2], header[3], header[4], header[5], header[6],
                header[7],
            ]);
            let length = u32::from_le_bytes([header[8], header[9], header[10], header[11]]) as usize;
            let value_at = self.offset + FEATURE_POINT_HEADER_BYTES;
            if stored > instant {
                return None;
            }
            self.offset = value_at + length;
            self.index += 1;
            if stored == instant {
                return Some(&bytes[value_at..value_at + length]);
            }
        }
        None
    }
}

/// One block of a run of feature points, held between them.
struct HeldFeatureBlock {
    address: ElementEntry,
    bytes: std::sync::Arc<[u8]>,
    /// `Some` for a sound ascending binary block; every other shape is read point by point.
    cursor: Option<AscendingCursor>,
}

/// Feature points read in order out of the blocks they sit in.
///
/// A range read used to copy each block out of the cache, decode the whole of it into a
/// `Vec<FeaturePoint>` (a value `Vec` per point), keep that in a table keyed by the block's address,
/// and clone the wanted point out of it: two allocations per point of the block, and a third copy for
/// each point it returned. The block's shared bytes are walked where they sit instead, and the value
/// is handed to the caller as a slice of them, so the caller allocates only for what it keeps.
///
/// The last block read is held, so the points of one block read it from the cache once.
pub(super) struct FeatureRangeReader {
    keys: BlockKeys,
    held: Option<HeldFeatureBlock>,
}

impl FeatureRangeReader {
    pub(super) fn new() -> Self {
        Self {
            keys: BlockKeys::new(),
            held: None,
        }
    }

    /// Hand `with` the value of the point at `timestamp_ms` in the block `address` names, borrowed
    /// from the block's bytes. `None` for a point the block does not hold, for a block that does not
    /// decode, and for a block that cannot be read: the answers `read_feature_point_cached` gave.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn with_point<R>(
        &mut self,
        cache: &MultiLayerCache,
        block_store: &BlockStore,
        shard_id: ShardId,
        timestamp_ms: u64,
        address: &ElementEntry,
        identity: BlockIdentity<'_>,
        routing_bucket: Option<u32>,
        with: impl FnOnce(&[u8]) -> R,
    ) -> Option<R> {
        let held_here = self.held.as_ref().is_some_and(|held| held.address == *address);
        if !held_here {
            let cache_key = self.keys.key(shard_id, address, routing_bucket);
            let bytes =
                read_block_shared_keyed(cache, block_store, shard_id, address, identity, cache_key)?;
            let cursor = ascending_cursor(&bytes);
            self.held = Some(HeldFeatureBlock {
                address: address.clone(),
                bytes,
                cursor,
            });
        }
        let held = self.held.as_mut()?;
        match &mut held.cursor {
            Some(cursor) => cursor.value_at(&held.bytes, timestamp_ms).map(with),
            None => match feature_point_in_place(&held.bytes, timestamp_ms, with) {
                PointRead::Found(found) => Some(found),
                PointRead::Absent | PointRead::Corrupt => None,
            },
        }
    }
}

#[cfg(test)]
mod block_encoding_tests {
    use super::*;
    use crate::types::FeaturePoint;

    fn fixtures() -> Vec<Vec<FeaturePoint>> {
        vec![
            vec![],
            vec![FeaturePoint { timestamp_ms: 0, value: vec![] }],
            vec![FeaturePoint { timestamp_ms: 7, value: vec![0, 1, 2, 254, 255] }],
            vec![FeaturePoint { timestamp_ms: u64::MAX, value: vec![0xff; 3] }],
            vec![
                FeaturePoint { timestamp_ms: 1, value: vec![b'a'; 300] },
                FeaturePoint { timestamp_ms: 2, value: vec![0u8; 300] },
            ],
        ]
    }

    /// Every shape a block can take must come back exactly as it went in.
    #[test]
    fn the_block_round_trips_every_shape() {
        for points in fixtures() {
            let encoded = encode_feature_block(&points);
            assert_eq!(
                decode_feature_block(&encoded).as_deref(),
                Some(points.as_slice()),
                "a block of {} point(s) did not decode back",
                points.len()
            );
        }
    }

    /// A torn block is reported, never trusted, and never sizes a buffer from what it claims.
    ///
    /// A declared length that the file does not have is what turned a corrupt tail into an aborted
    /// process once already. Every prefix of a real block is fed in here: each must either decode
    /// to exactly the points that survived or say it is corrupt, and none may panic.
    #[test]
    fn a_torn_block_is_reported_not_trusted() {
        let points = vec![
            FeaturePoint { timestamp_ms: 11, value: vec![b'x'; 40] },
            FeaturePoint { timestamp_ms: 12, value: vec![b'y'; 40] },
        ];
        let whole = encode_feature_block(&points);
        for cut in 0..whole.len() {
            match decode_feature_block_strict(&whole[..cut]) {
                PackedFeatureBlockDecode::Corrupt(_) => {}
                // A prefix shorter than the magic cannot claim to be one of our blocks.
                PackedFeatureBlockDecode::Legacy => assert!(
                    cut < FEATURE_BLOCK_BINARY_MAGIC.len(),
                    "a truncation at {cut} was read as an unframed block"
                ),
                PackedFeatureBlockDecode::Packed(got) => panic!(
                    "a block truncated at {cut} decoded as {} point(s)",
                    got.len()
                ),
            }
        }

        // And a block that claims more points than it carries is refused rather than reserved for.
        let mut lying = encode_feature_block(&points);
        let count_at = FEATURE_BLOCK_BINARY_MAGIC.len();
        lying[count_at..count_at + 4].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(matches!(
            decode_feature_block_strict(&lying),
            PackedFeatureBlockDecode::Corrupt(_)
        ));
    }

    /// The payload is stored once, not four times over.
    ///
    /// JSON wrote a `Vec<u8>` as an array of decimal numbers, so a block was 3.6x its payload and
    /// 4.3x for a small one. This pins the new shape at the payload plus a fixed header, which is
    /// the whole point of the change and the thing a future edit could quietly undo.
    #[test]
    fn a_block_is_no_longer_several_times_its_payload() {
        for payload_len in [48usize, 400, 4096] {
            let points = vec![FeaturePoint {
                timestamp_ms: 1_789_007_622_131,
                value: vec![7u8; payload_len],
            }];
            let encoded = encode_feature_block(&points);
            let overhead = encoded.len() - payload_len;
            assert_eq!(
                overhead,
                FEATURE_BLOCK_HEADER_BYTES + FEATURE_POINT_HEADER_BYTES,
                "a {payload_len} B payload should cost a fixed header, not a multiple"
            );
        }
    }
}

#[cfg(test)]
mod single_point_chunking_tests {
    use super::*;
    use crate::types::FeaturePoint;

    fn point(ts: u64, len: usize) -> FeaturePoint {
        FeaturePoint { timestamp_ms: ts, value: vec![b'v'; len] }
    }

    #[test]
    fn a_lone_point_chunks_exactly_as_the_general_path_would() {
        // The short circuit above returns early for one point. It is only correct because the
        // split fires solely when `current` is non-empty, which a lone point never is -- so this
        // pins the three cases that would catch it being wrong.

        // 1. One point -> one chunk holding it, however large. A block target cannot split a point.
        for len in [0usize, 8, 64_000] {
            let chunks = chunk_timestamped_kv_points(vec![point(1, len)]);
            assert_eq!(chunks.len(), 1, "one point must make one chunk (len {len})");
            assert_eq!(chunks[0].len(), 1, "the chunk must hold the point (len {len})");
            assert_eq!(chunks[0][0].timestamp_ms, 1, "and it must be THAT point (len {len})");
            assert_eq!(chunks[0][0].value.len(), len, "with its value intact (len {len})");
        }

        // 2. Two small points still share one block -- so the early return is not what produced
        //    the single chunk above, and this test can tell the two apart.
        let together = chunk_timestamped_kv_points(vec![point(1, 8), point(2, 8)]);
        assert_eq!(together.len(), 1, "two small points share a block");
        assert_eq!(together[0].len(), 2, "and both are in it");

        // 3. Two points too big to share DO split, which is the behaviour the short circuit must
        //    not have disabled for the multi-point path.
        let target = context_block_target_bytes();
        let split = chunk_timestamped_kv_points(vec![point(1, target), point(2, target)]);
        assert_eq!(split.len(), 2, "two oversized points must not share a block");
        assert_eq!(split[0][0].timestamp_ms, 1, "order is preserved across the split");
        assert_eq!(split[1][0].timestamp_ms, 2, "order is preserved across the split");
    }
}
