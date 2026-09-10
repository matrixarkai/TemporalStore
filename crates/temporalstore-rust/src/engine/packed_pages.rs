// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::collections::{BTreeMap, HashMap};

use crate::block_store::{BlockAddress, BlockStoreError, LocalBlockStore};
use crate::types::{FeaturePoint, ShardId};
use matrixcache::{CacheKey, MultiLayerCache};

use super::constants::{FEATURE_PAGE_BINARY_MAGIC, FEATURE_PAGE_MAGIC};
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

/// Borrows the points instead of owning them, so the page can be serialised without first
/// copying every value. Mirrors `PackedFeaturePage` field for field -- same names, same order,
/// same types -- and neither carries a serde attribute, so the two produce identical bytes.
#[derive(serde::Serialize)]
struct PackedFeaturePageRef<'a> {
    version: u8,
    points: &'a [FeaturePoint],
}

/// A page's fixed header: the magic and the point count.
const FEATURE_PAGE_HEADER_BYTES: usize = FEATURE_PAGE_BINARY_MAGIC.len() + 4;

/// Per point: an 8-byte timestamp and a 4-byte length in front of the value.
const FEATURE_POINT_HEADER_BYTES: usize = 12;

/// Write a page as bytes.
///
/// A value is a `Vec<u8>`, and JSON writes a byte vector as an array of decimal numbers -- roughly
/// four characters per byte. Measured on the JSON form this replaces, a page was 3.6x its payload,
/// and 4.3x for a small one. The framing and compression work reached the WAL and the index log
/// and never reached the page store, which left this the last place a payload was kept as text.
///
/// The timestamp stays, at 8 bytes rather than the 29 it cost in JSON: it is what picks one point
/// out of a page holding several. Every context write passes a single point, but the feature
/// series path does not, and addressing a point by ordinal instead would change `BlockAddress` and
/// every index that stores one.
pub(super) fn encode_feature_page(points: &[FeaturePoint]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(
        FEATURE_PAGE_HEADER_BYTES
            + points
                .iter()
                .map(|point| FEATURE_POINT_HEADER_BYTES + point.value.len())
                .sum::<usize>(),
    );
    bytes.extend_from_slice(FEATURE_PAGE_BINARY_MAGIC);
    // A page with more points than a u32 can count cannot be written, and cannot occur: the
    // chunker splits on a byte target long before this.
    bytes.extend_from_slice(&(points.len() as u32).to_le_bytes());
    for point in points {
        bytes.extend_from_slice(&point.timestamp_ms.to_le_bytes());
        bytes.extend_from_slice(&(point.value.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&point.value);
    }
    bytes
}

fn empty_feature_page_encoded_len() -> usize {
    FEATURE_PAGE_HEADER_BYTES
}

fn feature_point_encoded_len(point: &FeaturePoint) -> usize {
    FEATURE_POINT_HEADER_BYTES + point.value.len()
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
    if refs.is_empty() {
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
fn stage_timestamped_removal(
    shard_id: ShardId,
    kind: &str,
    key: &str,
    routing_bucket: u32,
    timestamp_ms: u64,
) {
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
    promote_sync_writes: bool,
    first_block_index: u32,
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
        promote_sync_writes,
        None,
        first_block_index,
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
    promote_sync_writes: bool,
    identity: u64,
    first_block_index: u32,
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
        promote_sync_writes,
        Some(identity),
        first_block_index,
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
    promote_sync_writes: bool,
    identity: Option<u64>,
    first_block_index: u32,
) -> Result<Vec<(u64, BlockAddress)>, BlockStoreError> {
    let object_id = stable_page_object_id(shard_id, kind, key, None);
    let mut refs = Vec::new();
    let chunks = chunk_timestamped_kv_points(points);
    if !async_storage {
        let mut chunk_points = Vec::with_capacity(chunks.len());
        let mut encoded_pages = Vec::with_capacity(chunks.len());
        for chunk in chunks {
            encoded_pages.push(encode_feature_page(&chunk));
            chunk_points.push(chunk);
        }
        // Hand the store a slice of each page instead of a clone of it. The page is kept anyway,
        // for the cache put below, so the clone was a second full copy of a JSON page -- and a
        // page is several times its payload, because a value is written as decimal numbers.
        let writes: Vec<crate::block_store::BlockAppendRecord<'_>> = encoded_pages
            .iter()
            .enumerate()
            .map(|(chunk, packed)| {
                (
                    packed.as_slice(),
                    Some(object_id),
                    Some(routing_bucket),
                    first_block_index.saturating_add(chunk as u32),
                )
            })
            .collect();
        // Carry these pages in this write's record, the way `append_value` does for a single
        // page. This writer batches straight to the block store, so it never staged anything: a
        // synchronous feature write's record named an address and carried nothing, and because it
        // carried nothing the record kept its operation instead. After a crash that loses the
        // un-fsynced block -- which the single barrier permits -- replay installed the outcomes,
        // the outcomes named a block that was never written, and the read had nothing to fall back
        // to. The whole series came back empty.
        if block_store.block_in_wal() {
            for packed in &encoded_pages {
                super::block_in_wal::stage(object_id, packed.as_slice());
            }
        }
        let addresses = block_store.append_batch_with_page_metadata(writes)?;
        if addresses.len() != chunk_points.len() {
            return Err(BlockStoreError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "batch page append returned fewer addresses than chunks",
            )));
        }
        for ((chunk, packed), address) in chunk_points.into_iter().zip(encoded_pages).zip(addresses)
        {
            if promote_sync_writes {
                cache.put_memory_only(
                    CacheKey::page_with_slot(
                        shard_id,
                        address.block_slab_id,
                        address.offset,
                        address.length,
                        address.routing_bucket(),
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
    // One point is always exactly one page, so measuring it is wasted work. The split below fires
    // only when `current` is non-empty, which a lone point never is, so its encoded length is
    // computed and never read -- and computing it is a FULL `serde_json` serialisation of the
    // point, the same work `encode_feature_page` is about to do again on the way to the page.
    //
    // A page carries its value as a `Vec<u8>`, which serde_json writes as an array of decimal
    // numbers, so that measurement costs several times the value it measures. Every context write
    // path -- summary, event, index, audit, child, compression -- passes exactly one point.
    if points.len() == 1 {
        return vec![points];
    }
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
    if let Some(payload) = bytes.strip_prefix(FEATURE_PAGE_BINARY_MAGIC) {
        return decode_binary_feature_page(payload);
    }
    if let Some(payload) = bytes.strip_prefix(FEATURE_PAGE_MAGIC) {
        // A page written before the byte format. Kept so a store that already exists keeps
        // reading; nothing writes this shape any more, and this arm goes when those stores do.
        return match serde_json::from_slice::<PackedFeaturePage>(payload) {
            Ok(page) if page.version == 1 => PackedFeaturePageDecode::Packed(page.points),
            Ok(page) => PackedFeaturePageDecode::Corrupt(format!(
                "unsupported packed feature page version {}",
                page.version
            )),
            Err(err) => PackedFeaturePageDecode::Corrupt(format!(
                "invalid packed feature page payload: {err}"
            )),
        };
    }
    PackedFeaturePageDecode::Legacy
}

/// Read a byte-format page, checking every declared length against what is actually there.
///
/// A torn page declares a length it does not have. Sizing a buffer from a declared length is what
/// once turned a corrupt tail into an aborted process, so nothing here is allocated until the
/// bytes behind it have been counted.
fn decode_binary_feature_page(payload: &[u8]) -> PackedFeaturePageDecode {
    let Some((count_bytes, mut rest)) = split_at_checked(payload, 4) else {
        return PackedFeaturePageDecode::Corrupt(
            "packed feature page ends before its point count".to_string(),
        );
    };
    let count = u32::from_le_bytes([count_bytes[0], count_bytes[1], count_bytes[2], count_bytes[3]])
        as usize;
    // The smallest a point can be is its own header, so a count that could not fit in the
    // remaining bytes is a torn or forged page -- and this is checked BEFORE reserving for it.
    if count.saturating_mul(FEATURE_POINT_HEADER_BYTES) > rest.len() {
        return PackedFeaturePageDecode::Corrupt(format!(
            "packed feature page declares {count} points but holds {} bytes",
            rest.len()
        ));
    }
    let mut points = Vec::with_capacity(count);
    for index in 0..count {
        let Some((header, tail)) = split_at_checked(rest, FEATURE_POINT_HEADER_BYTES) else {
            return PackedFeaturePageDecode::Corrupt(format!(
                "packed feature page ends inside the header of point {index}"
            ));
        };
        let timestamp_ms = u64::from_le_bytes([
            header[0], header[1], header[2], header[3], header[4], header[5], header[6], header[7],
        ]);
        let length = u32::from_le_bytes([header[8], header[9], header[10], header[11]]) as usize;
        let Some((value, tail)) = split_at_checked(tail, length) else {
            return PackedFeaturePageDecode::Corrupt(format!(
                "point {index} declares {length} bytes and the page has {}",
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
        return PackedFeaturePageDecode::Corrupt(format!(
            "packed feature page has {} bytes after its last point",
            rest.len()
        ));
    }
    PackedFeaturePageDecode::Packed(points)
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

#[cfg(test)]
mod page_encoding_tests {
    use super::*;
    use crate::types::FeaturePoint;

    /// What the page format costs to write and to read, measured on the code that ships.
    ///
    /// A page was JSON, and a value is a `Vec<u8>`, which JSON writes as an array of decimal
    /// numbers. That is paid twice per page -- once encoding, once decoding -- on every write and
    /// every read that misses the cache, and it is the half of this change that compression cannot
    /// give back: bytes the encoder never writes are bytes the compressor never reads.
    ///
    /// Reports rather than asserts a threshold: a timing assertion on shared hardware is a flake
    /// generator, and the correctness of both paths is pinned by the tests above.
    #[test]
    fn what_the_page_format_costs() {
        use std::time::Instant;

        let value: Vec<u8> = (0..400u32).map(|i| (i % 251) as u8).collect();
        let points = vec![FeaturePoint { timestamp_ms: 1_789_007_622_131, value }];
        let rounds = 20_000;

        // Warm both paths so the first does not pay for what the second reuses.
        let _ = encode_feature_page(&points);
        let owned = PackedFeaturePage { version: 1, points: points.to_vec() };
        let _ = serde_json::to_vec(&owned).expect("serialises");

        let started = Instant::now();
        let mut json_bytes = 0usize;
        for _ in 0..rounds {
            let owned = PackedFeaturePage { version: 1, points: points.to_vec() };
            let mut page = FEATURE_PAGE_MAGIC.to_vec();
            page.append(&mut serde_json::to_vec(&owned).expect("serialises"));
            json_bytes = page.len();
        }
        let json_encode_ns = started.elapsed().as_nanos();

        let started = Instant::now();
        let mut binary_bytes = 0usize;
        for _ in 0..rounds {
            binary_bytes = encode_feature_page(&points).len();
        }
        let binary_encode_ns = started.elapsed().as_nanos();

        let owned = PackedFeaturePage { version: 1, points: points.to_vec() };
        let mut json_page = FEATURE_PAGE_MAGIC.to_vec();
        json_page.append(&mut serde_json::to_vec(&owned).expect("serialises"));
        let binary_page = encode_feature_page(&points);

        let started = Instant::now();
        for _ in 0..rounds {
            assert!(matches!(
                decode_feature_page_strict(&json_page),
                PackedFeaturePageDecode::Packed(_)
            ));
        }
        let json_decode_ns = started.elapsed().as_nanos();

        let started = Instant::now();
        for _ in 0..rounds {
            assert!(matches!(
                decode_feature_page_strict(&binary_page),
                PackedFeaturePageDecode::Packed(_)
            ));
        }
        let binary_decode_ns = started.elapsed().as_nanos();

        let saved = |before: u128, after: u128| {
            if before == 0 { 0.0 } else { (before as f64 - after as f64) / before as f64 * 100.0 }
        };
        println!(
            "  page of one 400 B point: {json_bytes} B as JSON, {binary_bytes} B as bytes"
        );
        println!(
            "  encode x{rounds}: json {json_encode_ns} ns, bytes {binary_encode_ns} ns, {:.1}% saved",
            saved(json_encode_ns, binary_encode_ns)
        );
        println!(
            "  decode x{rounds}: json {json_decode_ns} ns, bytes {binary_decode_ns} ns, {:.1}% saved",
            saved(json_decode_ns, binary_decode_ns)
        );
    }

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

    /// Every shape a page can take must come back exactly as it went in.
    #[test]
    fn the_page_round_trips_every_shape() {
        for points in fixtures() {
            let encoded = encode_feature_page(&points);
            assert_eq!(
                decode_feature_page(&encoded).as_deref(),
                Some(points.as_slice()),
                "a page of {} point(s) did not decode back",
                points.len()
            );
        }
    }

    /// A page written before the byte format still reads.
    ///
    /// Stores exist that were written as JSON. Nothing writes that shape now, and this is what
    /// lets those stores keep serving until they are gone.
    #[test]
    fn a_page_written_as_json_still_reads() {
        for points in fixtures() {
            let owned = PackedFeaturePage { version: 1, points: points.to_vec() };
            let mut json_page = FEATURE_PAGE_MAGIC.to_vec();
            json_page.append(&mut serde_json::to_vec(&owned).expect("owned page serialises"));
            assert_eq!(
                decode_feature_page(&json_page).as_deref(),
                Some(points.as_slice()),
                "a JSON page of {} point(s) stopped reading",
                points.len()
            );
        }
    }

    /// A torn page is reported, never trusted, and never sizes a buffer from what it claims.
    ///
    /// A declared length that the file does not have is what turned a corrupt tail into an aborted
    /// process once already. Every prefix of a real page is fed in here: each must either decode
    /// to exactly the points that survived or say it is corrupt, and none may panic.
    #[test]
    fn a_torn_page_is_reported_not_trusted() {
        let points = vec![
            FeaturePoint { timestamp_ms: 11, value: vec![b'x'; 40] },
            FeaturePoint { timestamp_ms: 12, value: vec![b'y'; 40] },
        ];
        let whole = encode_feature_page(&points);
        for cut in 0..whole.len() {
            match decode_feature_page_strict(&whole[..cut]) {
                PackedFeaturePageDecode::Corrupt(_) => {}
                // A prefix shorter than the magic cannot claim to be one of our pages.
                PackedFeaturePageDecode::Legacy => assert!(
                    cut < FEATURE_PAGE_BINARY_MAGIC.len(),
                    "a truncation at {cut} was read as an unframed page"
                ),
                PackedFeaturePageDecode::Packed(got) => panic!(
                    "a page truncated at {cut} decoded as {} point(s)",
                    got.len()
                ),
            }
        }

        // And a page that claims more points than it carries is refused rather than reserved for.
        let mut lying = encode_feature_page(&points);
        let count_at = FEATURE_PAGE_BINARY_MAGIC.len();
        lying[count_at..count_at + 4].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(matches!(
            decode_feature_page_strict(&lying),
            PackedFeaturePageDecode::Corrupt(_)
        ));
    }

    /// The payload is stored once, not four times over.
    ///
    /// JSON wrote a `Vec<u8>` as an array of decimal numbers, so a page was 3.6x its payload and
    /// 4.3x for a small one. This pins the new shape at the payload plus a fixed header, which is
    /// the whole point of the change and the thing a future edit could quietly undo.
    #[test]
    fn a_page_is_no_longer_several_times_its_payload() {
        for payload_len in [48usize, 400, 4096] {
            let points = vec![FeaturePoint {
                timestamp_ms: 1_789_007_622_131,
                value: vec![7u8; payload_len],
            }];
            let encoded = encode_feature_page(&points);
            let overhead = encoded.len() - payload_len;
            assert_eq!(
                overhead,
                FEATURE_PAGE_HEADER_BYTES + FEATURE_POINT_HEADER_BYTES,
                "a {payload_len} B payload should cost a fixed header, not a multiple"
            );

            // Against what it used to cost, so the comparison is in the test rather than a note.
            let owned = PackedFeaturePage { version: 1, points: points.to_vec() };
            let json_len = FEATURE_PAGE_MAGIC.len()
                + serde_json::to_vec(&owned).expect("serialises").len();
            // At least halved. A small payload saves less than a large one because the
            // header is the same 23 bytes either way: 48 B costs 71 B against 168 B as JSON,
            // while 4 KiB costs 4,119 B against roughly four times that.
            assert!(
                encoded.len() * 2 < json_len,
                "{payload_len} B payload: {} B now against {json_len} B as JSON, which is not the \
                 saving this change exists for",
                encoded.len()
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

        // 1. One point -> one chunk holding it, however large. A page target cannot split a point.
        for len in [0usize, 8, 64_000] {
            let chunks = chunk_timestamped_kv_points(vec![point(1, len)]);
            assert_eq!(chunks.len(), 1, "one point must make one chunk (len {len})");
            assert_eq!(chunks[0].len(), 1, "the chunk must hold the point (len {len})");
            assert_eq!(chunks[0][0].timestamp_ms, 1, "and it must be THAT point (len {len})");
            assert_eq!(chunks[0][0].value.len(), len, "with its value intact (len {len})");
        }

        // 2. Two small points still share one page -- so the early return is not what produced
        //    the single chunk above, and this test can tell the two apart.
        let together = chunk_timestamped_kv_points(vec![point(1, 8), point(2, 8)]);
        assert_eq!(together.len(), 1, "two small points share a page");
        assert_eq!(together[0].len(), 2, "and both are in it");

        // 3. Two points too big to share DO split, which is the behaviour the short circuit must
        //    not have disabled for the multi-point path.
        let target = context_page_target_bytes();
        let split = chunk_timestamped_kv_points(vec![point(1, target), point(2, target)]);
        assert_eq!(split.len(), 2, "two oversized points must not share a page");
        assert_eq!(split[0][0].timestamp_ms, 1, "order is preserved across the split");
        assert_eq!(split[1][0].timestamp_ms, 2, "order is preserved across the split");
    }
}
