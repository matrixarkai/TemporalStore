// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The engine side of the ranked cell read: finds the snapshots a window names under a key, reads
//! and decodes each through the cache, and hands them to the pure ranking in [`super::cell_rank`]
//! (one key) or [`super::cell_fan_in`] (several).
//!
//! # WHAT IS READ
//!
//! The newest snapshot of the key is read first, for its header: the names a query uses are
//! resolved against it, and every older snapshot is decoded under it (a snapshot written before a
//! label or lane was added reads the new field as zero; one whose header is not a prefix of it is
//! refused). Then only the snapshots the window names are read, at most one more than the window
//! bound, newest first, so a window over the bound is refused after a bounded number of reads and
//! never after walking the whole series.
//!
//! # SEVERAL KEYS
//!
//! [`serve_many`] reads each key's newest header and settles ONE header for the request: keys whose
//! headers are equal use it, and keys whose headers differ only by labels or lanes appended to the
//! end are read under the widest (the same rule a write follows, [`check_header`]); any other
//! difference is refused naming the key. A key the shard does not hold, or one with nothing in the
//! window, contributes nothing and is not counted in `RankedCells::keys`. The snapshot and cell
//! ceilings of the request are checked while the snapshots are read, so a request over one stops
//! before it has decoded every key.
//!
//! # WHAT A REFUSAL LOOKS LIKE
//!
//! A read that cannot be served returns [`RankedCells`] with `refused` set and no rows. An absent
//! key, or a key with no snapshots, is an empty answer with `refused` empty: nothing to rank.

use super::cell_fan_in::{validate_chosen, validate_keys};
use super::cell_rank::{Plan, SnapshotView, MAX_WINDOW_SNAPSHOTS};
use super::cell_set::{check_header, decode_snapshot, split_value, Cell, CellHeader};
use super::object_record::ObjectSlot;
use super::*;
use crate::block_store::ElementEntry;
use crate::engine::timestamped_series::TimestampedSeries as _;
use crate::types::{CellRankSpec, CellWindow, RankedCells};

type Decoded = Vec<(u64, Vec<Cell>)>;

fn refused(why: impl Into<String>) -> RankedCells {
    RankedCells { refused: why.into(), ..RankedCells::default() }
}

/// The header of the key's newest snapshot, or `None` for a key with no snapshot.
fn newest_header(
    cache: &MultiLayerCache,
    block_store: &BlockStore,
    shard_id: ShardId,
    series: &ObjectSlot,
    key: &str,
    routing_bucket: Option<u32>,
    page_keys: &mut PageKeys,
) -> Result<Option<CellHeader>, String> {
    let Some((newest_ms, newest_address)) = series.range(..).next_back() else {
        return Ok(None);
    };
    match read_feature_value_with(
        cache,
        block_store,
        shard_id,
        newest_ms,
        newest_address,
        BlockIdentity::of(shard_id, "feature", key, None),
        routing_bucket,
        page_keys,
        |bytes| split_value(bytes).map(|(header, _)| header),
    ) {
        Some(Ok(header)) => Ok(Some(header)),
        Some(Err(why)) => Err(format!("the snapshot at {newest_ms}: {why}")),
        None => Err(format!("the snapshot at {newest_ms} cannot be read")),
    }
}

/// The snapshots the window names, newest first, bounded.
fn named_snapshots<'s>(series: &'s ObjectSlot, window: &CellWindow, now_ms: u64) -> Vec<(u64, &'s ElementEntry)> {
    let bounded = |from_ms: u64, to_ms: u64| -> Vec<(u64, &'s ElementEntry)> {
        series
            .range(crate::engine::timestamp_range_bounds(from_ms, to_ms))
            .rev()
            .take(MAX_WINDOW_SNAPSHOTS + 1)
            .collect()
    };
    match window {
        CellWindow::Between { start_ms, end_ms } => bounded(*start_ms, *end_ms),
        CellWindow::Last { duration_ms } => bounded(now_ms.saturating_sub(*duration_ms), now_ms),
        CellWindow::Since { start_ms } => bounded(*start_ms, u64::MAX),
        CellWindow::LastN { n } => series.range(..).rev().take(*n).collect(),
    }
}

/// Reads and decodes `named` (newest first) under `header`, returning them oldest first.
#[allow(clippy::too_many_arguments)]
fn decode_named(
    cache: &MultiLayerCache,
    block_store: &BlockStore,
    shard_id: ShardId,
    key: &str,
    routing_bucket: Option<u32>,
    page_keys: &mut PageKeys,
    named: Vec<(u64, &ElementEntry)>,
    header: &CellHeader,
) -> Result<Decoded, String> {
    let mut snapshots: Decoded = Vec::with_capacity(named.len());
    for (timestamp_ms, address) in named.into_iter().rev() {
        let decoded = read_feature_value_with(
            cache,
            block_store,
            shard_id,
            timestamp_ms,
            address,
            BlockIdentity::of(shard_id, "feature", key, None),
            routing_bucket,
            page_keys,
            |bytes| -> Result<Vec<Cell>, String> {
                let (own, packed) = split_value(bytes)?;
                if own != *header {
                    check_header(&own, header)?;
                }
                decode_snapshot(header, packed)
            },
        );
        match decoded {
            Some(Ok(cells)) => snapshots.push((timestamp_ms, cells)),
            Some(Err(why)) => return Err(format!("the snapshot at {timestamp_ms}: {why}")),
            None => return Err(format!("the snapshot at {timestamp_ms} cannot be read")),
        }
    }
    Ok(snapshots)
}

fn views(snapshots: &Decoded) -> Vec<SnapshotView<'_>> {
    snapshots
        .iter()
        .map(|(timestamp_ms, cells)| SnapshotView { timestamp_ms: *timestamp_ms, cells })
        .collect()
}

#[allow(clippy::too_many_arguments)]
pub(super) fn serve(
    cache: &MultiLayerCache,
    block_store: &BlockStore,
    shard_id: ShardId,
    shard: &ShardState,
    key: &str,
    spec: &CellRankSpec,
    start_routing_bucket: u32,
    end_routing_bucket: u32,
    now_ms: u64,
) -> RankedCells {
    let Some(series) = shard.features().get(key) else {
        return RankedCells::default();
    };
    let routing_bucket = Some(block_routing_bucket(key, start_routing_bucket, end_routing_bucket));
    let mut page_keys = PageKeys::new();

    // The header the names are resolved against: the key's newest snapshot's.
    let header = match newest_header(cache, block_store, shard_id, series, key, routing_bucket, &mut page_keys) {
        Ok(Some(header)) => header,
        Ok(None) => return RankedCells::default(),
        Err(why) => return refused(why),
    };
    let plan = match Plan::compile(&header, spec) {
        Ok(plan) => plan,
        Err(why) => return refused(why),
    };

    let named = named_snapshots(series, &spec.window, now_ms);
    let snapshots =
        match decode_named(cache, block_store, shard_id, key, routing_bucket, &mut page_keys, named, &header) {
            Ok(snapshots) => snapshots,
            Err(why) => return refused(why),
        };
    plan.run(now_ms, &views(&snapshots)).unwrap_or_else(refused)
}

/// The header a request over several keys reads under: `a` when it equals or extends `b`, `b` when
/// it extends `a`, and a refusal otherwise.
fn settle_header(a: CellHeader, b: &CellHeader) -> Result<CellHeader, String> {
    if a == *b {
        return Ok(a);
    }
    match check_header(b, &a) {
        Ok(_) => Ok(a),
        Err(first) => match check_header(&a, b) {
            Ok(_) => Ok(b.clone()),
            Err(_) => Err(format!("the keys do not share one header: {first}")),
        },
    }
}

/// A ranked read over the snapshots of every key in `keys`, which the caller has already held to
/// the key bound and refused duplicates in. Keys the shard does not hold (or whose window is
/// empty) contribute nothing.
#[allow(clippy::too_many_arguments)]
pub(super) fn serve_many(
    cache: &MultiLayerCache,
    block_store: &BlockStore,
    shard_id: ShardId,
    shard: &ShardState,
    keys: &[String],
    spec: &CellRankSpec,
    start_routing_bucket: u32,
    end_routing_bucket: u32,
    now_ms: u64,
) -> RankedCells {
    // Admission refuses these before the engine runs; a caller that skipped it is refused here.
    if let Err(why) = validate_keys(keys) {
        return refused(why);
    }
    let mut page_keys = PageKeys::new();

    // Which keys the shard holds, each one's newest header, and the one header they settle on.
    let mut held: Vec<Option<(&ObjectSlot, Option<u32>)>> = Vec::with_capacity(keys.len());
    let mut header: Option<CellHeader> = None;
    for (position, key) in keys.iter().enumerate() {
        let Some(series) = shard.features().get(key.as_str()) else {
            held.push(None);
            continue;
        };
        let routing_bucket = Some(block_routing_bucket(key, start_routing_bucket, end_routing_bucket));
        match newest_header(cache, block_store, shard_id, series, key, routing_bucket, &mut page_keys) {
            Ok(Some(own)) => {
                header = match header.take() {
                    None => Some(own),
                    Some(settled) => match settle_header(settled, &own) {
                        Ok(settled) => Some(settled),
                        Err(why) => return refused(format!("key {position}: {why}")),
                    },
                };
                held.push(Some((series, routing_bucket)));
            }
            Ok(None) => held.push(None),
            Err(why) => return refused(format!("key {position}: {why}")),
        }
    }
    let Some(header) = header else {
        return RankedCells::default();
    };
    let plan = match Plan::compile(&header, spec) {
        Ok(plan) => plan,
        Err(why) => return refused(why),
    };

    // The snapshots, key by key, with the request's ceilings checked as they are read.
    let mut decoded: Vec<Decoded> = Vec::with_capacity(keys.len());
    let (mut snapshot_count, mut cell_count) = (0usize, 0usize);
    for (position, (key, series)) in keys.iter().zip(&held).enumerate() {
        let Some((series, routing_bucket)) = series else {
            decoded.push(Vec::new());
            continue;
        };
        let named = named_snapshots(series, &spec.window, now_ms);
        snapshot_count += named.len();
        if let Err(why) = validate_chosen(snapshot_count, cell_count) {
            return refused(why);
        }
        match decode_named(cache, block_store, shard_id, key, *routing_bucket, &mut page_keys, named, &header) {
            Ok(snapshots) => {
                cell_count += snapshots.iter().map(|(_, cells)| cells.len()).sum::<usize>();
                if let Err(why) = validate_chosen(snapshot_count, cell_count) {
                    return refused(why);
                }
                decoded.push(snapshots);
            }
            Err(why) => return refused(format!("key {position}: {why}")),
        }
    }

    let owned: Vec<Vec<SnapshotView<'_>>> = decoded.iter().map(views).collect();
    let slices: Vec<&[SnapshotView<'_>]> = owned.iter().map(Vec::as_slice).collect();
    plan.run_fan_in(now_ms, &slices).unwrap_or_else(refused)
}
