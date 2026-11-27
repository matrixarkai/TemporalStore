// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The engine side of the ranked cell read: finds the samples a window names under a key, reads
//! and decodes each through the cache, and hands them to the pure ranking in [`super::window_reduce_rank`]
//! (one key) or [`super::window_reduce_fan_in`] (several).
//!
//! # WHAT IS READ
//!
//! The newest sample of the key is read first, for its header: the names a query uses are
//! resolved against it, and every older sample is decoded under it (a sample written before a
//! label or lane was added reads the new field as zero; one whose header is not a prefix of it is
//! refused). Then only the samples the window names are read, at most one more than the window
//! bound, newest first, so a window over the bound is refused after a bounded number of reads and
//! never after walking the whole series.
//!
//! # SEVERAL KEYS
//!
//! [`serve_many`] reads each key's newest header and settles ONE header for the request: keys whose
//! headers are equal use it, and keys whose headers differ only by labels or lanes appended to the
//! end are read under the widest (the same rule a write follows, [`check_header`]); any other
//! difference is refused naming the key. A key the shard does not hold, or one with nothing in the
//! window, contributes nothing and is not counted in `RankedCells::keys`. The sample and cell
//! ceilings of the request are checked while the samples are read, so a request over one stops
//! before it has decoded every key.
//!
//! # WHAT A REFUSAL LOOKS LIKE
//!
//! A read that cannot be served returns [`RankedCells`] with `refused` set and no rows. An absent
//! key, or a key with no samples, is an empty answer with `refused` empty: nothing to rank.

use super::window_reduce_fan_in::{validate_chosen, validate_keys};
use super::window_reduce_rank::{CutView, Plan, MAX_WINDOW_SAMPLES};
use super::cell_set::{check_header, split_value, within, CellHeader, FlatCells};
use super::object_record::ObjectSlot;
use super::window::{window_samples, RowsWindow};
use super::*;
use crate::block_store::ElementEntry;
use crate::engine::timestamped_series::TimestampedSeries as _;
use crate::types::{WindowReduceRankSpec, WindowReduceWindow, RankedCells};

/// The samples a read decoded, oldest first, each in the flat form: a fixed handful of
/// allocations per sample however many cells it holds.
type Decoded = Vec<(u64, FlatCells)>;

fn refused(why: impl Into<String>) -> RankedCells {
    RankedCells { refused: why.into(), ..RankedCells::default() }
}

/// The header of the key's newest sample, or `None` for a key with no sample.
fn newest_header(
    cache: &MultiLayerCache,
    block_store: &BlockStore,
    shard_id: ShardId,
    series: &ObjectSlot,
    key: &str,
    routing_bucket: Option<u32>,
    block_keys: &mut BlockKeys,
) -> Result<Option<CellHeader>, String> {
    newest_header_of(cache, block_store, shard_id, series.range(..).next_back(), key, routing_bucket, block_keys)
}

/// [`newest_header`] over the newest sample already found, so a caller that holds a copy of the
/// series entry rather than the series can ask it too.
fn newest_header_of(
    cache: &MultiLayerCache,
    block_store: &BlockStore,
    shard_id: ShardId,
    newest: Option<(u64, &ElementEntry)>,
    key: &str,
    routing_bucket: Option<u32>,
    block_keys: &mut BlockKeys,
) -> Result<Option<CellHeader>, String> {
    let Some((newest_ms, newest_address)) = newest else {
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
        block_keys,
        |bytes| split_value(bytes).map(|(header, _)| header),
    ) {
        Some(Ok(header)) => Ok(Some(header)),
        Some(Err(why)) => Err(format!("the sample at {newest_ms}: {why}")),
        None => Err(format!("the sample at {newest_ms} cannot be read")),
    }
}

/// The samples the window names, newest first, bounded. One more than the window bound is
/// returned for a range of stamps, so a caller can tell a window at the bound from one over it.
fn named_samples<'s>(series: &'s ObjectSlot, spec: &WindowReduceRankSpec, now_ms: u64) -> Vec<(u64, &'s ElementEntry)> {
    window_samples(series, &spec.window, spec.anchor, now_ms, MAX_WINDOW_SAMPLES + 1)
}

/// Reads and decodes `named` (newest first) under `header`, returning them oldest first.
#[allow(clippy::too_many_arguments)]
fn decode_named(
    cache: &MultiLayerCache,
    block_store: &BlockStore,
    shard_id: ShardId,
    key: &str,
    routing_bucket: Option<u32>,
    block_keys: &mut BlockKeys,
    named: Vec<(u64, &ElementEntry)>,
    header: &CellHeader,
) -> Result<Decoded, String> {
    let mut samples: Decoded = Vec::with_capacity(named.len());
    for (timestamp_ms, address) in named.into_iter().rev() {
        let decoded = read_feature_value_with(
            cache,
            block_store,
            shard_id,
            timestamp_ms,
            address,
            BlockIdentity::of(shard_id, "feature", key, None),
            routing_bucket,
            block_keys,
            |bytes| -> Result<FlatCells, String> {
                let (own, packed) = split_value(bytes)?;
                if own != *header {
                    check_header(&own, header)?;
                }
                FlatCells::decode(header, packed, 0)
            },
        );
        match decoded {
            Some(Ok(cells)) => samples.push((timestamp_ms, cells)),
            Some(Err(why)) => return Err(format!("the sample at {timestamp_ms}: {why}")),
            None => return Err(format!("the sample at {timestamp_ms} cannot be read")),
        }
    }
    Ok(samples)
}

/// Reads the newest samples of a rows window, one at a time from the newest back, until the cells
/// they hold reach `rows`. The sample that crosses the count is kept whole, with the number of its
/// first cells that fall outside the window beside it, so no cell is copied to cut it. A window
/// whose rows reach back over more samples than the window bound is refused, not cut short.
/// Returns the samples oldest first and, in the same order, how many of each one's first cells to
/// leave out.
#[allow(clippy::too_many_arguments)]
fn decode_newest_rows(
    cache: &MultiLayerCache,
    block_store: &BlockStore,
    shard_id: ShardId,
    key: &str,
    routing_bucket: Option<u32>,
    block_keys: &mut BlockKeys,
    named: Vec<(u64, &ElementEntry)>,
    header: &CellHeader,
    rows: usize,
) -> Result<(Decoded, Vec<usize>), String> {
    let mut window = RowsWindow::new(rows);
    let mut samples: Decoded = Vec::new();
    let mut skips: Vec<usize> = Vec::new();
    for (timestamp_ms, address) in named {
        let decoded = read_feature_value_with(
            cache,
            block_store,
            shard_id,
            timestamp_ms,
            address,
            BlockIdentity::of(shard_id, "feature", key, None),
            routing_bucket,
            block_keys,
            |bytes| -> Result<FlatCells, String> {
                let (own, packed) = split_value(bytes)?;
                if own != *header {
                    check_header(&own, header)?;
                }
                FlatCells::decode(header, packed, 0)
            },
        );
        let cells = match decoded {
            Some(Ok(cells)) => cells,
            Some(Err(why)) => return Err(format!("the sample at {timestamp_ms}: {why}")),
            None => return Err(format!("the sample at {timestamp_ms} cannot be read")),
        };
        let (skip, full) = window.take(cells.len());
        samples.push((timestamp_ms, cells));
        skips.push(skip);
        within("samples in the window", samples.len(), MAX_WINDOW_SAMPLES)?;
        if full {
            break;
        }
    }
    samples.reverse();
    skips.reverse();
    Ok((samples, skips))
}

/// The samples `window` names, decoded: oldest first, each with the number of its first cells that
/// fall outside the window (zero for every window but a rows window's edge sample).
#[allow(clippy::too_many_arguments)]
fn decode_window(
    cache: &MultiLayerCache,
    block_store: &BlockStore,
    shard_id: ShardId,
    key: &str,
    routing_bucket: Option<u32>,
    block_keys: &mut BlockKeys,
    named: Vec<(u64, &ElementEntry)>,
    header: &CellHeader,
    window: &WindowReduceWindow,
) -> Result<(Decoded, Vec<usize>), String> {
    match window {
        WindowReduceWindow::NewestRows { n } => {
            decode_newest_rows(cache, block_store, shard_id, key, routing_bucket, block_keys, named, header, *n)
        }
        _ => decode_named(cache, block_store, shard_id, key, routing_bucket, block_keys, named, header)
            .map(|samples| (samples, Vec::new())),
    }
}

/// The decoded samples as the plan reads them. `skips` is empty, or holds one count per sample.
fn views<'a>(samples: &'a Decoded, skips: &[usize]) -> Vec<CutView<'a>> {
    samples
        .iter()
        .enumerate()
        .map(|(at, (timestamp_ms, cells))| CutView {
            timestamp_ms: *timestamp_ms,
            cells,
            skip: skips.get(at).copied().unwrap_or(0),
        })
        .collect()
}

/// What a ranked read takes from the series: the newest sample's entry, for the header, and the
/// entries the window names. Copied out, so the shard can be released before any block is read.
#[derive(Default)]
pub(super) struct RankSnapshot {
    newest: Option<(u64, ElementEntry)>,
    named: Vec<(u64, ElementEntry)>,
}

impl RankSnapshot {
    /// Every entry the read will open, for pinning the slabs and log locations they name.
    pub(super) fn entries(&self) -> impl Iterator<Item = &ElementEntry> {
        self.newest.iter().map(|(_, entry)| entry).chain(self.named.iter().map(|(_, entry)| entry))
    }
}

/// Takes what [`serve_snapshot`] needs from `series`.
pub(super) fn snapshot(series: &ObjectSlot, spec: &WindowReduceRankSpec, now_ms: u64) -> RankSnapshot {
    RankSnapshot {
        newest: series.range(..).next_back().map(|(at, entry)| (at, entry.clone())),
        named: named_samples(series, spec, now_ms)
            .into_iter()
            .map(|(at, entry)| (at, entry.clone()))
            .collect(),
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn serve(
    cache: &MultiLayerCache,
    block_store: &BlockStore,
    shard_id: ShardId,
    shard: &ShardState,
    key: &str,
    spec: &WindowReduceRankSpec,
    start_routing_bucket: u32,
    end_routing_bucket: u32,
    now_ms: u64,
) -> RankedCells {
    let Some(series) = shard.features().get(key) else {
        return RankedCells::default();
    };
    serve_snapshot(
        cache,
        block_store,
        shard_id,
        key,
        spec,
        start_routing_bucket,
        end_routing_bucket,
        now_ms,
        snapshot(series, spec, now_ms),
    )
}

/// The ranked read over entries already taken from the series: every block read happens here, so
/// a caller can run it with no lock on the shard.
#[allow(clippy::too_many_arguments)]
pub(super) fn serve_snapshot(
    cache: &MultiLayerCache,
    block_store: &BlockStore,
    shard_id: ShardId,
    key: &str,
    spec: &WindowReduceRankSpec,
    start_routing_bucket: u32,
    end_routing_bucket: u32,
    now_ms: u64,
    held: RankSnapshot,
) -> RankedCells {
    let routing_bucket = Some(block_routing_bucket(key, start_routing_bucket, end_routing_bucket));
    let mut block_keys = BlockKeys::new();

    // The header the names are resolved against: the key's newest sample's.
    let newest = held.newest.as_ref().map(|(at, entry)| (*at, entry));
    let header = match newest_header_of(cache, block_store, shard_id, newest, key, routing_bucket, &mut block_keys) {
        Ok(Some(header)) => header,
        Ok(None) => return RankedCells::default(),
        Err(why) => return refused(why),
    };
    let plan = match Plan::compile(&header, spec) {
        Ok(plan) => plan,
        Err(why) => return refused(why),
    };

    let named: Vec<(u64, &ElementEntry)> = held.named.iter().map(|(at, entry)| (*at, entry)).collect();
    let (samples, skips) = match decode_window(
        cache,
        block_store,
        shard_id,
        key,
        routing_bucket,
        &mut block_keys,
        named,
        &header,
        &spec.window,
    ) {
        Ok(decoded) => decoded,
        Err(why) => return refused(why),
    };
    plan.run(now_ms, &views(&samples, &skips)).unwrap_or_else(refused)
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

/// What a ranked read over several keys takes from the shard: one snapshot per key, in order. A
/// key the shard does not hold has an empty one.
pub(super) fn snapshot_many(shard: &ShardState, keys: &[String], spec: &WindowReduceRankSpec, now_ms: u64) -> Vec<RankSnapshot> {
    keys.iter()
        .map(|key| {
            shard
                .features()
                .get(key.as_str())
                .map(|series| snapshot(series, spec, now_ms))
                .unwrap_or_default()
        })
        .collect()
}

/// A ranked read over the samples of every key in `keys`, which the caller has already held to
/// the key bound and refused duplicates in. Keys the shard does not hold (or whose window is
/// empty) contribute nothing.
#[allow(clippy::too_many_arguments)]
pub(super) fn serve_many(
    cache: &MultiLayerCache,
    block_store: &BlockStore,
    shard_id: ShardId,
    shard: &ShardState,
    keys: &[String],
    spec: &WindowReduceRankSpec,
    start_routing_bucket: u32,
    end_routing_bucket: u32,
    now_ms: u64,
) -> RankedCells {
    // Admission refuses these before the engine runs; a caller that skipped it is refused here.
    if let Err(why) = validate_keys(keys) {
        return refused(why);
    }
    serve_many_snapshots(
        cache,
        block_store,
        shard_id,
        keys,
        spec,
        start_routing_bucket,
        end_routing_bucket,
        now_ms,
        snapshot_many(shard, keys, spec, now_ms),
    )
}

/// [`serve_many`] over entries already taken from the shard, one snapshot per key in order: every
/// block read happens here, so a caller can run it with no lock on the shard.
#[allow(clippy::too_many_arguments)]
pub(super) fn serve_many_snapshots(
    cache: &MultiLayerCache,
    block_store: &BlockStore,
    shard_id: ShardId,
    keys: &[String],
    spec: &WindowReduceRankSpec,
    start_routing_bucket: u32,
    end_routing_bucket: u32,
    now_ms: u64,
    snapshots: Vec<RankSnapshot>,
) -> RankedCells {
    if let Err(why) = validate_keys(keys) {
        return refused(why);
    }
    let mut block_keys = BlockKeys::new();

    // Which keys hold a sample, each one's newest header, and the one header they settle on.
    let mut held: Vec<Option<(&RankSnapshot, Option<u32>)>> = Vec::with_capacity(keys.len());
    let mut header: Option<CellHeader> = None;
    for (position, (key, snapshot)) in keys.iter().zip(&snapshots).enumerate() {
        let newest = snapshot.newest.as_ref().map(|(at, entry)| (*at, entry));
        if newest.is_none() {
            held.push(None);
            continue;
        }
        let routing_bucket = Some(block_routing_bucket(key, start_routing_bucket, end_routing_bucket));
        match newest_header_of(cache, block_store, shard_id, newest, key, routing_bucket, &mut block_keys) {
            Ok(Some(own)) => {
                header = match header.take() {
                    None => Some(own),
                    Some(settled) => match settle_header(settled, &own) {
                        Ok(settled) => Some(settled),
                        Err(why) => return refused(format!("key {position}: {why}")),
                    },
                };
                held.push(Some((snapshot, routing_bucket)));
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

    // The samples, key by key, with the request's ceilings checked as they are read.
    let mut decoded: Vec<Decoded> = Vec::with_capacity(keys.len());
    let mut skipped: Vec<Vec<usize>> = Vec::with_capacity(keys.len());
    let (mut sample_count, mut cell_count) = (0usize, 0usize);
    // A rows window names the samples it reaches only by reading them, so its samples are counted
    // after each key is read rather than from the list the walk hands back.
    let rows_window = matches!(spec.window, WindowReduceWindow::NewestRows { .. });
    for (position, (key, taken)) in keys.iter().zip(&held).enumerate() {
        let Some((snapshot, routing_bucket)) = taken else {
            decoded.push(Vec::new());
            skipped.push(Vec::new());
            continue;
        };
        let named: Vec<(u64, &ElementEntry)> = snapshot.named.iter().map(|(at, entry)| (*at, entry)).collect();
        if !rows_window {
            sample_count += named.len();
            if let Err(why) = validate_chosen(sample_count, cell_count) {
                return refused(why);
            }
        }
        match decode_window(
            cache,
            block_store,
            shard_id,
            key,
            *routing_bucket,
            &mut block_keys,
            named,
            &header,
            &spec.window,
        ) {
            Ok((samples, skips)) => {
                if rows_window {
                    sample_count += samples.len();
                }
                cell_count += samples
                    .iter()
                    .enumerate()
                    .map(|(at, (_, cells))| cells.len().saturating_sub(skips.get(at).copied().unwrap_or(0)))
                    .sum::<usize>();
                if let Err(why) = validate_chosen(sample_count, cell_count) {
                    return refused(why);
                }
                decoded.push(samples);
                skipped.push(skips);
            }
            Err(why) => return refused(format!("key {position}: {why}")),
        }
    }

    let owned: Vec<Vec<CutView<'_>>> = decoded.iter().zip(&skipped).map(|(samples, skips)| views(samples, skips)).collect();
    let slices: Vec<&[CutView<'_>]> = owned.iter().map(Vec::as_slice).collect();
    plan.run_fan_in(now_ms, &slices).unwrap_or_else(refused)
}
