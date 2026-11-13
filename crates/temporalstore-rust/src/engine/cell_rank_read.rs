// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The engine side of the ranked cell read: finds the snapshots a window names under a key, reads
//! and decodes each through the cache, and hands them to the pure ranking in [`super::cell_rank`].
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
//! # WHAT A REFUSAL LOOKS LIKE
//!
//! A read that cannot be served returns [`RankedCells`] with `refused` set and no rows. An absent
//! key, or a key with no snapshots, is an empty answer with `refused` empty: nothing to rank.

use super::cell_rank::{Plan, SnapshotView, MAX_WINDOW_SNAPSHOTS};
use super::cell_set::{check_header, decode_snapshot, split_value, Cell, CellHeader};
use super::*;
use crate::block_store::ElementEntry;
use crate::engine::timestamped_series::TimestampedSeries as _;
use crate::types::{CellRankSpec, CellWindow, RankedCells};

fn refused(why: impl Into<String>) -> RankedCells {
    RankedCells { refused: why.into(), ..RankedCells::default() }
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
    let Some((newest_ms, newest_address)) = series.range(..).next_back() else {
        return RankedCells::default();
    };
    let header: CellHeader = match read_feature_value_with(
        cache,
        block_store,
        shard_id,
        newest_ms,
        newest_address,
        BlockIdentity::of(shard_id, "feature", key, None),
        routing_bucket,
        &mut page_keys,
        |bytes| split_value(bytes).map(|(header, _)| header),
    ) {
        Some(Ok(header)) => header,
        Some(Err(why)) => return refused(format!("the snapshot at {newest_ms}: {why}")),
        None => return refused(format!("the snapshot at {newest_ms} cannot be read")),
    };
    let plan = match Plan::compile(&header, spec) {
        Ok(plan) => plan,
        Err(why) => return refused(why),
    };

    // The snapshots the window names, newest first, bounded.
    let bounded = |from_ms: u64, to_ms: u64| -> Vec<(u64, &ElementEntry)> {
        series
            .range(crate::engine::timestamp_range_bounds(from_ms, to_ms))
            .rev()
            .take(MAX_WINDOW_SNAPSHOTS + 1)
            .collect()
    };
    let named: Vec<(u64, &ElementEntry)> = match &spec.window {
        CellWindow::Between { start_ms, end_ms } => bounded(*start_ms, *end_ms),
        CellWindow::Last { duration_ms } => bounded(now_ms.saturating_sub(*duration_ms), now_ms),
        CellWindow::Since { start_ms } => bounded(*start_ms, u64::MAX),
        CellWindow::LastN { n } => series.range(..).rev().take(*n).collect(),
    };

    let mut snapshots: Vec<(u64, Vec<Cell>)> = Vec::with_capacity(named.len());
    for (timestamp_ms, address) in named.into_iter().rev() {
        let decoded = read_feature_value_with(
            cache,
            block_store,
            shard_id,
            timestamp_ms,
            address,
            BlockIdentity::of(shard_id, "feature", key, None),
            routing_bucket,
            &mut page_keys,
            |bytes| -> Result<Vec<Cell>, String> {
                let (own, packed) = split_value(bytes)?;
                if own != header {
                    check_header(&own, &header)?;
                }
                decode_snapshot(&header, packed)
            },
        );
        match decoded {
            Some(Ok(cells)) => snapshots.push((timestamp_ms, cells)),
            Some(Err(why)) => return refused(format!("the snapshot at {timestamp_ms}: {why}")),
            None => return refused(format!("the snapshot at {timestamp_ms} cannot be read")),
        }
    }
    let views: Vec<SnapshotView<'_>> = snapshots
        .iter()
        .map(|(timestamp_ms, cells)| SnapshotView { timestamp_ms: *timestamp_ms, cells })
        .collect();
    plan.run(now_ms, &views).unwrap_or_else(refused)
}
