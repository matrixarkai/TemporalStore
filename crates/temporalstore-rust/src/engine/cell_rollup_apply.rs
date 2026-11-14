// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The engine side of a cell rollup: reads the snapshots a request names, hands them to the pure
//! [`super::cell_rollup::rollup_spec`], and says what the series must hold afterwards.
//!
//! # WHERE A VIOLATION IS REFUSED
//!
//! At VALIDATION, before the command reaches the log, as the merge-reduce append is: a refused
//! request writes nothing. Execution runs the SAME planner again. It is a pure function of the
//! command and the stored series (a cutoff is a stamp in the request, or an age resolved against
//! the newest STORED snapshot; there is no clock), so a follower applying the committed entry and a
//! restart replaying it reach the bytes the leader planned. Replay skips validation, so a plan that
//! fails there is a state divergence, not a bad request: it writes nothing and answers a receipt
//! of zeros.
//!
//! # WHAT IS READ
//!
//! Only the snapshots stamped below the first cutoff, and the newest snapshot of the key for its
//! header (a snapshot written under an older header is read under the newest one; one whose header
//! is not a prefix of it is refused). The number below the cutoff is counted from the address map
//! and refused over [`MAX_ROLLUP_SNAPSHOTS`] BEFORE a block is read. A truncation reads nothing: it
//! removes the oldest snapshots by stamp.
//!
//! # TRUNCATION
//!
//! It acts on the series as it will stand after the rollup: the rolled snapshots, then every
//! snapshot at or after the first cutoff. When that count is over the trigger the oldest are
//! removed until the newest `keep_last` remain. Rolled snapshots go first; if more must go, the
//! oldest untouched snapshots follow, by stamp. A request with a truncation and nothing below its
//! cutoff is a plain truncation.
//!
//! # WHAT THE RECEIPT COUNTS
//!
//! Snapshots are counted over the whole series. Cells are counted over the snapshots the request
//! touched, the ones below the first cutoff, and after a truncation only the cells of the rolled
//! snapshots that remain; the others are not read. `cells_dropped` is the cap's. A truncation
//! shows in `snapshots_after`.
//!
//! A request that would change nothing (every rolled snapshot already at its bucket start and
//! encoding to the bytes it has, and nothing to truncate) reports `changed == false` and writes
//! nothing.

use super::cell_rollup::{
    first_cutoff, rollup_spec, truncation_drop, truncation_of, validate_spec, RollupSpec, Snapshot,
    MAX_ROLLUP_SNAPSHOTS,
};
use super::cell_set::{check_header, decode_snapshot, encode_value, split_value, Cell, CellHeader};
use super::*;
use crate::engine::timestamped_series::TimestampedSeries;
use crate::types::FeaturePoint;

/// What a rollup request does to a series.
#[derive(Debug, PartialEq)]
pub(super) struct CellRollupPlan {
    /// Stamps of the stored snapshots the request replaces or removes, ascending, in one run of
    /// the series: those below the first cutoff, then the oldest untouched ones a truncation
    /// removes.
    pub(super) replaced: Vec<u64>,
    /// What replaces them, one point per surviving bucket, ascending, each value a stored cell
    /// snapshot.
    pub(super) points: Vec<FeaturePoint>,
    /// Whether the series changes.
    pub(super) changed: bool,
    pub(super) snapshots_before: u64,
    pub(super) snapshots_after: u64,
    pub(super) cells_before: u64,
    pub(super) cells_after: u64,
    pub(super) cells_dropped: u64,
}

fn refuse(key: &str, message: impl std::fmt::Display) -> Status {
    Status::error("invalid_argument", format!("{key}: {message}"))
}

/// Plans `spec` over `series`. `series` is `None` for a key that does not exist (or whose deadline
/// has passed).
pub(super) fn plan_cell_rollup(
    cache: &MultiLayerCache,
    block_store: &BlockStore,
    shard_id: ShardId,
    series: Option<&impl TimestampedSeries>,
    key: &str,
    routing_bucket: u32,
    spec: &RollupSpec,
) -> Result<CellRollupPlan, Status> {
    validate_spec(spec).map_err(|why| refuse(key, why))?;
    let empty = |snapshots: u64| CellRollupPlan {
        replaced: Vec::new(),
        points: Vec::new(),
        changed: false,
        snapshots_before: snapshots,
        snapshots_after: snapshots,
        cells_before: 0,
        cells_after: 0,
        cells_dropped: 0,
    };
    let Some(series) = series else { return Ok(empty(0)) };
    let total = series.len() as u64;
    let Some((newest_ms, newest_address)) = series.range(..).next_back() else { return Ok(empty(0)) };

    // Under an age anchor the cutoffs are resolved against the newest STORED stamp.
    let cutoff = first_cutoff(spec, newest_ms);
    let below: Vec<(u64, &crate::block_store::ElementEntry)> = series.range(..cutoff).collect();
    let truncation = truncation_of(spec);
    if below.is_empty() && truncation.is_none() {
        return Ok(empty(total));
    }
    super::cell_set::within("snapshots below the cutoff", below.len(), MAX_ROLLUP_SNAPSHOTS)
        .map_err(|why| refuse(key, format!("{why}: roll up in steps by moving the cutoff forward")))?;

    // Roll the snapshots below the cutoff.
    let mut stored: Vec<Vec<u8>> = Vec::with_capacity(below.len());
    let mut rolled: Vec<Snapshot> = Vec::new();
    let (mut cells_before, mut cells_dropped) = (0u64, 0u64);
    let mut header_for_points: Option<CellHeader> = None;
    if !below.is_empty() {
        let mut page_keys = PageKeys::new();
        let identity = || BlockIdentity::of(shard_id, "feature", key, None);
        let routing = Some(routing_bucket);

        // The header every older snapshot is read under: the newest snapshot's.
        let header: CellHeader = read_feature_value_with(
            cache,
            block_store,
            shard_id,
            newest_ms,
            newest_address,
            identity(),
            routing,
            &mut page_keys,
            |bytes| split_value(bytes).map(|(header, _)| header),
        )
        .ok_or_else(|| Status::error("data_loss", format!("{key}: the snapshot stored at {newest_ms} cannot be read")))?
        .map_err(|why| refuse(key, format!("the snapshot at {newest_ms} is not a cell snapshot: {why}")))?;

        let mut snapshots: Vec<Snapshot> = Vec::with_capacity(below.len());
        for &(timestamp_ms, address) in &below {
            let decoded = read_feature_value_with(
                cache,
                block_store,
                shard_id,
                timestamp_ms,
                address,
                identity(),
                routing,
                &mut page_keys,
                |bytes| -> Result<(Vec<Cell>, Vec<u8>), String> {
                    let (own, packed) = split_value(bytes)?;
                    if own != header {
                        check_header(&own, &header)?;
                    }
                    Ok((decode_snapshot(&header, packed)?, bytes.to_vec()))
                },
            )
            .ok_or_else(|| Status::error("data_loss", format!("{key}: the snapshot stored at {timestamp_ms} cannot be read")))?
            .map_err(|why| refuse(key, format!("the snapshot at {timestamp_ms}: {why}")))?;
            snapshots.push(Snapshot { timestamp_ms, cells: decoded.0 });
            stored.push(decoded.1);
        }
        let result = rollup_spec(&header, snapshots, spec, newest_ms).map_err(|why| refuse(key, why))?;
        cells_before = result.stats.cells_before;
        cells_dropped = result.stats.cells_dropped;
        rolled = result.snapshots;
        header_for_points = Some(header);
    }

    // The series as it will stand: the rolled snapshots, then everything at or after the cutoff.
    let untouched = total - below.len() as u64;
    let standing = rolled.len() as u64 + untouched;
    let drop = truncation.map_or(0, |truncation| truncation_drop(standing as usize, &truncation)) as u64;
    let dropped_rolled = drop.min(rolled.len() as u64) as usize;
    let dropped_untouched = (drop - dropped_rolled as u64) as usize;
    let removed_untouched: Vec<u64> = series.range(cutoff..).take(dropped_untouched).map(|(at, _)| at).collect();

    let mut cells_after = 0u64;
    let mut points = Vec::with_capacity(rolled.len() - dropped_rolled);
    if let Some(header) = &header_for_points {
        for snapshot in &rolled[dropped_rolled..] {
            cells_after += snapshot.cells.len() as u64;
            let value = encode_value(header, &snapshot.cells).map_err(|why| refuse(key, why))?;
            points.push(FeaturePoint { timestamp_ms: snapshot.timestamp_ms, value });
        }
    }
    let changed = drop > 0
        || points.len() != below.len()
        || points
            .iter()
            .zip(below.iter().zip(&stored))
            .any(|(point, ((timestamp_ms, _), bytes))| point.timestamp_ms != *timestamp_ms || point.value != *bytes);

    let mut replaced: Vec<u64> = below.iter().map(|(timestamp_ms, _)| *timestamp_ms).collect();
    replaced.extend(removed_untouched);
    Ok(CellRollupPlan {
        replaced,
        points,
        changed,
        snapshots_before: total,
        snapshots_after: standing - drop,
        cells_before,
        cells_after,
        cells_dropped,
    })
}
