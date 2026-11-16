// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The engine side of a cell removal: reads the samples inside the request's window, hands them
//! to the pure [`super::window_reduce_remove::remove_cells`], and says what the series must hold afterwards.
//!
//! # WHERE A VIOLATION IS REFUSED
//!
//! At VALIDATION, before the command reaches the log, as a rollup is: a refused request writes
//! nothing. Execution runs the SAME planner again. It is a pure function of the command and the
//! stored series (the window is stamps in the request; there is no clock), so a follower applying
//! the committed entry and a restart replaying it reach the bytes the leader planned. Replay skips
//! validation, so a plan that fails there is a state divergence, not a bad request: it writes
//! nothing and answers a receipt of zeros.
//!
//! # WHAT IS READ
//!
//! Only the samples stamped inside the window, and the newest sample of the key for its header
//! (a sample written under an older header is read under the newest one; one whose header is not
//! a prefix of it is refused). The number inside the window is counted from the address map and
//! refused over [`MAX_REMOVE_SAMPLES`] BEFORE a block is read. A name the key does not declare is
//! refused once the header is read; a window that holds no sample is answered without one.
//!
//! # WHAT IS WRITTEN
//!
//! One replacement of the stamp range from the first sample that changes to the last. Inside
//! that range a sample that lost cells is rewritten under the newest header, one that lost all
//! of them is left out, and one that lost none is put back as the very bytes it had, so the replace
//! never changes a sample the request did not touch. Outside that range nothing is read for
//! writing and nothing is written. A request that matches no cell reports zero removed and writes
//! nothing.
//!
//! # WHAT THE RECEIPT COUNTS
//!
//! Samples over the whole series, before and after (a sample left with no cell is gone, so the
//! count can fall). Cells over the samples inside the window: how many there were and how many
//! the request took out.

use super::window_reduce_remove::{remove_cells, validate_spec, Selection, MAX_REMOVE_SAMPLES};
use super::cell_set::{check_header, decode_sample, encode_value, split_value, Cell, CellHeader};
use super::*;
use crate::engine::timestamped_series::TimestampedSeries;
use crate::types::{CellRemoveSpec, FeaturePoint};

/// What a removal does to a series.
#[derive(Debug, PartialEq)]
pub(super) struct CellRemovePlan {
    /// The stamps of the samples the replacement covers, ascending, from the first that changes
    /// to the last: every stored sample in that range.
    pub(super) replaced: Vec<u64>,
    /// What replaces them: the untouched ones as they were stored, the changed ones without the
    /// removed cells, ascending. A sample left with no cell is not among them.
    pub(super) points: Vec<FeaturePoint>,
    /// Whether the series changes.
    pub(super) changed: bool,
    pub(super) snapshots_before: u64,
    pub(super) snapshots_after: u64,
    pub(super) cells_before: u64,
    pub(super) cells_removed: u64,
}

fn refuse(key: &str, message: impl std::fmt::Display) -> Status {
    Status::error("invalid_argument", format!("{key}: {message}"))
}

/// Plans `spec` over `series`. `series` is `None` for a key that does not exist (or whose deadline
/// has passed).
pub(super) fn plan_window_reduce_remove(
    cache: &MultiLayerCache,
    block_store: &BlockStore,
    shard_id: ShardId,
    series: Option<&impl TimestampedSeries>,
    key: &str,
    routing_bucket: u32,
    spec: &CellRemoveSpec,
) -> Result<CellRemovePlan, Status> {
    validate_spec(spec).map_err(|why| refuse(key, why))?;
    let nothing = |snapshots: u64, cells_before: u64| CellRemovePlan {
        replaced: Vec::new(),
        points: Vec::new(),
        changed: false,
        snapshots_before: snapshots,
        snapshots_after: snapshots,
        cells_before,
        cells_removed: 0,
    };
    let Some(series) = series else { return Ok(nothing(0, 0)) };
    let total = series.len() as u64;
    let Some((newest_ms, newest_address)) = series.range(..).next_back() else { return Ok(nothing(0, 0)) };

    let (from_ms, to_ms) = (spec.start_ms.unwrap_or(0), spec.end_ms.unwrap_or(u64::MAX));
    let inside: Vec<(u64, &crate::block_store::ElementEntry)> =
        series.range(crate::engine::timestamp_range_bounds(from_ms, to_ms)).collect();
    if inside.is_empty() {
        return Ok(nothing(total, 0));
    }
    super::cell_set::within("samples inside the window", inside.len(), MAX_REMOVE_SAMPLES)
        .map_err(|why| refuse(key, format!("{why}: narrow the window and remove in steps")))?;

    let mut page_keys = PageKeys::new();
    let identity = || BlockIdentity::of(shard_id, "feature", key, None);
    let routing = Some(routing_bucket);

    // The header every sample is read under: the newest sample's.
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
    .ok_or_else(|| Status::error("data_loss", format!("{key}: the sample stored at {newest_ms} cannot be read")))?
    .map_err(|why| refuse(key, format!("the sample at {newest_ms} is not a cell sample: {why}")))?;
    let selection = Selection::compile(&header, spec).map_err(|why| refuse(key, why))?;

    let mut stored: Vec<Vec<u8>> = Vec::with_capacity(inside.len());
    let mut decoded: Vec<(u64, Vec<Cell>)> = Vec::with_capacity(inside.len());
    for &(timestamp_ms, address) in &inside {
        let (cells, bytes) = read_feature_value_with(
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
                Ok((decode_sample(&header, packed)?, bytes.to_vec()))
            },
        )
        .ok_or_else(|| Status::error("data_loss", format!("{key}: the sample stored at {timestamp_ms} cannot be read")))?
        .map_err(|why| refuse(key, format!("the sample at {timestamp_ms}: {why}")))?;
        decoded.push((timestamp_ms, cells));
        stored.push(bytes);
    }

    let outcomes = remove_cells(&selection, decoded);
    let cells_before: u64 = outcomes.iter().map(|outcome| (outcome.kept.len() + outcome.removed) as u64).sum();
    let cells_removed: u64 = outcomes.iter().map(|outcome| outcome.removed as u64).sum();
    let (Some(first), Some(last)) = (
        outcomes.iter().position(|outcome| outcome.removed > 0),
        outcomes.iter().rposition(|outcome| outcome.removed > 0),
    ) else {
        return Ok(nothing(total, cells_before));
    };

    let mut points = Vec::with_capacity(last - first + 1);
    let mut replaced = Vec::with_capacity(last - first + 1);
    let mut emptied = 0u64;
    for at in first..=last {
        let outcome = &outcomes[at];
        replaced.push(outcome.timestamp_ms);
        if outcome.removed == 0 {
            points.push(FeaturePoint { timestamp_ms: outcome.timestamp_ms, value: stored[at].clone() });
        } else if outcome.kept.is_empty() {
            emptied += 1;
        } else {
            let value = encode_value(&header, &outcome.kept).map_err(|why| refuse(key, why))?;
            points.push(FeaturePoint { timestamp_ms: outcome.timestamp_ms, value });
        }
    }
    Ok(CellRemovePlan {
        replaced,
        points,
        changed: true,
        snapshots_before: total,
        snapshots_after: total - emptied,
        cells_before,
        cells_removed,
    })
}
