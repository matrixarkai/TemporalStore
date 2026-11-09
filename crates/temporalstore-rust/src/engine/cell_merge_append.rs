// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The merge-reduce write policy of the feature append: what the request becomes once its cells
//! are merged into the snapshots already stored.
//!
//! # WHERE A VIOLATION IS REFUSED
//!
//! At VALIDATION, before the command reaches the log, the same place `Block` is refused. A refused
//! request writes nothing: no block, no log record, no index entry. Validation can reach the
//! stored snapshot (it is handed the cache and the block store, as the hash-increment precheck
//! is), so the checks that need stored state -- the header against the newest stored header, the
//! merged snapshot against its cell limit, a stored value that is not a cell snapshot -- sit beside
//! the ones that do not.
//!
//! Execution runs the SAME planner again. It is a pure function of the command and the state the
//! command is applied to, so a follower applying the committed entry and a restart replaying it
//! reach the bytes the leader planned. Replay skips validation, so a plan that fails there is a
//! state divergence, not a bad request: it writes nothing and answers a receipt of zeros, which
//! is what landed.
//!
//! # ONE ENGINE WRITE PER KEY
//!
//! The plan holds one point per distinct timestamp, ascending. Several points of the request at
//! one timestamp merge into each other in request order before the one write; the append helper is
//! called once for the command, as it is for every other feature policy.

use super::*;
use crate::engine::cell_set::{self, CellHeader};
use crate::engine::timestamped_series::TimestampedSeries;
use std::collections::BTreeMap;

/// What a merge-reduce request lands.
#[derive(Debug, PartialEq)]
pub(super) struct CellMergePlan {
    /// One point per distinct timestamp, ascending, each holding the merged value.
    pub(super) points: Vec<FeaturePoint>,
    /// Cells that did not exist at their timestamp before.
    pub(super) cells_created: u64,
    /// Cells folded into a member already stored, or repeated within the request.
    pub(super) cells_merged: u64,
}

fn refuse(key: &str, message: impl std::fmt::Display) -> Status {
    Status::error("invalid_argument", format!("{key}: {message}"))
}

/// The value stored at `timestamp_ms`, read the way a feature query reads it.
fn stored_value(
    cache: &MultiLayerCache,
    block_store: &BlockStore,
    shard_id: ShardId,
    series: &impl TimestampedSeries,
    key: &str,
    timestamp_ms: u64,
    routing_bucket: u32,
) -> Result<Option<Vec<u8>>, Status> {
    let Some(address) = series.get(timestamp_ms) else {
        return Ok(None);
    };
    let mut scratch = HashMap::new();
    read_feature_point_cached(
        cache,
        block_store,
        shard_id,
        timestamp_ms,
        address,
        BlockIdentity::of(shard_id, "feature", key, None),
        &mut scratch,
        Some(routing_bucket),
    )
    .map(|point| Some(point.value))
    .ok_or_else(|| {
        Status::error(
            "data_loss",
            format!("{key}: the snapshot stored at {timestamp_ms} cannot be read"),
        )
    })
}

/// Merges `points` into the snapshots `series` holds. `series` is `None` for a key that does not
/// exist (or whose deadline has passed).
pub(super) fn plan_cell_merge(
    cache: &MultiLayerCache,
    block_store: &BlockStore,
    shard_id: ShardId,
    series: Option<&impl TimestampedSeries>,
    key: &str,
    routing_bucket: u32,
    points: &[FeaturePoint],
) -> Result<CellMergePlan, Status> {
    if points.is_empty() {
        return Err(refuse(key, "a merge-reduce append needs at least one point"));
    }
    // The header of the newest stored snapshot is the one every request is checked against.
    let mut newest: Option<CellHeader> = match series.and_then(|s| s.range(..).next_back()) {
        None => None,
        Some((timestamp_ms, _)) => {
            let value = stored_value(
                cache,
                block_store,
                shard_id,
                series.expect("a newest snapshot implies a series"),
                key,
                timestamp_ms,
                routing_bucket,
            )?
            .expect("the newest timestamp has an address");
            let (header, _) = cell_set::split_value(&value).map_err(|err| {
                refuse(key, format!("the newest stored value ({timestamp_ms}) is not a cell snapshot: {err}"))
            })?;
            Some(header)
        }
    };

    let mut merged: BTreeMap<u64, Vec<u8>> = BTreeMap::new();
    let (mut created, mut folded) = (0u64, 0u64);
    for point in points {
        // The request's own value for this timestamp merges into what is stored there -- or into
        // what an earlier point of this request already merged there -- under one entry point
        // that owns the header check, the upgrade of an older snapshot, the merge and the encode.
        let existing: Option<Vec<u8>> = match merged.remove(&point.timestamp_ms) {
            Some(already) => Some(already),
            None => match series {
                Some(series) => stored_value(
                    cache,
                    block_store,
                    shard_id,
                    series,
                    key,
                    point.timestamp_ms,
                    routing_bucket,
                )?,
                None => None,
            },
        };
        let (value, effective, counts) =
            cell_set::merge_request(newest.as_ref(), existing.as_deref(), &point.value).map_err(|err| {
                refuse(key, format!("point at {}: {err}", point.timestamp_ms))
            })?;
        // The stored key must be the one the declared spec builds, or two spellings of one entity
        // would file under different keys and never merge.
        cell_set::decode_key(&effective.key, key)
            .map_err(|err| refuse(key, format!("key does not match the declared key spec: {err}")))?;
        created += counts.created as u64;
        folded += counts.merged as u64;
        merged.insert(point.timestamp_ms, value);
        newest = Some(effective);
    }
    Ok(CellMergePlan {
        points: merged
            .into_iter()
            .map(|(timestamp_ms, value)| FeaturePoint { timestamp_ms, value })
            .collect(),
        cells_created: created,
        cells_merged: folded,
    })
}
