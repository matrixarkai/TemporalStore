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
//! reach the bytes the leader planned. The two runs differ in one step only: validation stops
//! after the merge ([`PlanOutput::CountOnly`]) because the value it would encode is thrown away,
//! execution encodes it. A plan is not carried from one to the other: the state it was made
//! against can change between a command being validated and applied, and a replay has no
//! validation to carry it from. Replay skips validation, so a plan that fails there is a
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
use std::rc::Rc;

/// What a merge-reduce request lands.
#[derive(Debug, PartialEq)]
pub(super) struct CellMergePlan {
    /// One point per distinct timestamp, ascending, each holding the merged value. Empty for a plan
    /// that was asked only to count ([`PlanOutput::CountOnly`]).
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

/// Whether a plan encodes the values it would write. Validation only needs to know the request is
/// accepted, and the value it would encode is thrown away, so it stops after the merge: the encode
/// cannot refuse what the merge accepted (the header it writes was validated when it was decoded
/// or extended, and the cells were bounded while they were merged).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum PlanOutput {
    /// Count what lands; write no values.
    CountOnly,
    /// Also encode the value of each snapshot.
    Values,
}

/// The newest stored snapshot of a key: where it is, its bytes, and its header read once.
struct NewestStored {
    timestamp_ms: u64,
    value: Vec<u8>,
    header: Rc<CellHeader>,
}

/// Merges `points` into the snapshots `series` holds. `series` is `None` for a key that does not
/// exist (or whose deadline has passed).
#[allow(clippy::too_many_arguments)]
pub(super) fn plan_cell_merge(
    cache: &MultiLayerCache,
    block_store: &BlockStore,
    shard_id: ShardId,
    series: Option<&impl TimestampedSeries>,
    key: &str,
    routing_bucket: u32,
    points: &[FeaturePoint],
    output: PlanOutput,
) -> Result<CellMergePlan, Status> {
    if points.is_empty() {
        return Err(refuse(key, "a merge-reduce append needs at least one point"));
    }
    // The newest stored snapshot is read ONCE: its header is the one every request is checked
    // against, and a request point at that timestamp merges into its cells.
    let newest_stored: Option<NewestStored> = match series.and_then(|s| s.range(..).next_back()) {
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
            let (header_bytes, _) = cell_set::split_value_raw(&value).map_err(|err| {
                refuse(key, format!("the newest stored value ({timestamp_ms}) is not a cell snapshot: {err}"))
            })?;
            let header = CellHeader::decode(header_bytes).map_err(|err| {
                refuse(key, format!("the newest stored value ({timestamp_ms}) is not a cell snapshot: {err}"))
            })?;
            Some(NewestStored { timestamp_ms, value, header: Rc::new(header) })
        }
    };
    let mut newest: Option<Rc<CellHeader>> = newest_stored.as_ref().map(|stored| Rc::clone(&stored.header));

    // What each timestamp of the request holds so far. Several points at one timestamp merge into
    // each other in request order; the value is encoded once, after the last.
    let mut merged: BTreeMap<u64, (Rc<CellHeader>, cell_set::FlatCells)> = BTreeMap::new();
    let (mut created, mut folded) = (0u64, 0u64);
    // The header whose key spec was last checked against the key: one spec is checked once, not
    // once per point.
    let mut key_checked: Option<Rc<CellHeader>> = None;
    for point in points {
        let timestamp_ms = point.timestamp_ms;
        // The request's own value for this timestamp merges into what is stored there -- or into
        // what an earlier point of this request already merged there -- under one entry point
        // that owns the header check, the upgrade of an older snapshot and the merge.
        let loaded: Option<Vec<u8>>;
        let own: CellHeader;
        let existing = match merged.remove(&timestamp_ms) {
            Some((header, cells)) => cell_set::Existing::Planned { header, cells },
            None => match series {
                None => cell_set::Existing::Nothing,
                Some(series) => match newest_stored.as_ref().filter(|n| n.timestamp_ms == timestamp_ms) {
                    Some(stored) => {
                        let (_, snapshot) = cell_set::split_value_raw(&stored.value)
                            .map_err(|err| refuse(key, format!("point at {timestamp_ms}: {err}")))?;
                        cell_set::Existing::Stored { header: &*stored.header, snapshot }
                    }
                    None => {
                        loaded = stored_value(
                            cache,
                            block_store,
                            shard_id,
                            series,
                            key,
                            timestamp_ms,
                            routing_bucket,
                        )?;
                        match loaded.as_deref() {
                            None => cell_set::Existing::Nothing,
                            Some(value) => {
                                let (header, snapshot) = cell_set::split_value(value)
                                    .map_err(|err| refuse(key, format!("point at {timestamp_ms}: {err}")))?;
                                own = header;
                                cell_set::Existing::Stored { header: &own, snapshot }
                            }
                        }
                    }
                },
            },
        };
        let (effective, cells, counts) = cell_set::merge_point(newest.as_ref(), existing, &point.value)
            .map_err(|err| refuse(key, format!("point at {timestamp_ms}: {err}")))?;
        // The stored key must be the one the declared spec builds, or two spellings of one entity
        // would file under different keys and never merge.
        if !key_checked.as_ref().is_some_and(|checked| Rc::ptr_eq(checked, &effective)) {
            cell_set::decode_key(&effective.key, key)
                .map_err(|err| refuse(key, format!("key does not match the declared key spec: {err}")))?;
            key_checked = Some(Rc::clone(&effective));
        }
        created += counts.created as u64;
        folded += counts.merged as u64;
        newest = Some(Rc::clone(&effective));
        merged.insert(timestamp_ms, (effective, cells));
    }
    let mut planned = Vec::new();
    if output == PlanOutput::Values {
        planned.reserve_exact(merged.len());
        for (timestamp_ms, (header, cells)) in merged {
            let value = cells
                .encode_value(&header)
                .map_err(|err| refuse(key, format!("point at {timestamp_ms}: {err}")))?;
            planned.push(FeaturePoint { timestamp_ms, value });
        }
    }
    Ok(CellMergePlan { points: planned, cells_created: created, cells_merged: folded })
}
