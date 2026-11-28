// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The engine side of a cell rollup: reads the samples of ONE time bucket, hands them to the pure
//! [`super::window_reduce_rollup::rollup_flat_spec`], and says what the series must hold afterwards.
//!
//! # ONE BUCKET A CALL
//!
//! A call folds the OLDEST UNSETTLED bucket below the first cutoff and no more. A bucket is the
//! samples that share a bucket start under the tiers; it is settled when it holds one sample stamped
//! at its start, and unsettled otherwise. Which buckets are unsettled is decided from the stamps in
//! the address map alone, so the work of a call that reads and writes does not depend on how many
//! samples lie below the cutoff: it reads the samples of one bucket, writes one point, and counts
//! (without reading a block) the unsettled buckets that are left. The receipt says how many it
//! folded (`buckets_folded`, 0 or 1) and how many remain (`buckets_remaining`); a caller calls again
//! until a call folds none.
//!
//! A bucket already settled is not touched, whatever a cap would take from it: the cap is applied
//! when a bucket is folded.
//!
//! # WHERE A VIOLATION IS REFUSED
//!
//! At VALIDATION, before the command reaches the log, as the merge-reduce append is: a refused
//! request writes nothing. Execution runs the SAME planner again. It is a pure function of the
//! command and the stored series (a cutoff is a stamp in the request, or an age resolved against
//! the newest STORED sample; there is no clock), so a follower applying the committed entry and a
//! restart replaying it reach the bytes the leader planned. Replay skips validation, so a plan that
//! fails there is a state divergence, not a bad request: it writes nothing and answers a receipt
//! of zeros.
//!
//! # WHAT IS READ
//!
//! Only the samples of the bucket being folded, and the newest sample of the key for its header (a
//! sample written under an older header is read under the newest one; one whose header is not a
//! prefix of it is refused). The samples in one bucket are refused over [`MAX_ROLLUP_SAMPLES`]
//! BEFORE a block is read.
//!
//! # TRUNCATION
//!
//! A call that finds no bucket to fold applies the truncation, if the request has one: when the
//! series holds more than the trigger, the oldest samples are removed until the newest `keep_last`
//! remain. A truncation therefore acts on the series as the folds left it, and runs in the call that
//! reports `buckets_folded == 0`.
//!
//! # WHAT THE RECEIPT COUNTS
//!
//! Samples are counted over the whole series. Cells are counted over the bucket folded: how many it
//! held and how many its rolled sample holds. `cells_dropped` is the cap's. A truncation shows in
//! `samples_after`.
//!
//! A request that would change nothing (no unsettled bucket and nothing to truncate) reports
//! `buckets_folded == 0` and writes nothing.

use super::window_reduce_rollup::{
    bucket_start_of, bucket_tiers, first_cutoff, rollup_flat_spec, truncation_drop, truncation_of, validate_spec,
    FlatSample, RollupSpec, MAX_ROLLUP_SAMPLES,
};
use super::cell_set::{check_header, split_value, CellHeader, FlatCells};
use super::*;
use crate::engine::timestamped_series::TimestampedSeries;
use crate::types::FeaturePoint;

/// What a rollup request does to a series.
#[derive(Debug, PartialEq)]
pub(super) struct WindowReduceRollupPlan {
    /// Stamps of the stored samples the request replaces or removes, ascending, in one run of the
    /// series: the samples of the bucket folded, or the oldest samples a truncation removes.
    pub(super) replaced: Vec<u64>,
    /// What replaces them: the rolled sample of the bucket, or nothing for a truncation.
    pub(super) points: Vec<FeaturePoint>,
    /// Whether the series changes.
    pub(super) changed: bool,
    pub(super) samples_before: u64,
    pub(super) samples_after: u64,
    pub(super) cells_before: u64,
    pub(super) cells_after: u64,
    pub(super) cells_dropped: u64,
    /// Buckets this call folded: 1, or 0 when none was unsettled.
    pub(super) buckets_folded: u64,
    /// Unsettled buckets still below the cutoff after this call.
    pub(super) buckets_remaining: u64,
}

fn refuse(key: &str, message: impl std::fmt::Display) -> Status {
    Status::error("invalid_argument", format!("{key}: {message}"))
}

/// The samples of one bucket, as found in the address map.
#[derive(Clone, Copy)]
struct Group {
    start: u64,
    count: usize,
    first: u64,
    last: u64,
}

impl Group {
    fn unsettled(&self) -> bool {
        self.count > 1 || self.first != self.start
    }
}

/// Plans `spec` over `series`. `series` is `None` for a key that does not exist (or whose deadline
/// has passed).
pub(super) fn plan_window_reduce_rollup(
    cache: &MultiLayerCache,
    block_store: &BlockStore,
    shard_id: ShardId,
    series: Option<&impl TimestampedSeries>,
    key: &str,
    routing_bucket: u32,
    spec: &RollupSpec,
) -> Result<WindowReduceRollupPlan, Status> {
    validate_spec(spec).map_err(|why| refuse(key, why))?;
    let empty = |samples: u64| WindowReduceRollupPlan {
        replaced: Vec::new(),
        points: Vec::new(),
        changed: false,
        samples_before: samples,
        samples_after: samples,
        cells_before: 0,
        cells_after: 0,
        cells_dropped: 0,
        buckets_folded: 0,
        buckets_remaining: 0,
    };
    let Some(series) = series else { return Ok(empty(0)) };
    let total = series.len() as u64;
    let Some((newest_ms, newest_address)) = series.range(..).next_back() else { return Ok(empty(0)) };

    // Under an age anchor the cutoffs are resolved against the newest STORED stamp.
    let cutoff = first_cutoff(spec, newest_ms);
    let tiers = bucket_tiers(spec, newest_ms);

    // The oldest unsettled bucket, and how many more there are: stamps only, no block is read.
    let mut target: Option<Group> = None;
    let mut remaining = 0u64;
    let mut open: Option<Group> = None;
    let close = |group: Group, target: &mut Option<Group>, remaining: &mut u64| {
        if group.unsettled() {
            if target.is_none() {
                *target = Some(group);
            } else {
                *remaining += 1;
            }
        }
    };
    for (timestamp_ms, _) in series.range(..cutoff) {
        let start = bucket_start_of(&tiers, timestamp_ms).expect("a sample below the first cutoff has a tier");
        match open.as_mut() {
            Some(group) if group.start == start => {
                group.count += 1;
                group.last = timestamp_ms;
            }
            _ => {
                if let Some(done) = open.take() {
                    close(done, &mut target, &mut remaining);
                }
                open = Some(Group { start, count: 1, first: timestamp_ms, last: timestamp_ms });
            }
        }
    }
    if let Some(done) = open.take() {
        close(done, &mut target, &mut remaining);
    }

    let Some(group) = target else {
        // Nothing to fold: a truncation, if the request has one and the series is over its trigger.
        let Some(truncation) = truncation_of(spec) else { return Ok(empty(total)) };
        let drop = truncation_drop(total as usize, &truncation);
        if drop == 0 {
            return Ok(empty(total));
        }
        let replaced: Vec<u64> = series.range(..).take(drop).map(|(at, _)| at).collect();
        return Ok(WindowReduceRollupPlan {
            replaced,
            points: Vec::new(),
            changed: true,
            samples_before: total,
            samples_after: total - drop as u64,
            cells_before: 0,
            cells_after: 0,
            cells_dropped: 0,
            buckets_folded: 0,
            buckets_remaining: 0,
        });
    };
    super::cell_set::within("samples in one bucket", group.count, MAX_ROLLUP_SAMPLES)
        .map_err(|why| refuse(key, format!("{why}: the bucket is too wide to fold in one call")))?;

    let mut block_keys = BlockKeys::new();
    let identity = || BlockIdentity::of(shard_id, "feature", key, None);
    let routing = Some(routing_bucket);

    // The header every older sample is read under: the newest sample's.
    let header: CellHeader = read_feature_value_with(
        cache,
        block_store,
        shard_id,
        newest_ms,
        newest_address,
        identity(),
        routing,
        &mut block_keys,
        |bytes| split_value(bytes).map(|(header, _)| header),
    )
    .ok_or_else(|| Status::error("data_loss", format!("{key}: the sample stored at {newest_ms} cannot be read")))?
    .map_err(|why| refuse(key, format!("the sample at {newest_ms} is not a cell sample: {why}")))?;

    let mut stored: Vec<Vec<u8>> = Vec::with_capacity(group.count);
    let mut samples: Vec<FlatSample> = Vec::with_capacity(group.count);
    let mut replaced: Vec<u64> = Vec::with_capacity(group.count);
    for (timestamp_ms, address) in series.range(group.first..=group.last) {
        let decoded = read_feature_value_with(
            cache,
            block_store,
            shard_id,
            timestamp_ms,
            address,
            identity(),
            routing,
            &mut block_keys,
            |bytes| -> Result<(FlatCells, Vec<u8>), String> {
                let (own, packed) = split_value(bytes)?;
                if own != header {
                    check_header(&own, &header)?;
                }
                Ok((FlatCells::decode(&header, packed, 0)?, bytes.to_vec()))
            },
        )
        .ok_or_else(|| Status::error("data_loss", format!("{key}: the sample stored at {timestamp_ms} cannot be read")))?
        .map_err(|why| refuse(key, format!("the sample at {timestamp_ms}: {why}")))?;
        samples.push(FlatSample { timestamp_ms, cells: decoded.0 });
        stored.push(decoded.1);
        replaced.push(timestamp_ms);
    }
    let result = rollup_flat_spec(&header, samples, spec, newest_ms).map_err(|why| refuse(key, why))?;

    let mut points = Vec::with_capacity(result.samples.len());
    let mut cells_after = 0u64;
    for sample in &result.samples {
        cells_after += sample.cells.len() as u64;
        let value = sample.cells.encode_value(&header).map_err(|why| refuse(key, why))?;
        points.push(FeaturePoint { timestamp_ms: sample.timestamp_ms, value });
    }
    let changed = points.len() != stored.len()
        || points
            .iter()
            .zip(replaced.iter().zip(&stored))
            .any(|(point, (timestamp_ms, bytes))| point.timestamp_ms != *timestamp_ms || point.value != *bytes);
    Ok(WindowReduceRollupPlan {
        samples_after: total - replaced.len() as u64 + points.len() as u64,
        replaced,
        points,
        changed,
        samples_before: total,
        cells_before: result.stats.cells_before,
        cells_after,
        cells_dropped: result.stats.cells_dropped,
        buckets_folded: 1,
        buckets_remaining: remaining,
    })
}
