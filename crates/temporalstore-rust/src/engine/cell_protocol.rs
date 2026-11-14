// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The cell protocol as a pure translator: wire messages (`proto/temporalstore/cells/v1`) to the
//! commands the engine already runs, and the engine's answers back to wire messages. It reads no
//! engine state and writes nothing.
//!
//! # ADMISSION
//!
//! Every bound is compared before any cell is built, through the one function
//! [`super::cell_set::within`], so a request over a limit costs work proportional to the limit and
//! not to its size. A refused request produces no command at all: nothing is partly translated.
//!
//! # WHAT IS NOT TRANSLATED YET
//!
//! Rank modes and windows that the ranked read cannot express today (`RECENT`, `MATCH_LANE`,
//! `MEMBERS_ONLY`, a quota, the newest-anchored window and the newest-rows window) are refused with
//! a message that says so; they map onto read-side options another change adds. `REMOVE` and
//! `DELETE` have no command to map to here.

use super::cell_set::{
    self, within, within_nonzero, Cell, CellHeader, CellKey, DimSpec, DimType, DimValue, EntityType,
    EntityValue, KeySpec, LaneSpec, Member, HEADER_VERSION, MAX_CELLS_PER_REQUEST, MAX_KEY_DIMENSIONS,
    MAX_LANES_PER_CELL, MAX_NAME_BYTES, MAX_TOP_K,
};
use crate::types::{
    CellMemberId, CellRankSpec, CellScore, CellWindow, Command, FeatureFilter, FeatureFilterOp,
    FeaturePoint, FeatureWritePolicy, RankedCells,
};

pub(crate) mod wire {
    #![allow(clippy::all, missing_docs)]
    tonic::include_proto!("temporalstore.cells.v1");
}
use wire::CellStatusCode;

/// Queries in one batch.
pub(crate) const MAX_BATCH_QUERIES: usize = 64;
/// Members on one side of a member list.
pub(crate) const MAX_MEMBER_LIST: usize = 1024;
/// Entities read together with the first.
pub(crate) const MAX_MORE_ENTITIES: usize = 63;
/// Weighted lanes in one rank.
pub(crate) const MAX_WEIGHTED: usize = 8;
/// The z a Wilson rank uses when the request names none.
pub(crate) const DEFAULT_WILSON_Z: f64 = 1.96;

/// A refusal: the status code and a message that names what was wrong.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Refusal {
    pub(crate) code: CellStatusCode,
    pub(crate) message: String,
}

fn invalid(message: impl Into<String>) -> Refusal {
    Refusal { code: CellStatusCode::InvalidArgument, message: message.into() }
}

fn bound(what: &str, got: usize, limit: usize) -> Result<(), Refusal> {
    within(what, got, limit).map_err(invalid)
}

/// How a label is derived when a request leaves it out.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LabelRule {
    /// Absent means zero.
    FromRequest,
    /// The bits `[shift, shift + bits)` of the member id.
    FromMember { shift: u32, bits: u32 },
}

/// The shape of a table's cells on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Shape {
    Pair,
    List,
}

/// What a deployment declares for one table name, once.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct TableProfile {
    pub(crate) name: String,
    pub(crate) entity: EntityType,
    pub(crate) dimensions: Vec<DimSpec>,
    /// Index into `dimensions` of the dimension the wire `channel` fills.
    pub(crate) channel: Option<usize>,
    /// Index into `dimensions` of the dimension the wire `kind` fills.
    pub(crate) kind: Option<usize>,
    /// Label names; the first is the one the wire `label` fills.
    pub(crate) labels: Vec<String>,
    pub(crate) label_rule: LabelRule,
    pub(crate) lanes: Vec<LaneSpec>,
    pub(crate) shape: Shape,
    /// A second cell for a member already in the snapshot is refused, not merged.
    pub(crate) refuse_duplicates: bool,
    /// Oldest and newest accepted timestamp, as distances from now, in microseconds.
    pub(crate) max_past_us: i64,
    pub(crate) max_future_us: i64,
}

impl TableProfile {
    /// Checks the profile is internally consistent.
    pub(crate) fn validate(&self) -> Result<(), String> {
        within_nonzero("table name bytes", self.name.len(), MAX_NAME_BYTES)?;
        self.header().validate()?;
        for (at, dim) in self.dimensions.iter().enumerate() {
            let bound_to_wire = self.channel == Some(at) || self.kind == Some(at);
            if bound_to_wire && dim.ty == DimType::Text {
                return Err(format!("dimension `{}` carries a number on the wire and cannot be text", dim.name));
            }
            if !bound_to_wire && dim.required {
                return Err(format!("dimension `{}` is required but nothing on the wire fills it", dim.name));
            }
        }
        for slot in [self.channel, self.kind].into_iter().flatten() {
            if slot >= self.dimensions.len() {
                return Err("channel or kind names a dimension that is not declared".to_string());
            }
        }
        if self.channel.is_some() && self.channel == self.kind {
            return Err("channel and kind cannot fill the same dimension".to_string());
        }
        match self.shape {
            Shape::Pair if self.lanes.len() != 2 => return Err("a pair table has exactly two lanes".to_string()),
            Shape::List => within_nonzero("lanes", self.lanes.len(), MAX_LANES_PER_CELL)?,
            _ => {}
        }
        if self.max_past_us <= 0 || self.max_future_us <= 0 {
            return Err("the accepted timestamp range must be positive on both sides".to_string());
        }
        Ok(())
    }

    pub(crate) fn header(&self) -> CellHeader {
        CellHeader {
            version: HEADER_VERSION,
            key: KeySpec {
                namespace: self.name.clone(),
                entity: self.entity,
                dimensions: self.dimensions.clone(),
            },
            labels: self.labels.clone(),
            lanes: self.lanes.clone(),
        }
    }
}

/// The profiles of one process, looked up by the wire table name.
#[derive(Debug, Default)]
pub(crate) struct Profiles {
    profiles: Vec<TableProfile>,
}

impl Profiles {
    pub(crate) fn new(profiles: Vec<TableProfile>) -> Result<Profiles, String> {
        for (at, profile) in profiles.iter().enumerate() {
            profile.validate()?;
            if profiles[..at].iter().any(|earlier| earlier.name == profile.name) {
                return Err(format!("table `{}` is declared twice", profile.name));
            }
        }
        Ok(Profiles { profiles })
    }

    /// The profile a wire table name selects.
    pub(crate) fn lookup(&self, table: &[u8]) -> Result<&TableProfile, Refusal> {
        within_nonzero("table name bytes", table.len(), MAX_NAME_BYTES).map_err(invalid)?;
        self.profiles
            .iter()
            .find(|profile| profile.name.as_bytes() == table)
            .ok_or_else(|| Refusal {
                code: CellStatusCode::NoSuchTable,
                message: format!("no table named `{}`", String::from_utf8_lossy(table)),
            })
    }
}

// ------------------------------------------------------------------------------------------
// Writes
// ------------------------------------------------------------------------------------------

fn dim_value(ty: DimType, what: &str, value: i32) -> Result<DimValue, Refusal> {
    let bad = || invalid(format!("{what} {value} does not fit its dimension"));
    Ok(match ty {
        DimType::U16 => DimValue::U16(u16::try_from(value).map_err(|_| bad())?),
        DimType::U32 => DimValue::U32(u32::try_from(value).map_err(|_| bad())?),
        DimType::U64 => DimValue::U64(u64::try_from(value).map_err(|_| bad())?),
        DimType::Text => return Err(bad()),
    })
}

/// The stored key an entity, channel and kind name under a profile.
fn key_of(
    profile: &TableProfile,
    entity: EntityValue,
    channel: i32,
    kind: i32,
) -> Result<String, Refusal> {
    within("dimensions", profile.dimensions.len(), MAX_KEY_DIMENSIONS).map_err(invalid)?;
    let mut dims: Vec<Option<DimValue>> = vec![None; profile.dimensions.len()];
    for (slot, what, value) in [(profile.channel, "channel", channel), (profile.kind, "kind", kind)] {
        match slot {
            Some(at) => dims[at] = Some(dim_value(profile.dimensions[at].ty, what, value)?),
            None if value != 0 => {
                return Err(invalid(format!("this table has no {what} dimension, so {what} must be 0")))
            }
            None => {}
        }
    }
    cell_set::encode_key(&profile.header().key, &CellKey { entity, dims }).map_err(invalid)
}

fn entity_of(
    profile: &TableProfile,
    id: i64,
    text: &str,
    bytes: &[u8],
) -> Result<EntityValue, Refusal> {
    let forms = [id != 0, !text.is_empty(), !bytes.is_empty()];
    if forms.iter().filter(|set| **set).count() > 1 {
        return Err(invalid("set at most one of the entity forms"));
    }
    match profile.entity {
        EntityType::U64 if forms[1] || forms[2] => Err(invalid("this table's entity is a 64 bit id")),
        EntityType::U64 => Ok(EntityValue::U64(id as u64)),
        EntityType::Text if id != 0 || forms[2] => Err(invalid("this table's entity is text")),
        EntityType::Text => Ok(EntityValue::Text(text.to_string())),
        EntityType::Bytes if id != 0 || forms[1] => Err(invalid("this table's entity is bytes")),
        EntityType::Bytes => Ok(EntityValue::Bytes(bytes.to_vec())),
    }
}

fn cell_of(profile: &TableProfile, entry: &wire::CellEntry) -> Result<Cell, Refusal> {
    let shape = match entry.shape() {
        wire::CellShape::Pair => Shape::Pair,
        wire::CellShape::List => Shape::List,
    };
    if shape != profile.shape {
        return Err(invalid(format!(
            "entry shape {shape:?} does not match the table's {:?}",
            profile.shape
        )));
    }
    let member = if !entry.member_bytes.is_empty() {
        if entry.member != 0 {
            return Err(invalid("set at most one of member and member_bytes"));
        }
        within_nonzero("member bytes", entry.member_bytes.len(), cell_set::MAX_MEMBER_BYTES).map_err(invalid)?;
        Member::Bytes(entry.member_bytes.clone())
    } else {
        Member::U64(entry.member as u64)
    };
    let lanes: Vec<i64> = match profile.shape {
        Shape::Pair => {
            let (first, second) = match (&entry.pair, entry.list.is_some()) {
                (Some(pair), _) => (pair.first, pair.second),
                (None, false) => (entry.shorthand_first, entry.shorthand_second),
                (None, true) => return Err(invalid("a pair table takes a pair, not a list")),
            };
            vec![first, second]
        }
        Shape::List => {
            let list = entry.list.as_ref().ok_or_else(|| invalid("a list table takes a list"))?;
            bound("lane values", list.values.len(), profile.lanes.len())?;
            let mut values = list.values.clone();
            values.resize(profile.lanes.len(), 0);
            values
        }
    };
    let labels = match profile.labels.len() {
        0 => {
            if entry.has_label && entry.label != 0 {
                return Err(invalid("this table declares no label, so a label must be absent or 0"));
            }
            Vec::new()
        }
        count => {
            let first = if entry.has_label {
                u64::try_from(entry.label).map_err(|_| invalid(format!("label {} is negative", entry.label)))?
            } else {
                match (profile.label_rule, &member) {
                    (LabelRule::FromMember { shift, bits }, Member::U64(id)) => {
                        let mask = if bits >= 64 { u64::MAX } else { (1u64 << bits) - 1 };
                        id.checked_shr(shift).unwrap_or(0) & mask
                    }
                    _ => 0,
                }
            };
            let mut labels = vec![0u64; count];
            labels[0] = first;
            labels
        }
    };
    Ok(Cell { member, labels, lanes })
}

fn micros_to_ms(micros: i64) -> u64 {
    micros.div_euclid(1000) as u64
}

/// The flag is carried on the wire, but no write mode implements it yet: a retried add is covered
/// by the retry token, and a silent merge would double-count what the sender meant to skip.
fn refuse_skip_existing(requested: bool) -> Result<(), Refusal> {
    if requested {
        return Err(invalid(
            "skip_existing is not supported yet: use a retry token to make a retried add safe",
        ));
    }
    Ok(())
}

/// One request's observations as feature appends: one command per distinct key, each holding one
/// point per distinct millisecond. `now_us` is the clock the accepted range is measured from.
pub(crate) fn translate_add(
    profile: &TableProfile,
    request: &wire::CellAddRequest,
    now_us: i64,
) -> Result<Vec<Command>, Refusal> {
    refuse_skip_existing(request.skip_existing)?;
    let observations: Vec<Obs<'_>> = request
        .observations
        .iter()
        .map(|o| Obs {
            entity: o.entity,
            entity_text: &o.entity_text,
            entity_bytes: &o.entity_bytes,
            ts_us: o.timestamp_us,
            kind: o.kind,
            channel: o.channel,
            entries: &o.entries,
        })
        .collect();
    translate_observations(profile, &observations, now_us)
}

/// Pre-aggregated cells written at their latest timestamp; the earliest must not follow it.
pub(crate) fn translate_restore(
    profile: &TableProfile,
    request: &wire::CellRestoreRequest,
    now_us: i64,
) -> Result<Vec<Command>, Refusal> {
    refuse_skip_existing(request.skip_existing)?;
    for o in &request.observations {
        if o.earliest_us > o.latest_us {
            return Err(invalid(format!(
                "earliest {} follows latest {}",
                o.earliest_us, o.latest_us
            )));
        }
    }
    let observations: Vec<Obs<'_>> = request
        .observations
        .iter()
        .map(|o| Obs {
            entity: o.entity,
            entity_text: &o.entity_text,
            entity_bytes: &o.entity_bytes,
            ts_us: o.latest_us,
            kind: o.kind,
            channel: o.channel,
            entries: &o.entries,
        })
        .collect();
    translate_observations(profile, &observations, now_us)
}

struct Obs<'a> {
    entity: i64,
    entity_text: &'a str,
    entity_bytes: &'a [u8],
    ts_us: i64,
    kind: i32,
    channel: i32,
    entries: &'a [wire::CellEntry],
}

fn translate_observations(
    profile: &TableProfile,
    observations: &[Obs<'_>],
    now_us: i64,
) -> Result<Vec<Command>, Refusal> {
    within_nonzero("observations", observations.len(), MAX_CELLS_PER_REQUEST).map_err(invalid)?;
    let mut total = 0usize;
    for o in observations {
        within_nonzero("entries per observation", o.entries.len(), MAX_CELLS_PER_REQUEST).map_err(invalid)?;
        total += o.entries.len();
        bound("cells per request", total, MAX_CELLS_PER_REQUEST)?;
    }
    let header = profile.header();
    // (key, timestamp_ms) groups in first-seen order; a request holds at most MAX_CELLS_PER_REQUEST.
    let mut groups: Vec<(String, u64, Vec<Cell>)> = Vec::new();
    for o in observations {
        if o.ts_us <= now_us.saturating_sub(profile.max_past_us)
            || o.ts_us >= now_us.saturating_add(profile.max_future_us)
        {
            return Err(invalid(format!(
                "timestamp {} is outside the accepted range (microseconds)",
                o.ts_us
            )));
        }
        let entity = entity_of(profile, o.entity, o.entity_text, o.entity_bytes)?;
        let key = key_of(profile, entity, o.channel, o.kind)?;
        let ts_ms = micros_to_ms(o.ts_us);
        let at = match groups.iter().position(|(k, t, _)| *k == key && *t == ts_ms) {
            Some(at) => at,
            None => {
                groups.push((key, ts_ms, Vec::new()));
                groups.len() - 1
            }
        };
        for entry in o.entries {
            let cell = cell_of(profile, entry)?;
            if profile.refuse_duplicates && groups[at].2.iter().any(|c| c.member == cell.member) {
                return Err(Refusal {
                    code: CellStatusCode::DuplicateRefused,
                    message: "a second cell for a member at one timestamp is refused by this table".to_string(),
                });
            }
            groups[at].2.push(cell);
        }
    }
    // One command per key, points ascending by time.
    groups.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
    let mut commands: Vec<Command> = Vec::new();
    for (key, timestamp_ms, cells) in groups {
        let value = cell_set::encode_value(&header, &cells).map_err(invalid)?;
        let point = FeaturePoint { timestamp_ms, value };
        match commands.last_mut() {
            Some(Command::FeatureAppendWithPolicy { key: last, points, .. }) if *last == key => {
                points.push(point)
            }
            _ => commands.push(Command::FeatureAppendWithPolicy {
                key,
                points: vec![point],
                policy: FeatureWritePolicy::MergeReduce,
            }),
        }
    }
    Ok(commands)
}

/// The response to a write, from what the engine's receipts add up to.
pub(crate) fn add_response(
    snapshots_written: u64,
    cells_created: u64,
    cells_merged: u64,
) -> wire::CellAddResponse {
    wire::CellAddResponse {
        code: CellStatusCode::Ok as i32,
        message: String::new(),
        snapshots_written,
        cells_created,
        cells_merged,
        replayed: false,
    }
}

pub(crate) fn refused_add_response(refusal: &Refusal) -> wire::CellAddResponse {
    wire::CellAddResponse {
        code: refusal.code as i32,
        message: refusal.message.clone(),
        ..Default::default()
    }
}

// ------------------------------------------------------------------------------------------
// Reads
// ------------------------------------------------------------------------------------------

/// A query as the engine reads it: the key and the ranked spec.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ReadPlan {
    pub(crate) key: String,
    pub(crate) spec: CellRankSpec,
}

fn lane_name(profile: &TableProfile, at: i32, what: &str) -> Result<String, Refusal> {
    usize::try_from(at)
        .ok()
        .and_then(|at| profile.lanes.get(at))
        .map(|lane| lane.name.clone())
        .ok_or_else(|| invalid(format!("{what} {at} is not a lane of this table")))
}

fn window_of(window: &wire::Window) -> Result<CellWindow, Refusal> {
    if window.span < 0 {
        return Err(invalid(format!("window span {} is negative", window.span)));
    }
    match window.kind() {
        wire::WindowKind::TrailingFromNow => Ok(CellWindow::Last { duration_ms: (window.span / 1000) as u64 }),
        wire::WindowKind::FromStart => {
            if window.span == 0 {
                return Err(invalid("a window of span 0 holds nothing"));
            }
            let start = window.start_us;
            let end = start.saturating_add(window.span);
            // Snapshots stamped t ms hold t * 1000 microseconds: [start, end) in whole ms.
            let first = start.div_euclid(1000) + i64::from(start.rem_euclid(1000) != 0);
            let after = end.div_euclid(1000) + i64::from(end.rem_euclid(1000) != 0);
            if first < 0 || after <= first {
                return Err(invalid("window holds no whole millisecond"));
            }
            Ok(CellWindow::Between { start_ms: first as u64, end_ms: (after - 1) as u64 })
        }
        other => Err(invalid(format!(
            "window kind {other:?} is not translated yet: it needs a read-side option that is not built"
        ))),
    }
}

fn score_of(profile: &TableProfile, selector: &wire::CellSelector) -> Result<CellScore, Refusal> {
    use wire::RankMode as Mode;
    Ok(match selector.mode() {
        Mode::FirstLane => CellScore::Lane { lane: lane_name(profile, 0, "lane")? },
        Mode::SecondLane => CellScore::Lane { lane: lane_name(profile, 1, "lane")? },
        Mode::Ratio => CellScore::Ratio {
            numerator: lane_name(profile, 0, "lane")?,
            denominator: lane_name(profile, 1, "lane")?,
        },
        Mode::Member => CellScore::Member,
        Mode::NamedLane => {
            let at = if selector.has_sort_lane { selector.sort_lane } else { 0 };
            CellScore::Lane { lane: lane_name(profile, at, "sort lane")? }
        }
        Mode::Wilson => {
            let z = if selector.has_wilson_z && selector.wilson_z > 0.0 { selector.wilson_z } else { DEFAULT_WILSON_Z };
            CellScore::Wilson {
                successes: lane_name(profile, 0, "lane")?,
                trials: lane_name(profile, 1, "lane")?,
                z,
            }
        }
        Mode::Weighted => {
            if !selector.has_weighted_lanes
                || !selector.has_weights
                || selector.weighted_lanes.is_empty()
                || selector.weighted_lanes.len() != selector.weights.len()
            {
                return Err(invalid("a weighted rank needs lanes and weights of the same, non-zero length"));
            }
            bound("weighted lanes", selector.weighted_lanes.len(), MAX_WEIGHTED)?;
            if let Some(w) = selector.weights.iter().find(|w| **w < 0) {
                return Err(invalid(format!("weight {w} is negative")));
            }
            let lanes = selector
                .weighted_lanes
                .iter()
                .map(|at| lane_name(profile, *at, "weighted lane"))
                .collect::<Result<Vec<_>, _>>()?;
            CellScore::Weighted { lanes, weights: selector.weights.iter().map(|w| f64::from(*w)).collect() }
        }
        other => {
            return Err(invalid(format!(
                "rank mode {other:?} is not translated yet: it needs a read-side option that is not built"
            )))
        }
    })
}

/// One wire query as the engine's ranked read.
pub(crate) fn translate_query(profile: &TableProfile, query: &wire::CellQuery) -> Result<ReadPlan, Refusal> {
    bound("more entities", query.more_entities.len(), MAX_MORE_ENTITIES)?;
    if !query.more_entities.is_empty() {
        return Err(invalid("reading several entities together is not translated yet"));
    }
    let window = query.window.as_ref().ok_or_else(|| invalid("a query needs a window"))?;
    let selector = query.selector.as_ref().ok_or_else(|| invalid("a query needs a selector"))?;
    let entity = entity_of(profile, query.entity, &query.entity_text, &query.entity_bytes)?;
    let key = key_of(profile, entity, selector.channel, selector.kind)?;
    let window = window_of(window)?;
    let score = score_of(profile, selector)?;
    if !selector.quota_lanes.is_empty() || !selector.quota_counts.is_empty() {
        return Err(invalid("a quota is not translated yet"));
    }
    if !selector.members.is_empty() || !selector.excluded_members.is_empty() {
        bound("members", selector.members.len(), MAX_MEMBER_LIST)?;
        bound("excluded members", selector.excluded_members.len(), MAX_MEMBER_LIST)?;
        return Err(invalid("a member list is not translated yet"));
    }
    let limit = selector.limit;
    let top_k = usize::try_from(limit)
        .ok()
        .filter(|k| *k > 0)
        .ok_or_else(|| invalid(format!("limit {limit} must be positive")))?;
    within_nonzero("limit", top_k, MAX_TOP_K).map_err(invalid)?;
    let mut filters = Vec::new();
    match profile.labels.first() {
        Some(name) => filters.push(FeatureFilter {
            field: name.clone(),
            op: FeatureFilterOp::Equal,
            value: u64::try_from(selector.label)
                .map_err(|_| invalid(format!("label {} is negative", selector.label)))?,
        }),
        None if selector.label != 0 => {
            return Err(invalid("this table declares no label, so the selector label must be 0"))
        }
        None => {}
    }
    Ok(ReadPlan { key, spec: CellRankSpec { window, filters, score, top_k, project: Vec::new() } })
}

/// A batch as its independent reads, in request order. A query that cannot be translated refuses
/// the whole batch, naming its position.
pub(crate) fn translate_batch(
    profiles: &Profiles,
    batch: &wire::CellBatchQuery,
) -> Result<Vec<ReadPlan>, Refusal> {
    within_nonzero("queries per batch", batch.queries.len(), MAX_BATCH_QUERIES).map_err(invalid)?;
    batch
        .queries
        .iter()
        .enumerate()
        .map(|(at, query)| {
            let profile = profiles.lookup(&query.table)?;
            translate_query(profile, query)
                .map_err(|r| Refusal { code: r.code, message: format!("query {at}: {}", r.message) })
        })
        .collect()
}

/// The engine's ranked answer as a wire answer.
pub(crate) fn answer_of(profile: &TableProfile, query: &wire::CellQuery, ranked: &RankedCells) -> wire::CellAnswer {
    if !ranked.refused.is_empty() {
        return wire::CellAnswer {
            entity: query.entity,
            code: CellStatusCode::InvalidArgument as i32,
            message: ranked.refused.clone(),
            ..Default::default()
        };
    }
    let shape = match profile.shape {
        Shape::Pair => wire::CellShape::Pair,
        Shape::List => wire::CellShape::List,
    };
    let (mut first, mut last) = (u64::MAX, 0u64);
    let entries = ranked
        .rows
        .iter()
        .map(|row| {
            first = first.min(row.last_ms);
            last = last.max(row.last_ms);
            let timestamp_us = (row.last_ms as i64).saturating_mul(1000);
            let mut entry = wire::CellEntry { shape: shape as i32, ..Default::default() };
            match &row.member {
                CellMemberId::U64(id) => entry.member = *id as i64,
                CellMemberId::Bytes(bytes) => entry.member_bytes = bytes.clone(),
            }
            if let Some(label) = row.labels.first() {
                entry.label = *label as i32;
                entry.has_label = true;
            }
            match profile.shape {
                Shape::Pair => {
                    entry.pair = Some(wire::PairLanes {
                        first: row.lanes.first().copied().unwrap_or(0),
                        second: row.lanes.get(1).copied().unwrap_or(0),
                        timestamp_us,
                    })
                }
                Shape::List => entry.list = Some(wire::LaneList { timestamp_us, values: row.lanes.clone() }),
            }
            entry
        })
        .collect::<Vec<_>>();
    let echoed = query.echo && query.has_echo;
    wire::CellAnswer {
        entity: query.entity,
        first_us: if entries.is_empty() { 0 } else { (first as i64).saturating_mul(1000) },
        last_us: (last as i64).saturating_mul(1000),
        entries,
        snapshot_info: None,
        echoed_selector: echoed.then(|| query.selector.clone()).flatten(),
        echoed_window: echoed.then(|| query.window.clone()).flatten(),
        code: CellStatusCode::Ok as i32,
        message: String::new(),
        snapshots: ranked.snapshots as u64,
        unscored: ranked.unscored,
    }
}

#[cfg(test)]
#[path = "cell_protocol_tests.rs"]
mod tests;
