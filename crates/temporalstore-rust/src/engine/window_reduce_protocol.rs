// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The cell protocol as a pure translator: wire messages (`proto/temporalstore/windowreduce/v1`) to the
//! commands the engine already runs, and the engine's answers back to wire messages. It reads no
//! engine state and writes nothing.
//!
//! # ADMISSION
//!
//! Every bound is compared before any cell is built, through the one function
//! [`super::cell_set::within`], so a request over a limit costs work proportional to the limit and
//! not to its size. A refused request produces no command at all: nothing is partly translated.
//!
//! # WHAT IS TRANSLATED
//!
//! A query becomes one ranked read ([`Command::WindowReduceRank`]); a query that names `more_entities`
//! becomes one merged read over all of their keys ([`Command::WindowReduceFanInRank`]); a batch of
//! queries becomes [`Command::WindowReduceRankBatch`]. The selector's member set, the window kinds
//! anchored on the newest sample, the quota and the decayed-lane mode map onto the options the
//! ranked read carries in its spec. Every translated spec is also passed through the engine's own
//! admission check ([`super::window_reduce_rank::validate_spec`]), so a request the translator accepts is one
//! the command accepts.
//!
//! # WHAT IS NOT TRANSLATED YET
//!
//! `RECENT` and `MATCH_LANE` answer stored cells, one row each and none reduced, newest first: both
//! are the ranked read with [`WindowReduceScore::Recent`], and `MATCH_LANE` adds an equality predicate on
//! the lane it names. The newest-rows window, which counts stored rows rather than samples, is
//! refused with a message that says so. `REMOVE` and `DELETE` are the one removal command
//! ([`Command::WindowReduceRemove`]); see [`remove`].
//! `MEMBERS_ONLY` reads exactly the include set, ordered by time.
//!
//! The selector's `limit_members` is the row count of a stored-cell (`RECENT`) read that names no
//! `limit`; every other read ignores it. `skip_existing` on a write is ignored: an add merges.
//! `want_sample_info` asks the answer to say which samples the read used and how many cells each
//! held (`SampleInfo`, oldest sample first).

use super::window_reduce_fan_in;
use super::window_reduce_rank;
use super::window_reduce_rank_options::MAX_QUOTA_LANES;
use super::cell_set::{
    self, within, within_nonzero, Cell, CellHeader, CellKey, DimSpec, DimType, DimValue, EntityType,
    EntityValue, KeySpec, LaneSpec, Member, HEADER_VERSION, MAX_CELLS_PER_REQUEST, MAX_KEY_DIMENSIONS,
    MAX_LANES_PER_CELL, MAX_NAME_BYTES, MAX_TOP_K,
};
use crate::types::{
    WindowReduceAnchor, WindowReduceLaneQuota, CellMemberFilter, CellMemberId, WindowReduceQuota, WindowReduceRankBatchItem, WindowReduceRankSpec,
    WindowReduceSampleInfo, WindowReduceScore, WindowReduceWindow, Command, FeatureFilter, FeatureFilterOp, FeaturePoint,
    FeatureWritePolicy, RankedCells,
};

pub(crate) mod wire {
    #![allow(clippy::all, missing_docs)]
    tonic::include_proto!("temporalstore.windowreduce.v1");
}
use wire::WindowReduceStatusCode;

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
    pub(crate) code: WindowReduceStatusCode,
    pub(crate) message: String,
}

fn invalid(message: impl Into<String>) -> Refusal {
    Refusal { code: WindowReduceStatusCode::InvalidArgument, message: message.into() }
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
    /// A second cell for a member already in the sample is refused, not merged.
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
                code: WindowReduceStatusCode::NoData,
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

/// One request's observations as feature appends: one command per distinct key, each holding one
/// point per distinct millisecond. `now_us` is the clock the accepted range is measured from.
pub(crate) fn translate_add(
    profile: &TableProfile,
    request: &wire::WindowReduceAddRequest,
    now_us: i64,
) -> Result<Vec<Command>, Refusal> {
    // `skip_existing` is carried on the wire and means nothing to a write: an add always merges.
    // A retried add is made safe by the retry token.
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
    // A zero token is the wire's way of carrying none.
    let retry_token = (request.retry_token != 0).then_some(request.retry_token);
    translate_observations(profile, &observations, now_us, retry_token)
}

/// Pre-aggregated cells written at their latest timestamp; the earliest must not follow it.
pub(crate) fn translate_restore(
    profile: &TableProfile,
    request: &wire::WindowReduceRestoreRequest,
    now_us: i64,
) -> Result<Vec<Command>, Refusal> {
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
    translate_observations(profile, &observations, now_us, None)
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
    retry_token: Option<u64>,
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
                    code: WindowReduceStatusCode::DuplicateRefused,
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
                // The same token rides on every key of the request: each key remembers it
                // separately, so a retried request is recognised key by key.
                retry_token,
            }),
        }
    }
    Ok(commands)
}

/// The response to a write, from what the engine's receipts add up to. `replayed` is true only when
/// every key of the request answered a remembered token, so nothing at all was written.
pub(crate) fn add_response(
    samples_written: u64,
    cells_created: u64,
    cells_merged: u64,
    replayed: bool,
) -> wire::WindowReduceAddResponse {
    wire::WindowReduceAddResponse {
        code: WindowReduceStatusCode::Ok as i32,
        message: String::new(),
        samples_written,
        cells_created,
        cells_merged,
        replayed,
    }
}

pub(crate) fn refused_add_response(refusal: &Refusal) -> wire::WindowReduceAddResponse {
    wire::WindowReduceAddResponse {
        code: refusal.code as i32,
        message: refusal.message.clone(),
        ..Default::default()
    }
}

// ------------------------------------------------------------------------------------------
// Reads
// ------------------------------------------------------------------------------------------

/// A query as the engine reads it: the key, the keys read together with it (empty for a one-key
/// read) and the ranked spec.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ReadPlan {
    pub(crate) key: String,
    /// The keys of `more_entities`, in request order. Non-empty makes the read a merged one.
    pub(crate) more_keys: Vec<String>,
    pub(crate) spec: WindowReduceRankSpec,
}

impl ReadPlan {
    /// The command that runs this read: a one-key ranked read, or one merged read over every key.
    pub(crate) fn command(self) -> Command {
        if self.more_keys.is_empty() {
            return Command::WindowReduceRank { key: self.key, spec: self.spec };
        }
        let mut keys = Vec::with_capacity(1 + self.more_keys.len());
        keys.push(self.key);
        keys.extend(self.more_keys);
        Command::WindowReduceFanInRank { keys, spec: self.spec }
    }
}

fn lane_name(profile: &TableProfile, at: i32, what: &str) -> Result<String, Refusal> {
    usize::try_from(at)
        .ok()
        .and_then(|at| profile.lanes.get(at))
        .map(|lane| lane.name.clone())
        .ok_or_else(|| invalid(format!("{what} {at} is not a lane of this table")))
}

/// `closed_end`: the window of a read that selects cells by member or by lane value (`MEMBERS_ONLY`,
/// `MATCH_LANE`) holds the stamp at its very end, `[start, start + span]`; every other read's
/// `FROM_START` window stops before it, `[start, start + span)`.
fn window_of(window: &wire::Window, closed_end: bool) -> Result<WindowReduceWindow, Refusal> {
    if window.span < 0 {
        return Err(invalid(format!("window span {} is negative", window.span)));
    }
    match window.kind() {
        // The two newest-anchored kinds are the same span; they differ from the first only in the
        // anchor, which `anchor_of` reads.
        wire::WindowKind::TrailingFromNow
        | wire::WindowKind::TrailingFromNewest
        | wire::WindowKind::TrailingFromNewestOfKind => {
            Ok(WindowReduceWindow::Last { duration_ms: (window.span / 1000) as u64 })
        }
        wire::WindowKind::FromStart => {
            if window.span == 0 && !closed_end {
                return Err(invalid("a window of span 0 holds nothing"));
            }
            let start = window.start_us;
            let end = start.saturating_add(window.span);
            // Samples stamped t ms hold t * 1000 microseconds: [start, end) in whole ms, or
            // [start, end] when the end is closed (the first ms after it is one past the end).
            let first = start.div_euclid(1000) + i64::from(start.rem_euclid(1000) != 0);
            let after = if closed_end {
                end.div_euclid(1000) + 1
            } else {
                end.div_euclid(1000) + i64::from(end.rem_euclid(1000) != 0)
            };
            if first < 0 || after <= first {
                return Err(invalid("window holds no whole millisecond"));
            }
            Ok(WindowReduceWindow::Between { start_ms: first as u64, end_ms: (after - 1) as u64 })
        }
        // `span` is a count of rows here, not a length of time. A window of no rows holds nothing;
        // the bound on the count is the engine's, applied by the admission every plan passes.
        wire::WindowKind::NewestRows => {
            if window.span == 0 {
                return Err(invalid("a window of the newest 0 rows holds nothing"));
            }
            let rows = usize::try_from(window.span)
                .map_err(|_| invalid(format!("window of the newest {} rows is too many", window.span)))?;
            Ok(WindowReduceWindow::NewestRows { n: rows })
        }
    }
}

/// Which instant a trailing window ends at: the newest stored sample for the two newest-anchored
/// kinds, the request clock for the rest.
fn anchor_of(window: &wire::Window) -> WindowReduceAnchor {
    match window.kind() {
        wire::WindowKind::TrailingFromNewest | wire::WindowKind::TrailingFromNewestOfKind => WindowReduceAnchor::Newest,
        _ => WindowReduceAnchor::Request,
    }
}

/// The half-life a decayed-lane rank asks for: `decay`, made a whole number of milliseconds by
/// `decay_rounding`. The engine's own bounds on a half-life are applied by admission afterwards.
fn half_life_of(selector: &wire::WindowReduceSelector) -> Result<u64, Refusal> {
    let decay = selector.decay;
    if !decay.is_finite() || decay < 0.0 {
        return Err(invalid(format!("decay {decay} is not a non-negative number of milliseconds")));
    }
    let whole = match selector.decay_rounding() {
        wire::Rounding::Floor => decay.floor(),
        wire::Rounding::Ceil => decay.ceil(),
        wire::Rounding::Round => decay.round(),
    };
    // 9e18 is far past any half-life the engine accepts, and keeps the cast exact.
    if whole >= 9.0e18 {
        return Err(invalid(format!("decay {decay} is too large to be a number of milliseconds")));
    }
    Ok(whole as u64)
}

/// The member set a selector names. `members` is the include set: present when flagged or when it
/// holds anything, so a present, empty set reaches admission and is refused there.
fn member_filter_of(selector: &wire::WindowReduceSelector) -> CellMemberFilter {
    let id = |member: &i64| CellMemberId::U64(*member as u64);
    CellMemberFilter {
        include: (selector.has_members || !selector.members.is_empty())
            .then(|| selector.members.iter().map(id).collect()),
        exclude: selector.excluded_members.iter().map(id).collect(),
    }
}

/// The quota a selector names: `quota_lanes[i]` (a lane position) with `quota_counts[i]`, and
/// `quota_counts[0]` as the count of members that fit no lane. The first count therefore appears
/// TWICE in what the quota reserves (see docs/WINDOW_REDUCE_PROTOCOL.md, Quota). A request that names only
/// the count of members that fit no lane sends one count and no lane.
fn quota_of(profile: &TableProfile, selector: &wire::WindowReduceSelector) -> Result<WindowReduceQuota, Refusal> {
    let (lanes, counts) = (&selector.quota_lanes, &selector.quota_counts);
    if lanes.is_empty() && counts.is_empty() {
        return Ok(WindowReduceQuota::default());
    }
    bound("lanes in a quota", lanes.len(), MAX_QUOTA_LANES)?;
    if counts.len() != lanes.len() && !(lanes.is_empty() && counts.len() == 1) {
        return Err(invalid(format!(
            "a quota names {} lanes and {} counts: one count for each lane",
            lanes.len(),
            counts.len()
        )));
    }
    let count_of = |count: &i32| {
        usize::try_from(*count).map_err(|_| invalid(format!("quota count {count} is negative")))
    };
    let others = count_of(&counts[0])?;
    let mut entries = Vec::with_capacity(lanes.len());
    for (lane, count) in lanes.iter().zip(counts) {
        entries.push(WindowReduceLaneQuota { lane: lane_name(profile, *lane, "quota lane")?, count: count_of(count)? });
    }
    Ok(WindowReduceQuota { lanes: entries, others })
}

fn score_of(profile: &TableProfile, selector: &wire::WindowReduceSelector) -> Result<WindowReduceScore, Refusal> {
    use wire::RankMode as Mode;
    Ok(match selector.mode() {
        Mode::FirstLane => WindowReduceScore::Lane { lane: lane_name(profile, 0, "lane")? },
        Mode::SecondLane => WindowReduceScore::Lane { lane: lane_name(profile, 1, "lane")? },
        Mode::Ratio => WindowReduceScore::Ratio {
            numerator: lane_name(profile, 0, "lane")?,
            denominator: lane_name(profile, 1, "lane")?,
        },
        Mode::Member => WindowReduceScore::Member,
        Mode::NamedLane => {
            let at = if selector.has_sort_lane { selector.sort_lane } else { 0 };
            if at < 0 {
                return Err(invalid(format!("sort lane {at} is negative")));
            }
            // A list table answers a lane past its last as a value of 0 for every member, so
            // the members come back in member order; a pair table has two lanes and no more.
            if profile.shape == Shape::List && usize::try_from(at).is_ok_and(|at| at >= profile.lanes.len()) {
                WindowReduceScore::Weighted { lanes: vec![lane_name(profile, 0, "lane")?], weights: vec![0.0] }
            } else {
                WindowReduceScore::Lane { lane: lane_name(profile, at, "sort lane")? }
            }
        }
        Mode::Wilson => {
            let z = if selector.has_wilson_z && selector.wilson_z > 0.0 { selector.wilson_z } else { DEFAULT_WILSON_Z };
            WindowReduceScore::Wilson {
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
            WindowReduceScore::Weighted { lanes, weights: selector.weights.iter().map(|w| f64::from(*w)).collect() }
        }
        Mode::DecayedLane => {
            let at = if selector.has_sort_lane { selector.sort_lane } else { 0 };
            WindowReduceScore::Decay { lane: lane_name(profile, at, "sort lane")?, half_life_ms: half_life_of(selector)? }
        }
        // A members-only read selects members rather than ranking them, so it comes back newest
        // first: the stamp of each member's newest sample in the window, equal stamps by member.
        Mode::MembersOnly => WindowReduceScore::Time,
        // These two answer the stored cells newest first, one row per cell and NOT reduced per
        // member; `match_filter_of` adds the lane test that makes the second one a match.
        Mode::Recent | Mode::MatchLane => WindowReduceScore::Recent,
    })
}

/// A test no cell passes: a member is never below 0.
fn matches_nothing() -> FeatureFilter {
    FeatureFilter { field: "member".to_string(), op: FeatureFilterOp::LessThan, value: 0 }
}

/// The lane test of a `MATCH_LANE` read: the stored cells whose lane at `match_lane` (the first
/// when absent) holds `match_value` (0 when absent, and any signed number: a lane is signed, and
/// the filter carries the value as the 64 bits it was sent in). A lane the table does not declare
/// holds nothing, so no cell matches and the read answers nothing.
fn match_filter_of(profile: &TableProfile, selector: &wire::WindowReduceSelector) -> Result<Option<FeatureFilter>, Refusal> {
    if selector.mode() != wire::RankMode::MatchLane {
        return Ok(None);
    }
    let at = if selector.has_match_lane { selector.match_lane } else { 0 };
    let value = if selector.has_match_value { selector.match_value } else { 0 };
    let lane = usize::try_from(at).ok().and_then(|at| profile.lanes.get(at));
    Ok(Some(match lane {
        Some(lane) => FeatureFilter { field: lane.name.clone(), op: FeatureFilterOp::Equal, value: value as u64 },
        None => matches_nothing(),
    }))
}

/// One wire query as the engine's ranked read.
pub(crate) fn translate_query(profile: &TableProfile, query: &wire::WindowReduceQuery) -> Result<ReadPlan, Refusal> {
    bound("more entities", query.more_entities.len(), MAX_MORE_ENTITIES)?;
    if let Some(selector) = query.selector.as_ref() {
        bound("members", selector.members.len(), MAX_MEMBER_LIST)?;
        bound("excluded members", selector.excluded_members.len(), MAX_MEMBER_LIST)?;
    }
    let window = query.window.as_ref().ok_or_else(|| invalid("a query needs a window"))?;
    let selector = query.selector.as_ref().ok_or_else(|| invalid("a query needs a selector"))?;
    let entity = entity_of(profile, query.entity, &query.entity_text, &query.entity_bytes)?;
    let key = key_of(profile, entity, selector.channel, selector.kind)?;
    let mut more_keys = Vec::with_capacity(query.more_entities.len());
    if !query.more_entities.is_empty() && profile.entity != EntityType::U64 {
        return Err(invalid("more entities are 64 bit ids, and this table's entity is not one"));
    }
    for more in &query.more_entities {
        more_keys.push(key_of(profile, EntityValue::U64(*more as u64), selector.channel, selector.kind)?);
    }
    if !more_keys.is_empty() {
        // How many keys, and none twice: the merged read counts a key as often as it is named.
        let named: Vec<String> = std::iter::once(key.clone()).chain(more_keys.iter().cloned()).collect();
        window_reduce_fan_in::validate_keys(&named).map_err(invalid)?;
    }
    let anchor = anchor_of(window);
    // A quota on stored cells is the walk of newest rows with a closed end, as the other cell selections.
    let closed_end = matches!(selector.mode(), wire::RankMode::MembersOnly | wire::RankMode::MatchLane)
        || (selector.mode() == wire::RankMode::Recent && !selector.quota_lanes.is_empty());
    let window = window_of(window, closed_end)?;
    let score = score_of(profile, selector)?;
    let quota = quota_of(profile, selector)?;
    let members = member_filter_of(selector);
    let limit = selector.limit;
    let top_k = if selector.mode() == wire::RankMode::MembersOnly {
        // A members-only read answers exactly the members it names, so its limit is ignored: the
        // read asks for as many as the include set holds.
        match members.include.as_ref().filter(|set| !set.is_empty()) {
            Some(set) => {
                within_nonzero("members in a members-only read", set.len(), MAX_TOP_K).map_err(invalid)?;
                set.len()
            }
            None => return Err(invalid("a members-only read needs the members to read")),
        }
    } else {
        // A stored-cell read (`RECENT`) with no `limit` takes the member count `limit_members`
        // names as its row count; in every other case `limit_members` asks nothing of the read.
        let limit = if selector.mode() == wire::RankMode::Recent
            && limit <= 0
            && selector.has_limit_members
            && selector.limit_members > 0
        {
            selector.limit_members
        } else {
            limit
        };
        let top_k = usize::try_from(limit)
            .ok()
            .filter(|k| *k > 0)
            .ok_or_else(|| invalid(format!("limit {limit} must be positive")))?;
        within_nonzero("limit", top_k, MAX_TOP_K).map_err(invalid)?;
        top_k
    };
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
    filters.extend(match_filter_of(profile, selector)?);
    // A false `want_sample_info` asks for nothing, with or without its presence flag.
    let sample_info = selector.want_sample_info;
    let spec = WindowReduceRankSpec {
        window,
        filters,
        score,
        top_k,
        project: Vec::new(),
        members,
        anchor,
        quota,
        sample_info,
    };
    // The engine's own admission: what is accepted here is accepted by the command.
    window_reduce_rank::validate_spec(&spec).map_err(invalid)?;
    Ok(ReadPlan { key, more_keys, spec })
}

/// A batch as its independent reads, in request order. A query that cannot be translated refuses
/// the whole batch, naming its position.
pub(crate) fn translate_batch(
    profiles: &Profiles,
    batch: &wire::WindowReduceBatchQuery,
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

/// A batch's plans as one batch command. A merged read (a query with `more_entities`) has no place in
/// a batch, whose reads are independent and answer one each: it is refused, naming its position. The
/// keys of a batch are read on the shard the request names, so they must live together.
pub(crate) fn batch_command(plans: Vec<ReadPlan>) -> Result<Command, Refusal> {
    let mut queries = Vec::with_capacity(plans.len());
    for (at, plan) in plans.into_iter().enumerate() {
        if !plan.more_keys.is_empty() {
            return Err(invalid(format!(
                "query {at}: a read over several entities cannot ride in a batch, send it on its own"
            )));
        }
        queries.push(WindowReduceRankBatchItem { key: plan.key, spec: plan.spec });
    }
    window_reduce_fan_in::validate_batch(&queries).map_err(invalid)?;
    Ok(Command::WindowReduceRankBatch { queries })
}

/// The answers of a batch, in request order, one per query, each with its own status.
pub(crate) fn batch_answer(
    profiles: &Profiles,
    batch: &wire::WindowReduceBatchQuery,
    rankings: &[RankedCells],
) -> wire::WindowReduceBatchAnswer {
    if rankings.len() != batch.queries.len() {
        return wire::WindowReduceBatchAnswer {
            code: WindowReduceStatusCode::Error as i32,
            message: format!("{} queries were answered with {} rankings", batch.queries.len(), rankings.len()),
            ..Default::default()
        };
    }
    let answers = batch
        .queries
        .iter()
        .zip(rankings)
        .map(|(query, ranked)| match profiles.lookup(&query.table) {
            Ok(profile) => answer_of(profile, query, ranked),
            Err(refusal) => wire::WindowReduceAnswer {
                entity: query.entity,
                code: refusal.code as i32,
                message: refusal.message,
                ..Default::default()
            },
        })
        .collect();
    wire::WindowReduceBatchAnswer { answers, code: WindowReduceStatusCode::Ok as i32, message: String::new() }
}

/// The engine's ranked answer as a wire answer.
pub(crate) fn answer_of(profile: &TableProfile, query: &wire::WindowReduceQuery, ranked: &RankedCells) -> wire::WindowReduceAnswer {
    if !ranked.refused.is_empty() {
        return wire::WindowReduceAnswer {
            entity: query.entity,
            code: WindowReduceStatusCode::InvalidArgument as i32,
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
    // The span the read scanned: the oldest and the newest sample of the window, before the top-k
    // kept any of it. A `MEMBERS_ONLY` read reports the oldest and the newest sample that held one of
    // its members, and a `MATCH_LANE` read the span of the rows it returned: only those cells were
    // looked for.
    let by_rows = matches!(query.selector.as_ref().map(|s| s.mode()), Some(wire::RankMode::MatchLane));
    let matched = matches!(query.selector.as_ref().map(|s| s.mode()), Some(wire::RankMode::MembersOnly));
    let (first_us, last_us) = if matched {
        if ranked.rows.is_empty() {
            (0, 0)
        } else {
            (
                (ranked.matched_first_ms as i64).saturating_mul(1000),
                (ranked.matched_last_ms as i64).saturating_mul(1000),
            )
        }
    } else if by_rows {
        (
            if entries.is_empty() { 0 } else { (first as i64).saturating_mul(1000) },
            (last as i64).saturating_mul(1000),
        )
    } else if ranked.samples == 0 {
        (0, 0)
    } else {
        (
            (ranked.window_first_ms as i64).saturating_mul(1000),
            (ranked.window_last_ms as i64).saturating_mul(1000),
        )
    };
    wire::WindowReduceAnswer {
        entity: query.entity,
        first_us,
        last_us,
        entries,
        sample_info: sample_info_of(&ranked.sample_info),
        echoed_selector: echoed.then(|| query.selector.clone()).flatten(),
        echoed_window: echoed.then(|| query.window.clone()).flatten(),
        code: WindowReduceStatusCode::Ok as i32,
        message: String::new(),
        samples: ranked.samples as u64,
        unscored: ranked.unscored,
        keys: ranked.keys as u64,
    }
}

/// The per-sample summary of an answer: stamps in microseconds (a sample is one instant, so its
/// start and its end are the same) and the cells each held. `None` when the read did not ask, which
/// leaves the field out of the encoded answer.
fn sample_info_of(used: &[WindowReduceSampleInfo]) -> Option<wire::SampleInfo> {
    if used.is_empty() {
        return None;
    }
    let stamps: Vec<i64> = used.iter().map(|sample| (sample.timestamp_ms as i64).saturating_mul(1000)).collect();
    Some(wire::SampleInfo {
        starts_us: stamps.clone(),
        ends_us: stamps,
        member_counts: used.iter().map(|sample| sample.cells as i64).collect(),
    })
}

// `REMOVE` and `DELETE`, as the one command that removes cells.
#[path = "window_reduce_protocol_remove.rs"]
pub(crate) mod remove;

#[cfg(test)]
#[path = "window_reduce_protocol_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "window_reduce_protocol_wire_tests.rs"]
mod wire_tests;
