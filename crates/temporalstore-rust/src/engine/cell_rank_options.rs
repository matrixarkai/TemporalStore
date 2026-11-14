// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Optional controls of the ranked cell read that are not part of its spec: which members it may
//! read, and which instant a `Last` window ends at. Both are pure data plus pure functions, so the
//! read can be proven against an independent fold without an engine.
//!
//! # MEMBER SET
//!
//! [`MemberFilter`] restricts a read to the members the caller names. `include` is a set the member
//! must belong to; `exclude` is a set it must not. Both are ANDed with the named predicates and
//! applied to a cell before that member's snapshots are reduced together.
//!
//! * `include: None` places no restriction. `include: Some([])` is REFUSED: a restriction to no
//!   member is almost always a caller that lost its list, and answering it with an empty read hides
//!   that.
//! * A member in both sets is excluded: the exclusion is the later, narrower statement.
//! * Each set holds at most [`MAX_FILTER_MEMBERS`] entries as sent, duplicates included, and a bytes
//!   member is at most [`MAX_MEMBER_BYTES`] long (a longer one could never match a stored member).
//!   Past either bound the request is refused, not truncated.
//! * The sets are sorted and de-duplicated when the plan is compiled, so the answer does not depend
//!   on the order or repetition of the request. A cell is tested by binary search.
//!
//! # ANCHOR
//!
//! A `Last { duration_ms }` window normally ends at the clock the request carries. With
//! [`Anchor::Newest`] it ends at the newest snapshot the key holds, so a key whose writes stopped
//! (replayed, lagged, paused) reads the same whenever it is read, and no clock enters the read.
//!
//! The newest stamp `n` is an INCLUSIVE end; the start is `(n + 1) - duration_ms`, floored at zero.
//! The window is therefore the half-open interval `[n + 1 - duration_ms, n + 1)`: it spans exactly
//! `duration_ms` stamps, and a duration of `0` holds nothing. (A `Last` window on the request clock
//! is `[now - duration_ms, now]`, one stamp wider, and a duration of `0` is the instant itself.)
//! Only a `Last` window has a clock to replace: an anchor on any other window is refused rather
//! than ignored. A key with no snapshot has no newest stamp and yields an empty window.

//! # QUOTA
//!
//! A [`Quota`] guarantees that the answer holds at least so many members with a non-zero value in
//! each of some lanes, where the best-ranked `top_k` alone might hold none. It is applied to the
//! ranked members, best first, after they are reduced and scored:
//!
//! * Every lane of the quota has a count. A member FITS a lane while that lane is still short and
//!   the member's reduced value of it is non-zero. A member that fits at least one short lane is
//!   kept, and counts toward every short lane it fits.
//! * A member that fits no short lane is kept while fewer than `others` such members have been
//!   kept.
//! * The walk stops as soon as every lane is satisfied (a quota with no lane: as soon as `others`
//!   are kept), even when `others` is not yet met. A lane no member can fill stays short and the
//!   walk ends with the members it was given.
//!
//! The walk covers the `top_k` best members and no others, so a quota never returns more than
//! `top_k` rows, and it only ever drops members from that list. A count is at least 1, a lane is
//! named once, and the counts plus `others` are bounded by [`MAX_QUOTA`]. A quota with no lane and
//! `others == 0` is no quota.

use super::cell_set::{within, Member, MAX_MEMBER_BYTES, MAX_TOP_K};
use crate::types::{CellMemberId, CellWindow};

/// Lanes one quota may name, and the sum of its counts plus its `others`.
pub(crate) const MAX_QUOTA_LANES: usize = 16;
/// The most members a quota may reserve: the same ceiling as `top_k`.
pub(crate) const MAX_QUOTA: usize = MAX_TOP_K;

/// Members one side (include or exclude) of a [`MemberFilter`] may name, as sent.
pub(crate) const MAX_FILTER_MEMBERS: usize = 1024;

/// The instant a `Last` window ends at.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum Anchor {
    /// The clock the request carries.
    #[default]
    Request,
    /// The newest snapshot of the key.
    Newest,
}

/// The members a read may use, as a request states them.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct MemberFilter {
    pub(crate) include: Option<Vec<CellMemberId>>,
    pub(crate) exclude: Vec<CellMemberId>,
}

/// The controls of one read beyond its spec. `Default` is a read exactly as it was before they
/// existed.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct RankOptions {
    pub(crate) members: MemberFilter,
    pub(crate) anchor: Anchor,
    pub(crate) quota: Quota,
}

/// One lane of a [`Quota`] and how many members must fit it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct LaneQuota {
    pub(crate) lane: String,
    pub(crate) count: usize,
}

/// See the module notes. `Default` asks for nothing.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Quota {
    pub(crate) lanes: Vec<LaneQuota>,
    pub(crate) others: usize,
}

impl Quota {
    pub(crate) fn is_none(&self) -> bool {
        self.lanes.is_empty() && self.others == 0
    }
}

fn as_member(id: &CellMemberId) -> Member {
    match id {
        CellMemberId::U64(v) => Member::U64(*v),
        CellMemberId::Bytes(b) => Member::Bytes(b.clone()),
    }
}

/// One side of the filter, sorted and de-duplicated.
fn compile_side(what: &str, side: &[CellMemberId]) -> Result<Vec<Member>, String> {
    within(what, side.len(), MAX_FILTER_MEMBERS)?;
    let mut members: Vec<Member> = side.iter().map(as_member).collect();
    for member in &members {
        if let Member::Bytes(bytes) = member {
            within("member bytes", bytes.len(), MAX_MEMBER_BYTES)?;
        }
    }
    members.sort();
    members.dedup();
    Ok(members)
}

/// A [`MemberFilter`] ready to test cells: sorted, de-duplicated, bounds met.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct CompiledMembers {
    include: Option<Vec<Member>>,
    exclude: Vec<Member>,
}

impl CompiledMembers {
    pub(crate) fn compile(filter: &MemberFilter) -> Result<CompiledMembers, String> {
        let include = match &filter.include {
            None => None,
            Some(side) if side.is_empty() => {
                return Err("an include set names no member: leave it out to read every member".to_string())
            }
            Some(side) => Some(compile_side("members in the include set", side)?),
        };
        Ok(CompiledMembers { include, exclude: compile_side("members in the exclude set", &filter.exclude)? })
    }

    /// Whether `member` may be read.
    pub(crate) fn allows(&self, member: &Member) -> bool {
        if let Some(include) = &self.include {
            if include.binary_search(member).is_err() {
                return false;
            }
        }
        self.exclude.is_empty() || self.exclude.binary_search(member).is_err()
    }

    /// Whether the filter restricts anything.
    pub(crate) fn is_open(&self) -> bool {
        self.include.is_none() && self.exclude.is_empty()
    }
}

/// The checks on a quota that need no header.
pub(crate) fn validate_quota(quota: &Quota) -> Result<(), String> {
    within("lanes in a quota", quota.lanes.len(), MAX_QUOTA_LANES)?;
    let mut reserved = quota.others;
    for (at, entry) in quota.lanes.iter().enumerate() {
        if entry.count == 0 {
            return Err(format!("the quota for lane `{}` asks for no member", entry.lane));
        }
        if quota.lanes[..at].iter().any(|earlier| earlier.lane == entry.lane) {
            return Err(format!("lane `{}` is in the quota twice", entry.lane));
        }
        reserved = reserved.saturating_add(entry.count);
    }
    within("members a quota reserves", reserved, MAX_QUOTA)
}

/// A [`Quota`] with its lanes resolved to positions.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CompiledQuota {
    /// `(lane position, count)`.
    lanes: Vec<(usize, usize)>,
    others: usize,
}

impl CompiledQuota {
    pub(crate) fn new(lanes: Vec<(usize, usize)>, others: usize) -> CompiledQuota {
        CompiledQuota { lanes, others }
    }

    /// Keeps the members of `ranked` the quota selects, in place and in order. `lanes_of` gives a
    /// member's reduced lanes. Allocates once, for the lanes still short.
    pub(crate) fn retain<T>(&self, ranked: &mut Vec<T>, lanes_of: impl Fn(&T) -> &[i64]) {
        let mut short: Vec<usize> = self.lanes.iter().map(|&(_, count)| count).collect();
        let mut others_left = self.others;
        let mut finished = false;
        ranked.retain(|member| {
            if finished {
                return false;
            }
            let lanes = lanes_of(member);
            let mut fits = false;
            for (left, &(lane, _)) in short.iter_mut().zip(&self.lanes) {
                if *left > 0 && lanes[lane] != 0 {
                    *left -= 1;
                    fits = true;
                }
            }
            let keep = fits || (others_left > 0 && {
                others_left -= 1;
                true
            });
            finished = if self.lanes.is_empty() { others_left == 0 } else { short.iter().all(|&left| left == 0) };
            keep
        });
    }
}

/// The checks on the options that need no header. A command runs this at admission.
pub(crate) fn validate_options(window: &CellWindow, options: &RankOptions) -> Result<(), String> {
    CompiledMembers::compile(&options.members)?;
    validate_quota(&options.quota)?;
    if options.anchor == Anchor::Newest && !matches!(window, CellWindow::Last { .. }) {
        return Err("an anchor at the newest snapshot applies to a `last` window only".to_string());
    }
    Ok(())
}

/// The inclusive stamp range a `Last` window covers, or `None` when it holds no stamp.
///
/// `newest_ms` is the newest stamp the key holds (`None`: no snapshot). With [`Anchor::Request`]
/// it is not read.
pub(crate) fn last_bounds(
    anchor: Anchor,
    duration_ms: u64,
    now_ms: u64,
    newest_ms: Option<u64>,
) -> Option<(u64, u64)> {
    match anchor {
        Anchor::Request => Some((now_ms.saturating_sub(duration_ms), now_ms)),
        Anchor::Newest => {
            let newest = newest_ms?;
            if duration_ms == 0 {
                return None;
            }
            // `newest + 1` is the exclusive end; computed wide so `u64::MAX` is exact. The result is
            // at most `newest`, so it fits.
            let start = (u128::from(newest) + 1).saturating_sub(u128::from(duration_ms));
            Some((start as u64, newest))
        }
    }
}
