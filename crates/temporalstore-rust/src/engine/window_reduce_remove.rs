// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Removing cells from the samples of one key: a pure function over decoded samples. It reads
//! no engine state; [`super::window_reduce_remove_apply`] reads the stored samples, hands them here and
//! writes what comes back.
//!
//! # WHICH CELLS GO
//!
//! A cell is removed when ALL of these hold, and each part that is left open holds for every cell:
//!
//! * its sample is stamped in `[start_ms, end_ms]` (an absent bound is open on that side);
//! * its member passes the member set (`include` names the members to take, `exclude` the members
//!   to spare, a member in both is spared; the sets are the ranked read's own and are sorted and
//!   de-duplicated, so the order and repetition of a request change nothing);
//! * every named predicate over a label, a lane or `member` holds on the cell as its sample
//!   stores it (the ranked read's own predicates, compiled by the same code).
//!
//! A request in which all three parts are open would empty the key and is refused: deleting the
//! key is the command for that, and a removal that lost its arguments must not look like one.
//!
//! # WHAT COMES BACK
//!
//! Per sample in the window, the cells that stay (in their stored order). A sample that keeps
//! every cell is reported unchanged and its stored bytes are never rewritten; one that keeps none
//! is reported empty, and the engine drops it, since a sample holds at least one cell. Nothing
//! here depends on a clock, so a follower applying the committed request and a replay reach the
//! bytes the leader planned.
//!
//! # BOUNDS
//!
//! Each is refused one past the limit and accepted at it: [`MAX_REMOVE_SAMPLES`] samples
//! inside the window, [`MAX_REMOVE_PREDICATES`] predicates, and the member sets' own 1024 entries a
//! side. The window is counted from the series' address map before a block is read.

use super::window_reduce_rank::{compile_predicates, Predicate};
use super::window_reduce_rank_options::CompiledMembers;
use super::cell_set::{within, Cell, CellHeader, CellRef};
use crate::types::WindowReduceRemoveSpec;

/// Samples a removal may read: the ones stamped inside its window.
pub(crate) const MAX_REMOVE_SAMPLES: usize = 1024;
/// Predicates a removal may carry.
pub(crate) const MAX_REMOVE_PREDICATES: usize = 8;

/// The checks that need no header: bounds, ranges and the rule that something must restrict. A
/// command runs this at admission before any sample is read; [`Selection::compile`] runs it
/// again so a selection can never be built from a spec that skipped it.
pub(crate) fn validate_spec(spec: &WindowReduceRemoveSpec) -> Result<(), String> {
    within("predicates per removal", spec.filters.len(), MAX_REMOVE_PREDICATES)?;
    if let (Some(start), Some(end)) = (spec.start_ms, spec.end_ms) {
        if start > end {
            return Err(format!("window starts at {start}, after it ends at {end}"));
        }
    }
    // Compiling the member set is what bounds and checks it, and is the one place that does.
    let members = CompiledMembers::compile(&spec.members)?;
    if spec.start_ms.is_none() && spec.end_ms.is_none() && members.is_open() && spec.filters.is_empty() {
        return Err(
            "a removal names no window, member or predicate, so it would empty the key: \
             delete the key to do that"
                .to_string(),
        );
    }
    Ok(())
}

/// A spec resolved against a header.
pub(crate) struct Selection {
    from_ms: u64,
    to_ms: u64,
    members: CompiledMembers,
    predicates: Vec<Predicate>,
}

impl Selection {
    pub(crate) fn compile(header: &CellHeader, spec: &WindowReduceRemoveSpec) -> Result<Selection, String> {
        validate_spec(spec)?;
        Ok(Selection {
            from_ms: spec.start_ms.unwrap_or(0),
            to_ms: spec.end_ms.unwrap_or(u64::MAX),
            members: CompiledMembers::compile(&spec.members)?,
            predicates: compile_predicates(header, &spec.filters)?,
        })
    }

    /// The stamps `[from, to]` the window covers, inclusive.
    pub(crate) fn window(&self) -> (u64, u64) {
        (self.from_ms, self.to_ms)
    }

    /// Whether the cell is removed, given that its sample is inside the window.
    fn takes(&self, cell: &Cell) -> bool {
        let view = CellRef { member: cell.member.view(), labels: &cell.labels, lanes: &cell.lanes };
        self.members.allows(view.member) && self.predicates.iter().all(|predicate| predicate.holds(&view))
    }
}

/// What a removal did to one sample of the window.
#[derive(Debug, PartialEq)]
pub(crate) struct Outcome {
    pub(crate) timestamp_ms: u64,
    /// The cells that stay, in stored order. Empty: the sample goes.
    pub(crate) kept: Vec<Cell>,
    /// Cells taken out of this sample; zero means it is unchanged.
    pub(crate) removed: usize,
}

/// The removal of `selection` from `samples` (ascending by stamp, each cell list as stored). A
/// sample outside the window passes through untouched and is not reported. Returns one outcome
/// per sample INSIDE the window, in the order given.
pub(crate) fn remove_cells(selection: &Selection, samples: Vec<(u64, Vec<Cell>)>) -> Vec<Outcome> {
    let (from, to) = selection.window();
    samples
        .into_iter()
        .filter(|(timestamp_ms, _)| (from..=to).contains(timestamp_ms))
        .map(|(timestamp_ms, cells)| {
            let before = cells.len();
            let kept: Vec<Cell> = cells.into_iter().filter(|cell| !selection.takes(cell)).collect();
            Outcome { timestamp_ms, removed: before - kept.len(), kept }
        })
        .collect()
}

#[cfg(test)]
#[path = "window_reduce_remove_tests.rs"]
mod tests;
