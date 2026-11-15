// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The one resolver of a window over a series of samples.
//!
//! A series holds one sample per instant. A read names the samples it wants with a [`CellWindow`]:
//! an inclusive range of instants, the last stretch of time before a clock reading, everything from
//! an instant on, or the newest `n`. [`resolve_window`] applies the clock and the anchor and says
//! what that asks of the series ([`WindowSpec`]); [`window_samples`] walks the series once and
//! returns the samples it names, newest first.
//!
//! # WHAT IS SHARED AND WHAT IS NOT
//!
//! The resolver knows instants and counts only. It does not know what a sample holds, so a ranked
//! read, a rollup and a sequence read can all name their samples through it. What each caller does
//! with a window that is too big (refuse it, truncate it) is the caller's: the walk takes a `cap`
//! for the ranges of instants and stops there, and a caller that wants to refuse an over-long window
//! passes one more than its limit and checks the length.
//!
//! The `n` of a newest-`n` window is not capped here: a request is bounded at admission, before a
//! read, and that bound belongs to the request.
//!
//! The boundaries of a `Last` window are computed by `cell_rank_options::last_bounds`, the function
//! the pure ranked read already uses, so a read over decoded samples and a read over a stored series
//! agree on what an anchor at the newest instant means.

use super::cell_rank_options::last_bounds;
use super::timestamped_series::TimestampedSeries;
use crate::block_store::ElementEntry;
use crate::types::{CellAnchor, CellWindow};

/// What a window asks of a series, once the clock and the anchor are applied.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum WindowSpec {
    /// The samples stamped in `[from_ms, to_ms]`. Reversed ends name no sample.
    Stamps { from_ms: u64, to_ms: u64 },
    /// The `n` newest samples.
    Newest { n: usize },
    /// No sample: the window holds no instant.
    Nothing,
}

/// Applies the clock and the anchor to `window`. `newest_ms` is asked for only by a `Last` window
/// anchored at the newest sample, and answers the newest instant the series holds (`None`: it holds
/// no sample).
pub(crate) fn resolve_window(
    window: &CellWindow,
    anchor: CellAnchor,
    now_ms: u64,
    newest_ms: impl FnOnce() -> Option<u64>,
) -> WindowSpec {
    match window {
        CellWindow::Between { start_ms, end_ms } => WindowSpec::Stamps { from_ms: *start_ms, to_ms: *end_ms },
        CellWindow::Last { duration_ms } => {
            let newest = match anchor {
                CellAnchor::Request => None,
                CellAnchor::Newest => newest_ms(),
            };
            match last_bounds(anchor, *duration_ms, now_ms, newest) {
                Some((from_ms, to_ms)) => WindowSpec::Stamps { from_ms, to_ms },
                None => WindowSpec::Nothing,
            }
        }
        CellWindow::Since { start_ms } => WindowSpec::Stamps { from_ms: *start_ms, to_ms: u64::MAX },
        CellWindow::LastN { n } => WindowSpec::Newest { n: *n },
    }
}

/// The samples a resolved window names, newest first. A range of instants yields at most `cap`
/// samples; a newest-`n` window yields at most `n`.
pub(crate) fn samples_of<'s, S: TimestampedSeries>(
    series: &'s S,
    spec: WindowSpec,
    cap: usize,
) -> Vec<(u64, &'s ElementEntry)> {
    match spec {
        WindowSpec::Stamps { from_ms, to_ms } => series
            .range(crate::engine::timestamp_range_bounds(from_ms, to_ms))
            .rev()
            .take(cap)
            .collect(),
        WindowSpec::Newest { n } => series.range(..).rev().take(n).collect(),
        WindowSpec::Nothing => Vec::new(),
    }
}

/// [`resolve_window`] then [`samples_of`]: the samples `window` names in `series`, newest first.
pub(crate) fn window_samples<'s, S: TimestampedSeries>(
    series: &'s S,
    window: &CellWindow,
    anchor: CellAnchor,
    now_ms: u64,
    cap: usize,
) -> Vec<(u64, &'s ElementEntry)> {
    let spec = resolve_window(window, anchor, now_ms, || series.range(..).next_back().map(|(at, _)| at));
    samples_of(series, spec, cap)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn xorshift(state: &mut u64) -> u64 {
        *state ^= *state << 13;
        *state ^= *state >> 7;
        *state ^= *state << 17;
        *state
    }

    /// The walk this module replaced, kept here as the statement of what it answers: a window over
    /// a plain ordered map, each kind spelled out on its own.
    fn the_walk_this_replaced(
        series: &BTreeMap<u64, ElementEntry>,
        window: &CellWindow,
        anchor: CellAnchor,
        now_ms: u64,
        cap: usize,
    ) -> Vec<u64> {
        let bounded = |from_ms: u64, to_ms: u64| -> Vec<u64> {
            if from_ms > to_ms {
                return Vec::new();
            }
            series.range(from_ms..=to_ms).rev().take(cap).map(|(at, _)| *at).collect()
        };
        match window {
            CellWindow::Between { start_ms, end_ms } => bounded(*start_ms, *end_ms),
            CellWindow::Last { duration_ms } => match anchor {
                CellAnchor::Request => bounded(now_ms.saturating_sub(*duration_ms), now_ms),
                CellAnchor::Newest => match series.keys().next_back() {
                    Some(&newest) if *duration_ms > 0 => {
                        let start = (u128::from(newest) + 1).saturating_sub(u128::from(*duration_ms)) as u64;
                        bounded(start, newest)
                    }
                    _ => Vec::new(),
                },
            },
            CellWindow::Since { start_ms } => bounded(*start_ms, u64::MAX),
            CellWindow::LastN { n } => series.keys().rev().take(*n).copied().collect(),
        }
    }

    fn series_of(stamps: &[u64]) -> BTreeMap<u64, ElementEntry> {
        stamps.iter().map(|&at| (at, ElementEntry::default())).collect()
    }

    fn stamps_of(samples: Vec<(u64, &ElementEntry)>) -> Vec<u64> {
        samples.into_iter().map(|(at, _)| at).collect()
    }

    #[test]
    fn every_window_kind_names_the_samples_the_walk_it_replaced_named() {
        let mut state = 0x9e37_79b9_7f4a_7c15u64;
        let mut compared = 0usize;
        let mut non_empty = 0usize;
        for round in 0..400 {
            let count = (xorshift(&mut state) % 40) as usize;
            let stamps: Vec<u64> = (0..count).map(|_| xorshift(&mut state) % 200).collect();
            let series = series_of(&stamps);
            for _ in 0..30 {
                let a = xorshift(&mut state) % 230;
                let b = xorshift(&mut state) % 230;
                let n = (xorshift(&mut state) % 12) as usize;
                let cap = 1 + (xorshift(&mut state) % 9) as usize;
                let now_ms = xorshift(&mut state) % 230;
                let duration_ms = xorshift(&mut state) % 60;
                let windows = [
                    CellWindow::Between { start_ms: a, end_ms: b },
                    CellWindow::Last { duration_ms },
                    CellWindow::Since { start_ms: a },
                    CellWindow::LastN { n },
                    CellWindow::Between { start_ms: 0, end_ms: u64::MAX },
                    CellWindow::Last { duration_ms: u64::MAX },
                ];
                for window in &windows {
                    for anchor in [CellAnchor::Request, CellAnchor::Newest] {
                        let expected = the_walk_this_replaced(&series, window, anchor, now_ms, cap);
                        let got = stamps_of(window_samples(&series, window, anchor, now_ms, cap));
                        assert_eq!(got, expected, "round {round}: {window:?} {anchor:?} now {now_ms} cap {cap}");
                        compared += 1;
                        non_empty += usize::from(!expected.is_empty());
                    }
                }
            }
        }
        assert!(compared > 100_000 && non_empty > 20_000, "compared {compared}, non-empty {non_empty}");
    }

    #[test]
    fn a_window_is_resolved_against_the_clock_and_the_anchor_it_names() {
        let never = || -> Option<u64> { panic!("only an anchor at the newest sample asks for it") };
        assert_eq!(
            resolve_window(&CellWindow::Last { duration_ms: 40 }, CellAnchor::Request, 100, never),
            WindowSpec::Stamps { from_ms: 60, to_ms: 100 }
        );
        assert_eq!(
            resolve_window(&CellWindow::Last { duration_ms: 500 }, CellAnchor::Request, 100, never),
            WindowSpec::Stamps { from_ms: 0, to_ms: 100 }
        );
        assert_eq!(
            resolve_window(&CellWindow::Last { duration_ms: 40 }, CellAnchor::Newest, 100, || Some(70)),
            WindowSpec::Stamps { from_ms: 31, to_ms: 70 }
        );
        assert_eq!(
            resolve_window(&CellWindow::Last { duration_ms: 40 }, CellAnchor::Newest, 100, || None),
            WindowSpec::Nothing
        );
        assert_eq!(
            resolve_window(&CellWindow::Last { duration_ms: 0 }, CellAnchor::Newest, 100, || Some(70)),
            WindowSpec::Nothing
        );
        assert_eq!(
            resolve_window(&CellWindow::Since { start_ms: 9 }, CellAnchor::Request, 100, never),
            WindowSpec::Stamps { from_ms: 9, to_ms: u64::MAX }
        );
        assert_eq!(
            resolve_window(&CellWindow::LastN { n: 3 }, CellAnchor::Request, 100, never),
            WindowSpec::Newest { n: 3 }
        );
        assert_eq!(
            resolve_window(&CellWindow::Between { start_ms: 5, end_ms: 6 }, CellAnchor::Request, 100, never),
            WindowSpec::Stamps { from_ms: 5, to_ms: 6 }
        );
    }

    #[test]
    fn a_range_stops_at_its_cap_newest_first_and_a_newest_n_is_not_capped_by_it() {
        let series = series_of(&[10, 20, 30, 40, 50]);
        let all = CellWindow::Between { start_ms: 0, end_ms: u64::MAX };
        assert_eq!(stamps_of(window_samples(&series, &all, CellAnchor::Request, 0, 3)), vec![50, 40, 30]);
        // Exactly the cap holds all of them, and one more than the series holds is not padded.
        assert_eq!(stamps_of(window_samples(&series, &all, CellAnchor::Request, 0, 5)), vec![50, 40, 30, 20, 10]);
        assert_eq!(stamps_of(window_samples(&series, &all, CellAnchor::Request, 0, 6)), vec![50, 40, 30, 20, 10]);
        let newest = CellWindow::LastN { n: 4 };
        assert_eq!(stamps_of(window_samples(&series, &newest, CellAnchor::Request, 0, 1)), vec![50, 40, 30, 20]);
        // Reversed ends name nothing and do not panic.
        let reversed = CellWindow::Between { start_ms: 40, end_ms: 10 };
        assert!(window_samples(&series, &reversed, CellAnchor::Request, 0, 9).is_empty());
        // An empty series answers nothing for every kind.
        let empty = series_of(&[]);
        for window in [all, newest, CellWindow::Last { duration_ms: 5 }, CellWindow::Since { start_ms: 0 }] {
            for anchor in [CellAnchor::Request, CellAnchor::Newest] {
                assert!(window_samples(&empty, &window, anchor, 7, 9).is_empty());
            }
        }
    }
}
