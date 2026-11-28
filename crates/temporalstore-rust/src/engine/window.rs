// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The one resolver of a window over a series of samples.
//!
//! A series holds one sample per instant. A read names the samples it wants with a [`WindowReduceWindow`]:
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
//! The boundaries of a `Last` window are computed by `window_reduce_rank_options::last_bounds`, the function
//! the pure ranked read already uses, so a read over decoded samples and a read over a stored series
//! agree on what an anchor at the newest instant means.

use super::window_reduce_rank_options::last_bounds;
use super::timestamped_series::TimestampedSeries;
use crate::block_store::ElementEntry;
use crate::types::{WindowReduceAnchor, WindowReduceWindow};

/// What a window asks of a series, once the clock and the anchor are applied.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum WindowSpec {
    /// The samples stamped in `[from_ms, to_ms]`. Reversed ends name no sample.
    Stamps { from_ms: u64, to_ms: u64 },
    /// The `n` newest samples.
    Newest { n: usize },
    /// As many of the newest samples as hold `n` cells between them. Which samples that is is not
    /// known until their cells are counted, so the walk hands back the newest samples up to the
    /// caller's cap and the caller counts, with [`RowsWindow`].
    Rows,
    /// No sample: the window holds no instant.
    Nothing,
}

/// Counts the cells of samples visited newest first until a rows window holds the `want` it asked
/// for.
///
/// A window of rows is `want` stored (instant, member) cells, the newest ones: samples are taken
/// whole from the newest back, and the sample that crosses `want` gives up only the cells it needs.
/// Cells of one sample are ordered by member, so the ones it gives up are its largest members, the
/// newest in (instant, member) order, and the ones it leaves out are its first `skip`.
#[derive(Clone, Copy, Debug)]
pub(crate) struct RowsWindow {
    want: usize,
    held: usize,
}

impl RowsWindow {
    pub(crate) fn new(want: usize) -> Self {
        Self { want, held: 0 }
    }

    /// Takes the next older sample, which holds `cells` cells. Returns how many of its first cells
    /// fall outside the window, and whether the window is full now.
    pub(crate) fn take(&mut self, cells: usize) -> (usize, bool) {
        let room = self.want.saturating_sub(self.held);
        if cells >= room {
            self.held = self.want;
            (cells - room, true)
        } else {
            self.held += cells;
            (0, false)
        }
    }
}

/// Applies the clock and the anchor to `window`. `newest_ms` is asked for only by a `Last` window
/// anchored at the newest sample, and answers the newest instant the series holds (`None`: it holds
/// no sample).
pub(crate) fn resolve_window(
    window: &WindowReduceWindow,
    anchor: WindowReduceAnchor,
    now_ms: u64,
    newest_ms: impl FnOnce() -> Option<u64>,
) -> WindowSpec {
    match window {
        WindowReduceWindow::Between { start_ms, end_ms } => WindowSpec::Stamps { from_ms: *start_ms, to_ms: *end_ms },
        WindowReduceWindow::Last { duration_ms } => {
            let newest = match anchor {
                WindowReduceAnchor::Request => None,
                WindowReduceAnchor::Newest => newest_ms(),
            };
            match last_bounds(anchor, *duration_ms, now_ms, newest) {
                Some((from_ms, to_ms)) => WindowSpec::Stamps { from_ms, to_ms },
                None => WindowSpec::Nothing,
            }
        }
        WindowReduceWindow::Since { start_ms } => WindowSpec::Stamps { from_ms: *start_ms, to_ms: u64::MAX },
        WindowReduceWindow::LastN { n } => WindowSpec::Newest { n: *n },
        WindowReduceWindow::NewestRows { .. } => WindowSpec::Rows,
    }
}

/// The samples a resolved window names, newest first. A range of instants yields at most `cap`
/// samples; a newest-`n` window yields at most `n`; a rows window yields at most `cap`, and its
/// caller stops reading at the sample that fills it.
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
        WindowSpec::Rows => series.range(..).rev().take(cap).collect(),
        WindowSpec::Nothing => Vec::new(),
    }
}

/// [`resolve_window`] then [`samples_of`]: the samples `window` names in `series`, newest first.
pub(crate) fn window_samples<'s, S: TimestampedSeries>(
    series: &'s S,
    window: &WindowReduceWindow,
    anchor: WindowReduceAnchor,
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
        window: &WindowReduceWindow,
        anchor: WindowReduceAnchor,
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
            WindowReduceWindow::Between { start_ms, end_ms } => bounded(*start_ms, *end_ms),
            WindowReduceWindow::Last { duration_ms } => match anchor {
                WindowReduceAnchor::Request => bounded(now_ms.saturating_sub(*duration_ms), now_ms),
                WindowReduceAnchor::Newest => match series.keys().next_back() {
                    Some(&newest) if *duration_ms > 0 => {
                        let start = (u128::from(newest) + 1).saturating_sub(u128::from(*duration_ms)) as u64;
                        bounded(start, newest)
                    }
                    _ => Vec::new(),
                },
            },
            WindowReduceWindow::Since { start_ms } => bounded(*start_ms, u64::MAX),
            WindowReduceWindow::LastN { n } => series.keys().rev().take(*n).copied().collect(),
            // The samples a rows window may need, newest first; the caller counts their cells.
            WindowReduceWindow::NewestRows { .. } => series.keys().rev().take(cap).copied().collect(),
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
                    WindowReduceWindow::Between { start_ms: a, end_ms: b },
                    WindowReduceWindow::Last { duration_ms },
                    WindowReduceWindow::Since { start_ms: a },
                    WindowReduceWindow::LastN { n },
                    WindowReduceWindow::NewestRows { n: n + 1 },
                    WindowReduceWindow::Between { start_ms: 0, end_ms: u64::MAX },
                    WindowReduceWindow::Last { duration_ms: u64::MAX },
                ];
                for window in &windows {
                    for anchor in [WindowReduceAnchor::Request, WindowReduceAnchor::Newest] {
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
            resolve_window(&WindowReduceWindow::Last { duration_ms: 40 }, WindowReduceAnchor::Request, 100, never),
            WindowSpec::Stamps { from_ms: 60, to_ms: 100 }
        );
        assert_eq!(
            resolve_window(&WindowReduceWindow::Last { duration_ms: 500 }, WindowReduceAnchor::Request, 100, never),
            WindowSpec::Stamps { from_ms: 0, to_ms: 100 }
        );
        assert_eq!(
            resolve_window(&WindowReduceWindow::Last { duration_ms: 40 }, WindowReduceAnchor::Newest, 100, || Some(70)),
            WindowSpec::Stamps { from_ms: 31, to_ms: 70 }
        );
        assert_eq!(
            resolve_window(&WindowReduceWindow::Last { duration_ms: 40 }, WindowReduceAnchor::Newest, 100, || None),
            WindowSpec::Nothing
        );
        assert_eq!(
            resolve_window(&WindowReduceWindow::Last { duration_ms: 0 }, WindowReduceAnchor::Newest, 100, || Some(70)),
            WindowSpec::Nothing
        );
        assert_eq!(
            resolve_window(&WindowReduceWindow::Since { start_ms: 9 }, WindowReduceAnchor::Request, 100, never),
            WindowSpec::Stamps { from_ms: 9, to_ms: u64::MAX }
        );
        assert_eq!(
            resolve_window(&WindowReduceWindow::LastN { n: 3 }, WindowReduceAnchor::Request, 100, never),
            WindowSpec::Newest { n: 3 }
        );
        assert_eq!(
            resolve_window(&WindowReduceWindow::Between { start_ms: 5, end_ms: 6 }, WindowReduceAnchor::Request, 100, never),
            WindowSpec::Stamps { from_ms: 5, to_ms: 6 }
        );
        assert_eq!(
            resolve_window(&WindowReduceWindow::NewestRows { n: 7 }, WindowReduceAnchor::Request, 100, never),
            WindowSpec::Rows
        );
    }

    #[test]
    fn a_rows_window_hands_back_samples_up_to_the_cap_and_never_asks_for_the_newest_instant() {
        let series = series_of(&[10, 20, 30, 40, 50]);
        let rows = WindowReduceWindow::NewestRows { n: 1 };
        // The count of rows is not the walk's: it hands back the newest samples up to the cap.
        assert_eq!(stamps_of(window_samples(&series, &rows, WindowReduceAnchor::Request, 0, 3)), vec![50, 40, 30]);
        assert_eq!(stamps_of(window_samples(&series, &rows, WindowReduceAnchor::Request, 0, 9)), vec![50, 40, 30, 20, 10]);
        assert!(window_samples(&series_of(&[]), &rows, WindowReduceAnchor::Request, 0, 9).is_empty());
    }

    #[test]
    fn a_rows_window_takes_whole_samples_and_cuts_only_the_one_that_crosses_its_count() {
        // Samples visited newest first hold 2, 3, 4, 5 cells; the window wants 7.
        let mut window = RowsWindow::new(7);
        assert_eq!(window.take(2), (0, false));
        assert_eq!(window.take(3), (0, false));
        // 5 held, room for 2 of the 4 cells: the first 2 are left out.
        assert_eq!(window.take(4), (2, true));
        // A sample that fills the window exactly is taken whole.
        let mut exact = RowsWindow::new(5);
        assert_eq!(exact.take(2), (0, false));
        assert_eq!(exact.take(3), (0, true));
        // The first sample may be the one that is cut, and an empty sample adds nothing.
        let mut first = RowsWindow::new(3);
        assert_eq!(first.take(0), (0, false));
        assert_eq!(first.take(10), (7, true));
        // A window of one row takes the last cell of the newest sample.
        assert_eq!(RowsWindow::new(1).take(6), (5, true));
    }

    #[test]
    fn a_range_stops_at_its_cap_newest_first_and_a_newest_n_is_not_capped_by_it() {
        let series = series_of(&[10, 20, 30, 40, 50]);
        let all = WindowReduceWindow::Between { start_ms: 0, end_ms: u64::MAX };
        assert_eq!(stamps_of(window_samples(&series, &all, WindowReduceAnchor::Request, 0, 3)), vec![50, 40, 30]);
        // Exactly the cap holds all of them, and one more than the series holds is not padded.
        assert_eq!(stamps_of(window_samples(&series, &all, WindowReduceAnchor::Request, 0, 5)), vec![50, 40, 30, 20, 10]);
        assert_eq!(stamps_of(window_samples(&series, &all, WindowReduceAnchor::Request, 0, 6)), vec![50, 40, 30, 20, 10]);
        let newest = WindowReduceWindow::LastN { n: 4 };
        assert_eq!(stamps_of(window_samples(&series, &newest, WindowReduceAnchor::Request, 0, 1)), vec![50, 40, 30, 20]);
        // Reversed ends name nothing and do not panic.
        let reversed = WindowReduceWindow::Between { start_ms: 40, end_ms: 10 };
        assert!(window_samples(&series, &reversed, WindowReduceAnchor::Request, 0, 9).is_empty());
        // An empty series answers nothing for every kind.
        let empty = series_of(&[]);
        for window in [all, newest, WindowReduceWindow::Last { duration_ms: 5 }, WindowReduceWindow::Since { start_ms: 0 }] {
            for anchor in [WindowReduceAnchor::Request, WindowReduceAnchor::Newest] {
                assert!(window_samples(&empty, &window, anchor, 7, 9).is_empty());
            }
        }
    }
}
