// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! How long a writer waits while a maintenance round runs, and in which phase of the round.
//!
//! A round that holds the shard write guard makes every writer of that shard wait for it, so the
//! cost a writer sees is the LONGEST hold, not the round's busy time. This probe measures that: a
//! writer thread issues string writes in a loop against the shard while the round runs on the
//! calling thread, and every write's latency is recorded against the phase the round was in when
//! the write began. A write that began in a phase and was served only after the round moved on was
//! blocked by that phase's hold, which is the attribution a hold bound needs.
//!
//! THE DEFINITION OF A STALL is the one the matched-run rig uses: a stretch with no acknowledged
//! write. The rig counts seconds with no completed set and reports the longest run of them; this
//! probe reports the longest single wait in microseconds, because a round in a test is far shorter
//! than a second. A write that waited W microseconds is W microseconds in which this writer got no
//! acknowledgement, so `max_wait_us` is the rig's `longest stall` at a finer resolution, and a rig
//! figure of S seconds corresponds to a wait of S * 1,000,000 here. The rig's per-second p99 has no
//! counterpart for a sub-second round and is not reported.
//!
//! TWO WAYS TO NAME A WINDOW. A round that names its phases through [`set_phase`] gets them
//! attributed as it runs. A round that does not, such as the real storage loop, is attributed
//! afterwards: every write is kept with its start time on one clock, and
//! [`WriterStall::window_stalls`] places the writes into windows the caller gives, for example the
//! stage rows of the loop report (`start_offset_us`, `duration_us`). The idle baseline is the writes
//! that began outside the round, so a stage's p99 and longest wait read against the same writer on
//! the same shard in the same run.
//!
//! The phase marker is process-wide, so one probe may run at a time: tests that use it belong in a
//! module whose tests do not overlap, or must be run with `--test-threads=1`.

use super::*;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

static PHASE: Mutex<&'static str> = Mutex::new("outside the round");

/// Called by a round at the start of each phase. Test builds only.
pub(crate) fn set_phase(name: &'static str) {
    *PHASE.lock().expect("phase marker poisoned") = name;
}

fn current_phase() -> &'static str {
    *PHASE.lock().expect("phase marker poisoned")
}

/// One write the probe's writer issued: when it began, on the probe's clock, and how long it took.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WriteSample {
    /// Microseconds from the start of the measurement to the moment the write was issued.
    pub(crate) started_us: u128,
    pub(crate) wait_us: u128,
}

/// What the writer saw inside one window.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WindowStall {
    pub(crate) name: String,
    /// Writes that began inside the window.
    pub(crate) writes: usize,
    pub(crate) p50_us: u128,
    pub(crate) p99_us: u128,
    /// The longest wait of a write that began inside the window: the stall the window caused.
    pub(crate) max_wait_us: u128,
}

/// What the writer saw while the round ran.
#[derive(Debug, Default)]
pub(crate) struct WriterStall {
    /// Writes completed while the round ran.
    pub(crate) writes: usize,
    /// The longest any single write waited, in microseconds.
    pub(crate) max_wait_us: u128,
    /// The longest wait of a write that began in each phase, in order of first appearance.
    pub(crate) max_wait_us_by_phase: Vec<(&'static str, u128)>,
    /// How long the round itself took, in microseconds.
    pub(crate) round_us: u128,
    /// Every write, in the order issued. `started_us` shares a clock with `round_start_us`.
    pub(crate) samples: Vec<WriteSample>,
    /// Microseconds from the start of the measurement to the moment the round was called.
    pub(crate) round_start_us: u128,
}

impl WriterStall {
    pub(crate) fn max_wait_in(&self, phase: &str) -> Option<u128> {
        self.max_wait_us_by_phase
            .iter()
            .find(|(name, _)| *name == phase)
            .map(|(_, wait)| *wait)
    }

    /// The writes that began before the round was called or after it returned: the same writer's
    /// latency with no round running, which every window is read against.
    pub(crate) fn idle_baseline(&self) -> WindowStall {
        let round_end = self.round_start_us + self.round_us;
        summarize(
            "idle",
            self.samples
                .iter()
                .filter(|s| s.started_us < self.round_start_us || s.started_us >= round_end),
        )
    }

    /// The writes that began inside each window. A window is `(name, start_offset_us,
    /// duration_us)` measured from the moment the round was called, which is the clock the loop
    /// report's stage rows use. A write is charged to the window it BEGAN in, so one that began in
    /// a stage and was served after the stage ended is that stage's wait. The offset between the
    /// moment the round was called and the moment the round took its own first timestamp is a few
    /// microseconds and is not corrected for.
    pub(crate) fn window_stalls(&self, windows: &[(String, u64, u64)]) -> Vec<WindowStall> {
        windows
            .iter()
            .map(|(name, offset_us, duration_us)| {
                let from = self.round_start_us + u128::from(*offset_us);
                let to = from + u128::from(*duration_us);
                summarize(
                    name,
                    self.samples
                        .iter()
                        .filter(|s| s.started_us >= from && s.started_us < to),
                )
            })
            .collect()
    }
}

/// The value at `permille` thousandths of an already sorted slice, by the nearest-rank method: the
/// smallest value with at least that share of the sample at or below it. Zero for no samples.
pub(crate) fn percentile_us(sorted: &[u128], permille: u32) -> u128 {
    if sorted.is_empty() {
        return 0;
    }
    let rank = (sorted.len() as u128 * u128::from(permille)).div_ceil(1000).max(1);
    sorted[(rank as usize - 1).min(sorted.len() - 1)]
}

fn summarize<'a>(name: &str, samples: impl Iterator<Item = &'a WriteSample>) -> WindowStall {
    let mut waits: Vec<u128> = samples.map(|s| s.wait_us).collect();
    waits.sort_unstable();
    WindowStall {
        name: name.to_string(),
        writes: waits.len(),
        p50_us: percentile_us(&waits, 500),
        p99_us: percentile_us(&waits, 990),
        max_wait_us: waits.last().copied().unwrap_or(0),
    }
}

/// Run `round` on this thread while a writer thread writes to `shard_id`, and report the waits.
///
/// The writer starts before the round and is stopped after it, so a hold at the very start or end
/// of the round is seen. A short pause lets the writer settle into its loop first.
pub(crate) fn measure_writer_stall(
    engine: &Arc<TemporalEngine>,
    shard_id: ShardId,
    round: impl FnOnce(),
) -> WriterStall {
    measure_writer_stall_with(engine, shard_id, round).0
}

/// [`measure_writer_stall`] for a round that returns something the caller needs, such as the loop
/// report whose stage rows name the windows.
pub(crate) fn measure_writer_stall_with<R>(
    engine: &Arc<TemporalEngine>,
    shard_id: ShardId,
    round: impl FnOnce() -> R,
) -> (WriterStall, R) {
    set_phase("outside the round");
    let epoch = Instant::now();
    let stop = Arc::new(AtomicBool::new(false));
    let writer = {
        let engine = Arc::clone(engine);
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            let mut by_phase: Vec<(&'static str, u128)> = Vec::new();
            let mut samples: Vec<WriteSample> = Vec::new();
            let mut writes = 0usize;
            let mut max_wait_us = 0u128;
            let mut sequence = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let phase = current_phase();
                let started = Instant::now();
                let response = engine.execute(ExecuteRequest {
                    shard_id,
                    command: Command::StringSet {
                        key: format!("stall-probe-{sequence}"),
                        value: vec![1u8; 16],
                    },
                });
                let waited = started.elapsed().as_micros();
                assert!(response.status.ok, "{:?}", response.status);
                sequence += 1;
                writes += 1;
                max_wait_us = max_wait_us.max(waited);
                samples.push(WriteSample {
                    started_us: started.duration_since(epoch).as_micros(),
                    wait_us: waited,
                });
                match by_phase.iter_mut().find(|(name, _)| *name == phase) {
                    Some((_, longest)) => *longest = (*longest).max(waited),
                    None => by_phase.push((phase, waited)),
                }
                std::thread::sleep(Duration::from_micros(200));
            }
            (writes, max_wait_us, by_phase, samples)
        })
    };
    std::thread::sleep(Duration::from_millis(20));
    let started = Instant::now();
    let round_start_us = started.duration_since(epoch).as_micros();
    let value = round();
    let round_us = started.elapsed().as_micros();
    set_phase("outside the round");
    std::thread::sleep(Duration::from_millis(20));
    stop.store(true, Ordering::Relaxed);
    let (writes, max_wait_us, max_wait_us_by_phase, samples) =
        writer.join().expect("writer thread");
    (
        WriterStall {
            writes,
            max_wait_us,
            max_wait_us_by_phase,
            round_us,
            samples,
            round_start_us,
        },
        value,
    )
}

#[cfg(test)]
mod window_math_tests {
    use super::*;

    fn stall(round_start_us: u128, round_us: u128, samples: &[(u128, u128)]) -> WriterStall {
        WriterStall {
            round_start_us,
            round_us,
            samples: samples
                .iter()
                .map(|(started_us, wait_us)| WriteSample { started_us: *started_us, wait_us: *wait_us })
                .collect(),
            ..WriterStall::default()
        }
    }

    #[test]
    fn nearest_rank_percentiles_of_a_known_sample() {
        let hundred: Vec<u128> = (1..=100).collect();
        assert_eq!(percentile_us(&hundred, 500), 50);
        assert_eq!(percentile_us(&hundred, 990), 99);
        assert_eq!(percentile_us(&hundred, 1000), 100);
        assert_eq!(percentile_us(&[7], 990), 7);
        assert_eq!(percentile_us(&[], 990), 0);
    }

    /// A write is charged to the window it BEGAN in, the idle baseline is what began outside the
    /// round, and a window's edges are start-inclusive and end-exclusive so no write is counted twice.
    #[test]
    fn a_write_is_charged_to_the_window_it_began_in() {
        // The round is called 1,000 us in and lasts 600 us: stage "a" is [0, 200), "b" is [200, 600).
        let s = stall(
            1_000,
            600,
            &[
                (100, 5),    // before the round: idle
                (1_050, 9),  // in a
                (1_199, 11), // in a, last instant
                (1_200, 900), // in b, first instant: began in b although it waited past the end
                (1_550, 7),  // in b
                (1_600, 6),  // at the round's end: idle
                (2_000, 4),  // after: idle
            ],
        );
        let windows = vec![("a".to_string(), 0u64, 200u64), ("b".to_string(), 200, 400)];
        let by_window = s.window_stalls(&windows);
        assert_eq!((by_window[0].writes, by_window[0].max_wait_us), (2, 11), "{by_window:?}");
        assert_eq!((by_window[1].writes, by_window[1].max_wait_us), (2, 900), "{by_window:?}");
        let idle = s.idle_baseline();
        assert_eq!((idle.writes, idle.max_wait_us), (3, 6), "{idle:?}");
    }
}
