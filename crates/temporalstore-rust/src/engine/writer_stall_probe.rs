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
//! The round names its phases through [`set_phase`]. The marker is process-wide, so one probe may
//! run at a time: tests that use it belong in a module whose tests do not overlap, or must be
//! run with `--test-threads=1`.

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
}

impl WriterStall {
    pub(crate) fn max_wait_in(&self, phase: &str) -> Option<u128> {
        self.max_wait_us_by_phase
            .iter()
            .find(|(name, _)| *name == phase)
            .map(|(_, wait)| *wait)
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
    set_phase("outside the round");
    let stop = Arc::new(AtomicBool::new(false));
    let writer = {
        let engine = Arc::clone(engine);
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            let mut by_phase: Vec<(&'static str, u128)> = Vec::new();
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
                match by_phase.iter_mut().find(|(name, _)| *name == phase) {
                    Some((_, longest)) => *longest = (*longest).max(waited),
                    None => by_phase.push((phase, waited)),
                }
                std::thread::sleep(Duration::from_micros(200));
            }
            (writes, max_wait_us, by_phase)
        })
    };
    std::thread::sleep(Duration::from_millis(20));
    let started = Instant::now();
    round();
    let round_us = started.elapsed().as_micros();
    set_phase("outside the round");
    std::thread::sleep(Duration::from_millis(20));
    stop.store(true, Ordering::Relaxed);
    let (writes, max_wait_us, max_wait_us_by_phase) = writer.join().expect("writer thread");
    WriterStall { writes, max_wait_us, max_wait_us_by_phase, round_us }
}
