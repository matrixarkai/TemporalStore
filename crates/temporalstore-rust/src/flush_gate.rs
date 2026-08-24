// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Shares one durability barrier between everything that is waiting for one.
//!
//! Used by the raft node log and by the index log: both append and fsync per write, and both
//! had every writer pay for a barrier that would have covered all of them.
//!
//! An `fsync` makes every byte already written to the file durable, not just the caller's own, so
//! a barrier taken while other writers are queued behind it covers them too. Taking one barrier
//! per writer therefore pays repeatedly for work a single barrier would have done: measured at 1,
//! 4 and 16 concurrent writers, barriers-per-write stayed flat at exactly 1.0 --
//! sixteen writers cost sixteen barriers where one would have sufficed.
//!
//! The rule here is that at most one barrier is ever in flight per file. A writer that arrives
//! while one is running does not start a second: it waits, and the next barrier covers it.
//! Barriers-per-write then falls roughly as 1/concurrency instead of staying flat, while a lone
//! writer still takes exactly one barrier and waits no longer than it does today.
//!
//! Ordering is what makes this safe. A writer registers its ticket only after its `write_all` has
//! returned, and a barrier claims a target of "every ticket registered so far", so a ticket a
//! barrier counts is always already in the file. Sequence numbers only increase, so
//! `durable >= mine` is a complete test for "my bytes are on disk".

use std::collections::BTreeMap;
use std::io;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Instant;

/// A writer's claim on a future barrier: "make everything up to here durable".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FlushTicket(u64);

#[derive(Debug, Default)]
struct GateState {
    /// Tickets handed out so far. Every one of these is already written to the file.
    issued: u64,
    /// The highest ticket a completed barrier has covered.
    durable: u64,
    /// Whether a barrier is running right now. At most one ever is.
    in_flight: bool,
}

/// One file's barrier gate.
#[derive(Debug, Default)]
pub struct FlushGate {
    state: Mutex<GateState>,
    changed: Condvar,
}

impl FlushGate {
    /// Claim a barrier for bytes that have ALREADY been written to the file.
    ///
    /// Call this after `write_all` returns and never before: a ticket a barrier can see must
    /// correspond to bytes that barrier will actually flush.
    pub fn register_write(&self) -> FlushTicket {
        let mut state = self.state.lock().expect("wal flush gate poisoned");
        state.issued += 1;
        FlushTicket(state.issued)
    }

    /// Make `ticket` durable, either by taking the barrier or by riding one already in flight.
    ///
    /// Returns how long the caller waited in milliseconds -- the barrier's own duration for the
    /// writer that took it, and the wait for one that rode along.
    pub fn await_durable<F>(&self, ticket: FlushTicket, barrier: F) -> io::Result<u64>
    where
        F: FnOnce() -> io::Result<()>,
    {
        let started = Instant::now();
        let target = {
            let mut state = self.state.lock().expect("wal flush gate poisoned");
            loop {
                if state.durable >= ticket.0 {
                    return Ok(started.elapsed().as_millis() as u64);
                }
                if !state.in_flight {
                    // First to arrive since the last barrier finished: take this one, and cover
                    // everyone who has queued up in the meantime.
                    state.in_flight = true;
                    break state.issued;
                }
                state = self.changed.wait(state).expect("wal flush gate poisoned");
            }
        };
        // The barrier runs with the gate UNLOCKED, which is the whole point: writers keep arriving
        // and registering while it is in flight, and the next barrier sweeps them up.
        let outcome = barrier();
        let mut state = self.state.lock().expect("wal flush gate poisoned");
        state.in_flight = false;
        match outcome {
            Ok(()) => {
                state.durable = state.durable.max(target);
                self.changed.notify_all();
                Ok(started.elapsed().as_millis() as u64)
            }
            Err(err) => {
                // A failed barrier advances `durable` for nobody. Waking the waiters is enough:
                // each re-checks, finds its ticket still not durable and no barrier in flight,
                // and takes its own -- so a failure is retried honestly rather than reported to
                // one thread and silently swallowed for the rest.
                self.changed.notify_all();
                Err(err)
            }
        }
    }
}

/// The gates, one per file, keyed by whatever identifies that file to the caller.
#[derive(Debug, Default)]
pub struct FlushRegistry {
    gates: Mutex<BTreeMap<(u64, u64), Arc<FlushGate>>>,
}

impl FlushRegistry {
    /// The gate for one file, created on first use.
    ///
    /// Keyed per file rather than shared process-wide: one shared gate would let a barrier on one
    /// file report another file's bytes durable, and a process here holds many.
    pub fn gate(&self, key: (u64, u64)) -> Arc<FlushGate> {
        let mut gates = self.gates.lock().expect("wal flush registry poisoned");
        Arc::clone(gates.entry(key).or_default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Barrier;

    #[test]
    fn a_lone_writer_takes_exactly_one_barrier() {
        let gate = FlushGate::default();
        let barriers = AtomicU64::new(0);
        for _ in 0..8 {
            let ticket = gate.register_write();
            gate.await_durable(ticket, || {
                barriers.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
            .unwrap();
        }
        // No concurrency to amortise against, so nothing is saved -- and nothing is lost.
        assert_eq!(barriers.load(Ordering::SeqCst), 8);
    }

    #[test]
    fn writers_that_arrive_during_a_barrier_ride_the_next_one() {
        let gate = Arc::new(FlushGate::default());
        let barriers = Arc::new(AtomicU64::new(0));
        let writers = 16usize;
        // Every writer registers BEFORE any barrier starts, so one barrier could cover them all.
        let tickets: Vec<FlushTicket> = (0..writers).map(|_| gate.register_write()).collect();
        let start = Arc::new(Barrier::new(writers));
        let handles: Vec<_> = tickets
            .into_iter()
            .map(|ticket| {
                let gate = Arc::clone(&gate);
                let barriers = Arc::clone(&barriers);
                let start = Arc::clone(&start);
                std::thread::spawn(move || {
                    start.wait();
                    gate.await_durable(ticket, || {
                        barriers.fetch_add(1, Ordering::SeqCst);
                        std::thread::sleep(std::time::Duration::from_millis(20));
                        Ok(())
                    })
                    .unwrap();
                })
            })
            .collect();
        for handle in handles {
            handle.join().unwrap();
        }
        let taken = barriers.load(Ordering::SeqCst);
        // The point of the exercise: far fewer barriers than writers.
        assert!(
            taken < writers as u64,
            "{taken} barriers for {writers} writers -- nothing coalesced"
        );
        assert!(taken >= 1);
    }

    #[test]
    fn a_failed_barrier_is_reported_and_not_mistaken_for_durability() {
        let gate = FlushGate::default();
        let ticket = gate.register_write();
        let err = gate
            .await_durable(ticket, || Err(io::Error::other("disk gone")))
            .unwrap_err();
        assert!(err.to_string().contains("disk gone"));
        // The ticket is still not durable, so a retry must actually take a barrier.
        let retried = AtomicU64::new(0);
        gate.await_durable(ticket, || {
            retried.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
        .unwrap();
        assert_eq!(retried.load(Ordering::SeqCst), 1);
    }
}
