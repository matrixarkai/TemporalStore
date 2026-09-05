// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! One ordered sender per follower.
//!
//! Replication used to fan out from two independent places -- each propose spawned its own
//! threads, and the timer loop sent heartbeats and catch-up batches on its own -- so two appends
//! to the same follower could be in flight at once, and under real network latency they arrived
//! out of order. The follower's rejections then did, badly, the job a sender-side window should
//! do: measured on a live cluster, the interleaving drove election churn and snapshot storms
//! that no loopback test reproduces, because loopback round trips never let the sends overlap.
//!
//! This module gives every follower exactly one sender thread, and every append to that follower
//! goes through it: propose-driven entries, catch-up batches, heartbeats, and the snapshot
//! fallback. Sends are synchronous, so at most one append is in flight per follower and order
//! holds by construction; throughput comes from batching, not pipelining depth. Proposers no
//! longer replicate at all -- they append to the leader's log, ring the senders, and wait for
//! the quorum commit signal. The commit index reaches followers on the next append or heartbeat
//! instead of a dedicated second round trip per propose.
//!
//! The senders also carry the liveness bookkeeping that used to ride the timer loop's own sends
//! (a peer marked dead after consecutive failures, alive again on any success, a stand-down when
//! a response names a newer term). If they did not, the timer loop would still have to probe
//! peers itself -- and two probes to one follower is exactly the interleaving this exists to end.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use super::*;

/// How long a sender sleeps after a failed send, so an unreachable peer costs retries on this
/// cadence rather than a busy loop.
const SEND_FAILURE_BACKOFF_MS: u64 = 50;

/// Consecutive rejections without progress before the sender concludes the log cannot reach this
/// peer and installs a snapshot instead. Rejections walk `next_index` back one probe at a time,
/// so a peer that is merely behind converges well before this.
const REJECTIONS_BEFORE_SNAPSHOT: u32 = 8;

#[derive(Default)]
struct PeerWake {
    /// New log entries may be waiting for this peer.
    entries: bool,
    /// A heartbeat is due whether or not anything is pending.
    heartbeat: bool,
    /// This sender generation is shutting down.
    stop: bool,
}

/// The doorbell for one follower's sender thread.
struct PeerSignal {
    wake: Mutex<PeerWake>,
    changed: Condvar,
}

impl PeerSignal {
    fn new() -> Self {
        Self {
            wake: Mutex::new(PeerWake::default()),
            changed: Condvar::new(),
        }
    }

    fn ring_entries(&self) {
        let mut wake = self.wake.lock().expect("peer signal poisoned");
        wake.entries = true;
        self.changed.notify_one();
    }

    fn ring_heartbeat(&self) {
        let mut wake = self.wake.lock().expect("peer signal poisoned");
        wake.heartbeat = true;
        self.changed.notify_one();
    }

    fn ring_stop(&self) {
        let mut wake = self.wake.lock().expect("peer signal poisoned");
        wake.stop = true;
        self.changed.notify_all();
    }

    /// Block until something rings, then take and clear the reasons.
    fn wait(&self) -> (bool, bool) {
        let mut wake = self.wake.lock().expect("peer signal poisoned");
        while !(wake.entries || wake.heartbeat || wake.stop) {
            wake = self.changed.wait(wake).expect("peer signal poisoned");
        }
        let out = (wake.heartbeat, wake.stop);
        wake.entries = false;
        wake.heartbeat = false;
        out
    }
}

/// What the sender knows about one peer's reachability, read by check-quorum.
pub(crate) struct PeerHealth {
    /// Milliseconds since pipeline start of the last successful exchange; 0 = never.
    last_ok_at_ms: AtomicU64,
    consecutive_failures: AtomicU64,
}

/// The per-follower senders for one cluster.
pub(crate) struct FollowerPipeline {
    signals: BTreeMap<RaftNodeId, Arc<PeerSignal>>,
    health: BTreeMap<RaftNodeId, Arc<PeerHealth>>,
    /// The peer set the senders were built for; a change rebuilds the pipeline.
    peer_set: Vec<RaftNodeId>,
    started: Instant,
    stopping: Arc<AtomicBool>,
}

impl FollowerPipeline {
    fn now_ms(&self) -> u64 {
        self.started.elapsed().as_millis() as u64
    }

    fn ring_entries(&self) {
        for signal in self.signals.values() {
            signal.ring_entries();
        }
    }

    fn ring_heartbeat(&self) {
        for signal in self.signals.values() {
            signal.ring_heartbeat();
        }
    }

    pub(crate) fn stop(&self) {
        self.stopping.store(true, Ordering::SeqCst);
        for signal in self.signals.values() {
            signal.ring_stop();
        }
    }

    /// How many peers answered a send within the last `window_ms`. Check-quorum counts these
    /// plus the leader itself.
    fn reached_within_ms(&self, window_ms: u64) -> usize {
        let now = self.now_ms();
        self.health
            .values()
            .filter(|health| {
                let at = health.last_ok_at_ms.load(Ordering::Relaxed);
                at > 0 && now.saturating_sub(at) <= window_ms
            })
            .count()
    }
}

impl RaftCluster {
    /// Bring the per-follower senders up, or rebuild them after the peer set changed. Cheap when
    /// already current: one lock and a comparison.
    pub(crate) fn ensure_follower_pipeline<T>(&self, transport: &T)
    where
        T: RaftTransport + Clone + Send + 'static,
    {
        let (local, mut wanted) = {
            let inner = self.inner.read().expect("raft cluster lock poisoned");
            let ids: Vec<RaftNodeId> = inner.nodes.keys().copied().collect();
            (inner.local_node_id, ids)
        };
        if let Some(local) = local {
            wanted.retain(|id| *id != local);
        } else {
            // The in-process topology has no single local node; the leader may be any of them.
            // Every node gets a sender, and a sender simply idles while its target is the
            // leader itself.
            let leader = {
                let inner = self.inner.read().expect("raft cluster lock poisoned");
                inner.leader_id
            };
            wanted.retain(|id| *id != leader);
        }
        let mut pipeline = self.follower_pipeline.lock().expect("pipeline lock poisoned");
        if let Some(existing) = pipeline.as_ref() {
            if existing.peer_set == wanted {
                return;
            }
            existing.stop();
        }
        let stopping = Arc::new(AtomicBool::new(false));
        let mut signals = BTreeMap::new();
        let mut health = BTreeMap::new();
        let started = Instant::now();
        for peer in &wanted {
            let signal = Arc::new(PeerSignal::new());
            let peer_health = Arc::new(PeerHealth {
                last_ok_at_ms: AtomicU64::new(0),
                consecutive_failures: AtomicU64::new(0),
            });
            signals.insert(*peer, Arc::clone(&signal));
            health.insert(*peer, Arc::clone(&peer_health));
            let cluster = self.clone();
            let transport = transport.clone();
            let peer = *peer;
            let stopping = Arc::clone(&stopping);
            std::thread::spawn(move || {
                sender_loop(cluster, transport, peer, signal, peer_health, stopping, started)
            });
        }
        *pipeline = Some(FollowerPipeline {
            signals,
            health,
            peer_set: wanted,
            started,
            stopping,
        });
    }

    /// Wake every sender: new entries are in the log.
    pub(crate) fn ring_replication(&self) {
        if let Some(pipeline) = self
            .follower_pipeline
            .lock()
            .expect("pipeline lock poisoned")
            .as_ref()
        {
            pipeline.ring_entries();
        }
    }

    /// Wake every sender for a heartbeat round.
    pub(crate) fn ring_heartbeats(&self) {
        if let Some(pipeline) = self
            .follower_pipeline
            .lock()
            .expect("pipeline lock poisoned")
            .as_ref()
        {
            pipeline.ring_heartbeat();
        }
    }

    /// Renew the leader lease after the timer loop confirmed quorum contact through the
    /// senders. The fan-out's own heartbeat round renews the lease as acks come back, but
    /// sender heartbeats fold their acks into the commit advance, which renews only when a
    /// commit actually moves -- so an IDLE leader's lease decayed, and the first propose
    /// after any gap longer than the lease bounced off `leader_lease_valid` until something
    /// committed. On hardware that surfaced as ~400 ms per propose on every cadence slower
    /// than the lease window, while busy cells never saw it.
    pub(crate) fn renew_leader_lease_after_quorum_contact(&self) {
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        let leader_id = inner.leader_id;
        let is_leader = inner
            .nodes
            .get(&leader_id)
            .map(|node| node.alive && node.role == RaftRole::Leader)
            .unwrap_or(false);
        if is_leader {
            inner.renew_leader_lease();
        }
    }

    /// Peers that answered within `window_ms`, for check-quorum. The leader counts itself.
    pub(crate) fn pipeline_reached_within(&self, window_ms: u64) -> usize {
        self.follower_pipeline
            .lock()
            .expect("pipeline lock poisoned")
            .as_ref()
            .map(|pipeline| pipeline.reached_within_ms(window_ms))
            .unwrap_or(0)
    }

    /// Block until the quorum commit reaches `index`, or the deadline passes.
    pub(crate) fn wait_for_quorum_commit(&self, index: u64, deadline: Duration) -> bool {
        let started = Instant::now();
        let (epoch_lock, condvar) = &*self.commit_signal;
        let mut epoch = epoch_lock.lock().expect("commit signal poisoned");
        loop {
            {
                let inner = self.inner.read().expect("raft cluster lock poisoned");
                let leader_id = inner.leader_id;
                if inner
                    .nodes
                    .get(&leader_id)
                    .map(|leader| leader.commit_index >= index)
                    .unwrap_or(false)
                {
                    return true;
                }
            }
            let elapsed = started.elapsed();
            if elapsed >= deadline {
                return false;
            }
            let (next, _timeout) = condvar
                .wait_timeout(epoch, deadline - elapsed)
                .expect("commit signal poisoned");
            epoch = next;
        }
    }

    /// Fold successful replication into the quorum commit, waking every waiting proposer.
    pub(crate) fn advance_quorum_commit(&self) {
        // Cheap pre-check under the read lock: every sender calls this after every response, and
        // most calls have nothing to advance. Taking the write lock for those serializes the
        // senders against the proposers appending under the same lock.
        {
            let inner = self.inner.read().expect("raft cluster lock poisoned");
            let leader_id = inner.leader_id;
            let Some(leader) = inner.nodes.get(&leader_id) else {
                return;
            };
            if leader.role != RaftRole::Leader {
                return;
            }
            let required = inner.required_majority();
            let leader_last = node_next_log_index(leader).saturating_sub(1);
            let mut matched: Vec<u64> = Vec::new();
            for (id, node) in &inner.nodes {
                if !node.replica_role.participates_in_quorum() {
                    continue;
                }
                matched.push(if *id == leader_id {
                    leader_last
                } else {
                    node.pipeline_state.match_index
                });
            }
            if matched.len() < required {
                return;
            }
            matched.sort_unstable_by(|left, right| right.cmp(left));
            if matched[required - 1] <= leader.commit_index {
                return;
            }
        }
        let advanced = {
            let mut inner = self.inner.write().expect("raft cluster lock poisoned");
            let advanced = inner.maybe_advance_quorum_commit();
            if advanced {
                // Commit-index durability keeps the old path's cadence: persisted with the
                // record that carries it. It is recoverable state either way.
                let _ = inner.persist_configured_wal();
            }
            advanced
        };
        if advanced {
            let (epoch_lock, condvar) = &*self.commit_signal;
            let mut epoch = epoch_lock.lock().expect("commit signal poisoned");
            *epoch = epoch.wrapping_add(1);
            condvar.notify_all();
        }
    }
}

/// One follower's sender: the only thread that ever sends this follower anything.
#[allow(clippy::too_many_arguments)]
fn sender_loop<T>(
    cluster: RaftCluster,
    transport: T,
    peer: RaftNodeId,
    signal: Arc<PeerSignal>,
    health: Arc<PeerHealth>,
    stopping: Arc<AtomicBool>,
    started: Instant,
) where
    T: RaftTransport + Clone + Send + 'static,
{
    let mut consecutive_rejections: u32 = 0;
    loop {
        let (heartbeat, stop) = signal.wait();
        if stop || stopping.load(Ordering::SeqCst) {
            return;
        }
        // Drain: keep sending while this peer is behind. One request at a time, response awaited
        // before the next, so order per follower holds by construction.
        let mut send_heartbeat = heartbeat;
        loop {
            if stopping.load(Ordering::SeqCst) {
                return;
            }
            match pipeline_step(
                &cluster,
                &transport,
                peer,
                send_heartbeat,
                &health,
                started,
                &mut consecutive_rejections,
            ) {
                PipelineStep::Sent => {
                    send_heartbeat = false;
                    continue;
                }
                PipelineStep::Idle => break,
                PipelineStep::Backoff => {
                    std::thread::sleep(Duration::from_millis(SEND_FAILURE_BACKOFF_MS));
                    break;
                }
            }
        }
    }
}

enum PipelineStep {
    /// A request went out and its outcome was folded in; there may be more to send.
    Sent,
    /// Nothing to send: the peer is caught up and no heartbeat is due.
    Idle,
    /// The send failed outright; wait before hammering an unreachable peer.
    Backoff,
}

/// Send at most one request to `peer` and fold its outcome into the cluster state.
fn pipeline_step<T>(
    cluster: &RaftCluster,
    transport: &T,
    peer: RaftNodeId,
    heartbeat: bool,
    health: &PeerHealth,
    started: Instant,
    consecutive_rejections: &mut u32,
) -> PipelineStep
where
    T: RaftTransport + Clone + Send + 'static,
{
    // Only a live leader sends; anything else idles until the next ring.
    let (behind, local_node_id) = {
        let inner = cluster.inner.read().expect("raft cluster lock poisoned");
        let leader_id = inner.leader_id;
        if peer == leader_id {
            return PipelineStep::Idle;
        }
        if let Some(local) = inner.local_node_id {
            if local != leader_id {
                return PipelineStep::Idle;
            }
        }
        let Some(leader) = inner.nodes.get(&leader_id) else {
            return PipelineStep::Idle;
        };
        if !leader.alive || leader.role != RaftRole::Leader {
            return PipelineStep::Idle;
        }
        let last = node_next_log_index(leader).saturating_sub(1);
        let leader_commit = leader.commit_index;
        let (next, peer_commit) = inner
            .nodes
            .get(&peer)
            .map(|node| (node.pipeline_state.next_index, node.commit_index))
            .unwrap_or((1, 0));
        // Commit lag counts as behind: an entry-less append carries the commit index, and a
        // follower that has not heard it keeps its in-flight window charged -- which blocks the
        // next real entry at the window and, with no heartbeat timer running (tests), forever.
        let commit_lagging = peer_commit < leader_commit;
        (last >= next || commit_lagging, inner.local_node_id)
    };
    if !behind && !heartbeat {
        return PipelineStep::Idle;
    }

    if *consecutive_rejections >= REJECTIONS_BEFORE_SNAPSHOT {
        *consecutive_rejections = 0;
        return pipeline_send_snapshot(cluster, transport, peer);
    }

    let request = match cluster.build_append_entries_request(peer) {
        Ok(request) => request,
        Err(_) => return PipelineStep::Backoff,
    };
    match transport.append_entries(request) {
        Ok(response) => {
            health
                .last_ok_at_ms
                .store(started.elapsed().as_millis().max(1) as u64, Ordering::Relaxed);
            health.consecutive_failures.store(0, Ordering::Relaxed);
            let success = response.success;
            let peer_term = response.term;
            let _ = cluster.record_append_entries_response(peer, &response);
            let _ = cluster.set_alive(peer, true);
            if success {
                *consecutive_rejections = 0;
                cluster.advance_quorum_commit();
            } else {
                *consecutive_rejections = consecutive_rejections.saturating_add(1);
                if peer_term > 0 {
                    // A peer on a newer term has seen a different leader: stand down rather
                    // than keep appending as a superseded one.
                    if let Some(local) = local_node_id {
                        let current = {
                            let inner =
                                cluster.inner.read().expect("raft cluster lock poisoned");
                            inner
                                .nodes
                                .get(&inner.leader_id)
                                .map(|leader| leader.current_term)
                                .unwrap_or_default()
                        };
                        if peer_term > current {
                            let _ = cluster.observe_higher_term(local, peer_term);
                        }
                    }
                }
            }
            // A rejection retreated `next_index` inside record_response; sending again
            // immediately is the probe walking backwards to where the follower really is.
            PipelineStep::Sent
        }
        Err(_) => {
            let failures = health
                .consecutive_failures
                .fetch_add(1, Ordering::Relaxed)
                .saturating_add(1);
            let _ = cluster.record_append_entries_send_failure(peer);
            if failures >= RAFT_PEER_FAILURE_THRESHOLD as u64 {
                let _ = cluster.set_alive(peer, false);
            }
            PipelineStep::Backoff
        }
    }
}

/// The log cannot reach this peer: install a snapshot, on this sender's thread so it serializes
/// with the peer's appends instead of racing them.
fn pipeline_send_snapshot<T>(
    cluster: &RaftCluster,
    transport: &T,
    peer: RaftNodeId,
) -> PipelineStep
where
    T: RaftTransport + Clone + Send + 'static,
{
    let request = match cluster.build_install_snapshot_request(peer) {
        Ok(request) => request,
        Err(_) => return PipelineStep::Backoff,
    };
    match transport.install_snapshot(request) {
        Ok(response) if response.success => {
            let _ = cluster.catch_up(peer);
            PipelineStep::Sent
        }
        _ => PipelineStep::Backoff,
    }
}
